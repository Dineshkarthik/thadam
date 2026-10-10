//! The open-time insert plan: which Arrow column feeds which table column,
//! and how.
//!
//! Built once in `open()` from `DESCRIBE TABLE` plus the dataset's Arrow
//! schema and the optional `columns` mapping. Every incompatibility found is
//! reported as one list; the sink never starts with a partial plan (the
//! no-DDL rule: it never creates or alters the table).

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::{DataType, FieldRef, TimeUnit};
use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::DateTimePrecision;
use thadam_arrow::columns::{FIRST_DECLARED_ID, LAST_DECLARED_ID};
use thadam_arrow::field_id;

/// One row of `DESCRIBE TABLE`, parsed.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TableCol {
    /// The column name.
    pub(crate) name: String,
    /// The declared type, parsed. A type the client's parser does not know is
    /// kept as its error: only a column the dataset actually maps to has to
    /// be understood.
    pub(crate) ty: Result<DataTypeNode, String>,
    /// `default_type` as reported (`""`, `DEFAULT`, `MATERIALIZED`, `ALIAS`).
    pub(crate) default_kind: String,
}

/// The resolved insertion target of one output column. The wrappers
/// `LowCardinality(_)` and `SimpleAggregateFunction(_, _)` are erased (the
/// server accepts the plain inner type and rebuilds the wrapper);
/// `Nullable` is tracked per column, not here.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Dst {
    /// `Bool`.
    Bool,
    /// `Int8`..`Int64`.
    Int(IntWidth),
    /// `UInt8`..`UInt64`.
    UInt(UIntWidth),
    /// `Float32` or `Float64`.
    Float(FloatWidth),
    /// `String`.
    Str,
    /// `FixedString(n)`. The server would turn an over-long value into NULL
    /// (nullable column) or fail the insert, so the length is checked here and
    /// a value longer than `n` bytes is a data error.
    FixedStr(usize),
    /// `DateTime64(precision)`. Values are rescaled from the Arrow timestamp
    /// unit and tagged `UTC`; values outside ClickHouse's supported range are
    /// data errors.
    Ts(u8),
    /// `DateTime` (seconds, `0..=u32::MAX`); out-of-range values are data
    /// errors.
    DateTime,
    /// `UUID`, fed from a 16-byte Arrow binary column (the `_event_id`
    /// lineage column uses this).
    Uuid,
    /// `Array(<elem>)` of a non-array element (deeper nesting is flatly
    /// rejected when the plan is built).
    Arr {
        /// Whether the array elements are `Nullable` themselves.
        elem_nullable: bool,
        /// The element target.
        elem: Box<Dst>,
    },
}

/// A signed integer width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntWidth {
    /// 8 bits.
    I8,
    /// 16 bits.
    I16,
    /// 32 bits.
    I32,
    /// 64 bits.
    I64,
}

/// An unsigned integer width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UIntWidth {
    /// 8 bits.
    U8,
    /// 16 bits.
    U16,
    /// 32 bits.
    U32,
    /// 64 bits.
    U64,
}

/// A float width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FloatWidth {
    /// 32 bits.
    F32,
    /// 64 bits.
    F64,
}

/// One planned output column.
#[derive(Clone, Debug)]
pub(crate) struct ColPlan {
    /// The table column this feeds.
    pub(crate) table: String,
    /// The index of the source column in the Arrow `RecordBatch`.
    pub(crate) arrow_index: usize,
    /// Whether the table column is `Nullable(..)`.
    pub(crate) nullable: bool,
    /// The resolved target.
    pub(crate) dst: Dst,
    /// The Arrow timestamp unit when the source is a timestamp; rescaling to
    /// the target precision happens at encode time.
    pub(crate) src_ts_nanos: Option<bool>,
}

/// The complete insert plan, Arrow-schema ordered.
#[derive(Clone, Debug)]
pub(crate) struct InsertPlan {
    /// Output columns in `RecordBatch` column order.
    pub(crate) cols: Vec<ColPlan>,
    /// Table columns the dataset does not feed (the server fills their
    /// defaults); reported in the open log line, not an error.
    pub(crate) unmapped_table_cols: Vec<String>,
}

fn erase_wrappers(node: &DataTypeNode) -> &DataTypeNode {
    match node {
        DataTypeNode::LowCardinality(inner) | DataTypeNode::SimpleAggregateFunction(_, inner) => {
            erase_wrappers(inner)
        }
        other => other,
    }
}

/// The nullability of a table column type, wrappers erased.
fn ch_nullable(node: &DataTypeNode) -> bool {
    matches!(erase_wrappers(node), DataTypeNode::Nullable(_))
}

fn precision_u8(p: &DateTimePrecision) -> u8 {
    match p {
        DateTimePrecision::Precision0 => 0,
        DateTimePrecision::Precision1 => 1,
        DateTimePrecision::Precision2 => 2,
        DateTimePrecision::Precision3 => 3,
        DateTimePrecision::Precision4 => 4,
        DateTimePrecision::Precision5 => 5,
        DateTimePrecision::Precision6 => 6,
        DateTimePrecision::Precision7 => 7,
        DateTimePrecision::Precision8 => 8,
        DateTimePrecision::Precision9 => 9,
    }
}

fn scalar_dst(node: &DataTypeNode, col: &str) -> Result<Dst, String> {
    Ok(match node {
        DataTypeNode::Bool => Dst::Bool,
        DataTypeNode::Int8 => Dst::Int(IntWidth::I8),
        DataTypeNode::Int16 => Dst::Int(IntWidth::I16),
        DataTypeNode::Int32 => Dst::Int(IntWidth::I32),
        DataTypeNode::Int64 => Dst::Int(IntWidth::I64),
        DataTypeNode::UInt8 => Dst::UInt(UIntWidth::U8),
        DataTypeNode::UInt16 => Dst::UInt(UIntWidth::U16),
        DataTypeNode::UInt32 => Dst::UInt(UIntWidth::U32),
        DataTypeNode::UInt64 => Dst::UInt(UIntWidth::U64),
        DataTypeNode::Float32 => Dst::Float(FloatWidth::F32),
        DataTypeNode::Float64 => Dst::Float(FloatWidth::F64),
        DataTypeNode::String => Dst::Str,
        DataTypeNode::FixedString(n) => Dst::FixedStr(*n),
        DataTypeNode::DateTime(_) => Dst::DateTime,
        DataTypeNode::DateTime64(prec, _) => Dst::Ts(precision_u8(prec)),
        DataTypeNode::UUID => Dst::Uuid,
        DataTypeNode::Array(elem) => {
            let (elem_nullable, inner) = match erase_wrappers(elem) {
                DataTypeNode::Nullable(inner) => (true, erase_wrappers(inner)),
                other => (false, other),
            };
            let elem = scalar_dst(inner, col)?;
            if matches!(elem, Dst::Arr { .. }) {
                return Err(format!(
                    "column `{col}` is a nested Array (Array(Array(..))), which the ClickHouse \
                     sink cannot encode in this release; flatten nested lists before the sink"
                ));
            }
            Dst::Arr {
                elem_nullable,
                elem: Box::new(elem),
            }
        }
        other => {
            return Err(format!(
                "column `{col}` has type `{other}`, which the ClickHouse sink cannot encode \
                 (supported: Bool, integers, floats, String/FixedString, DateTime(64), UUID, \
                 Array of a scalar, Nullable, LowCardinality); Tuple, Map, Enum, and Decimal \
                 are not supported in this release — flatten records before the sink or drop \
                 the column"
            ));
        }
    })
}

/// Resolves a table column type into an insertion target. Nullable is
/// reported separately by [`ch_nullable`].
fn resolve(ty: &DataTypeNode, col: &str) -> Result<Dst, String> {
    let node = erase_wrappers(ty);
    let core = match node {
        DataTypeNode::Nullable(inner) => erase_wrappers(inner),
        other => other,
    };
    scalar_dst(core, col)
}

/// Parses the `type` string `DESCRIBE TABLE` reports, with the parser the
/// client itself uses.
pub(crate) fn parse_ch_type(text: &str) -> Result<DataTypeNode, String> {
    DataTypeNode::new(text).map_err(|e| format!("unparsable ClickHouse type `{text}`: {e}"))
}

/// For the timestamp units `thadam-arrow` writes: `Some(true)` for
/// nanoseconds, `Some(false)` for microseconds, `None` for anything else.
fn ts_nanos(ty: &DataType) -> Option<bool> {
    match ty {
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(false),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(true),
        _ => None,
    }
}

/// The unsigned flag and width of an Arrow integer type.
fn arrow_int_bits(ty: &DataType) -> Option<(bool, u8)> {
    Some(match ty {
        DataType::Int8 => (false, 8),
        DataType::Int16 => (false, 16),
        DataType::Int32 => (false, 32),
        DataType::Int64 => (false, 64),
        DataType::UInt8 => (true, 8),
        DataType::UInt16 => (true, 16),
        DataType::UInt32 => (true, 32),
        DataType::UInt64 => (true, 64),
        _ => return None,
    })
}

/// Whether an Arrow integer column may feed this target, by width. Unsigned
/// fits into strictly wider signed and any same-or-wider unsigned; signed
/// into same-or-wider signed. `UInt64` into `Int64` is allowed but values
/// above `i64::MAX` are rejected at insert and quarantined (ADR-0008's open
/// unsigned-64 policy item); prefer `UInt64` table columns.
fn int_ok(src_unsigned: bool, src_bits: u8, dst: &Dst) -> bool {
    match (src_unsigned, dst) {
        (true, Dst::UInt(w)) => {
            let dst_bits = match w {
                UIntWidth::U8 => 8,
                UIntWidth::U16 => 16,
                UIntWidth::U32 => 32,
                UIntWidth::U64 => 64,
            };
            src_bits <= dst_bits
        }
        (true, Dst::Int(w)) => {
            let src_fits = match w {
                IntWidth::I8 => false,
                IntWidth::I16 => src_bits <= 8,
                IntWidth::I32 => src_bits <= 16,
                // u64 into i64: see the doc comment above.
                IntWidth::I64 => true,
            };
            src_fits && src_bits <= 64
        }
        (false, Dst::Int(w)) => {
            let dst_bits = match w {
                IntWidth::I8 => 8,
                IntWidth::I16 => 16,
                IntWidth::I32 => 32,
                IntWidth::I64 => 64,
            };
            src_bits <= dst_bits
        }
        _ => false,
    }
}

fn dst_label(dst: &Dst) -> String {
    match dst {
        Dst::Bool => "Bool".to_owned(),
        Dst::Int(IntWidth::I8) => "Int8".to_owned(),
        Dst::Int(IntWidth::I16) => "Int16".to_owned(),
        Dst::Int(IntWidth::I32) => "Int32".to_owned(),
        Dst::Int(IntWidth::I64) => "Int64".to_owned(),
        Dst::UInt(UIntWidth::U8) => "UInt8".to_owned(),
        Dst::UInt(UIntWidth::U16) => "UInt16".to_owned(),
        Dst::UInt(UIntWidth::U32) => "UInt32".to_owned(),
        Dst::UInt(UIntWidth::U64) => "UInt64".to_owned(),
        Dst::Float(FloatWidth::F32) => "Float32".to_owned(),
        Dst::Float(FloatWidth::F64) => "Float64".to_owned(),
        Dst::Str => "String".to_owned(),
        Dst::FixedStr(n) => format!("FixedString({n})"),
        Dst::Ts(p) => format!("DateTime64({p})"),
        Dst::DateTime => "DateTime".to_owned(),
        Dst::Uuid => "UUID".to_owned(),
        Dst::Arr { elem, .. } => format!("Array({})", dst_label(elem)),
    }
}

/// Whether an Arrow field type may feed the resolved target. Lists feed
/// `Array` targets element by element (one level only).
fn compat(arrow: &DataType, dst: &Dst, col: &str) -> Result<(), String> {
    let ok = match (arrow, dst) {
        (DataType::List(item), Dst::Arr { elem, .. }) => {
            return compat(item.data_type(), elem, col);
        }
        (DataType::Boolean, Dst::Bool) => true,
        (DataType::Float32, Dst::Float(_)) => true,
        (DataType::Float64, Dst::Float(FloatWidth::F64)) => true,
        (DataType::Utf8 | DataType::Binary, Dst::Str | Dst::FixedStr(_)) => true,
        (DataType::Dictionary(key, value), Dst::Str | Dst::FixedStr(_)) => {
            matches!(key.as_ref(), DataType::Int8 | DataType::Int32)
                && matches!(value.as_ref(), DataType::Utf8)
        }
        (DataType::FixedSizeBinary(16), Dst::Uuid) => true,
        (DataType::FixedSizeBinary(_), Dst::Str | Dst::FixedStr(_)) => true,
        (DataType::Timestamp(_, _), Dst::Ts(_) | Dst::DateTime) => ts_nanos(arrow).is_some(),
        (_, Dst::Int(_) | Dst::UInt(_)) => {
            matches!(arrow_int_bits(arrow), Some((u, b)) if int_ok(u, b, dst))
        }
        _ => false,
    };
    if ok {
        return Ok(());
    }
    Err(format!(
        "column `{col}`: Arrow type {arrow} cannot feed ClickHouse type {}",
        dst_label(dst),
    ))
}

/// Validates one arrow field against one table column into a planned output
/// column.
fn plan_pair(field: &FieldRef, arrow_index: usize, tcol: &TableCol) -> Result<ColPlan, String> {
    if matches!(tcol.default_kind.as_str(), "MATERIALIZED" | "ALIAS") {
        return Err(format!(
            "column `{}` is {} and cannot be inserted into; map the dataset column to a plain \
             column or drop it from `columns`",
            tcol.name, tcol.default_kind,
        ));
    }
    let ty = tcol
        .ty
        .as_ref()
        .map_err(|e| format!("column `{}`: {e}", tcol.name))?;
    let nullable = ch_nullable(ty);
    let dst = resolve(ty, &tcol.name)?;
    let ty = field.data_type();
    let src_ts_nanos = match ty {
        DataType::List(item) => ts_nanos(item.data_type()),
        other => ts_nanos(other),
    };
    compat(ty, &dst, &tcol.name)?;
    // ClickHouse arrays cannot be NULL, so an optional list is accepted and a
    // null list is stored as `[]` (documented); every other optional column
    // needs a `Nullable` table column.
    if field.is_nullable() && !nullable && !matches!(dst, Dst::Arr { .. }) {
        return Err(format!(
            "column `{}` is not Nullable in ClickHouse but dataset column `{}` is nullable; \
             make the table column Nullable(..) or declare the dataset field `required: true`",
            tcol.name,
            field.name(),
        ));
    }
    Ok(ColPlan {
        table: tcol.name.clone(),
        arrow_index,
        nullable,
        dst,
        src_ts_nanos,
    })
}

/// Whether the Arrow field is a declared dataset field (as opposed to an
/// envelope or identity-level column), by its Parquet field-ID range.
fn is_declared(field: &FieldRef) -> bool {
    field_id(field)
        .is_some_and(|id| id >= 0 && (FIRST_DECLARED_ID..=LAST_DECLARED_ID).contains(&(id as u32)))
}

/// Builds the insert plan from the dataset's Arrow fields, the `DESCRIBE
/// TABLE` rows, and the explicit `columns` mapping (if any).
///
/// Projection rules (the no-DDL contract):
///
/// - With `columns` given, it is the complete mapping: every declared
///   dataset field must appear in it, every mapped pair must exist on both
///   sides, and envelope and level columns feed the table only when they are
///   mapped explicitly.
/// - Without it, every declared dataset field must exist in the table under
///   its own name; envelope and level columns feed the table where the table
///   carries a column of the same name, and are skipped otherwise.
/// - Table columns the dataset does not feed are left to their server-side
///   defaults and listed in [`InsertPlan::unmapped_table_cols`].
pub(crate) fn build_plan(
    fields: &[FieldRef],
    describe: &[TableCol],
    columns: Option<&BTreeMap<String, String>>,
) -> Result<InsertPlan, Vec<String>> {
    let mut errors: Vec<String> = Vec::new();
    let mut plan: Vec<ColPlan> = Vec::new();
    let mut used_table: BTreeSet<&str> = BTreeSet::new();

    let find_col = |name: &str| describe.iter().find(|c| c.name == name);
    let arrow_named = |name: &str| fields.iter().enumerate().find(|(_, f)| f.name() == name);

    match columns {
        Some(map) => {
            let mut mapped_arrow: BTreeSet<&str> = BTreeSet::new();
            for (ds_name, table_name) in map {
                let Some((arrow_index, field)) = arrow_named(ds_name) else {
                    errors.push(format!(
                        "`columns` maps dataset column `{ds_name}`, which the dataset does not \
                         have (typo? known columns: {})",
                        fields
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ));
                    continue;
                };
                let Some(tcol) = find_col(table_name) else {
                    errors.push(format!(
                        "`columns` maps `{ds_name}` to table column `{table_name}`, which the \
                         table does not have"
                    ));
                    continue;
                };
                if !mapped_arrow.insert(ds_name.as_str()) {
                    errors.push(format!("dataset column `{ds_name}` is mapped twice"));
                }
                if !used_table.insert(table_name.as_str()) {
                    errors.push(format!(
                        "table column `{table_name}` is the target of two mappings"
                    ));
                }
                match plan_pair(field, arrow_index, tcol) {
                    Ok(p) => plan.push(p),
                    Err(e) => errors.push(e),
                }
            }
            for field in fields {
                if is_declared(field) && !mapped_arrow.contains(field.name().as_str()) {
                    errors.push(format!(
                        "declared dataset field `{}` has no entry in `columns`; with an explicit \
                         mapping every data field must be mapped (envelope columns are optional)",
                        field.name(),
                    ));
                }
            }
        }
        None => {
            for (arrow_index, field) in fields.iter().enumerate() {
                match find_col(field.name()) {
                    Some(tcol) => {
                        used_table.insert(tcol.name.as_str());
                        match plan_pair(field, arrow_index, tcol) {
                            Ok(p) => plan.push(p),
                            Err(e) => errors.push(e),
                        }
                    }
                    None if is_declared(field) => errors.push(format!(
                        "declared dataset field `{}` has no column in the table; add it to the \
                         table or give an explicit `columns` mapping",
                        field.name(),
                    )),
                    None => {}
                }
            }
        }
    }

    plan.sort_by_key(|c| c.arrow_index);

    let unmapped_table_cols: Vec<String> = describe
        .iter()
        .filter(|c| !used_table.contains(c.name.as_str()))
        .map(|c| c.name.clone())
        .collect();

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(InsertPlan {
        cols: plan,
        unmapped_table_cols,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::testutil::{DATA_TABLE, ENVELOPE_TABLE, describe, full_table, schema};

    fn fields() -> Vec<FieldRef> {
        schema().arrow_schema().fields().iter().cloned().collect()
    }

    fn plan(table: &[TableCol]) -> Result<InsertPlan, Vec<String>> {
        build_plan(&fields(), table, None)
    }

    fn problems(table: &[TableCol]) -> String {
        plan(table).expect_err("a mismatch").join(" | ")
    }

    /// The data table with one column's type replaced.
    fn with_type(name: &str, ty: &str) -> Vec<TableCol> {
        let cols: Vec<(&str, &str)> = DATA_TABLE
            .iter()
            .map(|(n, t)| if *n == name { (*n, ty) } else { (*n, *t) })
            .collect();
        describe(&cols)
    }

    fn mapping(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn a_matching_table_yields_a_plan_in_arrow_order() {
        let p = plan(&describe(DATA_TABLE)).expect("matches");
        let names: Vec<&str> = p.cols.iter().map(|c| c.table.as_str()).collect();
        assert_eq!(names, ["temp", "note", "n", "counter", "samples", "seen"]);
        assert!(
            p.cols
                .windows(2)
                .all(|w| w[0].arrow_index < w[1].arrow_index)
        );
        let nullable: Vec<bool> = p.cols.iter().map(|c| c.nullable).collect();
        assert_eq!(nullable, [true, true, false, false, false, false]);
        assert!(p.unmapped_table_cols.is_empty());
        assert_eq!(p.cols[3].dst, Dst::UInt(UIntWidth::U32));
        assert_eq!(p.cols[5].dst, Dst::Ts(6));
        assert!(matches!(
            &p.cols[4].dst,
            Dst::Arr { elem_nullable: false, elem } if **elem == Dst::Float(FloatWidth::F32)
        ));
    }

    #[test]
    fn envelope_and_level_columns_feed_only_where_the_table_has_them() {
        let bare = plan(&describe(DATA_TABLE)).expect("matches");
        assert_eq!(bare.cols.len(), 6, "no envelope column, none inserted");

        let full = plan(&full_table()).expect("envelope table matches");
        assert_eq!(full.cols.len(), ENVELOPE_TABLE.len() + DATA_TABLE.len());
        let by_name = |n: &str| full.cols.iter().find(|c| c.table == n).unwrap();
        assert_eq!(by_name("_event_id").dst, Dst::Uuid);
        assert_eq!(
            by_name("_time_domain").dst,
            Dst::Str,
            "LowCardinality is erased"
        );
        assert!(
            by_name("robot").nullable,
            "LowCardinality(Nullable(..)) is nullable"
        );
        assert_eq!(by_name("_wal_pos").dst, Dst::UInt(UIntWidth::U64));
        assert_eq!(by_name("_clock_synced").dst, Dst::Bool);
    }

    #[test]
    fn extra_table_columns_are_left_to_their_defaults() {
        let mut cols: Vec<(&str, &str)> = DATA_TABLE.to_vec();
        cols.push(("inserted_at", "DateTime DEFAULT now()"));
        cols.push(("tag", "String"));
        let p = plan(&describe(&cols)).expect("extras are fine");
        assert_eq!(p.unmapped_table_cols.len(), 2);
        assert!(p.unmapped_table_cols.contains(&"tag".to_owned()));
    }

    #[test]
    fn a_declared_field_without_a_column_is_reported() {
        let cols: Vec<(&str, &str)> = DATA_TABLE
            .iter()
            .filter(|(n, _)| *n != "n")
            .copied()
            .collect();
        let text = problems(&describe(&cols));
        assert!(
            text.contains("declared dataset field `n` has no column"),
            "{text}"
        );
    }

    #[test]
    fn type_mismatches_name_both_types() {
        let text = problems(&with_type("temp", "Nullable(String)"));
        assert!(text.contains("Float64"), "{text}");
        assert!(text.contains("String"), "{text}");

        // Narrowing is never silent: Int64 into Int32 and UInt32 into UInt16.
        assert!(problems(&with_type("n", "Int32")).contains("`n`"));
        assert!(problems(&with_type("counter", "UInt16")).contains("`counter`"));
        // Widening is.
        assert!(plan(&with_type("counter", "UInt64")).is_ok());
        assert!(plan(&with_type("counter", "Int64")).is_ok());
        assert!(plan(&with_type("temp", "Nullable(Float64)")).is_ok());
        // Signed into unsigned is not.
        assert!(plan(&with_type("n", "UInt64")).is_err());
        // Float64 cannot be squeezed into Float32.
        assert!(plan(&with_type("temp", "Nullable(Float32)")).is_err());
    }

    #[test]
    fn nullable_dataset_fields_need_nullable_columns() {
        let text = problems(&with_type("note", "String"));
        assert!(text.contains("is not Nullable in ClickHouse"), "{text}");
        assert!(text.contains("`note`"), "{text}");
    }

    #[test]
    fn lists_map_to_arrays_and_null_lists_are_tolerated() {
        // `samples` is optional in the dataset and the Array column is not
        // nullable: accepted, a null list is stored as `[]`.
        assert!(plan(&with_type("samples", "Array(Float32)")).is_ok());
        let p = plan(&with_type("samples", "Array(Nullable(Float32))")).unwrap();
        assert!(matches!(
            &p.cols[4].dst,
            Dst::Arr {
                elem_nullable: true,
                ..
            }
        ));
        // Float32 elements into Float64 arrays are fine.
        assert!(plan(&with_type("samples", "Array(Float64)")).is_ok());

        let text = problems(&with_type("samples", "Array(Array(Float32))"));
        assert!(text.contains("nested Array"), "{text}");
        assert!(problems(&with_type("samples", "Float32")).contains("`samples`"));
        assert!(problems(&with_type("samples", "Array(String)")).contains("`samples`"));
    }

    #[test]
    fn fixed_strings_keep_their_length_for_the_encoder() {
        let p = plan(&with_type("note", "Nullable(FixedString(3))")).unwrap();
        assert_eq!(p.cols[1].dst, Dst::FixedStr(3));
        assert!(p.cols[1].nullable);
        let p = plan(&with_type("samples", "Array(FixedString(4))"));
        assert!(p.is_err(), "Float32 elements cannot feed FixedString");
    }

    #[test]
    fn unsupported_table_types_are_explained() {
        let text = problems(&with_type("note", "Tuple(String, String)"));
        assert!(text.contains("cannot encode"), "{text}");
        let text = problems(&with_type("temp", "Decimal(18, 4)"));
        assert!(text.contains("Decimal"), "{text}");
    }

    #[test]
    fn timestamps_map_to_datetime_precisions() {
        for (ty, dst) in [
            ("DateTime64(3)", Dst::Ts(3)),
            ("DateTime64(9, 'UTC')", Dst::Ts(9)),
            ("DateTime64(0)", Dst::Ts(0)),
            ("DateTime", Dst::DateTime),
            ("DateTime('Europe/Berlin')", Dst::DateTime),
        ] {
            let p = plan(&with_type("seen", ty)).unwrap();
            assert_eq!(p.cols[5].dst, dst, "{ty}");
        }
        assert!(plan(&with_type("seen", "Date")).is_err());
    }

    #[test]
    fn an_unparsable_type_only_matters_when_it_is_mapped() {
        let mut cols: Vec<(&str, &str)> = DATA_TABLE.to_vec();
        cols.push(("blob", "SomeFutureType(1, 2)"));
        let p = plan(&describe(&cols)).expect("unmapped columns are never parsed for meaning");
        assert!(p.unmapped_table_cols.contains(&"blob".to_owned()));

        let text = problems(&with_type("note", "SomeFutureType(1, 2)"));
        assert!(text.contains("unparsable ClickHouse type"), "{text}");
    }

    #[test]
    fn materialized_and_alias_columns_cannot_be_targets() {
        let mut table = describe(DATA_TABLE);
        table[2].default_kind = "MATERIALIZED".to_owned();
        let text = plan(&table).expect_err("materialized").join(" | ");
        assert!(text.contains("MATERIALIZED"), "{text}");
        table[2].default_kind = "DEFAULT".to_owned();
        assert!(plan(&table).is_ok(), "a DEFAULT column is insertable");
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let cols: Vec<(&str, &str)> = vec![
            ("temp", "String"),
            ("note", "String"),
            ("counter", "UInt8"),
            ("samples", "Array(Float32)"),
            ("seen", "DateTime64(6)"),
        ];
        let errs = plan(&describe(&cols)).expect_err("several");
        assert!(errs.len() >= 4, "{errs:?}");
    }

    #[test]
    fn the_explicit_mapping_is_the_complete_projection() {
        let table = describe(&[
            ("temperature", "Nullable(Float64)"),
            ("remark", "Nullable(String)"),
            ("n", "Int64"),
            ("cnt", "UInt32"),
            ("samples", "Array(Float32)"),
            ("seen", "DateTime64(6, 'UTC')"),
            ("event_id", "UUID"),
        ]);
        let map = mapping(&[
            ("temp", "temperature"),
            ("note", "remark"),
            ("n", "n"),
            ("counter", "cnt"),
            ("samples", "samples"),
            ("seen", "seen"),
            ("_event_id", "event_id"),
        ]);
        let p = build_plan(&fields(), &table, Some(&map)).expect("maps");
        assert_eq!(p.cols.len(), 7);
        let by_table = |t: &str| p.cols.iter().find(|c| c.table == t).unwrap();
        assert_eq!(by_table("event_id").dst, Dst::Uuid);
        assert_eq!(by_table("cnt").dst, Dst::UInt(UIntWidth::U32));
        assert!(p.unmapped_table_cols.is_empty());

        // Dropping an entry for a declared field is an error; envelope
        // entries stay optional.
        let mut short = map.clone();
        short.remove("counter");
        short.remove("_event_id");
        let errs = build_plan(&fields(), &table, Some(&short)).expect_err("incomplete");
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0].contains("`counter` has no entry in `columns`"),
            "{errs:?}"
        );
    }

    #[test]
    fn mapping_typos_and_collisions_are_reported() {
        let table = describe(DATA_TABLE);
        let mut map = mapping(&[
            ("temp", "temp"),
            ("note", "note"),
            ("n", "n"),
            ("counter", "counter"),
            ("samples", "samples"),
            ("seen", "seen"),
        ]);
        map.insert("tmep".to_owned(), "temp".to_owned());
        map.insert("note".to_owned(), "ghost".to_owned());
        map.insert("seen".to_owned(), "n".to_owned());
        let text = build_plan(&fields(), &table, Some(&map))
            .expect_err("bad mapping")
            .join(" | ");
        assert!(text.contains("`tmep`"), "{text}");
        assert!(text.contains("typo? known columns"), "{text}");
        assert!(text.contains("`ghost`"), "{text}");
        assert!(text.contains("target of two mappings"), "{text}");
    }

    #[test]
    fn integer_widening_rules() {
        let sig = |bits| {
            Dst::Int(match bits {
                8 => IntWidth::I8,
                16 => IntWidth::I16,
                32 => IntWidth::I32,
                _ => IntWidth::I64,
            })
        };
        assert!(int_ok(false, 8, &sig(8)));
        assert!(!int_ok(false, 16, &sig(8)));
        assert!(int_ok(true, 8, &sig(16)));
        assert!(!int_ok(true, 16, &sig(16)), "u16 needs more than Int16");
        assert!(int_ok(true, 32, &sig(64)));
        // u64 into Int64 passes the plan; large values fail at insert.
        assert!(int_ok(true, 64, &sig(64)));
        assert!(!int_ok(true, 8, &sig(8)));
        assert!(int_ok(true, 16, &Dst::UInt(UIntWidth::U16)));
        assert!(!int_ok(true, 32, &Dst::UInt(UIntWidth::U16)));
    }
}
