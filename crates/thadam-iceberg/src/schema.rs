//! Segment footer → Iceberg schema, and field-ID-driven schema evolution.
//!
//! The Parquet footer is the single source of truth: `thadam-arrow` writes
//! the Arrow schema with `PARQUET:field_id` metadata into it, so a ranged
//! tail read plus the conversions inside the `iceberg` crate's Arrow-58
//! tree (`parquet` 58 footer → Arrow schema →
//! [`iceberg::arrow::arrow_schema_to_schema`]) rebuild the Iceberg schema
//! without any hand-maintained copy of the envelope/declared mapping. No
//! Arrow types cross the crate boundary: the workspace builds Arrow 60 while
//! `iceberg` builds Arrow 58, so the conversion can only live here.
//!
//! [`schema_evolution`] merges one segment's footer schema into the table's
//! current schema by field ID (step 2; docs/design/iceberg-append.md):
//!
//! - a file field ID the table does not have is **added** as an optional
//!   column (a required new file column lands optional: evolution never
//!   introduces required columns),
//! - the same ID with a different name is a **rename** (the file's spelling
//!   wins; IDs are the identity, ADR-0013),
//! - the same ID with a wider type is an Iceberg **promotion** (`int`→`long`,
//!   `float`→`double`, decimal precision widening at the same scale),
//! - table fields the file dropped simply **stay** in the table.
//!
//! Anything else — narrowing or an incompatible type on a reused ID, a
//! required-ness change in either direction, a required table column the
//! file lacks, list/map child-ID drift — returns every reason as a fatal
//! error; the table is never touched.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iceberg::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, StructType, Type,
};

use crate::error::IcebergError;
use crate::storage::S3Io;

/// Hard cap on a Parquet footer read (64 MiB), well above any segment footer
/// Thadam writes, so a corrupt 4-byte tail length is rejected before the range
/// read.
const MAX_FOOTER_BYTES: u64 = 64 * 1024 * 1024;

/// Reads one segment's Parquet footer through the table's storage (two
/// ranged GETs: the 8-byte tail, then the footer) and converts the embedded
/// Arrow schema to an Iceberg schema. Conversion failures (for example
/// `UInt64`, which the `unsigned: native` envelope writes for `_wal_epoch`
/// and `_wal_pos`) are [`IcebergError::UnsupportedType`] with the remedy in
/// the message.
pub(crate) async fn iceberg_schema_of(
    io: &S3Io,
    uri: &str,
) -> Result<iceberg::spec::Schema, IcebergError> {
    let size = io.size_of(uri).await?;
    if size < 8 {
        return Err(IcebergError::Internal(format!(
            "{uri}: too small to be a Parquet file ({size} bytes)"
        )));
    }
    let mut tail = [0u8; 8];
    tail.copy_from_slice(&io.read_range(uri, (size - 8)..size).await?);
    // Parquet tail: 4-byte little-endian footer length, then the "PAR1" magic.
    if &tail[4..] != b"PAR1" {
        return Err(footer_error(uri, "bad Parquet magic in the 8-byte tail"));
    }
    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap_or([0; 4])) as u64;
    if footer_len > MAX_FOOTER_BYTES {
        return Err(footer_error(
            uri,
            format!(
                "Parquet footer length ({footer_len} bytes) exceeds the {MAX_FOOTER_BYTES}-byte cap"
            ),
        ));
    }

    let start = size.saturating_sub(8 + footer_len);
    let body = io.read_range(uri, start..(size - 8)).await?;
    let body = if body.len() as u64 > footer_len {
        &body[(body.len() - footer_len as usize)..]
    } else {
        &body[..]
    };
    let meta = parquet58::file::metadata::ParquetMetaDataReader::decode_metadata(body)
        .map_err(|e| footer_error(uri, e))?;
    let arrow_schema = parquet58::arrow::parquet_to_arrow_schema(
        meta.file_metadata().schema_descr(),
        meta.file_metadata().key_value_metadata(),
    )
    .map_err(|e| footer_error(uri, e))?;
    iceberg::arrow::arrow_schema_to_schema(&arrow_schema)
        .map_err(|e| IcebergError::UnsupportedType(e.to_string()))
}

fn footer_error(uri: &str, e: impl std::fmt::Display) -> IcebergError {
    // A footer that does not decode contradicts the shipper's checksum
    // verification of the same bytes: stop loudly, never guess a schema.
    IcebergError::Internal(format!("{uri}: Parquet footer does not decode: {e}"))
}

/// The schema evolution (if any) the table needs before a segment with the
/// footer schema `file` can be appended.
///
/// - `Ok(None)`: the file is already covered — every file field ID exists in
///   the table with an equal or wider type and nothing renamed.
/// - `Ok(Some(merged))`: commit `merged` as the new current schema (adds,
///   renames, and/or promotions), then append.
/// - `Err(reasons)`: the file can never be registered — every rule violated,
///   listed. The table is untouched; the caller fails fatally.
///
/// The merge is idempotent once committed (re-deriving it from the merged
/// table schema yields `None`), which is what makes a crash between the
/// schema commit and the append commit safe to retry.
pub(crate) fn schema_evolution(
    table: &iceberg::spec::Schema,
    file: &iceberg::spec::Schema,
) -> Result<Option<iceberg::spec::Schema>, Vec<String>> {
    let mut errors = Vec::new();
    let (fields, changed) = merge_fields(
        table.as_struct().fields(),
        file.as_struct().fields(),
        "",
        &mut errors,
    );
    if !errors.is_empty() {
        return Err(errors);
    }
    if !changed {
        return Ok(None);
    }
    let merged = iceberg::spec::Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.identifier_field_ids())
        .build()
        .map_err(|e| vec![format!("merged schema failed validation: {e}")])?;
    Ok(Some(merged))
}

/// Table fields, merged with `file`'s same-level fields by ID: existing IDs
/// merge (rename/promote), file-only IDs append as optional, table-only IDs
/// stay. Every incompatibility is recorded in `errors`.
fn merge_fields(
    table: &[NestedFieldRef],
    file: &[NestedFieldRef],
    path: &str,
    errors: &mut Vec<String>,
) -> (Vec<NestedFieldRef>, bool) {
    let file_by_id: HashMap<i32, &NestedFieldRef> = file.iter().map(|f| (f.id, f)).collect();
    let mut used = HashSet::new();
    let mut changed = false;
    let mut out: Vec<NestedFieldRef> = Vec::with_capacity(table.len() + file.len());
    for tf in table {
        let path = join(path, &tf.name);
        let Some(ff) = file_by_id.get(&tf.id).copied() else {
            // Dropped from the dataset: the column stays. A required one the
            // file cannot fill would null into it — fatal, like step 1.
            if tf.required {
                errors.push(format!(
                    "table column `{path}` (id {}) is required but the segment file does not have it",
                    tf.id
                ));
            }
            out.push(Arc::clone(tf));
            continue;
        };
        used.insert(tf.id);
        let (merged, ch) = merge_field(tf, ff, &path, errors);
        changed |= ch;
        out.push(merged);
    }
    for ff in file {
        if !used.contains(&ff.id) {
            // New field ID: added optional (evolution never introduces a
            // required column; old files have no value for it).
            changed = true;
            out.push(as_optional(ff));
        }
    }
    (out, changed)
}

/// One field present by ID on both sides: rename to the file's spelling and
/// promote toward the wider type. Required-ness must already agree.
fn merge_field(
    tf: &NestedField,
    ff: &NestedField,
    path: &str,
    errors: &mut Vec<String>,
) -> (NestedFieldRef, bool) {
    if tf.required != ff.required {
        let (table, file) = if tf.required {
            ("required", "optional")
        } else {
            ("optional", "required")
        };
        errors.push(format!(
            "column `{path}` (field ID {}) is {table} in the table but {file} in the segment file; changing required-ness is fatal",
            tf.id
        ));
    }
    let (field_type, type_changed) =
        merge_type(&tf.field_type, &ff.field_type, (tf.id, path), errors);
    let renamed = tf.name != ff.name;
    if !type_changed && !renamed {
        return (Arc::new(clone_shallow(tf)), false);
    }
    let mut merged = clone_shallow(tf);
    merged.name.clone_from(&ff.name);
    merged.field_type = Box::new(field_type);
    (Arc::new(merged), true)
}

fn merge_type(
    table: &Type,
    file: &Type,
    (id, path): (i32, &str),
    errors: &mut Vec<String>,
) -> (Type, bool) {
    if table == file {
        return (table.clone(), false);
    }
    match (table, file) {
        (Type::Primitive(t), Type::Primitive(f)) => {
            if let Some(widened) = promotion(t, f) {
                return (Type::Primitive(widened), true);
            }
            errors.push(format!(
                "column `{path}` (field ID {id}): the segment file has type `{f}` but the table has `{t}`; only int→long, float→double and wider decimal precision (same scale) are allowed, so narrowing and any other change is fatal"
            ));
            (table.clone(), false)
        }
        (Type::Struct(t), Type::Struct(f)) => {
            let (fields, ch) = merge_fields(t.fields(), f.fields(), path, errors);
            if ch {
                (Type::Struct(StructType::new(fields)), true)
            } else {
                (table.clone(), false)
            }
        }
        (Type::List(t), Type::List(f)) => {
            if t.element_field.id != f.element_field.id {
                errors.push(format!(
                    "list `{path}` has element id {} in the table but {} in the file",
                    t.element_field.id, f.element_field.id
                ));
                return (table.clone(), false);
            }
            let elem_path = join(path, "element");
            let (element, ch) = merge_field(&t.element_field, &f.element_field, &elem_path, errors);
            if ch {
                (
                    Type::List(ListType {
                        element_field: element,
                    }),
                    true,
                )
            } else {
                (table.clone(), false)
            }
        }
        (Type::Map(t), Type::Map(f)) => {
            let mut ch = false;
            let mut merge_side = |kind: &str, tf: &NestedFieldRef, ff: &NestedFieldRef| {
                if tf.id != ff.id {
                    errors.push(format!(
                        "map `{path}` has {kind} id {} in the table but {} in the file",
                        tf.id, ff.id
                    ));
                    return Arc::clone(tf);
                }
                let side_path = join(path, kind);
                let (merged, changed) = merge_field(tf, ff, &side_path, errors);
                ch |= changed;
                merged
            };
            let key_field = merge_side("key", &t.key_field, &f.key_field);
            let value_field = merge_side("value", &t.value_field, &f.value_field);
            if ch {
                (
                    Type::Map(MapType {
                        key_field,
                        value_field,
                    }),
                    true,
                )
            } else {
                (table.clone(), false)
            }
        }
        (t, f) => {
            errors.push(format!(
                "column `{path}` (field ID {id}) changed kind (segment file `{f}`, table `{t}`); reusing a field ID with a different type is fatal"
            ));
            (table.clone(), false)
        }
    }
}

/// The type the table column becomes when `file` is wider along Iceberg's
/// promotion lattice; `None` for anything else (including narrowing).
fn promotion(table: &PrimitiveType, file: &PrimitiveType) -> Option<PrimitiveType> {
    match (table, file) {
        (PrimitiveType::Int, PrimitiveType::Long) => Some(PrimitiveType::Long),
        (PrimitiveType::Float, PrimitiveType::Double) => Some(PrimitiveType::Double),
        (
            PrimitiveType::Decimal {
                precision: p1,
                scale: s1,
            },
            PrimitiveType::Decimal {
                precision: p2,
                scale: s2,
            },
        ) if s1 == s2 && p2 > p1 => Some(PrimitiveType::Decimal {
            precision: *p2,
            scale: *s1,
        }),
        _ => None,
    }
}

/// A new table column is always optional: older files carry nothing for it,
/// so it can never be committed as required.
fn as_optional(f: &NestedFieldRef) -> NestedFieldRef {
    if f.required {
        let mut f = clone_shallow(f);
        f.required = false;
        Arc::new(f)
    } else {
        Arc::clone(f)
    }
}

fn clone_shallow(f: &NestedField) -> NestedField {
    NestedField {
        id: f.id,
        name: f.name.clone(),
        required: f.required,
        field_type: f.field_type.clone(),
        doc: f.doc.clone(),
        initial_default: f.initial_default.clone(),
        write_default: f.write_default.clone(),
    }
}

fn join(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_owned()
    } else {
        format!("{path}.{name}")
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, Type};

    use super::*;

    fn schema(fields: Vec<NestedField>) -> Schema {
        let fields: Vec<NestedFieldRef> = fields.into_iter().map(Arc::new).collect();
        Schema::builder()
            .with_fields(fields)
            .build()
            .expect("schema")
    }

    fn st(fields: Vec<NestedField>) -> Type {
        let fields: Vec<NestedFieldRef> = fields.into_iter().map(Arc::new).collect();
        Type::Struct(StructType::new(fields))
    }

    fn list(element_id: i32, ty: PrimitiveType) -> Type {
        Type::List(ListType::new(Arc::new(NestedField::optional(
            element_id,
            "element",
            Type::Primitive(ty),
        ))))
    }

    fn field<'a>(s: &'a Schema, name: &str) -> &'a NestedField {
        s.as_struct()
            .fields()
            .iter()
            .find(|f| f.name == name)
            .map(Arc::as_ref)
            .unwrap_or_else(|| panic!("no column {name}"))
    }

    #[test]
    fn identical_schemas_need_no_evolution() {
        let s = schema(vec![
            NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::required(10, "wal_epoch", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(1000, "temp", Type::Primitive(PrimitiveType::Float)),
        ]);
        assert_eq!(schema_evolution(&s, &s), Ok(None));
    }

    #[test]
    fn new_columns_are_added_as_optional_with_their_own_ids() {
        let table = schema(vec![NestedField::required(
            2,
            "ts",
            Type::Primitive(PrimitiveType::Timestamp),
        )]);
        let file = schema(vec![
            NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(1000, "temp", Type::Primitive(PrimitiveType::Float)),
            NestedField::required(1001, "speed", Type::Primitive(PrimitiveType::Long)),
        ]);
        let merged = schema_evolution(&table, &file)
            .expect("no errors")
            .expect("an evolution");
        let temp = field(&merged, "temp");
        assert_eq!(temp.id, 1000, "the file's own ID, never reassigned");
        assert!(!temp.required);
        assert_eq!(*temp.field_type, Type::Primitive(PrimitiveType::Float));
        let speed = field(&merged, "speed");
        assert!(!speed.required, "required file columns land optional");
        assert_eq!(speed.id, 1001);
        assert_eq!(merged.highest_field_id(), 1001, "last-column-id material");
    }

    #[test]
    fn same_id_with_a_new_name_is_a_rename() {
        let table = schema(vec![NestedField::optional(
            1000,
            "temp",
            Type::Primitive(PrimitiveType::Float),
        )]);
        let file = schema(vec![NestedField::optional(
            1000,
            "motor_temp",
            Type::Primitive(PrimitiveType::Float),
        )]);
        let merged = schema_evolution(&table, &file)
            .expect("no errors")
            .expect("an evolution");
        assert_eq!(merged.as_struct().fields().len(), 1);
        assert_eq!(merged.as_struct().fields()[0].name, "motor_temp");
        assert_eq!(merged.as_struct().fields()[0].id, 1000);
        assert_eq!(schema_evolution(&merged, &file), Ok(None), "idempotent");
    }

    #[test]
    fn int_long_and_float_double_promotions() {
        let table = schema(vec![NestedField::required(
            1,
            "n",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let file = schema(vec![NestedField::required(
            1,
            "n",
            Type::Primitive(PrimitiveType::Long),
        )]);
        let merged = schema_evolution(&table, &file)
            .expect("int→long")
            .expect("an evolution");
        assert_eq!(
            *field(&merged, "n").field_type,
            Type::Primitive(PrimitiveType::Long),
            "the table widens to cover the file"
        );

        let table = schema(vec![NestedField::optional(
            1,
            "n",
            Type::Primitive(PrimitiveType::Float),
        )]);
        let file = schema(vec![NestedField::optional(
            1,
            "n",
            Type::Primitive(PrimitiveType::Double),
        )]);
        let merged = schema_evolution(&table, &file)
            .expect("float→double")
            .expect("an evolution");
        assert_eq!(
            *field(&merged, "n").field_type,
            Type::Primitive(PrimitiveType::Double)
        );
    }

    #[test]
    fn narrowing_is_fatal() {
        let table = schema(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Double)),
        ]);
        let file = schema(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Float)),
        ]);
        let errors = schema_evolution(&table, &file).unwrap_err();
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors.iter().all(|m| m.contains("narrowing")), "{errors:?}");
    }

    #[test]
    fn decimal_precision_widens_at_the_same_scale() {
        let decimal = |precision| {
            Type::Primitive(PrimitiveType::Decimal {
                precision,
                scale: 2,
            })
        };
        let table = schema(vec![NestedField::optional(1, "d", decimal(10))]);
        let file = schema(vec![NestedField::optional(1, "d", decimal(18))]);
        let merged = schema_evolution(&table, &file)
            .expect("decimal widen")
            .expect("an evolution");
        assert_eq!(*field(&merged, "d").field_type, decimal(18));
        let narrower = schema(vec![NestedField::optional(1, "d", decimal(6))]);
        let errors = schema_evolution(&merged, &narrower).unwrap_err();
        assert_eq!(errors.len(), 1, "precision narrowing is fatal: {errors:?}");
        let different_scale = schema(vec![NestedField::optional(
            1,
            "d",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 18,
                scale: 4,
            }),
        )]);
        let errors = schema_evolution(&merged, &different_scale).unwrap_err();
        assert_eq!(errors.len(), 1, "{errors:?}");
    }

    #[test]
    fn dropped_fields_stay_in_the_table() {
        let table = schema(vec![
            NestedField::optional(1, "old", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(2, "kept", Type::Primitive(PrimitiveType::Long)),
        ]);
        let file = schema(vec![NestedField::optional(
            2,
            "kept",
            Type::Primitive(PrimitiveType::Long),
        )]);
        assert_eq!(schema_evolution(&table, &file), Ok(None));
    }

    #[test]
    fn incompatible_changes_are_fatal_and_every_reason_is_listed() {
        let table = schema(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::String)),
        ]);
        let file = schema(vec![
            // id 1: required table column arrives optional (tightening).
            NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int)),
            // id 2: ID reuse with a different type (Int → String is never allowed).
            NestedField::required(2, "b", Type::Primitive(PrimitiveType::String)),
            // id 3: String → Int is never allowed either; all reasons surface together.
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int)),
        ]);
        let errors = schema_evolution(&table, &file).unwrap_err();
        assert_eq!(errors.len(), 3, "{errors:?}");
        assert!(
            errors.iter().any(|m| m.contains("changing required-ness")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|m| m.contains("field ID 2")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|m| m.contains("field ID 3")),
            "{errors:?}"
        );
    }

    #[test]
    fn required_ness_changes_are_fatal_in_both_directions() {
        let optional = schema(vec![NestedField::optional(
            1,
            "a",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let required = schema(vec![NestedField::required(
            1,
            "a",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let tightened = schema_evolution(&optional, &required).unwrap_err();
        assert!(
            tightened[0].contains("optional in the table but required"),
            "{tightened:?}"
        );
        let loosened = schema_evolution(&required, &optional).unwrap_err();
        assert!(
            loosened[0].contains("required in the table but optional"),
            "{loosened:?}"
        );
    }

    #[test]
    fn a_required_table_column_missing_from_the_file_is_fatal() {
        let table = schema(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)),
        ]);
        let file = schema(vec![NestedField::optional(
            2,
            "b",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let errors = schema_evolution(&table, &file).unwrap_err();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("does not have it"), "{errors:?}");
    }

    #[test]
    fn a_new_id_reusing_an_existing_name_is_fatal() {
        let table = schema(vec![NestedField::optional(
            1,
            "speed",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let file = schema(vec![NestedField::optional(
            2,
            "speed",
            Type::Primitive(PrimitiveType::Int),
        )]);
        let errors = schema_evolution(&table, &file).unwrap_err();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("merged schema"), "{errors:?}");
    }

    #[test]
    fn struct_children_merge_by_id() {
        let table = schema(vec![NestedField::optional(
            1000,
            "rec",
            st(vec![NestedField::optional(
                8_388_608,
                "x",
                Type::Primitive(PrimitiveType::Int),
            )]),
        )]);
        let file = schema(vec![NestedField::optional(
            1000,
            "rec",
            st(vec![
                NestedField::optional(8_388_608, "x", Type::Primitive(PrimitiveType::Long)),
                NestedField::optional(8_388_609, "y", Type::Primitive(PrimitiveType::Double)),
            ]),
        )]);
        let merged = schema_evolution(&table, &file)
            .expect("no errors")
            .expect("an evolution");
        let Type::Struct(rec) = field(&merged, "rec").field_type.as_ref() else {
            panic!("rec stays a struct");
        };
        let x = rec.field_by_id(8_388_608).expect("x");
        let y = rec.field_by_id(8_388_609).expect("y added with its own ID");
        assert_eq!(*x.field_type, Type::Primitive(PrimitiveType::Long));
        assert!(!y.required);
    }

    #[test]
    fn struct_child_id_reuse_with_a_new_type_is_fatal_with_path() {
        let table = schema(vec![NestedField::optional(
            1000,
            "rec",
            st(vec![NestedField::optional(
                8_388_608,
                "x",
                Type::Primitive(PrimitiveType::Int),
            )]),
        )]);
        let file = schema(vec![NestedField::optional(
            1000,
            "rec",
            st(vec![NestedField::optional(
                8_388_608,
                "x",
                Type::Primitive(PrimitiveType::String),
            )]),
        )]);
        let errors = schema_evolution(&table, &file).unwrap_err();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("rec.x"), "{errors:?}");
    }

    #[test]
    fn list_element_ids_must_match() {
        let table = schema(vec![NestedField::optional(
            1000,
            "vals",
            list(8_388_608, PrimitiveType::Int),
        )]);
        let file = schema(vec![NestedField::optional(
            1000,
            "vals",
            list(8_388_609, PrimitiveType::Int),
        )]);
        let errors = schema_evolution(&table, &file).unwrap_err();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("element id"), "{errors:?}");
    }

    #[test]
    fn list_element_types_promote() {
        let table = schema(vec![NestedField::optional(
            1000,
            "vals",
            list(8_388_608, PrimitiveType::Int),
        )]);
        let file = schema(vec![NestedField::optional(
            1000,
            "vals",
            list(8_388_608, PrimitiveType::Long),
        )]);
        let merged = schema_evolution(&table, &file)
            .expect("no errors")
            .expect("an evolution");
        let Type::List(l) = field(&merged, "vals").field_type.as_ref() else {
            panic!("vals stays a list");
        };
        assert_eq!(
            *l.element_field.field_type,
            Type::Primitive(PrimitiveType::Long)
        );
    }

    #[tokio::test]
    async fn oversized_parquet_footer_len_is_rejected_before_range_read() {
        use bytes::Bytes;
        use iceberg::io::Storage as _;

        let store = Arc::new(object_store::memory::InMemory::new());
        let io = S3Io::new(store, "warehouse");
        let uri = io.uri_of("lake/seg.parquet");
        let over_cap = (MAX_FOOTER_BYTES as u32) + 1;
        let mut fake = Vec::with_capacity(16);
        fake.extend_from_slice(b"PAR1____");
        fake.extend_from_slice(&over_cap.to_le_bytes());
        fake.extend_from_slice(b"PAR1");
        io.write(&uri, Bytes::from(fake)).await.expect("write");

        let err = iceberg_schema_of(&io, &uri)
            .await
            .expect_err("oversized footer_len must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("exceeds"), "{msg}");
        assert!(msg.contains(&MAX_FOOTER_BYTES.to_string()), "{msg}");
    }
}
