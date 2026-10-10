//! The dynamic Native-block writer: an Arrow `RecordBatch` becomes a
//! `clickhouse::native::Block`, the client crate's only dynamic-schema
//! insertion path.
//!
//! Every column is written through the public `Encode` trait. Scalar targets
//! use the crate's own primitive implementations; three small wrappers cover
//! what it lacks: raw-byte `String` ([`ByteString`]), `UUID` from 16 bytes
//! ([`UuidBytes`]), and `DateTime64` ticks ([`Ts64`]).
//!
//! The block's column types come from those wrappers, not from the table; the
//! server converts them to the table's declared types on insert. The plan
//! picked each wrapper from the table's own type, so the conversion is the
//! identity except for `LowCardinality` (lossless), `DateTime64(0)` into
//! `DateTime`, and `String` into `FixedString(n)`. The server turns a value that
//! does not convert into NULL in a nullable column instead of failing, so the
//! last two are range- and length-checked here, where a bad value becomes a
//! data error rather than silent data loss.

use std::error::Error as StdError;

use arrow_array::RecordBatch;
use clickhouse::native::builder::BlockBuilder;
use clickhouse::native::encode::{Encode, ValueWriter};
use clickhouse::native::{Block, DataTypeNode};
use clickhouse_types::data_types::DateTimePrecision;

use crate::norm::{Leaves, Lists, Norm, NormError};
use crate::types::{ColPlan, Dst, FloatWidth, InsertPlan, IntWidth, UIntWidth};

/// A `String` value holding raw bytes. ClickHouse `String` is a byte string,
/// so binary payloads and broken UTF-8 are legal.
struct ByteString(Box<[u8]>);

impl Encode for ByteString {
    fn produces() -> DataTypeNode {
        DataTypeNode::String
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        writer.write_string(&self.0)?;
        Ok(())
    }
}

/// A `UUID` value from the 16 canonical (big-endian) bytes. The Native layout
/// is the two 64-bit halves of the UUID, each little-endian.
struct UuidBytes([u8; 16]);

impl Encode for UuidBytes {
    fn produces() -> DataTypeNode {
        DataTypeNode::UUID
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let mut wire = self.0;
        wire[..8].reverse();
        wire[8..].reverse();
        writer.write_fixed(&wire)?;
        Ok(())
    }
}

/// A `DateTime64(P, 'UTC')` value: signed ticks at precision `P`.
struct Ts64<const P: u8>(i64);

fn precision(p: u8) -> DateTimePrecision {
    match p {
        0 => DateTimePrecision::Precision0,
        1 => DateTimePrecision::Precision1,
        2 => DateTimePrecision::Precision2,
        3 => DateTimePrecision::Precision3,
        4 => DateTimePrecision::Precision4,
        5 => DateTimePrecision::Precision5,
        6 => DateTimePrecision::Precision6,
        7 => DateTimePrecision::Precision7,
        8 => DateTimePrecision::Precision8,
        _ => DateTimePrecision::Precision9,
    }
}

impl<const P: u8> Encode for Ts64<P> {
    fn produces() -> DataTypeNode {
        DataTypeNode::DateTime64(precision(P), Some("UTC".to_owned()))
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        writer.write_fixed(&self.0.to_le_bytes())?;
        Ok(())
    }
}

/// Why the batch could not become a block. The data errors ([`Range`],
/// [`NullNotAllowed`], [`TsOverflow`], [`UuidLength`]) mean rows that do not
/// fit their table columns; [`Shape`] means the open-time plan and the batch
/// disagree. All of them are Permanent for the sink: retrying cannot help, so
/// the runtime bisects and quarantines the offending events.
///
/// [`Range`]: EncodeError::Range
/// [`NullNotAllowed`]: EncodeError::NullNotAllowed
/// [`TsOverflow`]: EncodeError::TsOverflow
/// [`UuidLength`]: EncodeError::UuidLength
/// [`Shape`]: EncodeError::Shape
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum EncodeError {
    /// The normalized column did not match the plan.
    #[error("column `{0}`: the batch does not match the plan ({1})")]
    Shape(String, String),
    /// A null for a non-nullable table column (or array element).
    #[error("column `{0}`: a null cannot be inserted into a non-nullable column")]
    NullNotAllowed(String),
    /// A value is out of range for the target type, for example a `u64`
    /// above `i64::MAX` into `Int64`.
    #[error("column `{0}`: value out of range for the ClickHouse type ({1})")]
    Range(String, String),
    /// A timestamp overflows when rescaled to the target precision.
    #[error("column `{0}`: timestamp overflows when rescaled to the target precision")]
    TsOverflow(String),
    /// A `UUID` target got something other than 16 bytes.
    #[error("column `{0}`: UUID targets need 16 bytes, got {1}")]
    UuidLength(String, usize),
    /// Normalization of the Arrow column failed.
    #[error(transparent)]
    Norm(#[from] NormError),
}

fn shape(col: &ColPlan, msg: impl Into<String>) -> EncodeError {
    EncodeError::Shape(col.table.clone(), msg.into())
}

/// Rescales timestamp ticks from the source unit to the target precision.
/// Widening multiplies (checked); narrowing floors, deterministically.
fn rescale(col: &ColPlan, nanos: bool, prec: u8, ticks: i64) -> Result<i64, EncodeError> {
    let from = if nanos { 9u32 } else { 6u32 };
    let to = u32::from(prec);
    if to >= from {
        ticks
            .checked_mul(10i64.pow(to - from))
            .ok_or_else(|| EncodeError::TsOverflow(col.table.clone()))
    } else {
        Ok(ticks.div_euclid(10i64.pow(from - to)))
    }
}

/// The seconds ClickHouse represents in `DateTime64` (1900-01-01 to
/// 2299-12-31) and in `DateTime` (`u32`).
const DATETIME64_SECS: (i64, i64) = (-2_208_988_800, 10_413_791_999);
const DATETIME_SECS: (i64, i64) = (0, 4_294_967_295);

/// [`rescale`] plus a check that the result lies within `secs` (inclusive,
/// whole seconds) at precision `prec`.
fn rescale_checked(
    col: &ColPlan,
    nanos: bool,
    prec: u8,
    ticks: i64,
    secs: (i64, i64),
) -> Result<i64, EncodeError> {
    let v = rescale(col, nanos, prec, ticks)?;
    let scale = 10i128.pow(u32::from(prec));
    let (lo, hi) = (
        i128::from(secs.0) * scale,
        (i128::from(secs.1) + 1) * scale - 1,
    );
    if (lo..=hi).contains(&i128::from(v)) {
        Ok(v)
    } else {
        Err(EncodeError::Range(
            col.table.clone(),
            format!("timestamp {v} (precision {prec}) is outside the range the column holds"),
        ))
    }
}

fn fixed_from(col: &ColPlan, n: usize, bytes: Box<[u8]>) -> Result<ByteString, EncodeError> {
    if bytes.len() > n {
        return Err(EncodeError::Range(
            col.table.clone(),
            format!("{} bytes do not fit FixedString({n})", bytes.len()),
        ));
    }
    Ok(ByteString(bytes))
}

/// Maps every cell of a source run through `cvt`.
fn convert<S, T>(
    col: &ColPlan,
    cells: Vec<Option<S>>,
    cvt: impl Fn(&ColPlan, S) -> Result<T, EncodeError>,
) -> Result<Vec<Option<T>>, EncodeError> {
    cells
        .into_iter()
        .map(|cell| cell.map(|x| cvt(col, x)).transpose())
        .collect()
}

fn require<T>(col: &ColPlan, cells: Vec<Option<T>>) -> Result<Vec<T>, EncodeError> {
    cells
        .into_iter()
        .map(|c| c.ok_or_else(|| EncodeError::NullNotAllowed(col.table.clone())))
        .collect()
}

/// Splits the flat element run of a list column into one `Vec` per row, in
/// row order. A null list becomes an empty array (ClickHouse arrays cannot be
/// NULL).
fn split_rows<T>(
    col: &ColPlan,
    lists: &Lists,
    cells: Vec<Option<T>>,
) -> Result<Vec<Vec<Option<T>>>, EncodeError> {
    let mut it = cells.into_iter();
    let mut pos = 0usize;
    let mut rows = Vec::with_capacity(lists.rows.len());
    for bounds in &lists.rows {
        let Some((start, end)) = *bounds else {
            rows.push(Vec::new());
            continue;
        };
        if start < pos || end < start {
            return Err(shape(col, "list offsets are not monotonic"));
        }
        for _ in pos..start {
            it.next();
        }
        let elems: Vec<Option<T>> = it.by_ref().take(end - start).collect();
        if elems.len() != end - start {
            return Err(shape(col, "list offsets run past the element array"));
        }
        pos = end;
        rows.push(elems);
    }
    Ok(rows)
}

/// Adds one column of `T` cells to the block: a scalar column, or an `Array`
/// column when `lists` is present.
fn emit<T: Encode>(
    bb: &mut BlockBuilder,
    col: &ColPlan,
    cells: Vec<Option<T>>,
    lists: Option<&Lists>,
) -> Result<(), EncodeError> {
    let up = |e: &dyn std::fmt::Display| shape(col, e.to_string());
    let add = |e: Box<dyn StdError + Send + Sync>| shape(col, e.to_string());
    match (lists, &col.dst) {
        (None, Dst::Arr { .. }) => Err(shape(col, "expected a list column")),
        (Some(lists), Dst::Arr { elem_nullable, .. }) => {
            let rows = split_rows(col, lists, cells)?;
            if *elem_nullable {
                bb.upsert_column::<Vec<Option<T>>>(&col.table)
                    .map_err(|e| up(&e))?
                    .add_all(rows)
                    .map_err(add)?;
            } else {
                let rows = rows
                    .into_iter()
                    .map(|r| require(col, r))
                    .collect::<Result<Vec<_>, _>>()?;
                bb.upsert_column::<Vec<T>>(&col.table)
                    .map_err(|e| up(&e))?
                    .add_all(rows)
                    .map_err(add)?;
            }
            Ok(())
        }
        (Some(_), _) => Err(shape(col, "unexpected list column")),
        (None, _) if col.nullable => {
            bb.upsert_column::<Option<T>>(&col.table)
                .map_err(|e| up(&e))?
                .add_all(cells)
                .map_err(add)?;
            Ok(())
        }
        (None, _) => {
            bb.upsert_column::<T>(&col.table)
                .map_err(|e| up(&e))?
                .add_all(require(col, cells)?)
                .map_err(add)?;
            Ok(())
        }
    }
}

fn signed<T: TryFrom<i64>>(what: &'static str) -> impl Fn(&ColPlan, i64) -> Result<T, EncodeError> {
    move |col: &ColPlan, x: i64| {
        T::try_from(x).map_err(|_| {
            EncodeError::Range(col.table.clone(), format!("{x} does not fit into {what}"))
        })
    }
}

fn unsigned<T: TryFrom<u64>>(
    what: &'static str,
) -> impl Fn(&ColPlan, u64) -> Result<T, EncodeError> {
    move |col: &ColPlan, x: u64| {
        T::try_from(x).map_err(|_| {
            EncodeError::Range(col.table.clone(), format!("{x} does not fit into {what}"))
        })
    }
}

fn uuid_from(col: &ColPlan, bytes: &[u8]) -> Result<UuidBytes, EncodeError> {
    <[u8; 16]>::try_from(bytes)
        .map(UuidBytes)
        .map_err(|_| EncodeError::UuidLength(col.table.clone(), bytes.len()))
}

/// The leaf target of a column: the element type for arrays, the column type
/// otherwise.
fn leaf_dst(dst: &Dst) -> &Dst {
    match dst {
        Dst::Arr { elem, .. } => elem,
        other => other,
    }
}

fn mismatch(col: &ColPlan, want: &str) -> EncodeError {
    shape(col, format!("expected {want} cells"))
}

fn write_column(bb: &mut BlockBuilder, col: &ColPlan, norm: Norm) -> Result<(), EncodeError> {
    let Norm { leaves, lists } = norm;
    let lists = lists.as_ref();
    match (leaf_dst(&col.dst), leaves) {
        (Dst::Bool, Leaves::Bool(v)) => emit(bb, col, v, lists),
        (Dst::Int(w), Leaves::I64(v)) => match w {
            IntWidth::I8 => emit(bb, col, convert(col, v, signed::<i8>("Int8"))?, lists),
            IntWidth::I16 => emit(bb, col, convert(col, v, signed::<i16>("Int16"))?, lists),
            IntWidth::I32 => emit(bb, col, convert(col, v, signed::<i32>("Int32"))?, lists),
            IntWidth::I64 => emit(bb, col, v, lists),
        },
        // `UInt64` into `Int64` passes the plan; values above `i64::MAX` fail
        // here as data errors and quarantine the offending events.
        (Dst::Int(IntWidth::I64), Leaves::U64(v)) => {
            emit(bb, col, convert(col, v, unsigned::<i64>("Int64"))?, lists)
        }
        (Dst::UInt(w), Leaves::U64(v)) => match w {
            UIntWidth::U8 => emit(bb, col, convert(col, v, unsigned::<u8>("UInt8"))?, lists),
            UIntWidth::U16 => emit(bb, col, convert(col, v, unsigned::<u16>("UInt16"))?, lists),
            UIntWidth::U32 => emit(bb, col, convert(col, v, unsigned::<u32>("UInt32"))?, lists),
            UIntWidth::U64 => emit(bb, col, v, lists),
        },
        (Dst::Float(w), Leaves::F64(v)) => match w {
            FloatWidth::F32 => emit(
                bb,
                col,
                // Float32 arrow cells widened to f64 narrow back exactly.
                convert(col, v, |_, x| Ok(x as f32))?,
                lists,
            ),
            FloatWidth::F64 => emit(bb, col, v, lists),
        },
        (Dst::Str, Leaves::Bytes(v)) => {
            emit(bb, col, convert(col, v, |_, b| Ok(ByteString(b)))?, lists)
        }
        (Dst::FixedStr(n), Leaves::Bytes(v)) => {
            let n = *n;
            emit(bb, col, convert(col, v, |c, b| fixed_from(c, n, b))?, lists)
        }
        (Dst::Uuid, Leaves::Bytes(v)) => {
            emit(bb, col, convert(col, v, |c, b| uuid_from(c, &b))?, lists)
        }
        (Dst::DateTime, Leaves::I64(v)) => {
            let nanos = col.src_ts_nanos.unwrap_or(false);
            let cells = convert(col, v, |c, x| {
                rescale_checked(c, nanos, 0, x, DATETIME_SECS).map(Ts64::<0>)
            })?;
            emit(bb, col, cells, lists)
        }
        (Dst::Ts(p), Leaves::I64(v)) => {
            let nanos = col.src_ts_nanos.unwrap_or(false);
            let p = *p;
            macro_rules! ts {
                ($p:literal) => {
                    emit(
                        bb,
                        col,
                        convert(col, v, |c, x| {
                            rescale_checked(c, nanos, p, x, DATETIME64_SECS).map(Ts64::<$p>)
                        })?,
                        lists,
                    )
                };
            }
            match p {
                0 => ts!(0),
                1 => ts!(1),
                2 => ts!(2),
                3 => ts!(3),
                4 => ts!(4),
                5 => ts!(5),
                6 => ts!(6),
                7 => ts!(7),
                8 => ts!(8),
                _ => ts!(9),
            }
        }
        (Dst::Arr { .. }, _) => Err(shape(col, "nested arrays are not supported")),
        (Dst::Bool, _) => Err(mismatch(col, "boolean")),
        (Dst::Int(_), _) => Err(mismatch(col, "signed integer")),
        (Dst::UInt(_), _) => Err(mismatch(col, "unsigned integer")),
        (Dst::Float(_), _) => Err(mismatch(col, "float")),
        (Dst::Str | Dst::FixedStr(_) | Dst::Uuid, _) => Err(mismatch(col, "string or binary")),
        (Dst::Ts(_) | Dst::DateTime, _) => Err(mismatch(col, "timestamp")),
    }
}

/// Turns a `RecordBatch` into a ClickHouse Native block per the plan.
/// `Ok(None)` means zero rows, which the Native format cannot express: the
/// caller skips the insert.
pub(crate) fn build_block(
    plan: &InsertPlan,
    batch: &RecordBatch,
) -> Result<Option<Block>, EncodeError> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let mut bb = BlockBuilder::new();
    for col in &plan.cols {
        let array = batch
            .columns()
            .get(col.arrow_index)
            .ok_or_else(|| shape(col, "arrow column missing"))?;
        let norm = Norm::from_arrow(array.as_ref())?;
        if norm.rows() != batch.num_rows() {
            return Err(shape(col, "row count mismatch"));
        }
        write_column(&mut bb, col, norm)?;
    }
    bb.build()
        .map(Some)
        .map_err(|e| EncodeError::Shape("block".to_owned(), e.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use arrow_array::RecordBatch;
    use thadam_arrow::BatchEncoder;
    use thadam_core::{Event, EventBatch, WalEpoch, WalPos, WalRange};

    use super::*;
    use crate::testutil::{DATA_TABLE, event, full_table, schema};
    use crate::types::build_plan;

    fn record_batch(events: Vec<Event>) -> RecordBatch {
        let first = events.first().map_or(1, |e| e.wal_pos().get());
        let last = events.last().map_or(1, |e| e.wal_pos().get());
        let origin =
            WalRange::new(WalEpoch::new(1), WalPos::new(first), WalPos::new(last)).unwrap();
        let mut encoder = BatchEncoder::new(schema(), 8);
        let rejects = encoder.push_batch(&EventBatch::new(events, origin, true));
        assert!(rejects.is_empty(), "{rejects:?}");
        encoder.finish()
    }

    fn plan_for(table: &[crate::types::TableCol]) -> InsertPlan {
        let fields: Vec<_> = schema().arrow_schema().fields().iter().cloned().collect();
        build_plan(&fields, table, None).unwrap()
    }

    fn three_events() -> Vec<Event> {
        vec![
            event(
                1,
                Some(20.5),
                Some("héllo"),
                Some(vec![Some(1.0), Some(2.5)]),
            ),
            event(2, None, None, None),
            event(3, Some(-4.25), Some(""), Some(vec![])),
        ]
    }

    fn column<'a, T: clickhouse::native::decode::Decode<'a> + 'a>(
        block: &'a Block,
        name: &str,
    ) -> Vec<T> {
        block[name]
            .iter::<T>()
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn a_batch_becomes_a_block_with_every_planned_column() {
        let plan = plan_for(&full_table());
        let block = build_block(&plan, &record_batch(three_events()))
            .unwrap()
            .expect("three rows");
        assert_eq!(block.num_rows(), 3);
        assert_eq!(block.columns().len(), plan.cols.len());

        assert_eq!(
            column::<Option<f64>>(&block, "temp"),
            [Some(20.5), None, Some(-4.25)]
        );
        assert_eq!(
            column::<Option<String>>(&block, "note"),
            [Some("héllo".to_owned()), None, Some(String::new())]
        );
        assert_eq!(column::<i64>(&block, "n"), [1, 2, 3]);
        assert_eq!(column::<u32>(&block, "counter"), [11, 12, 13]);
        assert_eq!(column::<u64>(&block, "_wal_pos"), [1, 2, 3]);
        assert_eq!(column::<u64>(&block, "_wal_epoch"), [1, 1, 1]);
        assert_eq!(
            column::<String>(&block, "_quality_status"),
            ["good", "good", "good"]
        );
        assert_eq!(
            column::<String>(&block, "_time_domain"),
            ["utc", "utc", "utc"]
        );
        assert_eq!(
            column::<Option<String>>(&block, "robot"),
            vec![Some("r1".to_owned()); 3]
        );
        assert_eq!(column::<bool>(&block, "_clock_synced"), [true; 3]);
        // A null list is stored as an empty array.
        assert_eq!(
            column::<Vec<f32>>(&block, "samples"),
            [vec![1.0, 2.5], vec![], vec![]]
        );

        assert_eq!(
            *block["seen"].data_type(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision6, Some("UTC".to_owned()))
        );
        assert_eq!(*block["_event_id"].data_type(), DataTypeNode::UUID);
    }

    #[test]
    fn only_planned_columns_are_written() {
        let plan = plan_for(&crate::testutil::describe(DATA_TABLE));
        let block = build_block(&plan, &record_batch(three_events()))
            .unwrap()
            .unwrap();
        let names: Vec<&str> = block.columns().iter().map(|c| c.name()).collect();
        assert_eq!(names, ["temp", "note", "n", "counter", "samples", "seen"]);
    }

    #[test]
    fn nullable_elements_round_trip_in_arrays() {
        let table = crate::testutil::describe(&[
            ("n", "Int64"),
            ("counter", "UInt32"),
            ("seen", "DateTime64(6)"),
            ("temp", "Nullable(Float64)"),
            ("note", "Nullable(String)"),
            ("samples", "Array(Nullable(Float32))"),
        ]);
        let plan = plan_for(&table);
        let events = vec![event(1, None, None, Some(vec![Some(1.0), None, Some(3.0)]))];
        let block = build_block(&plan, &record_batch(events)).unwrap().unwrap();
        assert_eq!(
            column::<Vec<Option<f32>>>(&block, "samples"),
            [vec![Some(1.0), None, Some(3.0)]]
        );
    }

    #[test]
    fn a_null_element_for_a_non_nullable_array_is_a_data_error() {
        let plan = plan_for(&crate::testutil::describe(DATA_TABLE));
        let events = vec![event(1, None, None, Some(vec![Some(1.0), None]))];
        let err = build_block(&plan, &record_batch(events))
            .err()
            .expect("poison");
        assert_eq!(err, EncodeError::NullNotAllowed("samples".to_owned()));
    }

    #[test]
    fn a_null_for_a_non_nullable_column_is_a_data_error() {
        let mut plan = plan_for(&crate::testutil::describe(DATA_TABLE));
        plan.cols[1].nullable = false; // `note`
        let err = build_block(&plan, &record_batch(three_events()))
            .err()
            .expect("poison");
        assert_eq!(err, EncodeError::NullNotAllowed("note".to_owned()));
    }

    #[test]
    fn out_of_range_values_are_data_errors() {
        let mut plan = plan_for(&crate::testutil::describe(DATA_TABLE));
        // `n` is 1..=3; pretend the table narrowed it to Int8 after a retry
        // with a value that no longer fits.
        plan.cols[2].dst = Dst::Int(IntWidth::I8);
        let big = vec![event(200, Some(1.0), Some("x"), None)];
        let err = build_block(&plan, &record_batch(big)).err().expect("range");
        assert!(
            matches!(&err, EncodeError::Range(c, m) if c == "n" && m.contains("200")),
            "{err:?}"
        );
        assert!(build_block(&plan, &record_batch(vec![event(100, None, None, None)])).is_ok());
    }

    #[test]
    fn an_empty_batch_has_no_block() {
        let plan = plan_for(&crate::testutil::describe(DATA_TABLE));
        let empty = BatchEncoder::new(schema(), 1).finish();
        assert!(build_block(&plan, &empty).unwrap().is_none());
    }

    #[test]
    fn timestamps_rescale_exactly() {
        let col = ColPlan {
            table: "t".to_owned(),
            arrow_index: 0,
            nullable: false,
            dst: Dst::Ts(3),
            src_ts_nanos: Some(false),
        };
        assert_eq!(
            rescale(&col, false, 6, 1_700_000_000_123_456).unwrap(),
            1_700_000_000_123_456
        );
        assert_eq!(
            rescale(&col, false, 3, 1_700_000_000_123_456).unwrap(),
            1_700_000_000_123
        );
        assert_eq!(
            rescale(&col, false, 0, 1_700_000_000_999_999).unwrap(),
            1_700_000_000
        );
        assert_eq!(
            rescale(&col, true, 6, 1_700_000_000_123_456_789).unwrap(),
            1_700_000_000_123_456
        );
        assert_eq!(rescale(&col, true, 9, 5).unwrap(), 5);
        assert_eq!(rescale(&col, false, 9, 5).unwrap(), 5_000);
        // Floors toward negative infinity, not toward zero.
        assert_eq!(rescale(&col, false, 3, -1).unwrap(), -1);
        assert!(matches!(
            rescale(&col, false, 9, i64::MAX / 10),
            Err(EncodeError::TsOverflow(c)) if c == "t"
        ));
    }

    #[test]
    fn fixed_strings_and_datetime_are_checked_here_not_by_the_server() {
        let col = |dst| ColPlan {
            table: "c".to_owned(),
            arrow_index: 0,
            nullable: true,
            dst,
            src_ts_nanos: Some(false),
        };
        // FixedString(3): up to three bytes fit, more is a data error.
        assert!(fixed_from(&col(Dst::FixedStr(3)), 3, Box::from(&b"abc"[..])).is_ok());
        assert!(fixed_from(&col(Dst::FixedStr(3)), 3, Box::from(&b""[..])).is_ok());
        assert!(matches!(
            fixed_from(&col(Dst::FixedStr(3)), 3, Box::from(&b"toolong"[..])),
            Err(EncodeError::Range(c, m)) if c == "c" && m.contains("7 bytes")
        ));
        // DateTime holds 1970..=2106 seconds.
        let c = col(Dst::DateTime);
        assert_eq!(
            rescale_checked(&c, false, 0, 1_700_000_000_000_000, DATETIME_SECS).unwrap(),
            1_700_000_000
        );
        assert!(
            rescale_checked(&c, false, 0, -1, DATETIME_SECS).is_err(),
            "before 1970"
        );
        assert!(
            rescale_checked(&c, false, 0, 4_294_967_296_000_000, DATETIME_SECS).is_err(),
            "after 2106"
        );
        // DateTime64 holds 1900..=2299.
        let c = col(Dst::Ts(3));
        assert!(rescale_checked(&c, false, 3, -2_208_988_800_000_000, DATETIME64_SECS).is_ok());
        assert!(rescale_checked(&c, false, 3, -2_208_988_801_000_000, DATETIME64_SECS).is_err());
        assert!(rescale_checked(&c, false, 3, 10_413_792_000_000_000, DATETIME64_SECS).is_err());
    }

    #[test]
    fn blocks_are_deterministic_for_a_retried_batch() {
        let plan = plan_for(&full_table());
        let a = build_block(&plan, &record_batch(three_events()))
            .unwrap()
            .unwrap();
        let b = build_block(&plan, &record_batch(three_events()))
            .unwrap()
            .unwrap();
        assert_eq!(column::<i64>(&a, "n"), column::<i64>(&b, "n"));
        assert_eq!(
            column::<Option<f64>>(&a, "temp"),
            column::<Option<f64>>(&b, "temp")
        );
    }
}
