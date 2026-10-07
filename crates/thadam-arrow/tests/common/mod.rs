//! Shared helpers for the integration tests.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use thadam_arrow::{ArrowOptions, BatchEncoder, DatasetSchema};
use thadam_core::{
    AssetPath, Event, EventId, FieldDef, Quality, Record, Shape, ShapeRef, TimeDomain, Times,
    Timestamp, Value, WalPos,
};

/// Ingest time used by `event`: 2023-11-14T22:13:20.123456789Z.
pub const INGEST_NANOS: i64 = 1_700_000_000_123_456_789;

/// A cell read back from an Arrow array, with floats compared by bit pattern
/// (as `f64`, so an `f32` column and a widened `f64` value agree).
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i128),
    Float(u64),
    Str(String),
    Bytes(Vec<u8>),
    Ts(i64),
    List(Vec<Cell>),
    Struct(Vec<Cell>),
}

pub fn float(x: f64) -> Cell {
    Cell::Float(x.to_bits())
}

pub fn cell(a: &dyn Array, i: usize) -> Cell {
    if a.is_null(i) {
        return Cell::Null;
    }
    match a.data_type() {
        DataType::Boolean => Cell::Bool(a.as_boolean().value(i)),
        DataType::Int8 => Cell::Int(i128::from(a.as_primitive::<Int8Type>().value(i))),
        DataType::Int16 => Cell::Int(i128::from(a.as_primitive::<Int16Type>().value(i))),
        DataType::Int32 => Cell::Int(i128::from(a.as_primitive::<Int32Type>().value(i))),
        DataType::Int64 => Cell::Int(i128::from(a.as_primitive::<Int64Type>().value(i))),
        DataType::UInt8 => Cell::Int(i128::from(a.as_primitive::<UInt8Type>().value(i))),
        DataType::UInt16 => Cell::Int(i128::from(a.as_primitive::<UInt16Type>().value(i))),
        DataType::UInt32 => Cell::Int(i128::from(a.as_primitive::<UInt32Type>().value(i))),
        DataType::UInt64 => Cell::Int(i128::from(a.as_primitive::<UInt64Type>().value(i))),
        DataType::Float32 => float(f64::from(a.as_primitive::<Float32Type>().value(i))),
        DataType::Float64 => float(a.as_primitive::<Float64Type>().value(i)),
        DataType::Utf8 => Cell::Str(a.as_string::<i32>().value(i).to_owned()),
        DataType::Binary => Cell::Bytes(a.as_binary::<i32>().value(i).to_vec()),
        DataType::FixedSizeBinary(_) => Cell::Bytes(a.as_fixed_size_binary().value(i).to_vec()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Cell::Ts(a.as_primitive::<TimestampMicrosecondType>().value(i))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Cell::Ts(a.as_primitive::<TimestampNanosecondType>().value(i))
        }
        DataType::Dictionary(key, _) => {
            let (values, k) = match key.as_ref() {
                DataType::Int8 => {
                    let d = a.as_dictionary::<Int8Type>();
                    (d.values().clone(), d.keys().value(i) as usize)
                }
                DataType::Int32 => {
                    let d = a.as_dictionary::<Int32Type>();
                    (d.values().clone(), d.keys().value(i) as usize)
                }
                other => panic!("unexpected dictionary key {other}"),
            };
            Cell::Str(values.as_string::<i32>().value(k).to_owned())
        }
        DataType::List(_) => {
            let list = a.as_list::<i32>().value(i);
            Cell::List((0..list.len()).map(|j| cell(list.as_ref(), j)).collect())
        }
        DataType::Struct(_) => {
            let s = a.as_struct();
            Cell::Struct(
                (0..s.num_columns())
                    .map(|c| cell(s.column(c).as_ref(), i))
                    .collect(),
            )
        }
        other => panic!("unexpected type {other}"),
    }
}

pub fn column(batch: &RecordBatch, name: &str) -> Vec<Cell> {
    let a = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column `{name}`"));
    (0..a.len()).map(|i| cell(a.as_ref(), i)).collect()
}

/// The cell a value reads back as, ignoring the declared type: floats widen to
/// `f64`, timestamps convert to `ts_nanos / ts_divisor`.
pub fn cell_of(v: &Value, ts_divisor: i64) -> Cell {
    match v {
        Value::Null => Cell::Null,
        Value::Bool(x) => Cell::Bool(*x),
        Value::I8(x) => Cell::Int(i128::from(*x)),
        Value::I16(x) => Cell::Int(i128::from(*x)),
        Value::I32(x) => Cell::Int(i128::from(*x)),
        Value::I64(x) => Cell::Int(i128::from(*x)),
        Value::U8(x) => Cell::Int(i128::from(*x)),
        Value::U16(x) => Cell::Int(i128::from(*x)),
        Value::U32(x) => Cell::Int(i128::from(*x)),
        Value::U64(x) => Cell::Int(i128::from(*x)),
        Value::F32(x) => float(f64::from(*x)),
        Value::F64(x) => float(*x),
        Value::Str(s) => Cell::Str(s.as_str().to_owned()),
        Value::Bytes(b) => Cell::Bytes(b.to_vec()),
        Value::Ts(t) => Cell::Ts(t.as_nanos().div_euclid(ts_divisor)),
        Value::F32s(a) => Cell::List(a.iter().map(|x| float(f64::from(*x))).collect()),
        Value::F64s(a) => Cell::List(a.iter().map(|x| float(*x)).collect()),
        Value::I32s(a) => Cell::List(a.iter().map(|x| Cell::Int(i128::from(*x))).collect()),
        Value::I64s(a) => Cell::List(a.iter().map(|x| Cell::Int(i128::from(*x))).collect()),
        Value::List(items) => Cell::List(items.iter().map(|v| cell_of(v, ts_divisor)).collect()),
        Value::Record(r) => {
            Cell::Struct(r.values().iter().map(|v| cell_of(v, ts_divisor)).collect())
        }
        other => panic!("unexpected value {other:?}"),
    }
}

/// Gives every field without an ID `1000 + position`.
pub fn with_ids(fields: Vec<FieldDef>) -> Vec<FieldDef> {
    fields
        .into_iter()
        .enumerate()
        .map(|(i, f)| {
            if f.field_id.is_some() {
                f
            } else {
                f.with_field_id(1000 + i as u32)
            }
        })
        .collect()
}

pub fn schema(fields: Vec<FieldDef>, opts: ArrowOptions) -> Arc<DatasetSchema> {
    Arc::new(
        DatasetSchema::builder("t")
            .fields(with_ids(fields))
            .options(opts)
            .build()
            .unwrap(),
    )
}

pub fn encoder(fields: Vec<FieldDef>, opts: ArrowOptions) -> BatchEncoder {
    BatchEncoder::new(schema(fields, opts), 16)
}

pub fn shape(fields: Vec<FieldDef>) -> ShapeRef {
    ShapeRef::new(Shape::new(fields).unwrap())
}

pub fn times() -> Times {
    Times::new(Timestamp::from_nanos(INGEST_NANOS), TimeDomain::Utc)
}

/// An event carrying `values` under `shape`, with default metadata.
pub fn event(shape: &ShapeRef, values: Vec<Value>) -> Event {
    event_with(shape, values, times(), Quality::good(), AssetPath::empty())
}

pub fn event_with(
    shape: &ShapeRef,
    values: Vec<Value>,
    times: Times,
    quality: Quality,
    asset: AssetPath,
) -> Event {
    Event::builder(EventId::from_u128(0xabcdef), WalPos::new(7), times)
        .payload(Record::new(shape.clone(), values).unwrap())
        .quality(quality)
        .asset(asset)
        .build()
        .unwrap()
}

/// Every field of `schema`, nested ones included, as (dotted path, field ID).
/// Panics if a field has no ID.
pub fn all_ids(schema: &arrow_schema::Schema) -> Vec<(String, i32)> {
    fn walk(path: String, f: &arrow_schema::Field, out: &mut Vec<(String, i32)>) {
        let id = thadam_arrow::field_id(f).unwrap_or_else(|| panic!("`{path}` has no field id"));
        out.push((path.clone(), id));
        match f.data_type() {
            DataType::List(c) => walk(format!("{path}.{}", c.name()), c, out),
            DataType::Struct(fs) => fs
                .iter()
                .for_each(|c| walk(format!("{path}.{}", c.name()), c, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    schema
        .fields()
        .iter()
        .for_each(|f| walk(f.name().clone(), f, &mut out));
    out
}
