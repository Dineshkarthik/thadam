//! Shared fixtures for the unit tests: a dataset, events for it, and
//! `DESCRIBE TABLE` rows written the way the server prints them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use thadam_arrow::DatasetSchema;
use thadam_core::{
    AssetPath, Event, EventId, FieldDef, FieldType, Record, Shape, ShapeRef, TimeDomain, Times,
    Timestamp, Value, WalPos,
};

use crate::types::{TableCol, parse_ch_type};

/// A dataset exercising every mapping family: nullable and required scalars,
/// an unsigned integer, a list, a timestamp, and one identity level.
pub(crate) fn schema() -> Arc<DatasetSchema> {
    Arc::new(
        DatasetSchema::builder("imu")
            .level("robot", 100)
            .field(FieldDef::new("temp", FieldType::F64).with_field_id(1000))
            .field(FieldDef::new("note", FieldType::Str).with_field_id(1001))
            .field(
                FieldDef::new("n", FieldType::I64)
                    .required()
                    .with_field_id(1002),
            )
            .field(
                FieldDef::new("counter", FieldType::U32)
                    .required()
                    .with_field_id(1003),
            )
            .field(
                FieldDef::new("samples", FieldType::List(Box::new(FieldType::F32)))
                    .with_field_id(1004),
            )
            .field(
                FieldDef::new("seen", FieldType::Ts)
                    .required()
                    .with_field_id(1005),
            )
            .build()
            .expect("test schema"),
    )
}

/// The table for [`schema`] with only the declared columns.
pub(crate) const DATA_TABLE: &[(&str, &str)] = &[
    ("temp", "Nullable(Float64)"),
    ("note", "Nullable(String)"),
    ("n", "Int64"),
    ("counter", "UInt32"),
    ("samples", "Array(Float32)"),
    ("seen", "DateTime64(6, 'UTC')"),
];

/// The envelope and level columns of [`schema`], as a user would declare them.
pub(crate) const ENVELOPE_TABLE: &[(&str, &str)] = &[
    ("_event_id", "UUID"),
    ("_ingest_time", "DateTime64(6, 'UTC')"),
    ("_source_time", "Nullable(DateTime64(6, 'UTC'))"),
    ("_event_time", "Nullable(DateTime64(6, 'UTC'))"),
    ("_time_domain", "LowCardinality(String)"),
    ("_quality_status", "LowCardinality(String)"),
    ("_quality_flags", "UInt32"),
    ("_quality_reason", "Nullable(String)"),
    ("_clock_synced", "Bool"),
    ("_wal_epoch", "UInt64"),
    ("_wal_pos", "UInt64"),
    ("robot", "LowCardinality(Nullable(String))"),
];

/// `DESCRIBE TABLE` rows from `(name, type)` pairs.
pub(crate) fn describe(cols: &[(&str, &str)]) -> Vec<TableCol> {
    cols.iter()
        .map(|(name, ty)| TableCol {
            name: (*name).to_owned(),
            ty: parse_ch_type(ty),
            default_kind: String::new(),
        })
        .collect()
}

/// The envelope table followed by the data table.
pub(crate) fn full_table() -> Vec<TableCol> {
    describe(&[ENVELOPE_TABLE, DATA_TABLE].concat())
}

fn shape() -> ShapeRef {
    ShapeRef::new(
        Shape::new(vec![
            FieldDef::new("temp", FieldType::F64),
            FieldDef::new("note", FieldType::Str),
            FieldDef::new("n", FieldType::I64),
            FieldDef::new("counter", FieldType::U32),
            FieldDef::new("samples", FieldType::List(Box::new(FieldType::F32))),
            FieldDef::new("seen", FieldType::Ts),
        ])
        .expect("test shape"),
    )
}

/// One event of [`schema`]. The event id is `pos` as a UUID's low bits and the
/// ingest time is `1_700_000_000s + pos` microseconds.
pub(crate) fn event(
    pos: u64,
    temp: Option<f64>,
    note: Option<&str>,
    samples: Option<Vec<Option<f32>>>,
) -> Event {
    let values = vec![
        temp.map_or(Value::Null, Value::F64),
        note.map_or(Value::Null, Value::str),
        Value::I64(i64::try_from(pos).expect("small pos")),
        Value::U32(u32::try_from(pos).expect("small pos") + 10),
        samples.map_or(Value::Null, |s| {
            Value::List(
                s.into_iter()
                    .map(|x| x.map_or(Value::Null, Value::F32))
                    .collect(),
            )
        }),
        Value::Ts(Timestamp::from_nanos(
            1_700_000_000_000_000_000 + 1_500 * pos as i64,
        )),
    ];
    Event::builder(
        EventId::from_u128(0x0123_4567_89ab_cdef_0000_0000_0000_0000 + u128::from(pos)),
        WalPos::new(pos),
        Times::new(
            Timestamp::from_nanos(1_700_000_000_000_000_000 + 2_000 * pos as i64),
            TimeDomain::Utc,
        ),
    )
    .asset(AssetPath::new([("robot", "r1")]).expect("asset"))
    .payload(Record::new(shape(), values).expect("record"))
    .build()
    .expect("event")
}
