//! Round-trip readback: events in, Arrow arrays read back, and the batch
//! through the Arrow IPC stream format with its field IDs intact.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::io::Cursor;
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use common::{Cell, all_ids, cell_of, column, shape};
use proptest::prelude::*;
use thadam_arrow::{ArrowOptions, BatchContext, BatchEncoder, DatasetSchema, TsUnit, field_id};
use thadam_core::{
    AssetPath, Event, EventId, FieldDef, FieldType, Quality, QualityFlags, QualityStatus, Record,
    ShapeRef, TimeDomain, Times, Timestamp, Value, WalEpoch, WalPos,
};

fn list(inner: FieldType) -> FieldType {
    FieldType::List(Box::new(inner))
}

fn rec_shape() -> ShapeRef {
    shape(vec![
        FieldDef::new("x", FieldType::F64),
        FieldDef::new("n", FieldType::I32).required(),
        FieldDef::new("tags", list(FieldType::Str)),
    ])
}

fn declared() -> Vec<FieldDef> {
    vec![
        FieldDef::new("b", FieldType::Bool),
        FieldDef::new("i", FieldType::I64),
        FieldDef::new("u", FieldType::U32),
        FieldDef::new("f", FieldType::F32),
        FieldDef::new("d", FieldType::F64),
        FieldDef::new("s", FieldType::Str),
        FieldDef::new("by", FieldType::Bytes),
        FieldDef::new("t", FieldType::Ts),
        FieldDef::new("joints", list(FieldType::F64)),
        FieldDef::new("ints", list(FieldType::I64)),
        FieldDef::new("tags", list(FieldType::Str)),
        FieldDef::new("rec", FieldType::Record(rec_shape())),
    ]
}

fn schema(opts: ArrowOptions) -> Arc<DatasetSchema> {
    Arc::new(
        DatasetSchema::builder("roundtrip")
            .level("site", 100)
            .level("robot", 101)
            .exploded(true)
            .fields(common::with_ids(declared()))
            .options(opts)
            .build()
            .unwrap(),
    )
}

fn maybe(v: impl Strategy<Value = Value>) -> impl Strategy<Value = Value> {
    prop_oneof![1 => Just(Value::Null), 6 => v]
}

fn text() -> impl Strategy<Value = String> {
    "[a-z\u{e9}\u{4e16}]{0,12}"
}

fn tags() -> impl Strategy<Value = Value> {
    proptest::collection::vec(
        prop_oneof![1 => Just(Value::Null), 4 => text().prop_map(Value::str)],
        0..4,
    )
    .prop_map(Value::List)
}

fn record() -> impl Strategy<Value = Value> {
    (
        maybe(any::<f64>().prop_map(Value::F64)),
        any::<i32>(),
        maybe(tags()),
    )
        .prop_map(|(x, n, tags)| {
            Value::record(Record::new(rec_shape(), vec![x, Value::I32(n), tags]).unwrap())
        })
}

fn values() -> impl Strategy<Value = Vec<Value>> {
    let ints = prop_oneof![
        any::<i8>().prop_map(Value::I8),
        any::<i16>().prop_map(Value::I16),
        any::<i32>().prop_map(Value::I32),
        any::<i64>().prop_map(Value::I64),
        any::<u8>().prop_map(Value::U8),
        any::<u16>().prop_map(Value::U16),
        any::<u32>().prop_map(Value::U32),
    ];
    let unsigned = prop_oneof![
        any::<u8>().prop_map(Value::U8),
        any::<u16>().prop_map(Value::U16),
        any::<u32>().prop_map(Value::U32),
    ];
    let floats = prop_oneof![
        any::<f32>().prop_map(Value::F32),
        any::<f64>().prop_map(Value::F64)
    ];
    let joints = prop_oneof![
        proptest::collection::vec(any::<f64>(), 0..9).prop_map(Value::from),
        proptest::collection::vec(any::<f32>(), 0..9).prop_map(Value::from),
        proptest::collection::vec(
            prop_oneof![1 => Just(Value::Null), 4 => any::<f64>().prop_map(Value::F64)],
            0..5
        )
        .prop_map(Value::List),
    ];
    let int_lists = prop_oneof![
        proptest::collection::vec(any::<i32>(), 0..9).prop_map(Value::from),
        proptest::collection::vec(any::<i64>(), 0..9).prop_map(Value::from),
        proptest::collection::vec(prop_oneof![1 => Just(Value::Null), 4 => ints.clone()], 0..5)
            .prop_map(Value::List),
    ];
    (
        (
            maybe(any::<bool>().prop_map(Value::Bool)),
            maybe(ints),
            maybe(unsigned),
            maybe(any::<f32>().prop_map(Value::F32)),
            maybe(floats),
            maybe(text().prop_map(Value::str)),
        ),
        (
            maybe(
                proptest::collection::vec(any::<u8>(), 0..10)
                    .prop_map(|b| Value::bytes(Bytes::from(b))),
            ),
            maybe(any::<i64>().prop_map(|n| Value::Ts(Timestamp::from_nanos(n)))),
            maybe(joints),
            maybe(int_lists),
            maybe(tags()),
            maybe(record()),
        ),
    )
        .prop_map(|((a, b, c, d, e, f), (g, h, i, j, k, l))| {
            vec![a, b, c, d, e, f, g, h, i, j, k, l]
        })
}

#[derive(Debug, Clone)]
struct Meta {
    ingest: i64,
    source: Option<i64>,
    domain: usize,
    status: u8,
    flags: u32,
    reason: Option<String>,
    pos: u64,
    sample: Option<u16>,
    id: u128,
    site: Option<String>,
    robot: Option<String>,
    robot_first: bool,
}

fn meta() -> impl Strategy<Value = Meta> {
    (
        (
            any::<i64>(),
            proptest::option::of(any::<i64>()),
            0usize..5,
            0u8..3,
        ),
        (
            any::<u32>(),
            proptest::option::of("[a-z ]{0,8}"),
            any::<u64>(),
            proptest::option::of(any::<u16>()),
        ),
        (
            any::<u128>(),
            proptest::option::of("[a-z0-9-]{1,6}"),
            proptest::option::of("[a-z0-9-]{1,6}"),
            any::<bool>(),
        ),
    )
        .prop_map(
            |(
                (ingest, source, domain, status),
                (flags, reason, pos, sample),
                (id, site, robot, robot_first),
            )| Meta {
                ingest,
                source,
                domain,
                status,
                flags,
                reason,
                pos,
                sample,
                id,
                site,
                robot,
                robot_first,
            },
        )
}

fn build_event(m: &Meta, values: Vec<Value>) -> Event {
    let mut times = Times::new(Timestamp::from_nanos(m.ingest), TimeDomain::ALL[m.domain]);
    if let Some(s) = m.source {
        times.set_source(Timestamp::from_nanos(s)).unwrap();
    }
    let mut levels = Vec::new();
    if let Some(s) = &m.site {
        levels.push(("site", s.as_str()));
    }
    if let Some(r) = &m.robot {
        levels.push(("robot", r.as_str()));
    }
    if m.robot_first {
        levels.reverse();
    }
    let status = [
        QualityStatus::Good,
        QualityStatus::Uncertain,
        QualityStatus::Bad,
    ][usize::from(m.status)];
    let mut ev = Event::builder(EventId::from_u128(m.id), WalPos::new(m.pos), times)
        .payload(Record::new(shape(declared()), values).unwrap())
        .quality(Quality {
            status,
            flags: QualityFlags::from_bits_truncate(m.flags),
            reason: m.reason.as_deref().map(Into::into),
        })
        .asset(AssetPath::new(levels).unwrap())
        .build()
        .unwrap();
    ev.set_sample_idx(m.sample.map(u32::from));
    ev
}

fn opt<T>(v: Option<T>, f: impl FnOnce(T) -> Cell) -> Cell {
    v.map_or(Cell::Null, f)
}

fn check_envelope(
    batch: &arrow_array::RecordBatch,
    events: &[Event],
    div: i64,
    epoch: u64,
    synced: bool,
) {
    let ts = |t: Timestamp| Cell::Ts(t.as_nanos().div_euclid(div));
    let n = events.len();
    let col = |name: &str| column(batch, name);
    assert_eq!(
        col("_event_id"),
        events
            .iter()
            .map(|e| Cell::Bytes(e.id().to_bytes().to_vec()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_ingest_time"),
        events
            .iter()
            .map(|e| ts(e.times().ingest()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_source_time"),
        events
            .iter()
            .map(|e| opt(e.times().source(), ts))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_event_time"),
        events
            .iter()
            .map(|e| opt(e.times().event_time(), ts))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_time_domain"),
        events
            .iter()
            .map(|e| Cell::Str(e.times().domain().as_str().into()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_quality_status"),
        events
            .iter()
            .map(|e| Cell::Str(e.quality().status.as_str().into()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_quality_flags"),
        events
            .iter()
            .map(|e| Cell::Int(i128::from(e.quality().flags.bits())))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("_quality_reason"),
        events
            .iter()
            .map(|e| opt(e.quality().reason.as_deref(), |r| Cell::Str(r.into())))
            .collect::<Vec<_>>()
    );
    assert_eq!(col("_clock_synced"), vec![Cell::Bool(synced); n]);
    assert_eq!(col("_wal_epoch"), vec![Cell::Int(i128::from(epoch)); n]);
    assert_eq!(
        col("_wal_pos"),
        events
            .iter()
            .map(|e| Cell::Int(i128::from(e.wal_pos().get())))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("site"),
        events
            .iter()
            .map(|e| opt(e.asset().get("site"), |s| Cell::Str(s.into())))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("robot"),
        events
            .iter()
            .map(|e| opt(e.asset().get("robot"), |s| Cell::Str(s.into())))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        col("sample_idx"),
        events
            .iter()
            .map(|e| opt(e.sample_idx(), |s| Cell::Int(i128::from(s))))
            .collect::<Vec<_>>()
    );
}

fn ipc_round_trip(batch: &arrow_array::RecordBatch) -> arrow_array::RecordBatch {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    let reader = StreamReader::try_new(Cursor::new(buf), None).unwrap();
    assert_eq!(reader.schema(), batch.schema(), "schema, metadata included");
    let mut out: Vec<_> = reader.map(Result::unwrap).collect();
    assert_eq!(out.len(), 1);
    out.remove(0)
}

fn check_all(opts: ArrowOptions, div: i64, rows: Vec<(Meta, Vec<Value>)>) {
    let schema = schema(opts);
    let mut enc = BatchEncoder::new(Arc::clone(&schema), 4);
    enc.set_context(BatchContext {
        epoch: WalEpoch::new(0xfeed),
        clock_synced: true,
    });
    let events: Vec<Event> = rows
        .iter()
        .map(|(m, v)| build_event(m, v.clone()))
        .collect();
    for e in &events {
        enc.push(e).unwrap();
    }
    let batch = enc.finish();
    assert_eq!(batch.num_rows(), events.len());
    assert_eq!(batch.schema(), schema.arrow_schema());
    check_envelope(&batch, &events, div, 0xfeed, true);

    let names = [
        "b", "i", "u", "f", "d", "s", "by", "t", "joints", "ints", "tags", "rec",
    ];
    for (idx, name) in names.iter().enumerate() {
        let want: Vec<Cell> = rows.iter().map(|(_, v)| cell_of(&v[idx], div)).collect();
        assert_eq!(column(&batch, name), want, "column {name}");
    }

    let back = ipc_round_trip(&batch);
    assert_eq!(back, batch, "IPC round trip");
    for (path, id) in all_ids(&back.schema()) {
        assert!(id > 0, "{path}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn events_read_back_exactly_microseconds(rows in proptest::collection::vec((meta(), values()), 0..24)) {
        check_all(ArrowOptions::default(), 1_000, rows);
    }

    #[test]
    fn events_read_back_exactly_nanoseconds(rows in proptest::collection::vec((meta(), values()), 0..24)) {
        check_all(ArrowOptions::default().with_ts_unit(TsUnit::Nanos), 1, rows);
    }
}

#[test]
fn field_ids_survive_the_ipc_round_trip() {
    let schema = schema(ArrowOptions::default());
    let mut enc = BatchEncoder::new(Arc::clone(&schema), 4);
    let m = Meta {
        ingest: 1,
        source: None,
        domain: 0,
        status: 0,
        flags: 0,
        reason: None,
        pos: 1,
        sample: Some(3),
        id: 1,
        site: Some("a".into()),
        robot: None,
        robot_first: false,
    };
    let mut vals = vec![Value::Null; 12];
    vals[8] = Value::from(vec![1.0f64, 2.0]);
    vals[11] = Value::record(
        Record::new(
            rec_shape(),
            vec![Value::F64(1.0), Value::I32(2), Value::Null],
        )
        .unwrap(),
    );
    enc.push(&build_event(&m, vals)).unwrap();
    let batch = enc.finish();
    let back = ipc_round_trip(&batch);

    let before = all_ids(&schema.arrow_schema());
    let after = all_ids(&back.schema());
    assert_eq!(before, after);
    assert!(before.iter().any(|(p, _)| p == "joints.element"));
    assert!(before.iter().any(|(p, _)| p == "rec.tags.element"));
    let rec = back.schema().field_with_name("rec").unwrap().clone();
    assert_eq!(field_id(&rec), Some(1011));
}
