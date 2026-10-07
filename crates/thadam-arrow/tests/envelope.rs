//! Envelope, lineage, identity, and time columns.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use bytes::Bytes;
use common::{Cell, INGEST_NANOS, column, encoder, event, event_with, shape, times};
use thadam_arrow::{
    ArrowOptions, BatchContext, BatchEncoder, DatasetSchema, TsUnit, UnsignedPolicy, columns,
};
use thadam_core::{
    AssetPath, Event, EventBatch, EventId, FieldDef, FieldType, Payload, Quality, QualityFlags,
    QualityStatus, Record, TimeDomain, Times, Timestamp, Value, WalEpoch, WalPos, WalRange, codes,
};

fn one_field() -> Vec<FieldDef> {
    vec![FieldDef::new("v", FieldType::I32)]
}

fn rec_shape() -> thadam_core::ShapeRef {
    shape(one_field())
}

fn ev(times: Times, quality: Quality) -> Event {
    event_with(
        &rec_shape(),
        vec![Value::I32(1)],
        times,
        quality,
        AssetPath::empty(),
    )
}

fn names(schema: &DatasetSchema) -> Vec<String> {
    schema
        .arrow_schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[test]
fn column_list_follows_the_adr_order() {
    let schema = DatasetSchema::builder("d")
        .level("site", 100)
        .level("robot", 101)
        .exploded(true)
        .field(FieldDef::new("v", FieldType::I32).with_field_id(1000))
        .build()
        .unwrap();
    assert_eq!(
        names(&schema),
        [
            "_event_id",
            "_ingest_time",
            "_source_time",
            "_event_time",
            "_time_domain",
            "_quality_status",
            "_quality_flags",
            "_quality_reason",
            "_clock_synced",
            "_wal_epoch",
            "_wal_pos",
            "site",
            "robot",
            "sample_idx",
            "v",
        ]
    );
}

#[test]
fn envelope_types_and_nullability_follow_the_adr() {
    let schema = DatasetSchema::builder("d")
        .level("site", 100)
        .exploded(true)
        .build()
        .unwrap();
    let arrow = schema.arrow_schema();
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let dict8 = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let dict32 = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let expect = [
        ("_event_id", DataType::FixedSizeBinary(16), false),
        ("_ingest_time", ts.clone(), false),
        ("_source_time", ts.clone(), true),
        ("_event_time", ts, true),
        ("_time_domain", dict8.clone(), false),
        ("_quality_status", dict8, false),
        ("_quality_flags", DataType::UInt32, false),
        ("_quality_reason", DataType::Utf8, true),
        ("_clock_synced", DataType::Boolean, false),
        ("_wal_epoch", DataType::UInt64, false),
        ("_wal_pos", DataType::UInt64, false),
        ("site", dict32, true),
        ("sample_idx", DataType::UInt16, true),
    ];
    assert_eq!(arrow.fields().len(), expect.len());
    for (field, (name, ty, nullable)) in arrow.fields().iter().zip(expect) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &ty, "{name}");
        assert_eq!(field.is_nullable(), nullable, "{name}");
    }
}

#[test]
fn lineage_off_drops_exactly_the_three_lineage_columns() {
    let on = DatasetSchema::builder("d").build().unwrap();
    let off = DatasetSchema::builder("d")
        .options(ArrowOptions::default().with_lineage_columns(false))
        .build()
        .unwrap();
    let dropped: Vec<String> = names(&on)
        .into_iter()
        .filter(|n| !names(&off).contains(n))
        .collect();
    assert_eq!(dropped, ["_event_id", "_wal_epoch", "_wal_pos"]);
}

#[test]
fn lineage_off_encoder_writes_the_reduced_batch() {
    let schema = Arc::new(
        DatasetSchema::builder("d")
            .field(FieldDef::new("v", FieldType::I32).with_field_id(1000))
            .options(ArrowOptions::default().with_lineage_columns(false))
            .build()
            .unwrap(),
    );
    let mut enc = BatchEncoder::new(schema, 4);
    enc.push(&event(&rec_shape(), vec![Value::I32(1)])).unwrap();
    let batch = enc.finish();
    assert!(batch.column_by_name("_event_id").is_none());
    assert!(batch.column_by_name("_wal_pos").is_none());
    assert_eq!(column(&batch, "v"), vec![Cell::Int(1)]);
}

#[test]
fn envelope_values_round_trip() {
    let mut t = Times::new(Timestamp::from_nanos(INGEST_NANOS), TimeDomain::Utc)
        .with_source(Timestamp::from_nanos(1_700_000_000_000_000_999))
        .unwrap();
    let mut q = Quality {
        status: QualityStatus::Uncertain,
        flags: QualityFlags::OUT_OF_RANGE | QualityFlags::STALE,
        reason: Some("battery sensor drifting".into()),
    };
    let event = Event::builder(
        EventId::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
        WalPos::new(42),
        t,
    )
    .payload(Record::new(rec_shape(), vec![Value::I32(1)]).unwrap())
    .quality(q.clone())
    .build()
    .unwrap();
    let mut enc = encoder(one_field(), ArrowOptions::default());
    enc.set_context(BatchContext {
        epoch: WalEpoch::new(0xdead_beef),
        clock_synced: true,
    });
    enc.push(&event).unwrap();

    // A second event: no source time, TAI domain, a correction, bad quality.
    t = Times::new(Timestamp::from_nanos(INGEST_NANOS + 1_000), TimeDomain::Tai);
    t.correct(
        Timestamp::from_nanos(5_000_000),
        &mut q,
        QualityFlags::CLOCK_SKEW,
    )
    .unwrap();
    q.status = QualityStatus::Bad;
    q.reason = None;
    let second = Event::builder(EventId::from_u128(2), WalPos::new(43), t)
        .payload(Record::new(rec_shape(), vec![Value::I32(2)]).unwrap())
        .quality(q)
        .build()
        .unwrap();
    enc.push(&second).unwrap();

    let b = enc.finish();
    assert_eq!(b.num_rows(), 2);
    assert_eq!(
        column(&b, "_event_id"),
        vec![
            Cell::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            Cell::Bytes(EventId::from_u128(2).to_bytes().to_vec()),
        ]
    );
    assert_eq!(
        column(&b, "_ingest_time"),
        vec![
            Cell::Ts(INGEST_NANOS / 1000),
            Cell::Ts((INGEST_NANOS + 1_000) / 1000)
        ]
    );
    assert_eq!(
        column(&b, "_source_time"),
        vec![Cell::Ts(1_700_000_000_000_000), Cell::Null],
        "source time floors to microseconds; the second event has none"
    );
    assert_eq!(
        column(&b, "_event_time"),
        vec![Cell::Ts(1_700_000_000_000_000), Cell::Ts(5_000)],
        "UTC source time, then the correction"
    );
    assert_eq!(
        column(&b, "_time_domain"),
        vec![Cell::Str("utc".into()), Cell::Str("tai".into())]
    );
    assert_eq!(
        column(&b, "_quality_status"),
        vec![Cell::Str("uncertain".into()), Cell::Str("bad".into())]
    );
    let flags = (QualityFlags::OUT_OF_RANGE | QualityFlags::STALE).bits();
    assert_eq!(
        column(&b, "_quality_flags"),
        vec![
            Cell::Int(i128::from(flags)),
            Cell::Int(i128::from(flags | QualityFlags::CLOCK_SKEW.bits()))
        ]
    );
    assert_eq!(
        column(&b, "_quality_reason"),
        vec![Cell::Str("battery sensor drifting".into()), Cell::Null]
    );
    assert_eq!(
        column(&b, "_clock_synced"),
        vec![Cell::Bool(true), Cell::Bool(true)]
    );
    assert_eq!(
        column(&b, "_wal_epoch"),
        vec![Cell::Int(0xdead_beef), Cell::Int(0xdead_beef)]
    );
    assert_eq!(column(&b, "_wal_pos"), vec![Cell::Int(42), Cell::Int(43)]);
}

#[test]
fn event_time_is_corrected_else_utc_source_else_null() {
    let src = Timestamp::from_nanos(9_000_000);
    let cases: Vec<(Times, Quality, Cell)> = vec![
        (
            Times::new(Timestamp::EPOCH, TimeDomain::Utc)
                .with_source(src)
                .unwrap(),
            Quality::good(),
            Cell::Ts(9_000),
        ),
        (
            Times::new(Timestamp::EPOCH, TimeDomain::Monotonic)
                .with_source(src)
                .unwrap(),
            Quality::good(),
            Cell::Null,
        ),
        (
            Times::new(Timestamp::EPOCH, TimeDomain::Sim)
                .with_source(src)
                .unwrap(),
            Quality::good(),
            Cell::Null,
        ),
        (
            Times::new(Timestamp::EPOCH, TimeDomain::Utc),
            Quality::good(),
            Cell::Null,
        ),
    ];
    let mut enc = encoder(one_field(), ArrowOptions::default());
    let mut expected = Vec::new();
    for (t, q, e) in cases {
        enc.push(&ev(t, q)).unwrap();
        expected.push(e);
    }
    let corrected = {
        let mut q = Quality::good();
        let mut t = Times::new(Timestamp::EPOCH, TimeDomain::Monotonic)
            .with_source(src)
            .unwrap();
        t.correct(
            Timestamp::from_nanos(77_000),
            &mut q,
            QualityFlags::CLOCK_SKEW,
        )
        .unwrap();
        ev(t, q)
    };
    enc.push(&corrected).unwrap();
    expected.push(Cell::Ts(77));
    assert_eq!(column(&enc.finish(), "_event_time"), expected);
}

#[test]
fn every_time_domain_has_a_dictionary_entry() {
    let mut enc = encoder(one_field(), ArrowOptions::default());
    for d in TimeDomain::ALL {
        assert_eq!(
            d as usize,
            TimeDomain::ALL.iter().position(|x| *x == d).unwrap()
        );
        enc.push(&ev(Times::new(Timestamp::EPOCH, d), Quality::good()))
            .unwrap();
    }
    let got = column(&enc.finish(), "_time_domain");
    let want: Vec<Cell> = TimeDomain::ALL
        .iter()
        .map(|d| Cell::Str(d.as_str().to_owned()))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn micros_and_nanos_columns() {
    let ts = Timestamp::from_nanos(1_700_000_000_123_456_789);
    let fields = vec![FieldDef::new("t", FieldType::Ts)];
    let sh = shape(fields.clone());
    for (unit, divisor, dt) in [
        (TsUnit::Micros, 1_000, TimeUnit::Microsecond),
        (TsUnit::Nanos, 1, TimeUnit::Nanosecond),
    ] {
        let mut enc = encoder(fields.clone(), ArrowOptions::default().with_ts_unit(unit));
        enc.push(&event(&sh, vec![Value::Ts(ts)])).unwrap();
        let batch = enc.finish();
        for name in ["_ingest_time", "t"] {
            assert_eq!(
                batch.schema().field_with_name(name).unwrap().data_type(),
                &DataType::Timestamp(dt, Some("UTC".into())),
                "{name}"
            );
        }
        assert_eq!(
            column(&batch, "t"),
            vec![Cell::Ts(1_700_000_000_123_456_789 / divisor)]
        );
        assert_eq!(
            column(&batch, "_ingest_time"),
            vec![Cell::Ts(INGEST_NANOS / divisor)]
        );
    }
}

#[test]
fn microsecond_conversion_floors_and_never_overflows() {
    let fields = vec![FieldDef::new("t", FieldType::Ts)];
    let sh = shape(fields.clone());
    let mut micros = encoder(fields.clone(), ArrowOptions::default());
    let mut nanos = encoder(fields, ArrowOptions::default().with_ts_unit(TsUnit::Nanos));
    let values = [
        Timestamp::from_nanos(-1),
        Timestamp::from_nanos(-1_000),
        Timestamp::from_nanos(-1_001),
        Timestamp::MIN,
        Timestamp::MAX,
    ];
    for t in values {
        micros.push(&event(&sh, vec![Value::Ts(t)])).unwrap();
        nanos.push(&event(&sh, vec![Value::Ts(t)])).unwrap();
    }
    assert_eq!(
        column(&micros.finish(), "t"),
        values
            .iter()
            .map(|t| Cell::Ts(t.as_nanos().div_euclid(1000)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        column(&nanos.finish(), "t"),
        values
            .iter()
            .map(|t| Cell::Ts(t.as_nanos()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn identity_levels_are_dictionary_columns_with_nulls_for_missing_levels() {
    let schema = Arc::new(
        DatasetSchema::builder("d")
            .level("site", 100)
            .level("robot", 101)
            .field(FieldDef::new("v", FieldType::I32).with_field_id(1000))
            .build()
            .unwrap(),
    );
    let mut enc = BatchEncoder::new(schema, 8);
    let sh = rec_shape();
    let paths = [
        AssetPath::new([("site", "a"), ("robot", "r1")]).unwrap(),
        // Other order, and an undeclared level that is ignored.
        AssetPath::new([("robot", "r2"), ("zone", "z"), ("site", "b")]).unwrap(),
        AssetPath::new([("robot", "r3")]).unwrap(),
        AssetPath::empty(),
        AssetPath::new([("site", "a"), ("robot", "r1")]).unwrap(),
    ];
    for p in paths {
        enc.push(&event_with(
            &sh,
            vec![Value::I32(1)],
            times(),
            Quality::good(),
            p,
        ))
        .unwrap();
    }
    let b = enc.finish();
    let s = |x: &str| Cell::Str(x.to_owned());
    assert_eq!(
        column(&b, "site"),
        vec![s("a"), s("b"), Cell::Null, Cell::Null, s("a")]
    );
    assert_eq!(
        column(&b, "robot"),
        vec![s("r1"), s("r2"), s("r3"), Cell::Null, s("r1")]
    );
    assert!(b.column_by_name("zone").is_none());
}

#[test]
fn sample_idx_follows_the_event_and_checks_its_range() {
    let schema = Arc::new(
        DatasetSchema::builder("d")
            .exploded(true)
            .field(FieldDef::new("v", FieldType::I32).with_field_id(1000))
            .build()
            .unwrap(),
    );
    let mut enc = BatchEncoder::new(schema, 8);
    let mk = |idx: Option<u32>| {
        let mut e = event(&rec_shape(), vec![Value::I32(1)]);
        e.set_sample_idx(idx);
        e
    };
    enc.push(&mk(Some(0))).unwrap();
    enc.push(&mk(Some(65_535))).unwrap();
    enc.push(&mk(None)).unwrap();
    let reject = enc.push(&mk(Some(65_536))).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_NARROWING);
    assert_eq!(reject.error.context.get("field"), Some("sample_idx"));
    assert_eq!(
        column(&enc.finish(), "sample_idx"),
        vec![Cell::Int(0), Cell::Int(65_535), Cell::Null]
    );
}

#[test]
fn push_batch_sets_the_context_and_numbers_rejects_by_batch_position() {
    let mut enc = encoder(
        vec![FieldDef::new("v", FieldType::I32).required()],
        ArrowOptions::default(),
    );
    let sh = rec_shape();
    let good = |n: i32, pos: u64| {
        Event::builder(EventId::from_u128(1), WalPos::new(pos), times())
            .payload(Record::new(sh.clone(), vec![Value::I32(n)]).unwrap())
            .build()
            .unwrap()
    };
    let bad = Event::builder(EventId::from_u128(9), WalPos::new(11), times())
        .payload(Record::new(sh.clone(), vec![Value::Null]).unwrap())
        .build()
        .unwrap();
    let raw = Event::builder(EventId::from_u128(10), WalPos::new(12), times())
        .payload(Payload::raw("text/plain", Bytes::from_static(b"x")))
        .build()
        .unwrap();
    let origin = WalRange::new(WalEpoch::new(5), WalPos::new(10), WalPos::new(13)).unwrap();
    let batch = EventBatch::new(vec![good(1, 10), bad, raw, good(2, 13)], origin, true);

    let rejects = enc.push_batch(&batch);
    assert_eq!(rejects.iter().map(|r| r.index).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(rejects[0].error.code, codes::SCHEMA_MISSING_FIELD);
    assert_eq!(rejects[1].error.code, codes::SCHEMA_TYPE_MISMATCH);
    assert_eq!(
        enc.context(),
        BatchContext {
            epoch: WalEpoch::new(5),
            clock_synced: true
        }
    );

    let b = enc.finish();
    assert_eq!(column(&b, "v"), vec![Cell::Int(1), Cell::Int(2)]);
    assert_eq!(column(&b, "_wal_pos"), vec![Cell::Int(10), Cell::Int(13)]);
    assert_eq!(column(&b, "_wal_epoch"), vec![Cell::Int(5), Cell::Int(5)]);
    assert_eq!(
        column(&b, "_clock_synced"),
        vec![Cell::Bool(true), Cell::Bool(true)]
    );
    assert_eq!(
        enc.context().epoch,
        WalEpoch::new(5),
        "finish keeps the context"
    );
}

#[test]
fn default_context_is_epoch_zero_and_unsynced() {
    let mut enc = encoder(one_field(), ArrowOptions::default());
    enc.push(&event(&rec_shape(), vec![Value::I32(1)])).unwrap();
    let b = enc.finish();
    assert_eq!(column(&b, "_wal_epoch"), vec![Cell::Int(0)]);
    assert_eq!(column(&b, "_clock_synced"), vec![Cell::Bool(false)]);
}

#[test]
fn undecoded_payloads_are_rejected_not_encoded() {
    let mut enc = encoder(one_field(), ArrowOptions::default());
    let raw = Event::builder(EventId::from_u128(1), WalPos::new(1), times())
        .payload(Payload::raw("application/json", Bytes::from_static(b"{}")))
        .build()
        .unwrap();
    let reject = enc.push(&raw).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_TYPE_MISMATCH);
    assert!(enc.is_empty());
}

#[test]
fn envelope_unsigned_columns_follow_the_policy() {
    let schema = DatasetSchema::builder("d")
        .exploded(true)
        .options(ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed))
        .build()
        .unwrap();
    let arrow = schema.arrow_schema();
    let ty = |n: &str| arrow.field_with_name(n).unwrap().data_type().clone();
    assert_eq!(ty(columns::QUALITY_FLAGS.name), DataType::Int64);
    assert_eq!(ty(columns::WAL_EPOCH.name), DataType::Int64);
    assert_eq!(ty(columns::WAL_POS.name), DataType::Int64);
    assert_eq!(ty(columns::SAMPLE_IDX.name), DataType::Int32);
}
