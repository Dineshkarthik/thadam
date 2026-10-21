//! `decode: sparkplug_b` through a compiled chain: event shape, quality
//! mapping, birth-store seeding and write discipline, and failure surfacing.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;
use std::path::Path;

use common::*;
use prost::Message as _;
use serde_json::{Value as Json, json};
use thadam_core::{BirthStore, QualityFlags, QualityStatus, Value, codes};
use thadam_sparkplug::pb::{
    self, DataType as D,
    payload::{Metric, metric::Value as V},
};
use thadam_transform::{Chain, ChainLimits, ChainSpec};

fn spec(births: &Path) -> ChainSpec {
    ChainSpec::from_value(json!({ "datasets": [dataset(), samples_dataset()] }))
        .unwrap()
        .with_births_dir(births)
}

fn dataset() -> Json {
    json!({
        "id": "sp",
        "match": { "source": SOURCE, "topic": "spBv1.0/#" },
        "decode": "sparkplug_b",
        "identity": [{ "group": "{topic.1}" }, { "node": "{topic.3}" }],
        "time": { "field": "/timestamp", "unit": "ms", "domain": "utc" },
        "fields": [
            { "name": "kind", "type": "str" },
            { "name": "device", "type": "str" },
            { "name": "seq", "type": "u64" },
            { "name": "bd_seq", "type": "u64" },
            { "name": "temp", "type": "f32", "from": "/metrics/temp" },
            { "name": "speed", "type": "f64", "from": "/metrics/motor.speed" },
            { "name": "mode", "type": "str", "from": "/metrics/Line~1Mode" }
        ]
    })
}

/// Per-metric rows over the same messages.
fn samples_dataset() -> Json {
    json!({
        "id": "per_metric",
        "match": { "source": SOURCE, "topic": "spBv1.0/g/NDATA/e1" },
        "decode": "sparkplug_b",
        "identity": [{ "group": "{topic.1}" }, { "node": "{topic.3}" }],
        "explode": { "path": "/samples", "index_field": "n" },
        "fields": [
            { "name": "kind", "type": "str", "from": "^/kind" },
            { "name": "name", "type": "str" },
            { "name": "datatype", "type": "str" },
            { "name": "value", "type": "f64" }
        ]
    })
}

fn metric(name: Option<&str>, alias: Option<u64>, dt: D, value: Option<V>) -> Metric {
    Metric {
        name: name.map(Into::into),
        alias,
        datatype: Some(dt as u32),
        is_null: value.is_none().then_some(true),
        value,
        ..Default::default()
    }
}

fn payload(seq: Option<u64>, ts: u64, metrics: Vec<Metric>) -> Vec<u8> {
    pb::Payload {
        timestamp: Some(ts),
        seq,
        metrics,
        ..Default::default()
    }
    .encode_to_vec()
}

fn bd(n: u64) -> Metric {
    metric(Some("bdSeq"), None, D::Int64, Some(V::LongValue(n)))
}

fn nbirth(seq: u64, ts: u64) -> Vec<u8> {
    payload(
        Some(seq),
        ts,
        vec![
            bd(7),
            metric(Some("temp"), Some(1), D::Float, Some(V::FloatValue(1.0))),
            metric(
                Some("Line/Mode"),
                Some(2),
                D::String,
                Some(V::StringValue("run".into())),
            ),
        ],
    )
}

fn dbirth(seq: u64, ts: u64) -> Vec<u8> {
    payload(
        Some(seq),
        ts,
        vec![metric(
            Some("motor.speed"),
            Some(10),
            D::Double,
            Some(V::DoubleValue(0.0)),
        )],
    )
}

fn ndata(seq: u64, ts: u64, alias: u64, v: f32) -> Vec<u8> {
    payload(
        Some(seq),
        ts,
        vec![metric(None, Some(alias), D::Float, Some(V::FloatValue(v)))],
    )
}

fn ddata(seq: u64, ts: u64, v: f64) -> Vec<u8> {
    payload(
        Some(seq),
        ts,
        vec![metric(None, Some(10), D::Double, Some(V::DoubleValue(v)))],
    )
}

fn ndeath() -> Vec<u8> {
    payload(None, 0, vec![bd(7)])
}

fn frames(msgs: &[(&str, Vec<u8>)]) -> Frames {
    let mut f = Frames::new();
    for (i, (topic, bytes)) in msgs.iter().enumerate() {
        f.push(message(
            topic,
            bytes.clone(),
            1_700_000_000_000_000 + i as i64,
        ));
    }
    f
}

const NB: &str = "spBv1.0/g/NBIRTH/e1";
const ND: &str = "spBv1.0/g/NDATA/e1";
const DB: &str = "spBv1.0/g/DBIRTH/e1/dev1";
const DD: &str = "spBv1.0/g/DDATA/e1/dev1";
const NDEATH: &str = "spBv1.0/g/NDEATH/e1";
const TS: u64 = 1_700_000_000_000;

fn births_written(dir: &Path) -> usize {
    BirthStore::open(dir).load_all().unwrap().len()
}

#[test]
fn lifecycle_maps_to_events_quality_and_per_metric_rows() {
    let dir = tempfile::tempdir().unwrap();
    let births = dir.path().join("births");
    let mut chain = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    let r = run(
        &mut chain,
        &frames(&[
            (NB, nbirth(0, TS)),
            (DB, dbirth(1, TS + 1)),
            (ND, ndata(2, TS + 2, 1, 21.5)),
            (DD, ddata(3, TS + 3, 3.5)),
            (ND, ndata(5, TS + 4, 1, 22.5)),
            ("spBv1.0/g/STATE/host-1", br#"{"online":true}"#.to_vec()),
            ("spBv1.0/g/NCMD/e1", vec![0xff]),
            (NDEATH, ndeath()),
            (ND, ndata(9, TS + 5, 1, 23.5)),
        ]),
    );
    assert!(r.rejects.is_empty(), "{:?}", r.rejects);

    // One event per message, STATE and CMD dropped: 7 messages on `sp`.
    let events = r.events("sp");
    assert_eq!(events.len(), 7);
    let rec = |i: usize| events[i].payload().as_record().unwrap();
    let kind = |i: usize| rec(i).get("kind").cloned().unwrap();
    let text = |s: &str| Value::Str(s.into());

    // BIRTH is an event with its initial values.
    assert_eq!(kind(0), text("NBIRTH"));
    assert_eq!(rec(0).get("temp"), Some(&Value::F32(1.0)));
    assert_eq!(rec(0).get("mode"), Some(&text("run")));
    assert_eq!(rec(0).get("bd_seq"), Some(&Value::U64(7)));
    assert_eq!(rec(0).get("device"), Some(&Value::Null));
    assert_eq!(kind(1), text("DBIRTH"));
    assert_eq!(rec(1).get("device"), Some(&text("dev1")));
    assert_eq!(rec(1).get("speed"), Some(&Value::F64(0.0)));

    // Alias-only DATA resolves to the names the fields read.
    assert_eq!(rec(2).get("temp"), Some(&Value::F32(21.5)));
    assert_eq!(rec(3).get("speed"), Some(&Value::F64(3.5)));
    for i in [0, 1, 2, 3] {
        assert!(events[i].quality().flags.is_empty(), "event {i}");
        assert_eq!(events[i].quality().status, QualityStatus::Good);
    }
    assert_eq!(
        events[2].times().source().unwrap().as_millis(),
        (TS + 2) as i64
    );

    // seq 3 -> 5: one SEQUENCE_GAP event, status still Good.
    let gap = events[4].quality();
    assert_eq!(gap.flags, QualityFlags::SEQUENCE_GAP);
    assert_eq!(gap.status, QualityStatus::Good);
    assert_eq!(
        gap.reason.as_deref(),
        Some("sparkplug.seq expected 4 got 5")
    );
    assert_eq!(rec(4).get("temp"), Some(&Value::F32(22.5)));

    // NDEATH is an event with no metrics; data after it is STALE/Uncertain.
    assert_eq!(kind(5), text("NDEATH"));
    assert_eq!(rec(5).get("temp"), Some(&Value::Null));
    let stale = events[6].quality();
    assert_eq!(stale.flags, QualityFlags::STALE);
    assert_eq!(stale.status, QualityStatus::Uncertain);
    assert_eq!(stale.reason.as_deref(), Some("sparkplug.past-ndeath"));
    assert_eq!(rec(6).get("temp"), Some(&Value::F32(23.5)));

    // The identity comes from the topic.
    assert_eq!(
        events[0].asset().iter().collect::<Vec<_>>(),
        [("group", "g"), ("node", "e1")]
    );

    // The exploded dataset: one row per metric, the parent kind via `^/`.
    // It routes only NDATA, yet the chain decoder still saw every BIRTH
    // (the `sp` dataset matches them), so aliases resolve.
    let rows = r.events("per_metric");
    assert_eq!(rows.len(), 3);
    let row = |i: usize| rows[i].payload().as_record().unwrap();
    assert_eq!(row(0).get("kind"), Some(&text("NDATA")));
    assert_eq!(row(0).get("name"), Some(&text("temp")));
    assert_eq!(row(0).get("datatype"), Some(&text("FLOAT")));
    assert_eq!(row(0).get("value"), Some(&Value::F64(21.5)));
    assert_eq!(row(2).get("value"), Some(&Value::F64(23.5)));
    assert_eq!(rows[2].quality().flags, QualityFlags::STALE);

    // The births are in the store before the events were returned.
    assert_eq!(births_written(&births), 2);
}

#[test]
fn a_restarted_chain_resolves_data_from_the_birth_store() {
    let dir = tempfile::tempdir().unwrap();
    let births = dir.path().join("births");
    let mut first = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    let r = run(
        &mut first,
        &frames(&[(NB, nbirth(0, TS)), (DB, dbirth(1, TS))]),
    );
    assert!(r.rejects.is_empty());

    // A new chain over the same directory: DATA, no BIRTH anywhere.
    let mut second = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    let r = run(
        &mut second,
        &frames(&[(ND, ndata(40, TS, 1, 9.5)), (DD, ddata(41, TS, 2.5))]),
    );
    assert!(r.rejects.is_empty());
    let events = r.events("sp");
    assert_eq!(events.len(), 2);
    let rec = events[0].payload().as_record().unwrap();
    assert_eq!(rec.get("temp"), Some(&Value::F32(9.5)));
    assert_eq!(rec.get("bd_seq"), Some(&Value::U64(7)));
    assert!(events[0].quality().flags.is_empty(), "no false gap");
    let rec = events[1].payload().as_record().unwrap();
    assert_eq!(rec.get("speed"), Some(&Value::F64(2.5)));
    assert!(events[1].quality().flags.is_empty());

    // Without the store the same DATA flags its unknown aliases.
    let empty = dir.path().join("empty");
    let mut cold = Chain::compile(&spec(&empty), ChainLimits::default()).unwrap();
    let r = run(&mut cold, &frames(&[(ND, ndata(40, TS, 1, 9.5))]));
    assert_eq!(
        r.events("sp")[0].quality().flags,
        QualityFlags::DECODE_ERROR
    );
}

#[test]
fn death_is_persisted_so_a_restart_stays_stale() {
    let dir = tempfile::tempdir().unwrap();
    let births = dir.path().join("births");
    let mut first = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    run(
        &mut first,
        &frames(&[(NB, nbirth(0, TS)), (NDEATH, ndeath())]),
    );

    let mut second = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    let r = run(&mut second, &frames(&[(ND, ndata(3, TS, 1, 1.0))]));
    assert_eq!(r.events("sp")[0].quality().flags, QualityFlags::STALE);
}

#[test]
fn warm_up_and_read_only_chains_never_write_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let births = dir.path().join("births");
    let msgs = frames(&[(NB, nbirth(0, TS)), (DB, dbirth(1, TS))]);

    // Warm-up moves memory but not the store.
    let mut chain = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    warm(&mut chain, &msgs);
    assert!(!births.exists(), "warm wrote the birth store");
    let r = run(&mut chain, &frames(&[(ND, ndata(2, TS, 1, 1.0))]));
    assert_eq!(r.events("sp")[0].quality().flags, QualityFlags::empty());
    assert!(!births.exists());

    // A read-only chain (replay) processes BIRTHs and still writes nothing.
    let ro = spec(&births).with_births_read_only();
    let mut chain = Chain::compile(&ro, ChainLimits::default()).unwrap();
    let r = run(&mut chain, &msgs);
    assert!(r.rejects.is_empty());
    assert_eq!(r.events("sp").len(), 2);
    assert!(!births.exists(), "a read-only chain wrote the birth store");
}

#[test]
fn a_failed_store_write_rejects_the_message_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let births = dir.path().join("births");
    let mut chain = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    // The directory cannot be created: a file sits where it belongs.
    fs::write(&births, b"in the way").unwrap();

    let r = run(&mut chain, &frames(&[(NB, nbirth(0, TS))]));
    assert!(
        r.events("sp").is_empty(),
        "no event without a durable BIRTH"
    );
    assert_eq!(r.rejects.len(), 1);
    assert_eq!(r.rejects[0].error.code, codes::BUFFER_IO);
    assert_eq!(r.rejects[0].payload.as_ref(), nbirth(0, TS).as_slice());
}

#[test]
fn bad_input_is_rejected_without_payload_bytes_in_the_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut chain = Chain::compile(&spec(&dir.path().join("b")), ChainLimits::default()).unwrap();
    let secret = b"SECRETMARKER-9f3a".to_vec();
    let r = run(
        &mut chain,
        &frames(&[
            (ND, secret.clone()),
            ("spBv1.0/g/NDATA", nbirth(0, TS)),
            ("thadam/other/topic", nbirth(0, TS)),
        ]),
    );
    // The first fails protobuf decoding; the others are not routed here
    // (topic filter) so they are unmatched, not rejects.
    assert!(!r.rejects.is_empty());
    for reject in &r.rejects {
        let text = format!("{:?} {}", reject.error, reject.error.message);
        assert!(!text.contains("SECRETMARKER"), "{text}");
        assert_eq!(reject.error.code, codes::DECODE_INVALID_PAYLOAD);
    }
    assert!(r.events("sp").is_empty());
}

#[test]
fn compile_needs_the_birth_store_and_reports_a_damaged_one() {
    let spec_no_dir = ChainSpec::from_value(json!({ "datasets": [dataset()] })).unwrap();
    let errs = Chain::compile(&spec_no_dir, ChainLimits::default()).unwrap_err();
    assert_eq!(errs[0].code, codes::CONFIG_MISSING);
    assert_eq!(errs[0].context.get("path"), Some("datasets[0].decode"));

    let dir = tempfile::tempdir().unwrap();
    let births = dir.path().join("births");
    let mut chain = Chain::compile(&spec(&births), ChainLimits::default()).unwrap();
    run(&mut chain, &frames(&[(NB, nbirth(0, TS))]));
    for entry in fs::read_dir(&births).unwrap() {
        let path = entry.unwrap().path();
        let mut bytes = fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 1;
        fs::write(&path, bytes).unwrap();
    }
    let errs = Chain::compile(&spec(&births), ChainLimits::default()).unwrap_err();
    assert_eq!(errs[0].code, codes::BUFFER_CORRUPT);
}

#[test]
fn one_decoder_per_chain_sees_each_message_once() {
    // Two datasets match every message here; if each fed the decoder, the
    // seq cursor would see every seq twice and flag a gap.
    let dir = tempfile::tempdir().unwrap();
    let mut chain =
        Chain::compile(&spec(&dir.path().join("births")), ChainLimits::default()).unwrap();
    let r = run(
        &mut chain,
        &frames(&[
            (NB, nbirth(0, TS)),
            (ND, ndata(1, TS, 1, 1.0)),
            (ND, ndata(2, TS, 1, 1.0)),
        ]),
    );
    assert!(r.events("sp").iter().all(|e| e.quality().flags.is_empty()));
    assert!(
        r.events("per_metric")
            .iter()
            .all(|e| e.quality().flags.is_empty())
    );
}
