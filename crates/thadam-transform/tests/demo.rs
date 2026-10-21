//! End-to-end behavior of a compiled chain on the edge-capture demo datasets.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::*;
use serde_json::json;
use thadam_core::{
    ErrorDomain, QualityFlags, QualityStatus, TimeDomain, Timestamp, Value, WalPos, codes,
};
use thadam_transform::{Chain, ChainLimits, ChainSpec};

fn record(e: &thadam_core::Event) -> &thadam_core::Record {
    e.payload().as_record().unwrap()
}

#[test]
fn telemetry_decodes_converts_units_and_stamps_the_envelope() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    f.push(message(
        &topic("robot-01", "telemetry"),
        telemetry_payload(918_273, 1_791_369_600_123_456),
        1_791_369_600_200_000,
    ));
    let r = run(&mut chain, &f);
    assert!(r.rejects.is_empty(), "{:?}", r.rejects);

    let events = r.events("telemetry");
    assert_eq!(events.len(), 1);
    let e = &events[0];
    let rec = record(e);
    assert_eq!(rec.get("seq"), Some(&Value::U64(918_273)));
    assert_eq!(rec.get("soc_pct"), Some(&Value::F32(81.5)));
    assert_eq!(rec.get("voltage_v"), Some(&Value::F32(47.91)));
    assert_eq!(rec.get("current_a"), Some(&Value::F32(-3.12)));
    assert_eq!(rec.get("cpu_temp_c"), Some(&Value::F32(61.2)));
    assert_eq!(
        rec.get("motor_temp_c").unwrap().as_f32s(),
        Some(&[41.2f32, 39.8, 45.5, 43.0, 40.1, 38.8, 37.7][..])
    );
    assert!(rec.validate().is_ok());

    assert_eq!(
        e.asset().iter().collect::<Vec<_>>(),
        [("site", "site-a"), ("robot", "robot-01")]
    );
    assert_eq!(e.times().domain(), TimeDomain::Utc);
    assert_eq!(
        e.times().source().unwrap().as_micros(),
        1_791_369_600_123_456
    );
    assert_eq!(e.times().event_time(), e.times().source());
    assert_eq!(e.times().ingest().as_micros(), 1_791_369_600_200_000);
    assert_eq!(e.quality().status, QualityStatus::Good);
    assert!(e.quality().flags.is_empty());
    assert_eq!(e.wal_pos(), WalPos::new(0));
    assert_eq!(e.sample_idx(), None);
}

#[test]
fn declared_units_and_types_are_in_the_output_shape() {
    let chain = demo_chain();
    let info = &chain.datasets()[0];
    assert_eq!(&*info.id, "telemetry");
    let fields = info.shape.fields();
    let unit = |n: &str| {
        info.shape
            .index_of(n)
            .and_then(|i| info.shape.field(i))
            .and_then(|d| d.unit.clone())
    };
    assert_eq!(unit("voltage_v").as_deref(), Some("V"));
    assert_eq!(unit("current_a").as_deref(), Some("A"));
    assert_eq!(unit("cpu_temp_c").as_deref(), Some("Cel"));
    assert_eq!(unit("motor_temp_c").as_deref(), Some("Cel"));
    assert_eq!(unit("soc_pct"), None);
    assert!(!fields.iter().find(|d| &*d.name == "seq").unwrap().nullable);
    assert_eq!(info.time_domain, TimeDomain::Utc);
    assert_eq!(
        chain.datasets()[1].index_field.as_deref(),
        Some("sample_idx")
    );
}

#[test]
fn joints_explode_with_parent_paths_and_per_sample_time() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    f.push(message(
        &topic("robot-02", "joints"),
        joints_payload(55_120, 1_791_369_600_000_000, 10),
        1_791_369_600_120_000,
    ));
    let r = run(&mut chain, &f);
    assert!(r.rejects.is_empty(), "{:?}", r.rejects);

    let events = r.events("joints");
    assert_eq!(events.len(), 10);
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e.sample_idx(), Some(i as u32));
        assert_eq!(
            record(e).get("seq"),
            Some(&Value::U64(55_120)),
            "^/seq reads the parent"
        );
        assert_eq!(
            e.times().source().unwrap().as_micros(),
            1_791_369_600_000_000 + 10_000 * i as i64
        );
        assert_eq!(
            record(e)
                .get("position_rad")
                .unwrap()
                .as_f64s()
                .unwrap()
                .len(),
            7
        );
    }
    let mut ids: Vec<_> = events.iter().map(|e| e.id()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10, "ids are distinct per sample");
    let stats = chain.stats();
    let joints = &stats.datasets[1].1;
    assert_eq!(
        (joints.messages, joints.exploded, joints.written),
        (1, 10, 10)
    );
}

#[test]
fn a_bad_sample_is_rejected_alone_and_the_rest_proceed() {
    let mut chain = demo_chain();
    let payload = r#"{"v":1,"seq":9,"samples":[
        {"ts_us":1000000,"pos":[1],"vel":[1],"eff":[1]},
        {"ts_us":1000001,"pos":"nope","vel":[1],"eff":[1]},
        {"ts_us":1000002,"pos":[3],"vel":[3],"eff":[3]}]}"#;
    let mut f = Frames::new();
    f.push(message(&topic("r", "joints"), payload, 1_000_500));
    let r = run(&mut chain, &f);
    assert_eq!(r.events("joints").len(), 2);
    assert_eq!(r.rejects.len(), 1);
    let rej = &r.rejects[0];
    assert_eq!(rej.sample_idx, Some(1));
    assert_eq!(rej.error.code, codes::SCHEMA_TYPE_MISMATCH);
    assert_eq!(rej.payload.as_ref(), payload.as_bytes());
    assert_eq!(rej.dataset.as_deref(), Some("joints"));
}

#[test]
fn redelivery_is_deduplicated_but_distinct_keys_pass() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    let t = topic("robot-01", "telemetry");
    f.push(message(&t, telemetry_payload(1, 1_000_000), 1_000_100));
    f.push(message(&t, telemetry_payload(1, 1_000_000), 1_900_100));
    f.push(message(&t, telemetry_payload(2, 1_100_000), 1_100_100));
    f.push(message(
        &topic("robot-02", "telemetry"),
        telemetry_payload(1, 1_000_000),
        1_000_200,
    ));
    let r = run(&mut chain, &f);
    assert_eq!(r.events("telemetry").len(), 3);
    let s = &chain.stats().datasets[0].1;
    assert_eq!((s.messages, s.deduped, s.written), (4, 1, 3));
}

#[test]
fn dedup_forgets_after_the_window() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    let t = topic("robot-01", "telemetry");
    f.push(message(&t, telemetry_payload(1, 1_000_000), 1_000_000));
    f.push(message(
        &t,
        telemetry_payload(1, 1_000_000),
        1_000_000 + 601_000_000,
    ));
    assert_eq!(run(&mut chain, &f).events("telemetry").len(), 2);
}

#[test]
fn content_ids_survive_redelivery_and_positional_ids_do_not() {
    let spec = |with_key: bool| {
        let mut ds = events_dataset();
        if !with_key {
            ds.as_object_mut().unwrap().remove("event_id");
        }
        ChainSpec::from_value(json!({ "datasets": [ds] })).unwrap()
    };
    let payload = r#"{"seq":5,"level":"info","ts_us":1}"#;
    let mut frames = Frames::new();
    frames.push(message(&topic("r", "events"), payload, 10));
    frames.push(message(&topic("r", "events"), payload, 20));

    let mut keyed = Chain::compile(&spec(true), ChainLimits::default()).unwrap();
    let ev = run(&mut keyed, &frames).all_events();
    assert_eq!(ev[0].id(), ev[1].id());

    let mut positional = Chain::compile(&spec(false), ChainLimits::default()).unwrap();
    let ev = run(&mut positional, &frames).all_events();
    assert_ne!(ev[0].id(), ev[1].id());
    let mut again = Chain::compile(&spec(false), ChainLimits::default()).unwrap();
    assert_eq!(
        ev,
        run(&mut again, &frames).all_events(),
        "positional ids are stable across replay"
    );
}

#[test]
fn filter_drops_debug_events_and_counts_them() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    for (seq, level) in [(1, "info"), (2, "debug"), (3, "error")] {
        f.push(message(
            &topic("r", "events"),
            format!(r#"{{"seq":{seq},"level":"{level}","ts_us":{seq}}}"#),
            100,
        ));
    }
    let r = run(&mut chain, &f);
    let levels: Vec<_> = r
        .events("events")
        .iter()
        .map(|e| {
            record(e)
                .get("level")
                .and_then(Value::as_str)
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(levels, ["info", "error"]);
    assert_eq!(chain.stats().datasets[3].1.filtered, 1);
    assert!(r.rejects.is_empty());
}

#[test]
fn the_buggy_robot_filter_and_its_fix_replay_differently() {
    let build = |predicate: serde_json::Value| {
        Chain::compile(
            &ChainSpec::from_value(json!({
                "datasets": [events_dataset()],
                "transforms": [{ "kind": "filter", "dataset": "events", "where": predicate }],
            }))
            .unwrap(),
            ChainLimits::default(),
        )
        .unwrap()
    };
    let mut f = Frames::new();
    for (n, robot) in ["robot-01", "robot-03", "robot-04"].into_iter().enumerate() {
        f.push(message(
            &topic(robot, "events"),
            format!(r#"{{"seq":{n},"level":"info","ts_us":1}}"#),
            5,
        ));
    }
    let robots = |r: &Run| -> Vec<String> {
        r.events("events")
            .iter()
            .map(|e| e.asset().get("robot").unwrap().to_owned())
            .collect()
    };
    let buggy = run(
        &mut build(json!({ "field": "robot", "ne": "robot-03" })),
        &f,
    );
    assert_eq!(robots(&buggy), ["robot-01", "robot-04"]);
    let fixed = run(
        &mut build(json!({ "field": "robot", "ne": "robot-99" })),
        &f,
    );
    assert_eq!(robots(&fixed), ["robot-01", "robot-03", "robot-04"]);
}

#[test]
fn validate_flags_out_of_range_motor_temperatures() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    let hot = telemetry_payload(1, 1_000_000).replace(
        "[412,398,455,430,401,388,377]",
        "[412,398,1600,430,401,388,377]",
    );
    f.push(message(&topic("r", "telemetry"), hot, 1_000_100));
    f.push(message(
        &topic("r", "telemetry"),
        telemetry_payload(2, 1_000_000),
        1_000_100,
    ));
    let r = run(&mut chain, &f);
    let e = r.events("telemetry");
    assert_eq!(e[0].quality().flags, QualityFlags::OUT_OF_RANGE);
    assert_eq!(e[0].quality().status, QualityStatus::Bad);
    assert!(e[1].quality().flags.is_empty());
}

#[test]
fn clock_skew_and_missing_time_are_flagged_not_rejected() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    let t = topic("r", "telemetry");
    f.push(message(
        &t,
        telemetry_payload(1, 1_000_000),
        1_000_000 + 6_000_000,
    ));
    f.push(message(
        &t,
        telemetry_payload(2, 1_000_000).replace(r#""ts_us":1000000,"#, ""),
        1_000_000,
    ));
    f.push(message(
        &t,
        telemetry_payload(3, 1_000_000),
        1_000_000 + 4_000_000,
    ));
    let r = run(&mut chain, &f);
    assert!(r.rejects.is_empty());
    let e = r.events("telemetry");
    assert_eq!(e[0].quality().flags, QualityFlags::CLOCK_SKEW);
    assert_eq!(e[1].quality().flags, QualityFlags::MISSING_TIME);
    assert_eq!(e[1].times().source(), None);
    assert!(e[2].quality().flags.is_empty());
}

#[test]
fn state_reports_only_changes_and_heartbeats() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    let t = topic("r", "state");
    let state = |mode: &str, secs: i64| {
        message(
            &t,
            format!(r#"{{"ts_us":{secs},"mode":"{mode}","cycle_id":1,"program":"p"}}"#),
            secs * 1_000_000,
        )
    };
    for (mode, secs) in [
        ("IDLE", 0),
        ("IDLE", 10),
        ("RUNNING", 20),
        ("RUNNING", 30),
        ("RUNNING", 321),
    ] {
        f.push(state(mode, secs));
    }
    let r = run(&mut chain, &f);
    let modes: Vec<_> = r
        .events("state")
        .iter()
        .map(|e| {
            (
                record(e)
                    .get("mode")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_owned(),
                e.wal_pos().get(),
            )
        })
        .collect();
    assert_eq!(
        modes,
        [
            ("IDLE".to_owned(), 0),
            ("RUNNING".to_owned(), 2),
            ("RUNNING".to_owned(), 4)
        ],
        "first, change, and a heartbeat 5m after the last kept event"
    );
}

#[test]
fn u64_sequences_above_i64_max_survive() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    f.push(message(
        &topic("r", "events"),
        format!(r#"{{"seq":{},"level":"info","ts_us":1}}"#, u64::MAX),
        1,
    ));
    let r = run(&mut chain, &f);
    assert_eq!(
        record(&r.events("events")[0]).get("seq"),
        Some(&Value::U64(u64::MAX))
    );
}

#[test]
fn unmatched_records_are_counted_not_rejected() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    f.push(message("thadam-demo/v1/site-a/r/unknown", "{}", 1));
    f.push(message("other/topic", "{}", 1));
    let mut other_source = Frames::new();
    other_source.source = thadam_core::SourceId::new("elsewhere");
    other_source.push(message(
        &topic("r", "telemetry"),
        telemetry_payload(1, 1),
        1,
    ));
    let r = run(&mut chain, &f);
    let r2 = run(&mut chain, &other_source);
    assert!(r.rejects.is_empty() && r2.rejects.is_empty());
    assert_eq!(r.out.total_events() + r2.out.total_events(), 0);
    assert_eq!(chain.stats().unmatched, 3);
}

#[test]
fn bad_payloads_are_rejected_with_raw_bytes_and_stable_codes() {
    let mut chain = demo_chain();
    let t = topic("robot-01", "telemetry");
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (b"{".to_vec(), codes::DECODE_INVALID_PAYLOAD),
        (b"not json".to_vec(), codes::DECODE_INVALID_PAYLOAD),
        (vec![0xff, 0xfe, b'{'], codes::DECODE_INVALID_UTF8),
        (
            format!("{}1{}", "[".repeat(100), "]".repeat(100)).into_bytes(),
            codes::DECODE_DEPTH_EXCEEDED,
        ),
        (vec![b' '; 2 << 20], codes::DECODE_TOO_LARGE),
        (br#"{"ts_us":1}"#.to_vec(), codes::SCHEMA_MISSING_FIELD),
        (
            br#"{"seq":-1,"ts_us":1}"#.to_vec(),
            codes::SCHEMA_TYPE_MISMATCH,
        ),
        (
            br#"{"seq":1.5,"ts_us":1}"#.to_vec(),
            codes::SCHEMA_TYPE_MISMATCH,
        ),
        (
            br#"{"seq":11,"ts_us":"yesterday"}"#.to_vec(),
            codes::DECODE_INVALID_TIME,
        ),
        (
            br#"{"seq":12,"ts_us":1,"soc_pct":"high"}"#.to_vec(),
            codes::SCHEMA_TYPE_MISMATCH,
        ),
        (
            br#"{"seq":13,"ts_us":1,"motor_temp_dc":[1,null]}"#.to_vec(),
            codes::SCHEMA_TYPE_MISMATCH,
        ),
        (
            br#"{"seq":14,"ts_us":1,"motor_temp_dc":{"a":1}}"#.to_vec(),
            codes::SCHEMA_TYPE_MISMATCH,
        ),
        (b"[1,2,3]".to_vec(), codes::SCHEMA_MISSING_FIELD),
        (b"null".to_vec(), codes::SCHEMA_MISSING_FIELD),
    ];
    let mut f = Frames::new();
    for (payload, _) in &cases {
        f.push(message(&t, payload.clone(), 5));
    }
    let r = run(&mut chain, &f);
    assert_eq!(r.out.total_events(), 0);
    assert_eq!(r.rejects.len(), cases.len());
    for (rej, (payload, code)) in r.rejects.iter().zip(&cases) {
        assert_eq!(
            rej.error.code,
            *code,
            "{:?}",
            String::from_utf8_lossy(&payload[..payload.len().min(40)])
        );
        assert_eq!(
            rej.payload.as_ref(),
            payload.as_slice(),
            "raw bytes retained"
        );
        assert_eq!(rej.epoch.get(), EPOCH);
        assert!(matches!(
            rej.error.domain,
            ErrorDomain::Decode | ErrorDomain::Schema
        ));
        assert!(
            !rej.error.message.contains("yesterday"),
            "no payload text in errors"
        );
    }
    let s = &chain.stats().datasets[0].1;
    assert_eq!(s.rejected, cases.len() as u64);
}

#[test]
fn a_reject_does_not_stop_the_rest_of_the_batch() {
    let mut chain = demo_chain();
    let t = topic("r", "telemetry");
    let mut f = Frames::new();
    f.push(message(&t, telemetry_payload(1, 1_000_000), 1_000_000));
    f.push(message(&t, "garbage", 1_000_000));
    f.push(message(&t, telemetry_payload(2, 1_000_000), 1_000_000));
    let r = run(&mut chain, &f);
    assert_eq!(r.events("telemetry").len(), 2);
    assert_eq!(r.rejects.len(), 1);
    assert_eq!(r.rejects[0].pos, WalPos::new(1));
}

#[test]
fn unresolved_identity_is_a_reject() {
    let ds = json!({
        "id": "d", "match": { "topic": "#" },
        "identity": [{ "robot": "{topic.5}" }],
        "fields": []
    });
    let mut chain = Chain::compile(
        &ChainSpec::from_value(json!({ "datasets": [ds] })).unwrap(),
        ChainLimits::default(),
    )
    .unwrap();
    let mut f = Frames::new();
    f.push(message("a/b", "{}", 1));
    let r = run(&mut chain, &f);
    assert_eq!(r.rejects.len(), 1);
    assert_eq!(r.rejects[0].error.code, codes::SCHEMA_MISSING_FIELD);
}

#[test]
fn identity_can_come_from_the_payload() {
    let ds = json!({
        "id": "d", "match": { "topic": "#" },
        "identity": [{ "robot": "{payload:/robot/id}" }, { "src": "{source}" }],
        "fields": [{ "name": "x", "type": "i64" }]
    });
    let mut chain = Chain::compile(
        &ChainSpec::from_value(json!({ "datasets": [ds] })).unwrap(),
        ChainLimits::default(),
    )
    .unwrap();
    let mut f = Frames::new();
    f.push(message("t", r#"{"robot":{"id":42},"x":1}"#, 1));
    let r = run(&mut chain, &f);
    let e = &r.events("d")[0];
    assert_eq!(
        e.asset().iter().collect::<Vec<_>>(),
        [("robot", "42"), ("src", SOURCE)]
    );
}

#[test]
fn rfc3339_times_and_epoch_floats() {
    let ds = |unit: &str| {
        json!({
            "id": "d", "match": { "topic": "#" },
            "time": { "field": "/t", "unit": unit, "domain": "utc", "default_offset": if unit == "rfc3339" { json!("+01:00") } else { json!(null) } },
            "fields": []
        })
    };
    let mut ds_rfc = ds("rfc3339");
    let mut ds_s = ds("s");
    ds_s["time"]
        .as_object_mut()
        .unwrap()
        .remove("default_offset");
    ds_rfc["id"] = json!("rfc");
    ds_s["id"] = json!("secs");
    let mut chain = Chain::compile(
        &ChainSpec::from_value(json!({ "datasets": [ds_rfc, ds_s] })).unwrap(),
        ChainLimits::default(),
    )
    .unwrap();
    let mut f = Frames::new();
    f.push(message("t", r#"{"t":"2026-10-07T12:00:00"}"#, 1));
    f.push(message("t", r#"{"t":1791374400.5}"#, 1));
    let r = run(&mut chain, &f);
    assert_eq!(
        r.rejects.len(),
        2,
        "each message fits one dataset's time format only"
    );
    let rfc = r.events("rfc")[0].times().source().unwrap();
    assert_eq!(rfc, Timestamp::from_secs(1_791_370_800).unwrap());
    let secs = r.events("secs")[0].times().source().unwrap();
    assert_eq!(secs.as_nanos(), 1_791_374_400_500_000_000);
}

#[test]
fn compile_reports_every_problem_with_config_paths() {
    let spec = ChainSpec::from_value(json!({
        "datasets": [
            { "id": "a", "match": { "topic": "a/#/b" },
              "dedup": { "window": "1m", "max_keys": 10 },
              "fields": [{ "name": "x", "type": "decimal" }, { "name": "y", "type": "f32", "from": "^/y" }] },
            { "id": "a", "fields": [] }
        ],
        "transforms": [
            { "kind": "filter", "dataset": "nope", "where": { "field": "x", "eq": 1 } },
            { "kind": "filter", "dataset": "a", "expr": "x == 1" },
            { "kind": "cast", "dataset": "a", "fields": { "y": "i8" } }
        ]
    }))
    .unwrap();
    let errs = Chain::compile(&spec, ChainLimits::default()).unwrap_err();
    let paths: Vec<_> = errs.iter().filter_map(|e| e.context.get("path")).collect();
    for expected in [
        "datasets[0].match.topic",
        "datasets[0].dedup",
        "datasets[0].fields[0].type",
        "datasets[0].fields[1].from",
        "datasets[1].id",
        "transforms[0].dataset",
    ] {
        assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
    }
    assert!(errs.iter().all(|e| e.domain != ErrorDomain::Connector));
}

#[test]
fn dedup_without_a_content_key_is_refused() {
    let mut ds = telemetry_dataset();
    ds.as_object_mut().unwrap().remove("event_id");
    let spec = ChainSpec::from_value(json!({ "datasets": [ds] })).unwrap();
    let errs = Chain::compile(&spec, ChainLimits::default()).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("event_id.key")
        && e.context.get("path") == Some("datasets[0].dedup")));
}

#[test]
fn key_fields_of_exploded_datasets_must_read_the_parent() {
    let mut ds = joints_dataset();
    ds["fields"][0]["from"] = json!("/seq");
    let spec = ChainSpec::from_value(json!({ "datasets": [ds] })).unwrap();
    let errs = Chain::compile(&spec, ChainLimits::default()).unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("parent")));
}

#[test]
fn unit_errors_are_transform_errors() {
    let ds = json!({ "id": "d", "match": {}, "fields": [
        { "name": "a", "type": "f32", "unit": "mV", "convert_to": "Cel" },
        { "name": "b", "type": "f32", "unit": "furlong", "convert_to": "m" },
        { "name": "c", "type": "i32", "unit": "mV", "convert_to": "V" },
        { "name": "d", "type": "f32", "convert_to": "V" },
    ]});
    let errs = Chain::compile(
        &ChainSpec::from_value(json!({ "datasets": [ds] })).unwrap(),
        ChainLimits::default(),
    )
    .unwrap_err();
    assert_eq!(errs.len(), 4);
    assert_eq!(errs[0].code, codes::TRANSFORM_UNKNOWN_UNIT);
    assert_eq!(errs[1].code, codes::TRANSFORM_UNKNOWN_UNIT);
    assert_eq!(errs[2].code, codes::SCHEMA_UNSUPPORTED_TYPE);
}

#[test]
fn limits_bound_chain_size() {
    let limits = ChainLimits::default().with_max_datasets(1);
    let spec =
        ChainSpec::from_value(json!({ "datasets": [telemetry_dataset(), joints_dataset()] }))
            .unwrap();
    assert!(Chain::compile(&spec, limits).is_err());

    let limits = ChainLimits::default().with_max_shapes(1);
    let spec =
        ChainSpec::from_value(json!({ "datasets": [telemetry_dataset(), joints_dataset()] }))
            .unwrap();
    let errs = Chain::compile(&spec, limits).unwrap_err();
    assert_eq!(errs[0].code, codes::SCHEMA_SHAPE_LIMIT);

    let limits = ChainLimits::default().with_max_explode_samples(3);
    let mut chain = Chain::compile(&demo_spec(), limits).unwrap();
    let mut f = Frames::new();
    f.push(message(
        &topic("r", "joints"),
        joints_payload(1, 1_000_000, 4),
        1_000_000,
    ));
    let r = run(&mut chain, &f);
    assert_eq!(r.rejects[0].error.code, codes::DECODE_TOO_LARGE);
}

#[test]
fn enrichment_hash_is_exposed_for_lineage() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("robots.csv"),
        "robot_id,model\nrobot-01,A1\n",
    )
    .unwrap();
    let spec = ChainSpec::from_value(json!({
        "datasets": [telemetry_dataset()],
        "transforms": [{ "kind": "enrich", "dataset": "telemetry", "file": "robots.csv",
                         "key": { "field": "robot", "column": "robot_id" },
                         "add": [{ "name": "model", "type": "str" }] }]
    }))
    .unwrap()
    .with_base_dir(dir.path());
    let mut chain = Chain::compile(&spec, ChainLimits::default()).unwrap();
    let info = &chain.datasets()[0].enrichment[0];
    assert_eq!(info.file, "robots.csv");
    assert_eq!(info.rows, 1);
    assert_eq!(info.sha256.len(), 64);

    let mut f = Frames::new();
    f.push(message(
        &topic("robot-01", "telemetry"),
        telemetry_payload(1, 1_000_000),
        1_000_000,
    ));
    let r = run(&mut chain, &f);
    assert_eq!(
        record(&r.events("telemetry")[0])
            .get("model")
            .and_then(Value::as_str),
        Some("A1")
    );
}

/// A chain's enrichment hash covers the tables it loaded: empty without any,
/// stable for the same content, and different when a table's content changes.
#[test]
fn chain_enrichment_hash_covers_the_tables_it_loaded() {
    let hash_of = |csv: &str| {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("robots.csv"), csv).unwrap();
        let spec = ChainSpec::from_value(json!({
            "datasets": [telemetry_dataset()],
            "transforms": [{ "kind": "enrich", "dataset": "telemetry", "file": "robots.csv",
                             "key": { "field": "robot", "column": "robot_id" },
                             "add": [{ "name": "model", "type": "str" }] }]
        }))
        .unwrap()
        .with_base_dir(dir.path());
        Chain::compile(&spec, ChainLimits::default())
            .unwrap()
            .enrichment_hash()
    };
    let a = hash_of("robot_id,model\nrobot-01,A1\n");
    assert_eq!(a.len(), 64);
    assert_eq!(a, hash_of("robot_id,model\nrobot-01,A1\n"));
    assert_ne!(a, hash_of("robot_id,model\nrobot-01,B2\n"));

    let plain = ChainSpec::from_value(json!({ "datasets": [telemetry_dataset()] })).unwrap();
    assert_eq!(
        Chain::compile(&plain, ChainLimits::default())
            .unwrap()
            .enrichment_hash(),
        ""
    );
}

#[test]
fn chain_output_carries_the_origin_for_every_dataset() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    f.push(message(
        &topic("r", "state"),
        r#"{"mode":"A","ts_us":1}"#,
        1,
    ));
    f.push(message(
        &topic("r", "state"),
        r#"{"mode":"A","ts_us":2}"#,
        2,
    ));
    let r = run(&mut chain, &f);
    for b in &r.out.batches {
        assert_eq!(b.batch.origin.first(), WalPos::new(0));
        assert_eq!(b.batch.origin.last(), WalPos::new(1));
        assert!(b.batch.clock_synced);
    }
}

#[test]
fn raw_rejects_convert_to_core_rejects() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    f.push(message(&topic("r", "telemetry"), "oops", 1));
    let r = run(&mut chain, &f);
    let core = r.rejects[0].to_reject(4);
    assert_eq!(core.index, 4);
    assert_eq!(core.error, r.rejects[0].error);
}

#[allow(dead_code)]
fn auto_traits() {
    fn assert_send<T: Send>() {}
    assert_send::<Chain>();

    trait AmbiguousIfSync<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
    // Fails to compile (ambiguous impls) if `Chain` ever becomes `Sync`.
    let _ = <Chain as AmbiguousIfSync<_>>::some_item;
}

#[test]
fn dedup_keys_are_capped_by_the_chain_limit_and_evictions_are_counted() {
    let limits = ChainLimits::default().with_max_dedup_keys(4);
    let mut chain = Chain::compile(&demo_spec(), limits).unwrap();
    let mut f = Frames::new();
    let t = topic("r", "telemetry");
    for seq in 0..10 {
        f.push(message(
            &t,
            telemetry_payload(seq, 1_000_000),
            1_000_000 + seq as i64,
        ));
    }
    let r = run(&mut chain, &f);
    assert_eq!(
        r.events("telemetry").len(),
        10,
        "distinct keys are never dropped"
    );
    assert_eq!(chain.stats().datasets[0].1.dedup_evictions, 6);

    let mut again = Frames::new();
    again.push(message(&t, telemetry_payload(0, 1_000_000), 1_000_100));
    again.push(message(&t, telemetry_payload(9, 1_000_000), 1_000_101));
    let r = run(&mut chain, &again);
    assert_eq!(
        r.events("telemetry").len(),
        1,
        "an evicted key passes, a remembered one does not"
    );
}

#[test]
fn warming_does_not_count_as_processing() {
    let mut chain = demo_chain();
    let mut f = Frames::new();
    for seq in 0..5 {
        f.push(message(
            &topic("r", "telemetry"),
            telemetry_payload(seq, 1_000_000),
            1_000_000,
        ));
    }
    f.push(message(&topic("r", "nothing"), "{}", 1));
    f.push(message(&topic("r", "telemetry"), "bad", 1));
    warm(&mut chain, &f);
    let stats = chain.stats();
    assert_eq!(stats.unmatched, 0);
    assert!(stats.datasets.iter().all(|(_, s)| *s == Default::default()));

    let r = run(&mut chain, &f.slice(0..2));
    assert_eq!(r.out.total_events(), 0, "the warmed keys are remembered");
    assert_eq!(chain.stats().datasets[0].1.deduped, 2);
}

#[test]
fn topic_keyed_identity_is_cached_but_bounded() {
    let limits = ChainLimits::default().with_max_state_keys(2);
    let mut chain = Chain::compile(&demo_spec(), limits).unwrap();
    let mut f = Frames::new();
    for round in 0..3 {
        for robot in ["a", "b", "c", "d"] {
            f.push(message(
                &topic(robot, "state"),
                format!(r#"{{"ts_us":{round},"mode":"M{round}"}}"#),
                1_000 + round,
            ));
        }
    }
    let r = run(&mut chain, &f);
    assert!(r.rejects.is_empty());
    let robots: Vec<_> = r
        .events("state")
        .iter()
        .map(|e| e.asset().get("robot").unwrap().to_owned())
        .collect();
    assert_eq!(robots.len(), 12);
    assert_eq!(&robots[..4], ["a", "b", "c", "d"]);
    assert!(
        r.events("state")
            .iter()
            .all(|e| e.asset().get("site") == Some("site-a"))
    );
}

#[test]
fn state_evictions_are_reported() {
    let limits = ChainLimits::default().with_max_state_keys(2);
    let spec = ChainSpec::from_value(json!({
        "datasets": [state_dataset()],
        "transforms": [{ "kind": "on_change", "dataset": "state", "fields": ["mode"] }],
    }))
    .unwrap();
    let mut chain = Chain::compile(&spec, limits).unwrap();
    let mut f = Frames::new();
    for robot in ["a", "b", "c", "d", "e"] {
        f.push(message(
            &topic(robot, "state"),
            r#"{"ts_us":1,"mode":"M"}"#,
            1,
        ));
    }
    run(&mut chain, &f);
    assert_eq!(chain.stats().datasets[0].1.state_evictions, 3);
}
