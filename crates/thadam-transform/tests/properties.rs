//! Property tests: widening, dedup, deadband, determinism, warm start, and
//! adversarial input.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::{HashMap, HashSet};

use common::*;
use proptest::prelude::*;
use serde_json::json;
use thadam_core::{Event, Value, ValueType, WalPos};
use thadam_transform::{Chain, ChainLimits, ChainSpec};

const TYPES: [&str; 10] = [
    "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64",
];

fn value_type(name: &str) -> ValueType {
    match name {
        "i8" => ValueType::I8,
        "i16" => ValueType::I16,
        "i32" => ValueType::I32,
        "i64" => ValueType::I64,
        "u8" => ValueType::U8,
        "u16" => ValueType::U16,
        "u32" => ValueType::U32,
        "u64" => ValueType::U64,
        "f32" => ValueType::F32,
        _ => ValueType::F64,
    }
}

fn int_bounds(name: &str) -> Option<(i128, i128)> {
    Some(match name {
        "i8" => (i8::MIN.into(), i8::MAX.into()),
        "i16" => (i16::MIN.into(), i16::MAX.into()),
        "i32" => (i32::MIN.into(), i32::MAX.into()),
        "i64" => (i64::MIN.into(), i64::MAX.into()),
        "u8" => (0, u8::MAX.into()),
        "u16" => (0, u16::MAX.into()),
        "u32" => (0, u32::MAX.into()),
        "u64" => (0, u64::MAX.into()),
        _ => return None,
    })
}

fn widening_pairs() -> Vec<(&'static str, &'static str)> {
    TYPES
        .into_iter()
        .flat_map(|f| TYPES.into_iter().map(move |t| (f, t)))
        .filter(|(f, t)| f != t && value_type(f).widens_to(value_type(t)))
        .collect()
}

fn cast_chain(from: &str, to: &str) -> Result<Chain, Vec<thadam_core::ThadamError>> {
    let spec = ChainSpec::from_value(json!({
        "datasets": [{ "id": "d", "match": {}, "fields": [{ "name": "v", "type": from }] }],
        "transforms": [{ "kind": "cast", "dataset": "d", "fields": { "v": to } }],
    }))
    .unwrap();
    Chain::compile(&spec, ChainLimits::default())
}

#[test]
fn cast_compiles_exactly_when_it_widens() {
    for from in TYPES {
        for to in TYPES {
            let ok = cast_chain(from, to).is_ok();
            let expected = from == to || value_type(from).widens_to(value_type(to));
            assert_eq!(ok, expected, "{from} -> {to}");
        }
    }
}

#[derive(Clone, Debug)]
enum Num {
    Int(i128),
    Float(f64),
}

fn number_for(from: &str, seed: i128, bits: u64) -> (serde_json::Value, Num) {
    if let Some((lo, hi)) = int_bounds(from) {
        let span = (hi - lo + 1) as u128;
        let v = lo + (seed.unsigned_abs() % span) as i128;
        let json = if v < 0 {
            json!(i64::try_from(v).unwrap())
        } else {
            json!(u64::try_from(v).unwrap())
        };
        return (json, Num::Int(v));
    }
    let raw = f64::from_bits(bits);
    let x = if raw.is_finite() { raw } else { 1.5 };
    if from == "f32" {
        let x32 = x as f32;
        let x32 = if x32.is_finite() { x32 } else { 0.25 };
        (json!(f64::from(x32)), Num::Float(f64::from(x32)))
    } else {
        (json!(x), Num::Float(x))
    }
}

fn numeric(v: &Value) -> Num {
    match v {
        Value::F32(x) => Num::Float(f64::from(*x)),
        Value::F64(x) => Num::Float(*x),
        other => Num::Int(
            other
                .as_i64()
                .map(i128::from)
                .or_else(|| other.as_u64().map(i128::from))
                .expect("an integer value"),
        ),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn widening_never_narrows(pair in prop::sample::select(widening_pairs()), seed in any::<i128>(), bits in any::<u64>()) {
        let (from, to) = pair;
        let mut chain = cast_chain(from, to).unwrap();
        let (json, before) = number_for(from, seed, bits);
        let mut f = Frames::new();
        f.push(message("t", json!({ "v": json }).to_string(), 1));
        let r = run(&mut chain, &f);
        prop_assert!(r.rejects.is_empty());
        let rec = r.events("d")[0].payload().as_record().unwrap().clone();
        let value = rec.get("v").unwrap();
        prop_assert_eq!(value.value_type(), value_type(to));
        match (before, numeric(value)) {
            (Num::Int(a), Num::Int(b)) => prop_assert_eq!(a, b),
            (Num::Float(a), Num::Float(b)) => prop_assert_eq!(a.to_bits(), b.to_bits()),
            _ => prop_assert!(false, "a cast changed integer-ness"),
        }
        prop_assert!(rec.validate().is_ok());
    }
}

fn keyed_chain(window: &str, max_keys: usize) -> Chain {
    let spec = ChainSpec::from_value(json!({
        "datasets": [{
            "id": "d", "match": { "topic": "#" },
            "identity": [{ "robot": "{topic}" }],
            "event_id": { "key": ["robot", "seq"] },
            "dedup": { "window": window, "max_keys": max_keys },
            "fields": [{ "name": "seq", "type": "u64", "required": true }]
        }]
    }))
    .unwrap();
    Chain::compile(&spec, ChainLimits::default()).unwrap()
}

fn keyed_stream(items: &[(u8, u8, i32)]) -> Frames {
    let mut t = 1_000_000_000i64;
    let mut f = Frames::new();
    for (robot, seq, dt) in items {
        t += i64::from(*dt) * 1_000;
        f.push(message(
            &format!("r{robot}"),
            format!(r#"{{"seq":{seq}}}"#),
            t,
        ));
    }
    f
}

fn first_occurrences(items: &[(u8, u8, i32)]) -> Vec<u64> {
    let mut seen = HashSet::new();
    items
        .iter()
        .enumerate()
        .filter(|(_, (r, s, _))| seen.insert((*r, *s)))
        .map(|(i, _)| i as u64)
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn dedup_keeps_exactly_the_first_of_each_key_when_nothing_expires(
        items in prop::collection::vec((0u8..4, 0u8..6, 0i32..2_000), 0..60)
    ) {
        let mut chain = keyed_chain("10m", 10_000);
        let r = run(&mut chain, &keyed_stream(&items));
        let kept: Vec<u64> = r.events("d").iter().map(|e| e.wal_pos().get()).collect();
        prop_assert_eq!(kept, first_occurrences(&items));
    }

    #[test]
    fn dedup_never_drops_a_distinct_key_even_under_pressure(
        items in prop::collection::vec((0u8..6, 0u8..8, -5_000i32..60_000), 0..80),
        max_keys in 1usize..6,
        window_s in 1u32..120,
    ) {
        let mut chain = keyed_chain(&format!("{window_s}s"), max_keys);
        let r = run(&mut chain, &keyed_stream(&items));
        let kept: HashSet<u64> = r.events("d").iter().map(|e| e.wal_pos().get()).collect();
        for first in first_occurrences(&items) {
            prop_assert!(kept.contains(&first), "first occurrence {first} was dropped");
        }
        let mut ids = HashSet::new();
        let mut earlier = HashSet::new();
        for (i, (robot, seq, _)) in items.iter().enumerate() {
            let key = (*robot, *seq);
            if !kept.contains(&(i as u64)) {
                prop_assert!(earlier.contains(&key), "dropped message {i} was not a repeat");
            } else if earlier.contains(&key) {
                prop_assert!(ids.contains(&r.events("d").iter().find(|e| e.wal_pos().get() == i as u64).unwrap().id()));
            }
            earlier.insert(key);
            if let Some(e) = r.events("d").iter().find(|e| e.wal_pos().get() == i as u64) {
                ids.insert(e.id());
            }
        }
        let stats = &chain.stats().datasets[0].1;
        prop_assert_eq!(stats.written + stats.deduped, items.len() as u64);
    }

    #[test]
    fn equal_keys_get_equal_ids_and_distinct_keys_distinct_ids(
        items in prop::collection::vec((0u8..4, 0u8..4), 1..40)
    ) {
        let items: Vec<(u8, u8, i32)> = items.into_iter().map(|(r, s)| (r, s, 700_000)).collect();
        let mut chain = keyed_chain("1s", 100);
        let r = run(&mut chain, &keyed_stream(&items));
        let mut by_key: HashMap<(u8, u8), thadam_core::EventId> = HashMap::new();
        for (e, (robot, seq, _)) in r.events("d").iter().zip(&items) {
            let prior = by_key.entry((*robot, *seq)).or_insert(e.id());
            prop_assert_eq!(*prior, e.id());
        }
        let distinct_ids: HashSet<_> = by_key.values().collect();
        prop_assert_eq!(distinct_ids.len(), by_key.len());
    }
}

fn deadband_chain(deadband: f64, heartbeat: Option<&str>) -> Chain {
    let mut step = json!({ "kind": "on_change", "dataset": "d",
                           "fields": [{ "field": "temp", "deadband": deadband }] });
    if let Some(h) = heartbeat {
        step["heartbeat"] = json!(h);
    }
    let spec = ChainSpec::from_value(json!({
        "datasets": [{
            "id": "d", "match": { "topic": "#" },
            "identity": [{ "robot": "{topic}" }],
            "fields": [{ "name": "temp", "type": "f64" }]
        }],
        "transforms": [step],
    }))
    .unwrap();
    Chain::compile(&spec, ChainLimits::default()).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn deadband_holds_for_out_of_order_ingest_times(
        items in prop::collection::vec((0u8..3, -60i32..60, 0i64..2_000), 1..80),
        deadband in prop::sample::select(vec![0.0f64, 0.5, 2.0, 10.0]),
        heartbeat in prop::option::of(prop::sample::select(vec![5u32, 60, 600])),
    ) {
        let hb = heartbeat.map(|s| format!("{s}s"));
        let mut chain = deadband_chain(deadband, hb.as_deref());
        let mut f = Frames::new();
        for (robot, temp, secs) in &items {
            f.push(message(&format!("r{robot}"), format!(r#"{{"temp":{}}}"#, f64::from(*temp) * 0.5), secs * 1_000_000));
        }
        let r = run(&mut chain, &f);
        prop_assert!(r.rejects.is_empty());
        let kept: HashSet<u64> = r.events("d").iter().map(|e| e.wal_pos().get()).collect();

        let mut last_kept: HashMap<u8, f64> = HashMap::new();
        let mut model: HashMap<u8, (f64, i64)> = HashMap::new();
        for (i, (robot, temp, secs)) in items.iter().enumerate() {
            let v = f64::from(*temp) * 0.5;
            let now = *secs * 1_000_000_000;
            let expect = match model.get_mut(robot) {
                None => true,
                Some((last_v, last_t)) => {
                    let mut keep = (v - *last_v).abs() > deadband;
                    if !keep && let Some(h) = heartbeat {
                        keep = i128::from(now) - i128::from(*last_t) >= i128::from(h) * 1_000_000_000;
                    }
                    if !keep && now < *last_t {
                        *last_t = now;
                    }
                    keep
                }
            };
            if expect {
                model.insert(*robot, (v, now));
            }
            prop_assert_eq!(kept.contains(&(i as u64)), expect, "message {} of robot {}", i, robot);

            // Independent invariants, not derived from the model.
            match last_kept.get(robot) {
                None => prop_assert!(kept.contains(&(i as u64)), "a robot's first event is kept"),
                Some(prev) if !kept.contains(&(i as u64)) => {
                    prop_assert!((v - prev).abs() <= deadband, "a dropped event moved past the deadband");
                }
                Some(prev) if (v - prev).abs() <= deadband => {
                    prop_assert!(heartbeat.is_some(), "an unchanged event is kept only by a heartbeat");
                }
                Some(_) => {}
            }
            if kept.contains(&(i as u64)) {
                last_kept.insert(*robot, v);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Msg {
    Telemetry {
        robot: u8,
        seq: u8,
        dt: i32,
    },
    Joints {
        robot: u8,
        seq: u8,
        samples: u8,
        dt: i32,
    },
    State {
        robot: u8,
        mode: u8,
        dt: i32,
    },
    Event {
        robot: u8,
        seq: u8,
        debug: bool,
        dt: i32,
    },
    Garbage(Vec<u8>),
}

fn msg_strategy() -> impl Strategy<Value = Msg> {
    prop_oneof![
        4 => (0u8..3, 0u8..8, 0i32..400_000).prop_map(|(robot, seq, dt)| Msg::Telemetry { robot, seq, dt }),
        2 => (0u8..3, 0u8..6, 0u8..5, 0i32..400_000).prop_map(|(robot, seq, samples, dt)| Msg::Joints { robot, seq, samples, dt }),
        3 => (0u8..3, 0u8..3, 0i32..4_000_000).prop_map(|(robot, mode, dt)| Msg::State { robot, mode, dt }),
        2 => (0u8..3, 0u8..6, any::<bool>(), 0i32..1000).prop_map(|(robot, seq, debug, dt)| Msg::Event { robot, seq, debug, dt }),
        1 => prop::collection::vec(any::<u8>(), 0..40).prop_map(Msg::Garbage),
    ]
}

fn mixed_frames(msgs: &[Msg]) -> Frames {
    let mut t = 1_791_369_600_000_000i64;
    let mut f = Frames::new();
    for m in msgs {
        let (topic_s, payload, dt) = match m {
            Msg::Telemetry { robot, seq, dt } => (
                topic(&format!("robot-{robot}"), "telemetry"),
                telemetry_payload(u64::from(*seq), t),
                *dt,
            ),
            Msg::Joints {
                robot,
                seq,
                samples,
                dt,
            } => (
                topic(&format!("robot-{robot}"), "joints"),
                joints_payload(u64::from(*seq), t, usize::from(*samples)),
                *dt,
            ),
            Msg::State { robot, mode, dt } => (
                topic(&format!("robot-{robot}"), "state"),
                format!(r#"{{"ts_us":{t},"mode":"M{mode}","cycle_id":1,"program":"p"}}"#),
                *dt,
            ),
            Msg::Event {
                robot,
                seq,
                debug,
                dt,
            } => (
                topic(&format!("robot-{robot}"), "events"),
                format!(
                    r#"{{"seq":{seq},"level":"{}","ts_us":{t}}}"#,
                    if *debug { "debug" } else { "info" }
                ),
                *dt,
            ),
            Msg::Garbage(bytes) => (
                topic("robot-0", "telemetry"),
                String::from_utf8_lossy(bytes).into_owned(),
                1,
            ),
        };
        t += i64::from(dt);
        f.push(message(&topic_s, payload, t));
    }
    f
}

fn spec_with_sampling() -> ChainSpec {
    ChainSpec::from_value(json!({
        "datasets": [telemetry_dataset(), joints_dataset(), state_dataset(), events_dataset()],
        "transforms": [
            { "kind": "validate", "dataset": "telemetry",
              "rules": [{ "field": "cpu_temp_c", "min": 0, "max": 100 }] },
            { "kind": "sample", "dataset": "telemetry", "every_n": 2, "min_interval": "50ms" },
            { "kind": "on_change", "dataset": "state", "fields": ["mode"], "heartbeat": "5s" },
            { "kind": "filter", "dataset": "events", "where": { "field": "level", "ne": "debug" } },
            { "kind": "sample", "dataset": "joints", "every_n": 3 },
        ],
    }))
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn the_same_frames_twice_give_an_equal_row_set(msgs in prop::collection::vec(msg_strategy(), 0..40)) {
        let frames = mixed_frames(&msgs);
        let mut a = Chain::compile(&spec_with_sampling(), ChainLimits::default()).unwrap();
        let mut b = Chain::compile(&spec_with_sampling(), ChainLimits::default()).unwrap();
        let ra = run(&mut a, &frames);
        let rb = run(&mut b, &frames);
        for (x, y) in ra.out.batches.iter().zip(&rb.out.batches) {
            prop_assert_eq!(&x.batch.events, &y.batch.events);
        }
        prop_assert_eq!(&ra.rejects, &rb.rejects);
        prop_assert_eq!(a.stats(), b.stats());
    }

    #[test]
    fn warm_then_process_equals_a_continuous_run(
        msgs in prop::collection::vec(msg_strategy(), 1..40),
        split in 0usize..40,
    ) {
        let frames = mixed_frames(&msgs);
        let split = split.min(frames.records.len());

        let mut continuous = Chain::compile(&spec_with_sampling(), ChainLimits::default()).unwrap();
        let whole = run(&mut continuous, &frames);
        let tail = |events: Vec<Event>| -> Vec<Event> {
            events.into_iter().filter(|e| e.wal_pos() >= WalPos::new(split as u64)).collect()
        };

        let mut warmed = Chain::compile(&spec_with_sampling(), ChainLimits::default()).unwrap();
        warm(&mut warmed, &frames.slice(0..split));
        prop_assert_eq!(warmed.stats().datasets.iter().map(|(_, s)| s.messages).sum::<u64>(), 0, "warming leaves the counters alone");
        let rest = run(&mut warmed, &frames.slice(split..frames.records.len()));

        for (w, r) in whole.out.batches.iter().zip(&rest.out.batches) {
            prop_assert_eq!(tail(w.batch.events.clone()), r.batch.events.clone(), "dataset {}", &*w.dataset);
        }
        let whole_rejects: Vec<_> = whole.rejects.iter().filter(|x| x.pos >= WalPos::new(split as u64)).cloned().collect();
        prop_assert_eq!(whole_rejects, rest.rejects);
    }

    #[test]
    fn no_input_makes_the_chain_panic_or_lose_raw_bytes(
        payloads in prop::collection::vec(prop_oneof![
            prop::collection::vec(any::<u8>(), 0..200),
            adversarial_json(),
        ], 1..12)
    ) {
        let mut chain = demo_chain();
        let mut f = Frames::new();
        for (i, p) in payloads.iter().enumerate() {
            let stream = ["telemetry", "joints", "state", "events"][i % 4];
            f.push(message(&topic("r", stream), p.clone(), 1_000_000));
        }
        let r = run(&mut chain, &f);
        for rej in &r.rejects {
            let (_, _, original) = &f.records[rej.pos.get() as usize];
            prop_assert_eq!(rej.payload.as_ref(), original.payload.as_ref());
            prop_assert!(!rej.error.message.is_empty());
        }
        for e in r.all_events() {
            prop_assert!(e.payload().as_record().unwrap().validate().is_ok());
        }
    }
}

fn adversarial_json() -> impl Strategy<Value = Vec<u8>> {
    let token = prop_oneof![
        Just("{".to_owned()),
        Just("}".to_owned()),
        Just("[".to_owned()),
        Just("]".to_owned()),
        Just(",".to_owned()),
        Just(":".to_owned()),
        Just(r#""seq""#.to_owned()),
        Just(r#""ts_us""#.to_owned()),
        Just(r#""samples""#.to_owned()),
        Just(r#""level""#.to_owned()),
        Just(r#""mode""#.to_owned()),
        Just("18446744073709551616".to_owned()),
        Just("-9223372036854775809".to_owned()),
        Just("1e999".to_owned()),
        Just("-1e999".to_owned()),
        Just("0.1e-999".to_owned()),
        Just("null".to_owned()),
        Just("true".to_owned()),
        Just(r#""\ud800""#.to_owned()),
        Just(r#""\u0000""#.to_owned()),
        Just("\"\u{1F980}\"".to_owned()),
        any::<i64>().prop_map(|n| n.to_string()),
        any::<f64>().prop_map(|x| format!("{x:e}")),
    ];
    prop::collection::vec(token, 0..40).prop_map(|t| t.concat().into_bytes())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn nesting_and_size_attacks_are_bounded_rejects(depth in 1usize..4_000, width in 0usize..3_000) {
        let mut chain = demo_chain();
        let mut f = Frames::new();
        let t = topic("r", "telemetry");
        f.push(message(&t, format!("{}{}", "[".repeat(depth), "]".repeat(depth)), 1));
        f.push(message(&t, format!("{}0{}", "{\"a\":".repeat(depth), "}".repeat(depth)), 1));
        f.push(message(&t, format!(r#"{{"seq":1,"motor_temp_dc":[{}]}}"#, vec!["1"; width].join(",")), 1));
        let r = run(&mut chain, &f);
        prop_assert_eq!(r.out.total_events() + r.rejects.len(), 3);
    }
}
