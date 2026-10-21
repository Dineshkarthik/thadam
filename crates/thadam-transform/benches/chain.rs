//! Criterion benchmarks: decode throughput and ns per event for each step.
//!
//! Each step benchmark compiles a fresh chain per batch (outside the timed
//! region) and feeds it 1,024 distinct telemetry messages, so stateful steps
//! see realistic misses. The cost of a step is the difference from `baseline`
//! (decode, positional id, time, envelope). Criterion reports time per batch;
//! divide by 1,024 for ns per event, or read the `elem/s` throughput line.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::time::Duration;

use common::*;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::{Value, json};
use std::hint::black_box;
use thadam_transform::{Chain, ChainLimits, ChainSpec};

const BATCH: usize = 1024;

fn telemetry_frames(n: usize) -> Frames {
    let mut f = Frames::new();
    for i in 0..n {
        let robot = format!("robot-{:02}", i % 8);
        f.push(message(
            &topic(&robot, "telemetry"),
            telemetry_payload(i as u64, 1_791_369_600_000_000 + i as i64 * 1_000),
            1_791_369_600_000_000 + i as i64 * 1_000,
        ));
    }
    f
}

fn joints_frames(n: usize) -> Frames {
    let mut f = Frames::new();
    for i in 0..n {
        let robot = format!("robot-{:02}", i % 8);
        f.push(message(
            &topic(&robot, "joints"),
            joints_payload(i as u64, 1_791_369_600_000_000 + i as i64 * 100_000, 10),
            1_791_369_600_000_000 + i as i64 * 100_000,
        ));
    }
    f
}

fn plain_telemetry() -> Value {
    let mut ds = telemetry_dataset();
    let o = ds.as_object_mut().unwrap();
    o.remove("event_id");
    o.remove("dedup");
    ds
}

fn keyed_telemetry() -> Value {
    let mut ds = telemetry_dataset();
    ds.as_object_mut().unwrap().remove("dedup");
    ds
}

fn compile(datasets: Vec<Value>, transforms: Value) -> Chain {
    let spec =
        ChainSpec::from_value(json!({ "datasets": datasets, "transforms": transforms })).unwrap();
    Chain::compile(&spec, ChainLimits::default()).unwrap()
}

fn bench_decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("decode");
    g.sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));

    let payload = telemetry_payload(918_273, 1_791_369_600_123_456);
    let bytes = payload.as_bytes();
    g.throughput(Throughput::Bytes(bytes.len() as u64));
    g.bench_function("dom_parse_only", |b| {
        b.iter(|| serde_json::from_slice::<Value>(black_box(bytes)).unwrap());
    });
    g.bench_function("dom_parse_plus_pointer_lookups", |b| {
        b.iter(|| {
            let dom = serde_json::from_slice::<Value>(black_box(bytes)).unwrap();
            for p in [
                "/seq",
                "/soc_pct",
                "/voltage_mv",
                "/current_ma",
                "/cpu_temp_dc",
                "/motor_temp_dc",
                "/ts_us",
            ] {
                black_box(dom.pointer(p));
            }
        });
    });

    let frames = telemetry_frames(BATCH);
    let mut chain = compile(vec![plain_telemetry()], json!([]));
    g.throughput(Throughput::Bytes(
        frames
            .records
            .iter()
            .map(|(_, _, m)| m.payload.len() as u64)
            .sum(),
    ));
    g.bench_function("chain_decode_1024_msgs", |b| {
        b.iter(|| black_box(run(&mut chain, &frames)));
    });
    g.finish();
}

fn bench_steps(c: &mut Criterion) {
    let mut g = c.benchmark_group("step");
    g.sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .throughput(Throughput::Elements(BATCH as u64));
    let frames = telemetry_frames(BATCH);

    let dir = tempfile::tempdir().unwrap();
    let mut csv = String::from("robot_id,model,payload\n");
    for i in 0..8 {
        csv.push_str(&format!("robot-{i:02},model-{i},{i}.5\n"));
    }
    std::fs::write(dir.path().join("robots.csv"), csv).unwrap();

    let cases: Vec<(&str, Vec<Value>, Value)> = vec![
        ("baseline", vec![plain_telemetry()], json!([])),
        ("content_id", vec![keyed_telemetry()], json!([])),
        ("dedup_all_new", vec![telemetry_dataset()], json!([])),
        (
            "validate",
            vec![plain_telemetry()],
            json!([{ "kind": "validate", "dataset": "telemetry",
                     "rules": [{ "field": "motor_temp_c", "min": -40, "max": 150 },
                               { "field": "cpu_temp_c", "min": -40, "max": 150 }] }]),
        ),
        (
            "on_change_deadband",
            vec![plain_telemetry()],
            json!([{ "kind": "on_change", "dataset": "telemetry",
                     "fields": [{ "field": "soc_pct", "deadband": 0.5 }, "voltage_v"], "heartbeat": "5m" }]),
        ),
        (
            "sample_every_n_and_interval",
            vec![plain_telemetry()],
            json!([{ "kind": "sample", "dataset": "telemetry", "every_n": 2, "min_interval": "1ms" }]),
        ),
        (
            "filter_combo",
            vec![plain_telemetry()],
            json!([{ "kind": "filter", "dataset": "telemetry", "where": { "all": [
                { "field": "robot", "ne": "robot-03" },
                { "any": [{ "field": "quality.status", "eq": "good" }, { "field": "soc_pct", "ge": 10.0 }] },
                { "field": "motor_temp_c", "min": -40, "max": 150 } ] } }]),
        ),
        (
            "enrich_csv",
            vec![plain_telemetry()],
            json!([{ "kind": "enrich", "dataset": "telemetry", "file": "robots.csv",
                     "key": { "field": "robot", "column": "robot_id" },
                     "add": [{ "name": "model", "type": "str" },
                             { "name": "payload_kg", "type": "f32", "column": "payload" }] }]),
        ),
        (
            "rename",
            vec![plain_telemetry()],
            json!([{ "kind": "rename", "dataset": "telemetry", "fields": { "soc_pct": "battery_pct" } }]),
        ),
        (
            "project",
            vec![plain_telemetry()],
            json!([{ "kind": "project", "dataset": "telemetry", "fields": ["seq", "soc_pct", "motor_temp_c"] }]),
        ),
        (
            "cast_widen",
            vec![plain_telemetry()],
            json!([{ "kind": "cast", "dataset": "telemetry",
                     "fields": { "soc_pct": "f64", "motor_temp_c": "list<f64>" } }]),
        ),
    ];

    for (name, datasets, transforms) in cases {
        let spec = ChainSpec::from_value(json!({ "datasets": datasets, "transforms": transforms }))
            .unwrap()
            .with_base_dir(dir.path());
        g.bench_function(name, |b| {
            b.iter_batched(
                || Chain::compile(&spec, ChainLimits::default()).unwrap(),
                |mut chain| black_box(run(&mut chain, &frames)),
                BatchSize::LargeInput,
            );
        });
    }

    let dup_frames = {
        let mut f = Frames::new();
        let m = message(
            &topic("robot-01", "telemetry"),
            telemetry_payload(1, 1_791_369_600_000_000),
            1_791_369_600_000_000,
        );
        for _ in 0..BATCH {
            f.push(m.clone());
        }
        f
    };
    g.bench_function("dedup_all_duplicates", |b| {
        b.iter_batched(
            || compile(vec![telemetry_dataset()], json!([])),
            |mut chain| black_box(run(&mut chain, &dup_frames)),
            BatchSize::LargeInput,
        );
    });

    g.throughput(Throughput::Elements((BATCH * 10) as u64));
    let joints = joints_frames(BATCH);
    g.bench_function("explode_10_samples_per_msg_per_event", |b| {
        b.iter_batched(
            || compile(vec![joints_dataset()], json!([])),
            |mut chain| black_box(run(&mut chain, &joints)),
            BatchSize::LargeInput,
        );
    });
    g.finish();
}

fn bench_warm(c: &mut Criterion) {
    let mut g = c.benchmark_group("warm");
    g.sample_size(20)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(2))
        .throughput(Throughput::Elements(BATCH as u64));
    let frames = telemetry_frames(BATCH);
    g.bench_function("warm_dedup_state_1024_msgs", |b| {
        b.iter_batched(
            || compile(vec![telemetry_dataset()], json!([])),
            |mut chain| {
                warm(&mut chain, &frames);
                black_box(chain)
            },
            BatchSize::LargeInput,
        );
    });
    g.finish();
}

criterion_group!(benches, bench_decode, bench_steps, bench_warm);
criterion_main!(benches);
