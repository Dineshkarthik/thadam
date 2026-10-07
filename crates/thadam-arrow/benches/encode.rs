//! Encoder throughput in rows per second for the two reference workloads.
//!
//! - `telemetry`: the D8 spike's W1, seven scalar fields (one string), with the
//!   envelope, lineage, and two identity levels.
//! - `joint_state`: W2, three `list<f64>` typed arrays of seven elements, an
//!   exploded dataset (`sample_idx`), same envelope.
//!
//! Each iteration pushes `BATCH` pre-built events and calls `finish`, so the
//! number includes the per-batch cost of taking and re-reserving buffers.
//! Event construction is outside the timed section.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use thadam_arrow::{ArrowOptions, BatchContext, BatchEncoder, DatasetSchema, TsUnit};
use thadam_core::{
    AssetPath, Event, EventId, FieldDef, FieldType, Quality, Record, Shape, ShapeRef, TimeDomain,
    Times, Timestamp, Value, WalEpoch, WalPos,
};

const BATCH: usize = 8192;
const DISTINCT: usize = 1024;
const ROBOTS: usize = 40;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn shape(fields: &[FieldDef]) -> ShapeRef {
    ShapeRef::new(Shape::new(fields.to_vec()).expect("valid shape"))
}

fn paths() -> Vec<AssetPath> {
    (0..ROBOTS)
        .map(|i| {
            AssetPath::new([
                ("site", format!("site-{}", i % 4)),
                ("device", format!("robot-{i:02}")),
            ])
            .expect("valid path")
        })
        .collect()
}

fn event(i: usize, shape: &ShapeRef, values: Vec<Value>, path: &AssetPath) -> Event {
    let ingest = Timestamp::from_nanos(1_700_000_000_000_000_000 + i as i64 * 10_000_000);
    let times = Times::new(ingest, TimeDomain::Utc)
        .with_source(Timestamp::from_nanos(ingest.as_nanos() - 1_234_567))
        .expect("source set once");
    Event::builder(
        EventId::from_u128(i as u128),
        WalPos::new(i as u64 / 10),
        times,
    )
    .payload(Record::new(shape.clone(), values).expect("matching length"))
    .quality(Quality::good())
    .asset(path.clone())
    .build()
    .expect("valid event")
}

fn telemetry_fields() -> Vec<FieldDef> {
    vec![
        FieldDef::new("seq", FieldType::U32).with_field_id(1000),
        FieldDef::new("temperature", FieldType::F32).with_field_id(1001),
        FieldDef::new("voltage", FieldType::F32).with_field_id(1002),
        FieldDef::new("current", FieldType::F32).with_field_id(1003),
        FieldDef::new("battery_pct", FieldType::U8).with_field_id(1004),
        FieldDef::new("robot", FieldType::Str).with_field_id(1005),
    ]
}

fn joint_fields() -> Vec<FieldDef> {
    let array = || FieldType::List(Box::new(FieldType::F64));
    vec![
        FieldDef::new("position", array()).with_field_id(1000),
        FieldDef::new("velocity", array()).with_field_id(1001),
        FieldDef::new("effort", array()).with_field_id(1002),
    ]
}

fn telemetry_events() -> (Vec<FieldDef>, Vec<Event>) {
    let fields = telemetry_fields();
    let shape = shape(&fields);
    let paths = paths();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let distinct: Vec<Event> = (0..DISTINCT)
        .map(|i| {
            let values = vec![
                Value::U32(100_000 + i as u32),
                Value::F32((20.0 + rng.unit() * 15.0) as f32),
                Value::F32((11.0 + rng.unit() * 1.8) as f32),
                Value::F32((0.5 + rng.unit() * 3.0) as f32),
                Value::U8((rng.next() % 101) as u8),
                Value::str(format!("robot-{:02}", rng.next() % 40)),
            ];
            event(i, &shape, values, &paths[i % ROBOTS])
        })
        .collect();
    (fields, cycle(distinct))
}

fn joint_events() -> (Vec<FieldDef>, Vec<Event>) {
    let fields = joint_fields();
    let shape = shape(&fields);
    let paths = paths();
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut array = |lo: f64, hi: f64| {
        Value::from(
            (0..7)
                .map(|_| lo + rng.unit() * (hi - lo))
                .collect::<Vec<f64>>(),
        )
    };
    let distinct: Vec<Event> = (0..DISTINCT)
        .map(|i| {
            let values = vec![array(-3.1, 3.1), array(-2.0, 2.0), array(-40.0, 40.0)];
            let mut e = event(i, &shape, values, &paths[i % ROBOTS]);
            e.set_sample_idx(Some((i % 10) as u32));
            e
        })
        .collect();
    (fields, cycle(distinct))
}

fn cycle(distinct: Vec<Event>) -> Vec<Event> {
    distinct.iter().cycle().take(BATCH).cloned().collect()
}

fn schema(
    fields: Vec<FieldDef>,
    exploded: bool,
    levels: bool,
    options: ArrowOptions,
) -> Arc<DatasetSchema> {
    let builder = DatasetSchema::builder("bench");
    let builder = if levels {
        builder.level("site", 100).level("device", 101)
    } else {
        builder
    };
    Arc::new(
        builder
            .exploded(exploded)
            .fields(fields)
            .options(options)
            .build()
            .expect("valid schema"),
    )
}

fn bench_dataset(
    c: &mut Criterion,
    name: &str,
    fields: Vec<FieldDef>,
    events: &[Event],
    exploded: bool,
) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(events.len() as u64));
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);

    for (label, levels, options) in [
        ("micros", true, ArrowOptions::default()),
        (
            "nanos",
            true,
            ArrowOptions::default().with_ts_unit(TsUnit::Nanos),
        ),
        (
            "micros_no_lineage",
            true,
            ArrowOptions::default().with_lineage_columns(false),
        ),
        ("micros_no_levels", false, ArrowOptions::default()),
    ] {
        let mut encoder =
            BatchEncoder::new(schema(fields.clone(), exploded, levels, options), BATCH);
        encoder.set_context(BatchContext {
            epoch: WalEpoch::new(1),
            clock_synced: true,
        });
        let mut probe_bytes = 0;
        for e in events {
            encoder.push(e).expect("benchmark events are valid");
        }
        probe_bytes += encoder.estimated_bytes();
        black_box(encoder.finish());
        eprintln!(
            "{name}/{label}: {} rows, about {:.0} bytes per row (estimate)",
            events.len(),
            probe_bytes as f64 / events.len() as f64
        );

        group.bench_function(label, |b| {
            b.iter(|| {
                for e in events {
                    encoder
                        .push(black_box(e))
                        .expect("benchmark events are valid");
                }
                black_box(encoder.finish())
            });
        });
    }
    group.finish();
}

fn encode(c: &mut Criterion) {
    let (fields, events) = telemetry_events();
    bench_dataset(c, "telemetry", fields, &events, false);
    let (fields, events) = joint_events();
    bench_dataset(c, "joint_state", fields, &events, true);
}

criterion_group!(benches, encode);
criterion_main!(benches);
