//! Write-throughput benchmark (PR14, benchmark B6 core): MB/s through the
//! full sink path (`append` -> encode -> write -> finalize), with rotation
//! disabled so one file grows during each iteration.
//!
//! Run with `cargo bench -p thadam-segments`.

// Benchmarks plumbing deliberately unwraps: a failed setup is a failed
// benchmark, and `clippy.toml`'s test exemption does not cover criterion
// harness code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use thadam_core::{
    DiskBudget, Event, EventBatch, EventId, FieldDef, FieldType, Record, SegmentBatch, SegmentSink,
    Shape, ShapeRef, Shutdown, SinkContext, SinkId, TimeDomain, Times, Timestamp, Value, WalEpoch,
    WalPos, WalRange,
};
use thadam_segments::{ParquetSegmentSink, ParquetSinkConfig};
use thadam_testkit::FakeClock;

const DATASET: &str = "bench";

fn bench_shape() -> ShapeRef {
    static SHAPE: std::sync::OnceLock<ShapeRef> = std::sync::OnceLock::new();
    SHAPE
        .get_or_init(|| {
            let fields = vec![
                FieldDef::new("a", FieldType::I64),
                FieldDef::new("b", FieldType::F64),
                FieldDef::new("c", FieldType::Str),
                FieldDef::new("d", FieldType::Bool),
            ];
            ShapeRef::new(Shape::new(fields).expect("static bench shape"))
        })
        .clone()
}

fn bench_events(shape: &ShapeRef, wal_pos: u64, n: usize) -> Vec<Event> {
    let mut events = Vec::with_capacity(n);
    for i in 0..n {
        let pos = wal_pos + i as u64;
        let t = Timestamp::from_nanos(1_000_000_000_000 + pos as i64);
        let rec = Record::new(
            shape.clone(),
            vec![
                Value::I64(pos as i64),
                Value::F64((pos as f64) * 1.5),
                Value::str("benchmark-payload-string-0123456789abcdef"),
                Value::Bool(pos.is_multiple_of(2)),
            ],
        )
        .expect("bench record");
        events.push(
            Event::builder(
                EventId::from_u128(u128::from(pos)),
                WalPos::new(pos),
                Times::new(t, TimeDomain::Utc),
            )
            .payload(rec)
            .build()
            .expect("bench event"),
        );
    }
    events
}

fn test_config(dir: &std::path::Path, codec: &str) -> ParquetSinkConfig {
    let json = serde_json::json!({
        "compression": codec,
        "rotation": {"max_bytes": null, "max_rows": null, "max_age": null},
        "node_id": "bench"
    });
    let decls = [thadam_core::DatasetDecl::new(
        DATASET,
        vec![
            FieldDef::new("a", FieldType::I64).with_field_id(1000),
            FieldDef::new("b", FieldType::F64).with_field_id(1001),
            FieldDef::new("c", FieldType::Str).with_field_id(1002),
            FieldDef::new("d", FieldType::Bool).with_field_id(1003),
        ],
        vec![],
        false,
    )];
    match thadam_segments::parse_sink_config(&json, dir.to_path_buf(), "bench")
        .and_then(|options| options.with_datasets(&decls, "", ""))
    {
        Ok(cfg) => cfg,
        Err(es) => panic!("bench config parses: {es:?}"),
    }
}

fn write_mb_per_s(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("runtime");
    let shape = bench_shape();

    let mut group = c.benchmark_group("parquet_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for codec in ["zstd", "snappy", "none"] {
        // 25 batches of 2000 rows x ~100 input bytes.
        let batches: usize = 25;
        let rows: usize = 2000;
        let total_bytes = (rows * batches * 100) as u64;
        // Input events are amortized out of the timed loop.
        let shape = shape.clone();
        let events: Vec<SegmentBatch> = (0..batches)
            .map(|b| {
                let first = (b * rows + 1) as u64;
                let last = first + rows as u64 - 1;
                let range = WalRange::new(WalEpoch::new(1), WalPos::new(first), WalPos::new(last))
                    .expect("range");
                SegmentBatch::single(
                    DATASET,
                    EventBatch::new(bench_events(&shape, first, rows), range, true),
                )
            })
            .collect();

        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(BenchmarkId::new("mb_per_s", codec), &events, |b, events| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let ctx = SinkContext::new(
                        SinkId::new("bench"),
                        Shutdown::new(),
                        FakeClock::default().shared(),
                        dir.path().to_path_buf(),
                        Arc::new(DiskBudget::new()),
                    );
                    let sink = ParquetSegmentSink::new(test_config(dir.path(), codec));
                    (dir, ctx, sink)
                },
                |(_dir, ctx, sink)| {
                    runtime.block_on(async move {
                        let mut sink = sink.expect("sink builds");
                        sink.recover(&ctx).await.expect("recover");
                        for batch in events {
                            let rejected = sink.append(batch.clone()).await.expect("append");
                            assert!(rejected.is_empty());
                        }
                        sink.finalize_all().await.expect("finalize_all");
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, write_mb_per_s);
criterion_main!(benches);
