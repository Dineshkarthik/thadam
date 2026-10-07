//! WAL micro-benchmarks: append throughput per fsync mode (group commit with
//! pipelined submissions) and replay throughput from a cursor.
//!
//! Numbers from a laptop are for regression tracking only; reference-hardware
//! runs (x86 NVMe, ARM eMMC) are what ADR-0004 needs.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use thadam_core::{DiskBudget, RawBatch, RawMessage, SourceId, SystemClock, Timestamp};
use thadam_wal::{Fsync, OnFull, ReadItem, StartFrom, Wal, WalOptions};

/// One frame: 64 records of 200 B (the S1 small-record shape).
const RECORDS: usize = 64;
const RECORD_BYTES: usize = 200;
/// Frames submitted per iteration before awaiting the tickets.
const FRAMES_PER_ITER: usize = 64;

fn frame() -> RawBatch {
    let payload = Bytes::from(vec![0x5a; RECORD_BYTES]);
    let messages = (0..RECORDS)
        .map(|i| {
            let mut m = RawMessage::new(Timestamp::from_nanos(i as i64), payload.clone());
            m.attrs.insert("topic", "robots/r1/telemetry");
            m
        })
        .collect();
    RawBatch::new(SourceId::new("bench"), messages, true)
}

fn open(dir: &std::path::Path, fsync: Fsync) -> Wal {
    let mut o = WalOptions::new(OnFull::DropOldest);
    o.fsync = fsync;
    o.segment_bytes = 64 << 20;
    let (wal, _) = thadam_wal::open(
        dir,
        o,
        Arc::new(DiskBudget::new()),
        Arc::new(SystemClock::new()),
    )
    .unwrap_or_else(|e| panic!("open: {e}"));
    wal
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("runtime: {e}"))
}

fn append(c: &mut Criterion) {
    let rt = rt();
    let batch = frame();
    let mut group = c.benchmark_group("append");
    group.throughput(Throughput::Bytes(
        (FRAMES_PER_ITER * RECORDS * RECORD_BYTES) as u64,
    ));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    for (name, fsync) in [
        ("always", Fsync::Always),
        ("interval_10ms", Fsync::Interval(Duration::from_millis(10))),
        ("never", Fsync::Never),
    ] {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
        let wal = open(dir.path(), fsync);
        let writer = wal.start_writer().unwrap_or_else(|e| panic!("writer: {e}"));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut tickets = Vec::with_capacity(FRAMES_PER_ITER);
                    for _ in 0..FRAMES_PER_ITER {
                        let t = writer.submit(batch.clone()).await;
                        tickets.push(t.unwrap_or_else(|e| panic!("submit: {e}")));
                    }
                    for t in tickets {
                        black_box(t.await.unwrap_or_else(|e| panic!("commit: {e}")));
                    }
                });
            });
        });
        drop(writer);
        rt.block_on(wal.close())
            .unwrap_or_else(|e| panic!("close: {e}"));
    }
    group.finish();
}

fn read(c: &mut Criterion) {
    let rt = rt();
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let wal = open(dir.path(), Fsync::Never);
    let writer = wal.start_writer().unwrap_or_else(|e| panic!("writer: {e}"));
    let batch = frame();
    let frames = (64 << 20) / (RECORDS * RECORD_BYTES);
    rt.block_on(async {
        let mut tickets = Vec::new();
        for _ in 0..frames {
            tickets.push(
                writer
                    .submit(batch.clone())
                    .await
                    .unwrap_or_else(|e| panic!("submit: {e}")),
            );
        }
        for t in tickets {
            t.await.unwrap_or_else(|e| panic!("commit: {e}"));
        }
    });
    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Bytes((frames * RECORDS * RECORD_BYTES) as u64));
    group.sample_size(10);
    group.bench_function("replay_1mib_chunks", |b| {
        b.iter(|| {
            let mut r = wal
                .reader("bench", StartFrom::Earliest)
                .unwrap_or_else(|e| panic!("reader: {e}"));
            let mut n = 0usize;
            while let Some(item) = r.try_next(1 << 20).unwrap_or_else(|e| panic!("read: {e}")) {
                if let ReadItem::Frames(chunk) = item {
                    for f in chunk.frames() {
                        n += f.records().map(|r| r.payload().len()).sum::<usize>();
                    }
                }
            }
            black_box(n)
        });
    });
    group.finish();
}

criterion_group!(benches, append, read);
criterion_main!(benches);
