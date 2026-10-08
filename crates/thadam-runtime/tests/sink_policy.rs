//! Per-sink `required`, `max_lag`, and `start_from`: where a new sink starts,
//! and whether a stalled sink pins WAL retention.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use thadam_core::Shutdown;
use thadam_runtime::{ExitStatus, Phase, Pipeline};
use thadam_testkit::{MemorySourceConfig, TempDataDir};

fn range(from: u64, to: u64) -> Vec<u64> {
    (from..=to).collect()
}

fn quick_stop() -> thadam_runtime::RuntimeOptions {
    let mut o = fast_opts();
    o.drain_timeout = Duration::from_millis(100);
    o.shutdown_timeout = Duration::from_millis(500);
    o
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_from_decides_where_a_new_sink_begins() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();

    kit.sources.scripted("src", (1..=50).map(json_n));
    let first = yaml(dir.path(), "  - { id: a, kind: recseg }", "");
    let r1 = run_to_end(&kit, spec(&first), fast_opts()).await;
    assert_eq!(r1.exit, ExitStatus::Ok, "{:#?}", r1.status);
    assert_eq!(kit.rec.store("a").ns(), range(1, 50));

    kit.sources.scripted("src", (51..=100).map(json_n));
    let sinks = "  - { id: a, kind: recseg }\n  \
                 - { id: b, kind: recseg, start_from: latest }\n  \
                 - { id: c, kind: recseg }";
    let r2 = run_to_end(&kit, spec(&yaml(dir.path(), sinks, "")), fast_opts()).await;
    assert_eq!(r2.exit, ExitStatus::Ok, "{:#?}", r2.status);

    assert_eq!(kit.rec.store("a").ns(), range(1, 100));
    assert_eq!(
        kit.rec.store("b").ns(),
        range(51, 100),
        "latest skips history"
    );
    assert_eq!(
        kit.rec.store("c").ns(),
        range(1, 100),
        "earliest replays it"
    );
    let end = r2.status.wal.as_ref().unwrap().durable_end;
    for sink in ["a", "b", "c"] {
        assert_eq!(cursor(&r2.status, sink), end, "{sink}");
    }
    let b = r2.status.cursors.iter().find(|c| c.sink == "b").unwrap();
    assert_eq!(b.start_from, "latest");
    assert!(b.required);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_latest_sink_keeps_its_start_across_a_restart_before_its_first_commit() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();

    kit.sources.scripted("src", (1..=50).map(json_n));
    let first = yaml(dir.path(), "  - { id: a, kind: recseg }", "");
    let r1 = run_to_end(&kit, spec(&first), fast_opts()).await;
    let end1 = r1.status.wal.as_ref().unwrap().durable_end;

    // `b` is added with `start_from: latest` but is stuck, so it never commits
    // a cursor of its own while 51..=70 are captured.
    let sinks = "  - { id: a, kind: recseg }\n  - { id: b, kind: recseg, start_from: latest }";
    let both = yaml(dir.path(), sinks, "");
    lock(&kit.rec.store("b").0).stall = true;
    kit.sources.scripted("src", (51..=70).map(json_n));
    let r2 = run_to_end(&kit, spec(&both), quick_stop()).await;
    let end2 = r2.status.wal.as_ref().unwrap().durable_end;
    assert!(end2 > end1);
    assert_eq!(cursor(&r2.status, "a"), end2);
    assert_eq!(
        cursor(&r2.status, "b"),
        end1,
        "bootstrapped, never advanced"
    );
    assert!(kit.rec.store("b").ns().is_empty());

    lock(&kit.rec.store("b").0).stall = false;
    kit.sources.scripted("src", (71..=80).map(json_n));
    let r3 = run_to_end(&kit, spec(&both), fast_opts()).await;
    assert_eq!(r3.exit, ExitStatus::Ok, "{:#?}", r3.status);
    assert_eq!(
        kit.rec.store("b").ns(),
        range(51, 80),
        "the restart must not evaluate `latest` again and skip 51..=70"
    );
    assert_eq!(kit.rec.store("a").ns(), range(1, 80));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_latest_sink_on_an_empty_wal_loses_nothing_after_a_restart() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let sinks = "  - { id: a, kind: recseg, start_from: latest }";
    let y = yaml(dir.path(), sinks, "");

    lock(&kit.rec.store("a").0).stall = true;
    kit.sources.scripted("src", (1..=20).map(json_n));
    run_to_end(&kit, spec(&y), quick_stop()).await;
    assert!(kit.rec.store("a").ns().is_empty());

    lock(&kit.rec.store("a").0).stall = false;
    kit.sources.scripted("src", (21..=30).map(json_n));
    let r = run_to_end(&kit, spec(&y), fast_opts()).await;
    assert_eq!(r.exit, ExitStatus::Ok, "{:#?}", r.status);
    assert_eq!(kit.rec.store("a").ns(), range(1, 30));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_sink_with_start_from_latest_reads_only_new_frames() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();

    kit.sources.scripted("src", (1..=50).map(json_n));
    let first = yaml(dir.path(), "  - { id: a, kind: recseg }", "");
    run_to_end(&kit, spec(&first), fast_opts()).await;

    kit.sources.scripted("src", (51..=60).map(json_n));
    let batch = kit
        .batch
        .memory("b", thadam_testkit::MemoryBatchSinkConfig::default());
    let sinks = "  - { id: a, kind: recseg }\n  - { id: b, kind: membatch, start_from: latest }";
    let r = run_to_end(&kit, spec(&yaml(dir.path(), sinks, "")), fast_opts()).await;
    assert_eq!(r.exit, ExitStatus::Ok, "{:#?}", r.status);

    let seen = batch.applied_events();
    assert_eq!(seen, 10, "only 51..=60 reach the batch sink");
    assert_eq!(
        cursor(&r.status, "b"),
        r.status.wal.as_ref().unwrap().durable_end
    );
}

fn padded(n: u64) -> Bytes {
    Bytes::from(format!("{{\"n\":{n},\"pad\":\"{}\"}}", "x".repeat(1000)))
}

struct Settled {
    first_seq: u64,
    segments: usize,
}

/// Captures about 3.5 MB through a fast required sink while `slow` (a stuck
/// sink with the given extra keys) is attached, with `retain_for` short so
/// consumed segments are eligible at once. Waits until the fast sink has read
/// everything, then until `done` holds or half a second of maintenance passes.
async fn capture_with_stuck_sink(slow_keys: &str, expect_deletion: bool) -> Settled {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let up = kit.sources.memory("src", MemorySourceConfig::default());
    lock(&kit.rec.store("slow").0).stall = true;
    let sinks = format!(
        "  - {{ id: fast, kind: recseg, config: {{ frames_per_file: 1 }} }}\n  - {{ id: slow, kind: recseg{slow_keys} }}"
    );
    let y = yaml(dir.path(), &sinks, "").replace("retain_for: 1h", "retain_for: 1ms");
    let p = Pipeline::build(spec(&y), &kit.registry(), quick_stop()).unwrap();
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    handle.wait_for(Phase::Running).await;

    let total = 3500u64;
    for n in 1..=total {
        up.publish(padded(n));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let s = handle.status();
        let end = s.wal.as_ref().map_or(0, |w| w.durable_end);
        if up.acked_seqs().len() as u64 == total && end > 0 && cursor(&s, "fast") == end {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{s:#?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let settle_until = tokio::time::Instant::now() + Duration::from_millis(500);
    let wal = loop {
        let wal = handle.status().wal.unwrap();
        let now = tokio::time::Instant::now();
        if (expect_deletion && wal.first_seq > 1) || now >= settle_until {
            break wal;
        }
        assert!(
            !expect_deletion || now < deadline,
            "retention never deleted a segment: {wal:#?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    shutdown.trigger();
    task.await.unwrap().unwrap();
    Settled {
        first_seq: wal.first_seq,
        segments: wal.segments,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_required_sink_pins_wal_retention() {
    let s = capture_with_stuck_sink("", false).await;
    assert_eq!(s.first_seq, 1, "nothing may be deleted past the stuck sink");
    assert!(
        s.segments >= 3,
        "the data spans several segments: {}",
        s.segments
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_optional_sink_does_not_pin_wal_retention() {
    let s = capture_with_stuck_sink(", required: false", true).await;
    assert!(
        s.first_seq > 1,
        "consumed segments were deleted: {}",
        s.first_seq
    );
}
