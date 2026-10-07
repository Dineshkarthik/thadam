//! Panic isolation at the task boundary and inside the chain, and bounded
//! memory behind a stalled sink.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use thadam_core::{Shutdown, WalPos, codes};
use thadam_runtime::{ComponentState, ExitStatus, Phase, Pipeline};
use thadam_testkit::{MemorySourceConfig, TempDataDir};

fn one_message_per_frame() -> thadam_runtime::RuntimeOptions {
    let mut o = fast_opts();
    o.batch_limits.max_messages = 1;
    o
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_sink_fails_alone_and_loses_nothing() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let up = kit.sources.scripted("src", (1..=80).map(json_n));
    lock(&kit.rec.store("a").0).panic_on_n = Some(30);
    let sinks =
        "  - { id: a, kind: recseg, config: { frames_per_file: 4 } }\n  - { id: b, kind: recseg }";
    let y = yaml(dir.path(), sinks, "");
    let report = run_to_end(&kit, spec(&y), one_message_per_frame()).await;

    assert_eq!(report.exit, ExitStatus::RuntimeFailure);
    match &component(&report.status, "sink:a").state {
        ComponentState::Failed { error } => assert_eq!(error.code, codes::SINK_PANIC, "{error:?}"),
        other => panic!("sink:a is {other:?}"),
    }
    assert_eq!(component(&report.status, "sink:a").counters.panics, 1);
    assert_eq!(kit.rec.store("b").ns(), (1..=80).collect::<Vec<_>>());
    assert_eq!(up.acked_seqs().len(), 80, "capture is unaffected");
    assert!(cursor(&report.status, "a") < 30);

    kit.sources.scripted("src", std::iter::empty());
    let again = run_to_end(&kit, spec(&y), one_message_per_frame()).await;
    assert_eq!(again.exit, ExitStatus::Ok, "{:#?}", again.status);
    assert_eq!(
        kit.rec.store("a").ns(),
        (1..=80).collect::<Vec<_>>(),
        "no loss, no duplicates"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poison_frame_is_quarantined_after_retries_and_a_transient_panic_is_not() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=40).map(json_n));
    let mut o = one_message_per_frame();
    o.poison_retries = 2;
    o.failpoints.panic_chain_at(WalPos::new(5), 100);
    o.failpoints.panic_chain_at(WalPos::new(9), 1);
    let report = run_to_end(
        &kit,
        spec(&yaml(dir.path(), "  - { id: out, kind: recseg }", "")),
        o,
    )
    .await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    let mut want: Vec<u64> = (1..=40).collect();
    want.retain(|n| *n != 5);
    assert_eq!(kit.rec.store("out").ns(), want);
    let q = kit.quarantine_rows("out");
    assert_eq!(q.len(), 1, "{q:#?}");
    assert!(q[0].values.contains(codes::TRANSFORM_PANIC), "{q:#?}");
    assert!(q[0].values.contains("_wal_pos=U64(5)"), "{q:#?}");
    let chain = component(&report.status, "chain:out");
    assert!(chain.counters.panics >= 4, "{chain:#?}");
    assert_eq!(chain.state, ComponentState::Stopped);
    assert_eq!(
        cursor(&report.status, "out"),
        report.status.wal.as_ref().unwrap().durable_end
    );
}

fn padded(n: u64) -> Bytes {
    Bytes::from(format!("{{\"n\":{n},\"pad\":\"{}\"}}", "x".repeat(1000)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_sink_holds_bounded_memory_while_capture_continues() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let up = kit.sources.memory("src", MemorySourceConfig::default());
    lock(&kit.rec.store("slow").0).stall = true;
    let mut o = fast_opts();
    o.read_batch_bytes = 16 << 10;
    o.batch_limits.max_messages = 4;
    o.drain_timeout = Duration::from_millis(100);
    o.shutdown_timeout = Duration::from_millis(500);
    let sinks = "  - { id: slow, kind: recseg }\n  - { id: fast, kind: recseg, config: { frames_per_file: 1 } }";
    let p = Pipeline::build(spec(&yaml(dir.path(), sinks, "")), &kit.registry(), o).unwrap();
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    handle.wait_for(Phase::Running).await;

    let total = 2000u64;
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
    tokio::time::sleep(Duration::from_millis(50)).await;
    let s = handle.status();
    let slow = component(&s, "sink:slow");
    let chain = component(&s, "chain:slow");
    let lane_cap = 2 * (16u64 << 10);
    assert!(slow.counters.queued_bytes <= lane_cap, "{slow:#?}");
    let captured = total * 1000;
    assert!(
        chain.counters.bytes < captured / 10,
        "the stalled chain read {} of {captured} bytes",
        chain.counters.bytes
    );
    assert_eq!(cursor(&s, "slow"), 0);
    assert_eq!(kit.rec.store("fast").ns().len() as u64, total);

    shutdown.trigger();
    let report = task.await.unwrap().unwrap();
    assert_eq!(
        report.exit,
        ExitStatus::Interrupted,
        "the stuck append is aborted at the deadline"
    );
}
