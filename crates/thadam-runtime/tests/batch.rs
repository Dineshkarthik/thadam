//! Batch-sink delivery: same batch id on retry, bisection, quarantine of
//! poison events, and re-sending the in-flight batch after a crash.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::*;
use thadam_core::{
    BatchId, BatchSink, DeliveryClass, ErrorDomain, Retry, SinkBatch, SinkCapabilities,
    SinkContext, ThadamError, WriteOutcome, codes,
};
use thadam_runtime::{ExitStatus, RuntimeOptions, failpoints};
use thadam_testkit::{Fault, MemoryBatchSinkConfig, TempDataDir};

#[derive(Default)]
struct PoisonState {
    poison: Vec<u64>,
    retry: Option<Retry>,
    transient_left: u32,
    applied: Vec<u64>,
    attempts: Vec<BatchId>,
}

#[derive(Clone, Default)]
struct Poison(Arc<Mutex<PoisonState>>);

#[async_trait]
impl BatchSink for Poison {
    async fn open(&mut self, _ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        Ok(SinkCapabilities::new(DeliveryClass::IdempotentBatch))
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        let mut st = lock(&self.0);
        st.attempts.push(batch.id());
        let ns: Vec<u64> = batch.events().iter().filter_map(event_n).collect();
        if ns.iter().any(|n| st.poison.contains(n)) {
            match st.retry {
                Some(Retry::Transient) if st.transient_left > 0 => {
                    st.transient_left -= 1;
                    return Err(ThadamError::transient(
                        ErrorDomain::Sink,
                        codes::SINK_TIMEOUT,
                        "flaky",
                    ));
                }
                Some(Retry::Permanent) => {
                    return Err(ThadamError::permanent(
                        ErrorDomain::Sink,
                        codes::SINK_REJECTED,
                        "poison row",
                    ));
                }
                _ => {}
            }
        }
        st.applied.extend(ns);
        Ok(WriteOutcome::default())
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        Ok(())
    }
}

fn opts(max_events: usize) -> RuntimeOptions {
    let mut o = fast_opts();
    o.batch_limits.max_messages = 1;
    o.batch_max_events = max_events;
    o.max_attempts_before_bisect = 3;
    o
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_poison_is_bisected_out_and_quarantined() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=64).map(json_n));
    let sink = Poison::default();
    {
        let mut st = lock(&sink.0);
        st.poison = vec![13, 40];
        st.retry = Some(Retry::Permanent);
    }
    kit.batch.put("ch", sink.clone());
    let report = run_to_end(
        &kit,
        spec(&yaml(dir.path(), "  - { id: ch, kind: membatch }", "")),
        opts(16),
    )
    .await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    let st = lock(&sink.0);
    let mut applied = st.applied.clone();
    applied.sort_unstable();
    let want: Vec<u64> = (1..=64).filter(|n| *n != 13 && *n != 40).collect();
    assert_eq!(applied, want, "every good event exactly once");
    let q = kit.quarantine_rows("ch");
    assert_eq!(q.len(), 2, "{q:#?}");
    assert!(
        q.iter().all(|r| r.values.contains(codes::SINK_REJECTED)),
        "{q:#?}"
    );
    let c = component(&report.status, "sink:ch");
    assert!(c.counters.bisects >= 2, "{c:#?}");
    assert_eq!(c.counters.quarantined, 2);
    let single: Vec<_> = st
        .attempts
        .iter()
        .filter(|id| id.range().len() == 1)
        .collect();
    assert!(
        single.iter().any(|id| id.range().first().get() == 13),
        "{:?}",
        st.attempts
    );
    assert_eq!(
        cursor(&report.status, "ch"),
        report.status.wal.as_ref().unwrap().durable_end
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_transient_failures_bisect_and_then_deliver_everything() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=32).map(json_n));
    let sink = Poison::default();
    {
        let mut st = lock(&sink.0);
        st.poison = vec![7];
        st.retry = Some(Retry::Transient);
        st.transient_left = 6;
    }
    kit.batch.put("ch", sink.clone());
    let report = run_to_end(
        &kit,
        spec(&yaml(dir.path(), "  - { id: ch, kind: membatch }", "")),
        opts(16),
    )
    .await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    let st = lock(&sink.0);
    let mut applied = st.applied.clone();
    applied.sort_unstable();
    assert_eq!(applied, (1..=32).collect::<Vec<_>>());
    assert!(
        kit.quarantine_rows("ch").is_empty(),
        "transient failures never quarantine"
    );
    let first = st.attempts[0];
    assert_eq!(
        &st.attempts[..3],
        &[first, first, first],
        "the same batch id on every retry"
    );
    assert!(component(&report.status, "sink:ch").counters.bisects >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_outcomes_retry_the_same_batch_id_per_delivery_class() {
    for (class, expect) in [
        (DeliveryClass::IdempotentBatch, 40),
        (DeliveryClass::AtLeastOnce, 40 + 40),
    ] {
        let dir = TempDataDir::new().unwrap();
        let kit = Kit::default();
        kit.sources.scripted("src", (1..=40).map(json_n));
        let h = kit.batch.memory(
            "ch",
            MemoryBatchSinkConfig {
                delivery: class,
                ..MemoryBatchSinkConfig::default()
            },
        );
        h.inject(Fault::AppliedThenTransient);
        let mut o = opts(1000);
        o.batch_max_age = std::time::Duration::from_secs(5);
        let report = run_to_end(
            &kit,
            spec(&yaml(dir.path(), "  - { id: ch, kind: membatch }", "")),
            o,
        )
        .await;
        assert_eq!(report.exit, ExitStatus::Ok);
        let ids = h.attempt_ids();
        assert_eq!(ids[0], ids[1], "{ids:?}");
        assert_eq!(h.applied_events(), expect, "{class:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_flight_batch_is_resent_first_with_the_same_id_after_a_crash() {
    for point in [
        failpoints::BATCH_AFTER_INFLIGHT,
        failpoints::BATCH_AFTER_WRITE,
    ] {
        let dir = TempDataDir::new().unwrap();
        let kit = Kit::default();
        let y = yaml(dir.path(), "  - { id: ch, kind: membatch }", "");
        kit.sources.scripted("src", (1..=50).map(json_n));
        let first_sink = kit.batch.memory("ch", MemoryBatchSinkConfig::default());
        let o = opts(8);
        o.failpoints.arm(point, 1);
        let first = run_to_end(&kit, spec(&y), o).await;
        assert_eq!(first.exit, ExitStatus::RuntimeFailure, "{point}");
        let inflight = first
            .status
            .cursors
            .iter()
            .find(|c| c.sink == "ch")
            .and_then(|c| c.inflight)
            .unwrap_or_else(|| {
                panic!("{point}: no in-flight range in {:#?}", first.status.cursors)
            });
        let expected_first_ids = first_sink.attempt_ids();

        kit.sources.scripted("src", std::iter::empty());
        let second_sink = kit.batch.memory("ch", MemoryBatchSinkConfig::default());
        let second = run_to_end(&kit, spec(&y), opts(8)).await;
        assert_eq!(second.exit, ExitStatus::Ok, "{point}: {:#?}", second.status);
        let resent = second_sink.attempt_ids()[0].range();
        assert_eq!(
            (resent.first().get(), resent.last().get()),
            inflight,
            "{point}"
        );
        if point == failpoints::BATCH_AFTER_WRITE {
            assert_eq!(
                expected_first_ids.last().unwrap().range(),
                resent,
                "{point}"
            );
        }
        let total = first_sink.applied_events() + second_sink.applied_events();
        let dup = if point == failpoints::BATCH_AFTER_WRITE {
            resent.len() as usize
        } else {
            0
        };
        assert_eq!(
            total,
            50 + dup,
            "{point}: only the in-flight batch may repeat"
        );
        assert_eq!(
            second_sink.applied_events() + first_sink.applied_events() - dup,
            50
        );
    }
}
