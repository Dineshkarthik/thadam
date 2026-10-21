//! Behaviour of the memory batch sink beyond what the suite asserts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thadam_core::{
    BatchSink, DeliveryClass, DiskBudget, ErrorDomain, Retry, Shutdown, SinkBatch, SinkContext,
    SinkId, ThadamError, codes,
};
use thadam_testkit::{
    FakeClock, Fault, MemoryBatchSink, MemoryBatchSinkConfig, MemoryBatchSinkHandle, test_batch,
};

fn ctx() -> SinkContext {
    SinkContext::new(
        SinkId::new("mem"),
        Shutdown::new(),
        FakeClock::default().shared(),
        PathBuf::new(),
        Arc::new(DiskBudget::new()),
    )
}

async fn opened(cfg: MemoryBatchSinkConfig) -> (MemoryBatchSink, MemoryBatchSinkHandle) {
    let (mut sink, handle) = MemoryBatchSink::new(cfg);
    sink.open(&ctx()).await.unwrap();
    (sink, handle)
}

fn batch(first: u64, last: u64) -> SinkBatch {
    SinkBatch::new(test_batch(1, first, last))
}

#[tokio::test]
async fn open_reports_the_configured_delivery_class() {
    for delivery in [DeliveryClass::AtLeastOnce, DeliveryClass::IdempotentBatch] {
        let (mut sink, handle) = MemoryBatchSink::new(MemoryBatchSinkConfig {
            delivery,
            ..MemoryBatchSinkConfig::default()
        });
        assert!(!handle.is_open());
        assert_eq!(sink.open(&ctx()).await.unwrap().delivery, delivery);
        assert!(handle.is_open());
        sink.close().await.unwrap();
        assert!(handle.is_closed());
    }
}

#[tokio::test]
async fn open_can_be_made_to_fail() {
    let err = ThadamError::fatal(ErrorDomain::Auth, codes::AUTH_FAILED, "bad token");
    let (mut sink, _) = MemoryBatchSink::new(MemoryBatchSinkConfig {
        open_error: Some(err.clone()),
        ..MemoryBatchSinkConfig::default()
    });
    assert_eq!(sink.open(&ctx()).await.unwrap_err(), err);
}

#[tokio::test]
async fn write_before_open_is_refused() {
    let (mut sink, _) = MemoryBatchSink::new(MemoryBatchSinkConfig::default());
    assert!(sink.write(&batch(1, 1)).await.is_err());
}

#[tokio::test]
async fn faults_are_consumed_one_per_write_in_order() {
    let (mut sink, handle) = opened(MemoryBatchSinkConfig::default()).await;
    handle.inject(Fault::Fail(Retry::Transient));
    handle.inject(Fault::Fail(Retry::Permanent));
    handle.inject(Fault::Fail(Retry::Fatal));
    let b = batch(1, 2);
    let classes: Vec<_> = vec![
        sink.write(&b).await.unwrap_err().retry,
        sink.write(&b).await.unwrap_err().retry,
        sink.write(&b).await.unwrap_err().retry,
    ];
    assert_eq!(
        classes,
        vec![Retry::Transient, Retry::Permanent, Retry::Fatal]
    );
    sink.write(&b).await.unwrap();
    assert_eq!(handle.applied_events(), 2);
    assert_eq!(handle.attempt_ids().len(), 4);
}

#[tokio::test]
async fn a_lost_response_applies_the_batch_once_for_idempotent_and_twice_for_at_least_once() {
    for (delivery, expected) in [
        (DeliveryClass::IdempotentBatch, 3),
        (DeliveryClass::AtLeastOnce, 6),
    ] {
        let (mut sink, handle) = opened(MemoryBatchSinkConfig {
            delivery,
            ..MemoryBatchSinkConfig::default()
        })
        .await;
        handle.inject(Fault::AppliedThenTransient);
        let b = batch(1, 3);
        assert!(sink.write(&b).await.unwrap_err().is_transient());
        sink.write(&b).await.unwrap();
        assert_eq!(handle.applied_events(), expected, "{delivery:?}");
        assert_eq!(handle.applied_batches().len(), expected / 3);
    }
}

#[tokio::test]
async fn rejected_events_are_reported_and_not_stored() {
    let (mut sink, handle) = opened(MemoryBatchSinkConfig::default()).await;
    handle.reject_events_where(|e| {
        (e.wal_pos().get() % 2 == 0).then(|| {
            ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_REJECTED,
                "even frames are refused",
            )
        })
    });
    let outcome = sink.write(&batch(1, 5)).await.unwrap();
    let indices: Vec<u32> = outcome.rejected.iter().map(|r| r.index).collect();
    assert_eq!(indices, vec![1, 3]);
    assert_eq!(handle.applied_events(), 3);
}

#[tokio::test(start_paused = true)]
async fn latency_and_delay_faults_take_virtual_time() {
    let (mut sink, handle) = opened(MemoryBatchSinkConfig {
        latency: Duration::from_millis(20),
        ..MemoryBatchSinkConfig::default()
    })
    .await;
    handle.inject(Fault::Delay(Duration::from_millis(100)));
    let started = tokio::time::Instant::now();
    sink.write(&batch(1, 1)).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_millis(120));
    let started = tokio::time::Instant::now();
    sink.write(&batch(2, 2)).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_millis(20));
}
