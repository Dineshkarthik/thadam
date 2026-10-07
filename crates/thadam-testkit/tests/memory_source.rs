//! Behaviour of the memory source beyond what the conformance suite asserts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use thadam_core::{
    AckMode, AckOutcome, AckStatus, BatchLimits, ErrorDomain, ThadamError, Shutdown, Source,
    SourceContext, SourceId, codes,
};
use thadam_testkit::{
    FakeClock, MemorySource, MemorySourceConfig, MemoryUpstream, id_payload, payload_id,
};

fn ctx() -> SourceContext {
    SourceContext::new(
        SourceId::new("mem"),
        Shutdown::new(),
        FakeClock::default().shared(),
    )
}

fn limits(max_messages: usize, linger_ms: u64) -> BatchLimits {
    BatchLimits {
        max_messages,
        linger: Duration::from_millis(linger_ms),
        ..BatchLimits::default()
    }
}

async fn opened(cfg: MemorySourceConfig) -> (MemorySource, MemoryUpstream) {
    let (mut source, upstream) = MemorySource::new(cfg);
    source.open(&ctx()).await.unwrap();
    (source, upstream)
}

#[tokio::test(start_paused = true)]
async fn scripted_source_drains_then_ends() {
    let (mut source, upstream) = MemorySource::counting(5, MemorySourceConfig::default());
    source.open(&ctx()).await.unwrap();
    let batch = source.next_batch(limits(3, 0)).await.unwrap().unwrap();
    assert_eq!(batch.batch.len(), 3);
    assert_eq!(batch.batch.source, SourceId::new("mem"));
    assert!(batch.batch.clock_synced);
    batch.ack.complete(AckOutcome::Committed);
    let batch = source.next_batch(limits(3, 0)).await.unwrap().unwrap();
    assert_eq!(batch.batch.len(), 2);
    batch.ack.complete(AckOutcome::Committed);
    assert!(source.next_batch(limits(3, 0)).await.unwrap().is_none());
    assert_eq!(upstream.acked_seqs(), vec![1, 2, 3, 4, 5]);
    assert_eq!(upstream.unacked_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn linger_grows_batches_under_load_and_stays_small_when_idle() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    upstream.publish(id_payload(0));
    let publisher = {
        let upstream = upstream.clone();
        tokio::spawn(async move {
            for id in 1..4 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                upstream.publish(id_payload(id));
            }
        })
    };
    let batch = source.next_batch(limits(100, 50)).await.unwrap().unwrap();
    publisher.await.unwrap();
    assert_eq!(
        batch.batch.len(),
        4,
        "arrivals within the linger window share a batch"
    );
    batch.ack.complete(AckOutcome::Committed);

    upstream.publish(id_payload(9));
    let started = tokio::time::Instant::now();
    let batch = source.next_batch(limits(100, 5)).await.unwrap().unwrap();
    assert_eq!(batch.batch.len(), 1);
    assert!(
        started.elapsed() <= Duration::from_millis(5),
        "an idle source waits at most the linger"
    );
    batch.ack.complete(AckOutcome::Committed);
}

#[tokio::test(start_paused = true)]
async fn a_full_batch_returns_without_waiting_out_the_linger() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    for id in 0..10 {
        upstream.publish(id_payload(id));
    }
    let started = tokio::time::Instant::now();
    let batch = source.next_batch(limits(4, 10_000)).await.unwrap().unwrap();
    assert_eq!(batch.batch.len(), 4);
    assert!(started.elapsed() < Duration::from_secs(1));
    batch.ack.complete(AckOutcome::Committed);
}

#[tokio::test(start_paused = true)]
async fn max_bytes_bounds_a_batch_but_a_single_large_message_still_flows() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    upstream.publish(Bytes::from(vec![1u8; 100]));
    upstream.publish(Bytes::from(vec![2u8; 100]));
    upstream.publish(Bytes::from(vec![3u8; 100]));
    let tight = BatchLimits {
        max_bytes: 150,
        linger: Duration::ZERO,
        ..BatchLimits::default()
    };
    assert_eq!(
        source.next_batch(tight).await.unwrap().unwrap().batch.len(),
        1
    );
    let tiny = BatchLimits {
        max_bytes: 10,
        ..tight
    };
    assert_eq!(
        source.next_batch(tiny).await.unwrap().unwrap().batch.len(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn open_failures_are_returned_in_order_then_open_succeeds() {
    let boom =
        |n: &str| ThadamError::transient(ErrorDomain::Connector, codes::CONNECTOR_CONNECT_FAILED, n);
    let (mut source, _upstream) = MemorySource::new(MemorySourceConfig {
        open_failures: vec![boom("first"), boom("second")],
        ..MemorySourceConfig::default()
    });
    assert_eq!(source.open(&ctx()).await.unwrap_err().message, "first");
    assert_eq!(source.open(&ctx()).await.unwrap_err().message, "second");
    source.open(&ctx()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn next_batch_before_open_is_a_permanent_error() {
    let (mut source, _upstream) = MemorySource::new(MemorySourceConfig::default());
    let err = source.next_batch(BatchLimits::default()).await.unwrap_err();
    assert_eq!(err.retry, thadam_core::Retry::Permanent);
}

#[tokio::test(start_paused = true)]
async fn scripted_next_batch_errors_surface_once() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    upstream.publish(id_payload(1));
    upstream.fail_next_batch(ThadamError::transient(
        ErrorDomain::Connector,
        codes::CONNECTOR_DISCONNECTED,
        "link down",
    ));
    assert_eq!(
        source.next_batch(limits(8, 0)).await.unwrap_err().code,
        codes::CONNECTOR_DISCONNECTED
    );
    assert_eq!(
        source
            .next_batch(limits(8, 0))
            .await
            .unwrap()
            .unwrap()
            .batch
            .len(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn on_receive_acknowledges_before_the_handle_completes() {
    let (mut source, upstream) = opened(MemorySourceConfig {
        ack_mode: AckMode::OnReceive,
        ..MemorySourceConfig::default()
    })
    .await;
    assert_eq!(source.capabilities().ack_mode, AckMode::OnReceive);
    upstream.publish(id_payload(1));
    let batch = source.next_batch(limits(8, 0)).await.unwrap().unwrap();
    assert_eq!(
        upstream.acked_seqs(),
        vec![1],
        "acked on receipt, before any commit"
    );
    batch.ack.complete(AckOutcome::Committed);
    assert_eq!(
        upstream.acked_seqs(),
        vec![1],
        "completing does not acknowledge twice"
    );
}

#[tokio::test(start_paused = true)]
async fn ack_mode_none_has_no_acknowledgements() {
    let (mut source, upstream) = opened(MemorySourceConfig {
        ack_mode: AckMode::None,
        ..MemorySourceConfig::default()
    })
    .await;
    upstream.publish(id_payload(1));
    let batch = source.next_batch(limits(8, 0)).await.unwrap().unwrap();
    assert_eq!(
        batch.ack.complete(AckOutcome::Committed),
        AckStatus::Delivered
    );
    assert!(upstream.acked_seqs().is_empty());
    assert_eq!(upstream.unacked_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn abandoned_batches_stay_unacknowledged_and_return_after_a_session_loss() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    upstream.publish(id_payload(1));
    let batch = source.next_batch(limits(8, 0)).await.unwrap().unwrap();
    batch.ack.complete(AckOutcome::Abandoned);
    assert!(upstream.acked_seqs().is_empty());
    assert_eq!(upstream.unacked_count(), 1);

    upstream.lose_session();
    let again = source.next_batch(limits(8, 0)).await.unwrap().unwrap();
    assert_eq!(payload_id(&again.batch.messages[0].payload), Some(1));
    assert_eq!(upstream.stats().session_losses, 1);
    again.ack.complete(AckOutcome::Committed);
    assert_eq!(upstream.acked_seqs(), vec![1]);
}

#[tokio::test(start_paused = true)]
async fn out_of_order_completion_is_visible_to_the_broker() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    for id in 0..2 {
        upstream.publish(id_payload(id));
    }
    let first = source.next_batch(limits(1, 0)).await.unwrap().unwrap();
    let second = source.next_batch(limits(1, 0)).await.unwrap().unwrap();
    second.ack.complete(AckOutcome::Committed);
    first.ack.complete(AckOutcome::Committed);
    assert_eq!(upstream.stats().out_of_order_acks, 1);
}

#[tokio::test(start_paused = true)]
async fn close_stops_the_protocol_task_and_pings_stop_counting_as_violations() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    tokio::time::sleep(Duration::from_secs(12)).await;
    let pings = upstream.stats().pings;
    assert!(pings >= 6, "{pings} pings in 12 s at a 5 s keepalive");
    source.close().await.unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    let stats = upstream.stats();
    assert_eq!(
        stats.pings, pings,
        "the protocol task kept running after close"
    );
    assert_eq!(
        stats.keepalive_violations, 0,
        "a closed connection is not a violation"
    );
}

#[tokio::test(start_paused = true)]
async fn the_source_can_move_between_tasks() {
    let (mut source, upstream) = opened(MemorySourceConfig::default()).await;
    upstream.publish(id_payload(1));
    let handle = tokio::spawn(async move {
        let batch = source.next_batch(limits(8, 0)).await.unwrap().unwrap();
        Arc::new(batch.batch.len())
    });
    assert_eq!(*handle.await.unwrap(), 1);
}
