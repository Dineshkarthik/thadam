//! The memory source must pass its own suite, and the suite must catch a source
//! that breaks each clause.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use thadam_core::AckMode;
use thadam_testkit::conformance::source::{self, MemorySourceFixture};
use thadam_testkit::{MemorySourceConfig, NextBatchMode, ProtocolTask};

#[tokio::test(start_paused = true)]
async fn memory_source_passes_every_clause() {
    source::run_all(&MemorySourceFixture::default(), 7).await;
}

#[tokio::test(start_paused = true)]
async fn memory_source_passes_across_seeds() {
    let fixture = MemorySourceFixture::default();
    for seed in [0, 1, 2, 99, 12345] {
        source::ordered_ack(&fixture, seed).await;
        source::cancel_safety(&fixture, seed).await;
    }
}

#[tokio::test(start_paused = true)]
async fn memory_source_passes_with_a_short_keepalive() {
    let fixture = MemorySourceFixture {
        config: MemorySourceConfig {
            keepalive: std::time::Duration::from_millis(30),
            ..MemorySourceConfig::default()
        },
    };
    source::liveness(&fixture).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "missed keepalive pings")]
async fn a_protocol_task_driven_by_the_consumer_fails_liveness() {
    let fixture = MemorySourceFixture {
        config: MemorySourceConfig {
            protocol_task: ProtocolTask::DrivenByConsumer,
            ..MemorySourceConfig::default()
        },
    };
    source::liveness(&fixture).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "lost or duplicated after dropping next_batch")]
async fn a_next_batch_that_dequeues_before_waiting_fails_cancel_safety() {
    let fixture = MemorySourceFixture {
        config: MemorySourceConfig {
            next_batch_mode: NextBatchMode::DequeueThenWait,
            ..MemorySourceConfig::default()
        },
    };
    source::cancel_safety(&fixture, 3).await;
}

#[tokio::test(start_paused = true)]
async fn the_broken_sources_still_pass_the_clauses_they_do_not_break() {
    let consumer_driven = MemorySourceFixture {
        config: MemorySourceConfig {
            protocol_task: ProtocolTask::DrivenByConsumer,
            ..MemorySourceConfig::default()
        },
    };
    source::ordered_ack(&consumer_driven, 1).await;
    source::cancel_safety(&consumer_driven, 1).await;
    source::epoch_scoped_handles(&consumer_driven).await;

    let dequeue_first = MemorySourceFixture {
        config: MemorySourceConfig {
            next_batch_mode: NextBatchMode::DequeueThenWait,
            ..MemorySourceConfig::default()
        },
    };
    source::liveness(&dequeue_first).await;
}

#[test]
fn on_receive_is_a_distinct_ack_mode() {
    assert_ne!(AckMode::OnReceive, AckMode::OnCommit);
}
