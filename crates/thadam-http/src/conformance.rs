//! Runs the `thadam-testkit` source conformance suite against listen mode.
//!
//! The suite drives the upstream side synchronously and inspects queue-level
//! hooks, so the fixture sits in-crate and reaches the queue directly. Listen
//! mode has no protocol keepalive; the nominal interval below only exists so
//! the liveness clause has a window to measure.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use thadam_core::{Attributes, EpochGate};
use thadam_testkit::conformance::source::{
    Case, SourceFixture, Upstream, cancel_safety, epoch_scoped_handles, liveness, ordered_ack,
    run_all,
};

use crate::config::HttpConfig;
use crate::queue::{Message, Shared};
use crate::source::HttpSource;

const KEEPALIVE: Duration = Duration::from_millis(100);
const QUEUE_CAPACITY: usize = 100_000;

struct ListenUpstream {
    queue: Arc<Shared>,
}

impl Upstream for ListenUpstream {
    fn publish(&self, payload: Bytes) {
        let message = Message {
            payload,
            attrs: Attributes::new(),
            reply: None,
        };
        assert!(
            self.queue.try_push(message).is_ok(),
            "conformance queue full"
        );
    }

    fn finish(&self) {
        self.queue.finish();
    }

    fn lose_session(&self) {
        self.queue.lose_session();
    }

    fn acked_payloads(&self) -> Vec<Bytes> {
        self.queue.acked()
    }

    fn keepalive(&self) -> Duration {
        KEEPALIVE
    }

    fn keepalive_violations(&self) -> u64 {
        0
    }

    fn epoch_gate(&self) -> Option<EpochGate> {
        Some(self.queue.gate().clone())
    }
}

struct ListenFixture;

impl SourceFixture for ListenFixture {
    fn create(&self) -> Case {
        let config = HttpConfig::from_value(&serde_json::json!({
            "listen": { "bind": "127.0.0.1:0" },
            "queue_capacity": QUEUE_CAPACITY,
        }))
        .unwrap_or_else(|errors| panic!("fixture config: {errors:?}"));
        let source = HttpSource::new(config);
        let queue = source.shared();
        Case {
            source: Box::new(source),
            upstream: Box::new(ListenUpstream { queue }),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_passes_the_source_conformance_suite() {
    run_all(&ListenFixture, 7).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_orders_acks_under_random_completion() {
    ordered_ack(&ListenFixture, 99).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_survives_dropped_next_batch_futures() {
    cancel_safety(&ListenFixture, 4242).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_keeps_liveness_while_stalled() {
    liveness(&ListenFixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_makes_pre_loss_handles_inert() {
    epoch_scoped_handles(&ListenFixture).await;
}
