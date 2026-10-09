//! Runs the `thadam-testkit` source conformance suite against the gRPC source.
//!
//! The suite drives the upstream side synchronously and inspects queue-level
//! hooks, so the fixture sits in-crate and reaches the queue directly. The
//! gRPC server has no protocol keepalive; the nominal interval below only
//! exists so the liveness clause has a window to measure.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use thadam_core::{Attributes, EpochGate};
use thadam_testkit::conformance::source::{
    Case, SourceFixture, Upstream, cancel_safety, epoch_scoped_handles, liveness, ordered_ack,
    run_all,
};

use crate::config::GrpcConfig;
use crate::queue::{Message, Shared};
use crate::source::GrpcSource;

const KEEPALIVE: Duration = Duration::from_millis(100);
const QUEUE_CAPACITY: usize = 100_000;

struct GrpcUpstream {
    queue: Arc<Shared>,
}

impl Upstream for GrpcUpstream {
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

struct GrpcFixture;

impl SourceFixture for GrpcFixture {
    fn create(&self) -> Case {
        let config = GrpcConfig::from_value(&serde_json::json!({
            "bind": "127.0.0.1:0",
            "queue_capacity": QUEUE_CAPACITY,
        }))
        .unwrap_or_else(|errors| panic!("fixture config: {errors:?}"));
        let source = GrpcSource::new(config);
        let queue = source.shared();
        Case {
            source: Box::new(source),
            upstream: Box::new(GrpcUpstream { queue }),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_passes_the_source_conformance_suite() {
    run_all(&GrpcFixture, 7).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_orders_acks_under_random_completion() {
    ordered_ack(&GrpcFixture, 99).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_survives_dropped_next_batch_futures() {
    cancel_safety(&GrpcFixture, 4242).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_keeps_liveness_while_stalled() {
    liveness(&GrpcFixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_makes_pre_loss_handles_inert() {
    epoch_scoped_handles(&GrpcFixture).await;
}
