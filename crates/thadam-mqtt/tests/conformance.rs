//! The full `thadam-testkit` source conformance suite against real brokers.
//!
//! These tests need Docker and are `#[ignore]`d so `cargo nextest run` passes on
//! a machine without it. Run them with
//! `cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(conformance)'`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::broker::{Broker, Kind, Opts};
use common::fixture::BrokerFixture;
use thadam_testkit::conformance::source;

async fn conforms(kind: Kind, v5: bool) {
    // The suite holds uncommitted batches while it keeps pulling, so the
    // broker's window has to be larger than anything it holds back.
    let opts = Opts {
        max_inflight: Some(1000),
        ..Opts::default()
    };
    let broker = tokio::task::spawn_blocking(move || Broker::start(kind, &opts))
        .await
        .unwrap();
    let fixture = BrokerFixture {
        addr: broker.addr(),
        v5,
    };
    source::run_all(&fixture, 7).await;
    source::ordered_ack(&fixture, 99).await;
    source::cancel_safety(&fixture, 4242).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (Mosquitto 2)"]
async fn mosquitto_passes_the_source_conformance_suite() {
    conforms(Kind::Mosquitto, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (EMQX 5.8.6)"]
async fn emqx_passes_the_source_conformance_suite() {
    conforms(Kind::Emqx, false).await;
}

#[cfg(feature = "v5")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (Mosquitto 2); experimental MQTT 5"]
async fn mosquitto_passes_the_source_conformance_suite_over_mqtt5() {
    conforms(Kind::Mosquitto, true).await;
}

#[cfg(feature = "v5")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (EMQX 5.8.6); experimental MQTT 5"]
async fn emqx_passes_the_source_conformance_suite_over_mqtt5() {
    conforms(Kind::Emqx, true).await;
}
