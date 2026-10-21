//! Docker integration tests for the Kafka sink, against `apache/kafka`.
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker; run
//! them with
//!
//! ```text
//! cargo nextest run -p thadam-kafka --run-ignored only -E 'binary(docker_kafka)'
//! ```
//!
//! Each test starts its own single-node KRaft broker (labelled
//! `thadam-kafka-test`) and removes it when the test ends, including on
//! panic. Logs: run with `RUST_LOG=debug`. The broker's heap is capped at
//! 512 MiB; the nextest config runs these tests two at a time.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;
#[path = "docker_kafka/secure.rs"]
mod secure;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::*;
use rskafka::client::partition::UnknownTopicHandling;
use serde_json::json;
use tempfile::TempDir;
use thadam_core::{BatchSink, DeliveryClass, ErrorDomain, Retry, SinkBatch, codes};
use thadam_kafka::sink::EVENT_ID_HEADER;

const TOPIC: &str = "telemetry.imu.r7";
const RETRY_TIMEOUT: Duration = Duration::from_secs(120);

/// Ids of events at positions `0..count`.
fn want_ids(count: u64) -> BTreeSet<String> {
    (0..count).map(|p| event_id(p).to_string()).collect()
}

/// Writes `batch` until it succeeds, the way the runtime retries: a
/// transient error is retried, anything else fails the test.
async fn retry_until_ok(sink: &mut dyn BatchSink, batch: &SinkBatch) {
    let started = Instant::now();
    loop {
        match sink.write(batch).await {
            Ok(outcome) => {
                assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
                return;
            }
            Err(e) => {
                assert!(e.is_transient(), "a retry must stay transient: {e}");
                assert!(
                    started.elapsed() < RETRY_TIMEOUT,
                    "the sink never recovered"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn publishes_and_consumes_back() {
    init_tracing();
    let broker = Broker::start(Opts::new("basic")).await;
    let client = broker.client().await;
    create_topic(&client, TOPIC, 3).await;

    let mut sink = build_sink(&broker, json!({}));
    let dir = TempDir::new().expect("tempdir");
    let caps = sink.open(&sink_context(&dir)).await.expect("open");
    assert_eq!(caps.delivery, DeliveryClass::AtLeastOnce);

    let outcome = sink.write(&make_batch(0, 5, "r7")).await.expect("write");
    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);

    let stored = read_all(&client, TOPIC).await;
    assert_eq!(stored.len(), 5, "one record per event");
    let mut seen = BTreeSet::new();
    for record in &stored {
        let id = key_of(record);
        assert_eq!(
            record.headers.get(EVENT_ID_HEADER).map(Vec::as_slice),
            Some(id.as_bytes()),
            "the event id header repeats the key"
        );
        let doc: serde_json::Value =
            serde_json::from_slice(record.value.as_ref().expect("value")).expect("json value");
        assert_eq!(doc["_event_id"], id.as_str());
        assert_eq!(doc["_quality_status"], "good");
        assert_eq!(doc["robot"], "r7");
        let pos = (0..5u64)
            .find(|p| event_id(*p).to_string() == id)
            .expect("a known event id");
        assert_eq!(doc["note"], format!("m{pos}"));
        seen.insert(id);
    }
    assert_eq!(seen, want_ids(5));
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn gzip_and_snappy_batches_are_read_back_intact() {
    init_tracing();
    let broker = Broker::start(Opts::new("codecs")).await;
    let client = broker.client().await;
    let dir = TempDir::new().expect("tempdir");

    for (codec, robot) in [("gzip", "g1"), ("snappy", "s1")] {
        let topic = format!("telemetry.imu.{robot}");
        create_topic(&client, &topic, 1).await;
        let mut sink = build_sink(&broker, json!({"compression": codec}));
        sink.open(&sink_context(&dir)).await.expect("open");
        let outcome = sink.write(&make_batch(0, 4, robot)).await.expect("write");
        assert!(
            outcome.rejected.is_empty(),
            "{codec}: {:?}",
            outcome.rejected
        );

        let stored = read_all(&client, &topic).await;
        assert_eq!(stored.len(), 4, "{codec}");
        for record in &stored {
            let doc: serde_json::Value =
                serde_json::from_slice(record.value.as_ref().expect("value")).expect("json value");
            assert_eq!(doc["_event_id"], key_of(record).as_str(), "{codec}");
            assert_eq!(doc["robot"], robot, "{codec}");
        }
        sink.close().await.expect("close");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn broker_stop_start_retries_the_batch_at_least_once() {
    init_tracing();
    let broker = Broker::start(Opts::new("outage")).await;
    let client = broker.client().await;
    create_topic(&client, TOPIC, 3).await;

    let mut sink = build_sink(&broker, json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");
    sink.write(&make_batch(0, 3, "r7"))
        .await
        .expect("first write");

    broker.stop();
    let second = make_batch(3, 3, "r7");
    let e = sink.write(&second).await.expect_err("the broker is down");
    assert!(e.is_transient(), "an outage must be transient: {e}");

    broker.start_again().await;
    retry_until_ok(&mut *sink, &second).await;

    // At-least-once: every event is stored; a retried batch may land twice.
    let stored = read_all(&client, TOPIC).await;
    let ids: BTreeSet<String> = stored.iter().map(key_of).collect();
    assert_eq!(ids, want_ids(6), "every event stored");
    assert!(stored.len() >= 6, "{} records", stored.len());
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn produce_stalled_mid_batch_is_an_unknown_outcome_retried_at_least_once() {
    init_tracing();
    let broker = Broker::start(Opts::new("stall")).await;
    let client = broker.client().await;
    // One partition: the first write warms the only producer, so the stalled
    // write is exactly one produce request in flight.
    create_topic(&client, TOPIC, 1).await;

    let mut sink = build_sink(&broker, json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");
    sink.write(&make_batch(0, 3, "r7"))
        .await
        .expect("first write");

    // A frozen broker accepts the produce request but never answers it: the
    // sink times out with an unknown outcome. Once the broker resumes it may
    // still apply that request, so the retry can land the events twice.
    broker.pause();
    let second = make_batch(3, 3, "r7");
    let e = sink.write(&second).await.expect_err("the broker is frozen");
    broker.unpause();
    broker.wait_ready().await;
    assert!(e.is_transient(), "a stalled produce must be transient: {e}");
    assert_eq!(e.code, codes::SINK_TIMEOUT, "{e}");

    retry_until_ok(&mut *sink, &second).await;

    let stored = read_all(&client, TOPIC).await;
    let ids: BTreeSet<String> = stored.iter().map(key_of).collect();
    assert_eq!(ids, want_ids(6), "every event stored");
    assert!(
        (6..=9).contains(&stored.len()),
        "at most one extra copy per event of the stalled batch, got {}",
        stored.len()
    );
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn oversized_events_are_quarantined_and_their_neighbours_publish() {
    init_tracing();
    // The broker refuses records over 4 KiB; one partition puts all three
    // events of the batch in one produce request, so the refusal is isolated.
    let broker = Broker::start(Opts {
        message_max_bytes: Some(4096),
        ..Opts::new("oversize")
    })
    .await;
    let client = broker.client().await;
    create_topic(&client, TOPIC, 1).await;
    let dir = TempDir::new().expect("tempdir");

    let mut sink = build_sink(&broker, json!({}));
    sink.open(&sink_context(&dir)).await.expect("open");
    let batch = batch_of_notes(0, "r7", vec!["a".into(), "x".repeat(8192), "b".into()]);
    let outcome = sink.write(&batch).await.expect("write succeeds overall");
    assert_eq!(outcome.rejected.len(), 1, "{:?}", outcome.rejected);
    assert_eq!(outcome.rejected[0].index, 1);
    let e = &outcome.rejected[0].error;
    assert_eq!(
        (e.code, e.retry),
        (codes::SINK_REJECTED, Retry::Permanent),
        "{e}"
    );
    let stored = read_all(&client, TOPIC).await;
    assert_eq!(stored.len(), 2, "the neighbours are published");
    sink.close().await.expect("close");

    // The client-side limit rejects before anything reaches the broker.
    let mut sink = build_sink(&broker, json!({"max_message_bytes": 2048}));
    sink.open(&sink_context(&dir)).await.expect("open");
    let batch = batch_of_notes(10, "r7", vec!["c".into(), "y".repeat(3000), "d".into()]);
    let outcome = sink.write(&batch).await.expect("write succeeds overall");
    assert_eq!(outcome.rejected.len(), 1, "{:?}", outcome.rejected);
    assert_eq!(outcome.rejected[0].index, 1);
    let e = &outcome.rejected[0].error;
    assert_eq!(
        (e.code, e.retry),
        (codes::SINK_REJECTED, Retry::Permanent),
        "{e}"
    );
    assert_eq!(
        read_all(&client, TOPIC).await.len(),
        4,
        "the neighbours are published"
    );
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn missing_topic_is_a_fatal_config_error_when_the_broker_does_not_auto_create() {
    init_tracing();
    let broker = Broker::start(Opts {
        auto_create_topics: false,
        ..Opts::new("noauto")
    })
    .await;
    let client = broker.client().await;

    let mut sink = build_sink(&broker, json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");
    let e = sink
        .write(&make_batch(0, 2, "r7"))
        .await
        .expect_err("the topic is missing");
    assert_eq!(
        (e.domain, e.retry),
        (ErrorDomain::Config, Retry::Fatal),
        "{e}"
    );
    assert!(e.message.contains(TOPIC), "{e}");
    assert_eq!(partition_count(&client, TOPIC).await, None, "not created");
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn the_sink_never_creates_a_missing_topic_even_when_the_broker_auto_creates() {
    init_tracing();
    let broker = Broker::start(Opts::new("autocreate")).await;
    let client = broker.client().await;

    // Prove the broker auto-creates on a named lookup, so the sink's
    // restraint below is what keeps the topic from appearing.
    client
        .partition_client("autocreate-probe", 0, UnknownTopicHandling::Retry)
        .await
        .expect("a named lookup auto-creates on this broker");
    assert!(partition_count(&client, "autocreate-probe").await.is_some());

    let mut sink = build_sink(&broker, json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");
    let e = sink
        .write(&make_batch(0, 2, "r7"))
        .await
        .expect_err("the topic is missing");
    assert_eq!(
        (e.domain, e.retry),
        (ErrorDomain::Config, Retry::Fatal),
        "{e}"
    );
    assert_eq!(
        partition_count(&client, TOPIC).await,
        None,
        "not created by the sink"
    );
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn produce_after_the_topic_is_deleted_is_a_fatal_missing_topic_error() {
    init_tracing();
    let broker = Broker::start(Opts {
        auto_create_topics: false,
        ..Opts::new("deleted")
    })
    .await;
    let client = broker.client().await;
    create_topic(&client, TOPIC, 1).await;

    let mut sink = build_sink(&broker, json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");
    sink.write(&make_batch(0, 2, "r7"))
        .await
        .expect("first write");

    // The sink has cached the topic's partition count and producer, so the
    // next write goes straight to produce, which the broker answers with
    // UNKNOWN_TOPIC_OR_PARTITION once the deletion has completed.
    client
        .controller_client()
        .expect("controller client")
        .delete_topic(TOPIC, 10_000)
        .await
        .expect("delete topic");
    let started = Instant::now();
    while partition_count(&client, TOPIC).await.is_some() {
        assert!(started.elapsed() < RETRY_TIMEOUT, "topic never disappeared");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let e = sink
        .write(&make_batch(2, 2, "r7"))
        .await
        .expect_err("the topic was deleted");
    assert_eq!(
        (e.domain, e.retry),
        (ErrorDomain::Config, Retry::Fatal),
        "{e}"
    );
    assert!(e.message.contains(TOPIC), "{e}");
    assert!(e.message.contains("was deleted"), "{e}");
    sink.close().await.expect("close");
}
