//! Shared helpers for the Docker-backed tests.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

pub mod broker;
pub mod certs;
pub mod fixture;
pub mod proxy;

use std::future::Future;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use thadam_core::{
    AckHandle, AckOutcome, BatchLimits, BuildContext, RawBatch, ThadamError, Shutdown, Source,
    SourceBatch, SourceContext, SourceId, SystemClock,
};
use thadam_mqtt::{MqttConfig, MqttHandle, MqttSource};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Outgoing, QoS};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

pub fn unique(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "{prefix}-{}-{}-{:x}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos())
    )
}

/// A source configuration for tests: a persistent session, QoS 1, a short
/// keepalive, and an `expected_broker_window` generous enough for every broker
/// configuration the tests use.
pub fn base_config(url: &str, client_id: &str, topic: &str) -> Value {
    json!({
        "url": url,
        "client_id": client_id,
        "topics": [{ "filter": topic, "qos": 1 }],
        "keep_alive": "2s",
        "connect_timeout": "5s",
        "open_timeout": "30s",
        "expected_broker_window": 1000,
    })
}

pub fn with(mut config: Value, key: &str, value: Value) -> Value {
    config[key] = value;
    config
}

pub fn build_source(config: &Value) -> (MqttSource, MqttHandle) {
    let config =
        MqttConfig::from_value(config).unwrap_or_else(|e| panic!("bad test config: {e:?}"));
    let ctx = BuildContext::new("mqtt-test", Arc::new(SystemClock::new()), PathBuf::new());
    let source = MqttSource::new(config, &ctx);
    let handle = source.handle();
    (source, handle)
}

pub fn context(shutdown: &Shutdown) -> SourceContext {
    SourceContext::new(
        SourceId::new("mqtt-test"),
        shutdown.clone(),
        Arc::new(SystemClock::new()),
    )
}

pub struct Opened {
    pub source: MqttSource,
    pub handle: MqttHandle,
    pub shutdown: Shutdown,
}

pub async fn try_open(config: &Value) -> (Opened, Result<(), ThadamError>) {
    let (mut source, handle) = build_source(config);
    let shutdown = Shutdown::new();
    let result = source.open(&context(&shutdown)).await;
    (
        Opened {
            source,
            handle,
            shutdown,
        },
        result,
    )
}

pub async fn open_source(config: &Value) -> Opened {
    let (opened, result) = try_open(config).await;
    if let Err(e) = result {
        panic!("open failed: {e}");
    }
    opened
}

/// Polls `condition` until it holds, panicking with `what` after `timeout`.
pub async fn eventually(timeout: Duration, what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?} waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn payload_ids(batch: &RawBatch) -> Vec<u64> {
    batch
        .messages
        .iter()
        .map(|m| thadam_testkit::payload_id(&m.payload).expect("test payload"))
        .collect()
}

/// A payload whose first eight bytes are `id` (as `thadam_testkit::id_payload`)
/// padded to `size` bytes.
pub fn sized_payload(id: u64, size: usize) -> Bytes {
    let mut v = vec![0xA5u8; size.max(16)];
    v[..8].copy_from_slice(&id.to_be_bytes());
    Bytes::from(v)
}

pub fn limits(max_messages: usize) -> BatchLimits {
    BatchLimits {
        max_messages,
        linger: Duration::from_millis(5),
        ..BatchLimits::default()
    }
}

/// A test publisher on its own connection to the broker.
pub struct Publisher {
    client: AsyncClient,
    qos: QoS,
    published: Arc<AtomicU64>,
    confirmed: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Publisher {
    pub fn connect(addr: &str, qos: u8) -> Publisher {
        let (host, port) = addr.rsplit_once(':').expect("host:port");
        let mut options = MqttOptions::new(unique("test-pub"), host, port.parse().expect("port"));
        options
            .set_keep_alive(Duration::from_secs(30))
            .set_inflight(1000)
            .set_clean_session(true)
            .set_max_packet_size(8 << 20, 8 << 20);
        let (client, mut event_loop) = AsyncClient::new(options, 4096);
        let qos = if qos == 0 {
            QoS::AtMostOnce
        } else {
            QoS::AtLeastOnce
        };
        let confirmed = Arc::new(AtomicU64::new(0));
        let counter = confirmed.clone();
        let task = tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::PubAck(_))) if qos == QoS::AtLeastOnce => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(_))) if qos == QoS::AtMostOnce => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
        Publisher {
            client,
            qos,
            published: Arc::new(AtomicU64::new(0)),
            confirmed,
            task,
        }
    }

    pub async fn publish(&self, topic: &str, payload: Bytes) {
        self.published.fetch_add(1, Ordering::Relaxed);
        self.client
            .publish_bytes(topic, self.qos, false, payload)
            .await
            .expect("queue publish");
    }

    pub async fn publish_ids(&self, topic: &str, ids: Range<u64>, size: usize) {
        for id in ids {
            self.publish(topic, sized_payload(id, size)).await;
        }
    }

    /// Waits until the broker acknowledged (QoS 1) or the socket took (QoS 0)
    /// everything published so far.
    pub async fn flush(&self) {
        let target = self.published.load(Ordering::Relaxed);
        let confirmed = self.confirmed.clone();
        eventually(
            Duration::from_secs(60),
            "the publisher's messages to be confirmed",
            || confirmed.load(Ordering::Relaxed) >= target,
        )
        .await;
    }
}

/// Pulls batches until `count` messages arrived or `timeout` passed,
/// completing each batch as it arrives when `complete` is set. Returns the
/// ids in receive order and the handles that were not completed.
pub async fn pull_ids(
    source: &mut dyn Source,
    count: usize,
    timeout: Duration,
    complete: bool,
) -> (Vec<u64>, Vec<AckHandle>) {
    let deadline = Instant::now() + timeout;
    let mut got = Vec::new();
    let mut held = Vec::new();
    while got.len() < count {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left, source.next_batch(limits(64))).await {
            Ok(Ok(Some(SourceBatch { batch, ack }))) => {
                got.extend(payload_ids(&batch));
                if complete {
                    ack.complete(AckOutcome::Committed);
                } else {
                    held.push(ack);
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("next_batch failed: {e}"),
            Err(_) => break,
        }
    }
    (got, held)
}

pub async fn within<T>(timeout: Duration, what: &str, fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out after {timeout:?}: {what}"))
}
