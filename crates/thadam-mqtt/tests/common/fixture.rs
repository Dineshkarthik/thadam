//! Adapts a real broker to `thadam-testkit`'s source conformance suite.
//!
//! The suite was written against an in-memory upstream, so this fixture
//! supplies what a broker cannot say about itself. A wire proxy sits between the
//! source and the broker to observe PUBACKs and PINGREQs and to cut the
//! connection; the suite's end-of-stream is derived from "everything published
//! was delivered"; and `lose_session` cuts the connection and waits for the
//! connector to start a new epoch. Run it on a multi-threaded runtime: the
//! suite's `Upstream` methods are synchronous and block while the connector
//! (on another worker) catches up.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use thadam_core::{
    BatchLimits, EpochGate, Source, SourceBatch, SourceCapabilities, SourceContext, ThadamError,
};
use thadam_mqtt::{MqttHandle, MqttSource};
use thadam_testkit::conformance::source::{Case, SourceFixture, Upstream};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use super::proxy::Proxy;
use super::{Publisher, base_config, build_source, unique, with};

struct Control {
    published: AtomicU64,
    finished: AtomicBool,
}

pub struct BrokerFixture {
    pub addr: String,
    /// Run the suite over MQTT 5 (needs the `v5` feature).
    pub v5: bool,
}

impl BrokerFixture {
    /// MQTT 5 does not allow a keepalive under 5 s.
    fn keepalive(&self) -> Duration {
        Duration::from_secs(if self.v5 { 5 } else { 2 })
    }

    fn case(&self, connect_delay: Duration) -> Case {
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                let proxy = Proxy::start(self.addr.clone()).await;
                proxy.set_connect_delay(connect_delay);
                let topic = unique("thadam/conformance");
                let keepalive = self.keepalive();
                let mut config = with(
                    base_config(&proxy.url(), &unique("conformance"), &topic),
                    "keep_alive",
                    serde_json::json!(format!("{}s", keepalive.as_secs())),
                );
                if self.v5 {
                    config["protocol"] = serde_json::json!("v5");
                }
                let (source, handle) = build_source(&config);
                let publisher = Publisher::connect(&self.addr, 1);
                let control = Arc::new(Control {
                    published: AtomicU64::new(0),
                    finished: AtomicBool::new(false),
                });
                let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
                let feeder_topic = topic.clone();
                tokio::spawn(async move {
                    while let Some(payload) = rx.recv().await {
                        publisher.publish(&feeder_topic, payload).await;
                    }
                    publisher.flush().await;
                });
                Case {
                    source: Box::new(ConformanceSource {
                        inner: source,
                        handle: handle.clone(),
                        control: control.clone(),
                        delivered: HashSet::new(),
                    }),
                    upstream: Box::new(BrokerUpstream {
                        keepalive,
                        proxy,
                        handle,
                        control,
                        feeder: tx,
                    }),
                }
            })
        })
    }
}

impl SourceFixture for BrokerFixture {
    fn create(&self) -> Case {
        self.case(Duration::ZERO)
    }

    fn create_slow_connect(&self, delay: Duration) -> Option<Case> {
        Some(self.case(delay))
    }
}

struct BrokerUpstream {
    keepalive: Duration,
    proxy: Proxy,
    handle: MqttHandle,
    control: Arc<Control>,
    feeder: mpsc::UnboundedSender<Bytes>,
}

impl BrokerUpstream {
    /// Waits for every acknowledgement the runtime completed to reach the
    /// wire and the proxy's log, so assertions see what the broker saw.
    fn settle(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let stats = self.handle.stats();
            if stats.acks_pending() == 0 && self.proxy.pubacks_seen() as u64 >= stats.acks_written {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Upstream for BrokerUpstream {
    fn publish(&self, payload: Bytes) {
        self.control.published.fetch_add(1, Ordering::SeqCst);
        let _ = self.feeder.send(payload);
    }

    fn finish(&self) {
        self.control.finished.store(true, Ordering::SeqCst);
    }

    fn lose_session(&self) {
        let before = self.handle.epoch_gate().current();
        self.proxy.kill_connections();
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.handle.epoch_gate().current() == before {
            assert!(
                Instant::now() < deadline,
                "the connector never noticed the cut connection"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn acked_payloads(&self) -> Vec<Bytes> {
        self.settle();
        self.proxy.acked_payloads()
    }

    fn keepalive(&self) -> Duration {
        self.keepalive
    }

    fn keepalive_violations(&self) -> u64 {
        self.proxy.keepalive_violations(self.keepalive)
    }

    fn epoch_gate(&self) -> Option<EpochGate> {
        Some(self.handle.epoch_gate())
    }
}

/// The real source, plus the end of stream the suite expects: `Ok(None)` once
/// the upstream finished and everything published has been delivered.
struct ConformanceSource {
    inner: MqttSource,
    handle: MqttHandle,
    control: Arc<Control>,
    delivered: HashSet<u64>,
}

#[async_trait]
impl Source for ConformanceSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        self.inner.open(ctx).await
    }

    async fn next_batch(
        &mut self,
        limits: BatchLimits,
    ) -> Result<Option<SourceBatch>, ThadamError> {
        loop {
            let finished = self.control.finished.load(Ordering::SeqCst);
            let total = self.control.published.load(Ordering::SeqCst);
            if finished
                && self.delivered.len() as u64 >= total
                && self.handle.stats().queued_messages == 0
            {
                return Ok(None);
            }
            // Dropping the inner future on every slice is deliberate: it
            // exercises the cancel-safety clause on every call.
            match tokio::time::timeout(Duration::from_millis(25), self.inner.next_batch(limits))
                .await
            {
                Ok(Ok(Some(batch))) => {
                    for m in &batch.batch.messages {
                        if let Some(id) = thadam_testkit::payload_id(&m.payload) {
                            self.delivered.insert(id);
                        }
                    }
                    return Ok(Some(batch));
                }
                Ok(other) => return other,
                Err(_elapsed) => {}
            }
        }
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.inner.close().await
    }

    fn capabilities(&self) -> SourceCapabilities {
        self.inner.capabilities()
    }
}
