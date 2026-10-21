//! The MQTT publish sink against Mosquitto 2 and EMQX 5.8.6 in Docker.
//!
//! A test subscriber on the same broker records what arrives. Every scenario
//! runs once per broker. They are `#[ignore]`d so `cargo nextest run` passes
//! without Docker; run them with
//!
//! ```text
//! cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(docker_publish)'
//! ```
//!
//! Containers are labelled `thadam-mqtt-test` and removed when the test ends,
//! including on panic.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeSet;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common::broker::{Broker, Kind, Opts, TlsDir};
use common::certs::Certs;
use common::{eventually, unique};
use rumqttc::{AsyncClient, Event as MqttEvent, Incoming, MqttOptions, QoS};
use serde_json::{Value as Json, json};
use thadam_core::{
    AssetPath, BatchId, BatchSink, BatchSinkFactory, BuildContext, DatasetDecl, DeliveryClass,
    DiskBudget, Event, EventBatch, EventId, FieldDef, FieldType, LevelDecl, Record, Retry, Shape,
    ShapeRef, Shutdown, SinkBatch, SinkCapabilities, SinkContext, SinkId, SystemClock, ThadamError,
    TimeDomain, Times, Timestamp, Value, WalEpoch, WalPos, WalRange, WriteOutcome,
};
use thadam_mqtt::sink::MqttPublishSinkFactory;
use thadam_testkit::conformance::batch_sink::{
    self, BatchSinkCase, BatchSinkFixture, BatchSinkProbe, InjectedFault,
};

const TIMEOUT: Duration = Duration::from_secs(60);

macro_rules! per_broker {
    ($mosquitto:ident, $emqx:ident, $scenario:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "needs Docker (Mosquitto 2)"]
        async fn $mosquitto() {
            $scenario(Kind::Mosquitto).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "needs Docker (EMQX 5.8.6)"]
        async fn $emqx() {
            $scenario(Kind::Emqx).await;
        }
    };
}

async fn start(kind: Kind, opts: Opts) -> Broker {
    tokio::task::spawn_blocking(move || Broker::start(kind, &opts))
        .await
        .unwrap()
}

/// Records every message under a topic filter. It runs on a thread of its own
/// with its own runtime, so a blocking caller cannot starve it, and it
/// resubscribes after every reconnect.
type Received = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

struct Subscriber {
    received: Received,
    subacks: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl Subscriber {
    fn spawn(addr: &str, filter: &str) -> Subscriber {
        let (host, port) = addr.rsplit_once(':').expect("host:port");
        let (host, port): (String, u16) = (host.to_owned(), port.parse().expect("port"));
        let filter = filter.to_owned();
        let sub = Subscriber {
            received: Arc::default(),
            subacks: Arc::default(),
            stop: Arc::default(),
        };
        let (received, subacks, stop) =
            (sub.received.clone(), sub.subacks.clone(), sub.stop.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let mut options = MqttOptions::new(unique("test-sub"), host, port);
                options
                    .set_keep_alive(Duration::from_secs(5))
                    .set_clean_session(true)
                    .set_max_packet_size(8 << 20, 8 << 20);
                let (client, mut event_loop) = AsyncClient::new(options, 64);
                while !stop.load(Ordering::Relaxed) {
                    match event_loop.poll().await {
                        Ok(MqttEvent::Incoming(Incoming::ConnAck(_))) => {
                            client.subscribe(&filter, QoS::AtLeastOnce).await.unwrap();
                        }
                        Ok(MqttEvent::Incoming(Incoming::SubAck(_))) => {
                            subacks.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(MqttEvent::Incoming(Incoming::Publish(p))) => {
                            received.lock().unwrap().push((p.topic, p.payload.to_vec()));
                        }
                        Ok(_) => {}
                        Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                }
            });
        });
        sub
    }

    fn count(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    fn messages(&self) -> Vec<(String, Vec<u8>)> {
        self.received.lock().unwrap().clone()
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn block_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

async fn wait_subscribed(sub: &Subscriber, subacks: u64) {
    eventually(TIMEOUT, "the subscription", || {
        sub.subacks.load(Ordering::SeqCst) >= subacks
    })
    .await;
}

async fn wait_for_messages(sub: &Subscriber, n: usize) {
    eventually(TIMEOUT, "the subscriber to receive every message", || {
        sub.count() >= n
    })
    .await;
}

fn sink_config(url: &str, topic: &str, qos: u8) -> Json {
    json!({
        "url": url,
        "client_id": unique("sink"),
        "topic": topic,
        "qos": qos,
        "max_in_flight": 16,
        "request_timeout": "5s",
        "connect_timeout": "5s",
    })
}

fn build_sink(config: &Json, dataset: &str, levels: &[&str]) -> Box<dyn BatchSink> {
    let levels = levels
        .iter()
        .enumerate()
        .map(|(i, name)| LevelDecl::new(*name, 100 + i as u32))
        .collect();
    let decl = DatasetDecl::new(dataset, Vec::new(), levels, false);
    let ctx = BuildContext::new("mqtt-out", Arc::new(SystemClock::new()), PathBuf::new())
        .with_datasets(Arc::from([decl]));
    MqttPublishSinkFactory::new()
        .build(config.clone(), &ctx)
        .unwrap_or_else(|e| panic!("build failed: {e}"))
}

fn sink_context() -> SinkContext {
    SinkContext::new(
        SinkId::new("mqtt-test"),
        Shutdown::new(),
        Arc::new(SystemClock::new()),
        PathBuf::new(),
        Arc::new(DiskBudget::new()),
    )
}

fn robot_of(seq: u64) -> String {
    format!("r{}", seq % 3 + 1)
}

/// Event `seq` on robot `robot_of(seq)`, with one record field `seq`.
fn event(seq: u64) -> Event {
    let shape = ShapeRef::new(Shape::new(vec![FieldDef::new("seq", FieldType::I64)]).unwrap());
    let record = Record::new(shape, vec![Value::I64(seq as i64)]).unwrap();
    let times = Times::new(Timestamp::from_nanos(1_000 + seq as i64), TimeDomain::Utc);
    let robot = robot_of(seq);
    Event::builder(
        EventId::from_u128(u128::from(seq)),
        WalPos::new(seq + 1),
        times,
    )
    .asset(AssetPath::new([("robot", robot.as_str())]).unwrap())
    .payload(record)
    .build()
    .unwrap()
}

/// Events `first..first + count`, as one batch whose origin is their WAL range.
fn batch(first: u64, count: u64) -> SinkBatch {
    let events = (first..first + count).map(event).collect();
    let origin = WalRange::new(
        WalEpoch::new(1),
        WalPos::new(first + 1),
        WalPos::new(first + count),
    )
    .unwrap();
    SinkBatch::new(EventBatch::new(events, origin, true))
}

/// Every delivered message is on its expected topic, carries the documented
/// envelope, and the delivered set of events is exactly `seqs`.
fn check_envelopes(messages: &[(String, Vec<u8>)], base: &str, seqs: Range<u64>) {
    let mut seen = BTreeSet::new();
    for (topic, payload) in messages {
        let doc: Json = serde_json::from_slice(payload).unwrap();
        let seq = doc["seq"].as_u64().unwrap();
        let robot = robot_of(seq);
        assert_eq!(topic, &format!("{base}/imu/{robot}"));
        assert_eq!(
            doc["_event_id"].as_str(),
            Some(EventId::from_u128(u128::from(seq)).to_string().as_str())
        );
        assert_eq!(doc["robot"].as_str(), Some(robot.as_str()));
        assert_eq!(doc["_quality_status"].as_str(), Some("good"));
        assert!(doc["_event_time"].is_null());
        assert!(doc["_ingest_time"].as_str().unwrap().ends_with('Z'));
        assert!(seen.insert(seq), "event {seq} was delivered twice");
    }
    let want: BTreeSet<u64> = seqs.collect();
    assert_eq!(seen, want);
}

fn seqs_of(messages: &[(String, Vec<u8>)]) -> BTreeSet<u64> {
    messages
        .iter()
        .map(|(_, payload)| {
            let doc: Json = serde_json::from_slice(payload).unwrap();
            doc["seq"].as_u64().unwrap()
        })
        .collect()
}

async fn write_batch(config: &Json, levels: &[&str], batch: SinkBatch) {
    let mut sink = build_sink(config, "imu", levels);
    sink.open(&sink_context())
        .await
        .unwrap_or_else(|e| panic!("open failed: {e}"));
    let outcome = sink
        .write(&batch)
        .await
        .unwrap_or_else(|e| panic!("write failed: {e}"));
    assert!(
        outcome.rejected.is_empty(),
        "{} events were rejected",
        outcome.rejected.len()
    );
    sink.close()
        .await
        .unwrap_or_else(|e| panic!("close failed: {e}"));
}

// ---------------------------------------------------------------------------
// (a) and (b): delivery and envelopes on QoS 1 and QoS 0
// ---------------------------------------------------------------------------

async fn qos1_batch_reaches_subscriber(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let base = unique("thadam/qos1");
    let sub = Subscriber::spawn(&broker.addr(), &format!("{base}/#"));
    wait_subscribed(&sub, 1).await;

    let config = sink_config(&broker.url(), &format!("{base}/{{dataset}}/{{robot}}"), 1);
    let mut sink = build_sink(&config, "imu", &["robot"]);
    let caps = sink
        .open(&sink_context())
        .await
        .unwrap_or_else(|e| panic!("open failed: {e}"));
    assert_eq!(caps.delivery, DeliveryClass::AtLeastOnce);
    let outcome = sink
        .write(&batch(0, 300))
        .await
        .unwrap_or_else(|e| panic!("write failed: {e}"));
    assert!(outcome.rejected.is_empty());

    wait_for_messages(&sub, 300).await;
    check_envelopes(&sub.messages(), &base, 0..300);
    sink.close()
        .await
        .unwrap_or_else(|e| panic!("close failed: {e}"));
}

async fn qos0_batch_reaches_subscriber(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let base = unique("thadam/qos0");
    let sub = Subscriber::spawn(&broker.addr(), &format!("{base}/#"));
    wait_subscribed(&sub, 1).await;

    let config = sink_config(&broker.url(), &format!("{base}/{{dataset}}/{{robot}}"), 0);
    let mut sink = build_sink(&config, "imu", &["robot"]);
    let caps = sink
        .open(&sink_context())
        .await
        .unwrap_or_else(|e| panic!("open failed: {e}"));
    assert_eq!(caps.delivery, DeliveryClass::AtMostOnce);
    let outcome = sink
        .write(&batch(0, 200))
        .await
        .unwrap_or_else(|e| panic!("write failed: {e}"));
    assert!(outcome.rejected.is_empty());

    wait_for_messages(&sub, 200).await;
    check_envelopes(&sub.messages(), &base, 0..200);
    sink.close()
        .await
        .unwrap_or_else(|e| panic!("close failed: {e}"));
}

per_broker!(qos1_mosquitto, qos1_emqx, qos1_batch_reaches_subscriber);
per_broker!(qos0_mosquitto, qos0_emqx, qos0_batch_reaches_subscriber);

// ---------------------------------------------------------------------------
// (c) broker restart in the middle of a batch
// ---------------------------------------------------------------------------

async fn restart_mid_batch_retries_deliver_all(kind: Kind) {
    const EVENTS: u64 = 100_000;
    let broker = Arc::new(start(kind, Opts::default()).await);
    let base = unique("thadam/restart");
    let sub = Subscriber::spawn(&broker.addr(), &format!("{base}/#"));
    wait_subscribed(&sub, 1).await;

    // A small window keeps the batch in flight long enough for the restart
    // to land while it is being written.
    let mut config = sink_config(&broker.url(), &format!("{base}/{{dataset}}/{{robot}}"), 1);
    config["max_in_flight"] = json!(2);
    let mut sink = build_sink(&config, "imu", &["robot"]);
    sink.open(&sink_context())
        .await
        .unwrap_or_else(|e| panic!("open failed: {e}"));
    let big = batch(0, EVENTS);

    let restart = {
        let (broker, received) = (broker.clone(), sub.received.clone());
        tokio::spawn(async move {
            eventually(TIMEOUT, "the first deliveries", || {
                received.lock().unwrap().len() >= 500
            })
            .await;
            tokio::task::spawn_blocking(move || broker.restart())
                .await
                .unwrap();
        })
    };
    let first = sink.write(&big).await;
    restart.await.unwrap();
    let error = first.expect_err("a write interrupted by a broker restart must fail");
    assert_eq!(error.retry, Retry::Transient, "{error}");

    // Retry the same batch once the subscriber has resubscribed, so the
    // second attempt is not published into a window with no subscriber.
    wait_subscribed(&sub, 2).await;
    let mut attempts = 1;
    loop {
        match sink.write(&big).await {
            Ok(outcome) => {
                assert!(outcome.rejected.is_empty());
                break;
            }
            Err(e) if e.retry == Retry::Transient && attempts < 10 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("the retry failed after {attempts} attempts: {e}"),
        }
    }

    let all: BTreeSet<u64> = (0..EVENTS).collect();
    let started = Instant::now();
    loop {
        let seen = seqs_of(&sub.messages());
        if seen.is_superset(&all) {
            break;
        }
        assert!(
            started.elapsed() < TIMEOUT,
            "{} events never reached the subscriber",
            all.difference(&seen).count()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    sink.close()
        .await
        .unwrap_or_else(|e| panic!("close failed: {e}"));
}

per_broker!(
    restart_mosquitto,
    restart_emqx,
    restart_mid_batch_retries_deliver_all
);

// ---------------------------------------------------------------------------
// (d) TLS and mutual TLS through the relay
// ---------------------------------------------------------------------------

async fn tls_and_mtls_publish_through_relay(kind: Kind) {
    let certs = Certs::generate();
    let broker = start(
        kind,
        Opts {
            tls: Some(TlsDir {
                dir: certs.dir.path().to_owned(),
            }),
            ..Opts::default()
        },
    )
    .await;
    let base = unique("thadam/tls");
    let sub = Subscriber::spawn(&broker.addr(), &format!("{base}/#"));
    wait_subscribed(&sub, 1).await;
    let topic = format!("{base}/{{dataset}}/{{robot}}");

    // Server authentication against the broker's CA.
    let mut config = sink_config(
        &format!("mqtts://localhost:{}", broker.tls_port.unwrap()),
        &topic,
        1,
    );
    config["tls"] = json!({ "ca": certs.path("ca.pem") });
    write_batch(&config, &["robot"], batch(0, 50)).await;

    // Mutual TLS with a client certificate.
    let mut config = sink_config(
        &format!("mqtts://localhost:{}", broker.mtls_port.unwrap()),
        &topic,
        1,
    );
    config["tls"] = json!({
        "ca": certs.path("ca.pem"),
        "cert": certs.path("client.pem"),
        "key": certs.path("client.key"),
    });
    write_batch(&config, &["robot"], batch(50, 50)).await;

    wait_for_messages(&sub, 100).await;
    check_envelopes(&sub.messages(), &base, 0..100);
}

per_broker!(tls_mosquitto, tls_emqx, tls_and_mtls_publish_through_relay);

// ---------------------------------------------------------------------------
// (e) the thadam-testkit batch-sink conformance suite
// ---------------------------------------------------------------------------

/// Passes every batch id the sink is given through to the probe, because a
/// broker cannot report which batch a publish came from.
struct Recording {
    inner: Box<dyn BatchSink>,
    attempts: Arc<Mutex<Vec<BatchId>>>,
}

#[async_trait]
impl BatchSink for Recording {
    async fn open(&mut self, ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        self.inner.open(ctx).await
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        self.attempts.lock().unwrap().push(batch.id());
        self.inner.write(batch).await
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.inner.close().await
    }
}

/// Counts what the subscriber received. A real broker cannot inject faults,
/// so `inject` declines and the suite skips the checks that need them.
struct BrokerProbe {
    sub: Subscriber,
    attempts: Arc<Mutex<Vec<BatchId>>>,
}

impl BatchSinkProbe for BrokerProbe {
    fn applied_events(&self) -> usize {
        // Deliveries trail the broker's acknowledgement, so wait until the
        // count stops moving before reading it.
        let mut last = usize::MAX;
        let mut count = self.sub.count();
        while count != last {
            last = count;
            std::thread::sleep(Duration::from_millis(200));
            count = self.sub.count();
        }
        count
    }

    fn attempt_ids(&self) -> Vec<BatchId> {
        self.attempts.lock().unwrap().clone()
    }

    fn inject(&self, _fault: InjectedFault) -> bool {
        false
    }
}

struct BrokerSinkFixture {
    addr: String,
    url: String,
}

impl BatchSinkFixture for BrokerSinkFixture {
    fn create(&self) -> BatchSinkCase {
        let base = unique("thadam/conformance");
        let config = sink_config(&self.url, &format!("{base}/{{dataset}}"), 1);
        let sink = build_sink(&config, "events", &[]);
        let sub = Subscriber::spawn(&self.addr, &format!("{base}/#"));
        block_until("the subscription", || {
            sub.subacks.load(Ordering::SeqCst) >= 1
        });
        let attempts = Arc::new(Mutex::new(Vec::new()));
        BatchSinkCase {
            sink: Box::new(Recording {
                inner: sink,
                attempts: attempts.clone(),
            }),
            probe: Box::new(BrokerProbe { sub, attempts }),
        }
    }
}

async fn batch_sink_conformance(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let fixture = BrokerSinkFixture {
        addr: broker.addr(),
        url: broker.url(),
    };
    batch_sink::run_all(&fixture).await;
}

per_broker!(
    conformance_mosquitto,
    conformance_emqx,
    batch_sink_conformance
);
