//! Docker harness shared by the Kafka integration tests: one `apache/kafka`
//! KRaft broker per test, labelled `thadam-kafka-test`, removed on drop.
//!
//! The broker advertises `127.0.0.1:<host port>`, so the port is chosen once
//! per container and kept across stop/start and pause/unpause cycles (the
//! client bakes the address in). Heap is capped so several brokers fit in
//! memory at once.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use thadam_core::{
    AssetPath, BatchSink, BatchSinkFactory, BuildContext, DatasetDecl, DiskBudget, Event,
    EventBatch, EventId, FieldDef, FieldType, LevelDecl, Record, Shape, ShapeRef, Shutdown,
    SinkBatch, SinkContext, SinkId, SystemClock, TimeDomain, Times, Timestamp, Value, WalEpoch,
    WalPos, WalRange,
};
use thadam_kafka::KafkaSinkFactory;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record as KafkaRecord;
use tempfile::TempDir;

/// The pinned broker image (public Docker Hub, the Apache Software
/// Foundation's official image). Re-pin with `THADAM_TEST_KAFKA_IMAGE`.
pub const IMAGE: &str = "apache/kafka:4.3.1";
pub const LABEL: &str = "thadam-kafka-test";
const TIMEOUT: Duration = Duration::from_secs(180);
const HEAP: &str = "-Xmx512m -Xms512m";

pub fn image() -> String {
    std::env::var("THADAM_TEST_KAFKA_IMAGE").unwrap_or_else(|_| IMAGE.to_owned())
}

pub fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker cli")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

fn pull_once() {
    static PULL: Once = Once::new();
    PULL.call_once(|| {
        let image = image();
        let out = docker(&["pull", &image]);
        assert!(
            out.status.success(),
            "docker pull {image}: {}",
            stderr(&out)
        );
    });
}

/// Per-test broker settings.
#[derive(Clone, Debug)]
pub struct Opts {
    /// Part of the container name, for humans.
    pub tag: &'static str,
    /// The broker's `auto.create.topics.enable`.
    pub auto_create_topics: bool,
    /// The broker's `message.max.bytes` (the default when `None`).
    pub message_max_bytes: Option<u32>,
}

impl Opts {
    pub fn new(tag: &'static str) -> Self {
        Opts {
            tag,
            auto_create_topics: true,
            message_max_bytes: None,
        }
    }
}

/// A running single-node KRaft broker; removed on drop.
pub struct Broker {
    name: String,
    port: u16,
    /// `127.0.0.1:<port>`.
    pub addr: String,
}

impl Broker {
    pub async fn start(opts: Opts) -> Broker {
        let broker = tokio::task::spawn_blocking(move || Broker::create(&opts))
            .await
            .expect("docker task");
        broker.wait_ready().await;
        broker
    }

    fn create(opts: &Opts) -> Broker {
        pull_once();
        let mut last_err = String::new();
        // Another test process can claim the port between `free_port` and
        // the container start; retry with a fresh port.
        for attempt in 0..8 {
            let port = free_port();
            let name = format!(
                "thadam-kafka-{}-{}-{:x}-{attempt}",
                opts.tag,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let advertised = format!("PLAINTEXT://127.0.0.1:{port}");
            let publish = format!("127.0.0.1:{port}:9092");
            let mut env: Vec<String> = vec![
                "KAFKA_NODE_ID=1".into(),
                "KAFKA_PROCESS_ROLES=broker,controller".into(),
                "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093".into(),
                format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER".into(),
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
                    .into(),
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093".into(),
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1".into(),
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1".into(),
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1".into(),
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0".into(),
                format!(
                    "KAFKA_AUTO_CREATE_TOPICS_ENABLE={}",
                    opts.auto_create_topics
                ),
                format!("KAFKA_HEAP_OPTS={HEAP}"),
            ];
            if let Some(max) = opts.message_max_bytes {
                env.push(format!("KAFKA_MESSAGE_MAX_BYTES={max}"));
            }
            let image = image();
            let mut create: Vec<&str> =
                vec!["create", "--name", &name, "--label", LABEL, "-p", &publish];
            for var in &env {
                create.push("-e");
                create.push(var);
            }
            create.push(&image);
            let out = docker(&create);
            assert!(out.status.success(), "docker create: {}", stderr(&out));
            // The guard exists from here, so a failed start removes the container.
            let broker = Broker {
                name,
                port,
                addr: format!("127.0.0.1:{port}"),
            };
            let out = docker(&["start", &broker.name]);
            if !out.status.success() {
                last_err = stderr(&out);
                drop(broker);
                if last_err.contains("port is already allocated")
                    || last_err.contains("address already in use")
                {
                    continue;
                }
                panic!("docker start: {last_err}");
            }
            return broker;
        }
        panic!("docker start after port retries: {last_err}");
    }

    /// The broker answers a metadata request (the same check a sink's
    /// `open` makes).
    pub async fn wait_ready(&self) {
        let started = Instant::now();
        loop {
            if self.responds().await {
                return;
            }
            let state = docker(&["inspect", "-f", "{{.State.Running}}", &self.name]);
            if String::from_utf8_lossy(&state.stdout).trim() == "false" {
                let logs = docker(&["logs", &self.name]);
                panic!(
                    "kafka container exited during startup:\n{}{}",
                    String::from_utf8_lossy(&logs.stdout),
                    stderr(&logs)
                );
            }
            assert!(started.elapsed() < TIMEOUT, "kafka did not become ready");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn responds(&self) -> bool {
        let build = ClientBuilder::new(vec![self.addr.clone()]).build();
        match tokio::time::timeout(Duration::from_secs(5), build).await {
            Ok(Ok(client)) => matches!(
                tokio::time::timeout(Duration::from_secs(5), client.list_topics()).await,
                Ok(Ok(_))
            ),
            _ => false,
        }
    }

    pub fn stop(&self) {
        let out = docker(&["stop", &self.name]);
        assert!(out.status.success(), "docker stop: {}", stderr(&out));
    }

    pub async fn start_again(&self) {
        let out = docker(&["start", &self.name]);
        assert!(out.status.success(), "docker start: {}", stderr(&out));
        self.wait_ready().await;
    }

    /// Freezes every process of the broker. Requests sent meanwhile get no
    /// response until [`Broker::unpause`].
    pub fn pause(&self) {
        let out = docker(&["pause", &self.name]);
        assert!(out.status.success(), "docker pause: {}", stderr(&out));
    }

    pub fn unpause(&self) {
        let out = docker(&["unpause", &self.name]);
        assert!(out.status.success(), "docker unpause: {}", stderr(&out));
    }

    /// An independent test-side client towards the broker.
    pub async fn client(&self) -> Client {
        ClientBuilder::new(vec![self.addr.clone()])
            .build()
            .await
            .expect("test client")
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.name]);
    }
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Creates `name` with `partitions` partitions (replication 1) and waits
/// until metadata lists them.
pub async fn create_topic(client: &Client, name: &str, partitions: i32) {
    client
        .controller_client()
        .expect("controller client")
        .create_topic(name, partitions, 1, 10_000)
        .await
        .expect("create topic");
    let started = Instant::now();
    while partition_count(client, name).await != Some(partitions as usize) {
        assert!(started.elapsed() < TIMEOUT, "topic {name} never appeared");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The partition count of `name` in an unfiltered metadata snapshot.
pub async fn partition_count(client: &Client, name: &str) -> Option<usize> {
    client
        .list_topics()
        .await
        .expect("list topics")
        .iter()
        .find(|t| t.name == name)
        .map(|t| t.partitions.len())
}

/// Every record stored in `name`, across its partitions.
pub async fn read_all(client: &Client, name: &str) -> Vec<KafkaRecord> {
    let partitions = partition_count(client, name).await.expect("topic exists");
    let mut out = Vec::new();
    for p in 0..partitions {
        let pc = client
            .partition_client(name, p as i32, UnknownTopicHandling::Error)
            .await
            .expect("partition client");
        let end = pc
            .get_offset(OffsetAt::Latest)
            .await
            .expect("latest offset");
        let mut offset = pc
            .get_offset(OffsetAt::Earliest)
            .await
            .expect("earliest offset");
        while offset < end {
            let (records, _) = pc
                .fetch_records(offset, 1..1_048_576, 500)
                .await
                .expect("fetch");
            let last = records.last().expect("records below the high watermark");
            offset = last.offset + 1;
            out.extend(records.into_iter().map(|r| r.record));
        }
    }
    out
}

pub fn dataset_decl() -> DatasetDecl {
    DatasetDecl::new("imu", Vec::new(), vec![LevelDecl::new("robot", 100)], false)
}

/// The factory-built sink for dataset `imu` (exercises the registration
/// path), with `extra` merged over the defaults.
pub fn build_sink(broker: &Broker, extra: serde_json::Value) -> Box<dyn BatchSink> {
    let mut value = serde_json::json!({
        "bootstrap": [broker.addr],
        "topic": "telemetry.{dataset}.{robot}",
        "request_timeout": "2s",
        "max_in_flight": 8,
        "datasets": ["imu"]
    });
    if let Some(map) = extra.as_object() {
        for (k, v) in map {
            value[k] = v.clone();
        }
    }
    let ctx = BuildContext::new(
        "kafka-out",
        Arc::new(SystemClock::new()),
        PathBuf::from("/tmp/thadam-kafka-test-build"),
    )
    .with_datasets(Arc::from([dataset_decl()]));
    KafkaSinkFactory::new()
        .build(value, &ctx)
        .expect("build sink")
}

pub fn sink_context(dir: &TempDir) -> SinkContext {
    SinkContext::new(
        SinkId::new("kafka-out"),
        Shutdown::new(),
        Arc::new(SystemClock::new()),
        dir.path().to_owned(),
        Arc::new(DiskBudget::new()),
    )
}

pub fn shape() -> ShapeRef {
    ShapeRef::new(
        Shape::new(vec![
            FieldDef::new("temp", FieldType::F64),
            FieldDef::new("note", FieldType::Str),
        ])
        .expect("shape"),
    )
}

/// The event id of WAL position `pos` in the helpers below.
pub fn event_id(pos: u64) -> EventId {
    EventId::from_u128(0x5eed_0000_0000_0000_0000_0000_0000_0000 + u128::from(pos))
}

/// One event at WAL position `pos` for `robot`, with note `note`.
pub fn event(pos: u64, robot: &str, note: String) -> Event {
    Event::builder(
        event_id(pos),
        WalPos::new(pos),
        Times::new(
            Timestamp::from_nanos(1_000_000_000 + pos as i64),
            TimeDomain::Utc,
        ),
    )
    .asset(AssetPath::new([("robot", robot)]).expect("asset"))
    .payload(
        Record::new(
            shape(),
            vec![Value::F64(30.0 + pos as f64), Value::str(note)],
        )
        .expect("record"),
    )
    .build()
    .expect("event")
}

/// A batch of `count` events from WAL position `first` for `robot`; the
/// note of each event is `m<position>`.
pub fn make_batch(first: u64, count: u64, robot: &str) -> SinkBatch {
    let events: Vec<Event> = (0..count)
        .map(|k| {
            let pos = first + k;
            event(pos, robot, format!("m{pos}"))
        })
        .collect();
    let range = WalRange::new(
        WalEpoch::new(9),
        WalPos::new(first),
        WalPos::new(first + count - 1),
    )
    .expect("range");
    SinkBatch::new(EventBatch::new(events, range, true))
}

/// A batch from WAL position `first` for `robot`, one event per note.
pub fn batch_of_notes(first: u64, robot: &str, notes: Vec<String>) -> SinkBatch {
    let count = notes.len() as u64;
    let events: Vec<Event> = notes
        .into_iter()
        .enumerate()
        .map(|(k, note)| event(first + k as u64, robot, note))
        .collect();
    let range = WalRange::new(
        WalEpoch::new(9),
        WalPos::new(first),
        WalPos::new(first + count - 1),
    )
    .expect("range");
    SinkBatch::new(EventBatch::new(events, range, true))
}

/// The record key (the event id, hex) of a stored record.
pub fn key_of(record: &KafkaRecord) -> String {
    String::from_utf8(record.key.clone().expect("key")).expect("utf-8 key")
}
