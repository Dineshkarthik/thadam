//! Docker harness shared by the Kafka integration tests: one `apache/kafka`
//! KRaft broker per test, labelled `thadam-kafka-test`, removed on drop.
//!
//! The broker advertises `127.0.0.1:<host port>`, so the port is chosen once
//! per container and kept across stop/start and pause/unpause cycles (the
//! client bakes the address in). Heap is capped so several brokers fit in
//! memory at once.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

pub mod certs;

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
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record as KafkaRecord;
use tempfile::TempDir;
use tokio_rustls::rustls::ClientConfig;

use certs::Pki;

/// The pinned broker image (public Docker Hub, the Apache Software
/// Foundation's official image). Re-pin with `THADAM_TEST_KAFKA_IMAGE`.
pub const IMAGE: &str = "apache/kafka:4.3.1";
pub const LABEL: &str = "thadam-kafka-test";
const TIMEOUT: Duration = Duration::from_secs(180);
const HEAP: &str = "-Xmx512m -Xms512m";
/// The in-container port of a secured (TLS or SASL_SSL) client listener.
const SECURE_PORT: u16 = 9094;

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

/// The environment of a plaintext broker published on host port `port`.
fn plaintext_env(opts: &Opts, port: u16) -> Vec<String> {
    let advertised = format!("PLAINTEXT://127.0.0.1:{port}");
    let mut env: Vec<String> = vec![
        "KAFKA_NODE_ID=1".into(),
        "KAFKA_PROCESS_ROLES=broker,controller".into(),
        "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093".into(),
        format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER".into(),
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT".into(),
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
    env
}

/// A directory holding one broker's `server.properties`, mounted read-only
/// into a secured broker.
fn write_config(text: &str) -> TempDir {
    let dir = tempfile::tempdir().expect("config dir");
    let path = dir.path().join("server.properties");
    std::fs::write(&path, text).expect("write server.properties");
    // The broker container runs as an unprivileged user.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod dir");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    }
    dir
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
    /// The client-facing listener.
    pub listener: Listener,
}

impl Opts {
    pub fn new(tag: &'static str) -> Self {
        Opts {
            tag,
            auto_create_topics: true,
            message_max_bytes: None,
            listener: Listener::Plaintext,
        }
    }

    /// The `server.properties` of a secured listener published on host port
    /// `port`; `None` for plaintext, which is configured by environment.
    fn secure_properties(&self, port: u16) -> Option<String> {
        let (protocol, pki, client_auth, sasl) = match &self.listener {
            Listener::Plaintext => return None,
            Listener::Tls { pki, client_auth } => ("SSL", pki, *client_auth, false),
            Listener::SaslTls { pki } => ("SASL_SSL", pki, false, true),
        };
        let mut text = format!(
            "process.roles=broker,controller\n\
             node.id=1\n\
             controller.quorum.voters=1@localhost:9093\n\
             listeners=PLAINTEXT://:9092,{protocol}://:{SECURE_PORT},CONTROLLER://:9093\n\
             advertised.listeners=PLAINTEXT://localhost:9092,{protocol}://127.0.0.1:{port}\n\
             controller.listener.names=CONTROLLER\n\
             inter.broker.listener.name=PLAINTEXT\n\
             listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,{protocol}:{protocol}\n\
             log.dirs=/tmp/kraft-combined-logs\n\
             num.partitions=1\n\
             offsets.topic.replication.factor=1\n\
             transaction.state.log.replication.factor=1\n\
             transaction.state.log.min.isr=1\n\
             group.initial.rebalance.delay.ms=0\n\
             auto.create.topics.enable={}\n\
             ssl.keystore.type=PEM\n\
             ssl.keystore.certificate.chain={}\n\
             ssl.keystore.key={}\n\
             ssl.client.auth={}\n",
            self.auto_create_topics,
            pem_value(&pki.server_cert_pem),
            pem_value(&pki.server_key_pem),
            if client_auth { "required" } else { "none" },
        );
        if client_auth {
            text.push_str(&format!(
                "ssl.truststore.type=PEM\nssl.truststore.certificates={}\n",
                pem_value(&pki.ca_pem)
            ));
        }
        if let Some(max) = self.message_max_bytes {
            text.push_str(&format!("message.max.bytes={max}\n"));
        }
        if sasl {
            // The container's admin tools use the PLAINTEXT listener as
            // ANONYMOUS, which is why ANONYMOUS is a super user here. That
            // listener is never published to the host.
            text.push_str(
                "sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512\n\
                 listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_admin=\"admin-secret\" user_alice=\"alice-secret\" user_bob=\"bob-secret\";\n\
                 listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;\n\
                 listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;\n\
                 authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer\n\
                 super.users=User:admin;User:ANONYMOUS\n",
            );
        }
        Some(text)
    }
}

/// A server-properties value holding PEM text: Java properties read `\n` as
/// a newline, so the PEM goes on one line.
fn pem_value(pem: &str) -> String {
    pem.trim_end().replace('\n', "\\n")
}

/// The client-facing listener of a broker. The inter-broker and controller
/// listeners are always plaintext, bound inside the container, and never
/// published.
#[derive(Clone, Debug)]
pub enum Listener {
    Plaintext,
    /// TLS with the test server certificate. With `client_auth`, the broker
    /// requires a client certificate the test CA signed.
    Tls {
        pki: Pki,
        client_auth: bool,
    },
    /// SASL/PLAIN and SCRAM over TLS with the StandardAuthorizer. `admin` is
    /// the PLAIN super user; other principals are added per test.
    SaslTls {
        pki: Pki,
    },
}

impl Listener {
    fn access(&self) -> Access {
        match self {
            Listener::Plaintext => Access::default(),
            Listener::Tls { pki, client_auth } => Access {
                tls: Some(Arc::new(pki.client_config(*client_auth))),
                sasl: None,
            },
            Listener::SaslTls { pki } => Access {
                tls: Some(Arc::new(pki.client_config(false))),
                sasl: Some(SaslConfig::Plain(Credentials::new(
                    "admin".to_owned(),
                    "admin-secret".to_owned(),
                ))),
            },
        }
    }
}

/// How the test's own clients reach a broker: TLS trust and identity, and SASL.
#[derive(Clone, Default)]
pub struct Access {
    tls: Option<Arc<ClientConfig>>,
    sasl: Option<SaslConfig>,
}

/// A running single-node KRaft broker; removed on drop.
pub struct Broker {
    name: String,
    port: u16,
    /// `127.0.0.1:<port>`.
    pub addr: String,
    access: Access,
    /// The mounted `server.properties` of a secured broker.
    _config: Option<TempDir>,
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
            let config = opts.secure_properties(port).map(|text| write_config(&text));
            let (publish, env) = match config {
                Some(_) => (
                    format!("127.0.0.1:{port}:{SECURE_PORT}"),
                    vec![format!("KAFKA_HEAP_OPTS={HEAP}")],
                ),
                None => (format!("127.0.0.1:{port}:9092"), plaintext_env(opts, port)),
            };
            let image = image();
            let mut create: Vec<String> = vec![
                "create".into(),
                "--name".into(),
                name.clone(),
                "--label".into(),
                LABEL.into(),
                "-p".into(),
                publish,
            ];
            if let Some(dir) = &config {
                let mount = std::fs::canonicalize(dir.path()).expect("config dir");
                create.push("-v".into());
                create.push(format!("{}:/mnt/shared/config:ro", mount.display()));
            }
            for var in env {
                create.push("-e".into());
                create.push(var);
            }
            create.push(image);
            let refs: Vec<&str> = create.iter().map(String::as_str).collect();
            let out = docker(&refs);
            assert!(out.status.success(), "docker create: {}", stderr(&out));
            // The guard exists from here, so a failed start removes the container.
            let broker = Broker {
                name,
                port,
                addr: format!("127.0.0.1:{port}"),
                access: opts.listener.access(),
                _config: config,
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

    /// A client builder for this broker with the test's TLS and SASL settings.
    fn builder(&self) -> ClientBuilder {
        let mut builder = ClientBuilder::new(vec![self.addr.clone()]);
        if let Some(tls) = &self.access.tls {
            builder = builder.tls_config(Arc::clone(tls));
        }
        if let Some(sasl) = &self.access.sasl {
            builder = builder.sasl_config(sasl.clone());
        }
        builder
    }

    async fn responds(&self) -> bool {
        let build = self.builder().build();
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
        self.builder().build().await.expect("test client")
    }

    /// Runs a Kafka admin tool inside the container against its internal
    /// PLAINTEXT listener. The authorizer lets that listener's ANONYMOUS
    /// principal act as super user, and the listener is never published.
    fn admin(&self, tool: &str, args: &[&str]) {
        let bin = format!("/opt/kafka/bin/{tool}");
        let mut exec: Vec<&str> = vec![
            "exec",
            self.name.as_str(),
            bin.as_str(),
            "--bootstrap-server",
            "localhost:9092",
        ];
        exec.extend_from_slice(args);
        let out = docker(&exec);
        assert!(
            out.status.success(),
            "{tool} {args:?}: {}{}",
            String::from_utf8_lossy(&out.stdout),
            stderr(&out)
        );
    }

    /// Gives `user` SCRAM-SHA-256 and SCRAM-SHA-512 credentials.
    pub fn add_scram_user(&self, user: &str, password: &str) {
        for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
            // One mechanism per request: the tool rejects both in one.
            let config = format!("{mechanism}=[iterations=4096,password={password}]");
            self.admin(
                "kafka-configs.sh",
                &[
                    "--alter",
                    "--entity-type",
                    "users",
                    "--entity-name",
                    user,
                    "--add-config",
                    &config,
                ],
            );
        }
    }

    /// Allows `principal` the `operations` on the literal topic `topic`.
    pub fn allow(&self, principal: &str, operations: &[&str], topic: &str) {
        let principal = format!("User:{principal}");
        let mut args = vec!["--add", "--allow-principal", principal.as_str()];
        for &operation in operations {
            args.push("--operation");
            args.push(operation);
        }
        args.extend(["--topic", topic]);
        self.admin("kafka-acls.sh", &args);
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
