//! Docker integration tests for the NATS JetStream sink.
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker; run
//! them with
//!
//! ```text
//! cargo nextest run -p thadam-nats --run-ignored only -E 'binary(docker_nats)'
//! ```
//!
//! Images: public Docker Hub only, pinned by tag (`nats:2.12.2`, run with
//! `-js`); re-pin with `THADAM_TEST_NATS_IMAGE` when updating. The client port
//! 4222 is published on a host port chosen once per container: a stop and
//! start cycle re-allocates unpinned ephemeral host ports, and both the sink
//! and the test client bake the address in (the object-store outage test
//! learned this the hard way). Config files (auth, TLS) are copied into the
//! created container with `docker cp` before it starts, so no host path is
//! mounted.
//!
//! Containers are labelled `thadam-nats-test` and removed when a test ends,
//! including on panic. Logs: run with `RUST_LOG=debug` (the harness installs
//! a tracing subscriber).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use futures_util::StreamExt;
use thadam_core::{
    AssetPath, BatchSink, BatchSinkFactory, BuildContext, DeliveryClass, DiskBudget, ErrorDomain,
    Event, EventBatch, EventId, FieldDef, FieldType, LevelDecl, Record, Retry, Shape, ShapeRef,
    SinkBatch, SinkContext, SinkId, SystemClock, TimeDomain, Times, Timestamp, Value, WalEpoch,
    WalPos, WalRange, codes,
};
use thadam_nats::NatsSinkFactory;
use tempfile::TempDir;

/// The pinned NATS image (public Docker Hub only). Re-pin with
/// `THADAM_TEST_NATS_IMAGE` when updating.
const NATS_IMAGE: &str = "nats:2.12.2";
const LABEL: &str = "thadam-nats-test";
const TIMEOUT: Duration = Duration::from_secs(120);

fn nats_image() -> String {
    std::env::var("THADAM_TEST_NATS_IMAGE").unwrap_or_else(|_| NATS_IMAGE.to_owned())
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker cli")
}

fn err(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

/// What a container is started with besides `-js`: extra server arguments
/// and files copied into the (not yet started) container.
#[derive(Default)]
struct Launch<'a> {
    args: &'a [&'a str],
    files: &'a [(&'a str, &'a [u8])],
}

/// A running `nats -js` container; removed on Drop. The client port is
/// pinned on the host so a stop/start cycle (the simulated outage) keeps the
/// endpoint valid for the already-connected client.
struct Server {
    name: String,
    port: u16,
    /// `nats://127.0.0.1:<port>`.
    url: String,
}

impl Server {
    async fn start(tag: &str) -> Server {
        Self::start_with(tag, Launch::default()).await
    }

    async fn start_with(tag: &str, launch: Launch<'_>) -> Server {
        let tag = tag.to_owned();
        let args: Vec<String> = launch.args.iter().map(|a| (*a).to_owned()).collect();
        let files: Vec<(String, Vec<u8>)> = launch
            .files
            .iter()
            .map(|(path, bytes)| ((*path).to_owned(), bytes.to_vec()))
            .collect();
        tokio::task::spawn_blocking(move || Self::start_blocking(&tag, &args, &files))
            .await
            .expect("docker task")
    }

    /// `tls://` address of the same port.
    fn tls_url(&self) -> String {
        format!("tls://127.0.0.1:{}", self.port)
    }

    fn start_blocking(tag: &str, args: &[String], files: &[(String, Vec<u8>)]) -> Server {
        let mut last_err = String::new();
        let staging = TempDir::new().expect("staging dir");
        // Retry the start: another test process can claim the port between
        // `free_port` and the container start.
        for attempt in 0..8 {
            let port = free_port();
            let name = format!(
                "thadam-nats-{tag}-{}-{:x}-{attempt}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let publish = format!("127.0.0.1:{port}:4222");
            let image = nats_image();
            let mut create: Vec<&str> = vec![
                "create", "--name", &name, "--label", LABEL, "-p", &publish, &image, "-js",
            ];
            create.extend(args.iter().map(String::as_str));
            let out = docker(&create);
            assert!(out.status.success(), "docker create: {}", err(&out));
            // From here the container exists; Server's Drop removes it on
            // any later failure, so build the guard first.
            let server = Server {
                name,
                port,
                url: format!("nats://127.0.0.1:{port}"),
            };
            for (n, (path, bytes)) in files.iter().enumerate() {
                let host = staging.path().join(format!("{attempt}-{n}"));
                std::fs::write(&host, bytes).expect("stage file");
                let dest = format!("{}:{path}", server.name);
                let out = docker(&["cp", host.to_str().expect("utf-8 path"), &dest]);
                assert!(out.status.success(), "docker cp {path}: {}", err(&out));
            }
            let out = docker(&["start", &server.name]);
            if !out.status.success() {
                last_err = err(&out);
                drop(server);
                if last_err.contains("port is already allocated")
                    || last_err.contains("address already in use")
                {
                    continue;
                }
                panic!("docker start: {last_err}");
            }
            server.wait_ready();
            return server;
        }
        panic!("docker start after port retries: {last_err}");
    }

    /// Ready when the server answers a TCP connect with its `INFO` line.
    fn wait_ready(&self) {
        use std::io::{BufRead, BufReader};
        let port = self.port;
        let started = Instant::now();
        loop {
            let ready = std::net::TcpStream::connect(("127.0.0.1", port))
                .and_then(|stream| {
                    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                    let mut line = String::new();
                    BufReader::new(stream).read_line(&mut line)?;
                    Ok(line.starts_with("INFO "))
                })
                .unwrap_or(false);
            if ready {
                return;
            }
            let state = docker(&["inspect", "-f", "{{.State.Running}}", &self.name]);
            if String::from_utf8_lossy(&state.stdout).trim() == "false" {
                let logs = docker(&["logs", &self.name]);
                panic!(
                    "nats container exited during startup:\n{}{}",
                    String::from_utf8_lossy(&logs.stdout),
                    err(&logs)
                );
            }
            assert!(started.elapsed() < TIMEOUT, "nats did not become ready");
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    fn stop_container(&self) {
        let out = docker(&["stop", &self.name]);
        assert!(out.status.success(), "docker stop: {}", err(&out));
    }

    fn start_container(&self) {
        let out = docker(&["start", &self.name]);
        assert!(out.status.success(), "docker start: {}", err(&out));
        self.wait_ready();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.name]);
    }
}

/// An independent test-side client towards the container.
async fn jetstream_client(server: &Server) -> jetstream::Context {
    let client = async_nats::connect(&server.url).await.expect("connect");
    jetstream::new(client)
}

/// Creates the THADAM stream from the test side with an explicit duplicate
/// window.
async fn create_stream(js: &jetstream::Context, window: Duration) {
    js.create_stream(jetstream::stream::Config {
        name: "THADAM".to_owned(),
        subjects: vec!["thadam.>".to_owned()],
        duplicate_window: window,
        ..Default::default()
    })
    .await
    .expect("create stream");
}

/// Messages stored in the THADAM stream.
async fn stored_message_count(js: &jetstream::Context) -> u64 {
    let mut stream = js.get_stream("THADAM").await.expect("get stream");
    stream.info().await.expect("stream info").state.messages
}

fn config_value(server: &Server) -> serde_json::Value {
    serde_json::json!({
        "url": server.url,
        "subject": "thadam.{dataset}.{robot}",
        "stream": "THADAM",
        "request_timeout": "1500ms",
        "max_in_flight": 8,
        "datasets": ["imu"]
    })
}

fn dataset_decl() -> thadam_core::DatasetDecl {
    thadam_core::DatasetDecl::new("imu", Vec::new(), vec![LevelDecl::new("robot", 100)], false)
}

/// The factory-built sink (exercises the CLI registration path).
fn build_sink(server: &Server, extra: serde_json::Value) -> Box<dyn BatchSink> {
    let mut value = config_value(server);
    if let Some(map) = extra.as_object() {
        for (k, v) in map {
            value[k] = v.clone();
        }
    }
    let ctx = BuildContext::new(
        "nats-out",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-nats-test-build"),
    )
    .with_datasets(Arc::from([dataset_decl()]));
    NatsSinkFactory::new()
        .build(value, &ctx)
        .expect("build sink")
}

fn sink_context(dir: &TempDir) -> SinkContext {
    SinkContext::new(
        SinkId::new("nats-out"),
        thadam_core::Shutdown::new(),
        Arc::new(SystemClock::new()),
        dir.path().to_owned(),
        Arc::new(DiskBudget::new()),
    )
}

fn shape() -> ShapeRef {
    ShapeRef::new(
        Shape::new(vec![
            FieldDef::new("temp", FieldType::F64),
            FieldDef::new("note", FieldType::Str),
        ])
        .expect("shape"),
    )
}

/// A batch of `count` events from WAL position `first`, event ids derived
/// deterministically from the position.
fn make_batch(first: u64, count: u64, robot: &str) -> SinkBatch {
    let shape = shape();
    let events: Vec<Event> = (0..count)
        .map(|k| {
            let pos = first + k;
            let id =
                EventId::from_u128(0x5eed_0000_0000_0000_0000_0000_0000_0000 + u128::from(pos));
            Event::builder(
                id,
                WalPos::new(pos),
                Times::new(
                    Timestamp::from_nanos(1_000_000_000 + pos as i64),
                    TimeDomain::Utc,
                ),
            )
            .asset(AssetPath::new([("robot", robot)]).expect("asset"))
            .payload(
                Record::new(
                    shape.clone(),
                    vec![Value::F64(30.0 + pos as f64), Value::str(format!("m{pos}"))],
                )
                .expect("record"),
            )
            .build()
            .expect("event")
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

async fn wait_until_fn<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    loop {
        if check().await {
            return;
        }
        assert!(started.elapsed() < TIMEOUT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn publishes_and_consumes_back() {
    init_tracing();
    let server = Server::start("basic").await;
    let js = jetstream_client(&server).await;
    create_stream(&js, Duration::from_secs(120)).await;

    let mut sink = build_sink(&server, serde_json::json!({}));
    let dir = TempDir::new().expect("tempdir");
    let caps = sink.open(&sink_context(&dir)).await.expect("open");
    assert_eq!(caps.delivery, DeliveryClass::IdempotentBatch);

    // A core-NATS subscription sees the same published messages.
    let raw = async_nats::connect(&server.url).await.expect("raw client");
    let mut sub = raw.subscribe("thadam.>").await.expect("subscribe");
    // SUB is processed asynchronously: flush before publishing or the first
    // messages can be missed.
    raw.flush().await.expect("flush sub");

    let batch = make_batch(0, 5, "r7");
    let expected_ids: Vec<String> = batch.events().iter().map(|e| e.id().to_string()).collect();
    let outcome = sink.write(&batch).await.expect("write");
    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);

    for (k, want_id) in expected_ids.iter().enumerate() {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .expect("message in time")
            .expect("message");
        assert_eq!(msg.subject.to_string(), "thadam.imu.r7");
        let headers = msg.headers.as_ref().expect("headers");
        let msg_id = headers
            .get(async_nats::header::NATS_MESSAGE_ID)
            .expect("Nats-Msg-Id header");
        assert_eq!(msg_id.as_str(), want_id.as_str(), "dedupe header");

        let doc: serde_json::Value = serde_json::from_slice(&msg.payload).expect("json");
        assert_eq!(doc["_event_id"], want_id.as_str());
        assert!(doc["_ingest_time"].is_string());
        assert_eq!(doc["_quality_status"], "good");
        assert_eq!(doc["robot"], "r7");
        assert_eq!(doc["note"], format!("m{k}"));
    }
    drop(sub);

    assert_eq!(stored_message_count(&js).await, 5);
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn server_restart_then_recovery() {
    init_tracing();
    let server = Server::start("restart").await;
    let js = jetstream_client(&server).await;
    create_stream(&js, Duration::from_secs(120)).await;

    let mut sink = build_sink(&server, serde_json::json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");

    let first = make_batch(0, 3, "r7");
    sink.write(&first).await.expect("first write");

    // Take the broker out; a publish now has an unknown outcome and must be
    // classified transient (the driver would retry it later).
    server.stop_container();
    let second = make_batch(3, 3, "r7");
    write_expect_transient(&mut *sink, &second).await;

    server.start_container();
    // The client's reconnect loop needs a moment; retry like the driver does
    // (the same batch relies on the Nats-Msg-Id duplicate protection).
    let started = Instant::now();
    loop {
        if sink.write(&second).await.is_ok() {
            break;
        }
        assert!(started.elapsed() < TIMEOUT, "sink did not recover");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert_eq!(
        stored_message_count(&js).await,
        6,
        "both batches stored exactly once"
    );
    sink.close().await.expect("close");
}

/// A write attempt must report a transient batch error while the broker is
/// down; a (rare) fully-buffered flush is tolerated by looping a little.
async fn write_expect_transient(sink: &mut dyn BatchSink, batch: &SinkBatch) {
    let started = Instant::now();
    loop {
        match sink.write(batch).await {
            Err(e) => {
                assert!(e.is_transient(), "outage write must be transient: {e}");
                return;
            }
            Ok(_) => {
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "outage never produced a transient error"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn retried_events_deduplicate_by_msg_id() {
    init_tracing();
    let server = Server::start("dedup").await;
    let js = jetstream_client(&server).await;
    create_stream(&js, Duration::from_secs(120)).await;

    let mut sink = build_sink(&server, serde_json::json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");

    let batch = make_batch(0, 4, "r7");
    sink.write(&batch).await.expect("first write");
    // The driver retries the same batch with the same batch id after an
    // ambiguous outcome; Nats-Msg-Id (the event id) makes it a no-op.
    for _ in 0..3 {
        sink.write(&batch).await.expect("retry write");
    }

    assert_eq!(
        stored_message_count(&js).await,
        4,
        "retried events deduplicated by Nats-Msg-Id"
    );
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn missing_stream_at_open_is_a_config_error() {
    init_tracing();
    let server = Server::start("no-stream").await;
    // Note: no stream is created and `create_stream` stays false.
    let mut sink = build_sink(&server, serde_json::json!({}));
    let dir = TempDir::new().expect("tempdir");
    let err = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("missing stream must fail open");
    assert_eq!(err.domain, ErrorDomain::Config, "{err}");
    assert!(!err.is_transient(), "{err}");
    assert!(err.message.contains("THADAM"), "{err}");
    assert!(err.message.contains("create_stream"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn create_stream_and_delivery_class_probe() {
    init_tracing();
    let server = Server::start("probe").await;
    let js = jetstream_client(&server).await;
    let dir = TempDir::new().expect("tempdir");

    // An operator-requested zero duplicate window is clamped by the server
    // to its default (2m on nats:2.12): the probe must follow the PROBED
    // window, not the requested one — never claim at-least-once from config.
    create_stream(&js, Duration::ZERO).await;
    let mut sink = build_sink(&server, serde_json::json!({}));
    let caps = sink
        .open(&sink_context(&dir))
        .await
        .expect("open existing stream");
    assert_eq!(
        caps.delivery,
        DeliveryClass::IdempotentBatch,
        "the server clamps zero to its default window (verified in state below)"
    );
    sink.close().await.expect("close");
    // (The zero-window => AtLeastOnce branch boundary is unit-tested in
    // sink.rs; real servers refuse a disabled window.)

    // A sink with `create_stream: true` creates its stream with the
    // configured window and binds the template's wildcard subject.
    // The created stream gets its own prefix: overlapping subjects across
    // streams are forbidden in one account.
    let mut sink = build_sink(
        &server,
        serde_json::json!({
            "subject": "auto.{dataset}.{robot}",
            "stream": "THADAM_AUTO",
            "create_stream": true
        }),
    );
    let caps = sink
        .open(&sink_context(&dir))
        .await
        .expect("open with create_stream");
    assert_eq!(caps.delivery, DeliveryClass::IdempotentBatch);

    let mut stream = js.get_stream("THADAM_AUTO").await.expect("created stream");
    let info = stream.info().await.expect("info");
    assert_eq!(info.config.subjects, vec!["auto.*.*".to_owned()]);
    assert_eq!(info.config.duplicate_window, Duration::from_secs(120));

    let batch = make_batch(0, 2, "r1");
    sink.write(&batch).await.expect("write to created stream");
    wait_until_fn("messages stored", || async {
        js.get_stream("THADAM_AUTO")
            .await
            .expect("get")
            .cached_info()
            .state
            .messages
            == 2
    })
    .await;
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn per_event_rejects_quarantine_only_that_event() {
    init_tracing();
    let server = Server::start("reject").await;
    let js = jetstream_client(&server).await;
    create_stream(&js, Duration::from_secs(120)).await;

    let mut sink = build_sink(&server, serde_json::json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");

    // One unrenderable event (identity lacks `robot`) surrounded by good
    // ones: only it is rejected; the rest publish normally.
    let shape = shape();
    let event = |pos: u64, robot: Option<&str>| {
        let path = match robot {
            Some(r) => AssetPath::new([("robot", r)]).expect("asset"),
            None => AssetPath::empty(),
        };
        Event::builder(
            EventId::from_u128(0xbad0_0000_0000_0000_0000_0000_0000_0000 + u128::from(pos)),
            WalPos::new(pos),
            Times::new(
                Timestamp::from_nanos(1_000_000_000 + pos as i64),
                TimeDomain::Utc,
            ),
        )
        .asset(path)
        .payload(
            Record::new(shape.clone(), vec![Value::F64(1.0), Value::str("x")]).expect("record"),
        )
        .build()
        .expect("event")
    };
    let events = vec![event(0, Some("r7")), event(1, None), event(2, Some("r7"))];
    let range = WalRange::new(WalEpoch::new(9), WalPos::new(0), WalPos::new(2)).expect("range");
    let batch = SinkBatch::new(EventBatch::new(events, range, true));

    let outcome = sink.write(&batch).await.expect("write");
    assert_eq!(outcome.rejected.len(), 1);
    assert_eq!(outcome.rejected[0].index, 1);
    assert!(!outcome.rejected[0].error.is_transient());
    assert_eq!(stored_message_count(&js).await, 2, "good events published");
    sink.close().await.expect("close");
}

/// One event whose `note` is the given text (event id derived from `pos`).
fn event_with_note(pos: u64, note: String) -> Event {
    Event::builder(
        EventId::from_u128(0x0a0e_0000_0000_0000_0000_0000_0000_0000 + u128::from(pos)),
        WalPos::new(pos),
        Times::new(
            Timestamp::from_nanos(1_000_000_000 + pos as i64),
            TimeDomain::Utc,
        ),
    )
    .asset(AssetPath::new([("robot", "r7")]).expect("asset"))
    .payload(Record::new(shape(), vec![Value::F64(1.0), Value::str(note)]).expect("record"))
    .build()
    .expect("event")
}

fn batch_of_notes(notes: Vec<String>) -> SinkBatch {
    let count = notes.len() as u64;
    let events: Vec<Event> = notes
        .into_iter()
        .enumerate()
        .map(|(pos, note)| event_with_note(pos as u64, note))
        .collect();
    let range =
        WalRange::new(WalEpoch::new(9), WalPos::new(0), WalPos::new(count - 1)).expect("range");
    SinkBatch::new(EventBatch::new(events, range, true))
}

const AUTH_CONF: &str = r#"
authorization {
  users = [
    { user: "admin", password: "adminpw" }
    { user: "robot", password: "robotpw", permissions: {
        publish: { allow: ["$JS.API.>", "other.>"] }
        subscribe: { allow: ["_INBOX.>"] }
    } }
  ]
}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn bad_credentials_and_denied_publish_are_fatal() {
    init_tracing();
    let server = Server::start_with(
        "auth",
        Launch {
            args: &["-c", "/auth.conf"],
            files: &[("/auth.conf", AUTH_CONF.as_bytes())],
        },
    )
    .await;
    let admin =
        async_nats::ConnectOptions::with_user_and_password("admin".into(), "adminpw".into())
            .connect(&server.url)
            .await
            .expect("admin connect");
    create_stream(&jetstream::new(admin), Duration::from_secs(120)).await;
    let dir = TempDir::new().expect("tempdir");

    // Wrong password and no credentials: the server refuses the connection.
    for creds in [
        serde_json::json!({"user": "robot", "password": "nope"}),
        serde_json::json!({}),
    ] {
        let mut sink = build_sink(&server, creds.clone());
        let e = sink
            .open(&sink_context(&dir))
            .await
            .expect_err("credentials rejected");
        assert_eq!(
            (e.domain, e.retry),
            (ErrorDomain::Auth, Retry::Fatal),
            "{creds}: {e}"
        );
    }

    // Right credentials, but the user may not publish to `thadam.>`: the
    // server never acknowledges, it only announces the violation. That must
    // surface as Fatal, not as an endless transient timeout.
    let mut sink = build_sink(
        &server,
        serde_json::json!({"user": "robot", "password": "robotpw"}),
    );
    sink.open(&sink_context(&dir)).await.expect("open");
    let e = sink
        .write(&make_batch(0, 2, "r7"))
        .await
        .expect_err("publish is denied");
    assert_eq!(
        (e.domain, e.code, e.retry),
        (ErrorDomain::Auth, codes::AUTH_FORBIDDEN, Retry::Fatal),
        "{e}"
    );
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn token_credentials_work_and_a_wrong_token_is_fatal() {
    init_tracing();
    let server = Server::start_with(
        "token",
        Launch {
            args: &["--auth", "s3cret-token"],
            files: &[],
        },
    )
    .await;
    let admin = async_nats::ConnectOptions::with_token("s3cret-token".into())
        .connect(&server.url)
        .await
        .expect("admin connect");
    create_stream(&jetstream::new(admin.clone()), Duration::from_secs(120)).await;
    let dir = TempDir::new().expect("tempdir");

    let mut sink = build_sink(&server, serde_json::json!({"token": "wrong"}));
    let e = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("wrong token");
    assert_eq!(
        (e.domain, e.retry),
        (ErrorDomain::Auth, Retry::Fatal),
        "{e}"
    );

    let mut sink = build_sink(&server, serde_json::json!({"token": "s3cret-token"}));
    sink.open(&sink_context(&dir)).await.expect("open");
    let outcome = sink.write(&make_batch(0, 3, "r7")).await.expect("write");
    assert!(outcome.rejected.is_empty());
    assert_eq!(stored_message_count(&jetstream::new(admin)).await, 3);
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn no_responders_is_transient() {
    init_tracing();
    let server = Server::start("noresp").await;
    let js = jetstream_client(&server).await;
    // The stream exists but binds other subjects, so a publish to
    // `thadam.imu.r7` has no JetStream responder.
    js.create_stream(jetstream::stream::Config {
        name: "THADAM".to_owned(),
        subjects: vec!["elsewhere.>".to_owned()],
        ..Default::default()
    })
    .await
    .expect("create stream");

    let mut sink = build_sink(&server, serde_json::json!({}));
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open");
    let e = sink
        .write(&make_batch(0, 2, "r7"))
        .await
        .expect_err("no responders");
    assert!(e.is_transient(), "{e}");
    assert_eq!(e.code, codes::SINK_UNAVAILABLE, "{e}");
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn oversized_payloads_are_per_event_rejects() {
    init_tracing();
    let server = Server::start("oversize").await;
    let js = jetstream_client(&server).await;
    create_stream(&js, Duration::from_secs(120)).await;
    js.create_stream(jetstream::stream::Config {
        name: "LIMITED".to_owned(),
        subjects: vec!["limit.>".to_owned()],
        max_message_size: 1024,
        ..Default::default()
    })
    .await
    .expect("create limited stream");
    let dir = TempDir::new().expect("tempdir");

    // Beyond the server's max_payload (1 MiB by default): refused at publish.
    let mut sink = build_sink(&server, serde_json::json!({}));
    sink.open(&sink_context(&dir)).await.expect("open");
    let batch = batch_of_notes(vec!["a".into(), "x".repeat(2 << 20), "b".into()]);
    let outcome = sink.write(&batch).await.expect("write succeeds overall");
    assert_eq!(outcome.rejected.len(), 1, "{:?}", outcome.rejected);
    assert_eq!(outcome.rejected[0].index, 1);
    let error = &outcome.rejected[0].error;
    assert_eq!(
        (error.code, error.retry),
        (codes::SINK_REJECTED, Retry::Permanent)
    );
    assert_eq!(stored_message_count(&js).await, 2, "neighbours published");
    sink.close().await.expect("close");

    // Beyond the stream's max_msg_size (1 KiB): refused by the server in the
    // acknowledgement.
    let mut sink = build_sink(
        &server,
        serde_json::json!({"subject": "limit.{dataset}.{robot}", "stream": "LIMITED"}),
    );
    sink.open(&sink_context(&dir)).await.expect("open limited");
    let batch = batch_of_notes(vec!["a".into(), "y".repeat(8192), "b".into()]);
    let outcome = sink.write(&batch).await.expect("write succeeds overall");
    assert_eq!(outcome.rejected.len(), 1, "{:?}", outcome.rejected);
    assert_eq!(outcome.rejected[0].index, 1);
    let error = &outcome.rejected[0].error;
    assert_eq!(
        (error.code, error.retry),
        (codes::SINK_REJECTED, Retry::Permanent)
    );
    assert!(error.message.contains("size limit"), "{error}");
    let mut limited = js.get_stream("LIMITED").await.expect("limited stream");
    assert_eq!(limited.info().await.expect("info").state.messages, 2);
    sink.close().await.expect("close");
}

struct Pki {
    ca: String,
    server_cert: String,
    server_key: String,
    client_cert: String,
    client_key: String,
}

fn pki() -> Pki {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let server_key = rcgen::KeyPair::generate().unwrap();
    let server =
        rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned(), "localhost".to_owned()])
            .unwrap()
            .signed_by(&server_key, &issuer)
            .unwrap();
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client = rcgen::CertificateParams::new(vec!["thadam-client".to_owned()])
        .unwrap()
        .signed_by(&client_key, &issuer)
        .unwrap();
    Pki {
        ca: ca.pem(),
        server_cert: server.pem(),
        server_key: server_key.serialize_pem(),
        client_cert: client.pem(),
        client_key: client_key.serialize_pem(),
    }
}

const TLS_CONF: &str = r#"
tls {
  cert_file: "/server.pem"
  key_file: "/server.key"
  ca_file: "/ca.pem"
  verify: true
  timeout: 5
}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (NATS)"]
async fn mutual_tls_with_ca_and_client_certificate() {
    init_tracing();
    let keys = pki();
    let server = Server::start_with(
        "tls",
        Launch {
            args: &["-c", "/tls.conf"],
            files: &[
                ("/tls.conf", TLS_CONF.as_bytes()),
                ("/server.pem", keys.server_cert.as_bytes()),
                ("/server.key", keys.server_key.as_bytes()),
                ("/ca.pem", keys.ca.as_bytes()),
            ],
        },
    )
    .await;

    let certs = TempDir::new().expect("cert dir");
    let write = |name: &str, pem: &str| {
        let path = certs.path().join(name);
        std::fs::write(&path, pem).expect("write pem");
        path
    };
    let ca = write("ca.pem", &keys.ca);
    let client_cert = write("client.pem", &keys.client_cert);
    let client_key = write("client.key", &keys.client_key);

    let admin = async_nats::ConnectOptions::new()
        .add_root_certificates(ca.clone())
        .add_client_certificate(client_cert.clone(), client_key.clone())
        .require_tls(true)
        .connect(server.tls_url())
        .await
        .expect("admin tls connect");
    let js = jetstream::new(admin);
    create_stream(&js, Duration::from_secs(120)).await;
    let dir = TempDir::new().expect("tempdir");

    let tls_cfg = |tls: serde_json::Value| serde_json::json!({"url": server.tls_url(), "tls": tls});

    let mut sink = build_sink(
        &server,
        tls_cfg(serde_json::json!({
            "ca_file": ca,
            "client_cert": client_cert,
            "client_key": client_key,
        })),
    );
    let caps = sink
        .open(&sink_context(&dir))
        .await
        .expect("mutual tls open");
    assert_eq!(caps.delivery, DeliveryClass::IdempotentBatch);
    let outcome = sink.write(&make_batch(0, 3, "r7")).await.expect("write");
    assert!(outcome.rejected.is_empty());
    assert_eq!(stored_message_count(&js).await, 3);
    sink.close().await.expect("close");

    // The server demands a client certificate: a CA-only configuration, and
    // a client that does not trust the server's CA, both fail to connect.
    for (what, tls) in [
        ("no client certificate", serde_json::json!({"ca_file": ca})),
        (
            "untrusted server certificate",
            serde_json::json!({
                "ca_file": write("other-ca.pem", &pki().ca),
                "client_cert": client_cert,
                "client_key": client_key,
            }),
        ),
    ] {
        let mut sink = build_sink(&server, tls_cfg(tls));
        let e = sink.open(&sink_context(&dir)).await.expect_err(what);
        assert!(e.is_transient(), "{what}: {e}");
    }
}
