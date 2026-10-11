//! Docker integration tests for the ClickHouse sink against a real server.
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker. Run
//! them with
//!
//! ```text
//! export CARGO_TARGET_DIR=$PWD/target/ch
//! cargo nextest run -p thadam-clickhouse --run-ignored only -E 'binary(docker_clickhouse)'
//! ```
//!
//! Image: public Docker Hub only, pinned to the LTS tag
//! `clickhouse/clickhouse-server:26.8.22.13`; re-pin (or try the current
//! stable) with `THADAM_TEST_CLICKHOUSE_IMAGE`. The first run pulls the image.
//!
//! Each test starts its own container with a random free host port, chosen once
//! per container: `docker stop` + `docker start` re-allocates unpinned
//! ephemeral ports, and the sink bakes the URL in. Containers carry the label
//! `thadam-clickhouse-test` and are force-removed (with their anonymous
//! volumes) when the test ends, including on panic. Leftovers from a killed run:
//!
//! ```text
//! docker rm -f -v $(docker ps -aq --filter label=thadam-clickhouse-test)
//! ```
//!
//! Logs: run with `RUST_LOG=debug`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clickhouse::Client;
use thadam_clickhouse::ClickHouseSinkFactory;
use thadam_core::{
    AssetPath, BatchId, BatchSink, BatchSinkFactory, BuildContext, DatasetDecl, DeliveryClass,
    DiskBudget, ErrorDomain, Event, EventBatch, EventId, FieldDef, FieldType, LevelDecl, Record,
    Retry, ThadamError, Shape, ShapeRef, SinkBatch, SinkCapabilities, SinkContext, SinkId,
    SystemClock, TimeDomain, Times, Timestamp, Value, WalEpoch, WalPos, WalRange, WriteOutcome,
    codes,
};
use thadam_testkit::conformance::batch_sink::{
    self, BatchSinkCase, BatchSinkFixture, BatchSinkProbe, InjectedFault,
};
use tempfile::TempDir;

/// The pinned LTS image (public Docker Hub only).
const IMAGE: &str = "clickhouse/clickhouse-server:26.8.22.13";
const LABEL: &str = "thadam-clickhouse-test";
const USER: &str = "thadam";
const PASSWORD: &str = "thadam-test-password";
const READY_TIMEOUT: Duration = Duration::from_secs(180);

fn image() -> String {
    std::env::var("THADAM_TEST_CLICKHOUSE_IMAGE").unwrap_or_else(|_| IMAGE.to_owned())
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker cli")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind :0")
        .local_addr()
        .expect("local addr")
        .port()
}

/// A running ClickHouse container, removed on drop. The HTTP port is pinned on
/// the host so stop/start cycles keep the URL valid.
struct Server {
    name: String,
    port: u16,
}

impl Server {
    async fn start() -> Server {
        let server = tokio::task::spawn_blocking(Self::start_blocking)
            .await
            .expect("docker task");
        server.wait_ready_blocking();
        let admin = server.admin();
        exec(&admin, "CREATE DATABASE IF NOT EXISTS telemetry").await;
        server
    }

    fn start_blocking() -> Server {
        let mut last_err = String::new();
        // Another test process can claim the port between `free_port` and the
        // container start, so retry with a fresh one.
        for attempt in 0..8 {
            let port = free_port();
            let name = format!(
                "thadam-clickhouse-{}-{:x}-{attempt}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let publish = format!("127.0.0.1:{port}:8123");
            let user = format!("CLICKHOUSE_USER={USER}");
            let password = format!("CLICKHOUSE_PASSWORD={PASSWORD}");
            let image = image();
            let out = docker(&[
                "create",
                "--name",
                &name,
                "--label",
                LABEL,
                "--ulimit",
                "nofile=262144:262144",
                "-e",
                &user,
                "-e",
                &password,
                "-p",
                &publish,
                &image,
            ]);
            assert!(out.status.success(), "docker create: {}", stderr(&out));
            // The container exists from here: Drop removes it on any failure.
            let server = Server { name, port };
            let out = docker(&["start", &server.name]);
            if out.status.success() {
                return server;
            }
            last_err = stderr(&out);
            drop(server);
            if !(last_err.contains("port is already allocated")
                || last_err.contains("address already in use"))
            {
                break;
            }
        }
        panic!("docker start: {last_err}");
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn admin(&self) -> Client {
        Client::default()
            .with_url(self.url())
            .with_user(USER)
            .with_password(PASSWORD)
    }

    /// Ready when `/ping` answers and an authenticated statement succeeds.
    /// The image runs a throwaway localhost-only server for its init steps, so
    /// the published port only answers once the real one is up.
    fn wait_ready_blocking(&self) {
        let started = Instant::now();
        loop {
            if self.ping() {
                return;
            }
            let state = docker(&["inspect", "-f", "{{.State.Running}}", &self.name]);
            if String::from_utf8_lossy(&state.stdout).trim() == "false" {
                let logs = docker(&["logs", "--tail", "40", &self.name]);
                panic!(
                    "clickhouse container exited during startup:\n{}{}",
                    String::from_utf8_lossy(&logs.stdout),
                    stderr(&logs)
                );
            }
            assert!(
                started.elapsed() < READY_TIMEOUT,
                "clickhouse did not become ready"
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    fn ping(&self) -> bool {
        let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", self.port)) else {
            return false;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
        if stream
            .write_all(b"GET /ping HTTP/1.0\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return false;
        }
        let mut body = String::new();
        let _ = stream.read_to_string(&mut body);
        body.contains("Ok.")
    }

    fn run(&self, verb: &str) {
        let out = docker(&[verb, &self.name]);
        assert!(out.status.success(), "docker {verb}: {}", stderr(&out));
    }

    /// Simulated outage: the process exits and the port refuses connections.
    fn stop_container(&self) {
        self.run("stop");
    }

    fn start_container(&self) {
        self.run("start");
        self.wait_ready_blocking();
    }

    /// Simulated hang: the process is frozen, connections time out.
    fn pause_container(&self) {
        self.run("pause");
    }

    fn unpause_container(&self) {
        self.run("unpause");
        self.wait_ready_blocking();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.name]);
    }
}

async fn exec(client: &Client, sql: &str) {
    client
        .query(sql)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Runs a query and returns every cell as text; the SQL wraps non-string
/// columns in `toString`.
async fn rows(client: &Client, sql: &str) -> Vec<Vec<String>> {
    let mut cursor = client
        .query(sql)
        .fetch_native()
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    while let Some(block) = cursor.next().await.unwrap_or_else(|e| panic!("{sql}: {e}")) {
        let cols: Vec<Vec<String>> = block
            .columns()
            .iter()
            .map(|c| c.iter::<String>().unwrap().map(Result::unwrap).collect())
            .collect();
        for i in 0..block.num_rows() {
            out.push(cols.iter().map(|c| c[i].clone()).collect());
        }
    }
    out
}

async fn count(client: &Client, table: &str) -> u64 {
    rows(
        client,
        &format!("SELECT toString(count()) FROM telemetry.{table}"),
    )
    .await[0][0]
        .parse()
        .unwrap()
}

/// The telemetry table. `settings` is the `SETTINGS` clause body (empty for
/// none), `note_ty` the type of `note`. The `CHECK` constraint makes
/// `temp <= -273.15` a poison value the server refuses.
fn table_ddl(table: &str, settings: &str, note_ty: &str) -> String {
    let settings = if settings.is_empty() {
        String::new()
    } else {
        format!("SETTINGS {settings}")
    };
    format!(
        "CREATE TABLE telemetry.{table} (
            _event_id UUID,
            _ingest_time DateTime64(6, 'UTC'),
            _event_time Nullable(DateTime64(6, 'UTC')),
            _quality_status LowCardinality(String),
            _wal_pos UInt64,
            robot LowCardinality(Nullable(String)),
            temp Nullable(Float64),
            note {note_ty},
            n Int64,
            counter UInt32,
            samples Array(Float32),
            seen DateTime64(6, 'UTC'),
            CONSTRAINT temp_sane CHECK temp IS NULL OR temp > -273.15
        ) ENGINE = MergeTree ORDER BY _wal_pos {settings}"
    )
}

/// A table whose engine deduplicates retried inserts.
async fn create_dedup_table(admin: &Client, table: &str) {
    exec(
        admin,
        &table_ddl(
            table,
            "non_replicated_deduplication_window = 100",
            "Nullable(String)",
        ),
    )
    .await;
}

/// A table with the server default window (0): no deduplication.
async fn create_plain_table(admin: &Client, table: &str) {
    exec(admin, &table_ddl(table, "", "Nullable(String)")).await;
}

fn dataset_decl() -> DatasetDecl {
    DatasetDecl::new(
        "imu",
        vec![
            FieldDef::new("temp", FieldType::F64).with_field_id(1000),
            FieldDef::new("note", FieldType::Str).with_field_id(1001),
            FieldDef::new("n", FieldType::I64)
                .required()
                .with_field_id(1002),
            FieldDef::new("counter", FieldType::U32)
                .required()
                .with_field_id(1003),
            FieldDef::new("samples", FieldType::List(Box::new(FieldType::F32))).with_field_id(1004),
            FieldDef::new("seen", FieldType::Ts)
                .required()
                .with_field_id(1005),
        ],
        vec![LevelDecl::new("robot", 100)],
        false,
    )
}

fn build_sink_for(
    decl: DatasetDecl,
    server: &Server,
    table: &str,
    extra: serde_json::Value,
) -> Box<dyn BatchSink> {
    let mut value = serde_json::json!({
        "url": server.url(),
        "database": "telemetry",
        "table": table,
        "user": USER,
        "password": PASSWORD,
        "request_timeout": "20s",
        "datasets": [decl.id.to_string()],
    });
    for (k, v) in extra.as_object().expect("object") {
        value[k] = v.clone();
    }
    let ctx = BuildContext::new(
        "ch-out",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-clickhouse-test-build"),
    )
    .with_datasets(Arc::from([decl]));
    ClickHouseSinkFactory::new()
        .build(value, &ctx)
        .expect("build sink")
}

fn build_sink(server: &Server, table: &str, extra: serde_json::Value) -> Box<dyn BatchSink> {
    build_sink_for(dataset_decl(), server, table, extra)
}

fn sink_context(dir: &TempDir) -> SinkContext {
    SinkContext::new(
        SinkId::new("ch-out"),
        thadam_core::Shutdown::new(),
        Arc::new(SystemClock::new()),
        dir.path().to_owned(),
        Arc::new(DiskBudget::new()),
    )
}

async fn open(sink: &mut Box<dyn BatchSink>) -> SinkCapabilities {
    let dir = TempDir::new().expect("tempdir");
    sink.open(&sink_context(&dir)).await.expect("open")
}

fn shape() -> ShapeRef {
    ShapeRef::new(
        Shape::new(vec![
            FieldDef::new("temp", FieldType::F64),
            FieldDef::new("note", FieldType::Str),
            FieldDef::new("n", FieldType::I64),
            FieldDef::new("counter", FieldType::U32),
            FieldDef::new("samples", FieldType::List(Box::new(FieldType::F32))),
            FieldDef::new("seen", FieldType::Ts),
        ])
        .expect("shape"),
    )
}

const EVENT_ID_BASE: u128 = 0x0123_4567_89ab_cdef_0000_0000_0000_0000;
const T0_NANOS: i64 = 1_700_000_000_000_000_000;

/// The temperature of event `pos`: every third one is null.
fn temp_of(pos: u64) -> Option<f64> {
    (!pos.is_multiple_of(3)).then_some(20.0 + pos as f64 * 0.5)
}

fn note_of(pos: u64) -> Option<String> {
    (!pos.is_multiple_of(4)).then(|| format!("m{pos}"))
}

fn samples_of(pos: u64) -> Option<Vec<f32>> {
    pos.is_multiple_of(2).then(|| vec![pos as f32, 0.5])
}

fn event(pos: u64, temp: Option<f64>, note: Option<String>) -> Event {
    let values = vec![
        temp.map_or(Value::Null, Value::F64),
        note.map_or(Value::Null, Value::str),
        Value::I64(pos as i64),
        Value::U32(pos as u32 + 10),
        samples_of(pos).map_or(Value::Null, |s| {
            Value::List(s.into_iter().map(Value::F32).collect())
        }),
        Value::Ts(Timestamp::from_nanos(T0_NANOS + 1_500 * pos as i64)),
    ];
    Event::builder(
        EventId::from_u128(EVENT_ID_BASE + u128::from(pos)),
        WalPos::new(pos),
        Times::new(
            Timestamp::from_nanos(T0_NANOS + 2_000 * pos as i64),
            TimeDomain::Utc,
        ),
    )
    .asset(AssetPath::new([("robot", "r1")]).expect("asset"))
    .payload(Record::new(shape(), values).expect("record"))
    .build()
    .expect("event")
}

/// A value for one event that replaces the generated ones: its position, the
/// temperature, and the note.
type Special = (u64, Option<f64>, Option<String>);

/// Events `first..=last` as one batch; `special` replaces one event's values.
fn batch_with(first: u64, last: u64, special: Option<Special>) -> SinkBatch {
    let events: Vec<Event> = (first..=last)
        .map(|pos| match &special {
            Some((at, temp, note)) if *at == pos => event(pos, *temp, note.clone()),
            _ => event(pos, temp_of(pos), note_of(pos)),
        })
        .collect();
    let range =
        WalRange::new(WalEpoch::new(9), WalPos::new(first), WalPos::new(last)).expect("range");
    SinkBatch::new(EventBatch::new(events, range, true))
}

fn batch(first: u64, last: u64) -> SinkBatch {
    batch_with(first, last, None)
}

const READ_BACK: &str = "SELECT toString(_event_id), toString(toUnixTimestamp64Micro(_ingest_time)), \
     toString(_wal_pos), toString(_quality_status), ifNull(robot, 'NULL'), \
     ifNull(toString(temp), 'NULL'), ifNull(note, 'NULL'), toString(n), toString(counter), \
     toString(samples), toString(toUnixTimestamp64Micro(seen)) \
     FROM telemetry.TABLE ORDER BY _wal_pos";

fn uuid_text(pos: u64) -> String {
    let hex = format!("{:032x}", EVENT_ID_BASE + u128::from(pos));
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn expected_row(pos: u64) -> Vec<String> {
    let samples = samples_of(pos).map_or("[]".to_owned(), |s| {
        format!(
            "[{}]",
            s.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    });
    vec![
        uuid_text(pos),
        (T0_NANOS / 1_000 + 2 * pos as i64).to_string(),
        pos.to_string(),
        "good".to_owned(),
        "r1".to_owned(),
        temp_of(pos).map_or("NULL".to_owned(), |t| t.to_string()),
        note_of(pos).unwrap_or_else(|| "NULL".to_owned()),
        pos.to_string(),
        (pos + 10).to_string(),
        samples,
        ((T0_NANOS + 1_500 * pos as i64) / 1_000).to_string(),
    ]
}

/// Retries `write` like the runtime does for a Transient error, with the same
/// batch. Returns how many attempts it took.
async fn write_with_retries(
    sink: &mut Box<dyn BatchSink>,
    batch: &SinkBatch,
    attempts: u32,
) -> u32 {
    for attempt in 1..=attempts {
        match sink.write(batch).await {
            Ok(outcome) => {
                assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
                return attempt;
            }
            Err(e) if e.retry == Retry::Transient => {
                eprintln!("attempt {attempt}: transient {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("attempt {attempt}: {e}"),
        }
    }
    panic!("no success in {attempts} attempts");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn inserts_and_reads_back_every_column() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    create_dedup_table(&admin, "imu").await;

    let mut sink = build_sink(&server, "imu", serde_json::json!({}));
    let caps = open(&mut sink).await;
    assert_eq!(caps.delivery, DeliveryClass::IdempotentBatch);

    let b = batch(1, 8);
    let outcome = sink.write(&b).await.expect("write");
    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);

    let got = rows(&admin, &READ_BACK.replace("TABLE", "imu")).await;
    let want: Vec<Vec<String>> = (1..=8).map(expected_row).collect();
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g, w);
    }

    // A second distinct batch accumulates; the sink is reusable.
    sink.write(&batch(9, 10)).await.expect("second batch");
    assert_eq!(count(&admin, "imu").await, 10);
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn a_retried_batch_is_inserted_once_when_the_dedup_window_is_set() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    create_dedup_table(&admin, "imu").await;

    let mut sink = build_sink(&server, "imu", serde_json::json!({}));
    assert_eq!(
        open(&mut sink).await.delivery,
        DeliveryClass::IdempotentBatch
    );

    let b = batch(1, 5);
    for _ in 0..3 {
        sink.write(&b).await.expect("the same batch id, again");
    }
    assert_eq!(count(&admin, "imu").await, 5, "same token, no duplicates");

    // A different batch id is a different token and lands.
    sink.write(&batch(6, 8)).await.expect("next batch");
    assert_eq!(count(&admin, "imu").await, 8);

    // A restarted sink (a crash) rebuilds the batch from the same WAL range,
    // so the token repeats and the server still refuses the duplicate.
    let mut reborn = build_sink(&server, "imu", serde_json::json!({}));
    open(&mut reborn).await;
    reborn
        .write(&batch(1, 5))
        .await
        .expect("replay after crash");
    assert_eq!(count(&admin, "imu").await, 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn without_a_window_a_retried_batch_is_documented_to_duplicate() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();

    // No deduplication window: the probe says at-least-once and the retry
    // really does duplicate. This is the documented behavior.
    create_plain_table(&admin, "plain").await;
    let mut sink = build_sink(&server, "plain", serde_json::json!({}));
    assert_eq!(open(&mut sink).await.delivery, DeliveryClass::AtLeastOnce);
    let b = batch(1, 4);
    sink.write(&b).await.expect("first");
    sink.write(&b).await.expect("retry");
    assert_eq!(
        count(&admin, "plain").await,
        8,
        "duplicates without a window"
    );

    // `dedup: off` on a table that could deduplicate sends no token and
    // reports at-least-once. The server's own content-hash deduplication still
    // drops a byte-identical retry when the table keeps a window (observed on
    // the pinned server: `insert_deduplicate = 0` does not prevent it on
    // non-replicated tables), so the retry is a no-op here: a bonus the sink
    // does not rely on, not a guarantee it claims.
    create_dedup_table(&admin, "off").await;
    let mut off = build_sink(&server, "off", serde_json::json!({ "dedup": "off" }));
    assert_eq!(open(&mut off).await.delivery, DeliveryClass::AtLeastOnce);
    off.write(&b).await.expect("first");
    off.write(&b).await.expect("retry");
    assert_eq!(
        count(&admin, "off").await,
        4,
        "content-hash dedup by the table"
    );

    // `dedup: token` refuses a table that cannot honor it.
    let mut strict = build_sink(&server, "plain", serde_json::json!({ "dedup": "token" }));
    let dir = TempDir::new().unwrap();
    let err = strict
        .open(&sink_context(&dir))
        .await
        .expect_err("no window, token required");
    assert_eq!(err.domain, ErrorDomain::Config);
    assert_eq!(err.retry, Retry::Permanent);
    assert!(err.message.contains("no deduplication window"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn survives_the_server_going_down_and_coming_back() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    create_dedup_table(&admin, "imu").await;

    let mut sink = build_sink(
        &server,
        "imu",
        serde_json::json!({ "request_timeout": "5s" }),
    );
    assert_eq!(
        open(&mut sink).await.delivery,
        DeliveryClass::IdempotentBatch
    );
    sink.write(&batch(1, 3)).await.expect("healthy write");

    server.stop_container();

    // While the server is down every write is a Transient error: the runtime
    // keeps the batch and retries it.
    let b = batch(4, 8);
    let err = sink.write(&b).await.expect_err("server is down");
    assert_eq!(err.retry, Retry::Transient, "{err}");
    assert_eq!(err.domain, ErrorDomain::Sink, "{err}");

    // `open` against a dead server is Transient too, so a pipeline that starts
    // before ClickHouse waits for it instead of failing.
    let mut cold = build_sink(
        &server,
        "imu",
        serde_json::json!({ "request_timeout": "5s" }),
    );
    let dir = TempDir::new().unwrap();
    let err = cold
        .open(&sink_context(&dir))
        .await
        .expect_err("open while down");
    assert_eq!(err.retry, Retry::Transient, "{err}");

    server.start_container();

    // The retried batch lands, exactly once even if retried again afterwards.
    let attempts = write_with_retries(&mut sink, &b, 10).await;
    eprintln!("recovered after {attempts} attempt(s)");
    sink.write(&b).await.expect("extra retry");
    assert_eq!(count(&admin, "imu").await, 8);

    // The cold sink can now open and write.
    assert_eq!(
        open(&mut cold).await.delivery,
        DeliveryClass::IdempotentBatch
    );
    cold.write(&batch(9, 9)).await.expect("late sink");
    assert_eq!(count(&admin, "imu").await, 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn a_timed_out_insert_is_retried_with_the_same_token() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    create_dedup_table(&admin, "imu").await;

    let mut sink = build_sink(
        &server,
        "imu",
        serde_json::json!({ "request_timeout": "2s" }),
    );
    assert_eq!(
        open(&mut sink).await.delivery,
        DeliveryClass::IdempotentBatch
    );

    // A frozen server accepts the request and never answers: unknown outcome.
    server.pause_container();
    let b = batch(1, 6);
    let started = Instant::now();
    let err = sink.write(&b).await.expect_err("server is frozen");
    eprintln!(
        "paused write failed after {:?} with {err}",
        started.elapsed()
    );
    assert_eq!(err.retry, Retry::Transient, "{err}");
    assert!(
        err.code == codes::SINK_TIMEOUT || err.code == codes::SINK_UNAVAILABLE,
        "{err}"
    );
    server.unpause_container();

    // Whether or not the frozen attempt landed after the thaw, the retry with
    // the same batch id leaves exactly one copy.
    write_with_retries(&mut sink, &b, 10).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(count(&admin, "imu").await, 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn a_poison_row_is_permanent_and_the_halves_still_land() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    create_dedup_table(&admin, "imu").await;

    let mut sink = build_sink(&server, "imu", serde_json::json!({}));
    open(&mut sink).await;

    // Event 4 violates the table's CHECK constraint (temp <= -273.15).
    let poison =
        |first, last| batch_with(first, last, Some((4, Some(-300.0), Some("hot".to_owned()))));
    let whole = poison(1, 6);
    let err = sink.write(&whole).await.expect_err("poison batch");
    assert_eq!(err.retry, Retry::Permanent, "{err}");
    assert_eq!(err.domain, ErrorDomain::Sink, "{err}");
    assert_eq!(err.code, codes::SINK_REJECTED, "{err}");
    assert_eq!(err.context.get("clickhouse_code"), Some("469"), "{err}");
    assert!(
        !format!("{err:?}").contains("-300"),
        "no payload in errors: {err:?}"
    );
    assert_eq!(
        count(&admin, "imu").await,
        0,
        "a refused batch inserts nothing"
    );

    // The runtime bisects: halves have new ids and new tokens.
    let (left, right) = (poison(1, 3), poison(4, 6));
    assert_ne!(left.id(), whole.id());
    sink.write(&left).await.expect("left half is clean");
    let err = sink.write(&right).await.expect_err("poison half");
    assert_eq!(err.retry, Retry::Permanent);
    // ... down to the single poison event, and the clean remainder lands.
    let (just_poison, rest) = (poison(4, 4), batch(5, 6));
    let err = sink
        .write(&just_poison)
        .await
        .expect_err("single poison event");
    assert_eq!(err.retry, Retry::Permanent);
    sink.write(&rest).await.expect("clean remainder");

    let ids = rows(
        &admin,
        "SELECT toString(_wal_pos) FROM telemetry.imu ORDER BY _wal_pos",
    )
    .await;
    let ids: Vec<&str> = ids.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, ["1", "2", "3", "5", "6"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn an_overlong_fixed_string_is_a_permanent_error_not_a_silent_null() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    // `note` is FixedString(3). Left to the server, a Native `String` that is too
    // long for a nullable FixedString column is silently stored as NULL, so the
    // sink checks the length itself and fails the batch.
    exec(
        &admin,
        &table_ddl(
            "fixed",
            "non_replicated_deduplication_window = 100",
            "Nullable(FixedString(3))",
        ),
    )
    .await;

    let mut sink = build_sink(&server, "fixed", serde_json::json!({}));
    open(&mut sink).await;

    let b = batch_with(1, 3, Some((2, Some(1.0), Some("toolong".to_owned()))));
    let err = sink
        .write(&b)
        .await
        .expect_err("over-long FixedString value");
    assert_eq!(err.retry, Retry::Permanent, "{err}");
    assert_eq!(err.domain, ErrorDomain::Schema, "{err}");
    assert!(err.message.contains("FixedString(3)"), "{err}");
    assert_eq!(count(&admin, "fixed").await, 0);

    // Values that do fit still go in.
    let ok = batch_with(1, 3, Some((2, Some(1.0), Some("abc".to_owned()))));
    sink.write(&ok).await.expect("fits FixedString(3)");
    assert_eq!(count(&admin, "fixed").await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn datetime_columns_hold_seconds_and_timezones_do_not_shift_values() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    exec(
        &admin,
        "CREATE TABLE telemetry.dt (
            ingested DateTime, seen DateTime64(3, 'Europe/Berlin'),
            temp Nullable(Float64), note Nullable(String), n Int64, counter UInt32,
            samples Array(Float32)
        ) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    let mut sink = build_sink(
        &server,
        "dt",
        serde_json::json!({ "columns": {
            "_ingest_time": "ingested", "seen": "seen", "temp": "temp", "note": "note",
            "n": "n", "counter": "counter", "samples": "samples"
        }}),
    );
    open(&mut sink).await;
    sink.write(&batch(1, 3)).await.expect("write");
    let got = rows(
        &admin,
        "SELECT toString(toUnixTimestamp(ingested)), toString(toUnixTimestamp64Milli(seen)) \
         FROM telemetry.dt ORDER BY n",
    )
    .await;
    for (i, pos) in (1..=3u64).enumerate() {
        let ingest_secs = (T0_NANOS + 2_000 * pos as i64) / 1_000_000_000;
        let seen_millis = (T0_NANOS + 1_500 * pos as i64) / 1_000_000;
        assert_eq!(got[i], [ingest_secs.to_string(), seen_millis.to_string()]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn a_schema_mismatch_is_a_config_error_and_nothing_is_created_or_altered() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    exec(
        &admin,
        "CREATE TABLE telemetry.bad (
            temp String, note Nullable(String), n Int32, counter UInt32, seen DateTime64(6, 'UTC')
        ) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    let tables = || {
        rows(
            &admin,
            "SELECT name FROM system.tables WHERE database = 'telemetry' ORDER BY name",
        )
    };
    let ddl = || rows(&admin, "SHOW CREATE TABLE telemetry.bad");
    let (tables_before, ddl_before) = (tables().await, ddl().await);

    // Wrong types, a missing `samples` column, and a nullable-into-non-null.
    let mut sink = build_sink(&server, "bad", serde_json::json!({}));
    let dir = TempDir::new().unwrap();
    let err = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("schema mismatch");
    assert_eq!(err.domain, ErrorDomain::Config, "{err}");
    assert_eq!(err.retry, Retry::Permanent, "{err}");
    for want in [
        "`temp`",
        "Float64 cannot feed ClickHouse type String",
        "`n`",
        "declared dataset field `samples` has no column",
        "never creates or alters tables",
    ] {
        assert!(
            err.message.contains(want),
            "missing `{want}` in: {}",
            err.message
        );
    }

    // A table that does not exist is a Config error carrying the server's
    // explanation, and open does not create it.
    let mut ghost = build_sink(&server, "ghost", serde_json::json!({}));
    let err = ghost
        .open(&sink_context(&dir))
        .await
        .expect_err("missing table");
    assert_eq!(err.domain, ErrorDomain::Config, "{err}");
    assert_eq!(err.retry, Retry::Permanent, "{err}");
    assert!(err.message.contains("ghost"), "{err}");

    assert_eq!(tables().await, tables_before, "no table was created");
    assert_eq!(ddl().await, ddl_before, "no table was altered");

    // An explicit mapping can bridge differently named columns — including a
    // dotted table column, which backticking keeps one literal identifier.
    exec(
        &admin,
        "CREATE TABLE telemetry.mapped (
            temperature Nullable(Float64), remark Nullable(String), n Int64, cnt UInt32,
            readings Array(Float32), seen DateTime64(6, 'UTC'), event_id UUID,
            `pose.position.x` Nullable(Float64)
        ) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    let mut mapped = build_sink(
        &server,
        "mapped",
        serde_json::json!({ "columns": {
            "temp": "pose.position.x", "note": "remark", "n": "n", "counter": "cnt",
            "samples": "readings", "seen": "seen", "_event_id": "event_id"
        }}),
    );
    open(&mut mapped).await;
    mapped.write(&batch(1, 3)).await.expect("mapped write");
    let got = rows(
        &admin,
        "SELECT toString(event_id), ifNull(toString(`pose.position.x`), 'NULL'), toString(cnt) FROM telemetry.mapped ORDER BY n",
    )
    .await;
    assert_eq!(
        got[0],
        [
            uuid_text(1),
            temp_of(1).unwrap().to_string(),
            "11".to_owned()
        ]
    );
    assert_eq!(got[2][1], "NULL");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn bad_credentials_are_fatal() {
    init_tracing();
    let server = Server::start().await;
    create_dedup_table(&server.admin(), "imu").await;

    let mut sink = build_sink(&server, "imu", serde_json::json!({ "password": "wrong" }));
    let dir = TempDir::new().unwrap();
    let err = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("wrong password");
    assert_eq!(err.retry, Retry::Fatal, "{err}");
    assert_eq!(err.domain, ErrorDomain::Auth, "{err}");

    let mut sink = build_sink(&server, "imu", serde_json::json!({ "user": "nobody" }));
    let err = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("unknown user");
    assert_eq!(err.retry, Retry::Fatal, "{err}");
}

/// A multi-thousand-row batch (a realistic `max_rows`) round-trips, and its
/// retry is a no-op at the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn a_large_batch_round_trips_and_deduplicates() {
    init_tracing();
    let server = Server::start().await;
    let admin = server.admin();
    create_dedup_table(&admin, "imu").await;

    let mut sink = build_sink(&server, "imu", serde_json::json!({}));
    open(&mut sink).await;
    let b = batch(1, 3_000);
    sink.write(&b).await.expect("large batch");
    sink.write(&b).await.expect("retried large batch");
    assert_eq!(count(&admin, "imu").await, 3_000);
}

// --- the shared batch-sink conformance suite, against the live server -------

/// Wraps the sink under test to record every write attempt's batch id.
struct Recording {
    inner: Box<dyn BatchSink>,
    ids: Arc<Mutex<Vec<BatchId>>>,
}

#[async_trait]
impl BatchSink for Recording {
    async fn open(&mut self, ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        self.inner.open(ctx).await
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        self.ids.lock().unwrap().push(batch.id());
        self.inner.write(batch).await
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.inner.close().await
    }
}

struct ClickHouseProbe {
    admin: Client,
    table: String,
    ids: Arc<Mutex<Vec<BatchId>>>,
}

impl BatchSinkProbe for ClickHouseProbe {
    fn applied_events(&self) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { count(&self.admin, &self.table).await as usize })
        })
    }

    fn attempt_ids(&self) -> Vec<BatchId> {
        self.ids.lock().unwrap().clone()
    }

    fn inject(&self, _fault: InjectedFault) -> bool {
        // The server cannot be steered per write from here; those clauses are
        // covered by the dedicated outage, timeout, and poison tests above.
        false
    }
}

struct ClickHouseFixture {
    url: String,
    admin: Client,
    next: AtomicU32,
}

impl BatchSinkFixture for ClickHouseFixture {
    fn create(&self) -> BatchSinkCase {
        let table = format!("conformance_{}", self.next.fetch_add(1, Ordering::SeqCst));
        let ids = Arc::new(Mutex::new(Vec::new()));
        let sink = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                exec(
                    &self.admin,
                    &format!(
                        "CREATE TABLE telemetry.{table} (n Int64, _wal_pos UInt64) \
                         ENGINE = MergeTree ORDER BY _wal_pos \
                         SETTINGS non_replicated_deduplication_window = 100"
                    ),
                )
                .await;
            });
            let decl = DatasetDecl::new(
                "events",
                vec![
                    FieldDef::new("n", FieldType::I64)
                        .required()
                        .with_field_id(1000),
                ],
                Vec::new(),
                false,
            );
            let ctx = BuildContext::new(
                "ch-conformance",
                Arc::new(SystemClock::new()),
                std::path::PathBuf::from("/tmp/thadam-clickhouse-conformance"),
            )
            .with_datasets(Arc::from([decl]));
            ClickHouseSinkFactory::new()
                .build(
                    serde_json::json!({
                        "url": self.url,
                        "database": "telemetry",
                        "table": table,
                        "user": USER,
                        "password": PASSWORD,
                        "datasets": ["events"],
                    }),
                    &ctx,
                )
                .expect("build conformance sink")
        });
        BatchSinkCase {
            sink: Box::new(Recording {
                inner: sink,
                ids: Arc::clone(&ids),
            }),
            probe: Box::new(ClickHouseProbe {
                admin: self.admin.clone(),
                table,
                ids,
            }),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (ClickHouse)"]
async fn passes_the_batch_sink_conformance_suite() {
    init_tracing();
    let server = Server::start().await;
    let fixture = ClickHouseFixture {
        url: server.url(),
        admin: server.admin(),
        next: AtomicU32::new(0),
    };
    batch_sink::run_all(&fixture).await;
}
