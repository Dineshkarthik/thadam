//! Docker end-to-end test for `decode: sparkplug_b`.
//!
//! Mosquitto 2 in a container, a small Rust publisher playing Sparkplug B
//! edge nodes (NBIRTH, DBIRTH, alias-only NDATA/DDATA, a seq gap, an NDEATH,
//! data after death), and the real pipeline: an MQTT source subscribed to
//! `spBv1.0/#` feeding a `decode: sparkplug_b` dataset into local Parquet.
//!
//! Run 1 asserts row counts, resolved names and values, exactly one
//! `SEQUENCE_GAP` row and exactly one `STALE` row. Run 2 restarts `thadam`
//! over the same data directory and delivers only DATA (no BIRTH anywhere in
//! the new traffic or the replayed WAL range): its metrics must still resolve,
//! from the birth store, a live node must carry no false gap, and the dead
//! node must stay stale.
//!
//! Determinism: the test first creates the persistent MQTT session
//! (`clean_start: false`, fixed client id) that `thadam` will resume, so the
//! broker queues everything the publisher sends while `thadam` is down.
//!
//! `#[ignore]`d so `cargo nextest run` passes without Docker; run it with
//!
//! ```text
//! cargo nextest run -p thadam-cli --run-ignored only -E 'binary(sparkplug_docker)'
//! ```
//!
//! Image: public Docker Hub `eclipse-mosquitto:2`, pinned by tag (re-pin with
//! `THADAM_TEST_MQTT_IMAGE`). The container is labelled
//! `thadam-sparkplug-e2e` and removed when the test ends, including on panic.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use prost::Message as _;
use thadam_core::QualityFlags;
use thadam_sparkplug::pb::{
    self, DataType as D,
    payload::{Metric, metric::Value as V},
};
use rumqttc::{Client, Event, Incoming, MqttOptions, QoS};

const MQTT_IMAGE: &str = "eclipse-mosquitto:2";
const LABEL: &str = "thadam-sparkplug-e2e";
const CLIENT_ID: &str = "thadam-sparkplug-e2e";
const TIMEOUT: Duration = Duration::from_secs(120);
/// How long each `thadam run` is given to read and commit a handful of tiny
/// messages before the test stops it.
const CAPTURE: Duration = Duration::from_secs(8);

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker cli")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

/// A running Mosquitto container; removed on Drop.
struct Broker {
    name: String,
    port: u16,
    _conf: PathBuf,
}

impl Broker {
    fn start(tmp: &Path) -> Broker {
        let conf = tmp.join("mosquitto.conf");
        fs::write(
            &conf,
            "listener 1883\nallow_anonymous true\npersistence false\n",
        )
        .unwrap();
        let port = free_port();
        let name = format!("thadam-sparkplug-e2e-{}-{port}", std::process::id());
        let image =
            std::env::var("THADAM_TEST_MQTT_IMAGE").unwrap_or_else(|_| MQTT_IMAGE.to_owned());
        let out = docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            LABEL,
            "-p",
            &format!("127.0.0.1:{port}:1883"),
            "-v",
            &format!("{}:/mosquitto/config/mosquitto.conf:ro", conf.display()),
            &image,
        ]);
        assert!(
            out.status.success(),
            "docker run broker: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let broker = Broker {
            name,
            port,
            _conf: conf,
        };
        broker.wait_ready();
        broker
    }

    fn wait_ready(&self) {
        let started = Instant::now();
        loop {
            if mqtt_connect_ok(self.port) {
                return;
            }
            assert!(started.elapsed() < TIMEOUT, "mosquitto did not come up");
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.name]);
    }
}

/// A raw MQTT 3.1.1 CONNECT that waits for CONNACK: the port accepting TCP
/// is not proof the broker serves yet.
fn mqtt_connect_ok(port: u16) -> bool {
    use std::io::Write as _;
    let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let connect = [
        0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x02, b'r',
        b'v',
    ];
    if s.write_all(&connect).is_err() {
        return false;
    }
    let mut ack = [0u8; 4];
    s.read_exact(&mut ack).is_ok() && ack[0] == 0x20 && ack[3] == 0
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "thadam-sparkplug-e2e-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Kills the child on drop (after the test's own SIGTERM).
struct Thadam(Child);

impl Drop for Thadam {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sigterm(child: &Child) {
    let pid = child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("kill -TERM")
            .success(),
        "kill -TERM {pid}"
    );
}

fn wait_exit(child: &mut Child, within: Duration) -> std::process::ExitStatus {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        assert!(
            started.elapsed() < within,
            "thadam did not exit within {within:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn tail(path: &Path, lines: usize) -> String {
    let mut buf = String::new();
    if let Ok(mut f) = fs::File::open(path) {
        let _ = f.read_to_string(&mut buf);
    }
    let all: Vec<_> = buf.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

// ----- MQTT helpers (blocking rumqttc) ------------------------------------

fn options(port: u16, clean: bool) -> MqttOptions {
    let mut o = MqttOptions::new(CLIENT_ID, "127.0.0.1", port);
    o.set_clean_session(clean)
        .set_keep_alive(Duration::from_secs(5));
    o
}

/// Creates the persistent session thadam resumes: subscribes `spBv1.0/#` at
/// QoS 1 as `CLIENT_ID`, then disconnects, leaving the session on the broker.
fn create_session(port: u16) {
    let (client, mut connection) = Client::new(options(port, false), 8);
    client.subscribe("spBv1.0/#", QoS::AtLeastOnce).unwrap();
    for event in connection.iter() {
        if let Ok(Event::Incoming(Incoming::SubAck(_))) = event {
            break;
        }
    }
    client.disconnect().unwrap();
    for event in connection.iter() {
        if event.is_err() {
            break;
        }
    }
}

/// Publishes every message at QoS 1 (retain false, as Sparkplug requires for
/// edge traffic) on its own client id and waits for each PUBACK.
fn publish(port: u16, messages: &[(&str, Vec<u8>)]) {
    let mut o = MqttOptions::new("thadam-sparkplug-e2e-pub", "127.0.0.1", port);
    o.set_clean_session(true)
        .set_keep_alive(Duration::from_secs(5));
    let (client, mut connection) = Client::new(o, messages.len() + 8);
    for (topic, payload) in messages {
        client
            .publish(*topic, QoS::AtLeastOnce, false, payload.clone())
            .unwrap();
    }
    let mut acked = 0;
    for event in connection.iter() {
        if let Ok(Event::Incoming(Incoming::PubAck(_))) = event {
            acked += 1;
            if acked == messages.len() {
                break;
            }
        }
    }
    assert_eq!(acked, messages.len(), "every publish was acknowledged");
    client.disconnect().unwrap();
    for event in connection.iter() {
        if event.is_err() {
            break;
        }
    }
}

// ----- Sparkplug payloads --------------------------------------------------

const TS: u64 = 1_700_000_000_000;

fn metric(name: Option<&str>, alias: Option<u64>, dt: D, value: V) -> Metric {
    Metric {
        name: name.map(Into::into),
        alias,
        datatype: Some(dt as u32),
        value: Some(value),
        ..Default::default()
    }
}

fn payload(seq: Option<u64>, ts: u64, metrics: Vec<Metric>) -> Vec<u8> {
    pb::Payload {
        timestamp: Some(ts),
        seq,
        metrics,
        ..Default::default()
    }
    .encode_to_vec()
}

fn bd_seq(n: u64) -> Metric {
    metric(Some("bdSeq"), None, D::Int64, V::LongValue(n))
}

fn nbirth(bd: u64) -> Vec<u8> {
    payload(
        Some(0),
        TS,
        vec![
            bd_seq(bd),
            metric(Some("temp"), Some(1), D::Float, V::FloatValue(1.0)),
        ],
    )
}

fn dbirth(seq: u64) -> Vec<u8> {
    payload(
        Some(seq),
        TS + 1,
        vec![metric(
            Some("motor.speed"),
            Some(10),
            D::Double,
            V::DoubleValue(0.0),
        )],
    )
}

fn ndata(seq: u64, temp: f32) -> Vec<u8> {
    payload(
        Some(seq),
        TS + seq,
        vec![metric(None, Some(1), D::Float, V::FloatValue(temp))],
    )
}

fn ddata(seq: u64, speed: f64) -> Vec<u8> {
    payload(
        Some(seq),
        TS + seq,
        vec![metric(None, Some(10), D::Double, V::DoubleValue(speed))],
    )
}

// ----- the pipeline --------------------------------------------------------

const PIPELINE: &str = r#"
version: 1

pipeline:
  name: sparkplug-docker
  node_id: sparkplug-01
  data_dir: "{data_dir}"

sources:
  - id: plant
    kind: mqtt
    config:
      url: "mqtt://127.0.0.1:{port}"
      client_id: thadam-sparkplug-e2e
      clean_start: false
      keep_alive: 5s
      topics: [{ filter: "spBv1.0/#", qos: 1 }]
      ack_mode: on_commit

wal:
  max_disk: 256MiB
  segment_bytes: 8MiB
  fsync: { interval: 50ms }
  on_full: block
  retain_for: 1d

quarantine: { format: parquet, max_disk: 268435456 }

datasets:
  - id: sp
    match: { source: plant, topic: "spBv1.0/#" }
    decode: sparkplug_b
    identity:
      group: { id: 100, template: "{topic.1}" }
      node: { id: 101, template: "{topic.3}" }
    time: { field: /timestamp, unit: ms, domain: utc }
    fields:
      - { id: 1000, name: kind, type: str, from: /kind }
      - { id: 1001, name: device, type: str, from: /device }
      - { id: 1002, name: seq, type: u64, from: /seq }
      - { id: 1003, name: bd_seq, type: u64, from: /bd_seq }
      - { id: 1004, name: temp, type: f32, from: /metrics/temp }
      - { id: 1005, name: speed, type: f64, from: "/metrics/motor.speed" }

sinks:
  - id: local
    kind: parquet
    config:
      datasets: [sp]
      compression: zstd
      row_group_bytes: 1048576
      rotation: { max_bytes: 67108864, max_age: 30 }
      max_open_files: 8
      max_disk: 1073741824
      timestamp_unit: nanos
      finalize_on_shutdown: true

observability:
  logs: { format: pretty, level: info }
"#;

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return found;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(parquet_files(&path));
        } else if path.extension().is_some_and(|e| e == "parquet") {
            found.push(path);
        }
    }
    found.sort();
    found
}

type Cols = Vec<(String, Field)>;

fn rows(root: &Path) -> Vec<Cols> {
    let mut out = Vec::new();
    for file in parquet_files(root) {
        let reader = SerializedFileReader::new(fs::File::open(&file).unwrap()).unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            let row: Row = row.unwrap();
            out.push(
                row.get_column_iter()
                    .map(|(name, field)| (name.clone(), field.clone()))
                    .collect(),
            );
        }
    }
    out
}

fn col<'a>(row: &'a Cols, name: &str) -> &'a Field {
    &row.iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("no column {name} in {row:?}"))
        .1
}

fn text(row: &Cols, name: &str) -> Option<String> {
    match col(row, name) {
        Field::Str(s) => Some(s.clone()),
        Field::Null => None,
        other => panic!("{name} is not text: {other:?}"),
    }
}

fn float(row: &Cols, name: &str) -> Option<f64> {
    match col(row, name) {
        Field::Float(v) => Some(f64::from(*v)),
        Field::Double(v) => Some(*v),
        Field::Null => None,
        other => panic!("{name} is not a float: {other:?}"),
    }
}

fn flags(row: &Cols) -> u32 {
    match col(row, "_quality_flags") {
        Field::UInt(v) => *v,
        other => panic!("_quality_flags is not u32: {other:?}"),
    }
}

/// `thadam run` for `CAPTURE`, then SIGTERM; asserts a clean exit.
fn run_thadam(cfg: &str, log: &Path) {
    let log_file = fs::File::create(log).unwrap();
    let mut thadam = Thadam(
        Command::new(env!("CARGO_BIN_EXE_thadam"))
            .args(["run", cfg])
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn thadam run"),
    );
    std::thread::sleep(CAPTURE);
    sigterm(&thadam.0);
    let status = wait_exit(&mut thadam.0, Duration::from_secs(60));
    assert!(
        status.success(),
        "thadam run exited {status}\nrun log tail:\n{}",
        tail(log, 40)
    );
}

#[test]
#[ignore = "needs Docker (Mosquitto 2)"]
fn sparkplug_pipeline_decodes_flags_and_resumes_from_the_birth_store() {
    let tmp = TempDir::new();
    let broker = Broker::start(&tmp.0);
    let data = tmp.0.join("data").join("thadam");
    let config = tmp.0.join("pipeline.yaml");
    fs::write(
        &config,
        PIPELINE
            .replace("{data_dir}", &data.display().to_string())
            .replace("{port}", &broker.port.to_string()),
    )
    .unwrap();
    let cfg = config.to_str().unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(["validate", cfg])
        .output()
        .expect("thadam validate");
    assert!(
        out.status.success(),
        "thadam validate\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // The broker holds everything for thadam's persistent session.
    create_session(broker.port);

    // ---- Run 1: one full edge-node lifecycle on e1, a live node e2 ------
    publish(
        broker.port,
        &[
            ("spBv1.0/g/NBIRTH/e1", nbirth(7)),
            ("spBv1.0/g/DBIRTH/e1/dev1", dbirth(1)),
            ("spBv1.0/g/NDATA/e1", ndata(2, 21.5)),
            ("spBv1.0/g/DDATA/e1/dev1", ddata(3, 3.5)),
            // seq 4 never arrives: one SEQUENCE_GAP.
            ("spBv1.0/g/NDATA/e1", ndata(5, 22.5)),
            (
                "spBv1.0/g/NDEATH/e1",
                payload(None, TS + 6, vec![bd_seq(7)]),
            ),
            // After the death: one STALE row.
            ("spBv1.0/g/NDATA/e1", ndata(9, 23.5)),
            ("spBv1.0/g/NBIRTH/e2", nbirth(3)),
            // Host STATE is JSON and not telemetry: dropped, never an error.
            ("spBv1.0/g/STATE/host-1", br#"{"online":true}"#.to_vec()),
        ],
    );
    run_thadam(cfg, &tmp.0.join("run1.log"));

    let ready = data.join("sinks").join("local").join("ready").join("sp");
    let first = rows(&ready);
    let log1 = tail(&tmp.0.join("run1.log"), 40);
    // 7 messages on e1, 1 on e2; STATE is dropped.
    assert_eq!(first.len(), 8, "run 1 rows\n{log1}");

    let kinds: Vec<_> = first.iter().filter_map(|r| text(r, "kind")).collect();
    for kind in ["NBIRTH", "DBIRTH", "NDATA", "DDATA", "NDEATH"] {
        assert!(
            kinds.iter().any(|k| k == kind),
            "no {kind} row in {kinds:?}"
        );
    }
    assert_eq!(kinds.iter().filter(|k| *k == "NDATA").count(), 3);
    assert!(!kinds.iter().any(|k| k == "STATE"));

    // Names resolved from aliases (the DATA carried only aliases).
    let temps: Vec<f64> = first
        .iter()
        .filter(|r| text(r, "kind").as_deref() == Some("NDATA"))
        .filter_map(|r| float(r, "temp"))
        .collect();
    let mut sorted = temps.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(sorted, [21.5, 22.5, 23.5], "{temps:?}");
    let ddata_row = first
        .iter()
        .find(|r| text(r, "kind").as_deref() == Some("DDATA"))
        .unwrap();
    assert_eq!(float(ddata_row, "speed"), Some(3.5));
    assert_eq!(text(ddata_row, "device").as_deref(), Some("dev1"));

    // Exactly one SEQUENCE_GAP and exactly one STALE.
    let gap_bit = QualityFlags::SEQUENCE_GAP.bits();
    let stale_bit = QualityFlags::STALE.bits();
    let gaps: Vec<_> = first.iter().filter(|r| flags(r) & gap_bit != 0).collect();
    let stale: Vec<_> = first.iter().filter(|r| flags(r) & stale_bit != 0).collect();
    assert_eq!((gaps.len(), stale.len()), (1, 1), "{first:#?}");
    assert_eq!(float(gaps[0], "temp"), Some(22.5));
    assert_eq!(
        text(gaps[0], "_quality_reason").as_deref(),
        Some("sparkplug.seq expected 4 got 5")
    );
    assert_eq!(float(stale[0], "temp"), Some(23.5));
    assert_eq!(
        text(stale[0], "_quality_status").as_deref(),
        Some("uncertain")
    );
    assert_eq!(
        text(stale[0], "_quality_reason").as_deref(),
        Some("sparkplug.past-ndeath")
    );
    assert_eq!(
        first.iter().filter(|r| flags(r) != 0).count(),
        2,
        "every other row is clean"
    );

    // The births are durable under the data directory, one store per sink.
    let births = data.join("sparkplug").join("births").join("local");
    assert!(
        fs::read_dir(&births).unwrap().count() >= 3,
        "e1, e1/dev1 and e2 births in {births:?}"
    );

    // ---- Run 2: restart; DATA only, no BIRTH in the new traffic ----------
    publish(
        broker.port,
        &[
            // e2 is alive: seq continues, must resolve and carry no flags.
            ("spBv1.0/g/NDATA/e2", ndata(1, 31.5)),
            // e1 is still dead: resolves, and stays stale.
            ("spBv1.0/g/NDATA/e1", ndata(10, 24.5)),
        ],
    );
    run_thadam(cfg, &tmp.0.join("run2.log"));

    let all = rows(&ready);
    let log2 = tail(&tmp.0.join("run2.log"), 40);
    assert_eq!(all.len(), 10, "run 2 added two rows\n{log2}");
    let live = all
        .iter()
        .find(|r| float(r, "temp") == Some(31.5))
        .unwrap_or_else(|| panic!("the e2 row resolved `temp` from the store\n{log2}"));
    assert_eq!(flags(live), 0, "no false gap after the restart: {live:?}");
    assert_eq!(text(live, "_quality_status").as_deref(), Some("good"));
    let dead = all
        .iter()
        .find(|r| float(r, "temp") == Some(24.5))
        .unwrap_or_else(|| panic!("the e1 row resolved `temp` from the store\n{log2}"));
    assert_eq!(flags(dead), stale_bit, "death persisted across the restart");
}
