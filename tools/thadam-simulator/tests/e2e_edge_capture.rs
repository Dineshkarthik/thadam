//! Docker end-to-end test for the edge-capture demo (PR15).
//!
//! Boots Mosquitto in a container (with the demo's `mosquitto.conf`), runs
//! the simulator for ~10 s with raised chaos rates, runs `thadam run` on the
//! demo's level-1 config against it, drains, and then checks that the
//! Parquet row counts per dataset equal the oracle's acknowledged `deliver`
//! samples — and that the quarantine row count equals the oracle's
//! acknowledged `malformed` samples.
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker; run
//! them with
//!
//! ```text
//! cargo nextest run -p thadam-simulator --run-ignored only -E 'binary(e2e_edge_capture)'
//! ```
//!
//! Images: public Docker Hub only, pinned by tag (`eclipse-mosquitto:2`);
//! re-pin with `THADAM_TEST_MQTT_IMAGE`. The `docker` and `curl` CLIs must be
//! available. The `thadam` binary is expected as a sibling of the simulator
//! test binaries, or built on demand with `cargo build -p thadam-cli`.
//! Containers are labelled `thadam-simulator-e2e` and removed when the test
//! ends, including on panic.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use parquet::file::reader::{FileReader, SerializedFileReader};
use thadam_simulator::oracle::{Expected, OracleRow, read_all};
use thadam_simulator::types::Stream;

/// The pinned broker image (public Docker Hub, same line as the demo).
const MQTT_IMAGE: &str = "eclipse-mosquitto:2";
const LABEL: &str = "thadam-simulator-e2e";
const TIMEOUT: Duration = Duration::from_secs(240);

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

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn sim_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_thadam-simulator"))
}

/// The `thadam` binary: sibling of the simulator binary, or built on demand.
fn thadam_bin() -> PathBuf {
    if let Some(sibling) = sim_bin().parent().map(|p| p.join("thadam"))
        && sibling.exists()
    {
        return sibling;
    }
    let root = workspace_root();
    let status = Command::new("cargo")
        .args(["build", "-p", "thadam-cli"])
        .current_dir(&root)
        .status()
        .expect("cargo build -p thadam-cli");
    assert!(status.success(), "cargo build -p thadam-cli failed");
    root.join("target/debug/thadam")
}

/// A running Mosquitto container; removed on Drop.
struct Broker {
    name: String,
    port: u16,
}

impl Broker {
    fn start(config: &Path) -> Broker {
        let port = free_port();
        let name = format!("thadam-sim-e2e-{}-{}", std::process::id(), port);
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
            &format!("{}:/mosquitto/config/mosquitto.conf:ro", config.display()),
            &image,
        ]);
        assert!(
            out.status.success(),
            "docker run broker: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let broker = Broker { name, port };
        broker.wait_ready();
        broker
    }

    fn wait_ready(&self) {
        let started = Instant::now();
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
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

/// Child processes with log files; killed on drop.
struct Children {
    procs: Vec<Child>,
}

impl Children {
    fn new() -> Children {
        Children { procs: Vec::new() }
    }

    fn spawn(&mut self, mut cmd: Command, log: &Path) -> &mut Child {
        let out = std::fs::File::create(log).expect("log file");
        let err = out.try_clone().expect("log clone");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        self.procs.push(cmd.spawn().expect("spawn child"));
        let idx = self.procs.len() - 1;
        &mut self.procs[idx]
    }
}

impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.procs {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_http_ok(url: &str) {
    let started = Instant::now();
    loop {
        let ok = Command::new("curl")
            .args(["-sf", "--max-time", "3", url])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            return;
        }
        assert!(started.elapsed() < TIMEOUT, "{url} never went OK");
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// Waits for a child to exit, with a timeout; kills it on expiry.
fn wait_exit(child: &mut Child, within: Duration) -> std::process::ExitStatus {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        assert!(
            started.elapsed() < within,
            "child did not exit within {within:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn sigterm(child: &Child) {
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("kill -TERM");
    assert!(status.success(), "kill -TERM {pid}");
}

/// Every `.parquet` file under `dir`, recursively.
fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Total rows across a set of Parquet files (footer metadata only).
fn parquet_rows(files: &[PathBuf]) -> i64 {
    let mut rows = 0i64;
    for file in files {
        let opened = std::fs::File::open(file).unwrap_or_else(|e| panic!("open {file:?}: {e}"));
        let reader = SerializedFileReader::new(opened)
            .unwrap_or_else(|e| panic!("parquet read {file:?}: {e}"));
        rows += reader.metadata().file_metadata().num_rows();
    }
    rows
}

fn tail(path: &Path, lines: usize) -> String {
    let mut buf = String::new();
    if let Ok(mut f) = std::fs::File::open(path) {
        let _ = f.read_to_string(&mut buf);
    }
    buf.lines()
        .rev()
        .take(lines)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

/// Oracle expectation per stream: rows with publish_acked=true and the given
/// expected outcome.
fn oracle_counts(rows: &[OracleRow], expected: Expected) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for row in rows
        .iter()
        .filter(|r| r.publish_acked && r.expected == expected)
    {
        *out.entry(row.stream.clone()).or_default() += 1;
    }
    out
}

#[test]
#[ignore = "needs Docker (Mosquitto)"]
fn end_to_end_delivery_matches_the_oracle() {
    let root = workspace_root();
    let example = root.join("examples/edge-capture");
    let broker = Broker::start(&example.join("mosquitto/mosquitto.conf"));

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    let config = dir.path().join("pipeline.yaml");
    // The demo's level-1 config, repointed at this test's broker and a
    // throwaway data dir.
    let text = std::fs::read_to_string(example.join("thadam/pipeline.local.yaml")).expect("config");
    let text = text.replace(
        "url: mqtt://localhost:21883",
        &format!("url: mqtt://127.0.0.1:{}", broker.port),
    );
    let text = text.replace(
        "data_dir: ./data",
        &format!("data_dir: {}", data_dir.display()),
    );
    assert!(text.contains(&data_dir.display().to_string()));
    std::fs::write(&config, text).expect("write config");

    let admin_port = free_port();
    let thadam_log = dir.path().join("thadam.log");
    let sim_log = dir.path().join("simulator.log");
    let oracle_path = dir.path().join("oracle.ndjson");

    let mut children = Children::new();
    {
        let mut cmd = Command::new(thadam_bin());
        cmd.args([
            "run",
            &config.to_string_lossy(),
            "--log-format",
            "json",
            "--log-level",
            "info",
            "--admin-bind",
            &format!("127.0.0.1:{admin_port}"),
        ])
        .current_dir(dir.path());
        children.spawn(cmd, &thadam_log);
    }
    wait_http_ok(&format!("http://127.0.0.1:{admin_port}/readyz"));

    {
        let mut cmd = Command::new(sim_bin());
        cmd.args([
            "run",
            "--broker",
            &format!("mqtt://127.0.0.1:{}", broker.port),
            "--fleet",
            &example.join("simulator/fleet.yaml").to_string_lossy(),
            "--oracle",
            &oracle_path.to_string_lossy(),
            "--duration",
            "10s",
            "--drain-timeout",
            "8s",
            "--malformed-rate",
            "0.01",
            "--duplicate-rate",
            "0.02",
            "--out-of-range-rate",
            "0.005",
            "--client-id",
            "sim-e2e",
            "--log-level",
            "warn",
        ]);
        let sim = children.spawn(cmd, &sim_log);
        let status = wait_exit(sim, Duration::from_secs(60));
        assert!(
            status.success(),
            "simulator exited with {status}:\n{}",
            tail(&sim_log, 40)
        );
    }

    // Everything the broker acked is queued for Thadam (persistent session,
    // QoS 1); give the chains a moment to drain the WAL before shutdown.
    std::thread::sleep(Duration::from_secs(5));
    wait_http_ok(&format!("http://127.0.0.1:{admin_port}/readyz"));

    {
        let thadam = &mut children.procs[0];
        sigterm(thadam);
        let status = wait_exit(thadam, Duration::from_secs(60));
        assert!(
            status.success(),
            "thadam exited with {status}:\n{}",
            tail(&thadam_log, 60)
        );
    }

    let rows = read_all(&oracle_path).expect("oracle");
    assert!(!rows.is_empty(), "oracle is empty");
    let unacked = rows.iter().filter(|r| !r.publish_acked).count();
    assert_eq!(
        unacked, 0,
        "drain timeout must cover every PUBACK on a healthy broker"
    );

    let deliver = oracle_counts(&rows, Expected::Deliver);
    // Quarantine rows are one per rejected MESSAGE; the oracle counts one
    // row per exploded SAMPLE, so compare distinct malformed messages.
    let malformed_messages = rows
        .iter()
        .filter(|r| r.publish_acked && r.expected == Expected::Malformed)
        .map(|r| (r.site.clone(), r.robot.clone(), r.stream.clone(), r.seq))
        .collect::<std::collections::BTreeSet<_>>()
        .len() as u64;
    let duplicates: u64 = oracle_counts(&rows, Expected::DuplicateOf).values().sum();
    let debug_filtered: u64 = oracle_counts(&rows, Expected::FilteredDebug).values().sum();
    assert!(
        malformed_messages > 0 && duplicates > 0 && debug_filtered > 0,
        "chaos coverage: malformed={malformed_messages} duplicates={duplicates} filtered={debug_filtered}"
    );

    let mut failures = Vec::new();
    for stream in Stream::ALL {
        let name = stream.as_str();
        let files = parquet_files(&data_dir.join("sinks/local/ready").join(name));
        let actual = parquet_rows(&files) as u64;
        let expected_rows = deliver.get(name).copied().unwrap_or(0);
        let note = format!(
            "dataset {name}: parquet rows {actual} != oracle deliver {expected_rows} ({} files)",
            files.len()
        );
        if files.is_empty() {
            failures.push(format!("dataset {name}: no parquet files at all"));
        } else if actual != expected_rows {
            failures.push(note);
        }
    }

    let quarantine_files = parquet_files(&data_dir.join("quarantine/local/ready/_quarantine"));
    let quarantined = parquet_rows(&quarantine_files) as u64;
    if quarantined != malformed_messages {
        failures.push(format!(
            "quarantine rows {quarantined} != oracle malformed messages {malformed_messages} ({} files)",
            quarantine_files.len()
        ));
    }

    assert!(
        failures.is_empty(),
        "mismatches:\n  {}\n\noracle: {}\nthadam log tail:\n{}\nsim log tail:\n{}",
        failures.join("\n  "),
        dir.path().join("oracle.ndjson").display(),
        tail(&thadam_log, 60),
        tail(&sim_log, 20)
    );
    println!(
        "e2e ok: {} rows delivered across 6 datasets, {malformed_messages} quarantined, \
         {duplicates} duplicates dropped, {debug_filtered} debug events filtered",
        deliver.values().sum::<u64>()
    );
}
