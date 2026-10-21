//! Docker end-to-end test for the live ROS 2 source (`kind: ros2_live`).
//!
//! Builds a ROS 2 Jazzy image from `crates/thadam-ros2-live/tests/assets/docker`
//! (public Docker Hub `ros:jazzy-ros-base`, pinned by tag, plus
//! `ros-jazzy-rmw-zenoh-cpp`), runs `rmw_zenohd` with the Zenoh port
//! published on the host, runs the sensor-mix publisher on the same bridge
//! network, then captures ~10 s with the real pipeline: a `ros2_live`
//! source feeding `decode: ros2` datasets into local Parquet. Asserts rows
//! per topic, zero decode errors (the quarantine stays empty), and that
//! `/tf_static` arrives exactly once via the transient_local late-join
//! query (the publisher fires it once, before `thadam` subscribes).
//!
//! `#[ignore]`d so `cargo nextest run` passes without Docker; run it with
//!
//! ```text
//! cargo nextest run -p thadam --features ros2-live --run-ignored only -E 'binary(ros2_live_docker)'
//! ```
//!
//! The `docker` CLI must be available (public image pulls only). Containers
//! are labelled `thadam-ros2-live-e2e` and removed when the test ends,
//! including on panic. Re-pin the image tag with `THADAM_TEST_ROS2_IMAGE`.

#![cfg(feature = "ros2-live")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};

/// The pinned ROS 2 base/tag (public Docker Hub; the Dockerfile pins by tag).
const ROS_IMAGE: &str = "thadam-ros2-live-test:jazzy";
const LABEL: &str = "thadam-ros2-live-e2e";
const TIMEOUT: Duration = Duration::from_secs(240);
/// Publisher writes `/tf_static` once ~1 s after node start; thadam
/// subscribes well after, so the run exercises the late-join query.
const PRE_RUN_SETTLE: Duration = Duration::from_secs(5);
const CAPTURE: Duration = Duration::from_secs(10);

fn assets() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/thadam-ros2-live/tests/assets")
        .canonicalize()
        .expect("crate test assets")
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker cli")
}

fn docker_ok(args: &[&str]) -> Output {
    let out = docker(args);
    assert!(
        out.status.success(),
        "docker {}: {}\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

fn image() -> String {
    std::env::var("THADAM_TEST_ROS2_IMAGE").unwrap_or_else(|_| ROS_IMAGE.to_owned())
}

/// docker network + both containers; removed on drop (also on panic).
struct Stack {
    network: String,
    router: String,
    publisher: String,
    port: u16,
}

impl Stack {
    fn up() -> Stack {
        let pid = std::process::id();
        let network = format!("thadam-r2l-net-{pid}");
        let router = format!("thadam-r2l-router-{pid}");
        let publisher = format!("thadam-r2l-pub-{pid}");
        let port = free_port();
        let image = image();

        docker_ok(&[
            "build",
            "-t",
            &image,
            &assets().join("docker").display().to_string(),
        ]);
        docker_ok(&["network", "create", &network]);

        let router_args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            router.clone(),
            "--network".into(),
            network.clone(),
            "--network-alias".into(),
            "router".into(),
            "--label".into(),
            LABEL.into(),
            "-p".into(),
            format!("127.0.0.1:{port}:7447"),
            image.clone(),
            "bash".into(),
            "-c".into(),
            "source /opt/ros/jazzy/setup.bash && exec ros2 run rmw_zenoh_cpp rmw_zenohd".into(),
        ];
        docker_ok(&router_args.iter().map(String::as_str).collect::<Vec<_>>());

        let stack = Stack {
            network,
            router,
            publisher,
            port,
        };
        stack.wait_router();
        stack
    }

    fn wait_router(&self) {
        let started = Instant::now();
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            if started.elapsed() > TIMEOUT {
                panic!(
                    "rmw_zenohd did not listen on {}\n{}",
                    self.port,
                    self.logs(&self.router)
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    fn start_publisher(&self) {
        let image = image();
        let args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            self.publisher.clone(),
            "--network".into(),
            self.network.clone(),
            "--label".into(),
            LABEL.into(),
            "-e".into(),
            "RMW_IMPLEMENTATION=rmw_zenoh_cpp".into(),
            "-e".into(),
            "ROS_DOMAIN_ID=0".into(),
            "-e".into(),
            r#"ZENOH_CONFIG_OVERRIDE=connect/endpoints=["tcp/router:7447"];scouting/multicast/enabled=false"#
                .into(),
            image,
            "bash".into(),
            "-c".into(),
            "source /opt/ros/jazzy/setup.bash && exec python3 /usr/local/lib/thadam-ros2-live/sensor_pub.py"
                .into(),
        ];
        docker_ok(&args.iter().map(String::as_str).collect::<Vec<_>>());
    }

    fn logs(&self, container: &str) -> String {
        let out = docker(&["logs", "--tail", "40", container]);
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }

    fn publisher_logs(&self) -> String {
        self.logs(&self.publisher)
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.publisher]);
        let _ = docker(&["rm", "-f", "-v", &self.router]);
        let _ = docker(&["network", "rm", &self.network]);
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "thadam-ros2-live-e2e-{}-{nanos}",
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
    buf.lines()
        .rev()
        .take(lines)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

const PIPELINE: &str = r#"
version: 1

pipeline:
  name: ros2-live-docker
  node_id: ros2-live-01
  data_dir: "{data_dir}"

sources:
  - id: ros
    kind: ros2_live
    config:
      endpoint: "tcp/127.0.0.1:{port}"
      domain: 0
      msg_dirs: ["{msgs}"]
      topics:
        - { name: /imu, type: sensor_msgs/msg/Imu }
        - { name: /joint_states, type: sensor_msgs/msg/JointState }
        - { name: /tf, type: tf2_msgs/msg/TFMessage }
        - { name: /tf_static, type: tf2_msgs/msg/TFMessage, transient_local: true }
      reconnect: { min_backoff: 1s, max_backoff: 5s }

wal:
  max_disk: 256MiB
  segment_bytes: 8MiB
  fsync: { interval: 50ms }
  on_full: block
  retain_for: 1d

quarantine: { format: parquet, max_disk: 268435456 }

datasets:
  - id: imu
    match: { source: ros, topic: /imu }
    decode: ros2
    time: { field: /header.stamp, unit: ns, domain: utc }
    fields:
      - { id: 1000, name: header.stamp, type: ts, unit: ns, required: true }
      - { id: 1001, name: header.frame_id, type: str, required: true }
      - { id: 1002, name: angular_velocity.z, type: f64 }
      - { id: 1003, name: linear_acceleration.z, type: f64 }

  - id: joint_states
    match: { source: ros, topic: /joint_states }
    decode: ros2
    time: { field: /header.stamp, unit: ns, domain: utc }
    fields:
      - { id: 1000, name: header.stamp, type: ts, unit: ns, required: true }
      - { id: 1001, name: header.frame_id, type: str, required: true }
      - { id: 1002, name: name, type: "list<str>" }
      - { id: 1003, name: position, type: "list<f64>" }
      - { id: 1004, name: velocity, type: "list<f64>" }
      - { id: 1005, name: effort, type: "list<f64>" }

  - id: tf
    match: { source: ros, topic: /tf }
    decode: ros2
    explode: { path: /transforms, index_field: tf_idx }
    time: { field: /header.stamp, unit: ns, domain: utc }
    fields:
      - { id: 1000, name: header.stamp, type: ts, unit: ns, required: true }
      - { id: 1001, name: header.frame_id, type: str, required: true }
      - { id: 1002, name: child_frame_id, type: str, required: true }

  - id: tf_static
    match: { source: ros, topic: /tf_static }
    decode: ros2
    explode: { path: /transforms, index_field: tf_idx }
    time: { field: /header.stamp, unit: ns, domain: utc }
    fields:
      - { id: 1000, name: header.stamp, type: ts, unit: ns, required: true }
      - { id: 1001, name: header.frame_id, type: str, required: true }
      - { id: 1002, name: child_frame_id, type: str, required: true }

sinks:
  - id: local
    kind: parquet
    config:
      datasets: [imu, joint_states, tf, tf_static]
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

fn rows(root: &Path) -> Vec<Vec<(String, Field)>> {
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

fn value<'a>(row: &'a [(String, Field)], name: &str) -> &'a Field {
    &row.iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("no column {name} in {row:?}"))
        .1
}

fn assert_all_good(dataset: &str, rows: &[Vec<(String, Field)>]) {
    for row in rows {
        assert_eq!(
            value(row, "_quality_status"),
            &Field::Str("good".to_string()),
            "{dataset} row flagged: {row:?}"
        );
    }
}

#[test]
#[ignore = "needs Docker; ~2 min including the image build"]
fn ros2_live_captures_decodes_and_late_joins() {
    let stack = Stack::up();
    stack.start_publisher();
    // Let the publisher fire the one-shot /tf_static BEFORE thadam
    // subscribes (late join), and let the graph settle.
    std::thread::sleep(PRE_RUN_SETTLE);

    let tmp = TempDir::new();
    let data = tmp.0.join("data").join("thadam");
    let config = tmp.0.join("pipeline.yaml");
    let yaml = PIPELINE
        .replace("{data_dir}", &data.display().to_string())
        .replace("{port}", &stack.port.to_string())
        .replace("{msgs}", &assets().join("msgs").display().to_string());
    fs::write(&config, &yaml).unwrap();
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

    let run_log = tmp.0.join("thadam-run.log");
    let log_file = fs::File::create(&run_log).unwrap();
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
        "thadam run exited {status}\nrun log tail:\n{}\npublisher tail:\n{}",
        tail(&run_log, 40),
        stack.publisher_logs()
    );

    // Rows per topic in the ready Parquet segments.
    let sinks = data.join("sinks").join("local").join("ready");
    let imu = rows(&sinks.join("imu"));
    let joints = rows(&sinks.join("joint_states"));
    let tf = rows(&sinks.join("tf"));
    let tf_static = rows(&sinks.join("tf_static"));
    assert!(
        imu.len() >= 5_000,
        "imu rows = {} (want >= 5000)\n{}",
        imu.len(),
        tail(&run_log, 40)
    );
    assert!(joints.len() >= 500, "joint_states rows = {}", joints.len());
    assert!(tf.len() >= 200, "tf rows = {}", tf.len());
    assert_eq!(
        tf_static.len(),
        1,
        "tf_static late join delivers exactly the one-shot message: {tf_static:?}"
    );
    assert_eq!(
        value(&tf_static[0], "header.frame_id"),
        &Field::Str("world".to_string())
    );
    assert_eq!(
        value(&tf_static[0], "child_frame_id"),
        &Field::Str("mount_link".to_string())
    );
    assert_eq!(
        value(&imu[0], "header.frame_id"),
        &Field::Str("imu_link".to_string())
    );

    // Zero decode errors and zero rejections anywhere: every row good,
    // quarantine empty.
    assert_all_good("imu", &imu);
    assert_all_good("joint_states", &joints);
    assert_all_good("tf", &tf);
    assert_all_good("tf_static", &tf_static);
    let quarantine = data.join("quarantine");
    let rejects = rows(&quarantine);
    assert_eq!(
        rejects.len(),
        0,
        "quarantine must be empty: {} reject rows",
        rejects.len()
    );
}
