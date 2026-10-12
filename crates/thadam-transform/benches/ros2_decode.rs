//! Criterion benchmark: a `decode: ros2` dataset end to end through the chain.
//!
//! One 6-joint `sensor_msgs/JointState` and one
//! `geometry_msgs/PoseStamped` per iteration batch, each decoded from CDR,
//! routed through field/time extraction, and checked by its robotics step
//! (`joint_state`, `pose`). All field pointers are the documented ROS
//! spelling (a single flattened dotted token), so this exercises the direct
//! record read path where one exists. Criterion reports time per batch of
//! 512 messages; divide by 512 for ns per message.
//!
//! The `ros2_components` group isolates the fixed per-message costs the
//! chain work sits on top of: the content-addressed schema-store read and
//! the CDR decode itself.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use thadam_core::{SCHEMA_HASH_ATTR, SchemaStore};
use thadam_transform::{Chain, ChainLimits, ChainSpec};
use serde_json::{Value, json};
use std::hint::black_box;

const BATCH: usize = 512;

const TIME_MSG: &str = "int32 sec\nuint32 nanosec\n";
const HEADER_MSG: &str = "builtin_interfaces/Time stamp\nstring frame_id\n";
const JOINT_STATE_NAME: &str = "sensor_msgs/msg/JointState";
const POSE_STAMPED_NAME: &str = "geometry_msgs/msg/PoseStamped";

const JOINTS: [&str; 6] = [
    "shoulder_pan",
    "shoulder_lift",
    "elbow",
    "wrist_1",
    "wrist_2",
    "wrist_3",
];

/// The `ros2msg` concatenated form an MCAP record carries.
fn join_docs(parts: &[(&str, &str)]) -> String {
    let sep = "=".repeat(80);
    let mut out = String::new();
    for (name, text) in parts {
        out.push_str(&format!("{sep}\nMSG: {name}\n{text}"));
    }
    out
}

fn joint_state_schema() -> String {
    let main = "\
std_msgs/Header header
string[] name
float64[] position
float64[] velocity
float64[] effort
";
    let rest = join_docs(&[
        ("std_msgs/Header", HEADER_MSG),
        ("builtin_interfaces/Time", TIME_MSG),
    ]);
    format!("{main}{rest}")
}

fn pose_stamped_schema() -> String {
    let main = "std_msgs/Header header\ngeometry_msgs/Pose pose\n";
    let rest = join_docs(&[
        (
            "geometry_msgs/Pose",
            "geometry_msgs/Point position\ngeometry_msgs/Quaternion orientation\n",
        ),
        ("geometry_msgs/Point", "float64 x\nfloat64 y\nfloat64 z\n"),
        (
            "geometry_msgs/Quaternion",
            "float64 x 0\nfloat64 y 0\nfloat64 z 0\nfloat64 w 1\n",
        ),
        ("std_msgs/Header", HEADER_MSG),
        ("builtin_interfaces/Time", TIME_MSG),
    ]);
    format!("{main}{rest}")
}

/// A little-endian XCDR1 encoder, same as the fixture encoders in the
/// `thadam-mcap` and `thadam-ros2-msg` tests.
struct Enc {
    buf: Vec<u8>,
}

impl Enc {
    fn new() -> Self {
        Enc {
            buf: vec![0x00, 0x01, 0x00, 0x00],
        }
    }

    fn pad(&mut self, align: usize) {
        let rel = self.buf.len() - 4;
        let pad = (align - rel % align) % align;
        self.buf.resize(self.buf.len() + pad, 0);
    }

    fn u32(&mut self, v: u32) -> &mut Self {
        self.pad(4);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    fn f64(&mut self, v: f64) -> &mut Self {
        self.pad(8);
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
        self
    }

    fn string(&mut self, s: &str) -> &mut Self {
        self.u32(s.len() as u32 + 1);
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
        self
    }

    fn header(&mut self, sec: i32, nanosec: u32, frame_id: &str) -> &mut Self {
        self.u32(sec as u32);
        self.u32(nanosec);
        self.string(frame_id)
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

fn joint_state_payload(seq: u64) -> Vec<u8> {
    let stamp = 1_758_000_000 + (seq % 1_000_000) as i32;
    let mut enc = Enc::new();
    enc.header(stamp, 123_456_789, "arm");
    enc.u32(JOINTS.len() as u32);
    for name in JOINTS {
        enc.string(name);
    }
    let values = |base: f64, enc: &mut Enc| {
        enc.u32(JOINTS.len() as u32);
        for (i, _) in JOINTS.iter().enumerate() {
            enc.f64(base + i as f64 * 0.1);
        }
    };
    values(0.1, &mut enc);
    values(1.0, &mut enc);
    values(-2.0, &mut enc);
    enc.finish()
}

fn pose_stamped_payload(seq: u64) -> Vec<u8> {
    let stamp = 1_758_000_000 + (seq % 1_000_000) as i32;
    let mut enc = Enc::new();
    enc.header(stamp, 987_654_321, "map");
    enc.f64(1.0).f64(2.0).f64(3.0);
    enc.f64(0.0).f64(0.0).f64(0.0).f64(1.0);
    enc.finish()
}

fn joints_dataset() -> Value {
    json!({
        "id": "joints",
        "match": { "topic": "/joint_states" },
        "decode": "ros2",
        "identity": [{ "robot": "arm-1" }],
        "time": { "field": "/header.stamp", "unit": "ns", "domain": "utc" },
        "fields": [
            { "name": "header.stamp", "type": "ts", "unit": "ns" },
            { "name": "header.frame_id", "type": "str" },
            { "name": "name", "type": "list<str>" },
            { "name": "position", "type": "list<f64>" },
            { "name": "velocity", "type": "list<f64>" },
            { "name": "effort", "type": "list<f64>" }
        ]
    })
}

fn poses_dataset() -> Value {
    json!({
        "id": "poses",
        "match": { "topic": "/pose" },
        "decode": "ros2",
        "identity": [{ "robot": "arm-1" }],
        "time": { "field": "/header.stamp", "unit": "ns", "domain": "utc" },
        "fields": [
            { "name": "header.stamp", "type": "ts", "unit": "ns" },
            { "name": "header.frame_id", "type": "str" },
            { "name": "pose.position.x", "type": "f64" },
            { "name": "pose.position.y", "type": "f64" },
            { "name": "pose.position.z", "type": "f64" },
            { "name": "pose.orientation.x", "type": "f64" },
            { "name": "pose.orientation.y", "type": "f64" },
            { "name": "pose.orientation.z", "type": "f64" },
            { "name": "pose.orientation.w", "type": "f64" }
        ]
    })
}

fn frames(topic: &str, payloads: Vec<Vec<u8>>, hash: &str) -> Frames {
    let mut f = Frames::new();
    for (i, payload) in payloads.into_iter().enumerate() {
        let mut m = message(
            topic,
            Bytes::from(payload),
            1_791_369_600_000_000 + i as i64 * 1_000,
        );
        m.attrs.insert(SCHEMA_HASH_ATTR, hash);
        f.push(m);
    }
    f
}

fn bench_ros2_decode(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let store = SchemaStore::open(dir.path().join("schemas"));
    let joint_schema = joint_state_schema();
    let pose_schema = pose_stamped_schema();
    store
        .put(JOINT_STATE_NAME, "ros2msg", &joint_schema)
        .unwrap();
    store
        .put(POSE_STAMPED_NAME, "ros2msg", &pose_schema)
        .unwrap();
    let joint_hash = SchemaStore::hash(JOINT_STATE_NAME, "ros2msg", &joint_schema);
    let pose_hash = SchemaStore::hash(POSE_STAMPED_NAME, "ros2msg", &pose_schema);
    let schema_dir = store.dir().to_path_buf();

    let compile = |dataset: Value, transforms: Value| {
        let spec =
            ChainSpec::from_value(json!({ "datasets": [dataset], "transforms": transforms }))
                .unwrap()
                .with_schema_dir(&schema_dir);
        Chain::compile(&spec, ChainLimits::default()).unwrap()
    };
    let joints_frames = frames(
        "/joint_states",
        (0..BATCH).map(|i| joint_state_payload(i as u64)).collect(),
        &joint_hash,
    );
    let pose_frames = frames(
        "/pose",
        (0..BATCH).map(|i| pose_stamped_payload(i as u64)).collect(),
        &pose_hash,
    );

    let mut g = c.benchmark_group("ros2_decode");
    g.sample_size(40)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .throughput(Throughput::Elements(BATCH as u64));

    let mut chain = compile(joints_dataset(), json!([{ "kind": "joint_state" }]));
    g.bench_function("joint_state_6_joints", |b| {
        b.iter(|| black_box(run(&mut chain, &joints_frames)));
    });

    let mut chain = compile(poses_dataset(), json!([{ "kind": "pose" }]));
    g.bench_function("pose_stamped", |b| {
        b.iter(|| black_box(run(&mut chain, &pose_frames)));
    });

    let decode_only = |mut ds: Value| {
        let o = ds.as_object_mut().unwrap();
        o.remove("time");
        o.insert("fields".into(), json!([]));
        ds
    };
    let mut chain = compile(decode_only(joints_dataset()), json!([]));
    g.bench_function("joint_state_decode_only", |b| {
        b.iter(|| black_box(run(&mut chain, &joints_frames)));
    });
    g.finish();

    // Decomposition of the fixed per-message costs a `decode: ros2` dataset
    // sits on top of: the content-addressed schema-store read and the CDR
    // decode itself.
    let mut g = c.benchmark_group("ros2_components");
    g.sample_size(40)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    g.bench_function("schema_store_get", |b| {
        b.iter(|| black_box(store.get(black_box(&joint_hash)).unwrap()));
    });
    let mut registry = thadam_ros2_msg::Registry::new();
    let main = registry
        .register_ros2msg(JOINT_STATE_NAME, &joint_schema)
        .unwrap();
    let compiled = registry.compile(&main.to_string()).unwrap();
    let payload = joint_state_payload(7);
    g.bench_function("cdr_decode_joint_state", |b| {
        b.iter(|| black_box(compiled.decode(black_box(&payload)).unwrap()));
    });
    g.finish();
}

criterion_group!(benches, bench_ros2_decode);
criterion_main!(benches);
