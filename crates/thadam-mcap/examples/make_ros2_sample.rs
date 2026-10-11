//! Writes the deterministic ROS 2 sample recording used by
//! `examples/ros2-mcap-to-parquet`: `/joint_states` (`sensor_msgs/JointState`,
//! 200 messages at 100 Hz, six joints) and `/robot_pose`
//! (`geometry_msgs/PoseStamped`, 50 messages at 10 Hz, frame `map`), both
//! CDR-encoded with `ros2msg` schemas, merged in log-time order.
//!
//! Two messages are deliberately malformed so the quality flags have
//! something to show: a JointState whose `position` has five values for six
//! names, and a pose whose quaternion has norm 1.2.
//!
//! ```text
//! cargo run -p thadam-mcap --example make_ros2_sample -- examples/ros2-mcap-to-parquet/data/sample.mcap
//! ```
//!
//! The thadam-cli smoke test includes this file as a module, so the sample it
//! checks is the one this program writes.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use mcap::Writer;
use mcap::records::MessageHeader;

/// First log time, Unix nanoseconds (2025-09-16T05:20:00Z).
pub const START_NS: u64 = 1_758_000_000_000_000_000;
/// The JointState topic.
pub const JOINT_TOPIC: &str = "/joint_states";
/// The PoseStamped topic.
pub const POSE_TOPIC: &str = "/robot_pose";
/// JointState messages in the sample.
pub const JOINT_MESSAGES: usize = 200;
/// Gap between JointState messages (100 Hz).
pub const JOINT_PERIOD_NS: u64 = 10_000_000;
/// PoseStamped messages in the sample.
pub const POSE_MESSAGES: usize = 50;
/// Gap between PoseStamped messages (10 Hz).
pub const POSE_PERIOD_NS: u64 = 100_000_000;
/// The JointState whose `position` has one value fewer than `name`.
pub const BAD_JOINT_INDEX: usize = 120;
/// The PoseStamped whose quaternion is scaled to norm 1.2.
pub const BAD_POSE_INDEX: usize = 30;
/// Scale applied to the bad pose's quaternion.
pub const BAD_QUATERNION_SCALE: f64 = 1.2;
/// Joint names, in message order.
pub const JOINT_NAMES: [&str; 6] = [
    "shoulder_pan",
    "shoulder_lift",
    "elbow",
    "wrist_1",
    "wrist_2",
    "wrist_3",
];
/// The JointState schema name, as rosbag2 records it.
pub const JOINT_SCHEMA_NAME: &str = "sensor_msgs/msg/JointState";
/// The PoseStamped schema name, as rosbag2 records it.
pub const POSE_SCHEMA_NAME: &str = "geometry_msgs/msg/PoseStamped";

const SEP: &str =
    "================================================================================";

fn ros2msg(parts: &[&str]) -> String {
    parts.join(&format!("\n{SEP}\n"))
}

/// The `ros2msg` schema text of `sensor_msgs/JointState`.
pub fn joint_state_schema() -> String {
    ros2msg(&[
        "std_msgs/Header header\nstring[] name\nfloat64[] position\nfloat64[] velocity\nfloat64[] effort",
        "MSG: std_msgs/Header\nbuiltin_interfaces/Time stamp\nstring frame_id",
        "MSG: builtin_interfaces/Time\nint32 sec\nuint32 nanosec",
    ])
}

/// The `ros2msg` schema text of `geometry_msgs/PoseStamped`.
pub fn pose_stamped_schema() -> String {
    ros2msg(&[
        "std_msgs/Header header\ngeometry_msgs/Pose pose",
        "MSG: std_msgs/Header\nbuiltin_interfaces/Time stamp\nstring frame_id",
        "MSG: builtin_interfaces/Time\nint32 sec\nuint32 nanosec",
        "MSG: geometry_msgs/Pose\ngeometry_msgs/Point position\ngeometry_msgs/Quaternion orientation",
        "MSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z",
        "MSG: geometry_msgs/Quaternion\nfloat64 x\nfloat64 y\nfloat64 z\nfloat64 w",
    ])
}

/// A little-endian XCDR1 encoder. Alignment counts from after the 4-byte
/// encapsulation header, as the decoder expects.
struct Cdr {
    buf: Vec<u8>,
}

impl Cdr {
    fn new() -> Self {
        Cdr {
            buf: vec![0x00, 0x01, 0x00, 0x00],
        }
    }

    fn pad(&mut self, align: usize) {
        let rel = self.buf.len() - 4;
        let pad = (align - rel % align) % align;
        self.buf.resize(self.buf.len() + pad, 0);
    }

    fn u32(&mut self, v: u32) {
        self.pad(4);
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn i32(&mut self, v: i32) {
        self.u32(v as u32);
    }

    fn f64(&mut self, v: f64) {
        self.pad(8);
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
    }

    fn string(&mut self, s: &str) {
        self.u32(s.len() as u32 + 1);
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
    }

    fn header(&mut self, stamp: u64, frame_id: &str) {
        let sec = (stamp / 1_000_000_000) as i32;
        let nanosec = (stamp % 1_000_000_000) as u32;
        self.i32(sec);
        self.u32(nanosec);
        self.string(frame_id);
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

fn joint_state_payload(index: usize, stamp: u64) -> Vec<u8> {
    let phase = index as f64 * 0.05;
    let joints = JOINT_NAMES.len();
    let short = index == BAD_JOINT_INDEX;
    let mut enc = Cdr::new();
    enc.header(stamp, "base_link");
    enc.u32(joints as u32);
    for name in JOINT_NAMES {
        enc.string(name);
    }
    let position_len = if short { joints - 1 } else { joints };
    enc.u32(position_len as u32);
    for j in 0..position_len {
        enc.f64(0.5 * (phase + j as f64).sin());
    }
    enc.u32(joints as u32);
    for j in 0..joints {
        enc.f64(0.5 * 0.05 * (phase + j as f64).cos());
    }
    enc.u32(joints as u32);
    for j in 0..joints {
        enc.f64(1.0 + 0.1 * j as f64);
    }
    enc.finish()
}

fn pose_payload(index: usize, stamp: u64) -> Vec<u8> {
    let yaw = 0.02 * index as f64;
    let scale = if index == BAD_POSE_INDEX {
        BAD_QUATERNION_SCALE
    } else {
        1.0
    };
    let mut enc = Cdr::new();
    enc.header(stamp, "map");
    enc.f64(2.0 + 0.1 * index as f64);
    enc.f64(-1.0 + 0.05 * index as f64);
    enc.f64(0.0);
    enc.f64(0.0);
    enc.f64(0.0);
    enc.f64(scale * (yaw / 2.0).sin());
    enc.f64(scale * (yaw / 2.0).cos());
    enc.finish()
}

struct Sample {
    log_time: u64,
    topic: usize,
    sequence: u32,
    payload: Vec<u8>,
}

fn samples() -> Vec<Sample> {
    let mut out = Vec::with_capacity(JOINT_MESSAGES + POSE_MESSAGES);
    for i in 0..JOINT_MESSAGES {
        let stamp = START_NS + i as u64 * JOINT_PERIOD_NS;
        out.push(Sample {
            log_time: stamp,
            topic: 0,
            sequence: i as u32,
            payload: joint_state_payload(i, stamp),
        });
    }
    for j in 0..POSE_MESSAGES {
        let stamp = START_NS + j as u64 * POSE_PERIOD_NS;
        out.push(Sample {
            log_time: stamp,
            topic: 1,
            sequence: j as u32,
            payload: pose_payload(j, stamp),
        });
    }
    out.sort_by_key(|s| (s.log_time, s.topic));
    out
}

/// Writes the sample recording to `out`, replacing any file already there.
pub fn write_sample(out: &Path) -> Result<(), Box<dyn Error>> {
    let file = File::create(out)?;
    let mut writer = Writer::new(BufWriter::new(file))?;
    let joint_schema = writer.add_schema(
        JOINT_SCHEMA_NAME,
        "ros2msg",
        joint_state_schema().as_bytes(),
    )?;
    let pose_schema = writer.add_schema(
        POSE_SCHEMA_NAME,
        "ros2msg",
        pose_stamped_schema().as_bytes(),
    )?;
    let channels = [
        writer.add_channel(joint_schema, JOINT_TOPIC, "cdr", &BTreeMap::new())?,
        writer.add_channel(pose_schema, POSE_TOPIC, "cdr", &BTreeMap::new())?,
    ];
    for s in samples() {
        writer.write_to_known_channel(
            &MessageHeader {
                channel_id: channels[s.topic],
                sequence: s.sequence,
                log_time: s.log_time,
                publish_time: s.log_time,
            },
            &s.payload,
        )?;
    }
    writer.finish()?;
    Ok(())
}

fn main() -> ExitCode {
    let Some(out) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: make_ros2_sample <out.mcap>");
        return ExitCode::from(2);
    };
    if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty())
        && let Err(e) = fs::create_dir_all(parent)
    {
        eprintln!("cannot create {}: {e}", parent.display());
        return ExitCode::FAILURE;
    }
    match write_sample(&out) {
        Ok(()) => {
            println!(
                "wrote {}: {JOINT_MESSAGES} {JOINT_TOPIC}, {POSE_MESSAGES} {POSE_TOPIC}",
                out.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("cannot write {}: {e}", out.display());
            ExitCode::FAILURE
        }
    }
}
