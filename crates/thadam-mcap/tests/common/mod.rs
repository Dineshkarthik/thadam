//! MCAP fixture writer and a little-endian XCDR1 encoder, built the same
//! way as the `thadam-ros2-msg` test fixtures.
//!
//! Shared between test binaries, some of which use only part of it.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::io::BufWriter;
use std::path::Path;

use mcap::records::MessageHeader;
use mcap::{Compression, WriteOptions, Writer};

pub const JOINT_STATE_SCHEMA_NAME: &str = "sensor_msgs/msg/JointState";
pub const JOINT_STATE_TOPIC: &str = "/joint_states";
pub const JSON_TOPIC: &str = "/annotations";
pub const JSON_SCHEMA_NAME: &str = "example/Reading";

pub const TIME_MSG: &str = "int32 sec\nuint32 nanosec\n";
pub const HEADER_MSG: &str = "builtin_interfaces/Time stamp\nstring frame_id\n";
pub const JOINT_STATE_MSG: &str = "\
std_msgs/Header header
string[] name
float64[] position
float64[] velocity
float64[] effort
";
pub const JSON_SCHEMA: &str = r#"{"type":"object","properties":{"note":{"type":"string"}}}"#;

/// The `ros2msg` concatenated multi-definition document a rosbag2 MCAP
/// carries for JointState, like `register_ros2msg` consumes.
pub fn joint_state_schema() -> String {
    let sep = "=".repeat(80);
    format!(
        "{JOINT_STATE_MSG}{sep}\nMSG: std_msgs/Header\n{HEADER_MSG}{sep}\nMSG: builtin_interfaces/Time\n{TIME_MSG}"
    )
}

/// A little-endian XCDR1 encoder; alignment counts from after the 4-byte
/// encapsulation header, matching the decoder.
pub struct Enc {
    pub buf: Vec<u8>,
}

impl Enc {
    pub fn new() -> Self {
        Enc {
            buf: vec![0x00, 0x01, 0x00, 0x00],
        }
    }

    fn pad(&mut self, align: usize) {
        let rel = self.buf.len() - 4;
        let pad = (align - rel % align) % align;
        self.buf.resize(self.buf.len() + pad, 0);
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.pad(4);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.u32(v as u32)
    }

    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.pad(8);
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
        self
    }

    /// A CDR `string`: u32 length including the NUL, bytes, NUL.
    pub fn string(&mut self, s: &str) -> &mut Self {
        self.u32(s.len() as u32 + 1);
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
        self
    }

    /// The `std_msgs/Header` prefix: stamp + frame_id.
    pub fn header(&mut self, sec: i32, nanosec: u32, frame_id: &str) -> &mut Self {
        self.i32(sec);
        self.u32(nanosec);
        self.string(frame_id)
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// `sensor_msgs/JointState` payload: one joint "hip" at 0.1 rad with frame
/// "arm".
pub fn joint_state_payload() -> Vec<u8> {
    let mut enc = Enc::new();
    enc.header(1_758_000_000, 123_456_789, "arm");
    enc.u32(1).string("hip");
    enc.u32(1).f64(0.1);
    enc.u32(1).f64(1.5);
    enc.u32(1).f64(9.8);
    enc.finish()
}

/// One message to write, on one of the two fixture channels.
pub struct Msg {
    pub topic: &'static str,
    pub sequence: u32,
    pub log_time: u64,
    pub publish_time: u64,
    pub payload: Vec<u8>,
}

impl Msg {
    /// A `cdr` JointState message.
    pub fn joint(log_time: u64, sequence: u32) -> Self {
        Msg {
            topic: JOINT_STATE_TOPIC,
            sequence,
            log_time,
            publish_time: log_time + 7,
            payload: joint_state_payload(),
        }
    }

    /// A `json` annotations message `"..."`.
    pub fn json(log_time: u64, sequence: u32) -> Self {
        Msg {
            topic: JSON_TOPIC,
            sequence,
            log_time,
            publish_time: log_time + 7,
            payload: format!(r#"{{"note":"n{sequence}"}}"#).into_bytes(),
        }
    }

    /// An arbitrary-topic, schemaless `cdr`-encoded message.
    pub fn other(topic: &'static str, log_time: u64, sequence: u32) -> Self {
        Msg {
            topic,
            sequence,
            log_time,
            publish_time: log_time + 7,
            payload: joint_state_payload(),
        }
    }
}

/// Writes a fixture file. Chunks are on with a small target chunk size so
/// multi-chunk files are exercised; `compressed` toggles zstd on/off.
pub fn write_mcap(path: &Path, messages: &[Msg], compressed: bool) {
    write_mcap_with(path, messages, compressed, &joint_state_schema());
}

/// Like [`write_mcap`] with caller-provided JointState schema text, for
/// pre-flight failure fixtures.
pub fn write_mcap_with(path: &Path, messages: &[Msg], compressed: bool, schema_text: &str) {
    let file = std::fs::File::create(path).expect("create fixture");
    let compression = if compressed {
        Some(Compression::Zstd)
    } else {
        None
    };
    let options = WriteOptions::default()
        .compression(compression)
        .chunk_size(Some(1024));
    let mut writer = Writer::with_options(BufWriter::new(file), options).expect("writer");

    let joint_schema = writer
        .add_schema(JOINT_STATE_SCHEMA_NAME, "ros2msg", schema_text.as_bytes())
        .expect("schema");
    let json_schema = writer
        .add_schema(JSON_SCHEMA_NAME, "jsonschema", JSON_SCHEMA.as_bytes())
        .expect("schema");
    let mut channels: BTreeMap<&str, u16> = BTreeMap::new();
    for msg in messages {
        let channel_id = match channels.get(msg.topic) {
            Some(id) => *id,
            None => {
                let id = match msg.topic {
                    JOINT_STATE_TOPIC => writer
                        .add_channel(joint_schema, msg.topic, "cdr", &BTreeMap::new())
                        .expect("channel"),
                    JSON_TOPIC => writer
                        .add_channel(json_schema, msg.topic, "json", &BTreeMap::new())
                        .expect("channel"),
                    other => writer
                        .add_channel(0, other, "cdr", &BTreeMap::new())
                        .expect("channel"),
                };
                channels.insert(msg.topic, id);
                id
            }
        };
        writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id,
                    sequence: msg.sequence,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                },
                &msg.payload,
            )
            .expect("message");
    }
    writer.finish().expect("finish");
}
