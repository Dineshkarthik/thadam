//! Smoke test for `examples/ros2-mcap-to-parquet`: generates the sample
//! recording into a temp directory, runs the example pipeline to completion
//! there, checks the Parquet output, and round-trips it through
//! `thadam export --mcap`. No Docker or network.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../../../crates/thadam-mcap/examples/make_ros2_sample.rs"]
mod make_ros2_sample;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use make_ros2_sample::{JOINT_MESSAGES, JOINT_TOPIC, POSE_MESSAGES, POSE_TOPIC};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};

const PIPELINE: &str = include_str!("../../../examples/ros2-mcap-to-parquet/pipeline.yaml");

/// `_quality_flags` bits asserted below (see `thadam-arrow` quality flags).
const OUT_OF_RANGE: u32 = 1 << 0;
const INVALID: u32 = 1 << 8;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("thadam-ros2-example-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn assert_ok(out: &Output, what: &str) {
    assert_eq!(
        out.status.code(),
        Some(0),
        "{what}\nstdout: {}\nstderr: {}",
        text(&out.stdout),
        text(&out.stderr)
    );
}

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

type Columns = Vec<(String, Field)>;

fn rows(root: &Path) -> Vec<Columns> {
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

fn value<'a>(row: &'a Columns, name: &str) -> &'a Field {
    &row.iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .1
}

/// Non-`good` rows as (`_quality_flags`, `_quality_reason`). Good rows must
/// carry no flags and no reason.
fn flagged(rows: &[Columns]) -> Vec<(u32, String)> {
    let mut out = Vec::new();
    for row in rows {
        let Field::Str(status) = value(row, "_quality_status") else {
            panic!("_quality_status is not a string: {row:?}");
        };
        if status == "good" {
            assert_eq!(value(row, "_quality_flags"), &Field::UInt(0), "{row:?}");
            assert_eq!(value(row, "_quality_reason"), &Field::Null, "{row:?}");
            continue;
        }
        let Field::UInt(flags) = value(row, "_quality_flags") else {
            panic!("_quality_flags is not a u32: {row:?}");
        };
        let Field::Str(reason) = value(row, "_quality_reason") else {
            panic!("bad row without a reason: {row:?}");
        };
        out.push((*flags, reason.clone()));
    }
    out
}

#[derive(Debug, PartialEq)]
struct Replayed {
    schema_name: String,
    schema_encoding: String,
    schema: Vec<u8>,
    payloads: Vec<Vec<u8>>,
}

fn channels(bytes: &[u8]) -> BTreeMap<String, Replayed> {
    let mut out: BTreeMap<String, Replayed> = BTreeMap::new();
    for message in mcap::MessageStream::new(bytes).unwrap() {
        let message = message.unwrap();
        let channel = &message.channel;
        let entry = out.entry(channel.topic.clone()).or_insert_with(|| {
            let schema = channel.schema.as_ref().expect("a schema");
            Replayed {
                schema_name: schema.name.clone(),
                schema_encoding: schema.encoding.clone(),
                schema: schema.data.to_vec(),
                payloads: Vec::new(),
            }
        });
        entry.payloads.push(message.data.to_vec());
    }
    out
}

#[test]
fn ros2_example_runs_end_to_end_and_round_trips() {
    let tmp = TempDir::new();
    let base = &tmp.0;

    let sample = base.join("sample.mcap");
    make_ros2_sample::write_sample(&sample).unwrap();

    let data = base.join("data").join("thadam");
    let config = base.join("pipeline.yaml");
    let yaml = PIPELINE
        .replace("./data/sample.mcap", &sample.display().to_string())
        .replace("./data/thadam", &data.display().to_string());
    assert!(!yaml.contains("./data/"), "unreplaced path:\n{yaml}");
    fs::write(&config, yaml).unwrap();
    let cfg = config.to_str().unwrap();

    assert_ok(&thadam(&["validate", cfg]), "thadam validate");
    assert_ok(
        &thadam(&["run", cfg, "--exit-when-sources-finish"]),
        "thadam run",
    );

    let sink = data.join("sinks").join("local").join("ready");
    let joints = rows(&sink.join("joint_states"));
    let poses = rows(&sink.join("poses"));
    assert_eq!(joints.len(), JOINT_MESSAGES, "joint_states rows");
    assert_eq!(poses.len(), POSE_MESSAGES, "poses rows");

    // The same data directory again: the source resumes after the committed
    // messages, so nothing is ingested twice.
    assert_ok(
        &thadam(&["run", cfg, "--exit-when-sources-finish"]),
        "thadam run, second time",
    );
    assert_eq!(rows(&sink.join("joint_states")).len(), JOINT_MESSAGES);
    assert_eq!(rows(&sink.join("poses")).len(), POSE_MESSAGES);

    for col in [
        "header.stamp",
        "header.frame_id",
        "name",
        "position",
        "velocity",
        "effort",
    ] {
        value(&joints[0], col);
    }
    for col in [
        "header.stamp",
        "header.frame_id",
        "pose.position.x",
        "pose.position.y",
        "pose.position.z",
        "pose.orientation.x",
        "pose.orientation.y",
        "pose.orientation.z",
        "pose.orientation.w",
    ] {
        value(&poses[0], col);
    }

    assert_eq!(
        flagged(&joints),
        vec![(INVALID, "joint_state_length_mismatch".to_string())],
        "joint_states quality"
    );
    assert_eq!(
        flagged(&poses),
        vec![(OUT_OF_RANGE, "quaternion_not_unit".to_string())],
        "poses quality"
    );

    let roundtrip = base.join("roundtrip.mcap");
    assert_ok(
        &thadam(&[
            "export",
            cfg,
            "--mcap",
            roundtrip.to_str().unwrap(),
            "--from",
            "0",
        ]),
        "thadam export",
    );
    let original = channels(&fs::read(&sample).unwrap());
    let replayed = channels(&fs::read(&roundtrip).unwrap());
    assert_eq!(
        replayed.keys().map(String::as_str).collect::<Vec<_>>(),
        vec![JOINT_TOPIC, POSE_TOPIC],
        "exported channels"
    );
    let joint = &replayed[JOINT_TOPIC];
    assert_eq!(joint.schema_name, "sensor_msgs/msg/JointState");
    assert_eq!(joint.schema_encoding, "ros2msg");
    assert_eq!(
        joint.schema,
        make_ros2_sample::joint_state_schema().as_bytes()
    );
    assert_eq!(joint.payloads.len(), JOINT_MESSAGES);
    assert_eq!(replayed[POSE_TOPIC].payloads.len(), POSE_MESSAGES);
    assert_eq!(replayed, original, "round trip reproduces the recording");
}
