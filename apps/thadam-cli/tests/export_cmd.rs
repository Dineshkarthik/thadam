//! Tests for `thadam export`: the MCAP it writes, read back with the `mcap` crate.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use bytes::Bytes;
use mcap::records::MessageHeader;
use thadam_core::{DiskBudget, RawBatch, RawMessage, SourceId, SystemClock, Timestamp};
use thadam_mcap::attr;
use thadam_testkit::TempDataDir;
use thadam_wal::{OnFull, Wal, WalOptions, WalWriter};

const T: i64 = 1_758_000_000_000_000_000;
const SCHEMA_NAME: &str = "sensor/Joint";
const SCHEMA_TEXT: &[u8] = b"float64 position\n";

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("exited normally")
}

fn pipeline_yaml(data: &Path, bag: &Path) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: "{data}" }}
sources:
  - {{ id: bag, kind: mcap, config: {{ paths: ["{bag}"] }} }}
  - {{ id: plc, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: plc }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
sinks:
  - id: pq
    kind: parquet
    config: {{ datasets: [d], compression: none, rotation: {{ max_bytes: null, max_rows: 2, max_age: null }} }}
"#,
        data = data.display(),
        bag = bag.display()
    )
}

/// A recording with one `sensor/Joint` schema, the text an export must carry.
fn write_bag(path: &Path) {
    let file = File::create(path).unwrap();
    let mut writer = mcap::Writer::new(BufWriter::new(file)).unwrap();
    let schema = writer
        .add_schema(SCHEMA_NAME, "ros2msg", SCHEMA_TEXT)
        .unwrap();
    let channel = writer
        .add_channel(schema, "/joint", "cdr", &BTreeMap::new())
        .unwrap();
    let header = MessageHeader {
        channel_id: channel,
        sequence: 0,
        log_time: 1,
        publish_time: 1,
    };
    writer
        .write_to_known_channel(&header, &[0u8, 1, 0, 0][..])
        .unwrap();
    writer.finish().unwrap();
}

struct Fixture {
    base: TempDataDir,
    config: PathBuf,
    data: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let base = TempDataDir::new().unwrap();
        let data = base.join("data");
        let bag = base.join("bag.mcap");
        let config = base.join("pipeline.yaml");
        write_bag(&bag);
        fs::write(&config, pipeline_yaml(&data, &bag)).unwrap();
        Fixture { base, config, data }
    }

    fn config(&self) -> &str {
        self.config.to_str().unwrap()
    }

    fn out(&self, name: &str) -> PathBuf {
        self.base.join(name)
    }
}

fn open_wal(data: &Path) -> Wal {
    let mut opts = WalOptions::new(OnFull::Block);
    opts.node_id = Some("node-1".into());
    thadam_wal::open(
        &data.join("wal"),
        opts,
        Arc::new(DiskBudget::new()),
        Arc::new(SystemClock::new()),
    )
    .unwrap()
    .0
}

fn message(ingest: i64, attrs: &[(&str, &str)], payload: &'static [u8]) -> RawMessage {
    RawMessage {
        ingest_time: Timestamp::from_nanos(ingest),
        attrs: attrs.iter().copied().collect(),
        payload: Bytes::from_static(payload),
    }
}

async fn commit(writer: &WalWriter, source: &str, messages: Vec<RawMessage>) {
    let batch = RawBatch::new(SourceId::new(source), messages, true);
    writer.submit(batch).await.unwrap().await.unwrap();
}

const JOINT: &[(&str, &str)] = &[
    (attr::MCAP_TOPIC, "/joint"),
    (attr::MCAP_MESSAGE_ENCODING, "cdr"),
    (attr::MCAP_SCHEMA_NAME, SCHEMA_NAME),
    (attr::MCAP_SCHEMA_ENCODING, "ros2msg"),
];

/// Three frames: two from `plc` (JSON, no schema), one `/joint` frame from `bag`.
/// Returns the WAL epoch.
async fn populate(data: &Path) -> u64 {
    let wal = open_wal(data);
    let epoch = wal.epoch().get();
    let writer = wal.start_writer().unwrap();
    commit(
        &writer,
        "plc",
        vec![
            message(T + 10, &[(attr::TOPIC, "plc/state")], b"{\"n\":1}"),
            message(T + 11, &[(attr::TOPIC, "plc/state")], b"{\"n\":2}"),
        ],
    )
    .await;
    commit(
        &writer,
        "bag",
        vec![message(T + 20, JOINT, b"\x00\x01\x00\x00")],
    )
    .await;
    commit(
        &writer,
        "plc",
        vec![message(T + 30, &[(attr::TOPIC, "plc/state")], b"{\"n\":3}")],
    )
    .await;
    wal.close().await.unwrap();
    epoch
}

#[derive(Debug, PartialEq, Eq)]
struct Received {
    topic: String,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

fn messages(bytes: &[u8]) -> Vec<Received> {
    mcap::MessageStream::new(bytes)
        .unwrap()
        .map(|m| {
            let m = m.unwrap();
            Received {
                topic: m.channel.topic.clone(),
                log_time: m.log_time,
                publish_time: m.publish_time,
                data: m.data.to_vec(),
            }
        })
        .collect()
}

fn provenance(bytes: &[u8]) -> BTreeMap<String, String> {
    let summary = mcap::read::Summary::read(bytes).unwrap().unwrap();
    let index = summary
        .metadata_indexes
        .iter()
        .find(|i| i.name == "thadam.provenance")
        .expect("provenance record");
    mcap::read::metadata(bytes, index).unwrap().metadata
}

#[tokio::test]
async fn export_writes_channels_messages_schemas_and_provenance() {
    let f = Fixture::new();
    let epoch = populate(&f.data).await;
    let out_path = f.out("run.mcap");

    let out = thadam(&[
        "export",
        f.config(),
        "--mcap",
        out_path.to_str().unwrap(),
        "--from",
        "1",
        "--to",
        "3",
    ]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("positions: 1..=3"), "{stdout}");
    assert!(
        stdout.contains("messages: 4, channels: 2, gaps: 0, unresolved schemas: 0"),
        "{stdout}"
    );
    assert!(
        !f.out("run.mcap.tmp").exists(),
        "temporary file left behind"
    );

    let bytes = fs::read(&out_path).unwrap();
    let summary = mcap::read::Summary::read(&bytes).unwrap().unwrap();
    let mut channels: Vec<(String, String, Option<String>, String)> = summary
        .channels
        .values()
        .map(|c| {
            (
                c.topic.clone(),
                c.message_encoding.clone(),
                c.schema.as_ref().map(|s| s.name.clone()),
                c.metadata["thadam.source"].clone(),
            )
        })
        .collect();
    channels.sort();
    assert_eq!(
        channels,
        vec![
            (
                "/joint".into(),
                "cdr".into(),
                Some(SCHEMA_NAME.into()),
                "bag".into()
            ),
            ("plc/state".into(), "json".into(), None, "plc".into()),
        ]
    );

    let joint = summary
        .channels
        .values()
        .find(|c| c.topic == "/joint")
        .unwrap();
    let schema = joint.schema.as_ref().unwrap();
    assert_eq!(schema.encoding, "ros2msg");
    assert_eq!(&schema.data[..], SCHEMA_TEXT);

    let got = messages(&bytes);
    let want = |topic: &str, at: i64, data: &[u8]| Received {
        topic: topic.into(),
        log_time: at as u64,
        publish_time: at as u64,
        data: data.to_vec(),
    };
    assert_eq!(
        got,
        vec![
            want("plc/state", T + 10, b"{\"n\":1}"),
            want("plc/state", T + 11, b"{\"n\":2}"),
            want("/joint", T + 20, b"\x00\x01\x00\x00"),
            want("plc/state", T + 30, b"{\"n\":3}"),
        ]
    );

    let prov = provenance(&bytes);
    assert_eq!(prov["thadam_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(prov["wal_epoch"], epoch.to_string());
    assert_eq!(prov["first_pos"], "1");
    assert_eq!(prov["last_pos"], "3");
    assert_eq!(prov["node_id"], "node-1");
}

#[tokio::test]
async fn source_filter_keeps_only_the_selected_source() {
    let f = Fixture::new();
    populate(&f.data).await;
    let out_path = f.out("bag-only.mcap");

    let out = thadam(&[
        "export",
        f.config(),
        "--mcap",
        out_path.to_str().unwrap(),
        "--from",
        "1",
        "--source",
        "bag",
    ]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    assert!(text(&out.stdout).contains("positions: 2..=2"));

    let bytes = fs::read(&out_path).unwrap();
    let got = messages(&bytes);
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].topic, "/joint");
    let summary = mcap::read::Summary::read(&bytes).unwrap().unwrap();
    assert_eq!(summary.channels.len(), 1);
}

#[tokio::test]
async fn an_existing_output_is_kept_unless_force_is_given() {
    let f = Fixture::new();
    populate(&f.data).await;
    let out_path = f.out("keep.mcap");
    fs::write(&out_path, b"precious").unwrap();
    let args = |extra: &[&'static str]| {
        let mut v = vec!["export", f.config(), "--mcap", out_path.to_str().unwrap()];
        v.extend_from_slice(&["--from", "1"]);
        v.extend_from_slice(extra);
        v.iter().map(|s| (*s).to_owned()).collect::<Vec<String>>()
    };

    let refused = Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args(&[]))
        .output()
        .unwrap();
    assert_eq!(code(&refused), 2, "{}", text(&refused.stderr));
    assert!(text(&refused.stderr).contains("--force"));
    assert_eq!(fs::read(&out_path).unwrap(), b"precious");

    let forced = Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args(&["--force"]))
        .output()
        .unwrap();
    assert_eq!(code(&forced), 0, "{}", text(&forced.stderr));
    assert!(mcap::read::Summary::read(&fs::read(&out_path).unwrap()).is_ok());
}

#[tokio::test]
async fn export_is_refused_while_the_wal_is_open() {
    let f = Fixture::new();
    populate(&f.data).await;
    let held = open_wal(&f.data);
    let out_path = f.out("busy.mcap");

    let out = thadam(&[
        "export",
        f.config(),
        "--mcap",
        out_path.to_str().unwrap(),
        "--from",
        "1",
    ]);
    assert_eq!(code(&out), 3, "{}", text(&out.stderr));
    assert!(!out_path.exists());
    assert!(!f.out("busy.mcap.tmp").exists());
    drop(held);
}

#[tokio::test]
async fn a_topic_with_two_encodings_fails_and_leaves_no_file() {
    let f = Fixture::new();
    let wal = open_wal(&f.data);
    let writer = wal.start_writer().unwrap();
    commit(
        &writer,
        "bag",
        vec![message(
            T + 1,
            &[
                (attr::MCAP_TOPIC, "/x"),
                (attr::MCAP_MESSAGE_ENCODING, "cdr"),
            ],
            b"a",
        )],
    )
    .await;
    commit(
        &writer,
        "bag",
        vec![message(T + 2, &[(attr::MCAP_TOPIC, "/x")], b"b")],
    )
    .await;
    wal.close().await.unwrap();
    drop(writer);
    drop(wal);
    let out_path = f.out("conflict.mcap");

    let out = thadam(&[
        "export",
        f.config(),
        "--mcap",
        out_path.to_str().unwrap(),
        "--from",
        "1",
    ]);
    assert_eq!(code(&out), 1, "{}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("/x"), "{stderr}");
    assert!(stderr.contains("disagree"), "{stderr}");
    assert!(stderr.contains("hint:"), "{stderr}");
    assert!(stderr.contains("--from/--to"), "{stderr}");
    assert!(stderr.contains("--source"), "{stderr}");
    assert!(!out_path.exists());
    assert!(!f.out("conflict.mcap.tmp").exists());
}

#[tokio::test]
async fn malformed_points_and_unknown_sources_are_usage_errors() {
    let f = Fixture::new();
    populate(&f.data).await;
    let out_path = f.out("never.mcap");
    let path = out_path.to_str().unwrap();

    let bad_from = thadam(&["export", f.config(), "--mcap", path, "--from", "soon"]);
    assert_eq!(code(&bad_from), 2, "{}", text(&bad_from.stderr));
    assert!(text(&bad_from.stderr).contains("--from"));

    let unknown = thadam(&[
        "export",
        f.config(),
        "--mcap",
        path,
        "--from",
        "1",
        "--source",
        "nope",
    ]);
    assert_eq!(code(&unknown), 2, "{}", text(&unknown.stderr));
    assert!(text(&unknown.stderr).contains("`nope`"));

    let bad_encoding = thadam(&[
        "export",
        f.config(),
        "--mcap",
        path,
        "--from",
        "1",
        "--encoding",
        "bag=xml",
    ]);
    assert_eq!(code(&bad_encoding), 2, "{}", text(&bad_encoding.stderr));
    assert!(!out_path.exists());
}
