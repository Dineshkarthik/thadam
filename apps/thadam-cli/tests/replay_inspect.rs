//! Tests for `thadam replay` and `thadam inspect`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use thadam_config::MapEnv;
use thadam_core::{
    BuildContext, DiskBudget, ErrorDomain, RawBatch, RawMessage, ThadamError, Shutdown, Source,
    SourceFactory, SourceId, SystemClock, Timestamp, codes,
};
use thadam_runtime::{BackoffPolicy, Pipeline, Registry, RuntimeOptions};
use thadam_testkit::{MemorySource, MemorySourceConfig, TempDataDir};
use thadam_wal::{OnFull, WalOptions, WalWriter};
use serde_json::{Value, json};

const ROWS: u64 = 6;

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

fn ok_json(out: &Output) -> Value {
    assert_eq!(
        code(out),
        0,
        "stdout: {}\nstderr: {}",
        text(&out.stdout),
        text(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("stdout is JSON")
}

fn payload(n: u64) -> Bytes {
    Bytes::from(format!("{{\"n\":{n},\"temp\":{n}.5}}"))
}

/// Hands out the one prepared source to the pipeline build.
struct OneSource(Arc<Mutex<Option<Box<dyn Source>>>>);

impl SourceFactory for OneSource {
    fn kind(&self) -> &'static str {
        "memory"
    }
    fn config_schema(&self) -> Value {
        json!({"type": "object"})
    }
    fn validate(&self, _cfg: &Value) -> Result<(), Vec<ThadamError>> {
        Ok(())
    }
    fn build(&self, _cfg: Value, _ctx: &BuildContext) -> Result<Box<dyn Source>, ThadamError> {
        self.0.lock().unwrap().take().ok_or_else(|| {
            ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                "the test source was already built",
            )
        })
    }
}

fn pipeline_yaml(data_dir: &Path) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
      - {{ id: 1001, name: temp, type: f32, from: /temp }}
sinks:
  - id: pq
    kind: parquet
    config: {{ datasets: [d], compression: none, rotation: {{ max_bytes: null, max_rows: 2, max_age: null }} }}
"#,
        dir = data_dir.display()
    )
}

fn fast_opts() -> RuntimeOptions {
    let mut o = RuntimeOptions::default();
    o.poll_commit_interval = Duration::from_millis(10);
    o.cursor_commit_interval = Duration::from_millis(10);
    o.maintenance_interval = Duration::from_millis(50);
    o.drain_timeout = Duration::from_secs(10);
    o.shutdown_timeout = Duration::from_secs(30);
    o.sink_retry = BackoffPolicy::new(Duration::from_millis(5), Duration::from_millis(50));
    o.source_restart = BackoffPolicy::new(Duration::from_millis(5), Duration::from_millis(50));
    o.batch_max_age = Duration::from_millis(20);
    o.clock_sync_probe = None;
    o.batch_limits.linger = Duration::from_millis(1);
    o
}

/// A data directory whose `pq` sink wrote `ROWS` rows to Parquet, and the
/// configuration file that describes it.
struct Seeded {
    base: TempDataDir,
    config: PathBuf,
    data: PathBuf,
}

impl Seeded {
    async fn new() -> Self {
        let base = TempDataDir::new().unwrap();
        let data = base.join("data");
        let config = base.join("pipeline.yaml");
        let text = pipeline_yaml(&data);
        fs::write(&config, &text).unwrap();

        let (source, _upstream) =
            MemorySource::scripted((1..=ROWS).map(payload), MemorySourceConfig::default());
        let slot: Arc<Mutex<Option<Box<dyn Source>>>> =
            Arc::new(Mutex::new(Some(Box::new(source))));
        let mut registry = Registry::new();
        registry
            .source(Box::new(OneSource(slot)))
            .segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()))
            .quarantine_sink(Box::new(thadam_segments::QuarantineSinkFactory::new()));

        let spec = match thadam_config::load_str(&text, &MapEnv::new()) {
            Ok(spec) => spec,
            Err(report) => panic!("invalid test config:\n{}", report.render(&text)),
        };
        let mut opts = fast_opts();
        opts.stop_when_sources_finish = true;
        let pipeline =
            Pipeline::build(spec, &registry, opts).unwrap_or_else(|e| panic!("build: {e:?}"));
        pipeline
            .run(Shutdown::new())
            .await
            .expect("the production run failed");
        Seeded { base, config, data }
    }

    fn data(&self) -> &str {
        self.data.to_str().unwrap()
    }

    fn config(&self) -> &str {
        self.config.to_str().unwrap()
    }
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

/// Every row of every Parquet file under `root`, rendered and sorted, so two
/// outputs compare as row sets.
fn rows_under(root: &Path) -> Vec<String> {
    let mut rows = Vec::new();
    for file in parquet_files(root) {
        let reader = SerializedFileReader::new(fs::File::open(&file).unwrap()).unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            rows.push(row.unwrap().to_string());
        }
    }
    rows.sort();
    rows
}

async fn commit(writer: &WalWriter, n: u64) {
    let batch = RawBatch::new(
        SourceId::new("src"),
        vec![RawMessage::new(
            Timestamp::from_nanos(1_000 + i64::try_from(n).unwrap()),
            payload(n),
        )],
        true,
    );
    writer.submit(batch).await.unwrap().await.unwrap();
}

fn sink_ready_dir(data: &Path) -> PathBuf {
    data.join("sinks").join("pq").join("ready")
}

#[tokio::test]
async fn inspect_wal_reports_the_layout_as_json() {
    let seeded = Seeded::new().await;
    let v = ok_json(&thadam(&["inspect", "wal", seeded.data(), "--json"]));
    assert_eq!(v["clean_shutdown"], json!(true), "{v}");
    assert_eq!(v["node_id"], json!("node-1"), "{v}");
    assert_eq!(v["first_seq"], json!(1), "{v}");
    assert_eq!(v["next_seq"], v["durable_end"].as_u64().unwrap() + 1, "{v}");
    assert!(v["durable_end"].as_u64().unwrap() >= 1, "{v}");
    assert!(v["retained_bytes"].as_u64().unwrap() > 0, "{v}");
    assert_eq!(v["torn_tail"], json!(false), "{v}");
    assert!(!v["segments"].as_array().unwrap().is_empty(), "{v}");
}

#[tokio::test]
async fn inspect_cursors_reports_the_sink_cursor_as_json() {
    let seeded = Seeded::new().await;
    let v = ok_json(&thadam(&["inspect", "cursors", seeded.data(), "--json"]));
    let cursors = v["cursors"].as_array().unwrap();
    let pq = cursors
        .iter()
        .find(|c| c["sink"] == json!("pq"))
        .unwrap_or_else(|| panic!("no pq cursor in {v}"));
    assert_eq!(pq["name"], json!("sink.pq"), "{v}");
    assert_eq!(pq["inflight"], Value::Null, "{v}");
    assert!(pq["committed"].as_u64().unwrap() >= 1, "{v}");
}

#[tokio::test]
async fn inspect_segments_counts_the_sink_files_as_json() {
    let seeded = Seeded::new().await;
    let v = ok_json(&thadam(&[
        "inspect",
        "segments",
        seeded.data(),
        "--sink",
        "pq",
        "--json",
    ]));
    assert_eq!(v["sink"], json!("pq"), "{v}");
    let files = v["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "one batch is one frame, so one file: {v}");
    // Whether the sink's cursor commit lands before shutdown decides between
    // `finalized` and `eligible`; either is a correct catalog state.
    let status = files[0]["status"].as_str().unwrap();
    assert!(
        status == "finalized" || status == "eligible",
        "{status}: {v}"
    );
    assert_eq!(files[0]["rows"], json!(ROWS), "{v}");
    assert!(files[0]["bytes"].as_u64().unwrap() > 0, "{v}");
    let totals = &v["totals"];
    assert_eq!(totals[status]["files"], json!(1), "{v}");
    assert_eq!(totals[status]["bytes"], files[0]["bytes"], "{v}");
    for state in ["finalized", "eligible", "uploaded", "deleted"] {
        if state != status {
            assert_eq!(totals[state]["files"], json!(0), "{v}");
        }
    }
}

#[tokio::test]
async fn inspect_sample_prints_the_newest_payloads_as_json() {
    let seeded = Seeded::new().await;
    let v = ok_json(&thadam(&[
        "inspect",
        "sample",
        seeded.data(),
        "-n",
        "2",
        "--json",
    ]));
    let frames = v["frames"].as_array().unwrap();
    assert!(!frames.is_empty() && frames.len() <= 2, "{v}");
    let last = frames.last().unwrap();
    let payload = last["records"][0]["payload"].as_str().unwrap();
    assert!(payload.starts_with("{\"n\":"), "{v}");
    assert_eq!(last["records"][0]["truncated"], json!(false), "{v}");
}

#[tokio::test]
async fn inspect_text_output_is_human_readable() {
    let seeded = Seeded::new().await;
    let out = thadam(&["inspect", "wal", seeded.data()]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("durable end:"), "{stdout}");
    assert!(stdout.contains("clean shutdown: yes"), "{stdout}");
}

#[tokio::test]
async fn inspect_works_while_a_writer_holds_the_wal() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let wal_dir = data.join("wal");
    let (wal, _) = thadam_wal::open(
        &wal_dir,
        WalOptions::new(OnFull::Block),
        Arc::new(DiskBudget::new()),
        Arc::new(SystemClock::new()),
    )
    .unwrap();
    let writer = wal.start_writer().unwrap();
    commit(&writer, 1).await;

    let v = ok_json(&thadam(&[
        "inspect",
        "wal",
        data.to_str().unwrap(),
        "--json",
    ]));
    assert_eq!(v["durable_end"], json!(1), "{v}");
    assert_eq!(v["clean_shutdown"], json!(false), "{v}");
    assert!(wal_dir.join("LOCK").exists());

    let sample = ok_json(&thadam(&[
        "inspect",
        "sample",
        data.to_str().unwrap(),
        "--json",
    ]));
    let record = &sample["frames"][0]["records"][0];
    assert_eq!(
        record["payload"],
        json!("{\"n\":1,\"temp\":1.5}"),
        "{sample}"
    );

    commit(&writer, 2).await;
    let after = ok_json(&thadam(&[
        "inspect",
        "wal",
        data.to_str().unwrap(),
        "--json",
    ]));
    assert_eq!(after["durable_end"], json!(2), "{after}");
}

#[tokio::test]
async fn replay_into_output_matches_the_original_rows() {
    let seeded = Seeded::new().await;
    let original = rows_under(&sink_ready_dir(&seeded.data));
    assert_eq!(original.len(), usize::try_from(ROWS).unwrap());

    let out_dir = seeded.base.join("replay-out");
    let out = thadam(&[
        "replay",
        seeded.config(),
        "--from",
        "1",
        "--sink",
        "pq",
        "--output",
        out_dir.to_str().unwrap(),
        "--replay-id",
        "r1",
    ]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("replay id: r1"), "{stdout}");
    assert!(
        stdout.contains(&format!("output: {}", out_dir.display())),
        "{stdout}"
    );

    assert_eq!(rows_under(&out_dir), original);
}

#[tokio::test]
async fn replay_from_a_duration_matches_the_original_rows() {
    let seeded = Seeded::new().await;
    let original = rows_under(&sink_ready_dir(&seeded.data));

    let out_dir = seeded.base.join("replay-ago");
    let out = thadam(&[
        "replay",
        seeded.config(),
        "--from",
        "-1h",
        "--sink",
        "pq",
        "--output",
        out_dir.to_str().unwrap(),
        "--replay-id",
        "ago",
    ]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    assert_eq!(rows_under(&out_dir), original);
}

#[tokio::test]
async fn replay_leaves_the_production_sink_alone() {
    let seeded = Seeded::new().await;
    let before = parquet_files(&sink_ready_dir(&seeded.data));
    let out_dir = seeded.base.join("replay-only");
    let out = thadam(&[
        "replay",
        seeded.config(),
        "--from",
        "1",
        "--sink",
        "pq",
        "--output",
        out_dir.to_str().unwrap(),
    ]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    assert_eq!(parquet_files(&sink_ready_dir(&seeded.data)), before);
}

#[tokio::test]
async fn replay_rejects_an_output_inside_the_sink_directory() {
    let seeded = Seeded::new().await;
    let inside = seeded.data.join("sinks").join("pq").join("replayed");
    let out = thadam(&[
        "replay",
        seeded.config(),
        "--from",
        "1",
        "--sink",
        "pq",
        "--output",
        inside.to_str().unwrap(),
    ]);
    assert_eq!(code(&out), 2, "{}", text(&out.stderr));
    assert!(!inside.exists());
}

#[tokio::test]
async fn replay_rejects_a_malformed_from() {
    let seeded = Seeded::new().await;
    let out = thadam(&["replay", seeded.config(), "--from", "soon", "--sink", "pq"]);
    assert_eq!(code(&out), 2, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("--from"),
        "{}",
        text(&out.stderr)
    );
}
