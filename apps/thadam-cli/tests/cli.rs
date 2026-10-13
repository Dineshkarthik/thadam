//! End-to-end tests of the `thadam` binary: output and exit codes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use thadam_config::MapEnv;
use thadam_core::{BuildContext, ErrorDomain, ThadamError, Shutdown, Source, SourceFactory, codes};
use thadam_runtime::{Pipeline, Registry, RuntimeOptions};
use thadam_testkit::{MemorySource, MemorySourceConfig, TempDataDir};
use serde_json::{Value, json};

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn example() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/edge-capture/thadam/pipeline.yaml")
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/thadam-config/tests/fixtures")
        .join(name)
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("exited normally")
}

#[test]
fn version_command_and_flag() {
    let out = thadam(&["version"]);
    assert_eq!(code(&out), 0);
    let stdout = text(&out.stdout);
    assert!(stdout.starts_with("thadam "), "{stdout}");
    assert!(stdout.contains("config version: 1"));

    let flag = thadam(&["--version"]);
    assert_eq!(code(&flag), 0);
    assert!(text(&flag.stdout).starts_with("thadam "));
}

#[test]
fn validate_prints_a_summary_hash_and_memory_estimate() {
    let out = thadam(&[
        "validate",
        "--allow-unresolved",
        example().to_str().unwrap(),
    ]);
    let stdout = text(&out.stdout);
    assert_eq!(code(&out), 0, "{stdout}\n{}", text(&out.stderr));
    assert!(stdout.contains("configuration is valid"), "{stdout}");
    assert!(
        stdout.contains(
            "pipeline `edge-capture` (node `edge-01`): 1 source, 6 datasets, 3 transforms, 2 sinks"
        ),
        "{stdout}"
    );
    assert!(stdout.contains("config hash: "), "{stdout}");
    assert!(
        stdout.contains("estimated Thadam-owned memory: "),
        "{stdout}"
    );
    assert!(
        stdout.contains("runtime.memory_target: 256.0 MiB"),
        "{stdout}"
    );
    assert!(stdout.contains("dedup windows"), "{stdout}");
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("warning[config.missing]"),
        "unresolved secrets are warnings: {stderr}"
    );
}

#[test]
fn validate_fails_with_exit_2_when_secrets_are_unresolved() {
    let out = thadam(&["validate", example().to_str().unwrap()]);
    assert_eq!(code(&out), 2);
    let stderr = text(&out.stderr);
    assert!(stderr.contains("error[config.missing]"), "{stderr}");
    assert!(stderr.contains("sinks[1].config.access_key_id"), "{stderr}");
    let source = std::fs::read_to_string(example()).expect("read example");
    let line = source
        .lines()
        .position(|l| l.trim_start().starts_with("access_key_id:"))
        .map(|i| i + 1)
        .expect("example declares access_key_id");
    assert!(
        stderr.contains(&format!("pipeline.yaml:{line}:")),
        "expected a diagnostic at line {line}: {stderr}"
    );
    assert!(
        text(&out.stdout).is_empty(),
        "nothing on stdout for an invalid config"
    );
}

#[test]
fn validate_reports_every_error_with_path_line_and_hint() {
    let path = fixture("invalid/multi_error.yaml");
    let out = thadam(&["validate", path.to_str().unwrap()]);
    assert_eq!(code(&out), 2);
    let stderr = text(&out.stderr);
    assert!(stderr.contains("multi_error.yaml:9:1 (wall)"), "{stderr}");
    assert!(stderr.contains("hint: did you mean `wal`?"), "{stderr}");
    assert!(stderr.contains("(wal.retain_for)"), "{stderr}");
    assert!(
        stderr.contains("(datasets[0].fields[0].colour)"),
        "{stderr}"
    );
    assert!(stderr.contains("errors, 0 warnings"), "{stderr}");
}

#[test]
fn validate_accepts_a_minimal_config_and_exits_0() {
    let path = fixture("valid/minimal.yaml");
    let out = thadam(&["validate", path.to_str().unwrap()]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).is_empty(),
        "no diagnostics: {}",
        text(&out.stderr)
    );
}

#[test]
fn validate_rejects_unregistered_connector_kinds() {
    let path = fixture("invalid/unknown_connector_kind.yaml");
    let out = thadam(&["validate", path.to_str().unwrap()]);
    assert_eq!(code(&out), 2);
    assert!(text(&out.stderr).contains("config.unknown_kind"));
}

#[test]
fn a_missing_config_file_is_exit_2() {
    let out = thadam(&["validate", "/no/such/thadam.yaml"]);
    assert_eq!(code(&out), 2);
    assert!(text(&out.stderr).contains("cannot read"));
}

#[test]
fn usage_errors_are_exit_2() {
    assert_eq!(code(&thadam(&[])), 2);
    assert_eq!(code(&thadam(&["nonsense"])), 2);
    assert_eq!(code(&thadam(&["validate"])), 2);
    assert_eq!(code(&thadam(&["validate", "--bogus", "x"])), 2);
}

#[test]
fn help_is_exit_0() {
    let out = thadam(&["--help"]);
    assert_eq!(code(&out), 0);
    let stdout = text(&out.stdout);
    for cmd in ["validate", "version", "schema"] {
        assert!(stdout.contains(cmd), "{stdout}");
    }
}

#[test]
fn schema_prints_json() {
    let out = thadam(&["schema"]);
    assert_eq!(code(&out), 0);
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid JSON");
    assert_eq!(value["title"], "Thadam pipeline configuration");
    assert!(
        value["$defs"]["WalSpec"]["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k == "on_full")
    );
}

#[test]
fn the_same_config_prints_the_same_hash() {
    let run = || {
        let out = thadam(&[
            "validate",
            "--allow-unresolved",
            example().to_str().unwrap(),
        ]);
        text(&out.stdout)
            .lines()
            .find(|l| l.contains("config hash"))
            .map(str::to_owned)
            .expect("hash line")
    };
    assert_eq!(run(), run());
}

/// Hands the one prepared source to the seeding run. Its kind is `mqtt`, the
/// kind the configurations below name, so their hash is the same either way.
struct OneSource(Arc<Mutex<Option<Box<dyn Source>>>>);

impl SourceFactory for OneSource {
    fn kind(&self) -> &'static str {
        "mqtt"
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

/// A configuration with one MQTT source and one dataset, the given
/// `transforms:` block (empty or ending in a newline), and the sinks.
fn data_dir_config(data: &Path, name: &str, node: &str, transforms: &str, sinks: &str) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: {name}, node_id: {node}, data_dir: "{dir}" }}
sources:
  - id: src
    kind: mqtt
    config: {{ url: "mqtt://broker:1883", client_id: "edge-01", topics: [{{ filter: "t/+/data", qos: 1 }}] }}
wal: {{ max_disk: 256MiB, on_full: block }}
datasets:
  - id: readings
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: temp, type: f32, from: /temp }}
      - {{ id: 1001, name: device, type: str, from: /device }}
{transforms}sinks:
  - id: local
    kind: parquet
    config: {{ datasets: [readings] }}
{sinks}"#,
        dir = data.display()
    )
}

/// Runs `yaml` once over three messages, so the data directory holds what
/// `thadam run` leaves behind: `thadam.json`, `wal/`, and each sink's cursor.
async fn seed(yaml: &str) {
    let spec = thadam_config::load_str(yaml, &MapEnv::new())
        .unwrap_or_else(|r| panic!("{}", r.render(yaml)));
    let payloads = (1..=3).map(|n| Bytes::from(format!("{{\"temp\":{n}.5,\"device\":\"d{n}\"}}")));
    let (source, _upstream) = MemorySource::scripted(payloads, MemorySourceConfig::default());
    let slot: Arc<Mutex<Option<Box<dyn Source>>>> = Arc::new(Mutex::new(Some(Box::new(source))));
    let mut registry = Registry::new();
    registry
        .source(Box::new(OneSource(slot)))
        .segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()));
    let mut opts = RuntimeOptions::default();
    opts.stop_when_sources_finish = true;
    opts.clock_sync_probe = None;
    let pipeline =
        Pipeline::build(spec, &registry, opts).unwrap_or_else(|e| panic!("build: {e:?}"));
    pipeline
        .run(Shutdown::new())
        .await
        .expect("the seeding run failed");
}

fn write_config(base: &TempDataDir, yaml: &str) -> PathBuf {
    let path = base.join("pipeline.yaml");
    std::fs::write(&path, yaml).unwrap();
    path
}

#[test]
fn validate_against_data_dir_on_a_fresh_directory_exits_0_and_creates_nothing() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let config = write_config(&base, &data_dir_config(&data, "demo", "n1", "", ""));
    let out = thadam(&["validate", "--against-data-dir", config.to_str().unwrap()]);
    let stdout = text(&out.stdout);
    assert_eq!(code(&out), 0, "{stdout}\n{}", text(&out.stderr));
    assert!(
        stdout.contains(&format!("data directory: {}", data.display())),
        "{stdout}"
    );
    assert!(stdout.contains("no thadam.json"), "{stdout}");
    assert!(!data.exists(), "validate created the data directory");
}

#[tokio::test]
async fn validate_against_data_dir_on_a_matching_directory_reports_an_unchanged_hash() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let yaml = data_dir_config(&data, "demo", "n1", "", "");
    seed(&yaml).await;
    let config = write_config(&base, &yaml);
    let out = thadam(&["validate", "--against-data-dir", config.to_str().unwrap()]);
    let stdout = text(&out.stdout);
    assert_eq!(code(&out), 0, "{stdout}\n{}", text(&out.stderr));
    assert!(stdout.contains("(unchanged)"), "{stdout}");
    assert!(!stdout.contains("(changed)"), "{stdout}");
    assert!(!stdout.contains("has no cursor yet"), "{stdout}");
}

#[tokio::test]
async fn validate_against_data_dir_notes_the_hash_change_a_new_sink_and_orphaned_state() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    seed(&data_dir_config(&data, "demo", "n1", "", "")).await;
    std::fs::create_dir_all(data.join("sinks").join("gone")).unwrap();
    let extra = "  - id: local2\n    kind: parquet\n    config: { datasets: [readings] }\n";
    let config = write_config(&base, &data_dir_config(&data, "demo", "n1", "", extra));
    let out = thadam(&["validate", "--against-data-dir", config.to_str().unwrap()]);
    let stdout = text(&out.stdout);
    assert_eq!(code(&out), 0, "{stdout}\n{}", text(&out.stderr));
    assert!(stdout.contains("(changed)"), "{stdout}");
    assert!(
        stdout.contains("sink `local2` has no cursor yet, so it starts from `earliest`"),
        "{stdout}"
    );
    assert!(!stdout.contains("sink `local` has no cursor"), "{stdout}");
    assert!(
        stdout.contains("sinks/gone is not in the configuration"),
        "{stdout}"
    );
}

#[test]
fn validate_against_data_dir_exits_2_on_a_connector_config_error_that_plain_validate_accepts() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let typo =
        "  - id: typo\n    kind: parquet\n    config: { datasets: [readings], compresion: none }\n";
    let config = write_config(&base, &data_dir_config(&data, "demo", "n1", "", typo));
    let path = config.to_str().unwrap();

    let plain = thadam(&["validate", path]);
    assert_eq!(code(&plain), 0, "{}", text(&plain.stderr));
    let out = thadam(&["validate", "--against-data-dir", path]);
    assert_eq!(code(&out), 2, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("compresion"),
        "{}",
        text(&out.stderr)
    );
    assert!(!data.exists());
}

#[test]
fn validate_against_data_dir_exits_2_on_a_chain_that_does_not_compile() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let transforms = "transforms:\n  - { kind: enrich, dataset: readings, file: /nonexistent-thadam-validate/devices.csv, key: { field: device, column: id }, add: [{ name: model, type: str }], on_miss: reject }\n";
    let config = write_config(&base, &data_dir_config(&data, "demo", "n1", transforms, ""));
    let path = config.to_str().unwrap();

    let plain = thadam(&["validate", path]);
    assert_eq!(code(&plain), 0, "{}", text(&plain.stderr));
    let out = thadam(&["validate", "--against-data-dir", path]);
    assert_eq!(code(&out), 2, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("transforms[0].file"),
        "{}",
        text(&out.stderr)
    );
}

#[tokio::test]
async fn validate_against_data_dir_exits_3_on_a_pipeline_name_change() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    seed(&data_dir_config(&data, "demo", "n1", "", "")).await;
    let config = write_config(&base, &data_dir_config(&data, "other", "n1", "", ""));
    let path = config.to_str().unwrap();

    let plain = thadam(&["validate", path]);
    assert_eq!(
        code(&plain),
        0,
        "validate without the flag only reads the file"
    );
    let out = thadam(&["validate", "--against-data-dir", path]);
    assert_eq!(code(&out), 3, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("belongs to pipeline `demo`"),
        "{}",
        text(&out.stderr)
    );
}

#[tokio::test]
async fn validate_against_data_dir_exits_3_on_a_wal_bound_to_another_node() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    seed(&data_dir_config(&data, "demo", "n1", "", "")).await;
    let config = write_config(&base, &data_dir_config(&data, "demo", "n2", "", ""));

    let out = thadam(&["validate", "--against-data-dir", config.to_str().unwrap()]);
    assert_eq!(code(&out), 3, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("node_id `n1`"),
        "{}",
        text(&out.stderr)
    );
}

#[test]
fn validate_against_data_dir_refuses_allow_unresolved() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let config = write_config(&base, &data_dir_config(&data, "demo", "n1", "", ""));
    let out = thadam(&[
        "validate",
        "--against-data-dir",
        "--allow-unresolved",
        config.to_str().unwrap(),
    ]);
    assert_eq!(code(&out), 2, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("--allow-unresolved"),
        "{}",
        text(&out.stderr)
    );
    assert!(text(&out.stdout).is_empty());
    assert!(!data.exists());
}
