//! Tests for the `thadam run` command.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
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
fn run_help_lists_flags() {
    let out = thadam(&["run", "--help"]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("thadam run"), "{stdout}");
    assert!(stdout.contains("--repair"), "{stdout}");
    assert!(stdout.contains("--log-format"), "{stdout}");
    assert!(stdout.contains("--log-level"), "{stdout}");
    assert!(stdout.contains("--admin-bind"), "{stdout}");
    assert!(stdout.contains("--admin-token-file"), "{stdout}");
}

#[test]
fn run_nonexistent_file_exits_2() {
    let out = thadam(&["run", "/no/such/thadam.yaml"]);
    assert_eq!(
        code(&out),
        2,
        "stdout: {}\nstderr: {}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("cannot read") || stderr.contains("No such file"),
        "{stderr}"
    );
}

#[test]
fn run_invalid_config_exits_2() {
    let path = fixture("invalid/multi_error.yaml");
    let out = thadam(&["run", path.to_str().unwrap()]);
    assert_eq!(
        code(&out),
        2,
        "stdout: {}\nstderr: {}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("error") || stderr.contains("invalid"),
        "{stderr}"
    );
}

/// Post-startup log lines must reach stderr: the logging guard is kept alive
/// for the whole run. A config that loads but cannot build proves it: the
/// pipeline build errors are logged through tracing after startup.
#[test]
fn run_logs_pipeline_build_errors_to_stderr() {
    let dir = std::env::temp_dir().join(format!(
        "thadam-cli-logging-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("pipeline.yaml");
    let yaml = r#"version: 1
pipeline: { name: logtest, node_id: n1, data_dir: DATA_DIR }
sources:
  - { id: src, kind: nosuchkind }
wal: { max_disk: 1GiB, on_full: block }
datasets:
  - id: d
    match: { source: src }
    decode: json
    fields:
      - { id: 1000, name: n, type: u64, from: /n, required: true }
sinks:
  - { id: out, kind: alsomissing }
"#
    .replace("DATA_DIR", &dir.join("data").display().to_string());
    std::fs::write(&config, yaml).unwrap();

    let out = thadam(&["run", config.to_str().unwrap()]);
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(code(&out), 2, "stderr: {}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("pipeline build error"),
        "post-startup build errors are logged to stderr:\n{stderr}"
    );
}
