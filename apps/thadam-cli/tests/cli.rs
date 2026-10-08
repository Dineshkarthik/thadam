//! End-to-end tests of the `thadam` binary: output and exit codes.

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
    assert!(stderr.contains("pipeline.yaml:170:22"), "{stderr}");
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
