//! End-to-end tests for `thadam init`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::process::{Command, Output};

use thadam_testkit::TempDataDir;

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

#[test]
fn init_writes_starter_file_that_passes_validate() {
    let dir = TempDataDir::new().unwrap();
    let target = dir.join("pipeline.yaml");
    let path = target.to_str().unwrap();

    let init = thadam(&["init", path]);
    assert_eq!(code(&init), 0, "{}", text(&init.stderr));
    assert!(text(&init.stdout).contains(path), "{}", text(&init.stdout));

    let body = fs::read_to_string(&target).unwrap();
    assert!(body.contains("on_full: block"), "{body}");
    assert!(body.contains("`on_full` has no default"), "{body}");
    assert!(body.contains("drop_oldest"), "{body}");

    let val = thadam(&["validate", path]);
    assert_eq!(
        code(&val),
        0,
        "{}\n{}",
        text(&val.stdout),
        text(&val.stderr)
    );
    assert!(text(&val.stderr).is_empty(), "{}", text(&val.stderr));
    assert!(
        text(&val.stdout).contains("configuration is valid"),
        "{}",
        text(&val.stdout)
    );

    let full = thadam(&["validate", "--against-data-dir", path]);
    assert_eq!(
        code(&full),
        0,
        "{}\n{}",
        text(&full.stdout),
        text(&full.stderr)
    );
    assert!(text(&full.stderr).is_empty(), "{}", text(&full.stderr));
}

#[test]
fn init_defaults_to_pipeline_yaml_in_current_directory() {
    let dir = TempDataDir::new().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_thadam"))
        .current_dir(dir.path())
        .args(["init"])
        .output()
        .expect("run thadam");
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    assert!(dir.join("pipeline.yaml").is_file());
}

#[test]
fn init_refuses_to_overwrite_an_existing_file() {
    let dir = TempDataDir::new().unwrap();
    let target = dir.join("pipeline.yaml");
    fs::write(&target, "existing: true\n").unwrap();
    let path = target.to_str().unwrap();

    let out = thadam(&["init", path]);
    assert_eq!(code(&out), 2, "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("already exists"),
        "{}",
        text(&out.stderr)
    );
    assert_eq!(fs::read_to_string(&target).unwrap(), "existing: true\n");
}
