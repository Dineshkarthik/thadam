//! Fixture-driven tests: every invalid fixture is snapshotted as rendered
//! diagnostics (path, line and column, hint), and the valid ones must load.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};

use thadam_config::{CheckOptions, Checked, KindPolicy, MapEnv, check_str};

fn dir(sub: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(sub)
}

fn fixtures(sub: &str) -> Vec<(String, String)> {
    let mut out: Vec<_> = fs::read_dir(dir(sub))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
        .map(|p| {
            let name = p.file_stem().unwrap().to_string_lossy().into_owned();
            (name, fs::read_to_string(&p).unwrap())
        })
        .collect();
    out.sort();
    out
}

fn opts() -> CheckOptions {
    CheckOptions::new().kinds(
        KindPolicy::new()
            .sources(["mqtt"])
            .sinks(["parquet", "object_store"]),
    )
}

fn check(yaml: &str) -> Checked {
    check_str(yaml, &MapEnv::new(), &opts())
}

#[test]
fn every_invalid_fixture_is_rejected_with_located_diagnostics() {
    let all = fixtures("invalid");
    assert!(
        all.len() >= 50,
        "expected the full fixture set, found {}",
        all.len()
    );
    for (name, yaml) in &all {
        let checked = check(yaml);
        assert!(checked.spec.is_none(), "{name} must not load");
        assert!(checked.report.has_errors(), "{name} must report an error");
        for d in &checked.report.errors {
            assert!(d.code.starts_with("config."), "{name}: {}", d.code);
            if !matches!(name.as_str(), "empty_file" | "only_comment") {
                assert!(d.span.is_some(), "{name}: `{}` has no position", d.path);
            }
        }
        insta::assert_snapshot!(
            name.as_str(),
            checked.report.render(&format!("{name}.yaml"))
        );
    }
}

#[test]
fn valid_fixtures_load_without_diagnostics() {
    for (name, yaml) in &fixtures("valid") {
        let checked = check(yaml);
        assert!(
            checked.report.errors.is_empty(),
            "{name}: {}",
            checked.report
        );
        assert!(
            checked.report.warnings.is_empty(),
            "{name}: {}",
            checked.report
        );
        assert!(checked.spec.is_some(), "{name}");
    }
}

#[test]
fn warning_fixtures_load_with_a_warning() {
    for (name, yaml) in &fixtures("warnings") {
        let checked = check(yaml);
        assert!(checked.spec.is_some(), "{name}: {}", checked.report);
        assert!(!checked.report.warnings.is_empty(), "{name}");
        insta::assert_snapshot!(
            format!("warning_{name}"),
            checked.report.render(&format!("{name}.yaml"))
        );
    }
}

#[test]
fn the_demo_example_loads_with_stub_secrets_and_no_warnings() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/edge-capture/thadam/pipeline.yaml");
    let yaml = fs::read_to_string(path).unwrap();
    let env = MapEnv::new()
        .with_file("/run/secrets/minio_uploader_key", "AKIA-demo\n")
        .with_file("/run/secrets/minio_uploader_secret", "s3cr3t-demo\n");
    let checked = check_str(&yaml, &env, &opts());
    assert!(checked.report.errors.is_empty(), "{}", checked.report);
    assert!(checked.report.warnings.is_empty(), "{}", checked.report);
    let spec = checked.spec.unwrap();
    assert_eq!(spec.sources.len(), 1);
    assert_eq!(spec.datasets.len(), 6);
    assert_eq!(spec.transforms.len(), 3);
    assert_eq!(spec.sinks.len(), 2);
}
