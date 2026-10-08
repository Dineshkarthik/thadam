//! The per-sink `required`, `max_lag`, and `start_from` keys as typed values.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;
use std::time::Duration;

use thadam_config::{
    CheckOptions, DurationSpec, MapEnv, MaxLagSpec, PipelineSpec, SinkSpec, SizeSpec,
    StartFromSpec, check_str,
};

fn fixture(sub: &str, name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(sub)
            .join(format!("{name}.yaml")),
    )
    .unwrap()
}

fn load(yaml: &str) -> PipelineSpec {
    let c = check_str(yaml, &MapEnv::new(), &CheckOptions::new());
    assert!(c.report.errors.is_empty(), "{}", c.report);
    assert!(c.report.warnings.is_empty(), "{}", c.report);
    c.spec.unwrap()
}

fn assert_defaults(s: &SinkSpec) {
    assert!(s.required);
    assert_eq!(s.max_lag, None);
    assert_eq!(s.start_from, StartFromSpec::Earliest);
}

#[test]
fn a_sink_without_policy_keys_has_the_defaults() {
    let spec = load(&fixture("valid", "minimal"));
    assert_defaults(&spec.sinks[0]);
}

#[test]
fn the_policy_fixture_produces_the_typed_values() {
    let spec = load(&fixture("valid", "sink_policies"));
    let [local, archive] = spec.sinks.as_slice() else {
        panic!("expected two sinks");
    };
    assert_eq!(local.id, "local");
    assert!(!local.required);
    assert_eq!(local.max_lag, None);
    assert_eq!(local.start_from, StartFromSpec::Latest);

    assert_eq!(archive.id, "archive");
    assert!(archive.required);
    assert_eq!(
        archive.max_lag,
        Some(MaxLagSpec::Age(DurationSpec(Duration::from_secs(30 * 60))))
    );
    assert_eq!(archive.start_from, StartFromSpec::Earliest);
}

#[test]
fn max_lag_bytes_reads_sizes_in_every_spelling() {
    let base = fixture("valid", "minimal");
    for (text, bytes) in [
        ("2GiB", 2u64 << 30),
        ("512MiB", 512 << 20),
        ("1048576", 1 << 20),
    ] {
        let yaml = base.replace(
            "    kind: parquet\n",
            &format!("    kind: parquet\n    max_lag: {{ bytes: {text} }}\n"),
        );
        assert_eq!(
            load(&yaml).sinks[0].max_lag,
            Some(MaxLagSpec::Bytes(SizeSpec(bytes))),
            "{text}"
        );
    }
}

#[test]
fn sources_do_not_accept_the_sink_policy_keys() {
    let yaml = fixture("valid", "minimal")
        .replace("    kind: mqtt\n", "    kind: mqtt\n    required: false\n");
    let c = check_str(&yaml, &MapEnv::new(), &CheckOptions::new());
    assert!(c.spec.is_none());
    assert!(
        c.report
            .errors
            .iter()
            .any(|d| d.path == "sources[0].required" && d.message.contains("unknown key")),
        "{}",
        c.report
    );
}

#[test]
fn a_sink_debug_output_and_serialization_redact_secrets() {
    let yaml = fixture("valid", "minimal").replace(
        "config: { datasets: [readings] }",
        "config: { datasets: [readings], token: \"${env:SINK_TOKEN}\" }",
    );
    let env = MapEnv::new().with_var("SINK_TOKEN", "super-secret-token");
    let c = check_str(&yaml, &env, &CheckOptions::new());
    let sink = &c.spec.unwrap().sinks[0];
    assert_eq!(sink.secrets().len(), 1);
    assert!(!format!("{sink:?}").contains("super-secret-token"));
    assert!(
        !serde_json::to_string(sink)
            .unwrap()
            .contains("super-secret-token")
    );
}
