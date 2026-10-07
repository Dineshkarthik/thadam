//! Config hash stability (ADR-0012 PR5 detail 3).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;

use thadam_config::{CONFIG_HASH_VERSION, CheckOptions, MapEnv, PipelineSpec, check_str};

fn minimal() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/valid/minimal.yaml"),
    )
    .unwrap()
}

/// A frozen copy of the demo, so editing the example does not disturb the pinned hash.
fn demo() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/demo/pipeline.yaml"),
    )
    .unwrap()
}

fn env(key: &str, secret: &str) -> MapEnv {
    MapEnv::new()
        .with_file("/run/secrets/minio_uploader_key", key)
        .with_file("/run/secrets/minio_uploader_secret", secret)
}

fn load(yaml: &str, env: &MapEnv) -> PipelineSpec {
    let c = check_str(yaml, env, &CheckOptions::new());
    assert!(c.report.errors.is_empty(), "{}", c.report);
    c.spec.unwrap()
}

fn hash(yaml: &str) -> String {
    load(yaml, &MapEnv::new()).config_hash_hex()
}

#[test]
fn hashes_are_pinned() {
    // These literals must never change without bumping CONFIG_HASH_VERSION:
    // the hash is written into Parquet footers and the data directory.
    assert_eq!(CONFIG_HASH_VERSION, 1);
    assert_eq!(hash(&minimal()), "d65a9d2197f000a20ba65ee739a2bd5b");
    assert_eq!(
        load(&demo(), &env("k", "s")).config_hash_hex(),
        "414164d2430eab5caf362e63664b234c"
    );
}

#[test]
fn the_hash_is_deterministic() {
    assert_eq!(hash(&minimal()), hash(&minimal()));
    assert_eq!(hash(&minimal()).len(), 32);
}

#[test]
fn key_order_whitespace_comments_and_style_do_not_matter() {
    let reordered = "\
# a comment that changes nothing
sinks:
  - id: local
    kind: parquet
    config:
      datasets:   [ readings ]
datasets:
  - fields:
      - name: temp
        id: 1000
        type: f32
        from: /temp
      - {from: /level, type: \"str\", name: level, id: 1001}
    time: { domain: utc, unit: ms, field: /ts }
    identity: { device: { template: \"{topic.1}\", id: 100 } }   # trailing comment
    decode: json
    match: { topic: \"t/+/data\", source: src }
    id: readings
wal: { on_full: \"block\", max_disk: 1GiB }
sources:
  - config: { topics: [{ qos: 1, filter: \"t/+/data\" }], url: \"mqtt://broker:1883\" }
    kind: mqtt
    id: src
pipeline: { data_dir: /var/lib/thadam, node_id: n1, name: demo }
version: 1
";
    assert_eq!(hash(reordered), hash(&minimal()));
}

#[test]
fn equivalent_spellings_of_a_value_hash_alike() {
    let a = hash(&minimal().replace("max_disk: 1GiB", "max_disk: 1073741824"));
    let b = hash(&minimal().replace("max_disk: 1GiB", "max_disk: 1024MiB"));
    assert_eq!(a, hash(&minimal()));
    assert_eq!(b, a);
    let d1 = hash(&minimal().replace("on_full: block", "on_full: block, retain_for: 60m"));
    let d2 = hash(&minimal().replace("on_full: block", "on_full: block, retain_for: 1h"));
    assert_eq!(d1, d2);
    assert_ne!(d1, hash(&minimal()), "an added setting changes the hash");
}

#[test]
fn any_value_change_changes_the_hash() {
    let base = hash(&minimal());
    for (from, to) in [
        ("max_disk: 1GiB", "max_disk: 2GiB"),
        ("on_full: block", "on_full: drop_oldest"),
        ("node_id: n1", "node_id: n2"),
        ("type: f32", "type: f64"),
        ("from: /temp", "from: /temp2"),
        ("unit: ms", "unit: us"),
        ("qos: 1", "qos: 0"),
        ("datasets: [readings] }", "datasets: [readings, other] }"),
    ] {
        assert_ne!(hash(&minimal().replace(from, to)), base, "{from} -> {to}");
    }
}

#[test]
fn identity_order_is_part_of_the_hash() {
    let with = |identity: &str| {
        minimal().replace(
            "identity: { device: { id: 100, template: \"{topic.1}\" } }",
            identity,
        )
    };
    let zone_first = with(
        "identity: { zone: { id: 101, template: \"{topic.0}\" }, device: { id: 100, template: \"{topic.1}\" } }",
    );
    let device_first = with(
        "identity: { device: { id: 100, template: \"{topic.1}\" }, zone: { id: 101, template: \"{topic.0}\" } }",
    );
    assert_ne!(
        hash(&zone_first),
        hash(&device_first),
        "levels form an ordered path"
    );
    assert_eq!(hash(&zone_first), hash(&zone_first));
}

#[test]
fn rotating_a_secret_does_not_change_the_hash_but_repointing_it_does() {
    let a = load(&demo(), &env("key-1", "secret-1"));
    let b = load(&demo(), &env("key-2", "a-completely-different-secret"));
    assert_ne!(
        a.sinks[1].config, b.sinks[1].config,
        "the resolved values differ"
    );
    assert_eq!(a.config_hash(), b.config_hash());

    let repointed = demo().replace("minio_uploader_key", "other_key");
    let env = MapEnv::new()
        .with_file("/run/secrets/other_key", "key-1")
        .with_file("/run/secrets/minio_uploader_secret", "secret-1");
    assert_ne!(load(&repointed, &env).config_hash(), a.config_hash());
}

#[test]
fn the_spec_round_trips_through_its_serialized_form() {
    // The hash relies on the serialized shape matching the input shape.
    let spec = load(&minimal(), &MapEnv::new());
    let text = serde_json::to_string(&spec).unwrap();
    let again = load(&text, &MapEnv::new());
    assert_eq!(again, spec);
    assert_eq!(again.config_hash(), spec.config_hash());

    let demo = load(&demo(), &env("k", "s"));
    let text = serde_json::to_string(&demo).unwrap();
    let placeholder = check_str(&text, &MapEnv::new(), &CheckOptions::new());
    assert!(
        placeholder.report.errors.is_empty(),
        "{}",
        placeholder.report
    );
    let reloaded = placeholder.spec.unwrap();
    assert_eq!(reloaded.datasets, demo.datasets);
    assert_eq!(reloaded.transforms, demo.transforms);
}

#[test]
fn canonical_form_omits_absent_values_so_new_keys_do_not_move_hashes() {
    let canonical = load(&minimal(), &MapEnv::new()).canonical_json();
    assert!(!canonical.contains("null"), "{canonical}");
}

#[test]
fn canonical_form_is_snapshotted() {
    insta::assert_snapshot!(load(&minimal(), &MapEnv::new()).canonical_json());
}
