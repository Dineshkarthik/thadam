//! The shipped edge-capture demo configuration stays valid, so the
//! `thadam validate --allow-unresolved examples/edge-capture/thadam/pipeline.yaml`
//! line in the docs keeps working.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use thadam_config::{CheckOptions, KindPolicy, MapEnv, UnresolvedPolicy, check_file};

fn demo_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/edge-capture/thadam/pipeline.yaml")
}

#[test]
fn demo_pipeline_loads_with_placeholder_secrets() {
    let path = demo_path();
    let opts = CheckOptions::new()
        .unresolved(UnresolvedPolicy::Placeholder)
        .kinds(
            KindPolicy::new()
                .sources(["mqtt"])
                .sinks(["parquet", "object_store"]),
        );
    let checked = check_file(&path, &MapEnv::new(), &opts);
    let rendered = checked.report.render(&path.display().to_string());
    let spec = checked
        .spec
        .unwrap_or_else(|| panic!("demo loads:\n{rendered}"));
    assert!(
        checked.report.errors.is_empty(),
        "no diagnostics:\n{rendered}"
    );
    assert_eq!(spec.datasets.len(), 6, "the six demo datasets");

    // Schemas are declared once in the pipeline-level `datasets:` section;
    // the parquet sink only selects ids (no in-sink `schemas:` block), and
    // its writer options use the enum forms the sink accepts.
    let local = spec
        .sinks
        .iter()
        .find(|s| s.id == "local")
        .expect("local parquet sink");
    assert!(
        local.config.get("schemas").is_none(),
        "schemas live in `datasets:`, not the sink config"
    );
    let selector = local.config["datasets"]
        .as_array()
        .expect("a datasets selector");
    assert_eq!(selector.len(), 6);
    assert_eq!(local.config["timestamp_unit"].as_str(), Some("micros"));
}
