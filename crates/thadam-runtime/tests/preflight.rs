//! `Pipeline::check`: the checks `build` makes before any connector exists,
//! run without building one or touching the data directory.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use thadam_config::{MapEnv, PipelineSpec};
use thadam_core::{BuildContext, ErrorDomain, SegmentSink, SegmentSinkFactory, ThadamError, codes};
use thadam_runtime::{Pipeline, Registry, RuntimeOptions};
use thadam_testkit::TempDataDir;

/// `kind: counted`: refuses a `bad` key in its config, and counts builds.
struct CountedFactory {
    builds: Arc<AtomicUsize>,
}

impl SegmentSinkFactory for CountedFactory {
    fn kind(&self) -> &'static str {
        "counted"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        if cfg.get("bad").is_some() {
            return Err(vec![
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "`bad` is not a known key",
                )
                .with_context("path", "bad"),
            ]);
        }
        Ok(())
    }
    fn build(
        &self,
        _cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(RecSink::new(RecStore::default(), 4)))
    }
}

fn registry(builds: &Arc<AtomicUsize>) -> Registry {
    let mut r = Registry::new();
    r.source(Box::new(SlotSourceFactory(SourceSlots::default())))
        .segment_sink(Box::new(CountedFactory {
            builds: Arc::clone(builds),
        }))
        .batch_sink(Box::new(BatchSlotFactory(BatchSlots::default())));
    r
}

/// One memory source, one dataset with a string field `device`, the given
/// `transforms:` block (empty or ending in a newline), and one sink.
fn config(data: &Path, sink: &str, transforms: &str) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, on_full: block }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: v, type: f64, from: /v }}
      - {{ id: 1001, name: device, type: str, from: /device }}
{transforms}sinks:
  - {sink}
"#,
        dir = data.display()
    )
}

fn parse(text: &str) -> PipelineSpec {
    thadam_config::load_str(text, &MapEnv::new()).unwrap_or_else(|r| panic!("{}", r.render(text)))
}

#[test]
fn check_reports_a_connector_config_error_and_builds_nothing() {
    let dir = TempDataDir::new().unwrap();
    let builds = Arc::new(AtomicUsize::new(0));
    let reg = registry(&builds);
    let text = config(
        dir.path(),
        "{ id: out, kind: counted, config: { bad: true } }",
        "",
    );

    let errs = Pipeline::check(&parse(&text), &reg, &RuntimeOptions::default()).unwrap_err();
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert_eq!(errs[0].context.get("component"), Some("sinks[0]"));
    assert!(errs[0].message.contains("not a known key"), "{errs:?}");
    assert_eq!(builds.load(Ordering::SeqCst), 0);

    let built = Pipeline::build(parse(&text), &reg, RuntimeOptions::default())
        .expect_err("build refuses the same configuration");
    assert_eq!(built, errs);
}

#[test]
fn check_reports_a_chain_that_does_not_compile_and_builds_nothing() {
    let dir = TempDataDir::new().unwrap();
    let builds = Arc::new(AtomicUsize::new(0));
    let transforms = "transforms:\n  - { kind: enrich, dataset: d, file: /nonexistent-thadam-check/devices.csv, key: { field: device, column: id }, add: [{ name: model, type: str }], on_miss: reject }\n";
    let text = config(dir.path(), "{ id: out, kind: counted }", transforms);

    let errs = Pipeline::check(
        &parse(&text),
        &registry(&builds),
        &RuntimeOptions::default(),
    )
    .unwrap_err();
    assert!(
        errs.iter()
            .any(|e| e.context.get("component") == Some("sinks[0]")),
        "{errs:?}"
    );
    assert_eq!(builds.load(Ordering::SeqCst), 0);
}

#[test]
fn check_refuses_a_window_on_a_batch_sink() {
    let dir = TempDataDir::new().unwrap();
    let builds = Arc::new(AtomicUsize::new(0));
    let transforms = "transforms:\n  - { kind: window, dataset: d, size: 10s, aggregates: [{ field: v, fn: count, as: n }] }\n";
    let text = config(dir.path(), "{ id: out, kind: membatch }", transforms);

    let errs = Pipeline::check(
        &parse(&text),
        &registry(&builds),
        &RuntimeOptions::default(),
    )
    .unwrap_err();
    assert!(
        errs.iter()
            .any(|e| e.message.contains("needs a segment sink")),
        "{errs:?}"
    );
}

#[test]
fn check_passes_a_valid_pipeline_and_leaves_the_data_directory_alone() {
    let base = TempDataDir::new().unwrap();
    let data = base.join("data");
    let builds = Arc::new(AtomicUsize::new(0));
    let text = config(&data, "{ id: out, kind: counted }", "");

    Pipeline::check(
        &parse(&text),
        &registry(&builds),
        &RuntimeOptions::default(),
    )
    .expect("a valid pipeline checks");
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    assert!(!data.exists(), "check created the data directory");
}
