//! A windowed dataset through the real Parquet sink. The sink writes the
//! columns the dataset declares, matched by name, so a window's output
//! columns reach the file only when the dataset declares fields of those
//! names.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use thadam_core::Shutdown;
use thadam_runtime::{ExitStatus, Pipeline, Registry};
use thadam_testkit::TempDataDir;

fn config(dir: &Path, declare_outputs: bool) -> String {
    let outputs = if declare_outputs {
        "      - { id: 1001, name: n, type: u64 }\n      - { id: 1002, name: total, type: f64 }\n      - { id: 1003, name: window_start, type: ts, unit: ns }\n"
    } else {
        ""
    };
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
    identity: {{ robot: {{ id: 100, template: "{{topic.1}}" }} }}
    fields:
      - {{ id: 1000, name: v, type: f64, from: /v }}
{outputs}transforms:
  - {{ kind: window, dataset: d, size: 10s, aggregates: [{{ field: v, fn: count, as: n }}, {{ field: v, fn: sum, as: total }}] }}
sinks:
  - {{ id: pq, kind: parquet, config: {{ datasets: [d], compression: none }} }}
"#,
        dir = dir.display()
    )
}

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(parquet_files(&path));
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// Every row of every file under `root`, as column name to printed value.
fn read_rows(root: &Path) -> Vec<BTreeMap<String, String>> {
    let mut rows = Vec::new();
    for file in parquet_files(root) {
        let reader = SerializedFileReader::new(std::fs::File::open(file).unwrap()).unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            let row = row.unwrap();
            rows.push(
                row.get_column_iter()
                    .map(|(name, value)| (name.clone(), value.to_string()))
                    .collect(),
            );
        }
    }
    rows
}

async fn run(declare_outputs: bool) -> Vec<BTreeMap<String, String>> {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    feed(
        &kit,
        &[("a", 0, 1), ("a", 3, 2), ("a", 12, 3), ("b", 15, 7)],
    );
    let mut registry = Registry::new();
    registry
        .source(Box::new(SlotSourceFactory(kit.sources.clone())))
        .segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()))
        .quarantine_sink(Box::new(thadam_segments::QuarantineSinkFactory::new()));
    let mut o = fast_opts();
    o.stop_when_sources_finish = true;
    o.batch_limits.max_messages = 1;
    let p = Pipeline::build(spec(&config(dir.path(), declare_outputs)), &registry, o)
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let report = tokio::time::timeout(Duration::from_secs(60), p.run(Shutdown::new()))
        .await
        .expect("run timed out")
        .expect("run failed to start");
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    read_rows(&dir.path().join("sinks/pq"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_output_columns_carry_the_aggregates() {
    let rows = run(true).await;
    assert_eq!(rows.len(), 1, "one closed window: {rows:#?}");
    let row = &rows[0];
    assert_eq!(row["n"], "2");
    assert_eq!(row["total"], "3.0");
    assert_eq!(row["robot"], "\"a\"");
    assert_eq!(row["window_start"], "1970-01-01 00:00:00.000000 +00:00");
    assert_eq!(
        row["_event_time"], row["window_start"],
        "an aggregate's event time is its window start"
    );
    assert_eq!(row["_wal_pos"], "3", "the frame that closed the window");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undeclared_output_columns_are_not_written() {
    let rows = run(false).await;
    assert_eq!(rows.len(), 1, "{rows:#?}");
    let row = &rows[0];
    assert!(
        !row.contains_key("n") && !row.contains_key("total"),
        "{row:#?}"
    );
}
