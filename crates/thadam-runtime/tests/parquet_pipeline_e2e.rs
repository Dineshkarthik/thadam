//! The real Parquet segment sink end to end: datasets are declared once in
//! the pipeline configuration, the runtime hands them to the sink through the
//! build context (`BuildContext::datasets`), and the written files carry the
//! configured field IDs. Also proves a legacy in-sink `schemas:` block is
//! refused with a pointer to the pipeline-level `datasets:` section.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use thadam_config::PipelineSpec;
use thadam_core::Shutdown;
use thadam_runtime::{ExitStatus, Pipeline, Registry, RunReport, RuntimeOptions};
use thadam_testkit::TempDataDir;

fn parquet_registry(sources: SourceSlots) -> Registry {
    let mut r = Registry::new();
    r.source(Box::new(SlotSourceFactory(sources)))
        .segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()))
        .quarantine_sink(Box::new(thadam_segments::QuarantineSinkFactory::new()));
    r
}

async fn run_with(registry: &Registry, spec: PipelineSpec, mut opts: RuntimeOptions) -> RunReport {
    opts.stop_when_sources_finish = true;
    let p = Pipeline::build(spec, registry, opts).unwrap_or_else(|e| panic!("build: {e:?}"));
    tokio::time::timeout(Duration::from_secs(60), p.run(Shutdown::new()))
        .await
        .expect("run timed out")
        .expect("run failed to start")
}

/// A pipeline declaring dataset `d` (fields `n` at 1000 and `temp` at 1001)
/// and a second dataset `e` the Parquet sink does not select. `sink_config`
/// is spliced into the sink's `config:` block.
fn yaml_with_sink(data_dir: &Path, sink_config: &str) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: pqtest, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
      - {{ id: 1001, name: temp, type: f32, from: /temp }}
  - id: e
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
sinks:
  - id: pq
    kind: parquet
    config:
{sink_config}
"#,
        dir = data_dir.display()
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

/// Leaf column path to Parquet field ID, from the file's footer schema.
fn leaf_field_ids(path: &Path) -> BTreeMap<String, i32> {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .unwrap_or_else(|e| panic!("footer {}: {e}", path.display()));
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .filter_map(|c| {
            let info = c.self_type().get_basic_info();
            info.has_id().then(|| (c.path().string(), info.id()))
        })
        .collect()
}

/// Row count from a file's footer.
fn rows_of(path: &Path) -> i64 {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .unwrap_or_else(|e| panic!("footer {}: {e}", path.display()));
    (0..metadata.num_row_groups())
        .map(|i| metadata.row_group(i).num_rows())
        .sum()
}

/// A footer key-value, when present and valid UTF-8.
fn footer_kv(path: &Path, key: &str) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .ok()?;
    metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == key))
        .and_then(|kv| kv.value.clone())
}

const PAYLOADS: u64 = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_sink_builds_from_pipeline_datasets_and_writes_their_field_ids() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted(
        "src",
        (1..=PAYLOADS).map(|n| Bytes::from(format!("{{\"n\":{n},\"temp\":{n}.5}}"))),
    );
    let y = yaml_with_sink(
        dir.path(),
        "      datasets: [d]\n      compression: none\n      rotation: { max_bytes: null, max_rows: 2, max_age: null }",
    );
    let registry = parquet_registry(kit.sources.clone());
    let report = run_with(&registry, spec(&y), fast_opts()).await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    let files = parquet_files(&dir.path().join("sinks/pq/ready"));
    assert!(!files.is_empty(), "no segments under ready/");
    // The WAL intake packs the scripted payloads into one frame, so a single
    // file can cover all of them; rows are the ground truth either way.
    let total_rows: i64 = files.iter().map(|f| rows_of(f)).sum();
    assert_eq!(total_rows, PAYLOADS as i64, "{files:?}");
    for f in &files {
        let s = f.to_string_lossy().replace('\\', "/");
        assert!(s.contains("/ready/d/"), "only selected datasets: {s}");
    }

    // Every file carries the pipeline-declared IDs: the declared fields
    // `n` (1000) and `temp` (1001), plus the fixed envelope column `_wal_pos`
    // (11). Identity-level IDs go through the same `LevelDecl` path, covered
    // by the thadam-segments config tests.
    let ids = leaf_field_ids(&files[0]);
    assert_eq!(ids.get("n"), Some(&1000), "{ids:?}");
    assert_eq!(ids.get("temp"), Some(&1001), "{ids:?}");
    assert_eq!(ids.get("_wal_pos"), Some(&11), "{ids:?}");
    assert_eq!(footer_kv(&files[0], "thadam.dataset").as_deref(), Some("d"));

    // The unselected dataset `e` produced no files anywhere under the sink.
    let everything = parquet_files(&dir.path().join("sinks/pq"));
    assert!(
        everything
            .iter()
            .all(|f| !f.to_string_lossy().replace('\\', "/").contains("/e/")),
        "{everything:?}"
    );
}

/// The footer hashes of a real run equal what `thadam validate` prints: the
/// config hash is `PipelineSpec::config_hash_hex`, the function validate uses,
/// and a sink with no enrichment table records an empty enrichment hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_footer_carries_the_config_hash_that_validate_prints() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted(
        "src",
        (1..=PAYLOADS).map(|n| Bytes::from(format!("{{\"n\":{n},\"temp\":{n}.5}}"))),
    );
    let y = yaml_with_sink(
        dir.path(),
        "      datasets: [d]\n      compression: none\n      rotation: { max_bytes: null, max_rows: 2, max_age: null }",
    );
    let registry = parquet_registry(kit.sources.clone());
    let report = run_with(&registry, spec(&y), fast_opts()).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    let validate_hash = spec(&y).config_hash_hex();
    assert_eq!(report.status.config_hash, validate_hash);
    let files = parquet_files(&dir.path().join("sinks/pq/ready"));
    assert!(!files.is_empty(), "no segments under ready/");
    for f in &files {
        assert_eq!(
            footer_kv(f, "thadam.config_hash").as_deref(),
            Some(validate_hash.as_str()),
            "{}",
            f.display()
        );
        assert_eq!(footer_kv(f, "thadam.enrichment_hash").as_deref(), Some(""));
    }
}

/// A pipeline joining `d` with the CSV table at `table`, keyed by `robot`.
fn yaml_enriched(data_dir: &Path, table: &Path) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: pqtest, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: robot, type: str, from: /robot, required: true }}
transforms:
  - {{ kind: enrich, dataset: d, file: "{table}", key: {{ field: robot, column: robot_id }}, add: [{{ name: model, type: str }}] }}
sinks:
  - id: pq
    kind: parquet
    config:
      datasets: [d]
      compression: none
"#,
        dir = data_dir.display(),
        table = table.display()
    )
}

/// The enrichment hash footer of a run joined with `table_csv`.
async fn footer_enrichment_hash(table_csv: &str) -> String {
    let dir = TempDataDir::new().unwrap();
    let table_dir = tempfile::tempdir().unwrap();
    let table = table_dir.path().join("robots.csv");
    std::fs::write(&table, table_csv).unwrap();
    let kit = Kit::default();
    kit.sources
        .scripted("src", [Bytes::from_static(br#"{"robot":"r1"}"#)]);
    let registry = parquet_registry(kit.sources.clone());
    let report = run_with(
        &registry,
        spec(&yaml_enriched(dir.path(), &table)),
        fast_opts(),
    )
    .await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    let files = parquet_files(&dir.path().join("sinks/pq/ready"));
    assert!(!files.is_empty(), "no segments under ready/");
    footer_kv(&files[0], "thadam.enrichment_hash").expect("enrichment hash footer")
}

/// `thadam.enrichment_hash` is the lineage hash of the table the chain joined:
/// set for a joined table, and different when the table's content changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_footer_enrichment_hash_follows_the_table_it_joined() {
    let first = footer_enrichment_hash("robot_id,model\nr1,A1\n").await;
    let second = footer_enrichment_hash("robot_id,model\nr1,B2\n").await;
    assert_eq!(first.len(), 64, "{first:?}");
    assert!(first.bytes().all(|b| b.is_ascii_hexdigit()), "{first}");
    assert_ne!(first, second, "a changed table is a changed lineage");
}

/// Rejects go to quarantine, whose files record the same config hash as the
/// sink that produced them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quarantine_footer_carries_the_config_hash() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted(
        "src",
        [
            Bytes::from_static(b"{\"n\":1}"),
            Bytes::from_static(b"{\"temp\":2.0}"),
        ],
    );
    let y = yaml_with_sink(dir.path(), "      datasets: [d]\n      compression: none");
    let registry = parquet_registry(kit.sources.clone());
    let report = run_with(&registry, spec(&y), fast_opts()).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    let quarantined = parquet_files(&dir.path().join("quarantine/pq"));
    assert!(!quarantined.is_empty(), "the reject reached quarantine");
    assert_eq!(
        footer_kv(&quarantined[0], "thadam.config_hash"),
        Some(spec(&y).config_hash_hex())
    );
}

/// Without a sink `node_id`, segment and quarantine files are named by the
/// pipeline's `node_id`, so nodes that share one configuration never write
/// the same file name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_and_quarantine_files_are_named_by_the_pipeline_node_id() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted(
        "src",
        [
            Bytes::from_static(b"{\"n\":1}"),
            Bytes::from_static(b"{\"temp\":2.0}"),
        ],
    );
    let y = yaml_with_sink(dir.path(), "      datasets: [d]\n      compression: none")
        .replace("node_id: node-1", "node_id: edge-7");
    let registry = parquet_registry(kit.sources.clone());
    let report = run_with(&registry, spec(&y), fast_opts()).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    let segments = parquet_files(&dir.path().join("sinks/pq/ready"));
    let quarantined = parquet_files(&dir.path().join("quarantine/pq"));
    assert!(!segments.is_empty(), "no segments under ready/");
    assert!(!quarantined.is_empty(), "the reject reached quarantine");
    for f in segments.iter().chain(&quarantined) {
        let name = f.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("edge-7_"), "{name}");
    }
}

#[test]
fn pipeline_build_refuses_a_sink_schemas_block() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources
        .scripted("src", [Bytes::from_static(b"{\"n\":1}")]);
    let y = yaml_with_sink(
        dir.path(),
        "      schemas:\n        d: { fields: [ { name: n, type: u64, id: 1000 } ] }",
    );
    let registry = parquet_registry(kit.sources.clone());
    let errs = Pipeline::build(spec(&y), &registry, fast_opts())
        .err()
        .unwrap_or_else(|| panic!("a `schemas:` block must not build"));
    let text = errs
        .iter()
        .map(|e| e.message.clone())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("pipeline-level `datasets:`"),
        "the refusal points at the pipeline datasets section:\n{text}"
    );
}
