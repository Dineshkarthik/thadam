//! The fixtures every `thadam-segments` test builds on: a config builder for
//! the test shape (single `n: I64` column, the shape `thadam-testkit` events
//! use), filesystem scan helpers, and the conformance fixture.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thadam_arrow::{ArrowOptions, DatasetSchema, TsUnit, UnsignedPolicy};
use thadam_core::{DiskBudget, FieldDef, FieldType, Shutdown, SinkContext, SinkId, WalRange};
use thadam_testkit::FakeClock;
use thadam_testkit::conformance::segment_sink::SegmentSinkFixture;

use crate::catalog::SegmentCatalog;
use crate::config::{Compression, ParquetSinkConfig, RotationConfig};
use crate::failpoints::{Failpoint, Failpoints};
use crate::partition;
use crate::sink::ParquetSegmentSink;

/// The shared schema for test events: one nullable `n: I64` at field ID
/// 1000 (the `thadam-testkit` test events are exactly this shape).
pub(crate) fn test_schema(name: &str) -> Arc<DatasetSchema> {
    Arc::new(
        DatasetSchema::builder(name)
            .field(FieldDef::new("n", FieldType::I64).with_field_id(1000))
            .options(ArrowOptions::default())
            .build()
            .unwrap_or_else(|e| panic!("test schema {name}: {e}")),
    )
}

/// A sink configuration over `dir`, defaulting to no compression
/// (deterministic, fast tests).
pub(crate) fn config_for(
    datasets: &[&str],
    dir: &Path,
    rotation: RotationConfig,
    max_open_files: usize,
) -> ParquetSinkConfig {
    let schemas: BTreeMap<String, Arc<DatasetSchema>> = datasets
        .iter()
        .map(|d| (d.to_string(), test_schema(d)))
        .collect();
    ParquetSinkConfig {
        dir: dir.to_path_buf(),
        node_id: "edge01".to_owned(),
        exploded: schemas.keys().map(|k| (k.clone(), false)).collect(),
        datasets: schemas,
        compression: Compression::None,
        row_group_bytes: 1 << 16,
        rotation,
        max_open_files,
        max_disk: None,
        timestamp_unit: TsUnit::Micros,
        unsigned: UnsignedPolicy::Native,
        finalize_on_shutdown: true,
        config_hash: "c0ffee".to_owned(),
        enrichment_hash: String::new(),
    }
}

/// The suite-facing fixture: owns the data directory, failpoints, and clock,
/// so reopening a sink simulates a process restart over durable state.
pub(crate) struct ParquetFixture {
    pub(crate) dir: tempfile::TempDir,
    pub(crate) fps: Failpoints,
    pub(crate) clock: FakeClock,
    datasets: Vec<String>,
    rotation: RotationConfig,
    max_open_files: usize,
}

impl ParquetFixture {
    /// `max_rows` one, like the memory fixture's `frames_per_segment` 1: a
    /// poll finalizes after every append, so crash failpoints fire
    /// mid-stream.
    pub(crate) fn suite(datasets: &[&str]) -> Self {
        Self::new(
            datasets,
            RotationConfig {
                max_bytes: None,
                max_rows: Some(1),
                max_age: None,
            },
            4,
        )
    }

    pub(crate) fn new(datasets: &[&str], rotation: RotationConfig, max_open_files: usize) -> Self {
        ParquetFixture {
            dir: tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}")),
            fps: Failpoints::default(),
            clock: FakeClock::default(),
            datasets: datasets.iter().map(|d| d.to_string()).collect(),
            rotation,
            max_open_files,
        }
    }

    pub(crate) fn batch_config(&self) -> ParquetSinkConfig {
        let ds: Vec<&str> = self.datasets.iter().map(String::as_str).collect();
        config_for(&ds, self.dir.path(), self.rotation, self.max_open_files)
    }

    pub(crate) fn open_sink(&self) -> ParquetSegmentSink {
        let mut sink = ParquetSegmentSink::new(self.batch_config())
            .unwrap_or_else(|e| panic!("sink builds: {e}"));
        sink.set_failpoints(self.fps.clone());
        sink
    }

    pub(crate) fn ctx(&self) -> SinkContext {
        SinkContext::new(
            SinkId::new("test"),
            Shutdown::new(),
            self.clock.shared(),
            self.dir.path().to_path_buf(),
            Arc::new(DiskBudget::new()),
        )
    }

    /// Every `ready/` Parquet file as `(dataset, name, wal-range)`, sorted by
    /// `(dataset, first)`.
    pub(crate) fn ready_files(&self) -> Vec<(String, String, WalRange)> {
        let root = self.dir.path().join("ready");
        let mut out = Vec::new();
        for path in files_of(&root) {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_owned();
            let Some(parsed) = partition::parse_segment_name(&name) else {
                continue;
            };
            let Ok(range) = WalRange::new(parsed.epoch, parsed.first, parsed.last) else {
                continue;
            };
            let dataset = path
                .strip_prefix(&root)
                .ok()
                .and_then(|rel| rel.components().next())
                .and_then(|c| c.as_os_str().to_str())
                .unwrap_or_default()
                .to_owned();
            out.push((dataset, name, range));
        }
        out.sort_by_key(|a| (a.0.clone(), a.2.first()));
        out
    }

    /// The merged WAL cover of on-disk segment files (overlap-coalesced).
    pub(crate) fn merged_ranges(&self) -> Vec<WalRange> {
        let mut ranges: Vec<WalRange> = self.ready_files().into_iter().map(|f| f.2).collect();
        ranges.sort_by_key(|r| (r.first(), r.last()));
        merge_ranges(&ranges)
    }

    /// Rows per dataset across all `ready/` files (datasets with no file
    /// present count as zero).
    pub(crate) fn dataset_rows(&self) -> BTreeMap<String, usize> {
        let mut out: BTreeMap<String, usize> =
            self.datasets.iter().map(|d| (d.clone(), 0usize)).collect();
        let root = self.dir.path().join("ready");
        for dataset in &self.datasets {
            for path in files_of(&root.join(dataset)) {
                *out.entry(dataset.clone()).or_default() += rows_of(&path);
            }
        }
        out
    }

    /// `(dataset, wal_pos)` pairs from every ready file, for dedup checks.
    pub(crate) fn all_rows(&self) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        let root = self.dir.path().join("ready");
        for dataset in &self.datasets {
            for path in files_of(&root.join(dataset)) {
                for pos in wal_pos_of(&path) {
                    out.push((dataset.clone(), pos));
                }
            }
        }
        out.sort();
        out
    }

    /// Reopens the catalog directly (blocking).
    pub(crate) fn open_catalog(&self) -> SegmentCatalog {
        let (catalog, _) = SegmentCatalog::open(
            self.dir.path(),
            "edge01",
            None,
            thadam_core::BudgetCategory::Segments,
            &self.datasets,
        )
        .unwrap_or_else(|e| panic!("catalog opens: {e}"));
        catalog
    }
}

pub(crate) fn merge_ranges(sorted: &[WalRange]) -> Vec<WalRange> {
    let mut out: Vec<WalRange> = Vec::new();
    for r in sorted {
        match out.last_mut() {
            Some(prev)
                if prev.epoch() == r.epoch()
                    && r.first().get() <= prev.last().get().saturating_add(1) =>
            {
                if r.last() > prev.last() {
                    *prev = WalRange::new(r.epoch(), prev.first(), r.last()).unwrap_or(*prev);
                }
            }
            _ => out.push(*r),
        }
    }
    out
}

pub(crate) fn files_of(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(files_of(&path));
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// Row count from a file's footer.
pub(crate) fn rows_of(path: &Path) -> usize {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .unwrap_or_else(|e| panic!("footer {}: {e}", path.display()));
    let rows: i64 = (0..metadata.num_row_groups())
        .map(|i| metadata.row_group(i).num_rows())
        .sum();
    usize::try_from(rows).unwrap_or(0)
}

/// The `_wal_pos` column of a file, read back through the Arrow reader.
pub(crate) fn wal_pos_of(path: &Path) -> Vec<u64> {
    use arrow_array::UInt64Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("reader {}: {e}", path.display()));
    let pos_col = builder
        .schema()
        .index_of("_wal_pos")
        .unwrap_or_else(|e| panic!("wal pos column in {}: {e}", path.display()));
    let reader = builder
        .with_batch_size(1 << 16)
        .build()
        .unwrap_or_else(|e| panic!("build reader {}: {e}", path.display()));
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap_or_else(|e| panic!("batch {}: {e}", path.display()));
        let col = batch
            .column(pos_col)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("u64 wal pos column in {}", path.display()));
        out.extend(col.iter().flatten());
    }
    out
}

impl SegmentSinkFixture for ParquetFixture {
    fn open_sink(&self) -> Box<dyn thadam_core::SegmentSink> {
        Box::new(ParquetFixture::open_sink(self))
    }

    fn failpoints(&self) -> Vec<&'static str> {
        Failpoint::ALL.iter().map(|f| f.name()).collect()
    }

    fn arm(&self, failpoint: &str) {
        match Failpoint::from_name(failpoint) {
            Some(fp) => self.fps.arm(fp),
            None => panic!("unknown failpoint {failpoint}"),
        }
    }

    fn disarm(&self) {
        self.fps.disarm();
    }

    fn finalized_ranges(&self) -> Vec<WalRange> {
        self.merged_ranges()
    }

    fn finalized_dataset_events(&self) -> Option<BTreeMap<String, usize>> {
        Some(self.dataset_rows())
    }

    fn reset(&self) {
        let path = self.dir.path().to_path_buf();
        std::fs::remove_dir_all(&path).unwrap_or_else(|e| panic!("wipe: {e}"));
        std::fs::create_dir(&path).unwrap_or_else(|e| panic!("recreate: {e}"));
    }
}
