//! The per-file Parquet machinery: one open segment per (dataset, partition),
//! and the finalize sequence of ARCHITECTURE.md section 8.
//!
//! Finalize, in order: finish the writer (the SHA-256 has been updated as
//! bytes were written, so it is complete once the footer is), fdatasync,
//! rename `staging/` to `ready/`, fsync the parent directory, append
//! `Finalized` to the catalog (fsynced). The caller then computes the new
//! watermark; the runtime commits the cursor; `Eligible` follows
//! `cursor_committed` (D18).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use thadam_core::{Timestamp, WalRange};
use sha2::{Digest, Sha256};

use crate::catalog::log as fs;
use crate::catalog::{SegmentCatalog, SegmentId, SegmentRecord};
use crate::config::{Compression, ParquetSinkConfig};
use crate::error::SegmentError;
use crate::partition::{self, Partition};

pub(crate) const THADAM_VERSION_KEY: &str = "thadam.version";
pub(crate) const WAL_RANGE_KEY: &str = "thadam.wal_range";
pub(crate) const CLOCK_SYNCED_KEY: &str = "thadam.clock_synced";
pub(crate) const CONFIG_HASH_KEY: &str = "thadam.config_hash";
pub(crate) const ENRICHMENT_HASH_KEY: &str = "thadam.enrichment_hash";
pub(crate) const SCHEMA_FP_KEY: &str = "thadam.schema_fp";

pub(crate) fn kv(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue::new(key.to_owned(), value.into())
}

fn codec(compression: Compression) -> Result<parquet::basic::Compression, SegmentError> {
    Ok(match compression {
        Compression::None => parquet::basic::Compression::UNCOMPRESSED,
        Compression::Snappy => parquet::basic::Compression::SNAPPY,
        Compression::Zstd1 => {
            let level = parquet::basic::ZstdLevel::try_new(1)
                .map_err(|e| SegmentError::catalog(format!("zstd level: {e}")))?;
            parquet::basic::Compression::ZSTD(level)
        }
        Compression::Lz4 => parquet::basic::Compression::LZ4,
    })
}

/// Writer properties shared by every file of one dataset or the quarantine:
/// codec, sizing, and the footer key-values that do not change per file
/// (`thadam.wal_range` and `thadam.clock_synced` are appended at finalize, once
/// the range is final). `created_by` is `thadam <version>`.
pub(crate) fn base_props(
    compression: Compression,
    kvs: Vec<KeyValue>,
) -> Result<WriterProperties, SegmentError> {
    Ok(WriterProperties::builder()
        .set_created_by(format!("thadam {}", env!("CARGO_PKG_VERSION")))
        .set_compression(codec(compression)?)
        // Row groups are explicitly flushed by anticipated bytes (D21);
        // keep the row-count ceiling out of the way.
        .set_max_row_group_row_count(Some(usize::MAX))
        .set_dictionary_enabled(true)
        .set_key_value_metadata(Some(kvs))
        .build())
}

/// A dataset's footer key-values (ADR-0013 "Footer key-value metadata").
pub(crate) fn dataset_kvs(
    dataset: &thadam_arrow::DatasetSchema,
    cfg: &ParquetSinkConfig,
) -> Vec<KeyValue> {
    vec![
        kv(THADAM_VERSION_KEY, env!("CARGO_PKG_VERSION")),
        kv(thadam_arrow::columns::DATASET_META_KEY, dataset.name()),
        kv(SCHEMA_FP_KEY, dataset.fingerprint_hex()),
        kv(CONFIG_HASH_KEY, cfg.config_hash.clone()),
        kv(ENRICHMENT_HASH_KEY, cfg.enrichment_hash.clone()),
    ]
}

/// [`dataset_kvs`] plus the writer properties for a dataset.
pub(crate) fn dataset_props(
    dataset: &thadam_arrow::DatasetSchema,
    cfg: &ParquetSinkConfig,
) -> Result<WriterProperties, SegmentError> {
    base_props(cfg.compression, dataset_kvs(dataset, cfg))
}

struct HashingFile {
    file: File,
    hasher: Sha256,
}

impl Write for HashingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.file.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.file.write_all(buf)?;
        self.hasher.update(buf);
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// The two failpoint hooks of the finalize sequence, packed so `finalize`
/// stays within the argument-count lint.
pub(crate) struct Hooks<B, A>
where
    B: FnOnce() -> Result<(), SegmentError>,
    A: FnOnce() -> Result<(), SegmentError>,
{
    /// Fires before the writer is closed: nothing is durable yet.
    pub(crate) before: B,
    /// Fires after rename plus directory fsync, before the catalog append.
    pub(crate) after_rename: A,
}

/// One open segment file in `staging/`.
pub(crate) struct OpenFile {
    writer: ArrowWriter<HashingFile>,
    dataset: String,
    staging: PathBuf,
    partition: Partition,
    clock_synced: bool,
    rows: u64,
    min_event: Option<Timestamp>,
    max_event: Option<Timestamp>,
}

impl OpenFile {
    /// Creates the staging file and its Parquet writer.
    pub(crate) fn create(
        root: &Path,
        seq: u64,
        dataset: &str,
        schema: SchemaRef,
        props: WriterProperties,
        partition: Partition,
        clock_synced: bool,
    ) -> Result<Self, SegmentError> {
        let staging_rel = partition::staging_rel(dataset, seq);
        let staging = root.join(&staging_rel);
        if let Some(parent) = staging.parent() {
            fs::create_dirs(parent)?;
        }
        let mut opts = OpenOptions::new();
        opts.create_new(true).write(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        let file = opts
            .open(&staging)
            .map_err(|e| SegmentError::io("create staging file", &staging, e))?;
        let hashing = HashingFile {
            file,
            hasher: Sha256::new(),
        };
        let writer = ArrowWriter::try_new(hashing, schema, Some(props))
            .map_err(|e| SegmentError::encode(dataset, e.to_string()))?;
        Ok(OpenFile {
            writer,
            dataset: dataset.to_owned(),
            staging,
            partition,
            clock_synced,
            rows: 0,
            min_event: None,
            max_event: None,
        })
    }

    /// Anticipated encoded size of the in-progress row group.
    pub(crate) fn in_progress_size(&self) -> usize {
        self.writer.in_progress_size()
    }

    /// Bytes written so far.
    pub(crate) fn bytes_written(&self) -> u64 {
        u64::try_from(self.writer.bytes_written()).unwrap_or(u64::MAX)
    }

    /// Rows accepted into the file.
    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    /// Encodes one batch, then flushes the row group once its anticipated
    /// size reached `row_group_bytes` (ADR-0013 "Writer settings").
    pub(crate) fn write_batch(
        &mut self,
        batch: &RecordBatch,
        row_group_bytes: usize,
    ) -> Result<(), SegmentError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.writer
            .write(batch)
            .map_err(|e| SegmentError::encode(&self.dataset, e.to_string()))?;
        self.rows += batch.num_rows() as u64;
        if self.writer.in_progress_size() >= row_group_bytes {
            self.writer
                .flush()
                .map_err(|e| SegmentError::encode(&self.dataset, e.to_string()))?;
        }
        Ok(())
    }

    /// The whole finalize sequence (the catalog append included, fsynced).
    /// Returns the catalog record of the segment.
    ///
    /// The two failpoint hooks bracket the durable steps: `before_finalize`
    /// before anything is durable, `after_rename` after rename plus directory
    /// fsync but before the catalog append. For a zero-row segment the caller
    /// does not build an [`OpenFile`] at all; see [`marker_record`].
    pub(crate) fn finalize<B, A>(
        mut self,
        root: &Path,
        catalog: &SegmentCatalog,
        node_id: &str,
        range: WalRange,
        bounds: (Option<Timestamp>, Option<Timestamp>),
        hooks: Hooks<B, A>,
    ) -> Result<SegmentRecord, SegmentError>
    where
        B: FnOnce() -> Result<(), SegmentError>,
        A: FnOnce() -> Result<(), SegmentError>,
    {
        self.min_event = bounds.0;
        self.max_event = bounds.1;
        (hooks.before)()?;
        for kv in [
            kv(WAL_RANGE_KEY, partition::wal_range_text(range)),
            kv(
                CLOCK_SYNCED_KEY,
                crate::catalog::record::clock_synced_text(self.clock_synced),
            ),
        ] {
            self.writer.append_key_value_metadata(kv);
        }
        // Close writes the footer; the SHA-256 then passes over the exact
        // finalized bytes, reread once (hash-as-you-write through the writer
        // is not recoverable after close: `into_inner` cannot follow
        // `finish`, and the hashing wrapper is consumed with the writer).
        self.writer
            .close()
            .map_err(|e| SegmentError::encode(&self.dataset, e.to_string()))?;
        let (bytes, checksum) = hash_file(&self.staging)?;
        fs::fsync_file(
            &std::fs::File::open(&self.staging)
                .map_err(|e| SegmentError::io("open for fdatasync", &self.staging, e))?,
        )
        .map_err(|e| SegmentError::io("fdatasync", &self.staging, e))?;

        let name = partition::segment_name(node_id, range.epoch(), range.first(), range.last());
        let ready_rel = partition::ready_rel(&self.dataset, self.partition, &name);
        let ready = root.join(&ready_rel);
        if let Some(parent) = ready.parent() {
            fs::create_dirs(parent)?;
        }
        std::fs::rename(&self.staging, &ready)
            .map_err(|e| SegmentError::io("rename", &ready, e))?;
        if let Some(parent) = ready.parent() {
            fs::fsync_dir(parent)
                .map_err(|e| SegmentError::io("fsync partition dir", parent, e))?;
        }
        (hooks.after_rename)()?;

        let record = SegmentRecord {
            // The id is the dataset-scoped relative path: file names alone
            // collide across datasets.
            id: SegmentId(ready_rel.clone()),
            dataset: self.dataset.clone(),
            path: Some(PathBuf::from(&ready_rel)),
            range,
            rows: self.rows,
            bytes,
            min_event_time: self.min_event,
            max_event_time: self.max_event,
            checksum,
            clock_synced: self.clock_synced,
        };
        catalog.append_finalized_sync(&record)?;
        Ok(record)
    }
}

fn hash_file(path: &Path) -> Result<(u64, [u8; 32]), SegmentError> {
    let mut file = File::open(path).map_err(|e| SegmentError::io("open for hashing", path, e))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 1 << 16];
    let mut total = 0u64;
    loop {
        use std::io::Read;
        let n = file
            .read(&mut buf)
            .map_err(|e| SegmentError::io("read for hashing", path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hasher.finalize().into()))
}

/// The catalog record for a segment that closed with no rows: a coverage
/// marker, so recovery can tell an empty dataset from a lost one.
pub(crate) fn marker_record(dataset: &str, range: WalRange, clock_synced: bool) -> SegmentRecord {
    SegmentRecord {
        id: SegmentId::marker(dataset, range),
        dataset: dataset.to_owned(),
        path: None,
        range,
        rows: 0,
        bytes: 0,
        min_event_time: None,
        max_event_time: None,
        checksum: [0; 32],
        clock_synced,
    }
}
