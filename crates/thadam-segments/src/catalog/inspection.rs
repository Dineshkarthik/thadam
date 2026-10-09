//! Read-only inspection of a sink's catalog.
//!
//! Unlike [`SegmentCatalog::open`](super::SegmentCatalog::open), nothing here
//! creates, truncates, or deletes files: the log is replayed into a private
//! in-memory state. Safe to call while the sink's writer is running. Files the
//! catalog has not logged yet (a crash between rename and append) are not
//! listed; startup recovery adopts them.

use std::path::{Path, PathBuf};

use thadam_core::WalRange;

use super::record::SegmentState;
use super::{CatalogState, SegmentTotals, apply_ref, log, totals_of};
use crate::error::SegmentError;

/// Where a file-bearing segment sits in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SegmentStatus {
    /// Finalized locally, not yet ship-eligible.
    Finalized,
    /// The WAL cursor passed the segment's end.
    Eligible,
    /// Uploaded, still on local disk.
    Uploaded,
    /// Removed locally after upload; the tombstone keeps the record.
    Deleted,
}

impl SegmentStatus {
    /// The state name used in human and JSON output.
    pub fn as_str(self) -> &'static str {
        match self {
            SegmentStatus::Finalized => "finalized",
            SegmentStatus::Eligible => "eligible",
            SegmentStatus::Uploaded => "uploaded",
            SegmentStatus::Deleted => "deleted",
        }
    }
}

/// One segment that has (or had) a Parquet file.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SegmentFile {
    /// The segment id.
    pub id: String,
    /// The dataset the rows belong to.
    pub dataset: String,
    /// The segment's state.
    pub status: SegmentStatus,
    /// The file, relative to the sink's directory.
    pub path: PathBuf,
    /// The inclusive WAL range the segment covers.
    pub range: WalRange,
    /// Rows written.
    pub rows: u64,
    /// File size in bytes.
    pub bytes: u64,
}

/// A read-only snapshot of a sink's catalog.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CatalogInspection {
    /// Per-state file and byte totals.
    pub totals: SegmentTotals,
    /// Every segment with a file, newest WAL range first.
    pub files: Vec<SegmentFile>,
}

/// Inspects the catalog under the sink directory `root`. A sink that has
/// never written reports an empty inspection, and nothing is created.
pub fn inspect(root: &Path) -> Result<CatalogInspection, SegmentError> {
    let log_path = root.join("catalog.log");
    if !log_path.is_file() {
        return Ok(CatalogInspection::default());
    }
    let replayed = log::replay(&log_path)?;
    let mut state = CatalogState::default();
    for record in &replayed.records {
        apply_ref(&mut state, record);
    }
    let totals = totals_of(&state);
    let mut files: Vec<SegmentFile> = state
        .segments
        .values()
        .filter_map(|entry| {
            let path = entry.record.path.clone()?;
            let status = match entry.state {
                SegmentState::Finalized => SegmentStatus::Finalized,
                SegmentState::Eligible { .. } => SegmentStatus::Eligible,
                SegmentState::Uploaded { .. } => SegmentStatus::Uploaded,
                SegmentState::Deleted { .. } => SegmentStatus::Deleted,
            };
            Some(SegmentFile {
                id: entry.record.id.0.clone(),
                dataset: entry.record.dataset.clone(),
                status,
                path,
                range: entry.record.range,
                rows: entry.record.rows,
                bytes: entry.record.bytes,
            })
        })
        .collect();
    files.sort_by(|a, b| {
        b.range
            .last()
            .cmp(&a.range.last())
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(CatalogInspection { totals, files })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use thadam_core::{WalEpoch, WalPos, WalRange};

    use super::*;
    use crate::catalog::log::LogWriter;
    use crate::catalog::record::{CatalogRecord, DeletedRecord, EligibleRecord, UploadedRecord};
    use crate::catalog::{SegmentId, SegmentRecord};

    fn range(first: u64, last: u64) -> WalRange {
        WalRange::new(WalEpoch::new(7), WalPos::new(first), WalPos::new(last)).expect("valid range")
    }

    fn finalized(id: &str, first: u64, last: u64, bytes: u64, path: Option<&str>) -> CatalogRecord {
        CatalogRecord::from(&SegmentRecord::new(
            SegmentId(id.to_owned()),
            "d",
            path.map(PathBuf::from),
            range(first, last),
            if path.is_some() { 3 } else { 0 },
            bytes,
            None,
            None,
            [0; 32],
            true,
        ))
    }

    fn write_log(root: &Path, records: &[CatalogRecord]) {
        let mut writer = LogWriter::create(&root.join("catalog.log")).expect("create log");
        writer.append(records).expect("append");
    }

    #[test]
    fn a_missing_catalog_is_empty_and_creates_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let out = inspect(dir.path()).expect("inspect");
        assert_eq!(out, CatalogInspection::default());
        assert_eq!(std::fs::read_dir(dir.path()).expect("read dir").count(), 0);
    }

    #[test]
    fn states_are_counted_and_files_listed_newest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_log(
            dir.path(),
            &[
                finalized("a", 1, 10, 100, Some("ready/a.parquet")),
                finalized("b", 11, 20, 200, Some("ready/b.parquet")),
                CatalogRecord::Eligible(EligibleRecord {
                    id: "b".into(),
                    pos: 20,
                }),
                CatalogRecord::Uploaded(UploadedRecord {
                    id: "b".into(),
                    key: "k/b".into(),
                    etag: None,
                    uploaded_unix: 1,
                }),
                finalized("c", 21, 30, 300, Some("ready/c.parquet")),
                CatalogRecord::Eligible(EligibleRecord {
                    id: "c".into(),
                    pos: 30,
                }),
                CatalogRecord::Uploaded(UploadedRecord {
                    id: "c".into(),
                    key: "k/c".into(),
                    etag: None,
                    uploaded_unix: 1,
                }),
                CatalogRecord::Deleted(DeletedRecord {
                    id: "c".into(),
                    uploaded: true,
                    deleted_unix: 2,
                }),
                finalized("marker", 31, 31, 0, None),
            ],
        );

        let out = inspect(dir.path()).expect("inspect");
        assert_eq!(out.totals.finalized.files, 1);
        assert_eq!(out.totals.finalized.bytes, 100);
        assert_eq!(out.totals.eligible.files, 0);
        assert_eq!(out.totals.uploaded.files, 1);
        assert_eq!(out.totals.uploaded.bytes, 200);
        assert_eq!(out.totals.deleted.files, 1);
        assert_eq!(out.totals.deleted.bytes, 300);

        let ids: Vec<_> = out.files.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, ["c", "b", "a"]);
        assert_eq!(out.files[0].status, SegmentStatus::Deleted);
        assert_eq!(out.files[1].status, SegmentStatus::Uploaded);
        assert_eq!(out.files[2].status, SegmentStatus::Finalized);
        assert_eq!(out.files[2].path, PathBuf::from("ready/a.parquet"));
        assert_eq!(out.files[2].rows, 3);
    }

    #[test]
    fn inspection_does_not_modify_the_log() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_log(
            dir.path(),
            &[finalized("a", 1, 2, 10, Some("ready/a.parquet"))],
        );
        let log_path = dir.path().join("catalog.log");
        let before = std::fs::read(&log_path).expect("read log");
        inspect(dir.path()).expect("inspect");
        assert_eq!(std::fs::read(&log_path).expect("read log"), before);
    }
}
