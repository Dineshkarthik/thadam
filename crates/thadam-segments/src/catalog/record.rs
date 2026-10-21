//! Records of the segment catalog log and the shipper-facing segment view.
//!
//! On disk the log is a sequence of length-prefixed, CRC-checked JSON frames
//! (see `super::log`). [`CatalogRecord::Finalized`] with no path is a
//! *coverage marker*: a dataset that had no rows in a WAL range still
//! records that the range was processed, so recovery can tell an empty
//! dataset from a lost one without writing footer-only Parquet files.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thadam_core::{Timestamp, WalPos, WalRange};

/// A segment identifier: the Parquet file name for real files, or
/// `empty.{dataset}.{epoch:016x}.{first:020}.{last:020}` for coverage
/// markers.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub String);

impl SegmentId {
    /// A coverage-marker id (unique per dataset and range, never a file).
    pub(crate) fn marker(dataset: &str, range: WalRange) -> Self {
        SegmentId(format!(
            "empty.{dataset}.{:016x}.{:020}.{:020}",
            range.epoch().get(),
            range.first().get(),
            range.last().get()
        ))
    }

    /// The raw id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The shipper-facing view of one segment (implementation plan 5.7).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SegmentRecord {
    /// The segment id.
    pub id: SegmentId,
    /// The dataset the rows belong to.
    pub dataset: String,
    /// The Parquet file, relative to the sink's directory, or `None` for a
    /// coverage marker (empty segment: nothing was uploaded, ever).
    pub path: Option<PathBuf>,
    /// The inclusive WAL range the segment covers.
    pub range: WalRange,
    /// Rows written (`0` for coverage markers).
    pub rows: u64,
    /// File size in bytes (`0` for markers).
    pub bytes: u64,
    /// Minimum `_event_time` in the file. Known at finalize; lost when the
    /// catalog entry was rebuilt from a Parquet footer after a crash.
    pub min_event_time: Option<Timestamp>,
    /// Maximum `_event_time`, see [`SegmentRecord::min_event_time`].
    pub max_event_time: Option<Timestamp>,
    /// SHA-256 of the file (all zeros for markers).
    pub checksum: [u8; 32],
    /// The `clock_synced` flag the rows were received under (D16).
    pub clock_synced: bool,
}

impl SegmentRecord {
    /// A record with every field set. Cross-crate consumers (the shipper's
    /// tests, later drivers) cannot build a `#[non_exhaustive]` type with a
    /// struct literal.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: SegmentId,
        dataset: impl Into<String>,
        path: Option<PathBuf>,
        range: WalRange,
        rows: u64,
        bytes: u64,
        min_event_time: Option<Timestamp>,
        max_event_time: Option<Timestamp>,
        checksum: [u8; 32],
        clock_synced: bool,
    ) -> Self {
        SegmentRecord {
            id,
            dataset: dataset.into(),
            path,
            range,
            rows,
            bytes,
            min_event_time,
            max_event_time,
            checksum,
            clock_synced,
        }
    }

    /// Whether the segment has a real file (coverage markers do not).
    pub fn has_file(&self) -> bool {
        self.path.is_some()
    }
}

pub(crate) fn clock_synced_text(synced: bool) -> &'static str {
    if synced { "true" } else { "false" }
}

/// Ship/upload bookkeeping attached to a segment over its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentState {
    /// Finalized locally, not yet ship-eligible (D18 step 1).
    Finalized,
    /// The WAL cursor passed the segment's end (D18 step 3).
    Eligible {
        /// The cursor position that made the segment eligible.
        pos: WalPos,
    },
    /// Uploaded (`object_key`, `etag`, upload time in unix seconds).
    Uploaded {
        /// When the upload completed, unix seconds.
        uploaded_unix: u64,
    },
    /// Removed locally. `uploaded` is true only when the file was shipped
    /// first; records dropped by recovery before ever being eligible have
    /// `uploaded: false` and contribute nothing to coverage.
    Deleted {
        /// Whether the segment had been uploaded before deletion.
        uploaded: bool,
    },
}

/// One append to `catalog.log`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum CatalogRecord {
    /// A segment file was finalized (or a coverage marker recorded).
    Finalized(FinalizedRecord),
    /// The WAL cursor passed a segment's end (D18 step 3).
    Eligible(EligibleRecord),
    /// The shipper uploaded the segment's file.
    Uploaded(UploadedRecord),
    /// The segment's file was removed locally (or a non-eligible record was
    /// dropped by recovery).
    Deleted(DeletedRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalizedRecord {
    pub id: String,
    pub dataset: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    pub epoch: u64,
    pub first: u64,
    pub last: u64,
    pub rows: u64,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_event_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_event_nanos: Option<i64>,
    pub clock_synced: bool,
}

impl From<&SegmentRecord> for CatalogRecord {
    fn from(r: &SegmentRecord) -> Self {
        CatalogRecord::Finalized(FinalizedRecord {
            id: r.id.0.clone(),
            dataset: r.dataset.clone(),
            path: r.path.clone(),
            epoch: r.range.epoch().get(),
            first: r.range.first().get(),
            last: r.range.last().get(),
            rows: r.rows,
            bytes: r.bytes,
            sha256: if r.checksum == [0; 32] {
                String::new()
            } else {
                hex(&r.checksum)
            },
            min_event_nanos: r.min_event_time.map(Timestamp::as_nanos),
            max_event_nanos: r.max_event_time.map(Timestamp::as_nanos),
            clock_synced: r.clock_synced,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EligibleRecord {
    pub id: String,
    /// The cursor position that made the segment eligible.
    pub pos: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UploadedRecord {
    pub id: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    pub uploaded_unix: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeletedRecord {
    pub id: String,
    pub uploaded: bool,
    pub deleted_unix: u64,
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

pub(crate) fn unhex(text: &str) -> Option<[u8; 32]> {
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    let nibble = |b: u8| -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    };
    let mut out = [0u8; 32];
    for (i, &[a, b]) in bytes.as_chunks::<2>().0.iter().enumerate() {
        out[i] = (nibble(a)? << 4) | nibble(b)?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::WalEpoch;

    #[test]
    fn segment_record_to_log_record_round_trip_shape() {
        let range = WalRange::new(WalEpoch::new(7), WalPos::new(1), WalPos::new(9)).unwrap();
        let rec = SegmentRecord {
            id: SegmentId(
                "edge01_0000000000000007_00000000000000000001_00000000000000000009.parquet".into(),
            ),
            dataset: "telemetry".into(),
            path: Some(PathBuf::from(
                "ready/telemetry/date=2026-10-08/hour=13/x.parquet",
            )),
            range,
            rows: 3,
            bytes: 100,
            min_event_time: Some(Timestamp::from_nanos(5)),
            max_event_time: Some(Timestamp::from_nanos(50)),
            checksum: unhex(&"ab".repeat(32)).unwrap(),
            clock_synced: true,
        };
        let json = match CatalogRecord::from(&rec) {
            CatalogRecord::Finalized(f) => serde_json::to_value(&f).unwrap(),
            _ => panic!(),
        };
        assert_eq!(json["sha256"], serde_json::json!("ab".repeat(32)));
        assert_eq!(json["epoch"], serde_json::json!(7));
        assert_eq!(json["min_event_nanos"], serde_json::json!(5));

        let mut marker = rec.clone();
        marker.path = None;
        marker.bytes = 0;
        marker.rows = 0;
        marker.checksum = [0; 32];
        let json = match CatalogRecord::from(&marker) {
            CatalogRecord::Finalized(f) => serde_json::to_value(&f).unwrap(),
            _ => panic!(),
        };
        assert_eq!(json.get("path"), None);
        assert_eq!(json.get("sha256"), None);
    }

    #[test]
    fn hex_round_trip() {
        assert_eq!(unhex(&hex(&[1, 2, 3, 250])), None, "length is enforced");
        let bytes = [0xabu8; 32];
        assert_eq!(hex(&bytes), "ab".repeat(32));
        assert_eq!(unhex(&hex(&bytes)), Some(bytes));
        assert_eq!(unhex("zz"), None);
    }

    #[test]
    fn marker_ids_are_unique_and_never_file_names() {
        let r1 = WalRange::new(WalEpoch::new(1), WalPos::new(1), WalPos::new(2)).unwrap();
        let r2 = WalRange::new(WalEpoch::new(1), WalPos::new(3), WalPos::new(4)).unwrap();
        assert_ne!(SegmentId::marker("a", r1), SegmentId::marker("a", r2));
        assert_ne!(SegmentId::marker("a", r1), SegmentId::marker("b", r1));
        assert!(!SegmentId::marker("a", r1).as_str().ends_with(".parquet"));
    }
}
