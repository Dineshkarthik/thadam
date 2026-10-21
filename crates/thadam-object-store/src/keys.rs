//! Object-key layout (ADR-0013 "Paths, names, and keys") and put metadata.
//!
//! A finalized segment at
//! `ready/<dataset>/date=YYYY-MM-DD/hour=HH/{node}_{epoch:016x}_{first:020}_{last:020}.parquet`
//! ships under
//! `{prefix}/thadam/v1/{pipeline}/{dataset}/date=.../hour=.../{node}_{epoch:016x}_{first:020}_{last:020}.parquet`:
//! the `ready/` component becomes `thadam/v1/{pipeline}` and nothing else
//! changes, which is what makes the key deterministic after a restart.

use std::borrow::Cow;

use object_store::Attributes;
use object_store::path::Path;
use thadam_core::WalRange;
use thadam_segments::SegmentRecord;

/// Deterministic layout version 1 (ADR-0013): `thadam/v1/`.
const KEY_ROOT: &str = "thadam/v1";

/// `x-amz-meta` keys the shipper writes; the sha-256 is how a conflict is
/// told from a collision.
pub(crate) const SHA256_ATTR: &str = "thadam-sha256";
pub(crate) const WAL_RANGE_ATTR: &str = "thadam-wal-range";
const DATASET_ATTR: &str = "thadam-dataset";
const NODE_ATTR: &str = "thadam-node";

/// The [`Attribute`] key for `thadam-sha256` user metadata.
pub(crate) fn sha256_attr() -> object_store::Attribute {
    object_store::Attribute::Metadata(Cow::Borrowed(SHA256_ATTR))
}

/// The store-relative object key:
/// `thadam/v1/{pipeline}/{dataset}/{partition...}/{name}.parquet`.
///
/// `None` when the record has no file or its path does not match the
/// `ready/<dataset>/...` shape (a bug in the upstream segment sink).
pub(crate) fn object_key(pipeline: &str, record: &SegmentRecord) -> Option<String> {
    let path = record.path.as_ref()?;
    let parts: Vec<String> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().replace('\\', "/"))
        .collect();
    let rest = match parts.as_slice() {
        [ready, dataset, rest @ ..]
            if ready == "ready" && *dataset == record.dataset && !rest.is_empty() =>
        {
            rest.join("/")
        }
        _ => return None,
    };
    Some(format!("{KEY_ROOT}/{pipeline}/{}/{rest}", record.dataset))
}

/// The key as an operator sees it: with the bucket prefix from `url`.
pub(crate) fn display_key(prefix: &str, store_key: &str) -> String {
    if prefix.is_empty() {
        store_key.to_owned()
    } else {
        format!("{prefix}/{store_key}")
    }
}

/// A store path from a store-relative key.
pub(crate) fn store_path(store_key: &str) -> Path {
    Path::from(store_key)
}

/// `{epoch:x}-{first}-{last}`, replicating `thadam.wal_range` for metadata.
pub(crate) fn wal_range_text(range: WalRange) -> String {
    format!(
        "{:x}-{}-{}",
        range.epoch().get(),
        range.first().get(),
        range.last().get()
    )
}

/// The user metadata written with every put: the SHA-256 of the file (how a
/// crash-recovery conflict is told from a collision), the covered WAL range,
/// the dataset, and the writing node.
pub(crate) fn put_attributes(
    record: &SegmentRecord,
    sha256_hex: &str,
    node_id: &str,
) -> Attributes {
    [
        (
            sha256_attr(),
            object_store::AttributeValue::from(sha256_hex.to_owned()),
        ),
        (
            meta(WAL_RANGE_ATTR),
            object_store::AttributeValue::from(wal_range_text(record.range)),
        ),
        (
            meta(DATASET_ATTR),
            object_store::AttributeValue::from(record.dataset.clone()),
        ),
        (
            meta(NODE_ATTR),
            object_store::AttributeValue::from(node_id.to_owned()),
        ),
    ]
    .into_iter()
    .collect()
}

fn meta(name: &'static str) -> object_store::Attribute {
    object_store::Attribute::Metadata(Cow::Borrowed(name))
}

/// Whether an existing object is the same segment (the crash-recovery path)
/// or different content (the Fatal collision).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Verification {
    /// Size (and SHA-256 where verifiable) match: uploading already
    /// succeeded; just mark the segment uploaded.
    SameContent,
    /// Something differs: Fatal `sink.object_key_collision`.
    Differs(String),
}

/// Compares the remote HEAD/attribute facts with what the segment says.
/// `metadata_supported` comes from the startup probe: on a store that drops
/// user metadata, size is all that can be checked.
pub(crate) fn verify_remote(
    size: u64,
    remote_sha: Option<&str>,
    local_bytes: u64,
    local_sha_hex: &str,
    metadata_supported: bool,
) -> Verification {
    if size != local_bytes {
        return Verification::Differs(format!(
            "remote size {size} differs from the local {local_bytes}"
        ));
    }
    if !metadata_supported {
        return Verification::SameContent;
    }
    match remote_sha {
        Some(remote) if remote == local_sha_hex => Verification::SameContent,
        Some(remote) => Verification::Differs(format!(
            "remote thadam-sha256 {remote} != local {local_sha_hex}"
        )),
        None => Verification::Differs(
            "remote object has no thadam-sha256 metadata; it was not written by this shipper"
                .to_owned(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use thadam_core::{Timestamp, WalEpoch, WalPos, WalRange};
    use thadam_segments::SegmentId;

    fn record(path: &str, dataset: &str) -> SegmentRecord {
        SegmentRecord::new(
            SegmentId(path.to_owned()),
            dataset,
            Some(PathBuf::from(path)),
            WalRange::new(WalEpoch::new(0xab), WalPos::new(3), WalPos::new(9)).unwrap(),
            7,
            123,
            Some(Timestamp::from_nanos(5)),
            Some(Timestamp::from_nanos(50)),
            [0xabu8; 32],
            true,
        )
    }

    #[test]
    fn key_layout_follows_adr_0013() {
        let r = record(
            "ready/telemetry/date=2026-10-08/hour=13/edge01_00000000000000ab_00000000000000000003_00000000000000000009.parquet",
            "telemetry",
        );
        let key = object_key("edge-capture", &r).unwrap();
        assert_eq!(
            key,
            "thadam/v1/edge-capture/telemetry/date=2026-10-08/hour=13/edge01_00000000000000ab_00000000000000000003_00000000000000000009.parquet"
        );
        assert_eq!(display_key("fleet", &key), format!("fleet/{key}"));
        assert_eq!(display_key("", &key), key);
    }

    #[test]
    fn key_layout_for_unsynced_partition() {
        let r = record(
            "ready/joints/unsynced/n_00000000000000ab_00000000000000000003_00000000000000000009.parquet",
            "joints",
        );
        assert_eq!(
            object_key("p", &r).unwrap(),
            "thadam/v1/p/joints/unsynced/n_00000000000000ab_00000000000000000003_00000000000000000009.parquet"
        );
    }

    #[test]
    fn distinct_node_ids_yield_distinct_keys() {
        let key = |node: &str| {
            let r = record(
                &format!(
                    "ready/telemetry/date=2026-10-08/hour=13/{node}_00000000000000ab_00000000000000000003_00000000000000000009.parquet"
                ),
                "telemetry",
            );
            object_key("edge-capture", &r).unwrap()
        };
        assert_ne!(key("edge-7"), key("edge-8"));
        assert!(key("edge-7").ends_with(
            "/edge-7_00000000000000ab_00000000000000000003_00000000000000000009.parquet"
        ));
    }

    #[test]
    fn foreign_shapes_have_no_key() {
        let mut r = record(
            "ready/telemetry/date=2026-10-08/hour=13/a_00000000000000ab_00000000000000000003_00000000000000000009.parquet",
            "other-dataset",
        );
        assert!(object_key("p", &r).is_none(), "path/dataset mismatch");
        r.path = None;
        r.dataset = "telemetry".into();
        assert!(object_key("p", &r).is_none(), "coverage marker");
    }

    #[test]
    fn verification_matrix() {
        assert_eq!(
            verify_remote(10, Some("aa"), 10, "aa", true),
            Verification::SameContent
        );
        assert!(matches!(
            verify_remote(9, Some("aa"), 10, "aa", true),
            Verification::Differs(_)
        ));
        assert!(matches!(
            verify_remote(10, Some("bb"), 10, "aa", true),
            Verification::Differs(_)
        ));
        assert!(matches!(
            verify_remote(10, None, 10, "aa", true),
            Verification::Differs(_)
        ));
        assert_eq!(
            verify_remote(10, None, 10, "aa", false),
            Verification::SameContent,
            "metadata-less stores verify on size alone"
        );
    }

    #[test]
    fn wal_range_metadata_format() {
        let r = WalRange::new(WalEpoch::new(0xab), WalPos::new(3), WalPos::new(9)).unwrap();
        assert_eq!(wal_range_text(r), "ab-3-9");
    }
}
