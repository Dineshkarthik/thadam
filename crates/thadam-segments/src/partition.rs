//! Partition keys, on-disk paths, and segment file names (ADR-0013).
//!
//! A file never spans two partition keys: the key is the hour of the first
//! row's ingest time plus the clock-sync flag. Rows whose ingest hour differs
//! from the file's key keep their exact `_ingest_time`; readers must not
//! assume the path hour equals every row's hour.

use thadam_core::{Timestamp, WalEpoch, WalPos, WalRange};

const NANOS_PER_HOUR: i64 = 3_600_000_000_000;
const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;

/// The partition a segment's rows belong to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Partition {
    /// The clock was synchronized: `date=YYYY-MM-DD/hour=HH`.
    Synced {
        /// Days since the Unix epoch.
        days: i64,
        /// Hour within the day.
        hour: u8,
    },
    /// The clock was not synchronized: `unsynced/`.
    Unsynced,
}

impl Partition {
    /// The partition of a row ingested at `ingest` received under the
    /// clock-sync flag `clock_synced` (D16).
    pub(crate) fn of(clock_synced: bool, ingest: Timestamp) -> Partition {
        if !clock_synced {
            return Partition::Unsynced;
        }
        let nanos = ingest.as_nanos();
        let days = nanos.div_euclid(NANOS_PER_DAY);
        let hour = nanos.rem_euclid(NANOS_PER_DAY) / NANOS_PER_HOUR;
        Partition::Synced {
            days,
            hour: u8::try_from(hour).unwrap_or(0),
        }
    }

    /// The directory under `ready/` for this partition, without the dataset.
    pub(crate) fn dir_part(self) -> String {
        match self {
            Partition::Synced { days, hour } => {
                let (y, m, d) = civil_from_days(days);
                format!("date={y:04}-{m:02}-{d:02}/hour={hour:02}")
            }
            Partition::Unsynced => "unsynced".to_owned(),
        }
    }
}

/// Gregorian date of `days` days since 1970-01-01 (Howard Hinnant's
/// `civil_from_days`), so no datetime crate is needed.
pub(crate) fn civil_from_days(days: i64) -> (i64, u64, u64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u64;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u64;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `staging/<dataset>/<seq:06>.inprogress`; unique per open segment.
pub(crate) fn staging_rel(dataset: &str, seq: u64) -> String {
    format!("staging/{dataset}/{seq:06}.inprogress")
}

/// `ready/<dataset>/<partition>/<name>`.
pub(crate) fn ready_rel(dataset: &str, partition: Partition, name: &str) -> String {
    format!("ready/{dataset}/{}/{name}", partition.dir_part())
}

/// `{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet`; `last` is the
/// inclusive end of the covered WAL range.
pub(crate) fn segment_name(node_id: &str, epoch: WalEpoch, first: WalPos, last: WalPos) -> String {
    format!(
        "{node_id}_{:016x}_{:020}_{:020}.parquet",
        epoch.get(),
        first.get(),
        last.get()
    )
}

/// The identity a segment name encodes, node id included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SegmentName {
    /// The node that wrote the file.
    pub(crate) node_id: String,
    /// The WAL epoch.
    pub(crate) epoch: WalEpoch,
    /// First covered WAL position, inclusive.
    pub(crate) first: WalPos,
    /// Last covered WAL position, inclusive.
    pub(crate) last: WalPos,
}

/// Parses a name written by [`segment_name`]. Splits from the right, so node
/// ids may contain underscores. Returns `None` for files that are not Thadam
/// segments (foreign files are left alone).
pub(crate) fn parse_segment_name(name: &str) -> Option<SegmentName> {
    let stem = name.strip_suffix(".parquet")?;
    let (rest, last) = stem.rsplit_once('_')?;
    let (rest, first) = rest.rsplit_once('_')?;
    let (node_id, epoch) = rest.rsplit_once('_')?;
    if node_id.is_empty() {
        return None;
    }
    Some(SegmentName {
        node_id: node_id.to_owned(),
        epoch: WalEpoch::new(u64::from_str_radix(epoch, 16).ok()?),
        first: WalPos::new(first.parse().ok()?),
        last: WalPos::new(last.parse().ok()?),
    })
}

/// A dataset name must be a single path component: no separators, not a
/// parent reference.
pub(crate) fn dataset_component_ok(dataset: &str) -> bool {
    !dataset.is_empty()
        && dataset != "."
        && dataset != ".."
        && !dataset.contains('/')
        && !dataset.contains('\\')
}

/// A node id must be a single safe file-name component.
pub(crate) fn node_component_ok(node_id: &str) -> bool {
    !node_id.is_empty()
        && !node_id.contains('/')
        && !node_id.contains('\\')
        && node_id != "."
        && node_id != ".."
}

/// Formats a WAL range as `{epoch:x}-{first}-{last}` for `thadam.wal_range`.
pub(crate) fn wal_range_text(range: WalRange) -> String {
    format!(
        "{:x}-{}-{}",
        range.epoch().get(),
        range.first().get(),
        range.last().get()
    )
}

/// Parses the [`wal_range_text`] format.
pub(crate) fn parse_wal_range(text: &str) -> Option<WalRange> {
    let (epoch, rest) = text.split_once('-')?;
    let (first, last) = rest.split_once('-')?;
    WalRange::new(
        WalEpoch::new(u64::from_str_radix(epoch, 16).ok()?),
        WalPos::new(first.parse().ok()?),
        WalPos::new(last.parse().ok()?),
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_days_matches_known_dates() {
        // Anchors verified against `date -u -r $((days*86400)) +%F`.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_521), (2026, 3, 9));
        assert_eq!(civil_from_days(20_389), (2025, 10, 28));
        assert_eq!(civil_from_days(20_734), (2026, 10, 8));
        assert_eq!(civil_from_days(20_743), (2026, 10, 17));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        // Leap handling: 2000 is leap, 2100 is not.
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(11_017), (2000, 3, 1));
        assert_eq!(civil_from_days(47_481), (2099, 12, 31));
        assert_eq!(civil_from_days(47_482), (2100, 1, 1));
    }

    #[test]
    fn partition_dirs_follow_ingest_hour_and_sync_flag() {
        // 2026-10-17T13:30:00Z
        let t =
            Timestamp::from_nanos(20_743 * NANOS_PER_DAY + 13 * NANOS_PER_HOUR + 1_800_000_000_000);
        let p = Partition::of(true, t);
        assert_eq!(p.dir_part(), "date=2026-10-17/hour=13");
        assert_eq!(Partition::of(false, t), Partition::Unsynced);
        assert_eq!(Partition::of(false, t).dir_part(), "unsynced");
        // The hour is taken floor-wise, near boundaries.
        let edge = Timestamp::from_nanos(14 * NANOS_PER_HOUR - 1);
        assert_eq!(
            Partition::of(true, edge).dir_part(),
            "date=1970-01-01/hour=13"
        );
    }

    #[test]
    fn segment_names_round_trip() {
        let name = segment_name(
            "edge01",
            WalEpoch::new(0xab),
            WalPos::new(3),
            WalPos::new(9),
        );
        assert_eq!(
            name,
            "edge01_00000000000000ab_00000000000000000003_00000000000000000009.parquet"
        );
        let parsed = parse_segment_name(&name).expect("parses");
        assert_eq!(parsed.node_id, "edge01");
        assert_eq!(parsed.epoch.get(), 0xab);
        assert_eq!(parsed.first, WalPos::new(3));
        assert_eq!(parsed.last, WalPos::new(9));
        assert!(parse_segment_name("notes.txt").is_none());
        assert!(parse_segment_name("_ab_3_9.parquet").is_none());
        // Node ids may contain underscores: parsing splits from the right.
        let parsed = parse_segment_name("edge_01_ab_3_9.parquet").expect("parses");
        assert_eq!(parsed.node_id, "edge_01");
        assert!(node_component_ok("edge01"));
        assert!(node_component_ok("edge_01"));
        assert!(!node_component_ok(""));
        assert!(dataset_component_ok("telemetry"));
        assert!(!dataset_component_ok("a/b"));
        assert!(!dataset_component_ok(".."));
    }

    #[test]
    fn wal_ranges_round_trip() {
        for (epoch, first, last) in [(1u64, 0u64, 0u64), (0xdead_beef, 3, 9), (u64::MAX, 5, 5)] {
            let r = WalRange::new(WalEpoch::new(epoch), WalPos::new(first), WalPos::new(last))
                .expect("range");
            assert_eq!(parse_wal_range(&wal_range_text(r)), Some(r));
        }
        assert!(parse_wal_range("1-2").is_none());
        assert!(parse_wal_range("x-2-3").is_none());
        assert!(parse_wal_range("1-9-3").is_none());
    }
}
