//! Positions in the capture WAL, and the batch identity derived from them.

use std::fmt;
use std::str::FromStr;

/// The generation of a WAL directory. A new epoch starts when the data
/// directory is created, so positions are only comparable within one epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct WalEpoch(u64);

impl WalEpoch {
    /// Wraps an epoch number.
    pub const fn new(epoch: u64) -> Self {
        WalEpoch(epoch)
    }

    /// The epoch number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WalEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

/// A frame sequence number within one [`WalEpoch`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct WalPos(u64);

impl WalPos {
    /// The first position.
    pub const ZERO: WalPos = WalPos(0);

    /// Wraps a frame sequence number.
    pub const fn new(pos: u64) -> Self {
        WalPos(pos)
    }

    /// The frame sequence number.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The following position, or `None` at `u64::MAX`.
    pub const fn checked_next(self) -> Option<WalPos> {
        match self.0.checked_add(1) {
            Some(next) => Some(WalPos(next)),
            None => None,
        }
    }
}

impl fmt::Display for WalPos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Why a [`WalRange`] could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("wal range is inverted: first {first} is after last {last}")]
pub struct InvertedWalRange {
    first: WalPos,
    last: WalPos,
}

/// An inclusive range of frame positions within one epoch.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WalRange {
    epoch: WalEpoch,
    first: WalPos,
    last: WalPos,
}

impl WalRange {
    /// Builds a range; `first` must not be after `last`.
    pub fn new(epoch: WalEpoch, first: WalPos, last: WalPos) -> Result<Self, InvertedWalRange> {
        if first > last {
            return Err(InvertedWalRange { first, last });
        }
        Ok(WalRange { epoch, first, last })
    }

    /// A range holding one position.
    pub fn single(epoch: WalEpoch, pos: WalPos) -> Self {
        WalRange {
            epoch,
            first: pos,
            last: pos,
        }
    }

    /// The epoch both ends belong to.
    pub fn epoch(&self) -> WalEpoch {
        self.epoch
    }

    /// The first position, inclusive.
    pub fn first(&self) -> WalPos {
        self.first
    }

    /// The last position, inclusive.
    pub fn last(&self) -> WalPos {
        self.last
    }

    /// Number of positions covered, saturating at `u64::MAX`.
    pub fn len(&self) -> u64 {
        (self.last.0 - self.first.0).saturating_add(1)
    }

    /// Always false: a range holds at least one position.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Whether `pos` lies inside the range.
    pub fn contains(&self, pos: WalPos) -> bool {
        self.first <= pos && pos <= self.last
    }

    /// The range from this range's first to `next`'s last, if `next` is in the
    /// same epoch and entirely after this range. Positions skipped between
    /// them (frames that carried nothing for this consumer) are covered.
    pub fn extended_by(&self, next: &WalRange) -> Option<WalRange> {
        (self.epoch == next.epoch && self.last < next.first).then_some(WalRange {
            epoch: self.epoch,
            first: self.first,
            last: next.last,
        })
    }
}

impl fmt::Display for WalRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.epoch, self.first, self.last)
    }
}

/// The identity of a batch handed to a [`BatchSink`](crate::BatchSink).
///
/// It is the WAL range the batch was formed from, so it is deterministic: the
/// batch driver persists the in-flight range in the cursor before sending, and
/// after a crash rebuilds exactly the same `BatchId` from that range. Every
/// retry of a batch presents the same id.
///
/// Text form, used in logs and cursor files: `{epoch:x}-{first}-{last}`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BatchId(WalRange);

impl BatchId {
    /// The id of the batch formed from `range`.
    pub fn from_range(range: WalRange) -> Self {
        BatchId(range)
    }

    /// The WAL range the batch covers.
    pub fn range(&self) -> WalRange {
        self.0
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why a [`BatchId`] string did not parse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseBatchIdError {
    /// The text is not `{epoch:x}-{first}-{last}`.
    #[error("batch id must look like `<epoch hex>-<first>-<last>`")]
    Malformed,
    /// The first position is after the last.
    #[error(transparent)]
    Inverted(#[from] InvertedWalRange),
}

impl FromStr for BatchId {
    type Err = ParseBatchIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('-');
        let (Some(epoch), Some(first), Some(last), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(ParseBatchIdError::Malformed);
        };
        let digits = |t: &str, radix| {
            if t.is_empty() || !t.bytes().all(|b| (b as char).is_digit(radix)) {
                return Err(ParseBatchIdError::Malformed);
            }
            u64::from_str_radix(t, radix).map_err(|_| ParseBatchIdError::Malformed)
        };
        let range = WalRange::new(
            WalEpoch(digits(epoch, 16)?),
            WalPos(digits(first, 10)?),
            WalPos(digits(last, 10)?),
        )?;
        Ok(BatchId(range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(e: u64, a: u64, b: u64) -> WalRange {
        WalRange::new(WalEpoch::new(e), WalPos::new(a), WalPos::new(b)).unwrap()
    }

    #[test]
    fn inverted_range_is_rejected() {
        assert!(WalRange::new(WalEpoch::new(1), WalPos::new(5), WalPos::new(4)).is_err());
    }

    #[test]
    fn range_len_and_contains() {
        let r = range(1, 10, 12);
        assert_eq!(r.len(), 3);
        assert!(r.contains(WalPos::new(10)) && r.contains(WalPos::new(12)));
        assert!(!r.contains(WalPos::new(9)) && !r.contains(WalPos::new(13)));
        assert_eq!(range(1, 0, u64::MAX).len(), u64::MAX);
    }

    #[test]
    fn extension_requires_same_epoch_and_strictly_later() {
        let a = range(1, 1, 3);
        assert_eq!(a.extended_by(&range(1, 6, 8)), Some(range(1, 1, 8)));
        assert_eq!(a.extended_by(&range(1, 3, 8)), None);
        assert_eq!(a.extended_by(&range(2, 6, 8)), None);
    }

    #[test]
    fn batch_id_text_form() {
        let id = BatchId::from_range(range(0xabc, 7, 9));
        assert_eq!(id.to_string(), "abc-7-9");
        assert_eq!("abc-7-9".parse::<BatchId>(), Ok(id));
    }

    #[test]
    fn batch_id_parse_rejects_garbage() {
        for bad in [
            "", "1-2", "1-2-3-4", "x-1-2", "1--2", "1-2-z", "1-5-4", "+1-2-3", "1-2-3 ",
        ] {
            assert!(bad.parse::<BatchId>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn checked_next_stops_at_max() {
        assert_eq!(WalPos::new(1).checked_next(), Some(WalPos::new(2)));
        assert_eq!(WalPos::new(u64::MAX).checked_next(), None);
    }
}
