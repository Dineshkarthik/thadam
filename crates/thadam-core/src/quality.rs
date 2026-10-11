//! Event-level data quality (ADR-0001; field-level quality is deferred).

use std::fmt;

use bitflags::bitflags;

use crate::text::Sym;

/// The three-level quality status. Discriminants are stable column values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[non_exhaustive]
#[repr(u8)]
pub enum QualityStatus {
    /// The data can be trusted.
    #[default]
    Good = 0,
    /// The data may be wrong.
    Uncertain = 1,
    /// The data is known to be wrong.
    Bad = 2,
}

impl QualityStatus {
    /// The stable lowercase name used in columns and filters.
    pub const fn as_str(self) -> &'static str {
        match self {
            QualityStatus::Good => "good",
            QualityStatus::Uncertain => "uncertain",
            QualityStatus::Bad => "bad",
        }
    }
}

impl fmt::Display for QualityStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

bitflags! {
    /// The quality flag bitset stored in `_quality_flags`.
    ///
    /// Bit positions are never reassigned; new flags take new bits.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
    pub struct QualityFlags: u32 {
        /// A value is outside its declared range.
        const OUT_OF_RANGE = 1 << 0;
        /// The device clock disagrees with the ingest clock.
        const CLOCK_SKEW = 1 << 1;
        /// The message carried no usable time.
        const MISSING_TIME = 1 << 2;
        /// The event arrived out of order.
        const OUT_OF_ORDER = 1 << 3;
        /// The event duplicates an earlier one.
        const DUPLICATE = 1 << 4;
        /// The value is older than its freshness bound.
        const STALE = 1 << 5;
        /// A gap in the source sequence precedes this event.
        const SEQUENCE_GAP = 1 << 6;
        /// The payload was only partly decoded.
        const DECODE_ERROR = 1 << 7;
        /// A validation step found the data itself unusable: a value that is
        /// missing, of the wrong kind, not finite, or inconsistent with its
        /// siblings (product.md section 22's `invalid`).
        const INVALID = 1 << 8;
    }
}

/// Event quality: a status, the flag bitset, and an optional short reason.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Quality {
    /// Overall status.
    pub status: QualityStatus,
    /// Flags raised against the event.
    pub flags: QualityFlags,
    /// Optional short reason, stored as a column.
    pub reason: Option<Sym>,
}

impl Quality {
    /// Good quality with no flags and no reason.
    pub fn good() -> Self {
        Quality::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_quality_is_good_and_clean() {
        let q = Quality::default();
        assert_eq!(q.status, QualityStatus::Good);
        assert!(q.flags.is_empty());
        assert!(q.reason.is_none());
    }

    #[test]
    fn flag_registry_snapshot() {
        let text: String = QualityFlags::all()
            .iter_names()
            .map(|(name, flag)| format!("{name} = {:#06x}\n", flag.bits()))
            .collect();
        insta::assert_snapshot!(text);
    }

    #[test]
    fn status_discriminants_are_stable() {
        assert_eq!(QualityStatus::Good as u8, 0);
        assert_eq!(QualityStatus::Uncertain as u8, 1);
        assert_eq!(QualityStatus::Bad as u8, 2);
        assert!(QualityStatus::Good < QualityStatus::Bad);
    }
}
