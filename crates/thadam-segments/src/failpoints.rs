//! Crash failpoints in the finalize sequence, armed from tests.
//!
//! The names follow the crash-between-steps matrix of ADR-0011 and PR14:
//! `segments::before_finalize` (nothing of the segment is durable yet),
//! `segments::after_rename_before_catalog` (the file is renamed and fsynced
//! but not yet catalogued; recovery rebuilds it from the footer), and
//! `segments::before_cursor_commit` (the catalog append is fsynced but the
//! watermark was not reported; recovery replays the catalog).
//!
//! Compiled in for this crate's own unit tests, and for other crates under
//! the `failpoints` feature.

use std::sync::{Arc, Mutex};

use crate::error::SegmentError;

/// A point in the finalize sequence where an armed sink crashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Failpoint {
    /// Before the Parquet writer is finished: nothing about the segment is
    /// durable.
    BeforeFinalize,
    /// After the file was renamed into `ready/` and the directory fsynced,
    /// before the `Finalized` catalog record is appended.
    AfterRenameBeforeCatalog,
    /// After the `Finalized` record is fsynced in the catalog, before the new
    /// watermark is returned to the caller of `poll_commit`/`finalize_all`.
    BeforeCursorCommit,
}

impl Failpoint {
    /// Every failpoint, in finalize order.
    #[cfg(any(test, feature = "failpoints"))]
    pub const ALL: [Failpoint; 3] = [
        Failpoint::BeforeFinalize,
        Failpoint::AfterRenameBeforeCatalog,
        Failpoint::BeforeCursorCommit,
    ];

    /// The stable failpoint name.
    #[cfg(any(test, feature = "failpoints"))]
    pub const fn name(self) -> &'static str {
        match self {
            Failpoint::BeforeFinalize => "segments::before_finalize",
            Failpoint::AfterRenameBeforeCatalog => "segments::after_rename_before_catalog",
            Failpoint::BeforeCursorCommit => "segments::before_cursor_commit",
        }
    }

    /// Looks a failpoint up by name.
    #[cfg(any(test, feature = "failpoints"))]
    pub fn from_name(name: &str) -> Option<Failpoint> {
        Self::ALL.into_iter().find(|f| f.name() == name)
    }
}

/// The arming registry shared by a test fixture and the sinks it opens.
#[derive(Clone, Debug, Default)]
pub struct Failpoints {
    armed: Arc<Mutex<Option<Failpoint>>>,
}

impl Failpoints {
    /// Arms a failpoint: it fires once, at the next finalize that reaches it.
    #[cfg(any(test, feature = "failpoints"))]
    pub fn arm(&self, failpoint: Failpoint) {
        *self.armed.lock().unwrap_or_else(|e| e.into_inner()) = Some(failpoint);
    }

    /// Disarms any failpoint that did not fire.
    #[cfg(any(test, feature = "failpoints"))]
    pub fn disarm(&self) {
        *self.armed.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Fires the armed failpoint when it is `failpoint`, consuming the arm.
    pub(crate) fn hit(&self, failpoint: Failpoint) -> Result<(), SegmentError> {
        let mut armed = self.armed.lock().unwrap_or_else(|e| e.into_inner());
        if *armed == Some(failpoint) {
            *armed = None;
            return Err(SegmentError::State("crashed at a finalize failpoint"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_arm_fires_once() {
        for fp in Failpoint::ALL {
            assert_eq!(Failpoint::from_name(fp.name()), Some(fp));
        }
        assert_eq!(Failpoint::from_name("segments::nope"), None);

        let fps = Failpoints::default();
        fps.arm(Failpoint::BeforeFinalize);
        assert!(fps.hit(Failpoint::AfterRenameBeforeCatalog).is_ok());
        assert!(fps.hit(Failpoint::BeforeFinalize).is_err());
        assert!(fps.hit(Failpoint::BeforeFinalize).is_ok(), "fired once");
        fps.disarm();
    }
}
