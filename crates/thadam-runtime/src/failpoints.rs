//! Fault injection for crash and isolation tests.
//!
//! A [`Failpoints`] handle is attached to one run through
//! [`RuntimeOptions::failpoints`](crate::RuntimeOptions::failpoints), so tests
//! running in parallel do not interfere. Armed points fire once (or the given
//! number of times) and are free when disarmed: one relaxed atomic load.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use thadam_core::WalPos;

/// A segment driver "crashes" after the sink reported a watermark and before
/// the cursor is committed.
pub const SEGMENT_BEFORE_CURSOR_COMMIT: &str = "segment::before_cursor_commit";
/// A segment driver "crashes" after the cursor commit and before the sink is
/// told through `cursor_committed`.
pub const SEGMENT_BEFORE_CURSOR_COMMITTED: &str = "segment::before_cursor_committed";
/// A segment driver "crashes" after the sink's `append` returned (with sink
/// rejects) and before those rejects are staged for quarantine.
pub const SEGMENT_AFTER_APPEND_BEFORE_STAGE: &str = "segment::after_append_before_stage";
/// A quarantine instance "crashes" after a push of rejects was fsynced into
/// its staging file and before the rows reach the quarantine segment sink.
pub const QUARANTINE_AFTER_STAGE: &str = "quarantine::after_stage";
/// A batch driver "crashes" after persisting the in-flight range and before
/// writing the batch.
pub const BATCH_AFTER_INFLIGHT: &str = "batch::after_inflight_persist";
/// A batch driver "crashes" after a successful write and before the cursor
/// commit.
pub const BATCH_AFTER_WRITE: &str = "batch::after_write_before_commit";

#[derive(Default)]
struct Inner {
    any: AtomicBool,
    armed: Mutex<HashMap<&'static str, u32>>,
    chain_panics: Mutex<HashMap<u64, u32>>,
}

/// Named one-shot fault switches. Clones share state.
#[derive(Clone, Default)]
pub struct Failpoints(Arc<Inner>);

impl std::fmt::Debug for Failpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Failpoints").finish_non_exhaustive()
    }
}

impl Failpoints {
    /// Arms `name` to fire the next `times` times it is reached.
    pub fn arm(&self, name: &'static str, times: u32) {
        self.0
            .armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, times);
        self.0.any.store(true, Ordering::Relaxed);
    }

    /// Makes the chain panic while processing the frame at `pos`, `times` times.
    pub fn panic_chain_at(&self, pos: WalPos, times: u32) {
        self.0
            .chain_panics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(pos.get(), times);
        self.0.any.store(true, Ordering::Relaxed);
    }

    /// Whether `name` is still armed.
    pub fn is_armed(&self, name: &str) -> bool {
        self.0
            .armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .is_some_and(|n| *n > 0)
    }

    pub(crate) fn hit(&self, name: &'static str) -> bool {
        if !self.0.any.load(Ordering::Relaxed) {
            return false;
        }
        let mut armed = self.0.armed.lock().unwrap_or_else(PoisonError::into_inner);
        match armed.get_mut(name) {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn chain_panic(&self, first: WalPos, last: WalPos) -> bool {
        if !self.0.any.load(Ordering::Relaxed) {
            return false;
        }
        let mut map = self
            .0
            .chain_panics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        map.iter_mut()
            .find(|(pos, n)| (first.get()..=last.get()).contains(pos) && **n > 0)
            .map(|(_, n)| *n -= 1)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn points_fire_the_armed_number_of_times() {
        let fp = Failpoints::default();
        assert!(!fp.hit(SEGMENT_BEFORE_CURSOR_COMMIT));
        fp.arm(SEGMENT_BEFORE_CURSOR_COMMIT, 2);
        assert!(fp.is_armed(SEGMENT_BEFORE_CURSOR_COMMIT));
        assert!(fp.hit(SEGMENT_BEFORE_CURSOR_COMMIT));
        assert!(fp.hit(SEGMENT_BEFORE_CURSOR_COMMIT));
        assert!(!fp.hit(SEGMENT_BEFORE_CURSOR_COMMIT));
        assert!(!fp.is_armed(SEGMENT_BEFORE_CURSOR_COMMIT));

        fp.panic_chain_at(WalPos::new(5), 1);
        assert!(!fp.chain_panic(WalPos::new(1), WalPos::new(4)));
        assert!(fp.chain_panic(WalPos::new(3), WalPos::new(6)));
        assert!(!fp.chain_panic(WalPos::new(3), WalPos::new(6)));
    }
}
