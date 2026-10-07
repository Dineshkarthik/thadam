//! The clock abstraction.
//!
//! Wall time can step backwards and is wrong on devices without a real-time
//! clock, so it is never used to measure durations: retention, rotation, and
//! timeouts use [`Clock::monotonic`] and WAL positions.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::time::Timestamp;

/// A source of wall time, monotonic time, and the clock-sync state.
pub trait Clock: Send + Sync {
    /// Current wall time as nanoseconds since the Unix epoch, UTC.
    fn now(&self) -> Timestamp;

    /// A monotonic instant for measuring durations.
    fn monotonic(&self) -> Instant;

    /// Whether the kernel reports the wall clock as synchronized, recorded per
    /// batch as `clock_synced`.
    fn synced(&self) -> bool;
}

/// A shared flag a maintenance task sets from the kernel's NTP state.
#[derive(Clone, Debug, Default)]
pub struct SyncFlag(Arc<AtomicBool>);

impl SyncFlag {
    /// Records whether the clock is synchronized.
    pub fn set(&self, synced: bool) {
        self.0.store(synced, Ordering::Relaxed);
    }

    /// The last recorded state.
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// The real clock.
///
/// Reading the kernel's NTP state needs a syscall that is not available under
/// `forbid(unsafe_code)` here, so the sync state comes from a [`SyncFlag`] that
/// a maintenance task updates. Until it reports otherwise the clock claims to
/// be unsynchronized.
#[derive(Clone, Debug, Default)]
pub struct SystemClock {
    sync: SyncFlag,
}

impl SystemClock {
    /// A system clock that reports itself unsynchronized until told otherwise.
    pub fn new() -> Self {
        Self::default()
    }

    /// A system clock whose sync state is driven by `flag`.
    pub fn with_sync_flag(flag: SyncFlag) -> Self {
        SystemClock { sync: flag }
    }

    /// The flag a maintenance task updates.
    pub fn sync_flag(&self) -> &SyncFlag {
        &self.sync
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
            Err(before) => i64::try_from(before.duration().as_nanos()).map_or(i64::MIN, |n| -n),
        };
        Timestamp::from_nanos(nanos)
    }

    fn monotonic(&self) -> Instant {
        Instant::now()
    }

    fn synced(&self) -> bool {
        self.sync.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_is_after_2020_and_monotonic_does_not_go_back() {
        let clock = SystemClock::new();
        let jan_2020 = Timestamp::from_secs(1_577_836_800).unwrap();
        assert!(clock.now() > jan_2020);
        let a = clock.monotonic();
        let b = clock.monotonic();
        assert!(b >= a);
    }

    #[test]
    fn sync_state_follows_the_flag() {
        let flag = SyncFlag::default();
        let clock = SystemClock::with_sync_flag(flag.clone());
        assert!(!clock.synced());
        flag.set(true);
        assert!(clock.synced());
    }
}
