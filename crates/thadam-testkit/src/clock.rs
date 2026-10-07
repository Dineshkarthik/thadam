//! A controllable clock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use thadam_core::{Clock, Timestamp};

#[derive(Debug)]
struct Inner {
    base: Instant,
    wall_nanos: AtomicI64,
    mono_nanos: AtomicU64,
    synced: AtomicBool,
}

/// A clock tests drive by hand.
///
/// Wall time and monotonic time start together and move together on
/// [`FakeClock::advance`]. Wall time can additionally step in either direction
/// ([`FakeClock::step_wall`], [`FakeClock::set_wall`]) without touching
/// monotonic time, like an NTP step. Clones share state.
#[derive(Clone, Debug)]
pub struct FakeClock(Arc<Inner>);

impl FakeClock {
    /// A synchronized clock reading `start`.
    pub fn new(start: Timestamp) -> Self {
        FakeClock(Arc::new(Inner {
            base: Instant::now(),
            wall_nanos: AtomicI64::new(start.as_nanos()),
            mono_nanos: AtomicU64::new(0),
            synced: AtomicBool::new(true),
        }))
    }

    /// Moves wall and monotonic time forward by `d`.
    pub fn advance(&self, d: Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.0.mono_nanos.fetch_add(nanos, Ordering::SeqCst);
        let wall = i64::try_from(nanos).unwrap_or(i64::MAX);
        self.0.wall_nanos.fetch_add(wall, Ordering::SeqCst);
    }

    /// Steps wall time by `delta_nanos` (negative steps it backwards) without
    /// moving monotonic time.
    pub fn step_wall(&self, delta_nanos: i64) {
        self.0.wall_nanos.fetch_add(delta_nanos, Ordering::SeqCst);
    }

    /// Sets wall time without moving monotonic time.
    pub fn set_wall(&self, to: Timestamp) {
        self.0.wall_nanos.store(to.as_nanos(), Ordering::SeqCst);
    }

    /// Sets the clock-sync flag.
    pub fn set_synced(&self, synced: bool) {
        self.0.synced.store(synced, Ordering::SeqCst);
    }

    /// This clock as a shared trait object.
    pub fn shared(&self) -> Arc<dyn Clock> {
        Arc::new(self.clone())
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        // 2026-01-01T00:00:00Z
        Self::new(Timestamp::from_nanos(1_767_225_600_000_000_000))
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.0.wall_nanos.load(Ordering::SeqCst))
    }

    fn monotonic(&self) -> Instant {
        self.0.base + Duration::from_nanos(self.0.mono_nanos.load(Ordering::SeqCst))
    }

    fn synced(&self) -> bool {
        self.0.synced.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_moves_both_clocks() {
        let c = FakeClock::new(Timestamp::from_nanos(100));
        let m0 = c.monotonic();
        c.advance(Duration::from_nanos(50));
        assert_eq!(c.now(), Timestamp::from_nanos(150));
        assert_eq!(c.monotonic() - m0, Duration::from_nanos(50));
    }

    #[test]
    fn wall_steps_leave_monotonic_time_alone() {
        let c = FakeClock::new(Timestamp::from_nanos(1_000));
        let m0 = c.monotonic();
        c.step_wall(-400);
        assert_eq!(c.now(), Timestamp::from_nanos(600));
        assert_eq!(c.monotonic(), m0);
        c.set_wall(Timestamp::from_nanos(5));
        assert_eq!(c.now(), Timestamp::from_nanos(5));
    }

    #[test]
    fn backward_step_then_advance_keeps_durations_positive() {
        let c = FakeClock::new(Timestamp::from_nanos(10_000));
        let (w0, m0) = (c.now(), c.monotonic());
        c.step_wall(-9_000);
        c.advance(Duration::from_nanos(100));
        assert!(c.now() < w0);
        assert_eq!(c.monotonic() - m0, Duration::from_nanos(100));
    }

    #[test]
    fn sync_flag_and_clones_share_state() {
        let c = FakeClock::default();
        let shared = c.shared();
        assert!(shared.synced());
        c.set_synced(false);
        assert!(!shared.synced());
        let c2 = c.clone();
        c2.advance(Duration::from_secs(1));
        assert_eq!(c.now(), shared.now());
    }
}
