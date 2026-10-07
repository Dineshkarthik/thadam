//! Time-domain and clock-step scenarios against a scripted clock.
#![cfg(not(loom))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use thadam_core::{Clock, Quality, QualityFlags, TimeDomain, Times, Timestamp};

/// A clock whose wall time can jump while monotonic time only moves forward.
struct ScriptedClock {
    base: Instant,
    state: Mutex<(i64, Duration, bool)>,
}

impl ScriptedClock {
    fn new(wall: i64, synced: bool) -> Self {
        ScriptedClock {
            base: Instant::now(),
            state: Mutex::new((wall, Duration::ZERO, synced)),
        }
    }

    fn advance(&self, d: Duration) {
        let mut s = self.state.lock().unwrap();
        s.0 += i64::try_from(d.as_nanos()).unwrap();
        s.1 += d;
    }

    fn step_wall(&self, delta_nanos: i64) {
        self.state.lock().unwrap().0 += delta_nanos;
    }

    fn set_synced(&self, synced: bool) {
        self.state.lock().unwrap().2 = synced;
    }
}

impl Clock for ScriptedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.state.lock().unwrap().0)
    }
    fn monotonic(&self) -> Instant {
        self.base + self.state.lock().unwrap().1
    }
    fn synced(&self) -> bool {
        self.state.lock().unwrap().2
    }
}

#[test]
fn backward_wall_step_does_not_shorten_monotonic_durations() {
    let clock = ScriptedClock::new(1_000_000_000_000, true);
    let (wall_before, mono_before) = (clock.now(), clock.monotonic());

    clock.advance(Duration::from_secs(10));
    clock.step_wall(-3_600 * 1_000_000_000);
    clock.advance(Duration::from_secs(5));

    assert!(
        clock.now() < wall_before,
        "NTP stepped wall time back an hour"
    );
    assert_eq!(clock.now().checked_duration_since(wall_before), None);
    assert_eq!(clock.monotonic() - mono_before, Duration::from_secs(15));
}

#[test]
fn unsynced_boot_marks_the_batch_and_a_correction_keeps_device_time() {
    let clock = ScriptedClock::new(0, false);
    assert!(
        !clock.synced(),
        "no RTC: the device boots with the epoch as wall time"
    );

    let device_time = Timestamp::from_secs(1_800_000_000).unwrap();
    let mut times = Times::new(clock.now(), TimeDomain::Utc)
        .with_source(device_time)
        .unwrap();
    let mut quality = Quality::default();

    clock.set_synced(true);
    clock.step_wall(1_800_000_000 * 1_000_000_000);
    let corrected = Timestamp::from_secs(1_800_000_000).unwrap();
    times
        .correct(corrected, &mut quality, QualityFlags::CLOCK_SKEW)
        .unwrap();

    assert_eq!(
        times.ingest(),
        Timestamp::EPOCH,
        "ingest time is what the unsynced clock said"
    );
    assert_eq!(times.source(), Some(device_time));
    assert_eq!(times.event_time(), Some(corrected));
    assert!(quality.flags.contains(QualityFlags::CLOCK_SKEW));
}

#[test]
fn non_utc_domains_never_yield_an_event_time_without_a_correction() {
    let ingest = Timestamp::from_nanos(5_000);
    let cases = [
        (
            TimeDomain::Tai,
            Timestamp::from_secs(1_800_000_037).unwrap(),
        ),
        (TimeDomain::Monotonic, Timestamp::from_nanos(123_456)),
        (TimeDomain::Sim, Timestamp::from_secs(42).unwrap()),
        (TimeDomain::Unknown, Timestamp::from_secs(1).unwrap()),
    ];
    for (domain, source) in cases {
        let times = Times::new(ingest, domain).with_source(source).unwrap();
        assert_eq!(times.source(), Some(source));
        assert_eq!(times.event_time(), None, "{domain}");
    }
}
