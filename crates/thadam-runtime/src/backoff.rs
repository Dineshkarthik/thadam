//! Jittered exponential backoff.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::options::BackoffPolicy;

/// The delay sequence of one retry loop. Full jitter: each delay is uniform in
/// `[0, min(max, initial * 2^attempt)]`, so restarts across components spread
/// out instead of synchronizing.
#[derive(Debug)]
pub(crate) struct Backoff {
    policy: BackoffPolicy,
    attempt: u32,
    rng: u64,
}

impl Backoff {
    pub(crate) fn new(policy: BackoffPolicy) -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            ^ (u64::from(std::process::id()) << 32);
        Backoff {
            policy,
            attempt: 0,
            rng: seed | 1,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let cap = self
            .policy
            .initial
            .saturating_mul(1u32 << self.attempt.min(20))
            .min(self.policy.max);
        self.attempt = self.attempt.saturating_add(1);
        let nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
        if nanos == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.next_u64() % (nanos + 1))
    }

    fn next_u64(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_stay_under_the_growing_cap() {
        let mut b = Backoff::new(BackoffPolicy::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
        ));
        for attempt in 0..12u32 {
            let cap = Duration::from_millis(10)
                .saturating_mul(1 << attempt)
                .min(Duration::from_millis(100));
            assert!(b.next_delay() <= cap);
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(10));
    }
}
