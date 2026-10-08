//! Unbounded, capped, jittered per-file backoff (plan 5.8 "Retry").
//!
//! Edge uploads never give up: the delay grows from `initial` by a factor of
//! two until `max`, then stays there. Jitter draws each delay from
//! `[base/2, base]` so a fleet recovering from a shared outage does not
//! re-synchronize.

use std::time::Duration;

/// How long to wait between upload attempts for one file.
#[derive(Clone, Debug)]
pub(crate) struct Backoff {
    initial: Duration,
    max: Duration,
    jitter: bool,
    attempt: u32,
    seed: u64,
}

impl Backoff {
    /// A fresh backoff from the retry config.
    pub(crate) fn new(retry: crate::config::RetryConfig, seed: u64) -> Self {
        Backoff {
            initial: retry.initial,
            max: retry.max,
            jitter: retry.jitter,
            attempt: 0,
            seed: seed | 1,
        }
    }

    /// The delay before the next attempt; advances the attempt counter.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let shift = self.attempt.min(20);
        let base = self.initial.saturating_mul(1u32 << shift).min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        if !self.jitter || base.is_zero() {
            return base;
        }
        let base_ns = base.as_nanos() as u64;
        let jittered = base_ns / 2 + (self.next_rand() % (base_ns / 2 + 1));
        Duration::from_nanos(jittered)
    }

    /// The number of the attempt about to be made (1-based).
    pub(crate) fn attempt(&self) -> u32 {
        self.attempt.saturating_add(1)
    }

    /// SplitMix64: deterministic, dependency-free, and good enough to
    /// de-correlate retry schedules.
    fn next_rand(&mut self) -> u64 {
        self.seed = self.seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RetryConfig;

    fn retry(initial_ms: u64, max_ms: u64, jitter: bool) -> RetryConfig {
        RetryConfig {
            initial: Duration::from_millis(initial_ms),
            max: Duration::from_millis(max_ms),
            jitter,
        }
    }

    #[test]
    fn doubles_until_the_cap_then_stays() {
        let mut b = Backoff::new(retry(100, 500, false), 1);
        let delays: Vec<Duration> = (0..6).map(|_| b.next_delay()).collect();
        assert_eq!(
            delays,
            [100, 200, 400, 500, 500, 500].map(Duration::from_millis)
        );
    }

    #[test]
    fn jitter_stays_inside_base_div_2_to_base() {
        let mut b = Backoff::new(retry(100, 400, true), 42);
        let mut base_nanos = 100_000_000u128;
        for _ in 0..10 {
            let d = b.next_delay().as_nanos();
            let cap = base_nanos.min(400_000_000);
            assert!(
                d >= cap / 2 && d <= cap,
                "{d} not in [{}, {}]",
                cap / 2,
                cap
            );
            base_nanos = (base_nanos * 2).min(400_000_000);
        }
    }

    #[test]
    fn a_seed_changes_the_sequence_but_is_repeatable() {
        let mut a = Backoff::new(retry(100, 800, true), 7);
        let mut b = Backoff::new(retry(100, 800, true), 7);
        let mut c = Backoff::new(retry(100, 800, true), 9);
        let va: Vec<_> = (0..5).map(|_| a.next_delay()).collect();
        let vb: Vec<_> = (0..5).map(|_| b.next_delay()).collect();
        let vc: Vec<_> = (0..5).map(|_| c.next_delay()).collect();
        assert_eq!(va, vb);
        assert_ne!(va, vc);
    }
}
