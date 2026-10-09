//! Deterministic randomness: one ChaCha8 stream per (seed, tag) pair.
//!
//! The runner derives tags like `robot-3/behavior` and `robot-3/chaos`, so a
//! message's values depend only on the seed and its robot and purpose, never
//! on task scheduling or on what other streams drew. `rand_chacha` sequences
//! are platform-stable.

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;

/// FNV-1a 64 of the tag; stable across platforms and runs, cheap, and only
/// ever mixed with the master seed (not a secrecy boundary).
fn tag_hash(tag: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in tag.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// A deterministic RNG derived from the master seed plus a string tag.
#[derive(Clone, Debug)]
pub struct SimRng {
    inner: ChaCha8Rng,
}

impl SimRng {
    /// A stream for `tag`; different tags give independent sequences.
    pub fn new(seed: u64, tag: &str) -> SimRng {
        SimRng {
            inner: ChaCha8Rng::seed_from_u64(seed ^ tag_hash(tag)),
        }
    }

    /// Uniform in `[min, max]`; panics only on an inverted range, which
    /// configuration validation rejects.
    pub fn range(&mut self, min: f64, max: f64) -> f64 {
        debug_assert!(min <= max, "inverted range {min}..{max}");
        min + (max - min) * self.inner.random::<f64>()
    }

    /// Uniform integer in `min..=max`.
    pub fn range_u64(&mut self, min: u64, max: u64) -> u64 {
        debug_assert!(min <= max, "inverted range {min}..={max}");
        self.inner.random_range(min..=max)
    }

    /// True with probability `p`.
    pub fn bernoulli(&mut self, p: f64) -> bool {
        debug_assert!((0.0..=1.0).contains(&p), "not a probability: {p}");
        self.inner.random::<f64>() < p
    }

    /// A symmetric noise value in `[-amp, amp]`.
    pub fn noise(&mut self, amp: f64) -> f64 {
        (self.inner.random::<f64>() * 2.0 - 1.0) * amp
    }

    /// Picks one element; `list` is never empty for built-ins.
    pub fn pick<'a, T>(&mut self, list: &'a [T]) -> &'a T {
        debug_assert!(!list.is_empty(), "pick from an empty list");
        let idx = self.inner.random_range(0..list.len());
        &list[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_and_tag_repeats() {
        let mut a = SimRng::new(42, "robot-3/values");
        let mut b = SimRng::new(42, "robot-3/values");
        let va: Vec<f64> = (0..64).map(|_| a.range(-1.0, 1.0)).collect();
        let vb: Vec<f64> = (0..64).map(|_| b.range(-1.0, 1.0)).collect();
        assert_eq!(va, vb);
    }

    #[test]
    fn different_tags_diverge() {
        let mut a = SimRng::new(42, "robot-3/values");
        let mut b = SimRng::new(42, "robot-3/chaos");
        let va: Vec<u64> = (0..8).map(|_| a.range_u64(0, u64::MAX)).collect();
        let vb: Vec<u64> = (0..8).map(|_| b.range_u64(0, u64::MAX)).collect();
        assert_ne!(va, vb);
    }

    #[test]
    fn beroneulli_endpoints_are_exact() {
        let mut rng = SimRng::new(7, "t");
        assert!((0..1000).all(|_| !rng.bernoulli(0.0)));
        assert!((0..1000).all(|_| rng.bernoulli(1.0)));
    }
}
