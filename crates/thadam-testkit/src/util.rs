//! Small helpers shared inside the testkit.

use std::future::Future;
use std::pin::pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;

/// Locks a mutex, ignoring poisoning: a panicking test thread must not turn
/// every later assertion into a poison error.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A small deterministic generator (SplitMix64), so randomized suites are
/// reproducible from a seed without a dependency.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    /// A generator starting from `seed`.
    pub fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..bound`; `bound` must be non-zero.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "bound must be non-zero");
        self.next_u64() % bound
    }

    /// A value in `lo..=hi`.
    pub fn between(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "empty range");
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `percent` out of 100.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Polls `fut` exactly once. Returns its output if it finished, or `None`
/// after dropping it. This is the sharpest cancellation a future can suffer.
pub async fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    std::future::poll_fn(|cx| {
        Poll::Ready(match fut.as_mut().poll(cx) {
            Poll::Ready(v) => Some(v),
            Poll::Pending => None,
        })
    })
    .await
}

/// Polls `fut` up to `polls` times, yielding to the scheduler between polls,
/// and drops it if it has not finished. Returns the output if it finished.
pub async fn poll_up_to<F: Future>(fut: F, polls: u32) -> Option<F::Output> {
    let mut fut = pin!(fut);
    for _ in 0..polls.max(1) {
        let ready = std::future::poll_fn(|cx| {
            Poll::Ready(match fut.as_mut().poll(cx) {
                Poll::Ready(v) => Some(v),
                Poll::Pending => None,
            })
        })
        .await;
        if ready.is_some() {
            return ready;
        }
        tokio::task::yield_now().await;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_deterministic_and_bounded() {
        let mut a = SplitMix64::new(7);
        let mut b = SplitMix64::new(7);
        for _ in 0..100 {
            let x = a.next_u64();
            assert_eq!(x, b.next_u64());
        }
        let mut r = SplitMix64::new(1);
        for _ in 0..1000 {
            assert!(r.below(10) < 10);
            let v = r.between(5, 8);
            assert!((5..=8).contains(&v));
        }
    }

    #[tokio::test]
    async fn poll_once_returns_ready_values_and_drops_pending_futures() {
        assert_eq!(poll_once(async { 5 }).await, Some(5));
        assert_eq!(poll_once(std::future::pending::<()>()).await, None);
        let sleeping = tokio::time::sleep(std::time::Duration::from_secs(3600));
        assert_eq!(poll_once(sleeping).await, None);
    }

    #[tokio::test]
    async fn poll_up_to_lets_a_yielding_future_finish() {
        let fut = async {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            9
        };
        assert_eq!(poll_up_to(fut, 10).await, Some(9));
        assert_eq!(poll_up_to(std::future::pending::<()>(), 3).await, None);
    }
}
