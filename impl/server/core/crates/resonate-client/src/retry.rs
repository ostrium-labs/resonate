//! When to try again, and how long to wait.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Retries for a call that got no answer.
///
/// A call is retried when an attempt cannot connect, times out, or is
/// answered `503` (the gateway's rendering of [`resonate_core::Unavailable`]),
/// or, over HTTP, when a proxy answers `502` or `504` without an envelope.
/// Nothing else is retried: a `4xx` or a `500` is an answer.
///
/// **Retries assume idempotence**, which is the protocol's own contract for
/// `Unavailable`: the attempt that timed out may have been applied. For most
/// requests a repeat is harmless — `promise.create` on an existing id answers
/// the existing promise, `promise.settle` on a settled one answers it as it
/// is. For `task.acquire`, `task.fulfill`, `task.suspend` and `task.fence` a
/// repeat of an applied attempt answers `409`, because the first attempt moved
/// the task's version on. [`Error::Rejected`](crate::Error::Rejected) carries
/// `attempts`, so a caller that sees a `409` from a call that needed more than
/// one can re-read the task before concluding it lost the lease.
///
/// The wait before attempt `n + 1` is drawn uniformly from
/// `[0, min(cap, base * 2^(n-1))]` ("full jitter"), and the whole call is
/// bounded by both `max_attempts` and `deadline`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts in total, including the first. `1` disables retries.
    pub max_attempts: u32,
    /// The first backoff's upper bound.
    pub base: Duration,
    /// No backoff is longer than this.
    pub cap: Duration,
    /// The whole call, every attempt and every wait, ends by this.
    pub deadline: Duration,
}

impl RetryPolicy {
    /// One attempt, no retries.
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            base: Duration::ZERO,
            cap: Duration::ZERO,
            deadline: Duration::from_secs(3600),
        }
    }
}

impl Default for RetryPolicy {
    /// Six attempts, 50 ms doubling to 2 s, all within 30 s.
    fn default() -> Self {
        Self {
            max_attempts: 6,
            base: Duration::from_millis(50),
            cap: Duration::from_secs(2),
            deadline: Duration::from_secs(30),
        }
    }
}

/// The jitter's random source: xorshift over a per-client seed. Not
/// cryptographic and does not need to be; it only has to keep clients that
/// failed together from retrying together.
#[derive(Debug)]
pub(crate) struct Jitter(AtomicU64);

impl Jitter {
    pub(crate) fn new() -> Self {
        Self(AtomicU64::new(crate::corr::random_u64() | 1))
    }

    fn next(&self) -> u64 {
        let mut x = self.0.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0.store(x, Ordering::Relaxed);
        x
    }

    /// The wait after failed attempt `attempt` (1-based).
    pub(crate) fn backoff(&self, policy: &RetryPolicy, attempt: u32) -> Duration {
        let exp = policy.base.saturating_mul(
            1u32.checked_shl(attempt.saturating_sub(1))
                .unwrap_or(u32::MAX),
        );
        let ceiling = exp.min(policy.cap).as_millis() as u64;
        if ceiling == 0 {
            return Duration::ZERO;
        }
        Duration::from_millis(self.next() % (ceiling + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_stays_under_the_doubling_ceiling_and_the_cap() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base: Duration::from_millis(10),
            cap: Duration::from_millis(100),
            deadline: Duration::from_secs(1),
        };
        let jitter = Jitter::new();
        for attempt in 1..=40 {
            let ceiling = Duration::from_millis(10 * (1u64 << (attempt - 1).min(20)))
                .min(Duration::from_millis(100));
            for _ in 0..50 {
                assert!(
                    jitter.backoff(&policy, attempt) <= ceiling,
                    "attempt {attempt}"
                );
            }
        }
    }

    #[test]
    fn backoff_is_jittered() {
        let policy = RetryPolicy::default();
        let jitter = Jitter::new();
        let waits: std::collections::HashSet<_> =
            (0..32).map(|_| jitter.backoff(&policy, 6)).collect();
        assert!(waits.len() > 1, "full jitter draws different waits");
    }

    #[test]
    fn no_retries_means_no_wait() {
        assert_eq!(
            Jitter::new().backoff(&RetryPolicy::none(), 1),
            Duration::ZERO
        );
    }
}
