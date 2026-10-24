//! Correlation ids.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Makes the `corrId` for each request.
///
/// The server echoes it, and the client checks the echo, so it only has to be
/// unique among a client's requests in flight. It is also what ties a request
/// to its response in a gateway's logs, so the default makes it unique across
/// clients too. A retry reuses the id of the request it retries.
///
/// A closure `Fn(&str) -> String`, given the request kind, is a source.
pub trait CorrIds: Send + Sync {
    fn next_id(&self, kind: &str) -> String;
}

impl<F> CorrIds for F
where
    F: Fn(&str) -> String + Send + Sync,
{
    fn next_id(&self, kind: &str) -> String {
        self(kind)
    }
}

/// `<prefix>-<n>`: a prefix random per generator, and a counter.
///
/// The prefix is 16 hex digits, so two clients' ids do not collide in a
/// shared log, and the counter makes one client's ids distinct and ordered.
#[derive(Debug)]
pub struct Sequential {
    prefix: String,
    next: AtomicU64,
}

impl Sequential {
    /// A generator with a random prefix.
    pub fn new() -> Self {
        Self::with_prefix(format!("rc-{:016x}", random_u64()))
    }

    /// A generator with a fixed prefix, for logs a person reads and for tests.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            next: AtomicU64::new(1),
        }
    }
}

impl Default for Sequential {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrIds for Sequential {
    fn next_id(&self, _kind: &str) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        format!("{}-{n}", self.prefix)
    }
}

/// 64 random bits from the standard library: `RandomState` is seeded per
/// process from the OS and perturbed per instance, which is plenty for an id
/// prefix and a backoff jitter, and saves a dependency on `rand`.
pub(crate) fn random_u64() -> u64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_distinct_and_carry_the_prefix() {
        let ids = Sequential::with_prefix("t");
        assert_eq!(ids.next_id("promise.get"), "t-1");
        assert_eq!(ids.next_id("promise.get"), "t-2");
    }

    #[test]
    fn two_generators_do_not_share_a_prefix() {
        let (a, b) = (Sequential::new(), Sequential::new());
        assert_ne!(a.next_id("k"), b.next_id("k"));
    }
}
