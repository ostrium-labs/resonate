//! Which trace a promise belongs to, for the messages emitted about it later.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::TraceParent;

/// A bounded map from promise id to the trace it was created in.
///
/// The durable copy of a context is the promise's `traceparent` tag, which a
/// client sets and every backend stores. This is the near copy: the router
/// reads it when it hands an execute message to a worker, so that a task
/// dispatched long after its `promise.create` — redispatched by a retry
/// deadline, resumed by a callback — still continues the trace that created
/// it, without the engines having to put tags on every message they emit.
///
/// What one process has seen, and no more. After a restart, or for a promise
/// created through another instance, there is no entry and a message goes out
/// with the context of whatever emitted it, or none. That is the same
/// best-effort contract the rest of tracing has: a trace may break, a request
/// never fails because of one.
pub struct TraceContexts {
    capacity: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// id → (generation, context)
    map: HashMap<String, (u64, TraceParent)>,
    /// Insertion order, for eviction. An entry whose generation no longer
    /// matches the map was replaced or removed and is skipped.
    order: VecDeque<(String, u64)>,
    next: u64,
}

impl Default for TraceContexts {
    fn default() -> Self {
        Self::new(100_000)
    }
}

impl TraceContexts {
    /// Hold at most `capacity` contexts, forgetting the oldest first.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// File `id` under `tp`, replacing what it was filed under.
    pub fn insert(&self, id: &str, tp: TraceParent) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let generation = inner.next;
        inner.next += 1;
        inner.map.insert(id.to_string(), (generation, tp));
        inner.order.push_back((id.to_string(), generation));
        while inner.map.len() > self.capacity {
            let Some((old, g)) = inner.order.pop_front() else {
                break;
            };
            if inner.map.get(&old).is_some_and(|(cur, _)| *cur == g) {
                inner.map.remove(&old);
            }
        }
        // Removals leave their order entries behind; keep the queue from
        // growing without bound when ids are removed faster than evicted.
        if inner.order.len() > self.capacity.saturating_mul(2) {
            let Inner { map, order, .. } = &mut *inner;
            order.retain(|(id, g)| map.get(id).is_some_and(|(cur, _)| cur == g));
        }
    }

    /// The trace `id` was filed under.
    pub fn get(&self, id: &str) -> Option<TraceParent> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.map.get(id).map(|(_, tp)| tp.clone())
    }

    /// Forget `id`: its task is done, and nothing more will be sent for it.
    pub fn remove(&self, id: &str) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.map.remove(id);
    }

    /// How many ids are filed.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(n: u8) -> TraceParent {
        TraceParent::parse(
            &format!("00-{:032x}-00f067aa0ba902b7-01", u128::from(n) + 1),
            None,
        )
        .unwrap()
    }

    #[test]
    fn the_oldest_is_forgotten_first() {
        let c = TraceContexts::new(2);
        c.insert("a", tp(1));
        c.insert("b", tp(2));
        c.insert("c", tp(3));
        assert!(c.get("a").is_none());
        assert_eq!(c.get("b"), Some(tp(2)));
        assert_eq!(c.get("c"), Some(tp(3)));
    }

    #[test]
    fn a_refiled_id_is_not_evicted_by_its_old_entry() {
        let c = TraceContexts::new(2);
        c.insert("a", tp(1));
        c.insert("b", tp(2));
        c.insert("a", tp(3));
        c.insert("c", tp(4));
        assert_eq!(c.get("a"), Some(tp(3)));
        assert!(c.get("b").is_none());
    }

    #[test]
    fn removals_do_not_grow_the_queue_without_bound() {
        let c = TraceContexts::new(4);
        for i in 0..1_000 {
            let id = format!("p{i}");
            c.insert(&id, tp(1));
            c.remove(&id);
        }
        assert!(c.is_empty());
        assert!(c.inner.lock().unwrap().order.len() <= 8);
    }
}
