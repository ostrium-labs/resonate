//! Every store operation in a span, timed.
//!
//! A wrapper rather than spans in each [`Store`] implementation, so a store
//! added later — over TiKV, say — is traced and measured the moment
//! [`Server::build`](crate::server::Server::build) is handed one, without its
//! author having to remember. The span names and the histogram labels are the
//! six operations of the port; the store itself is named by
//! [`Store::system`].

use std::sync::Arc;

use async_trait::async_trait;

use crate::store::{Etag, Store, StoreError};

/// A [`Store`] that traces and times what it forwards.
pub struct TracedStore {
    inner: Arc<dyn Store>,
    system: &'static str,
}

impl TracedStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        let system = inner.system();
        Self::named(inner, system)
    }

    /// Under a name of the caller's choosing rather than the store's own.
    pub fn named(inner: Arc<dyn Store>, system: &'static str) -> Self {
        Self { inner, system }
    }
}

/// Whether a result is an answer. A lost compare-and-swap is: it is the CAS
/// doing its job, and counting it as an error would page someone for
/// contention.
fn answered<T>(r: &Result<T, StoreError>) -> bool {
    !matches!(r, Err(StoreError::Unavailable(_)))
}

#[async_trait]
impl Store for TracedStore {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        resonate_trace::store_op(self.system, "get", answered, self.inner.get(key)).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        resonate_trace::store_op(
            self.system,
            "put_if_match",
            answered,
            self.inner.put_if_match(key, body, etag),
        )
        .await
    }

    async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        resonate_trace::store_op(
            self.system,
            "put_if_none_match",
            answered,
            self.inner.put_if_none_match(key, body),
        )
        .await
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        resonate_trace::store_op(self.system, "put", answered, self.inner.put(key, body)).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        resonate_trace::store_op(self.system, "delete", answered, self.inner.delete(key)).await
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        resonate_trace::store_op(
            self.system,
            "list",
            answered,
            self.inner.list(prefix, max_keys),
        )
        .await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), StoreError> {
        // Forwarded whole, so a store with a native range delete keeps it; its
        // pieces, if it has none, are traced by the inner store's own calls.
        resonate_trace::store_op(
            self.system,
            "delete_prefix",
            answered,
            self.inner.delete_prefix(prefix),
        )
        .await
    }

    fn system(&self) -> &'static str {
        self.system
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ObjectStoreAdapter;

    // A name of its own, so the other tests in this binary, which build
    // servers over `blob` stores concurrently, cannot move these counts.
    const SYSTEM: &str = "traced_store_test";

    fn count(op: &str, outcome: &str) -> u64 {
        resonate_trace::STORE_OP_DURATION
            .with_label_values(&[SYSTEM, op, outcome])
            .get_sample_count()
    }

    #[tokio::test]
    async fn every_operation_is_timed_and_a_lost_race_is_an_answer() {
        let store = TracedStore::named(Arc::new(ObjectStoreAdapter::in_memory()), SYSTEM);
        store.put_if_none_match("a/b", b"1".to_vec()).await.unwrap();
        assert_eq!(
            store.put_if_none_match("a/b", b"2".to_vec()).await,
            Err(StoreError::PreconditionFailed)
        );
        assert!(store.get("a/b").await.unwrap().is_some());
        store.delete("a/b").await.unwrap();
        assert_eq!(count("put_if_none_match", "ok"), 2);
        assert_eq!(count("put_if_none_match", "error"), 0);
        assert_eq!(count("get", "ok"), 1);
        assert_eq!(count("delete", "ok"), 1);
    }
}
