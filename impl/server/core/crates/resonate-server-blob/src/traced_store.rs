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

use crate::store::{inner_index, Etag, IndexDelta, IndexView, Store, StoreError, StoreIndex};

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

    fn index(&self) -> Option<&dyn StoreIndex> {
        self.inner.index().map(|_| self as &dyn StoreIndex)
    }
}

#[async_trait]
impl StoreIndex for TracedStore {
    async fn put_indexed(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: Option<&Etag>,
        delta: &IndexDelta,
    ) -> Result<Etag, StoreError> {
        let ix = inner_index(&self.inner)?;
        resonate_trace::store_op(
            self.system,
            "put_indexed",
            answered,
            ix.put_indexed(key, body, expected, delta),
        )
        .await
    }

    async fn index_if_match(
        &self,
        key: &str,
        etag: &Etag,
        delta: &IndexDelta,
    ) -> Result<(), StoreError> {
        let ix = inner_index(&self.inner)?;
        resonate_trace::store_op(
            self.system,
            "index_if_match",
            answered,
            ix.index_if_match(key, etag, delta),
        )
        .await
    }

    async fn index_apply(&self, delta: &IndexDelta) -> Result<(), StoreError> {
        let ix = inner_index(&self.inner)?;
        resonate_trace::store_op(self.system, "index_apply", answered, ix.index_apply(delta))
            .await
    }

    async fn view(&self) -> Result<Box<dyn IndexView>, StoreError> {
        let ix = inner_index(&self.inner)?;
        let inner = resonate_trace::store_op(self.system, "index_view", answered, ix.view()).await?;
        Ok(Box::new(TracedView {
            inner,
            system: self.system,
        }))
    }
}

/// A view whose reads are traced and timed like the store's own.
struct TracedView {
    inner: Box<dyn IndexView>,
    system: &'static str,
}

#[async_trait]
impl IndexView for TracedView {
    async fn scan(
        &mut self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<String>, StoreError> {
        resonate_trace::store_op(
            self.system,
            "index_scan",
            answered,
            self.inner.scan(start, end, limit),
        )
        .await
    }

    async fn get_many(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<(Vec<u8>, Etag)>>, StoreError> {
        resonate_trace::store_op(
            self.system,
            "index_get_many",
            answered,
            self.inner.get_many(keys),
        )
        .await
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
