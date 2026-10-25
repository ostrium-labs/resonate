//! [`TikvStore`]: the [`Store`] port over TiKV's transactional API.
//!
//! # Layout (fixed: Loams 0.1.0 wrote it, and a 0.1.0 keyspace must open)
//!
//! Every key of one store lives under its `root` inside one API v2 keyspace:
//!
//! ```text
//! root ‖ "resonate/blob/" ‖ <object key>   →  revision (16 random bytes) ‖ body
//! root ‖ "t/" ‖ <16 random bytes>           →  expires_ms (u64 BE) [‖ "F"]
//! ```
//!
//! The revision is random rather than a counter or a timestamp: deleting a
//! key and creating it again can never make an old [`Etag`] match. The
//! `Etag` is the revision as a hyphenated UUID.
//!
//! The `t/` keys are commit tokens (below). Each lives 30 minutes and is
//! swept by [`TikvStore::sweep_expired_tokens`].
//!
//! # Writes
//!
//! Every write is one pessimistic transaction. `get_for_update` locks the key,
//! the revision is compared, the new value is put, and the transaction
//! commits. Concurrent writers to one origin queue on the lock instead of
//! aborting, and two server instances can never both win a compare-and-set.
//!
//! # Unknown outcomes
//!
//! A commit whose answer is lost (TiKV's `UndeterminedError`, or a commit
//! that misses its deadline) is never assumed either way. Each write also
//! puts a commit token, `t/<token>`, in the same transaction. When the
//! outcome is unknown, a resolving transaction reads the token at a fresh
//! timestamp; TiKV resolves any lock the commit left before it answers.
//!
//! - **Present:** the write landed.
//! - **Absent:** the resolver writes a fence at the token and commits it. A
//!   straggling request of the lost commit then conflicts, so the write
//!   certainly did not land, and it is retried.
//!
//! If the resolver cannot decide within two request timeouts, the store
//! answers [`StoreError::Conflict`]. The caller retries the same conditional
//! write. If the first write did land, the retry finds the new revision and
//! falls into the re-decide path, the same contract as S3's
//! `ConditionalRequestConflict`.
//!
//! Ported from Loams' `loams-tikv` runner and `loams-durable` TiKV store
//! (Apache-2.0, Ostrium Labs), which wrote this format in Loams 0.1.0.

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tikv_client::{
    CheckLevel, Config as ClientConfig, Error as TikvError, Key, Timestamp, Transaction,
    TransactionClient, TransactionOptions,
};
use uuid::Uuid;

use crate::store::{Etag, Store, StoreError};

/// Where object keys live under the root.
pub const OBJECT_PREFIX: &[u8] = b"resonate/blob/";
/// Where commit tokens live under the root.
pub const TOKEN_PREFIX: &[u8] = b"t/";
/// The length of the revision every value starts with.
pub const REVISION_BYTES: usize = 16;
/// How long a commit token is kept.
pub const TOKEN_TTL: Duration = Duration::from_secs(30 * 60);

const MAX_ATTEMPTS: u32 = 16;
const RUN_DEADLINE: Duration = Duration::from_secs(10);
const BACKOFF_BASE: Duration = Duration::from_millis(10);
const BACKOFF_MAX: Duration = Duration::from_secs(1);
const TOKEN_SWEEP_PAGE: u32 = 256;
/// tikv-client's default 4 MiB is below what a page of large documents takes.
const GRPC_MAX_DECODING_BYTES: usize = 16 * 1024 * 1024;
const TSO_CLOSED: &str = "TimestampRequest channel is closed";

/// How to reach TiKV and where the store's keys go.
#[derive(Debug, Clone)]
pub struct TikvStoreCfg {
    /// PD endpoints, `host:port`.
    pub pd: Vec<String>,
    /// The API v2 keyspace. Empty means API v1, a cluster without keyspaces.
    pub keyspace: String,
    /// The prefix every key of this store lives under inside the keyspace.
    /// Two deployments on one keyspace need different roots.
    pub root: Vec<u8>,
    /// The per-request timeout of the client.
    pub request_timeout: Duration,
    /// Async commit and one-phase commit where the transaction allows it.
    /// Off means classic two-phase commit.
    pub async_commit: bool,
}

impl TikvStoreCfg {
    pub fn new(pd: Vec<String>, keyspace: impl Into<String>) -> Self {
        Self {
            pd,
            keyspace: keyspace.into(),
            root: Vec::new(),
            request_timeout: Duration::from_secs(5),
            async_commit: true,
        }
    }

    fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::default()
            .with_timeout(self.request_timeout)
            .with_grpc_max_decoding_message_size(GRPC_MAX_DECODING_BYTES);
        if !self.keyspace.is_empty() {
            config = config.with_keyspace(&self.keyspace);
        }
        config
    }
}

/// The transaction client, rebuilt when its TSO stream dies (the client never
/// recovers from that by itself).
struct Clients {
    cfg: TikvStoreCfg,
    current: RwLock<(Arc<TransactionClient>, u64)>,
    rebuilding: tokio::sync::Mutex<()>,
}

impl Clients {
    fn get(&self) -> (Arc<TransactionClient>, u64) {
        let guard = self.current.read().unwrap_or_else(|e| e.into_inner());
        (Arc::clone(&guard.0), guard.1)
    }

    /// Replace generation `seen` with a fresh client, unless someone already
    /// has. Failure leaves the old client in place for the next attempt.
    async fn rebuild(&self, seen: u64) {
        let _one_at_a_time = self.rebuilding.lock().await;
        if self.get().1 != seen {
            return;
        }
        match TransactionClient::new_with_config(self.cfg.pd.clone(), self.cfg.client_config())
            .await
        {
            Ok(client) => {
                let mut guard = self.current.write().unwrap_or_else(|e| e.into_inner());
                *guard = (Arc::new(client), seen + 1);
                tracing::warn!(generation = seen + 1, "rebuilt the TiKV client");
            }
            Err(error) => tracing::warn!(%error, "rebuilding the TiKV client failed"),
        }
    }
}

/// The [`Store`] over TiKV. Cheap to clone.
#[derive(Clone)]
pub struct TikvStore {
    clients: Arc<Clients>,
    root: Arc<[u8]>,
    request_timeout: Duration,
    async_commit: bool,
    stats: Arc<Mutex<TikvStats>>,
}

/// What the store has had to do beyond the happy path.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TikvStats {
    /// Transactions restarted after a conflict or a not-applied error.
    pub restarts: u64,
    /// Commits whose outcome was unknown and went through the token.
    pub unknown_outcomes: u64,
    /// Clients rebuilt after their TSO stream died.
    pub client_rebuilds: u64,
}

impl std::fmt::Debug for TikvStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TikvStore")
            .field("keyspace", &self.clients.cfg.keyspace)
            .field("root", &String::from_utf8_lossy(&self.root))
            .finish_non_exhaustive()
    }
}

/// How a write treats what is there.
#[derive(Clone, Copy)]
enum Condition {
    Overwrite,
    Absent,
    Match(Uuid),
}

/// How one transaction attempt ended.
enum Outcome<T> {
    Done(T),
    /// Certainly not applied: run the transaction again. `true` when the
    /// cause was a conflict with another writer rather than a fault.
    Retry(String, bool),
    /// Applied or not, nobody knows.
    Unknown,
    Fail(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    Fatal,
    NotApplied,
    TsoClosed,
    Conflict,
    AlreadyExists,
    Undetermined,
}

fn classify(e: &TikvError) -> Class {
    match e {
        TikvError::UndeterminedError(_) => Class::Undetermined,
        TikvError::PessimisticLockError { inner, .. } => classify(inner),
        TikvError::MultipleKeyErrors(errors) | TikvError::MultipleRegionErrors(errors) => {
            errors.iter().map(classify).max().unwrap_or(Class::Fatal)
        }
        TikvError::KeyError(ke) => {
            if ke.already_exist.is_some() {
                Class::AlreadyExists
            } else if ke.conflict.is_some()
                || ke.locked.is_some()
                || ke.deadlock.is_some()
                || !ke.retryable.is_empty()
            {
                Class::Conflict
            } else if ke.abort.contains("KeyTooLarge")
                || ke.abort.contains("EntryTooLarge")
                || ke.abort.contains("TxnTooLarge")
            {
                // TiKV's size limits (8 KiB keys by default): no retry can
                // make the write fit.
                Class::Fatal
            } else if !ke.abort.is_empty()
                || ke.commit_ts_expired.is_some()
                || ke.txn_not_found.is_some()
                || ke.txn_lock_not_found.is_some()
                || ke.commit_ts_too_large.is_some()
                || ke.primary_mismatch.is_some()
            {
                Class::NotApplied
            } else {
                Class::Fatal
            }
        }
        TikvError::DuplicateKeyInsertion => Class::AlreadyExists,
        TikvError::ResolveLockError(_) => Class::Conflict,
        TikvError::InternalError { message } | TikvError::StringError(message)
            if message.contains(TSO_CLOSED) =>
        {
            Class::TsoClosed
        }
        TikvError::GrpcAPI(_)
        | TikvError::Grpc(_)
        | TikvError::RegionError(_)
        | TikvError::Io(_)
        | TikvError::Channel(_)
        | TikvError::Canceled(_)
        | TikvError::JoinError(_)
        | TikvError::LeaderNotFound { .. }
        | TikvError::RegionForKeyNotFound { .. }
        | TikvError::RegionForRangeNotFound { .. }
        | TikvError::RegionNotFoundInResponse { .. }
        | TikvError::NoCurrentRegions
        | TikvError::EntryNotFoundInRegionCache
        | TikvError::KvError { .. }
        | TikvError::TxnNotFound(_)
        | TikvError::OnePcFailure
        | TikvError::InternalError { .. }
        | TikvError::StringError(_) => Class::NotApplied,
        _ => Class::Fatal,
    }
}

fn backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(10);
    let ceiling = BACKOFF_BASE.saturating_mul(1 << exp).min(BACKOFF_MAX);
    let us = u64::try_from(ceiling.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(fastrand::u64(us / 2..=us.max(1)))
}

fn physical_ms(ts: &Timestamp) -> u64 {
    u64::try_from(ts.physical).unwrap_or(0)
}

/// The value of a token written at `now_ms`: when it expires.
pub fn token_value(now_ms: u64) -> Vec<u8> {
    let ttl = u64::try_from(TOKEN_TTL.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_add(ttl).to_be_bytes().to_vec()
}

/// The fence a resolver writes at an absent token.
pub fn fence_value(now_ms: u64) -> Vec<u8> {
    let mut v = token_value(now_ms);
    v.push(b'F');
    v
}

/// Whether a token value is a resolver's fence.
pub fn is_fence(value: &[u8]) -> bool {
    value.len() == 9 && value[8] == b'F'
}

/// When a token or fence expires, or `None` if `value` is neither.
pub fn token_expiry(value: &[u8]) -> Option<u64> {
    value
        .get(..8)
        .filter(|_| value.len() == 8 || is_fence(value))
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
}

/// The smallest key greater than every key that starts with `prefix`, or
/// empty (unbounded) when `prefix` is all `0xff`.
fn successor(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    Vec::new()
}

/// Splits a stored value into its body and revision.
fn decode(value: Vec<u8>) -> Result<(Vec<u8>, Etag), StoreError> {
    let revision = value
        .get(..REVISION_BYTES)
        .ok_or_else(|| StoreError::Unavailable("TiKV returned a truncated object".into()))?;
    let revision = Uuid::from_slice(revision)
        .map_err(|e| StoreError::Unavailable(format!("invalid object revision: {e}")))?;
    Ok((value[REVISION_BYTES..].to_vec(), Etag(revision.to_string())))
}

impl TikvStore {
    /// Connects to PD. Fails when no PD endpoint answers. The keyspace must
    /// exist: see [`ensure_keyspace`](super::ensure_keyspace).
    pub async fn connect(cfg: TikvStoreCfg) -> Result<Self, StoreError> {
        if cfg.pd.is_empty() || cfg.pd.iter().any(|p| p.trim().is_empty()) {
            return Err(StoreError::Unavailable(
                "TiKV needs at least one PD endpoint".into(),
            ));
        }
        let client = TransactionClient::new_with_config(cfg.pd.clone(), cfg.client_config())
            .await
            .map_err(|e| {
                StoreError::Unavailable(format!(
                    "cannot connect to TiKV (keyspace {:?}): {e}",
                    cfg.keyspace
                ))
            })?;
        Ok(Self {
            root: cfg.root.clone().into(),
            request_timeout: cfg.request_timeout,
            async_commit: cfg.async_commit,
            clients: Arc::new(Clients {
                cfg,
                current: RwLock::new((Arc::new(client), 0)),
                rebuilding: tokio::sync::Mutex::new(()),
            }),
            stats: Arc::default(),
        })
    }

    /// What the store has had to do beyond the happy path.
    pub fn stats(&self) -> TikvStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `root ‖ "resonate/blob/" ‖ name`.
    pub fn object_key(&self, name: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.root.len() + OBJECT_PREFIX.len() + name.len());
        key.extend_from_slice(&self.root);
        key.extend_from_slice(OBJECT_PREFIX);
        key.extend_from_slice(name.as_bytes());
        key
    }

    fn token_key(&self, token: &[u8; 16]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.root.len() + TOKEN_PREFIX.len() + token.len());
        key.extend_from_slice(&self.root);
        key.extend_from_slice(TOKEN_PREFIX);
        key.extend_from_slice(token);
        key
    }

    fn note(&self, f: impl FnOnce(&mut TikvStats)) {
        f(&mut self.stats.lock().unwrap_or_else(|e| e.into_inner()));
    }

    fn options(&self, pessimistic: bool) -> TransactionOptions {
        let options = if pessimistic {
            TransactionOptions::new_pessimistic()
        } else {
            TransactionOptions::new_optimistic()
        }
        .drop_check(CheckLevel::None);
        if self.async_commit {
            options.use_async_commit().try_one_pc()
        } else {
            options
        }
    }

    /// A read-only snapshot at a fresh timestamp.
    async fn snapshot(&self) -> Result<tikv_client::Snapshot, StoreError> {
        let (client, generation) = self.clients.get();
        match tokio::time::timeout(self.request_timeout, client.current_timestamp()).await {
            Ok(Ok(ts)) => Ok(client.snapshot(
                ts,
                TransactionOptions::new_optimistic()
                    .read_only()
                    .drop_check(CheckLevel::None),
            )),
            Ok(Err(e)) => {
                self.after_error(&e, generation).await;
                Err(StoreError::Unavailable(format!("TiKV timestamp: {e}")))
            }
            Err(_) => Err(StoreError::Unavailable("TiKV timestamp: timed out".into())),
        }
    }

    /// Runs a read again on a fault until the run's deadline. A read changes
    /// nothing, so retrying it is always safe, and a busy cluster's slow TSO
    /// answer should not surface as a 503 when the next attempt would do.
    async fn read<T, F, Fut>(&self, mut op: F) -> Result<T, StoreError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, StoreError>>,
    {
        let deadline = Instant::now() + RUN_DEADLINE;
        let mut attempt = 1;
        loop {
            match op().await {
                Err(StoreError::Unavailable(why)) => {
                    attempt += 1;
                    let pause = backoff(attempt);
                    if Instant::now() + pause >= deadline {
                        return Err(StoreError::Unavailable(why));
                    }
                    tracing::debug!(attempt, why, "TiKV read retried");
                    tokio::time::sleep(pause).await;
                }
                other => return other,
            }
        }
    }

    async fn after_error(&self, e: &TikvError, generation: u64) {
        if classify(e) == Class::TsoClosed {
            self.clients.rebuild(generation).await;
            self.note(|s| s.client_rebuilds += 1);
        }
    }

    async fn rollback(&self, txn: &mut Transaction) {
        let _ = tokio::time::timeout(self.request_timeout, txn.rollback()).await;
    }

    /// One conditional write of `value` (revision ‖ body) at `key`, retried
    /// on conflicts until the run's deadline, its unknown outcome resolved
    /// through a commit token.
    async fn write(
        &self,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        condition: Condition,
    ) -> Result<(), StoreError> {
        let deadline = Instant::now() + RUN_DEADLINE;
        let mut last = String::from("deadline passed");
        let mut contended = false;
        for attempt in 1..=MAX_ATTEMPTS {
            if attempt > 1 {
                self.note(|s| s.restarts += 1);
                let pause = backoff(attempt);
                if Instant::now() + pause >= deadline {
                    break;
                }
                tokio::time::sleep(pause).await;
            }
            let token: [u8; 16] = std::array::from_fn(|_| fastrand::u8(..));
            match self
                .attempt(&key, value.as_deref(), condition, &token, deadline)
                .await
            {
                Outcome::Done(result) => return result,
                Outcome::Retry(why, conflict) => {
                    tracing::debug!(attempt, why, "TiKV write restarted");
                    last = why;
                    contended = conflict;
                }
                Outcome::Unknown => {
                    self.note(|s| s.unknown_outcomes += 1);
                    match self.resolve(&token).await {
                        // A resolved write that landed: the precondition held
                        // when it committed, so it succeeded.
                        Some(true) => return Ok(()),
                        Some(false) => last = "commit outcome unknown; token absent".into(),
                        // Unknown it stays. The caller retries the same
                        // conditional write and re-decides if it landed.
                        None => return Err(StoreError::Conflict),
                    }
                }
                Outcome::Fail(why) => return Err(StoreError::Unavailable(why)),
            }
        }
        if contended {
            // Lost every lock race this run had time for. Nothing landed, so
            // this is S3's `ConditionalRequestConflict`: the caller retries
            // the same conditional write (and re-decides if it now fails
            // its precondition).
            tracing::debug!(last, "TiKV write still contended; answering Conflict");
            return Err(StoreError::Conflict);
        }
        Err(StoreError::Unavailable(format!(
            "TiKV write gave up: {last}"
        )))
    }

    async fn attempt(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        condition: Condition,
        token: &[u8; 16],
        deadline: Instant,
    ) -> Outcome<Result<(), StoreError>> {
        let (client, generation) = self.clients.get();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let mut txn =
            match tokio::time::timeout(remaining, client.begin_with_options(self.options(true)))
                .await
            {
                Ok(Ok(txn)) => txn,
                Ok(Err(e)) => {
                    self.after_error(&e, generation).await;
                    return self.failed(&e);
                }
                Err(_) => return Outcome::Retry("TiKV begin timed out".into(), false),
            };
        let start_ms = physical_ms(&txn.start_timestamp());

        let remaining = deadline.saturating_duration_since(Instant::now());
        let body = async {
            // Locked even for a blind write, as 0.1.0 did: writers to one
            // key queue rather than meet at prewrite.
            let current = txn.get_for_update(Key::from(key.to_vec())).await?;
            let allowed = match condition {
                Condition::Overwrite => true,
                Condition::Absent => current.is_none(),
                Condition::Match(expected) => current
                    .as_deref()
                    .and_then(|bytes| bytes.get(..REVISION_BYTES))
                    .is_some_and(|actual| actual == expected.as_bytes()),
            };
            if !allowed {
                return Ok(false);
            }
            match value {
                Some(value) => txn.put(Key::from(key.to_vec()), value.to_vec()).await?,
                None => txn.delete(Key::from(key.to_vec())).await?,
            }
            txn.put(Key::from(self.token_key(token)), token_value(start_ms))
                .await?;
            Ok::<bool, TikvError>(true)
        };
        match tokio::time::timeout(remaining, body).await {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                self.rollback(&mut txn).await;
                return Outcome::Done(Err(StoreError::PreconditionFailed));
            }
            Ok(Err(e)) => {
                self.rollback(&mut txn).await;
                self.after_error(&e, generation).await;
                return self.failed(&e);
            }
            Err(_) => {
                self.rollback(&mut txn).await;
                return Outcome::Retry("TiKV write body timed out".into(), false);
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, txn.commit()).await {
            Ok(Ok(_)) => Outcome::Done(Ok(())),
            Ok(Err(e)) if classify(&e) == Class::Undetermined => Outcome::Unknown,
            Ok(Err(e)) => {
                self.rollback(&mut txn).await;
                self.after_error(&e, generation).await;
                self.failed(&e)
            }
            // The commit future is dropped and its requests may still land.
            Err(_) => Outcome::Unknown,
        }
    }

    fn failed<T>(&self, e: &TikvError) -> Outcome<T> {
        match classify(e) {
            Class::Conflict | Class::AlreadyExists => Outcome::Retry(e.to_string(), true),
            Class::NotApplied | Class::TsoClosed => Outcome::Retry(e.to_string(), false),
            Class::Undetermined => Outcome::Unknown,
            Class::Fatal => Outcome::Fail(e.to_string()),
        }
    }

    /// Whether the write that put `token` committed: `Some(true)` it did,
    /// `Some(false)` it did not and now never will, `None` undecided after
    /// two request timeouts.
    async fn resolve(&self, token: &[u8; 16]) -> Option<bool> {
        let give_up = Instant::now() + self.request_timeout * 2;
        while Instant::now() < give_up {
            match tokio::time::timeout(self.request_timeout, self.resolve_once(token)).await {
                Ok(Some(decided)) => return Some(decided),
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        None
    }

    async fn resolve_once(&self, token: &[u8; 16]) -> Option<bool> {
        let key = Key::from(self.token_key(token));
        let (client, _) = self.clients.get();
        let mut txn = client
            .begin_with_options(TransactionOptions::new_optimistic().drop_check(CheckLevel::None))
            .await
            .ok()?;
        let start_ms = physical_ms(&txn.start_timestamp());
        let found = match txn.get(key.clone()).await {
            Ok(found) => found,
            Err(_) => {
                let _ = txn.rollback().await;
                return None;
            }
        };
        match found {
            Some(v) => {
                let _ = txn.rollback().await;
                Some(!is_fence(&v))
            }
            None => {
                if txn.put(key, fence_value(start_ms)).await.is_err() {
                    let _ = txn.rollback().await;
                    return None;
                }
                match txn.commit().await {
                    Ok(_) => Some(false),
                    Err(e) => {
                        if classify(&e) != Class::Undetermined {
                            let _ = txn.rollback().await;
                        }
                        None
                    }
                }
            }
        }
    }

    async fn conditional(
        &self,
        name: &str,
        body: Vec<u8>,
        condition: Condition,
    ) -> Result<Etag, StoreError> {
        let revision = Uuid::new_v4();
        let mut value = Vec::with_capacity(REVISION_BYTES + body.len());
        value.extend_from_slice(revision.as_bytes());
        value.extend_from_slice(&body);
        self.write(self.object_key(name), Some(value), condition)
            .await?;
        Ok(Etag(revision.to_string()))
    }

    /// Deletes the commit tokens and fences under this store's root that
    /// expired. Returns how many went.
    pub async fn sweep_expired_tokens(&self) -> Result<u64, StoreError> {
        let mut start = [&self.root[..], TOKEN_PREFIX].concat();
        let end = successor(&start);
        let mut swept = 0;
        loop {
            let mut snapshot = self.snapshot().await?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            let page: Vec<(Vec<u8>, Vec<u8>)> = snapshot
                .scan(start.clone()..end.clone(), TOKEN_SWEEP_PAGE)
                .await
                .map_err(|e| StoreError::Unavailable(format!("TiKV token scan: {e}")))?
                .map(|pair| (Vec::<u8>::from(pair.key().clone()), pair.value().clone()))
                .collect();
            let expired: Vec<Vec<u8>> = page
                .iter()
                .filter(|(_, value)| token_expiry(value).is_some_and(|until| until < now_ms))
                .map(|(key, _)| key.clone())
                .collect();
            if !expired.is_empty() {
                // No commit token here: a sweep whose outcome is lost is
                // simply done again by the next one. A token is never
                // rewritten, so re-checking expiry inside the transaction
                // is all the care a delete needs.
                swept += self.delete_expired(expired).await?;
            }
            match page.last() {
                Some((last, _)) if page.len() == TOKEN_SWEEP_PAGE as usize => {
                    start = last.clone();
                    start.push(0);
                }
                _ => return Ok(swept),
            }
        }
    }
}

impl TikvStore {
    async fn delete_expired(&self, keys: Vec<Vec<u8>>) -> Result<u64, StoreError> {
        let (client, generation) = self.clients.get();
        let fail = |e: TikvError| StoreError::Unavailable(format!("TiKV token sweep: {e}"));
        let mut txn = match client.begin_with_options(self.options(false)).await {
            Ok(txn) => txn,
            Err(e) => {
                self.after_error(&e, generation).await;
                return Err(fail(e));
            }
        };
        let now_ms = physical_ms(&txn.start_timestamp());
        let mut removed = 0;
        let found = match txn.batch_get(keys.into_iter().map(Key::from)).await {
            Ok(found) => found.collect::<Vec<_>>(),
            Err(e) => {
                self.rollback(&mut txn).await;
                return Err(fail(e));
            }
        };
        for pair in found {
            if token_expiry(pair.value()).is_some_and(|until| until < now_ms) {
                if let Err(e) = txn.delete(pair.key().clone()).await {
                    self.rollback(&mut txn).await;
                    return Err(fail(e));
                }
                removed += 1;
            }
        }
        match txn.commit().await {
            Ok(_) => Ok(removed),
            Err(e) => {
                if classify(&e) != Class::Undetermined {
                    self.rollback(&mut txn).await;
                }
                Err(fail(e))
            }
        }
    }
}

#[async_trait]
impl Store for TikvStore {
    async fn get(&self, name: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        let key = self.object_key(name);
        let value = self
            .read(|| async {
                let mut snapshot = self.snapshot().await?;
                tokio::time::timeout(self.request_timeout, snapshot.get(Key::from(key.clone())))
                    .await
                    .map_err(|_| StoreError::Unavailable("TiKV get timed out".into()))?
                    .map_err(|e| StoreError::Unavailable(format!("TiKV get: {e}")))
            })
            .await?;
        value.map(decode).transpose()
    }

    async fn put_if_match(
        &self,
        name: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        let expected = Uuid::parse_str(&etag.0).map_err(|_| StoreError::PreconditionFailed)?;
        self.conditional(name, body, Condition::Match(expected))
            .await
    }

    async fn put_if_none_match(&self, name: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.conditional(name, body, Condition::Absent).await
    }

    async fn put(&self, name: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.conditional(name, body, Condition::Overwrite).await
    }

    async fn delete(&self, name: &str) -> Result<(), StoreError> {
        self.write(self.object_key(name), None, Condition::Overwrite)
            .await
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        let start = self.object_key(prefix);
        let end = successor(&start);
        let strip = self.root.len() + OBJECT_PREFIX.len();
        let limit = u32::try_from(max_keys).unwrap_or(u32::MAX);
        let keys: Vec<Key> = self
            .read(|| async {
                let range: tikv_client::BoundRange = if end.is_empty() {
                    (Key::from(start.clone())..).into()
                } else {
                    (Key::from(start.clone())..Key::from(end.clone())).into()
                };
                let mut snapshot = self.snapshot().await?;
                tokio::time::timeout(self.request_timeout, snapshot.scan_keys(range, limit))
                    .await
                    .map_err(|_| StoreError::Unavailable("TiKV scan timed out".into()))?
                    .map(|keys| keys.collect())
                    .map_err(|e| StoreError::Unavailable(format!("TiKV scan: {e}")))
            })
            .await?;
        keys.into_iter()
            .map(|key| {
                let key: Vec<u8> = key.into();
                let name = key.get(strip..).ok_or_else(|| {
                    StoreError::Unavailable("TiKV returned a key outside the root".into())
                })?;
                String::from_utf8(name.to_vec())
                    .map_err(|e| StoreError::Unavailable(format!("invalid object key: {e}")))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_its_revision_then_its_body() {
        let id = Uuid::new_v4();
        let mut value = id.as_bytes().to_vec();
        value.extend_from_slice(b"payload");
        assert_eq!(
            decode(value),
            Ok((b"payload".to_vec(), Etag(id.to_string())))
        );
        assert!(matches!(
            decode(vec![0; 15]),
            Err(StoreError::Unavailable(_))
        ));
    }

    #[test]
    fn tokens_and_fences_carry_their_expiry() {
        let v = token_value(1_000);
        assert_eq!(token_expiry(&v), Some(1_000 + 30 * 60 * 1000));
        assert_eq!(token_expiry(b"short"), None);
        let f = fence_value(1_000);
        assert!(is_fence(&f) && !is_fence(&v));
        assert_eq!(token_expiry(&f), token_expiry(&v));
    }

    #[test]
    fn successor_bounds_a_prefix() {
        assert_eq!(successor(b"ab"), b"ac");
        assert_eq!(successor(b"a\xff"), b"b");
        assert_eq!(successor(b"\xff\xff"), b"");
    }
}
