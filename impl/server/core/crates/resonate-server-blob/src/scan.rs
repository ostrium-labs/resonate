//! Whole-store reads: the three searches and `debug.snap`.
//!
//! # Contract
//!
//! On a store with an index ([`crate::index`], TiKV), `promise.search` and
//! `task.search` read the index: O(results). Everywhere else — an object
//! store, `schedule.search`, `debug.snap`, and a store whose index is not
//! built yet — this is a LIST of the prefix followed by a decode of each
//! object, filtered and projected in memory. That is O(origins) GETs per
//! query, which is honest for a small deployment and for the differential
//! suite, and plainly wrong for a large one — the production caveat, stated
//! rather than hidden (and why the searches can be disabled by config).
//!
//! An index without its marker (a store written before the index existed)
//! is answered by the scan, and the first such search starts the one-time
//! rebuild in the background; once the marker is written, searches move to
//! the index. Both paths answer the same pages.
//!
//! Reads are cache-first but never *write* the cache. Only the applier does
//! that, so a scan racing a write cannot leave a stale entry behind for the
//! writer to trip over.
//!
//! A search does not sweep — it reads every origin, and sweeping each would
//! be a write per origin — but it answers in effective state: a promise whose
//! deadline has passed in an origin nothing has swept since is projected
//! before it is filtered or returned, so what a search says never depends on
//! whether an expiry has been written down yet. The SQL backends answer the
//! same way, through the same rule (`PromiseRecord::project`).
//!
//! # Dependencies
//!
//! The store and the codec to list and read every document, the cache
//! (read-only), the schedule service for `schedule.search`, and the sender for
//! the `messages` half of `debug.snap`.
//!
//! # Dependants
//!
//! `Server` alone, which routes `promise.search`, `task.search`,
//! `schedule.search` and `debug.snap` here.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use crate::kernel::state::{OriginDoc, Reply};
use resonate_core::types::{
    PromiseRecord, PromiseSearchData, PromiseSearchResponseData, ScheduleRecord,
    ScheduleSearchData, ScheduleSearchResponseData, Snapshot, SnapshotCallback, SnapshotListener,
    SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskRecord, TaskSearchData,
    TaskSearchResponseData,
};
use resonate_core::Unavailable;

use super::applier::KeySpace;
use super::cache::DocCache;
use super::codec;
use super::index;
use super::schedules::ScheduleService;
use super::sender::Sender;
use super::store::{Store, StoreError, StoreIndex};

/// The largest page any search will return, matching the SQL handlers.
const MAX_LIMIT: i64 = 1_000;

/// Where the index stands, as this process last saw it.
const INDEX_UNKNOWN: u8 = 0;
const INDEX_BUILDING: u8 = 1;
const INDEX_BUILT: u8 = 2;

pub struct ScanService {
    store: Arc<dyn Store>,
    cache: Arc<dyn DocCache>,
    schedules: Arc<ScheduleService>,
    sender: Arc<Sender>,
    keys: KeySpace,
    index_state: Arc<AtomicU8>,
}

impl ScanService {
    pub fn new(
        store: Arc<dyn Store>,
        cache: Arc<dyn DocCache>,
        schedules: Arc<ScheduleService>,
        sender: Arc<Sender>,
        keys: KeySpace,
    ) -> Self {
        Self {
            store,
            cache,
            schedules,
            sender,
            keys,
            index_state: Arc::new(AtomicU8::new(INDEX_UNKNOWN)),
        }
    }

    // -----------------------------------------------------------------------
    // The index
    // -----------------------------------------------------------------------

    /// The index, if the store has one and it is built. If it has one that
    /// is not, the rebuild is started (once) and the caller scans.
    async fn ready_index(&self) -> Option<&dyn StoreIndex> {
        let ix = self.store.index()?;
        match self.index_state.load(Ordering::Acquire) {
            INDEX_BUILT => return Some(ix),
            INDEX_BUILDING => return None,
            _ => {}
        }
        match index::is_built(ix, &self.keys).await {
            Ok(true) => {
                self.index_state.store(INDEX_BUILT, Ordering::Release);
                Some(ix)
            }
            Ok(false) => {
                self.spawn_rebuild();
                None
            }
            Err(error) => {
                tracing::warn!(%error, "Index marker unreadable; searching by scan");
                None
            }
        }
    }

    fn spawn_rebuild(&self) {
        if self
            .index_state
            .compare_exchange(
                INDEX_UNKNOWN,
                INDEX_BUILDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let store = Arc::clone(&self.store);
        let keys = self.keys.clone();
        let state = Arc::clone(&self.index_state);
        tokio::spawn(async move {
            let result = run_rebuild(&store, &keys).await;
            settle_rebuild(&state, result);
        });
    }

    /// Builds the index now, if the store has one and it is not built:
    /// what the plugin runs at startup, and tests run to be sure which path
    /// a search takes. Safe beside writers and beside a rebuild elsewhere.
    pub async fn build_index(&self) -> Result<(), Unavailable> {
        let Some(ix) = self.store.index() else {
            return Ok(());
        };
        if index::is_built(ix, &self.keys)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?
        {
            self.index_state.store(INDEX_BUILT, Ordering::Release);
            return Ok(());
        }
        let documents = run_rebuild(&self.store, &self.keys)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        tracing::info!(documents, "Search index built");
        self.index_state.store(INDEX_BUILT, Ordering::Release);
        Ok(())
    }

    /// `debug.reset`, after the documents are gone: drop every entry, and
    /// mark the (now empty) store indexed.
    pub async fn reset_index(&self) -> Result<(), Unavailable> {
        let Some(ix) = self.store.index() else {
            return Ok(());
        };
        index::reset(ix, &self.keys)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        self.index_state.store(INDEX_BUILT, Ordering::Release);
        Ok(())
    }

    /// Whether searches read the index (`true`) or scan (`false`) right now.
    pub fn index_in_use(&self) -> bool {
        self.store.index().is_some() && self.index_state.load(Ordering::Acquire) == INDEX_BUILT
    }

    /// Every origin document, by origin.
    async fn documents(&self) -> Result<BTreeMap<String, OriginDoc>, Unavailable> {
        let keys = self
            .store
            .list(&self.keys.doc_prefix(), usize::MAX)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        let mut out = BTreeMap::new();
        for key in keys {
            let origin = match self.keys.origin_of_doc_key(&key) {
                Some(origin) => origin,
                None => continue,
            };
            if let Some(cached) = self.cache.get(&origin) {
                out.insert(origin, cached.doc.as_ref().clone());
                continue;
            }
            match self.store.get(&key).await {
                Ok(Some((bytes, _))) => {
                    let doc = codec::decode(&bytes, &origin)
                        .map_err(|e| Unavailable::new(format!("document {key} unreadable: {e}")))?;
                    out.insert(origin, doc);
                }
                // Deleted between the listing and the read. It is simply gone.
                Ok(None) => {}
                Err(e) => return Err(Unavailable::new(e.to_string())),
            }
        }
        Ok(out)
    }

    /// Every promise, by id, across every origin.
    async fn promises(&self) -> Result<Vec<(String, PromiseRecord)>, Unavailable> {
        let mut out: Vec<(String, PromiseRecord)> = Vec::new();
        for doc in self.documents().await?.values() {
            for (id, p) in &doc.promises {
                out.push((id.clone(), p.to_record(id)));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Searches
    // -----------------------------------------------------------------------

    /// Search by effective state at `now`: every record is projected before
    /// it is filtered, so a pending promise past its deadline in an origin
    /// nothing has swept yet neither fills a "pending" page nor reads as
    /// pending on any other.
    pub async fn search_promises(
        &self,
        q: &PromiseSearchData,
        now: i64,
    ) -> Result<Reply, Unavailable> {
        let limit = match resolve_limit(q.limit, 100) {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };
        if let Some(ix) = self.ready_index().await {
            let mut view = ix.view().await.map_err(unavailable)?;
            let found = index::search_promises(view.as_mut(), &self.keys, q, page_len(limit), now)
                .await
                .map_err(unavailable)?;
            let (promises, cursor) = paginate(found, None, limit, |p| &p.id);
            return Ok(Reply::ok(&PromiseSearchResponseData { promises, cursor }));
        }
        let matches: Vec<PromiseRecord> = self
            .promises()
            .await?
            .into_iter()
            .map(|(_, mut p)| {
                p.project(now);
                p
            })
            .filter(|p| q.state.map(|s| p.state == s).unwrap_or(true))
            .filter(|p| match &q.tags {
                Some(want) => want
                    .iter()
                    .all(|(k, v)| p.tags.get(k).is_some_and(|got| got == v)),
                None => true,
            })
            .collect();
        let (promises, cursor) = paginate(matches, q.cursor.as_deref(), limit, |p| &p.id);
        Ok(Reply::ok(&PromiseSearchResponseData { promises, cursor }))
    }

    pub async fn search_tasks(&self, q: &TaskSearchData) -> Result<Reply, Unavailable> {
        let limit = match resolve_limit(q.limit, 100) {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };
        if let Some(ix) = self.ready_index().await {
            let mut view = ix.view().await.map_err(unavailable)?;
            let found = index::search_tasks(view.as_mut(), &self.keys, q, page_len(limit))
                .await
                .map_err(unavailable)?;
            let (tasks, cursor) = paginate(found, None, limit, |t| &t.id);
            return Ok(Reply::ok(&TaskSearchResponseData { tasks, cursor }));
        }
        let mut matches: Vec<TaskRecord> = Vec::new();
        for doc in self.documents().await?.values() {
            for (id, t) in &doc.tasks {
                if q.state.map(|s| t.state == s).unwrap_or(true) {
                    matches.push(t.to_record(id));
                }
            }
        }
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        let (tasks, cursor) = paginate(matches, q.cursor.as_deref(), limit, |t| &t.id);
        Ok(Reply::ok(&TaskSearchResponseData { tasks, cursor }))
    }

    pub async fn search_schedules(&self, q: &ScheduleSearchData) -> Result<Reply, Unavailable> {
        // Ten, not a hundred: op_schedule_search's default.
        let limit = match resolve_limit(q.limit, 10) {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };
        let mut matches: Vec<ScheduleRecord> = self
            .schedules
            .list_all()
            .await?
            .into_iter()
            .map(|(id, doc)| doc.to_record(&id))
            .filter(|s| match &q.tags {
                Some(want) => want
                    .iter()
                    .all(|(k, v)| s.promise_tags.get(k).is_some_and(|got| got == v)),
                None => true,
            })
            .collect();
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        let (schedules, cursor) = paginate(matches, q.cursor.as_deref(), limit, |s| &s.id);
        Ok(Reply::ok(&ScheduleSearchResponseData { schedules, cursor }))
    }

    // -----------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------

    /// The whole store, in the shape `debug.snap` compares.
    ///
    /// Each field is the projection its SQL counterpart makes, including the
    /// ones that are not simply "all of them": `promiseTimeouts` holds only
    /// deadlines that are actually armed, `callbacks` holds only the
    /// registrations that have *not* fired (`WHERE NOT ready` — a fired one is
    /// visible as a task's `resumes` count instead), and a task's `ttl` and
    /// `pid` appear only while it holds a lease.
    pub async fn snapshot(&self) -> Result<Snapshot, Unavailable> {
        let docs = self.documents().await?;

        let mut promises: Vec<PromiseRecord> = Vec::new();
        let mut promise_timeouts: Vec<SnapshotPromiseTimeout> = Vec::new();
        let mut callbacks: Vec<SnapshotCallback> = Vec::new();
        let mut listeners: Vec<SnapshotListener> = Vec::new();
        let mut tasks: Vec<TaskRecord> = Vec::new();
        let mut task_timeouts: Vec<SnapshotTaskTimeout> = Vec::new();

        for doc in docs.values() {
            for (id, p) in &doc.promises {
                promises.push(p.to_record(id));
                if p.timeout_armed() {
                    promise_timeouts.push(SnapshotPromiseTimeout {
                        id: id.clone(),
                        timeout: p.timeout_at,
                    });
                }
                for awaiter in &p.callbacks {
                    callbacks.push(SnapshotCallback {
                        awaiter: awaiter.clone(),
                        awaited: id.clone(),
                    });
                }
                for address in &p.listeners {
                    listeners.push(SnapshotListener {
                        promise_id: id.clone(),
                        address: address.clone(),
                    });
                }
            }
            for (id, t) in &doc.tasks {
                tasks.push(t.to_record(id));
                if let Some(at) = t.retry_at {
                    task_timeouts.push(SnapshotTaskTimeout {
                        id: id.clone(),
                        timeout_type: 0,
                        timeout: at,
                    });
                }
                if let Some(at) = t.lease_at {
                    task_timeouts.push(SnapshotTaskTimeout {
                        id: id.clone(),
                        timeout_type: 1,
                        timeout: at,
                    });
                }
            }
        }

        promises.sort_by(|a, b| a.id.cmp(&b.id));
        promise_timeouts.sort_by(|a, b| a.id.cmp(&b.id));
        callbacks.sort_by(|a, b| a.awaiter.cmp(&b.awaiter).then(a.awaited.cmp(&b.awaited)));
        listeners.sort_by(|a, b| {
            a.promise_id
                .cmp(&b.promise_id)
                .then(a.address.cmp(&b.address))
        });
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        task_timeouts.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(Snapshot {
            promises,
            promise_timeouts,
            callbacks,
            listeners,
            tasks,
            task_timeouts,
            messages: self.sender.snapshot(),
        })
    }
}

fn unavailable(e: StoreError) -> Unavailable {
    Unavailable::new(e.to_string())
}

/// A page's length as the index reads it. A limit below one returns
/// nothing, as `LIMIT 0` does.
fn page_len(limit: i64) -> usize {
    usize::try_from(limit.max(0)).unwrap_or(0)
}

async fn run_rebuild(store: &Arc<dyn Store>, keys: &KeySpace) -> Result<u64, StoreError> {
    let ix = store
        .index()
        .ok_or_else(|| StoreError::Unavailable("the store has no index".into()))?;
    tracing::info!("Building the search index");
    index::rebuild(store.as_ref(), ix, keys).await
}

fn settle_rebuild(state: &AtomicU8, result: Result<u64, StoreError>) {
    match result {
        Ok(documents) => {
            tracing::info!(documents, "Search index built");
            state.store(INDEX_BUILT, Ordering::Release);
        }
        Err(error) => {
            // The next search starts it again.
            tracing::warn!(%error, "Building the search index failed; searching by scan");
            let _ = state.compare_exchange(
                INDEX_BUILDING,
                INDEX_UNKNOWN,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

/// A page limit, or the 400 the SQL handlers give.
fn resolve_limit(limit: Option<i64>, default: i64) -> Result<i64, Reply> {
    match limit {
        Some(n) if n > MAX_LIMIT => Err(Reply::err(
            400,
            "Invalid 'limit' — must be between 1 and 1000",
        )),
        Some(n) => Ok(n),
        None => Ok(default),
    }
}

/// Take one page after `cursor`, reporting the next cursor only when there is
/// more — `id > cursor`, ordered by id, exactly as the SQL queries page.
fn paginate<T>(
    items: Vec<T>,
    cursor: Option<&str>,
    limit: i64,
    id_of: impl Fn(&T) -> &String,
) -> (Vec<T>, Option<String>) {
    let start = match cursor {
        Some(c) => items
            .iter()
            .position(|item| id_of(item).as_str() > c)
            .unwrap_or(items.len()),
        None => 0,
    };
    let limit = limit.max(0) as usize;
    let mut page: Vec<T> = items.into_iter().skip(start).take(limit + 1).collect();
    let has_more = page.len() > limit;
    page.truncate(limit);
    let next = if has_more {
        page.last().map(|item| id_of(item).clone())
    } else {
        None
    };
    (page, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applier::{ApplierCfg, OriginActors};
    use crate::cache::MemDocCache;
    use crate::kernel::state::Req;
    use crate::sender::NullRouter;
    use crate::store::ObjectStoreAdapter;
    use serde_json::{json, Value};

    const W: &str = "http://worker:9999";
    const PID: &str = "pid-1";
    const TTL: i64 = 60_000;

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    struct Rig {
        applier: Arc<OriginActors>,
        schedules: Arc<ScheduleService>,
        scan: ScanService,
    }

    fn rig() -> Rig {
        let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(64));
        let sender = Arc::new(Sender::new(Arc::new(NullRouter), true));
        let applier = Arc::new(OriginActors::new(
            Arc::clone(&store),
            Arc::clone(&cache),
            Arc::clone(&sender),
            Arc::new(crate::timer_queue::TimerQueue::new()),
            keys(),
            ApplierCfg::default(),
        ));
        let schedules = Arc::new(ScheduleService::new(
            Arc::clone(&store),
            Arc::clone(&applier),
            Arc::new(crate::timer_queue::TimerQueue::new()),
            keys(),
        ));
        let scan = ScanService::new(store, cache, Arc::clone(&schedules), sender, keys());
        Rig {
            applier,
            schedules,
            scan,
        }
    }

    async fn submit(r: &Rig, origin: &str, req: Req, now: i64) -> Reply {
        r.applier.submit(origin, req, now).await.unwrap()
    }

    fn parse<T: serde::de::DeserializeOwned>(v: Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    /// Build a kernel request from a protocol kind and its data.
    fn to_req(kind: &str, data: Value) -> Req {
        match kind {
            "promise.create" => Req::PromiseCreate(parse(data)),
            "promise.settle" => Req::PromiseSettle(parse(data)),
            "promise.register_callback" => Req::PromiseRegisterCallback(parse(data)),
            "promise.register_listener" => Req::PromiseRegisterListener(parse(data)),
            "task.create" => Req::TaskCreate(parse(data)),
            "task.acquire" => Req::TaskAcquire(parse(data)),
            "task.suspend" => Req::TaskSuspend(parse(data)),
            other => panic!("unhandled kind {other}"),
        }
    }

    #[tokio::test]
    async fn an_empty_store_snapshots_to_empty() {
        let r = rig();
        let snap = r.scan.snapshot().await.unwrap();
        assert!(snap.promises.is_empty());
        assert!(snap.tasks.is_empty());
        assert!(snap.messages.is_empty());
    }

    #[tokio::test]
    async fn a_snapshot_spans_every_origin() {
        let r = rig();
        for origin in ["alpha", "beta", "gamma"] {
            submit(
                &r,
                origin,
                to_req(
                    "promise.create",
                    json!({ "id": format!("{origin}:a"), "timeoutAt": 100_000,
                            "param": {}, "tags": {} }),
                ),
                0,
            )
            .await;
        }
        let snap = r.scan.snapshot().await.unwrap();
        assert_eq!(
            snap.promises
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha:a", "beta:a", "gamma:a"],
            "ordered by id across origins"
        );
    }

    // --- searches ---------------------------------------------------------

    async fn with_promises(r: &Rig) {
        for (n, tags) in [
            (0, json!({})),
            (1, json!({ "resonate:target": W })),
            (2, json!({ "k": "v" })),
            (3, json!({})),
        ] {
            submit(
                r,
                "o",
                to_req(
                    "promise.create",
                    json!({ "id": format!("o:p{n}"), "timeoutAt": 500_000,
                            "param": {}, "tags": tags }),
                ),
                1_000,
            )
            .await;
        }
        submit(
            r,
            "o",
            to_req(
                "promise.settle",
                json!({ "id": "o:p0", "state": "resolved", "value": {} }),
            ),
            2_000,
        )
        .await;
    }

    #[tokio::test]
    async fn searching_promises_filters_by_state() {
        let r = rig();
        with_promises(&r).await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "state": "resolved", "limit": 10 })), 0)
            .await
            .unwrap();
        assert_eq!(reply.status, 200);
        let ids: Vec<&str> = reply.data["promises"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["o:p0"]);
    }

    #[tokio::test]
    async fn searching_promises_requires_every_filter_tag_to_match() {
        let r = rig();
        with_promises(&r).await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "tags": { "k": "v" }, "limit": 10 })), 0)
            .await
            .unwrap();
        assert_eq!(reply.data["promises"].as_array().unwrap().len(), 1);
        let reply = r
            .scan
            .search_promises(
                &parse(json!({ "tags": { "k": "v", "other": "x" }, "limit": 10 })),
                0,
            )
            .await
            .unwrap();
        assert!(reply.data["promises"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_search_pages_by_id_and_reports_a_cursor_only_when_there_is_more() {
        let r = rig();
        with_promises(&r).await;
        let first = r
            .scan
            .search_promises(&parse(json!({ "limit": 2 })), 0)
            .await
            .unwrap();
        assert_eq!(
            first.data["promises"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["o:p0", "o:p1"]
        );
        assert_eq!(first.data["cursor"], "o:p1");

        let second = r
            .scan
            .search_promises(&parse(json!({ "limit": 2, "cursor": "o:p1" })), 0)
            .await
            .unwrap();
        assert_eq!(
            second.data["promises"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["o:p2", "o:p3"]
        );
        assert!(
            second.data.get("cursor").is_none(),
            "the last page has none"
        );
    }

    #[tokio::test]
    async fn a_cursor_past_the_end_returns_nothing() {
        let r = rig();
        with_promises(&r).await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "limit": 10, "cursor": "zzz" })), 0)
            .await
            .unwrap();
        assert!(reply.data["promises"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_oversized_limit_is_a_400() {
        let r = rig();
        for reply in [
            r.scan
                .search_promises(&parse(json!({ "limit": 1_001 })), 0)
                .await
                .unwrap(),
            r.scan
                .search_tasks(&parse(json!({ "limit": 1_001 })))
                .await
                .unwrap(),
            r.scan
                .search_schedules(&parse(json!({ "limit": 1_001 })))
                .await
                .unwrap(),
        ] {
            assert_eq!(reply.status, 400);
            assert_eq!(
                reply.data,
                json!("Invalid 'limit' — must be between 1 and 1000")
            );
        }
    }

    #[tokio::test]
    async fn searching_tasks_filters_by_state() {
        let r = rig();
        submit(
            &r,
            "o",
            to_req(
                "promise.create",
                json!({ "id": "o:a", "timeoutAt": 500_000, "param": {},
                        "tags": { "resonate:target": W } }),
            ),
            1_000,
        )
        .await;
        submit(
            &r,
            "o",
            to_req(
                "task.create",
                json!({ "pid": PID, "ttl": TTL, "action": {
                    "kind": "promise.create", "head": {}, "data": {
                        "id": "o:b", "timeoutAt": 500_000, "param": {},
                        "tags": { "resonate:target": W } } } }),
            ),
            1_000,
        )
        .await;
        let pending = r
            .scan
            .search_tasks(&parse(json!({ "state": "pending", "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(
            pending.data["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["o:a"]
        );
        let all = r
            .scan
            .search_tasks(&parse(json!({ "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(all.data["tasks"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn searching_schedules_defaults_to_ten_per_page() {
        let r = rig();
        for n in 0..12 {
            let d = json!({
                "id": format!("s{n:02}"), "cron": "* * * * *",
                "promiseId": "p-{{.id}}", "promiseTimeout": 60_000,
                "promiseParam": {}, "promiseTags": { "resonate:target": W }
            });
            r.schedules.create(&parse(d), 1_000).await.unwrap();
        }
        let reply = r.scan.search_schedules(&parse(json!({}))).await.unwrap();
        assert_eq!(reply.data["schedules"].as_array().unwrap().len(), 10);
        assert_eq!(reply.data["cursor"], "s09");
    }

    #[tokio::test]
    async fn searching_schedules_filters_by_promise_tags() {
        let r = rig();
        for (n, extra) in [(0, json!({})), (1, json!({ "team": "core" }))] {
            let mut tags = serde_json::Map::new();
            tags.insert("resonate:target".into(), json!(W));
            if let Some(obj) = extra.as_object() {
                for (k, v) in obj {
                    tags.insert(k.clone(), v.clone());
                }
            }
            let d = json!({
                "id": format!("s{n}"), "cron": "* * * * *",
                "promiseId": "p-{{.id}}", "promiseTimeout": 60_000,
                "promiseParam": {}, "promiseTags": Value::Object(tags)
            });
            r.schedules.create(&parse(d), 1_000).await.unwrap();
        }
        let reply = r
            .scan
            .search_schedules(&parse(json!({ "tags": { "team": "core" } })))
            .await
            .unwrap();
        assert_eq!(
            reply.data["schedules"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["s1"]
        );
    }

    #[tokio::test]
    async fn a_search_reports_stored_state_not_effective_state() {
        // The deadline has passed but nothing has named the promise, so no row
        // has changed — the SQL backends read the same way.
        let r = rig();
        submit(
            &r,
            "o",
            to_req(
                "promise.create",
                json!({ "id": "o:a", "timeoutAt": 5_000, "param": {}, "tags": {} }),
            ),
            1_000,
        )
        .await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "state": "pending", "limit": 10 })), 0)
            .await
            .unwrap();
        assert_eq!(reply.data["promises"].as_array().unwrap().len(), 1);
    }
}

/// The indexed searches against the scan, over one store.
#[cfg(test)]
mod indexed_tests {
    use super::*;
    use crate::applier::{ApplierCfg, OriginActors};
    use crate::cache::MemDocCache;
    use crate::kernel::state::Req;
    use crate::sender::NullRouter;
    use crate::store::{CountingStore, Etag, FaultStore, MemStore, StoreError};
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::collections::BTreeSet;

    const W: &str = "http://worker:9999";

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    /// The same store with its index hidden: what an object store sees, and
    /// what a server from before the index writes.
    struct NoIndex(Arc<dyn Store>);

    #[async_trait]
    impl Store for NoIndex {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
            self.0.get(key).await
        }
        async fn put_if_match(
            &self,
            key: &str,
            body: Vec<u8>,
            etag: &Etag,
        ) -> Result<Etag, StoreError> {
            self.0.put_if_match(key, body, etag).await
        }
        async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
            self.0.put_if_none_match(key, body).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
            self.0.put(key, body).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.0.delete(key).await
        }
        async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
            self.0.list(prefix, max_keys).await
        }
    }

    fn actors(store: Arc<dyn Store>) -> Arc<OriginActors> {
        Arc::new(OriginActors::new(
            store,
            Arc::new(MemDocCache::new(64)),
            Arc::new(Sender::new(Arc::new(NullRouter), true)),
            Arc::new(crate::timer_queue::TimerQueue::new()),
            keys(),
            ApplierCfg::default(),
        ))
    }

    fn scan_over(store: Arc<dyn Store>) -> ScanService {
        let applier = actors(Arc::clone(&store));
        let schedules = Arc::new(ScheduleService::new(
            Arc::clone(&store),
            applier,
            Arc::new(crate::timer_queue::TimerQueue::new()),
            keys(),
        ));
        ScanService::new(
            store,
            Arc::new(MemDocCache::new(64)),
            schedules,
            Arc::new(Sender::new(Arc::new(NullRouter), true)),
            keys(),
        )
    }

    fn parse<T: serde::de::DeserializeOwned>(v: Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    fn create(id: &str, timeout_at: i64, tags: Value) -> Req {
        Req::PromiseCreate(parse(
            json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags }),
        ))
    }

    /// A seeded workload over many origins: tagged, external, timer and
    /// runnable promises, tasks, settlements, sweeps, and one id too long
    /// for an entry. Returns the time it ended at.
    async fn workload(applier: &OriginActors, seed: u64, steps: usize) -> i64 {
        let mut rng = fastrand::Rng::with_seed(seed);
        let mut now = 1_000_000;
        let long = format!("o3:{}", "x".repeat(crate::index::MAX_ID + 10));
        for _ in 0..steps {
            now += rng.i64(0..2_000);
            let origin = format!("o{}", rng.u32(0..12));
            let id = if rng.u32(0..40) == 0 {
                long.clone()
            } else {
                format!("{origin}:p{}", rng.u32(0..6))
            };
            let origin = crate::index::origin_of(&id).to_string();
            let req = match rng.u32(0..10) {
                0..=4 => {
                    let mut tags = serde_json::Map::new();
                    if rng.bool() {
                        tags.insert("loams:run".into(), json!(format!("r{}", rng.u32(0..3))));
                    }
                    match rng.u32(0..5) {
                        0 => {
                            tags.insert("resonate:external".into(), json!("true"));
                        }
                        1 => {
                            tags.insert("resonate:timer".into(), json!("true"));
                        }
                        2 => {
                            tags.insert("resonate:target".into(), json!(W));
                        }
                        _ => {}
                    }
                    create(&id, now + rng.i64(1..60_000), Value::Object(tags))
                }
                5..=6 => {
                    let state = ["resolved", "rejected", "rejected_canceled"][rng.usize(0..3)];
                    Req::PromiseSettle(parse(
                        json!({ "id": id, "state": state, "value": {} }),
                    ))
                }
                7 => Req::TaskCreate(parse(json!({
                    "pid": "pid", "ttl": 60_000, "action": {
                        "kind": "promise.create", "head": {}, "data": {
                            "id": id, "timeoutAt": now + 90_000, "param": {},
                            "tags": { "resonate:target": W } } } }))),
                _ => {
                    applier.tick(&origin, now).await.unwrap();
                    continue;
                }
            };
            applier.submit(&origin, req, now).await.unwrap();
        }
        now
    }

    fn promise_queries() -> Vec<Value> {
        let states = [
            None,
            Some("pending"),
            Some("resolved"),
            Some("rejected"),
            Some("rejected_canceled"),
            Some("rejected_timedout"),
        ];
        let tags = [
            None,
            Some(json!({ "loams:run": "r1" })),
            Some(json!({ "loams:run": "r1", "resonate:external": "true" })),
            Some(json!({ "resonate:timer": "true" })),
            Some(json!({ "resonate:target": W })),
            Some(json!({ "nobody": "has" })),
        ];
        let mut out = Vec::new();
        for state in states {
            for tag in &tags {
                for limit in [1, 4, 100] {
                    let mut q = json!({ "limit": limit });
                    if let Some(s) = state {
                        q["state"] = json!(s);
                    }
                    if let Some(t) = tag {
                        q["tags"] = t.clone();
                    }
                    out.push(q);
                }
            }
        }
        out
    }

    /// Every page of `q`, following the cursor.
    async fn pages(scan: &ScanService, q: &Value, now: i64, tasks: bool) -> Vec<Value> {
        let mut out = Vec::new();
        let mut q = q.clone();
        loop {
            let reply = if tasks {
                scan.search_tasks(&parse(q.clone())).await.unwrap()
            } else {
                scan.search_promises(&parse(q.clone()), now).await.unwrap()
            };
            assert_eq!(reply.status, 200);
            let cursor = reply.data.get("cursor").cloned();
            out.push(reply.data);
            match cursor {
                Some(c) => q["cursor"] = c,
                None => return out,
            }
            assert!(out.len() < 1_000, "a search that never ends");
        }
    }

    /// The entries every document implies, from the documents themselves.
    async fn implied(store: &MemStore) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for key in store.list(&keys().doc_prefix(), usize::MAX).await.unwrap() {
            let origin = keys().origin_of_doc_key(&key).unwrap();
            let (bytes, _) = store.get(&key).await.unwrap().unwrap();
            let doc = codec::decode(&bytes, &origin).unwrap();
            out.extend(crate::index::entries(&keys(), &origin, &doc));
        }
        out
    }

    async fn assert_same_answers(indexed: &ScanService, scanned: &ScanService, now: i64) {
        assert!(indexed.index_in_use() || indexed.build_index().await.is_ok());
        assert!(indexed.index_in_use(), "the indexed service reads the index");
        assert!(!scanned.index_in_use());
        for at in [now, now + 30_000, now + 200_000] {
            for q in promise_queries() {
                assert_eq!(
                    pages(indexed, &q, at, false).await,
                    pages(scanned, &q, at, false).await,
                    "promise.search {q} at {at}"
                );
            }
        }
        for state in [None, Some("pending"), Some("acquired"), Some("fulfilled")] {
            for limit in [1, 3, 100] {
                let mut q = json!({ "limit": limit });
                if let Some(s) = state {
                    q["state"] = json!(s);
                }
                assert_eq!(
                    pages(indexed, &q, now, true).await,
                    pages(scanned, &q, now, true).await,
                    "task.search {q}"
                );
            }
        }
        // Cursors that are not ids, before everything and after it.
        for cursor in ["", "o1", "o5:p3z", "zzz"] {
            let q = json!({ "limit": 5, "cursor": cursor });
            assert_eq!(
                pages(indexed, &q, now, false).await,
                pages(scanned, &q, now, false).await,
                "promise.search {q}"
            );
        }
    }

    #[tokio::test]
    async fn the_index_answers_what_the_scan_answers() {
        for seed in 1..=4 {
            let mem = Arc::new(MemStore::new());
            let store: Arc<dyn Store> = mem.clone();
            let applier = actors(Arc::clone(&store));
            let now = workload(&applier, seed, 600).await;
            // Written through the index from the first document on, so the
            // entries are exactly what the documents imply.
            assert_eq!(
                mem.entries()
                    .into_iter()
                    .filter(|e| !e.ends_with("ix/v1"))
                    .collect::<BTreeSet<_>>(),
                implied(&mem).await,
                "seed {seed}"
            );
            let indexed = scan_over(Arc::clone(&store));
            let scanned = scan_over(Arc::new(NoIndex(Arc::clone(&store))));
            assert_same_answers(&indexed, &scanned, now).await;
        }
    }

    #[tokio::test]
    async fn an_indexed_search_reads_its_results_not_the_store() {
        let counting = Arc::new(CountingStore::new(Arc::new(MemStore::new())));
        let store: Arc<dyn Store> = counting.clone();
        let applier = actors(Arc::clone(&store));
        for n in 0..300 {
            let tags = if n % 100 == 7 {
                json!({ "loams:run": "needle" })
            } else {
                json!({ "loams:run": "hay" })
            };
            applier
                .submit(&format!("w{n}"), create(&format!("w{n}:a"), 10_000_000, tags), 1_000)
                .await
                .unwrap();
        }
        let q = json!({ "tags": { "loams:run": "needle" }, "state": "pending", "limit": 10 });
        let indexed = scan_over(Arc::clone(&store));
        indexed.build_index().await.unwrap();
        counting.reset_counts();
        let reply = indexed.search_promises(&parse(q.clone()), 2_000).await.unwrap();
        let by_index = counting.counts();
        assert_eq!(reply.data["promises"].as_array().unwrap().len(), 3);

        let scanned = scan_over(Arc::new(NoIndex(store)));
        counting.reset_counts();
        let reply_scan = scanned.search_promises(&parse(q), 2_000).await.unwrap();
        let by_scan = counting.counts();
        assert_eq!(reply.data, reply_scan.data);

        eprintln!("[reads] 300 documents, 3 matches: index {by_index:?}, scan {by_scan:?}");
        assert_eq!(by_index.objects(), 3, "one document per result");
        assert_eq!(by_index.lists, 0);
        // The state range, the overflow range: no more, whatever the store holds.
        assert!(by_index.scans <= 2, "{by_index:?}");
        assert_eq!(by_index.scanned, 3);
        assert_eq!(by_scan.objects(), 300, "the scan reads every document");
    }

    #[tokio::test]
    async fn a_store_written_without_the_index_is_scanned_then_rebuilt() {
        let mem = Arc::new(MemStore::new());
        let plain: Arc<dyn Store> = Arc::new(NoIndex(mem.clone()));
        // An older server wrote all of this: no entries, no marker.
        let now = workload(&actors(Arc::clone(&plain)), 7, 400).await;
        assert!(mem.entries().is_empty());

        let store: Arc<dyn Store> = mem.clone();
        let indexed = scan_over(Arc::clone(&store));
        let scanned = scan_over(Arc::clone(&plain));
        let q = json!({ "state": "pending", "limit": 3 });
        // Answered by the scan while the rebuild runs, and the same either way.
        assert_eq!(
            pages(&indexed, &q, now, false).await,
            pages(&scanned, &q, now, false).await
        );
        indexed.build_index().await.unwrap();
        assert!(indexed.index_in_use());
        let mut built: BTreeSet<String> = mem.entries().into_iter().collect();
        assert!(built.remove("p/ix/v1"), "the marker is written last");
        assert_eq!(built, implied(&mem).await);
        assert_same_answers(&indexed, &scanned, now).await;
    }

    #[tokio::test]
    async fn the_first_search_starts_the_rebuild_by_itself() {
        let mem = Arc::new(MemStore::new());
        let plain: Arc<dyn Store> = Arc::new(NoIndex(mem.clone()));
        let now = workload(&actors(plain), 3, 200).await;
        let indexed = scan_over(mem.clone());
        indexed
            .search_promises(&parse(json!({ "limit": 1 })), now)
            .await
            .unwrap();
        for _ in 0..200 {
            if indexed.index_in_use() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(indexed.index_in_use(), "the background rebuild finished");
        let mut built: BTreeSet<String> = mem.entries().into_iter().collect();
        assert!(built.remove("p/ix/v1"));
        assert_eq!(built, implied(&mem).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rebuild_beside_writers_leaves_the_entries_exact() {
        for seed in 10..14 {
            let mem = Arc::new(MemStore::new());
            let plain: Arc<dyn Store> = Arc::new(NoIndex(mem.clone()));
            workload(&actors(plain), seed, 300).await;
            // Index-aware writers keep going while two instances rebuild.
            let store: Arc<dyn Store> = mem.clone();
            let writers = actors(Arc::clone(&store));
            let a = scan_over(Arc::clone(&store));
            let b = scan_over(Arc::clone(&store));
            let (_, ra, rb) = tokio::join!(
                workload(&writers, seed + 100, 300),
                a.build_index(),
                b.build_index()
            );
            ra.unwrap();
            rb.unwrap();
            let mut built: BTreeSet<String> = mem.entries().into_iter().collect();
            assert!(built.remove("p/ix/v1"));
            assert_eq!(built, implied(&mem).await, "seed {seed}");
        }
    }

    #[tokio::test]
    async fn a_failed_commit_changes_neither_the_document_nor_its_entries() {
        let mem = Arc::new(MemStore::new());
        let fault = Arc::new(FaultStore::new(mem.clone()));
        let applier = actors(fault.clone());
        applier
            .submit("o", create("o:a", 900_000, json!({ "t": "1" })), 1_000)
            .await
            .unwrap();
        let entries = mem.entries();
        let doc = mem.get(&keys().doc_key("o")).await.unwrap();
        fault.fail_after(0);
        let settle = Req::PromiseSettle(parse(
            json!({ "id": "o:a", "state": "resolved", "value": {} }),
        ));
        assert!(applier.submit("o", settle, 2_000).await.is_err());
        assert_eq!(mem.entries(), entries);
        assert_eq!(mem.get(&keys().doc_key("o")).await.unwrap(), doc);
    }

    #[tokio::test]
    async fn a_reset_store_is_indexed_and_empty() {
        let mem = Arc::new(MemStore::new());
        let store: Arc<dyn Store> = mem.clone();
        workload(&actors(Arc::clone(&store)), 5, 100).await;
        let scan = scan_over(Arc::clone(&store));
        store.delete_prefix(&keys().doc_prefix()).await.unwrap();
        scan.reset_index().await.unwrap();
        assert_eq!(mem.entries(), vec!["p/ix/v1".to_string()]);
        assert!(scan.index_in_use());
    }
}
