//! The harness behind the gateway-facing protocol profile's scenario tests.
//!
//! `docs/protocol-profile.md` says what a gateway may rely on: which requests,
//! which answers, which limits. `tests/` holds that document to account. Each
//! scenario is written once against [`Client`] and runs against every store
//! this build can reach, through the [`ResonateServer`] port and nothing else,
//! so a scenario cannot depend on how a store is built.
//!
//! The stores:
//!
//! | name       | what                                         | when |
//! |------------|----------------------------------------------|------|
//! | `oracle`   | the in-memory reference model                | always |
//! | `sqlite`   | the SQL server over an in-memory SQLite      | always |
//! | `blob`     | the blob server over an in-memory object store | always |
//! | `postgres` | the SQL server over Postgres                 | `TEST_POSTGRES_URL` is set |
//! | `mysql`    | the SQL server over MySQL or TiDB            | `TEST_MYSQL_URL` is set |
//! | `tikv`     | the blob server over TiKV (FW1)              | `--cfg resonate_profile_tikv` and `TEST_TIKV_PD` (see Cargo.toml) |
//!
//! `TEST_BACKENDS=sqlite,postgres` keeps only the named stores.
//!
//! Time belongs to the test. Every server is started under the debug flag, so
//! `now` rides in `head.debug_time` and moves only when a scenario says so;
//! deadlines fire on `debug.tick`. That is what makes a lease expiry or a
//! timer a deterministic step rather than a sleep.
//!
//! With `TEST_TRACE_DIR` set, every scenario writes its external calls as
//! NDJSON in the `spec/valid` trace format, one file per scenario and store.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, RequestEnvelope, RequestHead,
    ResponseEnvelope, UnblockMsg, UnblockMsgData, UnblockMsgHead, SUPPORTED_VERSIONS,
};
use resonate_core::{ResonateRouter, ResonateServer, Unavailable};
use resonate_oracle::SharedOracle;
use resonate_server_blob::server::{Server as BlobServer, ServerCfg as BlobCfg};
use resonate_server_blob::store::ObjectStoreAdapter;
use resonate_server_mysql::MysqlEngine;
use resonate_server_postgres::PostgresEngine;
use resonate_server_sqlite::SqliteEngine;
use resonate_sql::engine::{Engine, Outgoing};
use serde_json::{json, Value};

/// The epoch every scenario starts at, in milliseconds. Arbitrary, but far
/// enough from zero that `now - x` never goes negative.
pub const T0: i64 = 1_700_000_000_000;
/// Stamped into every `execute` message by every store.
pub const SERVER_URL: &str = "http://profile-test-server:8001";
/// The SQL stores' default (`retry_timeout`), set explicitly so every store
/// redispatches a pending task on the same schedule.
pub const RETRY_TIMEOUT: i64 = 30_000;
/// The stores' default `preload_limit`.
pub const PRELOAD_LIMIT: u32 = 10;

/// Postgres is one shared database and `debug.reset` empties it, so the
/// scenarios that use it take turns.
static POSTGRES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
/// The same, for MySQL.
static MYSQL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------
// The outbound port, recorded
// ---------------------------------------------------------------------------

/// A router that keeps what it was asked to deliver.
///
/// Every store delivers inside `process`, after its commit and before its
/// response, so when a call returns, the recorder holds everything that call
/// emitted.
#[derive(Default)]
pub struct Recorder {
    sent: Mutex<Vec<Routed>>,
}

/// One message, as a store handed it to its router.
#[derive(Debug, Clone, PartialEq)]
pub struct Routed {
    pub address: String,
    pub message: Value,
}

impl Routed {
    /// `"execute"` or `"unblock"`.
    pub fn kind(&self) -> &str {
        self.message["kind"].as_str().unwrap_or_default()
    }
}

impl Recorder {
    fn take(&self) -> Vec<Routed> {
        let mut out: Vec<Routed> = self
            .sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        out.sort_by_key(|r| (r.address.clone(), r.message.to_string()));
        out
    }
}

#[async_trait]
impl ResonateRouter for Recorder {
    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let message = serde_json::to_value(msg).expect("a message serializes");
        self.sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Routed {
                address: address.to_string(),
                message,
            });
        Ok(())
    }
}

/// The reference model as a server that delivers through a router, the way
/// the SQL server delivers what its engine emitted. `SharedOracle` alone
/// keeps its messages in a queue for `debug.snap`; this routes them.
struct OracleServer {
    oracle: SharedOracle,
    router: Arc<dyn ResonateRouter>,
}

#[async_trait]
impl ResonateServer for OracleServer {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        let (resp, emitted) = {
            let mut o = self.oracle.lock();
            let resp = o.apply(req);
            let emitted = o.take_emitted();
            let _ = o.take_armed();
            (resp, emitted)
        };
        for msg in emitted {
            let (address, payload) = match msg {
                Outgoing::Execute {
                    address,
                    task_id,
                    version,
                } => (
                    address,
                    Message::Execute(ExecuteMsg {
                        kind: "execute".to_string(),
                        head: MessageHead {
                            server_url: SERVER_URL.to_string(),
                        },
                        data: ExecuteMsgData {
                            task: ExecuteMsgTask {
                                id: task_id,
                                version,
                            },
                        },
                    }),
                ),
                Outgoing::Unblock { address, promise } => (
                    address,
                    Message::Unblock(UnblockMsg {
                        kind: "unblock".to_string(),
                        head: UnblockMsgHead {},
                        data: UnblockMsgData { promise },
                    }),
                ),
            };
            let _ = self.router.route(&address, &payload).await;
        }
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

/// A store behind the server port, and the router it delivers through.
pub struct Backend {
    pub name: String,
    server: Arc<dyn ResonateServer>,
    router: Arc<Recorder>,
    _turn: Option<tokio::sync::MutexGuard<'static, ()>>,
}

fn sql_options() -> resonate_sql::server::Options {
    resonate_sql::server::Options {
        server_url: SERVER_URL.to_string(),
        wheel_capacity: 8192,
        wheel_refresh: 30_000,
        sweep_interval: 60_000,
    }
}

async fn oracle() -> Backend {
    let router = Arc::new(Recorder::default());
    let server = Arc::new(OracleServer {
        oracle: SharedOracle::with_preload_limit(PRELOAD_LIMIT),
        router: router.clone(),
    });
    Backend {
        name: "oracle".into(),
        server,
        router,
        _turn: None,
    }
}

async fn sqlite() -> Backend {
    let router = Arc::new(Recorder::default());
    let server = resonate_sql::server::Server::new(
        Box::new(|debug| {
            Box::pin(async move {
                let engine =
                    SqliteEngine::open(":memory:", RETRY_TIMEOUT, PRELOAD_LIMIT, true, debug)
                        .map_err(|e| Unavailable::new(format!("cannot open sqlite: {e}")))?;
                Ok(Arc::new(engine) as Arc<dyn Engine>)
            })
        }),
        router.clone(),
        sql_options(),
    );
    server.init(true).await.expect("sqlite init");
    Backend {
        name: "sqlite".into(),
        server,
        router,
        _turn: None,
    }
}

async fn postgres(url: String) -> Backend {
    let turn = POSTGRES.lock().await;
    let router = Arc::new(Recorder::default());
    let server = resonate_sql::server::Server::new(
        Box::new(move |debug| {
            Box::pin(async move {
                let engine = PostgresEngine::connect(&url, 4, RETRY_TIMEOUT, PRELOAD_LIMIT, debug)
                    .await
                    .map_err(|e| Unavailable::new(format!("cannot connect: {e}")))?;
                engine
                    .init(true)
                    .await
                    .map_err(|e| Unavailable::new(format!("schema: {e}")))?;
                Ok(Arc::new(engine) as Arc<dyn Engine>)
            })
        }),
        router.clone(),
        sql_options(),
    );
    server.init(true).await.expect("postgres init");
    Backend {
        name: "postgres".into(),
        server,
        router,
        _turn: Some(turn),
    }
}

async fn mysql(url: String) -> Backend {
    let turn = MYSQL.lock().await;
    let router = Arc::new(Recorder::default());
    let server = resonate_sql::server::Server::new(
        Box::new(move |debug| {
            Box::pin(async move {
                let engine = MysqlEngine::connect(&url, 4, RETRY_TIMEOUT, PRELOAD_LIMIT, debug)
                    .await
                    .map_err(|e| Unavailable::new(format!("cannot connect: {e}")))?;
                engine
                    .init(true)
                    .await
                    .map_err(|e| Unavailable::new(format!("schema: {e}")))?;
                Ok(Arc::new(engine) as Arc<dyn Engine>)
            })
        }),
        router.clone(),
        sql_options(),
    );
    server.init(true).await.expect("mysql init");
    Backend {
        name: "mysql".into(),
        server,
        router,
        _turn: Some(turn),
    }
}

fn blob_cfg() -> BlobCfg {
    BlobCfg {
        debug: true,
        // Off by default in the plugin (each search reads every document);
        // on here so the scenarios can show what a search answers.
        search: true,
        server_url: SERVER_URL.to_string(),
        ..Default::default()
    }
}

async fn blob() -> Backend {
    let router = Arc::new(Recorder::default());
    let mut cfg = blob_cfg();
    cfg.applier.kernel.retry_timeout = RETRY_TIMEOUT;
    let server = BlobServer::build(
        Arc::new(ObjectStoreAdapter::in_memory()),
        router.clone(),
        cfg,
    );
    server.init(true).await.expect("blob init");
    Backend {
        name: "blob".into(),
        server,
        router,
        _turn: None,
    }
}

/// The blob server over TiKV, one fresh root per call so runs never share
/// state. Needs FW1's `resonate_server_blob::tikv`; see Cargo.toml.
#[cfg(resonate_profile_tikv)]
async fn tikv(pd: String) -> Backend {
    use resonate_server_blob::tikv::{ensure_keyspace, keyspace::pd_http_url};
    use resonate_server_blob::tikv::{TikvStore, TikvStoreCfg};

    let pd: Vec<String> = pd.split(',').map(str::to_owned).collect();
    let keyspace =
        std::env::var("TEST_TIKV_KEYSPACE").unwrap_or_else(|_| "resonate_profile".into());
    ensure_keyspace(&pd_http_url(&pd[0]), &keyspace)
        .await
        .expect("TEST_TIKV_PD keyspace");
    let mut store_cfg = TikvStoreCfg::new(pd, keyspace);
    store_cfg.root = format!("profile-{:016x}/", rand_u64()).into_bytes();
    let store = TikvStore::connect(store_cfg).await.expect("tikv connect");
    let router = Arc::new(Recorder::default());
    let mut cfg = blob_cfg();
    cfg.applier.kernel.retry_timeout = RETRY_TIMEOUT;
    let server = BlobServer::build(Arc::new(store), router.clone(), cfg);
    server.init(true).await.expect("tikv init");
    Backend {
        name: "tikv".into(),
        server,
        router,
        _turn: None,
    }
}

#[cfg(resonate_profile_tikv)]
fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish()
}

/// Every store this build and this environment can reach, filtered by
/// `TEST_BACKENDS`.
pub async fn backends() -> Vec<Backend> {
    let want: Option<BTreeSet<String>> = std::env::var("TEST_BACKENDS").ok().map(|w| {
        w.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    let wanted = |name: &str| want.as_ref().is_none_or(|w| w.contains(name));

    let mut out = Vec::new();
    if wanted("oracle") {
        out.push(oracle().await);
    }
    if wanted("sqlite") {
        out.push(sqlite().await);
    }
    if wanted("blob") {
        out.push(blob().await);
    }
    match std::env::var("TEST_POSTGRES_URL") {
        Ok(url) if wanted("postgres") => out.push(postgres(url).await),
        Ok(_) => {}
        Err(_) => eprintln!("[profile] TEST_POSTGRES_URL not set: postgres skipped"),
    }
    match std::env::var("TEST_MYSQL_URL") {
        Ok(url) if wanted("mysql") => out.push(mysql(url).await),
        _ => {}
    }
    #[cfg(resonate_profile_tikv)]
    match std::env::var("TEST_TIKV_PD") {
        Ok(pd) if !pd.is_empty() && wanted("tikv") => out.push(tikv(pd).await),
        _ => eprintln!("[profile] TEST_TIKV_PD not set: tikv skipped"),
    }
    #[cfg(not(resonate_profile_tikv))]
    if std::env::var("TEST_TIKV_PD").is_ok() {
        eprintln!(
            "[profile] TEST_TIKV_PD is set but this build has no TiKV leg (--cfg resonate_profile_tikv): tikv skipped"
        );
    }
    if let Some(w) = &want {
        for name in w {
            assert!(
                out.iter().any(|b| &b.name == name),
                "TEST_BACKENDS names '{name}', which is not available here"
            );
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The client a scenario drives
// ---------------------------------------------------------------------------

/// One answer: the status and the data, the two things a gateway reads.
#[derive(Debug, Clone)]
pub struct Res {
    pub status: i32,
    pub data: Value,
}

impl Res {
    /// `data.promise`, for the requests that answer with one.
    pub fn promise(&self) -> &Value {
        &self.data["promise"]
    }
    /// `data.task`.
    pub fn task(&self) -> &Value {
        &self.data["task"]
    }
}

/// A scenario's handle on one store: a clock, a trace, and what was routed.
pub struct Client {
    pub backend: Backend,
    pub scenario: String,
    now: i64,
    trace: Vec<Value>,
    corr: u64,
}

impl Client {
    /// Start a scenario on `backend` at [`T0`], from an empty store.
    pub async fn start(backend: Backend, scenario: &str) -> Self {
        let mut c = Client {
            backend,
            scenario: scenario.to_string(),
            now: T0,
            trace: Vec::new(),
            corr: 0,
        };
        let r = c.raw("debug.reset", json!({})).await;
        assert_eq!(
            r.status, 200,
            "[{}] debug.reset: {:?}",
            c.backend.name, r.data
        );
        c.backend.router.take();
        c
    }

    pub fn name(&self) -> &str {
        &self.backend.name
    }

    pub fn now(&self) -> i64 {
        self.now
    }

    /// Move the clock without firing anything. The next request carries the
    /// new time; deadlines that have passed fire on the next [`tick`](Self::tick)
    /// or when a request names the promise.
    pub fn advance(&mut self, ms: i64) {
        assert!(ms >= 0, "time only moves forward");
        self.now += ms;
    }

    /// Move the clock to `now + ms` and fire every deadline due by then.
    pub async fn tick(&mut self, ms: i64) {
        self.advance(ms);
        let now = self.now;
        let r = self.raw("debug.tick", json!({ "time": now })).await;
        assert_eq!(r.status, 200, "[{}] debug.tick: {:?}", self.name(), r.data);
    }

    /// Everything routed since the last call, sorted by address then body.
    pub fn routed(&self) -> Vec<Routed> {
        self.backend.router.take()
    }

    fn envelope(&mut self, kind: &str, data: Value) -> RequestEnvelope {
        self.corr += 1;
        RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: format!("{}-{}", self.scenario, self.corr),
                version: SUPPORTED_VERSIONS[0].to_string(),
                auth: None,
                debug_time: Some(self.now),
            },
            data,
        }
    }

    async fn raw(&mut self, kind: &str, data: Value) -> Res {
        let req = self.envelope(kind, data);
        let resp = self
            .backend
            .server
            .process(&req)
            .await
            .unwrap_or_else(|e| panic!("[{}] no answer for {kind}: {e}", self.backend.name));
        Res {
            status: resp.head.status,
            data: resp.data,
        }
    }

    /// Send one protocol request at the current time and record it in the
    /// trace.
    pub async fn call(&mut self, kind: &str, data: Value) -> Res {
        let req = self.envelope(kind, data.clone());
        let resp = self
            .backend
            .server
            .process(&req)
            .await
            .unwrap_or_else(|e| panic!("[{}] no answer for {kind}: {e}", self.backend.name));
        let res = Res {
            status: resp.head.status,
            data: resp.data.clone(),
        };
        // The trace checker's vocabulary: protocol requests, never `debug.*`
        // (internal steps are the checker's to find) and never searches. A
        // 400 is left out too: validation happens before any state is read,
        // and the specification's machine has no malformed requests to
        // answer (a negative `timeoutAt` does not even decode there).
        if !kind.starts_with("debug.") && !kind.ends_with(".search") && res.status != 400 {
            self.trace.push(json!({
                "kind": kind,
                "now": self.now,
                "req": data,
                "res": serde_json::to_value(&resp).expect("a response serializes"),
            }));
        }
        res
    }

    /// Write the trace to `$TEST_TRACE_DIR/<scenario>.<store>.ndjson` when the
    /// variable is set.
    pub fn finish(self) {
        let Ok(dir) = std::env::var("TEST_TRACE_DIR") else {
            return;
        };
        // The checkers cannot follow a schedule: `nextCron` and `occurrences`
        // are opaque in the specification (spec/valid/lean/schedules.lean),
        // so a trace with a schedule in it explains nothing.
        if self.trace.iter().any(|e| {
            e["kind"]
                .as_str()
                .is_some_and(|k| k.starts_with("schedule."))
        }) {
            eprintln!(
                "[profile] {}.{}: not traced (schedules are opaque to the checkers)",
                self.scenario, self.backend.name
            );
            return;
        }
        std::fs::create_dir_all(&dir).expect("TEST_TRACE_DIR");
        let path = std::path::Path::new(&dir)
            .join(format!("{}.{}.ndjson", self.scenario, self.backend.name));
        let body: String = self.trace.iter().map(|e| format!("{e}\n")).collect();
        std::fs::write(&path, body).expect("write trace");
    }

    // --- the requests the profile uses, as they go on the wire ---

    pub async fn promise_create(&mut self, id: &str, timeout_at: i64, tags: Value) -> Res {
        self.call(
            "promise.create",
            json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags }),
        )
        .await
    }

    pub async fn promise_get(&mut self, id: &str) -> Res {
        self.call("promise.get", json!({ "id": id })).await
    }

    pub async fn promise_settle(&mut self, id: &str, state: &str, data: Option<&str>) -> Res {
        let value = match data {
            Some(d) => json!({ "headers": {}, "data": d }),
            None => json!({}),
        };
        self.call(
            "promise.settle",
            json!({ "id": id, "state": state, "value": value }),
        )
        .await
    }

    pub async fn register_listener(&mut self, awaited: &str, address: &str) -> Res {
        self.call(
            "promise.register_listener",
            json!({ "awaited": awaited, "address": address }),
        )
        .await
    }

    pub async fn task_get(&mut self, id: &str) -> Res {
        self.call("task.get", json!({ "id": id })).await
    }

    pub async fn task_acquire(&mut self, id: &str, version: i64, pid: &str, ttl: i64) -> Res {
        self.call(
            "task.acquire",
            json!({ "id": id, "version": version, "pid": pid, "ttl": ttl }),
        )
        .await
    }

    pub async fn task_heartbeat(&mut self, pid: &str, tasks: &[(&str, i64)]) -> Res {
        let tasks: Vec<Value> = tasks
            .iter()
            .map(|(id, v)| json!({ "id": id, "version": v }))
            .collect();
        self.call("task.heartbeat", json!({ "pid": pid, "tasks": tasks }))
            .await
    }

    pub async fn task_fulfill(
        &mut self,
        id: &str,
        version: i64,
        state: &str,
        data: Option<&str>,
    ) -> Res {
        let value = match data {
            Some(d) => json!({ "headers": {}, "data": d }),
            None => json!({}),
        };
        self.call(
            "task.fulfill",
            json!({
                "id": id,
                "version": version,
                "action": {
                    "kind": "promise.settle",
                    "head": {},
                    "data": { "id": id, "state": state, "value": value },
                },
            }),
        )
        .await
    }

    pub async fn task_release(&mut self, id: &str, version: i64) -> Res {
        self.call("task.release", json!({ "id": id, "version": version }))
            .await
    }

    pub async fn task_create(
        &mut self,
        pid: &str,
        ttl: i64,
        id: &str,
        timeout_at: i64,
        tags: Value,
    ) -> Res {
        self.call(
            "task.create",
            json!({
                "pid": pid,
                "ttl": ttl,
                "action": {
                    "kind": "promise.create",
                    "head": {},
                    "data": { "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags },
                },
            }),
        )
        .await
    }
}
