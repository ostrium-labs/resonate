//! Every request kind, against a real SQLite server, through both transports.
//!
//! The same workload runs in process (`Client::local` over the server) and
//! over HTTP (`Client::http` to `resonate-gateway-http` on a free port), and
//! asserts the server's actual answer codes: the idempotent re-creates, the
//! conflicts, the not-founds. Then the transport-level contract: `503` and
//! connection failures retried within the policy, the auth hook per attempt,
//! the client standing in as a `ResonateServer`, and a trace for `spec/valid`.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use resonate_client::trace::{Exchange, Observer, TraceRecorder};
use resonate_client::*;
use resonate_core::types::{Message, RequestEnvelope, RequestHead, ResponseEnvelope};
use resonate_core::{ResonateGateway, ResonateRouter, Unavailable};
use resonate_server_sqlite::SqliteEngine;
use resonate_sql::engine::Engine;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Takes every message and drops it: these tests are about answers, not
/// delivery.
struct NullRouter;

#[async_trait]
impl ResonateRouter for NullRouter {
    async fn route(&self, _address: &str, _msg: &Message) -> Result<(), Unavailable> {
        Ok(())
    }
}

/// A SQLite server in debug mode, so the clock is the caller's and no timer or
/// sweep runs underneath the test.
async fn sqlite_server() -> Arc<dyn ResonateServer> {
    let server = resonate_sql::server::Server::new(
        Box::new(|debug| {
            Box::pin(async move {
                let engine = SqliteEngine::open(":memory:", 30_000, 10, true, debug)
                    .map_err(|e| Unavailable::new(format!("cannot open sqlite: {e}")))?;
                Ok(Arc::new(engine) as Arc<dyn Engine>)
            })
        }),
        Arc::new(NullRouter),
        resonate_sql::server::Options {
            server_url: "http://localhost:8001".to_string(),
            wheel_capacity: 1024,
            wheel_refresh: 30_000,
            sweep_interval: 60_000,
        },
    );
    server.init(true).await.expect("sqlite init");
    server
}

/// The caller's clock, read into `resonate:debug_time` on every request.
#[derive(Clone)]
struct Clock(Arc<AtomicI64>);

impl Clock {
    fn new() -> Self {
        Self(Arc::new(AtomicI64::new(1_000_000)))
    }
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
    fn advance(&self, ms: i64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
    fn reader(&self) -> impl Fn() -> i64 + Send + Sync + 'static {
        let c = Arc::clone(&self.0);
        move || c.load(Ordering::SeqCst)
    }
}

/// Which kinds were answered, so the workload can prove it covered them all.
#[derive(Default)]
struct Kinds(Mutex<BTreeSet<String>>);

impl Observer for Kinds {
    fn observe(&self, x: &Exchange<'_>) {
        self.0.lock().unwrap().insert(x.kind.to_string());
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `resonate-gateway-http`, the real one, in front of `server`.
async fn gateway(
    server: Arc<dyn ResonateServer>,
    auth: Option<resonate_auth::Config>,
) -> (resonate_gateway_http::HttpGateway, String) {
    let port = free_port();
    let gw = resonate_gateway_http::HttpGateway::new(
        server,
        resonate_plugin::Routes::new(),
        resonate_gateway_http::Config {
            bind: format!("127.0.0.1:{port}"),
            auth,
            ..Default::default()
        },
    );
    gw.init(false).await.expect("gateway binds");
    (gw, format!("http://127.0.0.1:{port}/"))
}

fn quick_retries() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 5,
        base: Duration::from_millis(5),
        cap: Duration::from_millis(50),
        deadline: Duration::from_secs(10),
    }
}

const FAR: i64 = 10_000_000_000;

fn global(id: &str) -> PromiseCreate {
    PromiseCreate::new(id, FAR).tag("resonate:scope", "global")
}

fn rejected(r: Result<impl std::fmt::Debug, Error>) -> u16 {
    match r {
        Err(e) => e
            .status()
            .unwrap_or_else(|| panic!("expected a status, got {e:?}")),
        Ok(v) => panic!("expected a rejection, got {v:?}"),
    }
}

// ---------------------------------------------------------------------------
// The workload
// ---------------------------------------------------------------------------

/// Every protocol request kind with the answers SQLite actually gives.
/// `debug` adds the `debug.*` kinds and a validation failure, which the
/// specification does not model and a trace must not contain.
async fn workload(c: &Client, clock: &Clock, debug: bool) {
    // --- promise.* ---------------------------------------------------------

    assert_eq!(rejected(c.promise_get("p-missing").await), 404);

    let p1 = c
        .promise_create(&global("p1").data("aGk="))
        .await
        .expect("create p1");
    assert_eq!(p1.status, 200);
    assert_eq!(p1.data.state, PromiseState::Pending);
    assert_eq!(p1.data.param.data.as_deref(), Some("aGk="));
    assert_eq!(p1.data.created_at, clock.now());
    assert_eq!(p1.attempts, 1);

    // Idempotent re-create: the same 200 and the record as it was, even when
    // the second request asks for something else.
    clock.advance(10);
    let again = c
        .promise_create(&global("p1").data("b3RoZXI="))
        .await
        .expect("re-create p1");
    assert_eq!(again.status, 200, "a re-create is 200, not 201");
    assert_eq!(
        again.data, p1.data,
        "a re-create answers the existing record"
    );

    assert_eq!(c.promise_get("p1").await.unwrap().data, p1.data);

    let settled = c
        .promise_settle(&PromiseSettle::new("p1", SettleState::Resolved).data("ZG9uZQ=="))
        .await
        .expect("settle p1");
    assert_eq!(settled.data.state, PromiseState::Resolved);
    assert_eq!(settled.data.value.data.as_deref(), Some("ZG9uZQ=="));
    assert_eq!(settled.data.settled_at, Some(clock.now()));
    // Settling a settled promise answers it as it is.
    let resettle = c
        .promise_settle(&PromiseSettle::new("p1", SettleState::Rejected))
        .await
        .expect("re-settle p1");
    assert_eq!(resettle.data, settled.data);
    assert_eq!(
        rejected(
            c.promise_settle(&PromiseSettle::new("p-missing", SettleState::Resolved))
                .await
        ),
        404
    );

    // Listeners: on an awaitable promise, not on a missing or internal one.
    c.promise_create(&global("p2")).await.unwrap();
    let listened = c
        .promise_register_listener("p2", "poll://any@listeners")
        .await
        .expect("listen on p2");
    assert_eq!(listened.data.id, "p2");
    assert_eq!(listened.data.state, PromiseState::Pending);
    assert_eq!(
        rejected(
            c.promise_register_listener("p-missing", "poll://any@l")
                .await
        ),
        404
    );
    c.promise_create(&PromiseCreate::new("p3", FAR))
        .await
        .unwrap();
    assert_eq!(
        rejected(c.promise_register_listener("p3", "poll://any@l").await),
        422,
        "an internal promise is not awaitable"
    );

    // Callbacks: an awaiter with a task, an awaited promise of its origin.
    c.promise_create(&PromiseCreate::new("wf", FAR).target("poll://any@g"))
        .await
        .unwrap();
    c.promise_create(&global("wf:1")).await.unwrap();
    let cb = c
        .promise_register_callback("wf:1", "wf")
        .await
        .expect("callback");
    assert_eq!(cb.data.id, "wf:1", "a callback answers the awaited promise");
    assert_eq!(
        rejected(c.promise_register_callback("wf:missing", "wf").await),
        404
    );
    assert_eq!(
        rejected(c.promise_register_callback("wf:1", "wf:nope").await),
        422,
        "a missing awaiter is unprocessable, not not-found"
    );

    let page = c
        .promise_search(&PromiseSearch {
            state: Some(PromiseState::Pending),
            limit: Some(100),
            ..Default::default()
        })
        .await
        .expect("search promises");
    let ids: BTreeSet<_> = page.data.items.iter().map(|p| p.id.as_str()).collect();
    assert!(ids.contains("p2") && ids.contains("wf"), "{ids:?}");
    assert!(!ids.contains("p1"), "p1 is resolved");

    // --- task.* ------------------------------------------------------------

    let ttl = 60_000;
    let t1 = c
        .task_create(
            "w1",
            ttl,
            // One branch, so the server preloads t1's children with it.
            &PromiseCreate::new("t1", FAR)
                .target("poll://any@g")
                .tag("resonate:branch", "t1"),
        )
        .await
        .expect("task.create");
    assert_eq!(t1.data.task.state, TaskState::Acquired);
    assert_eq!(t1.data.task.version, 1);
    assert_eq!(t1.data.task.pid.as_deref(), Some("w1"));
    assert_eq!(t1.data.promise.id, "t1");
    // Re-create while the lease is held: 409.
    assert_eq!(
        rejected(
            c.task_create(
                "w9",
                ttl,
                &PromiseCreate::new("t1", FAR).target("poll://any@g")
            )
            .await
        ),
        409
    );
    // A promise that exists without a task cannot become one: 422.
    assert_eq!(
        rejected(
            c.task_create(
                "w9",
                ttl,
                &PromiseCreate::new("p2", FAR).target("poll://any@g")
            )
            .await
        ),
        422
    );

    assert_eq!(c.task_get("t1").await.unwrap().data, t1.data.task);
    assert_eq!(rejected(c.task_get("t-missing").await), 404);
    assert_eq!(
        c.task_heartbeat("w1", &[("t1", 1)]).await.unwrap().status,
        200
    );

    // Fenced writes, under the lease and not without it.
    let fenced = c
        .task_fence(
            "t1",
            1,
            &FenceAction::Create(global("t1:c1").tag("resonate:branch", "t1")),
        )
        .await
        .expect("fence create");
    assert_eq!(fenced.data.action.kind, "promise.create");
    assert_eq!(fenced.data.action.status, 200);
    assert_eq!(
        fenced.data.action.promise.as_ref().map(|p| p.id.as_str()),
        Some("t1:c1")
    );
    c.task_fence(
        "t1",
        1,
        &FenceAction::Create(global("t1:c2").tag("resonate:branch", "t1")),
    )
    .await
    .unwrap();
    let fenced_settle = c
        .task_fence(
            "t1",
            1,
            &FenceAction::Settle(PromiseSettle::new("t1:c1", SettleState::Resolved)),
        )
        .await
        .expect("fence settle");
    assert_eq!(
        fenced_settle.data.action.promise.unwrap().state,
        PromiseState::Resolved
    );
    assert_eq!(
        rejected(
            c.task_fence("t1", 7, &FenceAction::Create(global("t1:c3")))
                .await
        ),
        409,
        "a fence on a version not held"
    );

    // Suspend on an already-settled promise does not suspend: 300.
    let resume = c.task_suspend("t1", 1, &["t1:c1"]).await.expect("suspend");
    assert_eq!(resume.status, 300);
    let Suspension::Resume { preload } = &resume.data else {
        panic!("expected a resume, got {:?}", resume.data)
    };
    assert!(preload.iter().any(|p| p.id == "t1:c1"), "{preload:?}");

    // Suspend on a pending one does: 200.
    let suspended = c.task_suspend("t1", 1, &["t1:c2"]).await.expect("suspend");
    assert_eq!(suspended.status, 200);
    assert_eq!(suspended.data, Suspension::Suspended);
    assert_eq!(
        c.task_get("t1").await.unwrap().data.state,
        TaskState::Suspended
    );

    // Settling what it awaits resumes it.
    clock.advance(10);
    c.promise_settle(&PromiseSettle::new("t1:c2", SettleState::Resolved))
        .await
        .unwrap();
    let pending = c.task_get("t1").await.unwrap().data;
    assert_eq!(pending.state, TaskState::Pending);

    let claim = c
        .task_acquire("t1", pending.version, "w2", ttl)
        .await
        .expect("acquire");
    assert_eq!(claim.data.task.state, TaskState::Acquired);
    assert_eq!(claim.data.task.pid.as_deref(), Some("w2"));
    let v = claim.data.task.version;
    assert_eq!(v, pending.version + 1);
    assert_eq!(
        rejected(c.task_acquire("t1", pending.version, "w3", ttl).await),
        409,
        "acquiring a held task"
    );
    assert_eq!(
        c.task_heartbeat("w2", &[("t1", v)]).await.unwrap().status,
        200
    );

    assert_eq!(c.task_release("t1", v).await.unwrap().status, 200);
    assert_eq!(
        rejected(c.task_release("t1", v).await),
        409,
        "releasing a task that is not held"
    );
    let released = c.task_get("t1").await.unwrap().data;
    assert_eq!(released.state, TaskState::Pending);

    let claim = c
        .task_acquire("t1", released.version, "w2", ttl)
        .await
        .unwrap();
    let v = claim.data.task.version;
    let done = c
        .task_fulfill(
            "t1",
            v,
            SettleState::Resolved,
            PromiseValue {
                headers: None,
                data: Some("b2s=".into()),
            },
        )
        .await
        .expect("fulfill");
    assert_eq!(done.data.id, "t1");
    assert_eq!(done.data.state, PromiseState::Resolved);
    assert_eq!(
        rejected(
            c.task_fulfill("t1", v, SettleState::Resolved, PromiseValue::default())
                .await
        ),
        409,
        "fulfilling a fulfilled task"
    );
    // Re-create of a fulfilled task answers it as it is: idempotent, 200.
    let recreate = c
        .task_create(
            "w9",
            ttl,
            &PromiseCreate::new("t1", FAR).target("poll://any@g"),
        )
        .await
        .expect("re-create a fulfilled task");
    assert_eq!(recreate.status, 200);
    assert_eq!(recreate.data.task.state, TaskState::Fulfilled);
    assert_eq!(recreate.data.promise.state, PromiseState::Resolved);

    // Halt and continue.
    c.promise_create(&PromiseCreate::new("t2", FAR).target("poll://any@g"))
        .await
        .unwrap();
    assert_eq!(
        c.task_get("t2").await.unwrap().data.state,
        TaskState::Pending
    );
    assert_eq!(c.task_halt("t2").await.unwrap().status, 200);
    assert_eq!(
        c.task_get("t2").await.unwrap().data.state,
        TaskState::Halted
    );
    assert_eq!(
        c.task_halt("t2").await.unwrap().status,
        200,
        "halt is idempotent"
    );
    assert_eq!(c.task_continue("t2").await.unwrap().status, 200);
    assert_eq!(
        c.task_get("t2").await.unwrap().data.state,
        TaskState::Pending
    );
    assert_eq!(rejected(c.task_continue("t2").await), 409, "not halted");
    assert_eq!(rejected(c.task_halt("t1").await), 409, "fulfilled");
    assert_eq!(rejected(c.task_halt("t-missing").await), 404);

    let tasks = c
        .task_search(&TaskSearch {
            state: Some(TaskState::Pending),
            limit: Some(100),
            ..Default::default()
        })
        .await
        .expect("search tasks");
    assert!(tasks.data.items.iter().any(|t| t.id == "t2"));

    // --- schedule.* --------------------------------------------------------

    let s = ScheduleCreate::new(
        "s1",
        "*/5 * * * *",
        "s1.{{.timestamp}}",
        60_000,
        "poll://any@g",
    );
    let s1 = c.schedule_create(&s).await.expect("schedule.create");
    assert_eq!(s1.data.id, "s1");
    assert_eq!(s1.data.cron, "*/5 * * * *");
    let s1_again = c.schedule_create(&s).await.expect("re-create schedule");
    assert_eq!(s1_again.status, 200);
    assert_eq!(s1_again.data.created_at, s1.data.created_at);
    assert_eq!(c.schedule_get("s1").await.unwrap().data.id, "s1");
    assert_eq!(rejected(c.schedule_get("s-missing").await), 404);
    let schedules = c
        .schedule_search(&ScheduleSearch {
            limit: Some(10),
            ..Default::default()
        })
        .await
        .expect("search schedules");
    assert!(schedules.data.items.iter().any(|s| s.id == "s1"));
    assert_eq!(c.schedule_delete("s1").await.unwrap().status, 200);
    assert_eq!(rejected(c.schedule_delete("s1").await), 404);

    if !debug {
        return;
    }

    // --- validation, and debug.* -------------------------------------------

    assert_eq!(
        rejected(c.promise_create(&PromiseCreate::new("", FAR)).await),
        400,
        "a request the server's validation refuses"
    );
    let snap = c.debug_snap().await.expect("debug.snap");
    assert!(snap.data["promises"]
        .as_array()
        .is_some_and(|p| !p.is_empty()));
    assert_eq!(c.debug_tick(clock.now()).await.unwrap().status, 200);
    assert_eq!(c.debug_reset().await.unwrap().status, 200);
    assert_eq!(rejected(c.promise_get("p1").await), 404, "reset forgot p1");
}

fn assert_covers_every_kind(kinds: &Kinds) {
    let seen = kinds.0.lock().unwrap().clone();
    let missing: Vec<_> = KINDS.iter().filter(|k| !seen.contains(**k)).collect();
    assert!(missing.is_empty(), "kinds never sent: {missing:?}");
}

// ---------------------------------------------------------------------------
// Both transports
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_kind_round_trips_in_process() {
    let clock = Clock::new();
    let kinds = Arc::new(Kinds::default());
    let client = Client::local(sqlite_server().await)
        .debug_clock(clock.reader())
        .observer(kinds.clone())
        .build()
        .unwrap();
    workload(&client, &clock, true).await;
    assert_covers_every_kind(&kinds);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_kind_round_trips_over_http() {
    let clock = Clock::new();
    let kinds = Arc::new(Kinds::default());
    let (gw, url) = gateway(sqlite_server().await, None).await;
    let client = Client::http(&url)
        .debug_clock(clock.reader())
        .observer(kinds.clone())
        .build()
        .unwrap();
    assert!(client.ready().await, "the gateway's /ready answers");
    workload(&client, &clock, true).await;
    assert_covers_every_kind(&kinds);
    gw.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------------

/// Unavailable for the first `fail` calls, then the real server.
struct Flaky {
    inner: Arc<dyn ResonateServer>,
    fail: AtomicU32,
    calls: AtomicU32,
}

impl Flaky {
    fn new(inner: Arc<dyn ResonateServer>, fail: u32) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail: AtomicU32::new(fail),
            calls: AtomicU32::new(0),
        })
    }
}

#[async_trait]
impl ResonateServer for Flaky {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let left = self.fail.load(Ordering::SeqCst);
        if left > 0 {
            self.fail.store(left - 1, Ordering::SeqCst);
            return Err(Unavailable::new("store is down"));
        }
        self.inner.process(req).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_503_is_retried_over_http_and_in_process() {
    for over_http in [false, true] {
        let flaky = Flaky::new(sqlite_server().await, 2);
        let (gw, client) = if over_http {
            let (gw, url) = gateway(flaky.clone(), None).await;
            (Some(gw), Client::http(url))
        } else {
            (None, Client::local(flaky.clone()))
        };
        let client = client.retry(quick_retries()).build().unwrap();
        let r = client
            .promise_create(&global("retried"))
            .await
            .expect("the third attempt lands");
        assert_eq!(r.status, 200);
        assert_eq!(
            r.attempts, 3,
            "two 503s, then an answer (http: {over_http})"
        );
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 3);
        if let Some(gw) = gw {
            gw.stop().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_stop_at_the_attempt_bound() {
    let flaky = Flaky::new(sqlite_server().await, u32::MAX);
    let (gw, url) = gateway(flaky.clone(), None).await;
    let client = Client::http(url)
        .retry(RetryPolicy {
            max_attempts: 3,
            ..quick_retries()
        })
        .build()
        .unwrap();
    let err = client.promise_get("x").await.unwrap_err();
    let Error::Unavailable {
        attempts, message, ..
    } = &err
    else {
        panic!("expected unavailable, got {err:?}")
    };
    assert_eq!(*attempts, 3);
    assert!(message.contains("store is down"), "{message}");
    assert_eq!(flaky.calls.load(Ordering::SeqCst), 3);
    gw.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_stop_at_the_deadline() {
    // Nothing listens there.
    let client = Client::http(format!("http://127.0.0.1:{}/", free_port()))
        .retry(RetryPolicy {
            max_attempts: 1_000,
            base: Duration::from_millis(20),
            cap: Duration::from_millis(40),
            deadline: Duration::from_millis(300),
        })
        .build()
        .unwrap();
    let started = std::time::Instant::now();
    let err = client.promise_get("x").await.unwrap_err();
    assert!(err.is_unavailable(), "{err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "bounded by the deadline"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_refused_is_retried_until_the_gateway_comes_up() {
    let port = free_port();
    let server = sqlite_server().await;
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let gw = resonate_gateway_http::HttpGateway::new(
            server,
            resonate_plugin::Routes::new(),
            resonate_gateway_http::Config {
                bind: format!("127.0.0.1:{port}"),
                ..Default::default()
            },
        );
        gw.init(false).await.unwrap();
        gw
    });
    let client = Client::http(format!("http://127.0.0.1:{port}/"))
        .retry(RetryPolicy {
            max_attempts: 100,
            base: Duration::from_millis(20),
            cap: Duration::from_millis(100),
            deadline: Duration::from_secs(10),
        })
        .build()
        .unwrap();
    let r = client.promise_create(&global("late")).await.expect("lands");
    assert!(r.attempts > 1, "the first attempt found nothing listening");
    late.await.unwrap().stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_is_not_retried() {
    let flaky = Flaky::new(sqlite_server().await, 0);
    let client = Client::local(flaky.clone())
        .retry(quick_retries())
        .build()
        .unwrap();
    assert_eq!(rejected(client.promise_get("missing").await), 404);
    assert_eq!(flaky.calls.load(Ordering::SeqCst), 1, "a 404 is an answer");
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// An unsigned JWT, which the gateway's unsigned mode (`publickey = "none"`)
/// accepts with the usual claim checks.
fn unsigned_jwt(claims: Value) -> String {
    let header = json!({ "alg": "HS256", "typ": "JWT" });
    format!(
        "{}.{}.c2ln",
        b64(header.to_string().as_bytes()),
        b64(claims.to_string().as_bytes())
    )
}

fn exp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_auth_hook_mints_a_token_per_attempt_over_http() {
    let flaky = Flaky::new(sqlite_server().await, 1);
    let (gw, url) = gateway(
        flaky,
        Some(resonate_auth::Config {
            publickey: "none".into(),
            iss: None,
            aud: None,
        }),
    )
    .await;

    // Without a token the gateway refuses.
    let anonymous = Client::http(&url).build().unwrap();
    assert_eq!(rejected(anonymous.promise_get("p").await), 401);

    // With one, minted fresh for every attempt.
    let minted = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&minted);
    let client = Client::http(&url)
        .retry(quick_retries())
        .auth(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Some(unsigned_jwt(json!({ "exp": exp(), "prefix": "tenant-a" })))
        })
        .build()
        .unwrap();
    let r = client
        .promise_create(&global("tenant-a.p"))
        .await
        .expect("authorized");
    assert_eq!(r.attempts, 2, "one 503, then the answer");
    assert_eq!(minted.load(Ordering::SeqCst), 2, "one token per attempt");
    // The prefix claim is enforced on what the token names.
    assert_eq!(rejected(client.promise_get("tenant-b.p").await), 403);
    gw.stop().await.unwrap();
}

/// Records `head.auth`, which is where an in-process token goes.
#[derive(Default)]
struct SeesAuth(Mutex<Vec<Option<String>>>);

#[async_trait]
impl ResonateServer for SeesAuth {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.0.lock().unwrap().push(req.head.auth.clone());
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            json!({}),
        ))
    }
}

#[tokio::test]
async fn the_auth_hook_fills_head_auth_in_process() {
    let seen = Arc::new(SeesAuth::default());
    let client = Client::local(seen.clone())
        .auth(StaticToken("tok".into()))
        .build()
        .unwrap();
    client.task_halt("t").await.unwrap();
    // Through the port, an envelope that names its own token keeps it.
    let req = RequestEnvelope {
        kind: "task.halt".into(),
        head: RequestHead {
            corr_id: "c-own".into(),
            version: "2026-04-01".into(),
            auth: Some("own".into()),
            debug_time: None,
        },
        data: json!({ "id": "t" }),
    };
    client.process(&req).await.unwrap();
    assert_eq!(
        *seen.0.lock().unwrap(),
        vec![Some("tok".to_string()), Some("own".to_string())]
    );
}

#[tokio::test]
async fn a_failing_token_source_sends_nothing() {
    struct Broken;
    #[async_trait]
    impl TokenSource for Broken {
        async fn token(&self) -> Result<Option<String>, String> {
            Err("idp down".into())
        }
    }
    let seen = Arc::new(SeesAuth::default());
    let client = Client::local(seen.clone()).auth(Broken).build().unwrap();
    let err = client.task_halt("t").await.unwrap_err();
    assert_eq!(err, Error::Auth("idp down".into()));
    assert!(seen.0.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// The client as the port
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_is_a_resonate_server() {
    let clock = Clock::new();
    let (gw, url) = gateway(sqlite_server().await, None).await;
    let remote: Arc<dyn ResonateServer> = Arc::new(
        Client::http(&url)
            .debug_clock(clock.reader())
            .build()
            .unwrap(),
    );

    // The port, used directly: a caller's envelope, answered as-is.
    let req = RequestEnvelope {
        kind: "promise.create".into(),
        head: RequestHead {
            corr_id: "c-port".into(),
            version: "2026-04-01".into(),
            auth: None,
            debug_time: None,
        },
        data: json!({ "id": "via-port", "timeoutAt": FAR, "tags": { "resonate:scope": "global" } }),
    };
    let resp = remote.process(&req).await.expect("answered");
    assert_eq!(resp.head.status, 200);
    assert_eq!(resp.head.corr_id, "c-port");
    assert_eq!(resp.data["promise"]["createdAt"], json!(clock.now()));

    // And as the server under another client: local over remote over HTTP.
    let layered = Client::local(remote.clone()).build().unwrap();
    assert_eq!(
        layered.promise_get("via-port").await.unwrap().data.id,
        "via-port"
    );
    assert_eq!(rejected(layered.promise_get("nope").await), 404);

    // And behind another gateway, which is how a gateway fronts a remote
    // server: the whole workload through two hops.
    let (front, front_url) = gateway(remote, None).await;
    let through = Client::http(front_url)
        .debug_clock(clock.reader())
        .build()
        .unwrap();
    workload(&through, &clock, false).await;
    front.stop().await.unwrap();
    gw.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_503_stays_unavailable_through_the_port() {
    let flaky = Flaky::new(sqlite_server().await, u32::MAX);
    let (gw, url) = gateway(flaky, None).await;
    let remote = Client::http(&url)
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let req = RequestEnvelope {
        kind: "promise.get".into(),
        head: RequestHead {
            corr_id: "c".into(),
            version: "2026-04-01".into(),
            auth: None,
            debug_time: None,
        },
        data: json!({ "id": "x" }),
    };
    let err = remote.process(&req).await.unwrap_err();
    assert!(err.message.contains("503"), "{err}");
    gw.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// Correlation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn corr_ids_come_from_the_generator_and_are_checked_on_the_way_back() {
    let seen = Arc::new(SeesAuth::default());
    let client = Client::local(seen)
        .corr_ids(Sequential::with_prefix("job"))
        .build()
        .unwrap();
    assert_eq!(client.task_halt("t").await.unwrap().corr_id, "job-1");
    assert_eq!(client.task_halt("t").await.unwrap().corr_id, "job-2");

    /// Answers with someone else's correlation id.
    struct Crossed;
    #[async_trait]
    impl ResonateServer for Crossed {
        async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
            Ok(ResponseEnvelope::new(
                req.kind.clone(),
                "not-yours".into(),
                200,
                json!({}),
            ))
        }
    }
    let client = Client::local(Arc::new(Crossed)).build().unwrap();
    let err = client.task_halt("t").await.unwrap_err();
    assert!(matches!(err, Error::Protocol { .. }), "{err:?}");
}

// ---------------------------------------------------------------------------
// A trace for spec/valid
// ---------------------------------------------------------------------------

/// The protocol workload, recorded in the format `spec/valid` reads.
///
/// Written to `$CARGO_TARGET_TMPDIR/resonate-client-trace.ndjson`, and to
/// `$RESONATE_CLIENT_TRACE` when that is set, for
/// `lake exe checktrace < trace.ndjson` (spec/valid/lean) or
/// `go run ./cmd/lincheck < trace.ndjson` (spec/valid/porc). This test checks
/// the format; the checkers check the content.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_workload_records_a_spec_trace() {
    let clock = Clock::new();
    let trace = Arc::new(TraceRecorder::new());
    let client = Client::local(sqlite_server().await)
        .debug_clock(clock.reader())
        .observer(trace.clone())
        .build()
        .unwrap();
    workload(&client, &clock, false).await;

    let lines = trace.lines();
    assert!(lines.len() > 50, "{} lines", lines.len());
    let mut last_now = i64::MIN;
    let mut kinds: HashMap<String, usize> = HashMap::new();
    for line in &lines {
        let kind = line["kind"].as_str().expect("kind");
        assert!(
            resonate_client::trace::SPEC_KINDS.contains(&kind),
            "{kind} is not a kind the checkers model"
        );
        let now = line["now"].as_i64().expect("now");
        assert!(now >= last_now, "now is non-decreasing");
        last_now = now;
        assert!(line["req"].is_object(), "req is the request data");
        assert_eq!(line["res"]["kind"], json!(kind));
        assert!(line["res"]["head"]["status"].is_u64());
        assert!(line["res"]["head"]["corrId"].is_string());
        *kinds.entry(kind.to_string()).or_default() += 1;
    }
    for kind in resonate_client::trace::SPEC_KINDS {
        assert!(kinds.contains_key(*kind), "the trace has no {kind}");
    }

    let ndjson = trace.to_ndjson();
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    std::fs::write(dir.join("resonate-client-trace.ndjson"), &ndjson).unwrap();
    if let Ok(path) = std::env::var("RESONATE_CLIENT_TRACE") {
        std::fs::write(path, &ndjson).unwrap();
    }
}
