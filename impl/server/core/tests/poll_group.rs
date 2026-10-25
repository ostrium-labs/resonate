//! A poll group with many consumers, end to end (Loams design 74, FW8).
//!
//! Several workers — or gateway replicas — share one `poll://any@<group>`.
//! This drives the whole process the way a deployment runs it: the server
//! plugin, the real `http-poll` transport and the real HTTP gateway, built by
//! `resonate_base::build` and reached over loopback HTTP. Nothing is stubbed.
//!
//! Three consumers hold SSE connections to `/poll/<group>/<id>`. A producer
//! creates N promises targeted at the group, never more than a window of them
//! unfinished. Each consumer acquires what it is sent and fulfils it. Half-way
//! through, one consumer is killed: its connection and everything it was doing
//! are dropped mid-flight, the way a crashed process leaves them.
//!
//! What must hold, on every store:
//!
//! - **One delivery per hand-off.** The consumers never read more messages
//!   than the transport accepted from the router (`resonate_deliveries_total`),
//!   so no message went to two consumers. A second delivery of a task is the
//!   server sending it again on its retry deadline, and is counted.
//! - **Acquired once per attempt.** No task version is acquired by two
//!   consumers, and a task finished before the kill was acquired once.
//! - **Redelivery within TTL + 1 s.** Every task unfinished at the kill is
//!   delivered to a live consumer, which then acquires it, within the lease
//!   TTL plus one second of the kill. The servers' `retry_timeout` is set to
//!   the TTL, since a message the dead consumer had received but not acquired
//!   comes back on the retry deadline, not the lease.
//! - **The dead consumer is dropped.** A task created after the kill reaches
//!   a live consumer at once, not on a retry deadline after being lost in the
//!   dead connection.
//!
//! Stores: SQLite and the in-memory blob server always; Postgres when
//! `TEST_POSTGRES_URL` names a database (every id is unique to the run, so
//! the database need not be empty).
//!
//! Knobs:
//!   TEST_POLL_GROUP_TASKS=N   tasks per store (default 10000)
//!   TEST_POLL_GROUP_TTL_MS=N  lease TTL and retry timeout (default 3000)
//!   RUST_LOG=...              the server's log, to stderr (default none)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use resonate_core::types::SUPPORTED_VERSIONS;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

const CONSUMERS: [&str; 3] = ["c1", "c2", "c3"];
/// The one that is killed.
const VICTIM: &str = "c3";
/// Unfinished tasks the producer allows. Bounds how much is in flight, so a
/// task's latency is the system's and not the length of a backlog.
const WINDOW: usize = 32;
/// Tasks a consumer works on at once.
const PER_CONSUMER: usize = 8;
const PRODUCERS: usize = 4;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as i64
}

// ─── The process under test ─────────────────────────────────────────────────

enum Store {
    Sqlite(std::path::PathBuf),
    Blob,
    Postgres(String),
}

impl Store {
    fn name(&self) -> &'static str {
        match self {
            Store::Sqlite(_) => "sqlite",
            Store::Blob => "blob (in-memory)",
            Store::Postgres(_) => "postgres",
        }
    }
}

struct Stack {
    running: resonate_base::Running,
    base: String,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("addr")
        .port()
}

/// Build and start the server, the poll transport and the HTTP gateway.
async fn start(store: &Store, retry_timeout_ms: i64) -> Stack {
    let registry = resonate_base::Registry::new()
        .server(&resonate_server_sqlite::PLUGIN)
        .server(&resonate_server_postgres::PLUGIN)
        .server(&resonate_server_blob::PLUGIN)
        .worker(&resonate_transport_http_poll::PLUGIN)
        .gateway(&resonate_gateway_http::PLUGIN);

    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let (id, mut settings): (&str, Vec<(String, String)>) = match store {
        Store::Sqlite(path) => (
            "server_sqlite",
            vec![("path".into(), path.display().to_string())],
        ),
        Store::Blob => ("server_blob", vec![]),
        // More connections than the default 10: the run can have 30 and more
        // requests in flight, and a timer firing that waits for a connection
        // behind them is measuring the pool, not the poll group.
        Store::Postgres(url) => (
            "server_postgres",
            vec![
                ("url".into(), url.clone()),
                ("pool_size".into(), "40".into()),
            ],
        ),
    };
    settings.push(("retry_timeout".into(), retry_timeout_ms.to_string()));
    settings.push(("server_url".into(), base.clone()));

    let mut loader = resonate_plugin::Loader::new()
        .set("servers.active", id)
        .expect("key")
        .set("gateways.gateway_http.bind", &format!("127.0.0.1:{port}"))
        .expect("key");
    for (k, v) in &settings {
        loader = loader.set(&format!("servers.{id}.{k}"), v).expect("key");
    }
    let config = loader.load();
    let running = resonate_base::build(&registry, &config, &resonate_base::Options::default())
        .expect("builds");
    running.start(false).await.expect("starts");
    Stack { running, base }
}

// ─── The protocol, over HTTP ────────────────────────────────────────────────

async fn call(http: &reqwest::Client, base: &str, kind: &str, data: Value) -> (i64, Value) {
    let body = json!({
        "kind": kind,
        "head": { "corrId": fastrand::u64(..).to_string(), "version": SUPPORTED_VERSIONS[0] },
        "data": data,
    });
    let resp: Value = http
        .post(format!("{base}/"))
        .json(&body)
        .send()
        .await
        .expect("the gateway answers")
        .json()
        .await
        .expect("a response envelope");
    let status = resp["head"]["status"].as_i64().expect("a status");
    (status, resp["data"].clone())
}

// ─── What happened, as the consumers saw it ─────────────────────────────────

#[derive(Default)]
struct Log {
    /// task -> every delivery, as (consumer, version, when).
    deliveries: HashMap<String, Vec<(String, i64, Instant)>>,
    /// task -> every successful acquire, as (consumer, version it holds, the
    /// delivery that led to it, when).
    acquires: HashMap<String, Vec<(String, i64, Instant)>>,
    /// task -> the consumer that fulfilled it, and when.
    fulfilled: HashMap<String, (String, Instant)>,
    /// task -> when its create was sent, and when it was answered.
    created: HashMap<String, (Instant, Instant)>,
    conflicts: usize,
}

struct Run {
    base: String,
    group: String,
    ttl: i64,
    api: reqwest::Client,
    log: Mutex<Log>,
    /// The producer's window: a permit per unfinished task.
    window: Semaphore,
    done: AtomicUsize,
}

impl Run {
    fn record<R>(&self, f: impl FnOnce(&mut Log) -> R) -> R {
        f(&mut self.log.lock().unwrap())
    }
}

/// Hold an SSE connection to the group and work on what arrives, until the
/// stream ends or this future is dropped. Dropping it is the kill: the socket
/// and every in-flight request go with it.
async fn consume(run: Arc<Run>, name: &'static str) {
    let sse = reqwest::Client::new();
    let mut resp = sse
        .get(format!("{}/poll/{}/{name}", run.base, run.group))
        .send()
        .await
        .expect("the poll endpoint answers");
    assert_eq!(resp.status(), 200, "{name} could not join the group");
    let limit = Arc::new(Semaphore::new(PER_CONSUMER));
    let mut work = tokio::task::JoinSet::new();
    let mut buf = String::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        buf.push_str(std::str::from_utf8(&chunk).expect("utf-8"));
        while let Some(end) = buf.find("\n\n") {
            let frame: String = buf.drain(..end + 2).collect();
            let Some(data) = frame.lines().find_map(|l| l.strip_prefix("data:")) else {
                continue; // a keepalive
            };
            let msg: Value = serde_json::from_str(data.trim()).expect("an SSE frame is JSON");
            let task = msg["data"]["task"]["id"]
                .as_str()
                .expect("an id")
                .to_string();
            let version = msg["data"]["task"]["version"].as_i64().expect("a version");
            run.record(|l| {
                l.deliveries.entry(task.clone()).or_default().push((
                    name.to_string(),
                    version,
                    Instant::now(),
                ))
            });
            // Read on while busy, so a delivery is recorded when it arrives
            // rather than when a worker slot frees up: what is measured is
            // the transport, not this consumer's queue.
            let limit = Arc::clone(&limit);
            let run = Arc::clone(&run);
            work.spawn(async move {
                let _permit = limit.acquire_owned().await.expect("open");
                execute(&run, name, &task, version).await;
            });
            while work.try_join_next().is_some() {}
        }
    }
}

/// Acquire, then fulfil. A refused acquire is another consumer's attempt.
async fn execute(run: &Run, name: &str, task: &str, version: i64) {
    let (status, data) = call(
        &run.api,
        &run.base,
        "task.acquire",
        json!({ "id": task, "version": version, "pid": name, "ttl": run.ttl }),
    )
    .await;
    if status != 200 {
        run.record(|l| l.conflicts += 1);
        return;
    }
    let held = data["task"]["version"]
        .as_i64()
        .expect("the acquired version");
    run.record(|l| {
        l.acquires.entry(task.to_string()).or_default().push((
            name.to_string(),
            held,
            Instant::now(),
        ))
    });
    let (status, _) = call(
        &run.api,
        &run.base,
        "task.fulfill",
        json!({
            "id": task,
            "version": held,
            "action": {
                "kind": "promise.settle",
                "head": {},
                "data": { "id": task, "state": "resolved", "value": {} }
            }
        }),
    )
    .await;
    if status == 200 {
        let first = run.record(|l| {
            l.fulfilled
                .insert(task.to_string(), (name.to_string(), Instant::now()))
                .is_none()
        });
        assert!(first, "{task} fulfilled twice");
        run.done.fetch_add(1, Ordering::SeqCst);
        run.window.add_permits(1);
    }
}

async fn produce(run: Arc<Run>, prefix: String, next: Arc<AtomicUsize>, n: usize) {
    loop {
        let i = next.fetch_add(1, Ordering::SeqCst);
        if i >= n {
            return;
        }
        run.window.acquire().await.expect("open").forget();
        let id = format!("{prefix}-{i}");
        let sent = Instant::now();
        let (status, _) = call(
            &run.api,
            &run.base,
            "promise.create",
            json!({
                "id": id,
                "timeoutAt": now_ms() + 3_600_000,
                "param": {},
                "tags": { "resonate:target": format!("poll://any@{}", run.group) }
            }),
        )
        .await;
        assert_eq!(status, 200, "create {id}");
        run.record(|l| l.created.insert(id, (sent, Instant::now())));
    }
}

/// Milliseconds from `origin` to `at`, signed.
fn rel(at: Instant, origin: Instant) -> i64 {
    if at >= origin {
        at.duration_since(origin).as_millis() as i64
    } else {
        -(origin.duration_since(at).as_millis() as i64)
    }
}

/// Messages a worker has accepted, as the router counts them.
fn handed_off() -> f64 {
    resonate_base::router::DELIVERIES_TOTAL
        .with_label_values(&["success"])
        .get()
}

/// One leg at a time: the router's counter is per process.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ─── The scenario ───────────────────────────────────────────────────────────

async fn scenario(store: Store) {
    let _one = ONE_AT_A_TIME.lock().await;
    if let Ok(filter) = std::env::var("RUST_LOG") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_writer(std::io::stderr)
            .try_init();
    }
    let n: usize = env_or("TEST_POLL_GROUP_TASKS", 10_000);
    let ttl: i64 = env_or("TEST_POLL_GROUP_TTL_MS", 3_000);
    let stack = start(&store, ttl).await;
    let tag = format!("{:08x}", fastrand::u32(..));
    let run = Arc::new(Run {
        base: stack.base.clone(),
        group: format!("fw8-{tag}"),
        ttl,
        api: reqwest::Client::new(),
        log: Mutex::new(Log::default()),
        window: Semaphore::new(WINDOW),
        done: AtomicUsize::new(0),
    });
    let handed_off_before = handed_off();
    let began = Instant::now();

    let mut consumers = HashMap::new();
    for name in CONSUMERS {
        consumers.insert(name, tokio::spawn(consume(Arc::clone(&run), name)));
    }
    // Every consumer is in the group before the first task exists, so the
    // first deliveries are spread over all three.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let next = Arc::new(AtomicUsize::new(0));
    let mut producers = Vec::new();
    for _ in 0..PRODUCERS {
        producers.push(tokio::spawn(produce(
            Arc::clone(&run),
            format!("fw8-{tag}"),
            Arc::clone(&next),
            n,
        )));
    }

    // The kill, half-way through.
    while run.done.load(Ordering::SeqCst) < n / 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    consumers[VICTIM].abort();
    let killed_at = Instant::now();
    let pending_at_kill: HashSet<String> = run.record(|l| {
        l.created
            .keys()
            .filter(|t| !l.fulfilled.contains_key(*t))
            .cloned()
            .collect()
    });

    // Until every task is settled. A fulfil the victim had in flight may
    // have landed without its answer reaching anyone; the store knows.
    let deadline = killed_at + Duration::from_secs(600);
    let mut settled_unseen: HashSet<String> = HashSet::new();
    loop {
        let missing: Vec<String> = run.record(|l| {
            (0..n)
                .map(|i| format!("fw8-{tag}-{i}"))
                .filter(|t| !l.fulfilled.contains_key(t) && !settled_unseen.contains(t))
                .collect()
        });
        if missing.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{} tasks never finished, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
        if killed_at.elapsed() > Duration::from_millis(ttl as u64 + 2_000) && missing.len() <= 64 {
            for t in &missing {
                let (status, data) =
                    call(&run.api, &run.base, "promise.get", json!({ "id": t })).await;
                if status == 200 && data["promise"]["state"] == "resolved" {
                    settled_unseen.insert(t.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = began.elapsed();
    for p in producers {
        p.await.expect("producer");
    }
    for (_, c) in consumers {
        c.abort();
    }
    stack.running.stop(Duration::from_secs(5)).await;

    // ─── Judgement ───
    let log = run.log.lock().unwrap();
    // A live consumer's answer may have landed after the store was asked.
    settled_unseen.retain(|t| !log.fulfilled.contains_key(t));
    let ttl_d = Duration::from_millis(ttl as u64);
    let bound = ttl_d + Duration::from_secs(1);

    // Acquired once per attempt: no version is held by two consumers.
    for (task, acquires) in &log.acquires {
        let versions: HashSet<i64> = acquires.iter().map(|(_, v, _)| *v).collect();
        assert_eq!(
            versions.len(),
            acquires.len(),
            "{task}: one version acquired twice: {acquires:?}"
        );
    }

    // One hand-off reaches at most one consumer. The router counts every
    // message a worker accepted; the consumers count every one they read. A
    // transport that gave one message to two consumers would have them read
    // more than it accepted. (Fewer is the victim: what was in its socket.)
    let handed_off = handed_off() - handed_off_before;
    let read: usize = log.deliveries.values().map(|d| d.len()).sum();
    assert!(
        read as f64 <= handed_off,
        "consumers read {read} messages, the transport accepted {handed_off}"
    );
    // The server sends a task again when its retry deadline passes before
    // anyone acquired it; those are counted, not forbidden.
    let mut retried = 0;
    for ds in log.deliveries.values() {
        let versions: HashSet<i64> = ds.iter().map(|(_, v, _)| *v).collect();
        retried += ds.len() - versions.len();
    }

    // What finished before the kill was acquired once.
    let mut clean = 0;
    for (task, (_, at)) in &log.fulfilled {
        if *at >= killed_at {
            continue;
        }
        clean += 1;
        let a = &log.acquires[task];
        assert_eq!(a.len(), 1, "{task} acquired more than once: {a:?}");
    }

    // Everything unfinished at the kill reached a live consumer, which
    // acquired it, within TTL + 1 s of the kill.
    let mut worst = Duration::ZERO;
    let mut redelivered = 0;
    for task in &pending_at_kill {
        if settled_unseen.contains(task) {
            continue; // the victim's fulfil landed
        }
        let (by, _) = &log.fulfilled[task];
        if by == VICTIM {
            continue; // its last answer raced the kill
        }
        let (_, _, acquired_at) = log.acquires[task]
            .iter()
            .find(|(who, _, _)| who == by)
            .expect("the fulfiller acquired it");
        let (_, _, delivered_at) = log.deliveries[task]
            .iter()
            .rfind(|(who, _, at)| who == by && at <= acquired_at)
            .expect("the fulfiller was sent it");
        let after = delivered_at.saturating_duration_since(killed_at);
        worst = worst.max(after);
        if log.deliveries[task].len() > 1 {
            redelivered += 1;
        }
        assert!(
            after <= bound,
            "{task} redelivered {after:?} after the kill; the bound is {bound:?}. \
             Created {:?}; deliveries {:?}; acquires {:?}",
            log.created.get(task).map(|(s, _)| rel(*s, killed_at)),
            log.deliveries[task]
                .iter()
                .map(|(w, v, at)| (w, v, rel(*at, killed_at)))
                .collect::<Vec<_>>(),
            log.acquires[task]
                .iter()
                .map(|(w, v, at)| (w, v, rel(*at, killed_at)))
                .collect::<Vec<_>>(),
        );
    }

    // The dead consumer is out of the group: what was created after it died
    // (with a moment for the server to notice the closed socket) reached a
    // live consumer at once, rather than going into the dead connection and
    // coming back on the retry deadline.
    let mut after_kill = 0;
    let mut slowest = Duration::ZERO;
    for (task, (sent, answered)) in &log.created {
        if *sent < killed_at + Duration::from_millis(250) {
            continue;
        }
        after_kill += 1;
        let first = log.deliveries[task]
            .iter()
            .map(|(_, _, at)| *at)
            .min()
            .expect("delivered");
        // From the create's answer: the server dispatches once the create
        // has committed, and how long a store takes to commit is not the
        // transport's doing.
        let took = first.saturating_duration_since(*answered);
        slowest = slowest.max(took);
        assert!(
            took < ttl_d,
            "{task}, created after the kill, first delivered after {took:?}"
        );
    }

    let mut by_consumer: HashMap<&str, usize> = HashMap::new();
    for ds in log.deliveries.values() {
        for (who, _, _) in ds {
            *by_consumer.entry(who.as_str()).or_default() += 1;
        }
    }
    let mut by_consumer: Vec<_> = by_consumer.into_iter().collect();
    by_consumer.sort();
    eprintln!(
        "[{}] {n} tasks in {elapsed:.2?} ({:.0}/s); deliveries by consumer {by_consumer:?}; \
         {clean} finished before the kill, acquired once each; {} unfinished at the kill, \
         {redelivered} of them redelivered, the last {worst:.2?} after the kill \
         (bound {bound:?}); {} settled by the victim unseen; {after_kill} created after \
         the kill, first delivered within {slowest:.2?}; {retried} redeliveries on a \
         retry deadline; {read} read of {handed_off} handed off; {} acquires refused",
        store.name(),
        n as f64 / elapsed.as_secs_f64(),
        pending_at_kill.len(),
        settled_unseen.len(),
        log.conflicts,
    );
    assert_eq!(log.fulfilled.len() + settled_unseen.len(), n);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_group_sqlite() {
    let dir = std::env::temp_dir().join(format!("resonate-fw8-{:08x}", fastrand::u32(..)));
    std::fs::create_dir_all(&dir).expect("temp dir");
    scenario(Store::Sqlite(dir.join("resonate.db"))).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_group_blob() {
    scenario(Store::Blob).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_group_postgres() {
    let Ok(url) = std::env::var("TEST_POSTGRES_URL") else {
        eprintln!("TEST_POSTGRES_URL unset: skipping the Postgres leg");
        return;
    };
    scenario(Store::Postgres(url)).await;
}
