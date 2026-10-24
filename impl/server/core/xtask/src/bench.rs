//! `cargo xtask bench` — throughput and latency of a live server.
//!
//! Three phases against one server, each with `--clients` concurrent clients:
//!
//! 1. **create/settle** for `--duration` seconds: each client creates a
//!    promise carrying a `--payload`-byte value and settles it, in a loop.
//!    Reported as promises created and settled per second.
//! 2. **task claim** for `--duration` seconds: each client creates a promise
//!    targeted at a `poll://` group this harness listens on, and waits for it
//!    to run. A listener receives the execute message, acquires the task and
//!    fulfills it. Claim latency is from sending the create to the acquire's
//!    answer: what a worker waits between being asked for and owning a task.
//! 3. **timers**: `--timers` promises with a deadline `--timer-ms` ahead, each
//!    with a listener registered. When the server's timer rejects one as timed
//!    out, the unblock message arrives on the listener; lateness is its arrival
//!    minus the deadline.
//!
//! One JSON line (machine-readable, on stdout) and a table (on stderr).

use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, ValueEnum};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::{core, free_port, ready, with_fresh_db, Backend, DbArgs, Result, Server};

const VERSION: &str = "2026-04-01";

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BenchBackend {
    Sqlite,
    Postgres,
    Mysql,
    Tidb,
    Blob,
    /// The blob server over TiKV. Not in this build yet (FW1): bench a server
    /// that is already running with `--server-url`.
    Tikv,
}

impl BenchBackend {
    fn name(self) -> &'static str {
        match self {
            BenchBackend::Sqlite => "sqlite",
            BenchBackend::Postgres => "postgres",
            BenchBackend::Mysql => "mysql",
            BenchBackend::Tidb => "tidb",
            BenchBackend::Blob => "blob",
            BenchBackend::Tikv => "tikv",
        }
    }

    fn backend(self) -> Option<Backend> {
        match self {
            BenchBackend::Sqlite => Some(Backend::Sqlite),
            BenchBackend::Postgres => Some(Backend::Postgres),
            BenchBackend::Mysql => Some(Backend::Mysql),
            BenchBackend::Tidb => Some(Backend::Tidb),
            BenchBackend::Blob => Some(Backend::Blob),
            BenchBackend::Tikv => None,
        }
    }
}

#[derive(Args, Clone)]
pub struct BenchArgs {
    /// Concurrent clients in each phase.
    #[arg(long, default_value_t = 16)]
    pub clients: usize,
    /// Seconds each of the create/settle and task phases runs.
    #[arg(long, default_value_t = 10)]
    pub duration: u64,
    /// Bytes in each promise's param and value.
    #[arg(long, default_value_t = 256)]
    pub payload: usize,
    /// Promises in the timer phase.
    #[arg(long, default_value_t = 500)]
    pub timers: usize,
    /// How far ahead each timer promise's deadline is, in milliseconds.
    #[arg(long, default_value_t = 2_000)]
    pub timer_ms: i64,
    /// Bench a server that is already running, at this URL, instead of
    /// starting one. Its database is whatever it is pointed at: make it fresh.
    #[arg(long)]
    pub server_url: Option<String>,
    /// How long after the last deadline to wait for a late timer before it
    /// is counted as missing, in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    pub timer_grace_ms: i64,
    /// Where the started server keeps its files (the SQLite database). The
    /// default is under cargo's target directory, which is on a real disk,
    /// unlike a tmpfs `/tmp`.
    #[arg(long)]
    pub work_dir: Option<std::path::PathBuf>,
    /// The server binary to start, instead of building one in release mode.
    #[arg(long)]
    pub binary: Option<std::path::PathBuf>,
    /// The log level of the server this starts. `warn` by default, so the
    /// numbers are not the speed of a terminal.
    #[arg(long, default_value = "warn")]
    pub log_level: String,
    /// Phases to skip: `cs`, `task`, `timer`.
    #[arg(long, value_delimiter = ',')]
    pub skip: Vec<String>,
}

pub async fn bench(backend: BenchBackend, db: &DbArgs, args: &BenchArgs) -> Result<()> {
    if let Some(url) = &args.server_url {
        let report = run_phases(backend, url, args).await?;
        report.print();
        return Ok(());
    }
    let Some(kind) = backend.backend() else {
        return Err(format!(
            "{} has no server plugin in this build yet: start one and pass --server-url",
            backend.name()
        ));
    };
    let binary = match &args.binary {
        Some(b) => b.clone(),
        None => build_release()?,
    };
    let args = args.clone();
    let db_args = db.clone();
    let base = match &args.work_dir {
        Some(d) => d.clone(),
        None => crate::target_dir()?.join("xtask-bench"),
    };
    with_fresh_db(kind, db, "bench", move |url| {
        let dir = base.join(format!("run-{}-{}", std::process::id(), now_ms()));
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let port = free_port()?;
        let mut server = Command::new(&binary);
        server
            .arg("serve")
            .current_dir(&dir)
            .env("RESONATE_LEVEL", &args.log_level)
            .env(
                "RESONATE_GATEWAYS__GATEWAY_HTTP__BIND",
                format!("127.0.0.1:{port}"),
            )
            .env(
                "RESONATE_GATEWAYS__GATEWAY_METRICS__BIND",
                format!("127.0.0.1:{}", free_port()?),
            )
            .stdout(Stdio::from(
                std::fs::File::create(dir.join("server.log")).map_err(|e| e.to_string())?,
            ))
            .stderr(Stdio::from(
                std::fs::File::create(dir.join("server.err")).map_err(|e| e.to_string())?,
            ));
        crate::server_env(&mut server, kind, url, &db_args, &dir);
        eprintln!("\n==> server on 127.0.0.1:{port} ({dir:?})");
        let _server = Server(server.spawn().map_err(|e| format!("server: {e}"))?);
        let started = Instant::now();
        while !ready(port) {
            if started.elapsed() > Duration::from_secs(60) {
                return Err(format!(
                    "server did not become ready; see {}",
                    dir.join("server.err").display()
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        let url = format!("http://127.0.0.1:{port}");
        let report = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(run_phases(backend, &url, &args))
        })?;
        report.print();
        Ok(())
    })
    .await
}

/// Build the release server and copy it out of the target directory, which
/// other worktrees may be writing over.
fn build_release() -> Result<std::path::PathBuf> {
    crate::run(
        "cargo build --release --bin resonate",
        crate::cargo().args(["build", "--release", "--bin", "resonate"]),
    )?;
    let built = crate::target_dir()?.join("release/resonate");
    let dir = crate::target_dir()?.join("xtask-bench");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let copy = dir.join(format!("resonate-{}", std::process::id()));
    std::fs::copy(&built, &copy).map_err(|e| format!("copy {}: {e}", built.display()))?;
    Ok(copy)
}

// ---------------------------------------------------------------------------
// the client
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Client {
    http: reqwest::Client,
    url: String,
    seq: Arc<AtomicU64>,
}

impl Client {
    fn new(url: &str) -> Self {
        Self {
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(1_024)
                .build()
                .expect("an HTTP client"),
            url: url.trim_end_matches('/').to_string(),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// One protocol request; its in-band status and data.
    async fn rpc(&self, kind: &str, data: Value) -> std::result::Result<(i64, Value), String> {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "kind": kind,
            "head": {"corrId": format!("bench-{n}"), "version": VERSION},
            "data": data,
        });
        let resp = self
            .http
            .post(format!("{}/", self.url))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let status = v["head"]["status"].as_i64().unwrap_or(0);
        Ok((status, v["data"].clone()))
    }

    /// Open `GET /poll/{group}/{id}` and hand every message on it to `each`.
    async fn listen<F>(&self, group: &str, id: &str, each: F) -> Result<tokio::task::JoinHandle<()>>
    where
        F: Fn(Value) + Send + 'static,
    {
        let mut resp = self
            .http
            .get(format!("{}/poll/{group}/{id}", self.url))
            .send()
            .await
            .map_err(|e| format!("poll {group}/{id}: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("poll {group}/{id}: {}", resp.status()));
        }
        Ok(tokio::spawn(async move {
            let mut buf = String::new();
            while let Ok(Some(chunk)) = resp.chunk().await {
                buf.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(end) = buf.find("\n\n") {
                    let frame: String = buf.drain(..end + 2).collect();
                    let data: String = frame
                        .lines()
                        .filter_map(|l| l.strip_prefix("data:"))
                        .map(str::trim_start)
                        .collect::<Vec<_>>()
                        .join("\n");
                    if let Ok(v) = serde_json::from_str::<Value>(&data) {
                        each(v);
                    }
                }
            }
        }))
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `n` bytes of zeros, base64-encoded, for a payload of that size.
fn payload(n: usize) -> String {
    let mut s = "A".repeat(n / 3 * 4);
    match n % 3 {
        1 => s.push_str("AA=="),
        2 => s.push_str("AAA="),
        _ => {}
    }
    s
}

// ---------------------------------------------------------------------------
// the phases
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Report {
    backend: String,
    url: String,
    clients: usize,
    duration: u64,
    payload: usize,
    cs: Option<CreateSettle>,
    task: Option<Tasks>,
    timer: Option<Timers>,
}

struct CreateSettle {
    seconds: f64,
    created: u64,
    settled: u64,
    errors: u64,
    create_ms: Vec<f64>,
    settle_ms: Vec<f64>,
}

struct Tasks {
    seconds: f64,
    completed: u64,
    errors: u64,
    dispatch_ms: Vec<f64>,
    claim_ms: Vec<f64>,
    e2e_ms: Vec<f64>,
}

struct Timers {
    deadline_ms: i64,
    created: usize,
    fired: usize,
    late_ms: Vec<f64>,
}

async fn run_phases(backend: BenchBackend, url: &str, args: &BenchArgs) -> Result<Report> {
    let client = Client::new(url);
    let run = format!("{:x}", now_ms());
    let mut report = Report {
        backend: backend.name().to_string(),
        url: url.to_string(),
        clients: args.clients,
        duration: args.duration,
        payload: args.payload,
        ..Report::default()
    };
    let skip = |p: &str| args.skip.iter().any(|s| s == p);
    if !skip("cs") {
        eprintln!(
            "==> create/settle: {} clients, {}s",
            args.clients, args.duration
        );
        report.cs = Some(create_settle(&client, &run, args).await);
    }
    if !skip("task") {
        eprintln!(
            "==> task claim: {} clients, {}s",
            args.clients, args.duration
        );
        report.task = Some(tasks(&client, &run, args).await?);
    }
    if !skip("timer") {
        eprintln!(
            "==> timers: {} promises, deadline +{}ms",
            args.timers, args.timer_ms
        );
        report.timer = Some(timers(&client, &run, args).await?);
    }
    Ok(report)
}

async fn create_settle(client: &Client, run: &str, args: &BenchArgs) -> CreateSettle {
    let data = payload(args.payload);
    let until = Instant::now() + Duration::from_secs(args.duration);
    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..args.clients {
        let client = client.clone();
        let data = data.clone();
        let run = run.to_string();
        handles.push(tokio::spawn(async move {
            let (mut created, mut settled, mut errors) = (0u64, 0u64, 0u64);
            let (mut create_ms, mut settle_ms) = (Vec::new(), Vec::new());
            let mut n = 0u64;
            while Instant::now() < until {
                // No ':' — every promise its own origin, so a backend that
                // serializes per origin is not measured on one.
                let id = format!("cs-{run}-{c}-{n}");
                n += 1;
                let t = Instant::now();
                let r = client
                    .rpc(
                        "promise.create",
                        json!({"id": id, "timeoutAt": now_ms() + 3_600_000,
                            "param": {"data": data}, "tags": {}}),
                    )
                    .await;
                match r {
                    Ok((200 | 201, _)) => {
                        created += 1;
                        create_ms.push(t.elapsed().as_secs_f64() * 1e3);
                    }
                    _ => {
                        errors += 1;
                        continue;
                    }
                }
                let t = Instant::now();
                let r = client
                    .rpc(
                        "promise.settle",
                        json!({"id": id, "state": "resolved", "value": {"data": data}}),
                    )
                    .await;
                match r {
                    Ok((200, _)) => {
                        settled += 1;
                        settle_ms.push(t.elapsed().as_secs_f64() * 1e3);
                    }
                    _ => errors += 1,
                }
            }
            (created, settled, errors, create_ms, settle_ms)
        }));
    }
    let mut out = CreateSettle {
        seconds: 0.0,
        created: 0,
        settled: 0,
        errors: 0,
        create_ms: Vec::new(),
        settle_ms: Vec::new(),
    };
    for h in handles {
        if let Ok((c, s, e, cm, sm)) = h.await {
            out.created += c;
            out.settled += s;
            out.errors += e;
            out.create_ms.extend(cm);
            out.settle_ms.extend(sm);
        }
    }
    out.seconds = start.elapsed().as_secs_f64();
    out
}

/// What a waiting client is told by the listener that ran its task.
struct Done {
    dispatched: Instant,
    acquired: Instant,
    fulfilled: Instant,
    ok: bool,
}

type Waiting = Arc<Mutex<HashMap<String, oneshot::Sender<Done>>>>;

async fn tasks(client: &Client, run: &str, args: &BenchArgs) -> Result<Tasks> {
    let group = format!("bench-{run}");
    let waiting: Waiting = Arc::default();
    // One listener per client: each message goes to one connection in the
    // group, and runs on its own task so a slow acquire does not hold up the
    // next frame.
    let mut listeners = Vec::new();
    for w in 0..args.clients {
        let waiting = Arc::clone(&waiting);
        let worker = client.clone();
        let pid = format!("w{w}");
        listeners.push(
            client
                .listen(&group, &pid.clone(), move |msg| {
                    if msg["kind"] != "execute" {
                        return;
                    }
                    let dispatched = Instant::now();
                    let id = msg["data"]["task"]["id"].as_str().unwrap_or("").to_string();
                    let version = msg["data"]["task"]["version"].clone();
                    let (worker, waiting, pid) =
                        (worker.clone(), Arc::clone(&waiting), pid.clone());
                    tokio::spawn(async move {
                        let acquire = worker
                            .rpc(
                                "task.acquire",
                                json!({"id": id, "version": version, "pid": pid, "ttl": 60_000}),
                            )
                            .await;
                        let acquired = Instant::now();
                        // Acquiring moves the version on; fulfill names the new one.
                        let (ok, version) = match &acquire {
                            Ok((200, data)) => (true, data["task"]["version"].clone()),
                            _ => (false, version),
                        };
                        let fulfill = if ok {
                            worker
                                .rpc(
                                    "task.fulfill",
                                    json!({"id": id, "version": version, "action": {
                                        "kind": "promise.settle", "head": {},
                                        "data": {"id": id, "state": "resolved", "value": {}}}}),
                                )
                                .await
                        } else {
                            Err("not acquired".into())
                        };
                        let fulfilled = Instant::now();
                        if let Some(tx) = waiting.lock().unwrap().remove(&id) {
                            let _ = tx.send(Done {
                                dispatched,
                                acquired,
                                fulfilled,
                                ok: ok && matches!(fulfill, Ok((200, _))),
                            });
                        }
                    });
                })
                .await?,
        );
    }

    let data = payload(args.payload);
    let until = Instant::now() + Duration::from_secs(args.duration);
    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..args.clients {
        let client = client.clone();
        let (waiting, group, data, run) = (
            Arc::clone(&waiting),
            group.clone(),
            data.clone(),
            run.to_string(),
        );
        handles.push(tokio::spawn(async move {
            let (mut completed, mut errors) = (0u64, 0u64);
            let (mut dispatch_ms, mut claim_ms, mut e2e_ms) = (Vec::new(), Vec::new(), Vec::new());
            let mut n = 0u64;
            while Instant::now() < until {
                let id = format!("task-{run}-{c}-{n}");
                n += 1;
                let (tx, rx) = oneshot::channel();
                waiting.lock().unwrap().insert(id.clone(), tx);
                let t = Instant::now();
                let r = client
                    .rpc(
                        "promise.create",
                        json!({"id": id, "timeoutAt": now_ms() + 3_600_000,
                            "param": {"data": data},
                            "tags": {"resonate:target": format!("poll://any@{group}")}}),
                    )
                    .await;
                if !matches!(r, Ok((200 | 201, _))) {
                    waiting.lock().unwrap().remove(&id);
                    errors += 1;
                    continue;
                }
                match tokio::time::timeout(Duration::from_secs(30), rx).await {
                    Ok(Ok(done)) if done.ok => {
                        completed += 1;
                        let ms = |i: Instant| (i - t).as_secs_f64() * 1e3;
                        dispatch_ms.push(ms(done.dispatched));
                        claim_ms.push(ms(done.acquired));
                        e2e_ms.push(ms(done.fulfilled));
                    }
                    _ => {
                        waiting.lock().unwrap().remove(&id);
                        errors += 1;
                    }
                }
            }
            (completed, errors, dispatch_ms, claim_ms, e2e_ms)
        }));
    }
    let mut out = Tasks {
        seconds: 0.0,
        completed: 0,
        errors: 0,
        dispatch_ms: Vec::new(),
        claim_ms: Vec::new(),
        e2e_ms: Vec::new(),
    };
    for h in handles {
        if let Ok((c, e, d, cl, ee)) = h.await {
            out.completed += c;
            out.errors += e;
            out.dispatch_ms.extend(d);
            out.claim_ms.extend(cl);
            out.e2e_ms.extend(ee);
        }
    }
    out.seconds = start.elapsed().as_secs_f64();
    for l in listeners {
        l.abort();
    }
    Ok(out)
}

async fn timers(client: &Client, run: &str, args: &BenchArgs) -> Result<Timers> {
    let group = format!("timers-{run}");
    // id → (deadline, arrival)
    let deadlines: Arc<Mutex<HashMap<String, i64>>> = Arc::default();
    let late: Arc<Mutex<Vec<f64>>> = Arc::default();
    let listener = {
        let (deadlines, late) = (Arc::clone(&deadlines), Arc::clone(&late));
        client
            .listen(&group, "t0", move |msg| {
                let arrived = now_ms();
                if msg["kind"] != "unblock" {
                    return;
                }
                let promise = &msg["data"]["promise"];
                if promise["state"] != "rejected_timedout" {
                    return;
                }
                let id = promise["id"].as_str().unwrap_or("");
                if let Some(deadline) = deadlines.lock().unwrap().remove(id) {
                    late.lock().unwrap().push((arrived - deadline) as f64);
                }
            })
            .await?
    };

    let per_client = args.timers.div_ceil(args.clients.max(1));
    let mut handles = Vec::new();
    for c in 0..args.clients {
        let (client, deadlines, group, run) = (
            client.clone(),
            Arc::clone(&deadlines),
            group.clone(),
            run.to_string(),
        );
        let timer_ms = args.timer_ms;
        let count = per_client.min(args.timers.saturating_sub(c * per_client));
        handles.push(tokio::spawn(async move {
            let mut created = 0usize;
            for n in 0..count {
                let id = format!("timer-{run}-{c}-{n}");
                let deadline = now_ms() + timer_ms;
                // Global, so the deadline is armed: an internal promise is
                // only ever expired lazily, when something next reads it.
                let r = client
                    .rpc(
                        "promise.create",
                        json!({"id": id, "timeoutAt": deadline, "tags": {"resonate:scope": "global"}}),
                    )
                    .await;
                if !matches!(r, Ok((200 | 201, _))) {
                    continue;
                }
                deadlines.lock().unwrap().insert(id.clone(), deadline);
                let r = client
                    .rpc(
                        "promise.register_listener",
                        json!({"awaited": id, "address": format!("poll://any@{group}")}),
                    )
                    .await;
                if matches!(r, Ok((200, _))) {
                    created += 1;
                } else {
                    deadlines.lock().unwrap().remove(&id);
                }
            }
            created
        }));
    }
    let mut created = 0;
    for h in handles {
        created += h.await.unwrap_or(0);
    }
    // Every deadline has passed, plus room for the slowest to land.
    let last = now_ms() + args.timer_ms;
    while now_ms() < last + args.timer_grace_ms {
        if deadlines.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    listener.abort();
    let late_ms = late.lock().unwrap().clone();
    Ok(Timers {
        deadline_ms: args.timer_ms,
        created,
        fired: late_ms.len(),
        late_ms,
    })
}

// ---------------------------------------------------------------------------
// the report
// ---------------------------------------------------------------------------

/// Nearest-rank percentile; NaN for no samples.
fn pct(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    let mut v = samples.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let rank = ((p / 100.0) * v.len() as f64).ceil() as usize;
    v[rank.clamp(1, v.len()) - 1]
}

fn round(x: f64) -> Value {
    if x.is_finite() {
        json!((x * 100.0).round() / 100.0)
    } else {
        Value::Null
    }
}

fn commit() -> String {
    Command::new("git")
        .current_dir(core())
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

impl Report {
    fn json(&self) -> Value {
        let mut v = json!({
            "bench": "resonate",
            "backend": self.backend,
            "server_url": self.url,
            "commit": commit(),
            "clients": self.clients,
            "duration_s": self.duration,
            "payload_bytes": self.payload,
            "host_cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            "unix_ms": now_ms(),
        });
        if let Some(cs) = &self.cs {
            v["create_per_s"] = round(cs.created as f64 / cs.seconds);
            v["settle_per_s"] = round(cs.settled as f64 / cs.seconds);
            v["create_p50_ms"] = round(pct(&cs.create_ms, 50.0));
            v["create_p99_ms"] = round(pct(&cs.create_ms, 99.0));
            v["settle_p50_ms"] = round(pct(&cs.settle_ms, 50.0));
            v["settle_p99_ms"] = round(pct(&cs.settle_ms, 99.0));
            v["cs_errors"] = json!(cs.errors);
        }
        if let Some(t) = &self.task {
            v["tasks_per_s"] = round(t.completed as f64 / t.seconds);
            v["dispatch_p50_ms"] = round(pct(&t.dispatch_ms, 50.0));
            v["claim_p50_ms"] = round(pct(&t.claim_ms, 50.0));
            v["claim_p95_ms"] = round(pct(&t.claim_ms, 95.0));
            v["claim_p99_ms"] = round(pct(&t.claim_ms, 99.0));
            v["task_e2e_p50_ms"] = round(pct(&t.e2e_ms, 50.0));
            v["task_e2e_p99_ms"] = round(pct(&t.e2e_ms, 99.0));
            v["task_errors"] = json!(t.errors);
        }
        if let Some(t) = &self.timer {
            v["timer_deadline_ms"] = json!(t.deadline_ms);
            v["timers_created"] = json!(t.created);
            v["timers_fired"] = json!(t.fired);
            v["timer_late_p50_ms"] = round(pct(&t.late_ms, 50.0));
            v["timer_late_p95_ms"] = round(pct(&t.late_ms, 95.0));
            v["timer_late_p99_ms"] = round(pct(&t.late_ms, 99.0));
            v["timer_late_max_ms"] = round(pct(&t.late_ms, 100.0));
        }
        v
    }

    fn print(&self) {
        let v = self.json();
        println!("{v}");
        let f = |k: &str| match &v[k] {
            Value::Null => "-".to_string(),
            x => x.to_string(),
        };
        let mut rows: Vec<(&str, String)> = vec![
            ("backend", self.backend.clone()),
            ("commit", f("commit").trim_matches('"').to_string()),
            (
                "clients / duration / payload",
                format!("{} / {}s / {}B", self.clients, self.duration, self.payload),
            ),
        ];
        if self.cs.is_some() {
            rows.extend([
                ("promise create/s", f("create_per_s")),
                ("promise settle/s", f("settle_per_s")),
                (
                    "create p50 / p99 ms",
                    format!("{} / {}", f("create_p50_ms"), f("create_p99_ms")),
                ),
                (
                    "settle p50 / p99 ms",
                    format!("{} / {}", f("settle_p50_ms"), f("settle_p99_ms")),
                ),
                ("create/settle errors", f("cs_errors")),
            ]);
        }
        if self.task.is_some() {
            rows.extend([
                ("tasks completed/s", f("tasks_per_s")),
                ("dispatch p50 ms", f("dispatch_p50_ms")),
                (
                    "claim p50 / p95 / p99 ms",
                    format!(
                        "{} / {} / {}",
                        f("claim_p50_ms"),
                        f("claim_p95_ms"),
                        f("claim_p99_ms")
                    ),
                ),
                (
                    "task e2e p50 / p99 ms",
                    format!("{} / {}", f("task_e2e_p50_ms"), f("task_e2e_p99_ms")),
                ),
                ("task errors", f("task_errors")),
            ]);
        }
        if self.timer.is_some() {
            rows.extend([
                (
                    "timers fired / created",
                    format!("{} / {}", f("timers_fired"), f("timers_created")),
                ),
                (
                    "timer lateness p50 / p95 / p99 / max ms",
                    format!(
                        "{} / {} / {} / {}",
                        f("timer_late_p50_ms"),
                        f("timer_late_p95_ms"),
                        f("timer_late_p99_ms"),
                        f("timer_late_max_ms")
                    ),
                ),
            ]);
        }
        let width = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
        eprintln!();
        for (k, val) in rows {
            eprintln!("  {k:<width$}  {val}");
        }
    }
}
