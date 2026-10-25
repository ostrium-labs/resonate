//! Raw Resonate over its HTTP protocol (`POST /` of one request envelope,
//! protocol `2026-04-01`), driven by Goose.
//!
//! Each Goose user is both the caller and a co-located worker, issuing the
//! exact requests the TypeScript SDK issues for the same workflow
//! (`impl/sdk/ts/src/{resonate,core,context,util}.ts`):
//!
//! - start: `task.create` (the root promise, acquired by this worker);
//! - a step (`ctx.run` of a local function): `task.fence` + `promise.create`
//!   (scope local) then `task.fence` + `promise.settle`;
//! - a durable sleep (`ctx.sleep`): `task.fence` + `promise.create` with
//!   `resonate:timer`, then `task.suspend` with a callback on it; the server
//!   settles the timer at its deadline and re-dispatches the root task, which
//!   this worker learns about from the HTTP push transport (or by polling
//!   `task.get`) and claims again with `task.acquire`. The timer's own task is
//!   acquired and dropped, as the SDK does;
//! - end: `task.fulfill` settling the root promise.
//!
//! Requests issued one after another go through `GooseUser::request`, so Goose
//! counts them too. A fan-out's K parallel step requests use the user's
//! client directly (`request` takes `&mut self`, so one user cannot have two
//! in flight through it); every request, either way, is timed by rbench.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use goose::prelude::*;
use opentelemetry::Context;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::otel;
use crate::record::{now_ns, Collector, StepRec, WfRec};

pub const VERSION: &str = "2026-04-01";

#[derive(Clone, Debug)]
pub struct Cfg {
    pub run_id: String,
    pub system: String,
    pub workload: String,
    pub k: usize,
    pub l: usize,
    pub sleep_ms: u64,
    /// The path requests are POSTed to, under the host (`/` or `/durable/`).
    pub path: String,
    pub auth: Option<String>,
    /// `push`: the root's target is our push listener; `poll`: `task.get`.
    pub wake: Wake,
    pub push_url: String,
    pub poll_target: String,
    pub ttl_ms: u64,
    pub max_retries: u32,
    pub timer_target: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Wake {
    Push,
    Poll,
}

pub struct Run {
    pub cfg: Cfg,
    pub col: Arc<Collector>,
    seq: AtomicU64,
    waiters: Mutex<HashMap<String, oneshot::Sender<u64>>>,
    early: Mutex<HashMap<String, u64>>,
    pub client: reqwest::Client,
    pub base: String,
}

static RUN: OnceLock<Arc<Run>> = OnceLock::new();

pub fn install(cfg: Cfg, col: Arc<Collector>, base: String) -> Arc<Run> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(512)
        .build()
        .expect("http client");
    let run = Arc::new(Run {
        cfg,
        col,
        seq: AtomicU64::new(0),
        waiters: Mutex::new(HashMap::new()),
        early: Mutex::new(HashMap::new()),
        client,
        base,
    });
    let _ = RUN.set(run.clone());
    run
}

fn run() -> Arc<Run> {
    RUN.get().expect("run installed").clone()
}

/// The push transport's delivery: `{"kind":"execute","data":{"task":{"id","version"}}}`.
pub async fn on_push(body: axum::body::Bytes) -> axum::http::StatusCode {
    let Ok(v) = serde_json::from_slice::<Value>(&body) else {
        return axum::http::StatusCode::BAD_REQUEST;
    };
    if v["kind"] != "execute" {
        return axum::http::StatusCode::OK;
    }
    let id = v["data"]["task"]["id"].as_str().unwrap_or_default().to_string();
    let version = v["data"]["task"]["version"].as_u64().unwrap_or(0);
    let r = run();
    let waiter = r.waiters.lock().unwrap().remove(&id);
    match waiter {
        Some(tx) => {
            let _ = tx.send(version);
        }
        None if is_root(&id) => {
            r.early.lock().unwrap().insert(id, version);
        }
        None => {
            // A timer's task: acquire it and let the lease lapse (the SDK's
            // "dropping not-yet-due timer task").
            let r2 = r.clone();
            tokio::spawn(async move {
                let body = envelope(
                    "task.acquire",
                    json!({"id": id, "version": version, "pid": "rbench-timer", "ttl": r2.cfg.ttl_ms}),
                    Some(&origin_of(&id)),
                );
                let _ = raw_send(&r2, &Context::current(), "task.acquire(timer)", body).await;
            });
        }
    }
    axum::http::StatusCode::OK
}

fn is_root(id: &str) -> bool {
    !id.contains(':')
}

fn origin_of(id: &str) -> String {
    id.split(':').next().unwrap_or(id).to_string()
}

fn envelope(kind: &str, data: Value, origin: Option<&str>) -> Value {
    let mut head = json!({"corrId": format!("{:x}", now_ns()), "version": VERSION});
    if let Some(o) = origin {
        head["resonate:origin"] = json!(o);
    }
    json!({"kind": kind, "head": head, "data": data})
}

fn inner(kind: &str, data: Value) -> Value {
    envelope(kind, data, None)
}

/// The server's answer: (HTTP status, `head.status`, body).
struct Answer {
    status: u16,
    body: Value,
}

impl Answer {
    fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}

/// One request through Goose (sequential path).
async fn send(user: &mut GooseUser, cx: &Context, kind: &str, body: Value) -> Result<Answer, String> {
    let r = run();
    let mut attempt = 0;
    loop {
        let (rcx, headers) = otel::request_span(cx, &format!("resonate {kind}"));
        let mut rb = user
            .get_request_builder(&GooseMethod::Post, &r.cfg.path)
            .map_err(|e| format!("{e:?}"))?
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap());
        for (k, v) in &headers {
            rb = rb.header(k, v);
        }
        if let Some(a) = &r.cfg.auth {
            rb = rb.header("authorization", a);
        }
        let req = GooseRequest::builder()
            .set_request_builder(rb)
            .name(kind)
            // The protocol answers 300 for "continue" and 409 for a lost race;
            // rbench decides what is an error.
            .expect_status_code(200)
            .build();
        let t0 = now_ns();
        let res = user.request(req).await;
        let (status, value) = match res {
            Ok(g) => match g.response {
                Ok(resp) => {
                    let st = resp.status().as_u16();
                    let v = resp.json::<Value>().await.unwrap_or(Value::Null);
                    (st, v)
                }
                Err(e) => (0, json!(e.to_string())),
            },
            Err(e) => (0, json!(format!("{e:?}"))),
        };
        r.col.req(kind, t0, status);
        otel::end_request(&rcx, status);
        if (status == 0 || status == 429 || status == 503) && attempt < r.cfg.max_retries {
            attempt += 1;
            let _ = RETRIES.try_with(|c| c.set(c.get() + 1));
            tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
            continue;
        }
        return Ok(Answer { status, body: value });
    }
}

/// One request on the shared client (parallel path), timed by rbench only.
async fn raw_send(r: &Run, cx: &Context, kind: &str, body: Value) -> Result<Answer, String> {
    let mut attempt = 0;
    loop {
        let (rcx, headers) = otel::request_span(cx, &format!("resonate {kind}"));
        let mut rb = r
            .client
            .post(format!("{}{}", r.base, r.cfg.path))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap());
        for (k, v) in &headers {
            rb = rb.header(k, v);
        }
        if let Some(a) = &r.cfg.auth {
            rb = rb.header("authorization", a);
        }
        let t0 = now_ns();
        let (status, value) = match rb.send().await {
            Ok(resp) => {
                let st = resp.status().as_u16();
                (st, resp.json::<Value>().await.unwrap_or(Value::Null))
            }
            Err(e) => (0, json!(e.to_string())),
        };
        r.col.req(kind, t0, status);
        otel::end_request(&rcx, status);
        if (status == 0 || status == 429 || status == 503) && attempt < r.cfg.max_retries {
            attempt += 1;
            let _ = RETRIES.try_with(|c| c.set(c.get() + 1));
            tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
            continue;
        }
        return Ok(Answer { status, body: value });
    }
}

tokio::task_local! {
    static RETRIES: std::cell::Cell<u32>;
}

fn lineage(id: &str, parent: &str, root: &str, scope: &str) -> Value {
    json!({
        "resonate:scope": scope,
        "resonate:branch": if scope == "local" { root } else { id },
        "resonate:parent": parent,
        "resonate:origin": root,
    })
}

struct Wf {
    root: String,
    task: String,
    version: u64,
    cx: Context,
    seq: usize,
    /// The claim the heartbeat keeps alive: (version, held). The SDK beats
    /// every ttl/3 while it holds a task (`impl/sdk/ts/src/heartbeat.ts`).
    beat: Arc<Mutex<(u64, bool)>>,
    hb: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Wf {
    fn drop(&mut self) {
        if let Some(h) = self.hb.take() {
            h.abort();
        }
    }
}

fn heartbeat(pid: String, task: String, root: String, beat: Arc<Mutex<(u64, bool)>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let r = run();
        let every = Duration::from_millis((r.cfg.ttl_ms / 3).max(1000));
        loop {
            tokio::time::sleep(every).await;
            let (v, held) = *beat.lock().unwrap();
            if !held {
                continue;
            }
            let body = envelope(
                "task.heartbeat",
                json!({"pid": pid, "tasks": [{"id": task, "version": v}]}),
                Some(&root),
            );
            let _ = raw_send(&r, &Context::current(), "task.heartbeat", body).await;
        }
    })
}

impl Wf {
    fn next_id(&mut self) -> String {
        let id = format!("{}:{}", self.root, self.seq);
        self.seq += 1;
        id
    }
}

fn err_of(kind: &str, a: &Answer) -> String {
    let msg = a.body["data"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| a.body.to_string());
    format!("{kind} {}: {}", a.status, msg.chars().take(160).collect::<String>())
}

async fn start(user: &mut GooseUser, root: &str, cx: Context) -> Result<Wf, String> {
    let r = run();
    let target = match r.cfg.wake {
        Wake::Push => r.cfg.push_url.clone(),
        Wake::Poll => r.cfg.poll_target.clone(),
    };
    let mut tags = lineage(root, root, root, "global");
    tags["resonate:target"] = json!(target);
    tags["rbench:run"] = json!(r.cfg.run_id);
    if let Some(tp) = otel::traceparent(&cx) {
        tags["rbench:traceparent"] = json!(tp);
    }
    let param = json!({"headers": {}, "data": b64(&json!({"func": r.cfg.workload, "args": [r.cfg.k, r.cfg.l, r.cfg.sleep_ms], "version": 1}).to_string())});
    let body = envelope(
        "task.create",
        json!({
            "pid": format!("rbench-{}", user.weighted_users_index),
            "ttl": r.cfg.ttl_ms,
            "action": inner("promise.create", json!({
                "id": root,
                "timeoutAt": (now_ns() / 1_000_000) as u64 + 3_600_000,
                "param": param,
                "tags": tags,
            })),
        }),
        Some(root),
    );
    let a = send(user, &cx, "task.create", body).await?;
    if !a.ok() {
        return Err(err_of("task.create", &a));
    }
    let task = &a.body["data"]["task"];
    let task_id = task["id"].as_str().unwrap_or(root).to_string();
    let version = task["version"].as_u64().unwrap_or(1);
    let beat = Arc::new(Mutex::new((version, true)));
    let hb = heartbeat(format!("rbench-{}", user.weighted_users_index), task_id.clone(), root.to_string(), beat.clone());
    Ok(Wf {
        root: root.to_string(),
        task: task_id,
        version,
        cx,
        seq: 0,
        beat,
        hb: Some(hb),
    })
}

fn fence_body(wf: &Wf, action: Value) -> Value {
    envelope(
        "task.fence",
        json!({"id": wf.task, "version": wf.version, "action": action}),
        Some(&wf.root),
    )
}

fn child_create(wf: &Wf, id: &str) -> Value {
    fence_body(
        wf,
        inner(
            "promise.create",
            json!({
                "id": id,
                "timeoutAt": (now_ns() / 1_000_000) as u64 + 3_600_000,
                "param": {"headers": {}, "data": b64("{\"func\":\"noop\",\"args\":[]}")},
                "tags": lineage(id, &wf.root, &wf.root, "local"),
            }),
        ),
    )
}

fn child_settle(wf: &Wf, id: &str) -> Value {
    fence_body(
        wf,
        inner(
            "promise.settle",
            json!({"id": id, "state": "resolved", "value": {"headers": {}, "data": b64("null")}}),
        ),
    )
}

/// A step on the sequential path.
async fn step(user: &mut GooseUser, wf: &mut Wf) -> Result<(), String> {
    let r = run();
    let id = wf.next_id();
    let t0 = now_ns();
    let a = send(user, &wf.cx, "task.fence(promise.create)", child_create(wf, &id)).await?;
    if !a.ok() {
        return Err(err_of("fence create", &a));
    }
    let a = send(user, &wf.cx, "task.fence(promise.settle)", child_settle(wf, &id)).await?;
    if !a.ok() {
        return Err(err_of("fence settle", &a));
    }
    r.col.step(StepRec {
        wf_id: wf.root.clone(),
        step: id,
        kind: "activity".into(),
        sched_ns: None,
        start_ns: t0,
        end_ns: now_ns(),
        ok: true,
    });
    Ok(())
}

/// K steps in parallel on the shared client.
async fn fan(wf: &mut Wf, k: usize) -> Result<(), String> {
    let r = run();
    let ids: Vec<String> = (0..k).map(|_| wf.next_id()).collect();
    let wf_ref = &*wf;
    let futs = ids.into_iter().map(|id| {
        let r = r.clone();
        async move {
            let t0 = now_ns();
            let a = raw_send(&r, &wf_ref.cx, "task.fence(promise.create)", child_create(wf_ref, &id)).await?;
            if !a.ok() {
                return Err(err_of("fence create", &a));
            }
            let a = raw_send(&r, &wf_ref.cx, "task.fence(promise.settle)", child_settle(wf_ref, &id)).await?;
            if !a.ok() {
                return Err(err_of("fence settle", &a));
            }
            r.col.step(StepRec {
                wf_id: wf_ref.root.clone(),
                step: id,
                kind: "activity".into(),
                sched_ns: None,
                start_ns: t0,
                end_ns: now_ns(),
                ok: true,
            });
            Ok(())
        }
    });
    for res in futures::future::join_all(futs).await {
        res?;
    }
    Ok(())
}

/// A durable sleep: timer promise, suspend, wake, re-acquire.
async fn sleep(user: &mut GooseUser, wf: &mut Wf, ms: u64) -> Result<(), String> {
    let r = run();
    let id = wf.next_id();
    let t0 = now_ns();
    let due_ms = (t0 / 1_000_000) as u64 + ms;
    let mut tags = lineage(&id, &wf.root, &wf.root, "global");
    // The stores' shared model forbids a target on a timer (Postgres:
    // CHECK well_formed_promise_timer_not_targeted); the TypeScript SDK in this
    // tree sets one, which SQLite accepts. Off by default, so every store runs
    // the same requests.
    if r.cfg.timer_target {
        tags["resonate:target"] = json!(match r.cfg.wake {
            Wake::Push => r.cfg.push_url.clone(),
            Wake::Poll => r.cfg.poll_target.clone(),
        });
    }
    tags["resonate:timer"] = json!("true");
    let create = fence_body(
        wf,
        inner(
            "promise.create",
            json!({"id": id, "timeoutAt": due_ms, "param": {"headers": {}, "data": ""}, "tags": tags}),
        ),
    );
    let a = send(user, &wf.cx, "task.fence(timer)", create).await?;
    if !a.ok() {
        return Err(err_of("timer create", &a));
    }
    // Register the waiter before suspending, so a fast wake can't be missed.
    let (tx, rx) = oneshot::channel();
    if r.cfg.wake == Wake::Push {
        r.waiters.lock().unwrap().insert(wf.task.clone(), tx);
    }
    let suspend = envelope(
        "task.suspend",
        json!({
            "id": wf.task,
            "version": wf.version,
            "actions": [inner("promise.register_callback", json!({"awaiter": wf.root, "awaited": id}))],
        }),
        Some(&wf.root),
    );
    let a = send(user, &wf.cx, "task.suspend", suspend).await?;
    if a.status != 300 {
        wf.beat.lock().unwrap().1 = false;
    }
    let version = if a.status == 300 {
        // Already settled: carry on with the same claim.
        r.waiters.lock().unwrap().remove(&wf.task);
        None
    } else if !a.ok() {
        r.waiters.lock().unwrap().remove(&wf.task);
        return Err(err_of("task.suspend", &a));
    } else {
        Some(match r.cfg.wake {
            Wake::Push => {
                let early = r.early.lock().unwrap().remove(&wf.task);
                match early {
                    Some(v) => {
                        r.waiters.lock().unwrap().remove(&wf.task);
                        v
                    }
                    None => match tokio::time::timeout(Duration::from_millis(ms + 60_000), rx).await {
                        Ok(Ok(v)) => v,
                        _ => return Err("no wake-up within sleep + 60 s".into()),
                    },
                }
            }
            Wake::Poll => poll_pending(user, wf, ms).await?,
        })
    };
    if let Some(v) = version {
        let acq = envelope(
            "task.acquire",
            json!({"id": wf.task, "version": v, "pid": format!("rbench-{}", user.weighted_users_index), "ttl": r.cfg.ttl_ms}),
            Some(&wf.root),
        );
        let a = send(user, &wf.cx, "task.acquire", acq).await?;
        if !a.ok() {
            return Err(err_of("task.acquire", &a));
        }
        wf.version = a.body["data"]["task"]["version"].as_u64().unwrap_or(v + 1);
        *wf.beat.lock().unwrap() = (wf.version, true);
    }
    r.col.step(StepRec {
        wf_id: wf.root.clone(),
        step: id,
        kind: "sleep".into(),
        sched_ns: Some(t0),
        start_ns: due_ms as u128 * 1_000_000,
        end_ns: now_ns(),
        ok: true,
    });
    Ok(())
}

async fn poll_pending(user: &mut GooseUser, wf: &Wf, ms: u64) -> Result<u64, String> {
    tokio::time::sleep(Duration::from_millis(ms.saturating_sub(5))).await;
    let deadline = now_ns() + 60_000_000_000;
    loop {
        let get = envelope("task.get", json!({"id": wf.task}), Some(&wf.root));
        let a = send(user, &wf.cx, "task.get", get).await?;
        if a.ok() && a.body["data"]["task"]["state"] == "pending" {
            return Ok(a.body["data"]["task"]["version"].as_u64().unwrap_or(0));
        }
        if now_ns() > deadline {
            return Err("task not pending within sleep + 60 s".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn finish(user: &mut GooseUser, wf: &Wf) -> Result<(), String> {
    let body = envelope(
        "task.fulfill",
        json!({
            "id": wf.task,
            "version": wf.version,
            "action": inner("promise.settle", json!({"id": wf.root, "state": "resolved", "value": {"headers": {}, "data": b64("\"ok\"")}})),
        }),
        Some(&wf.root),
    );
    let a = send(user, &wf.cx, "task.fulfill", body).await?;
    if !a.ok() {
        return Err(err_of("task.fulfill", &a));
    }
    Ok(())
}

async fn workflow(user: &mut GooseUser) -> Result<(), String> {
    let r = run();
    let n = r.seq.fetch_add(1, Ordering::Relaxed);
    let root = format!("{}-{}", r.cfg.run_id, n);
    let cx = otel::workflow_span(&r.cfg.run_id, &r.cfg.workload, &root, &r.cfg.system);
    let t0 = now_ns();
    let (res, retries): (Result<(), String>, u32) = RETRIES
        .scope(std::cell::Cell::new(0), async {
            let res = async {
            let mut wf = start(user, &root, cx.clone()).await?;
            match r.cfg.workload.as_str() {
                "fanout" => fan(&mut wf, r.cfg.k).await?,
                "chain" => {
                    for _ in 0..r.cfg.l {
                        step(user, &mut wf).await?;
                        sleep(user, &mut wf, r.cfg.sleep_ms).await?;
                    }
                }
                "burst" => step(user, &mut wf).await?,
                w => return Err(format!("unknown workload {w}")),
            }
            finish(user, &wf).await
            }
            .await;
            (res, RETRIES.with(|c| c.get()))
        })
        .await;
    let end = now_ns();
    otel::end_workflow(&cx, res.is_ok(), res.as_ref().err().map(String::as_str));
    r.col.wf(WfRec {
        wf_id: root,
        start_ns: t0,
        end_ns: end,
        ok: res.is_ok(),
        error: res.as_ref().err().cloned(),
        retries,
    });
    res
}

/// The Goose transaction: one whole workflow.
pub async fn transaction(user: &mut GooseUser) -> TransactionResult {
    if let Err(e) = workflow(user).await {
        eprintln!("rbench: workflow failed: {e}");
    }
    Ok(())
}
