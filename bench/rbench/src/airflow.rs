//! Apache Airflow 3 over its REST API (`/api/v2`), driven by Goose.
//!
//! One workflow is one Dag run: `POST .../dagRuns` with the workload in `conf`,
//! then `GET` the run every `poll_ms` until it is terminal, then its task
//! instances for the per-step records (`queued_when` → `start_date` →
//! `end_date`, as Airflow records them). The end of a workflow is when rbench
//! *observes* the terminal state, so it carries up to `poll_ms` of polling
//! delay; the run's own `end_date` is kept in the step file as `run` for
//! comparison.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use goose::prelude::*;
use opentelemetry::Context;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::otel;
use crate::record::{now_ns, Collector, StepRec, WfRec};

#[derive(Clone, Debug)]
pub struct Cfg {
    pub run_id: String,
    pub system: String,
    pub workload: String,
    pub k: usize,
    pub l: usize,
    pub sleep_ms: u64,
    pub user: String,
    pub password: String,
    pub poll_ms: u64,
    pub timeout_s: u64,
}

pub struct Run {
    pub cfg: Cfg,
    pub col: Arc<Collector>,
    seq: AtomicU64,
    token: RwLock<Option<(String, u128)>>,
}

static RUN: OnceLock<Arc<Run>> = OnceLock::new();

pub fn install(cfg: Cfg, col: Arc<Collector>) -> Arc<Run> {
    let r = Arc::new(Run {
        cfg,
        col,
        seq: AtomicU64::new(0),
        token: RwLock::new(None),
    });
    let _ = RUN.set(r.clone());
    r
}

fn run() -> Arc<Run> {
    RUN.get().expect("installed").clone()
}

fn dag(workload: &str) -> &'static str {
    match workload {
        "fanout" => "rbench_fanout",
        "chain" => "rbench_chain",
        _ => "rbench_single",
    }
}

async fn token(user: &mut GooseUser) -> Result<String, String> {
    let r = run();
    if let Some((t, at)) = r.token.read().await.clone() {
        // Airflow's default JWT lifetime is 24 h; refresh hourly regardless.
        if now_ns() - at < 3_600_000_000_000 {
            return Ok(t);
        }
    }
    let mut w = r.token.write().await;
    if let Some((t, at)) = w.clone() {
        if now_ns() - at < 3_600_000_000_000 {
            return Ok(t);
        }
    }
    let rb = user
        .get_request_builder(&GooseMethod::Post, "/auth/token")
        .map_err(|e| format!("{e:?}"))?
        .json(&json!({"username": r.cfg.user, "password": r.cfg.password}));
    let g = user
        .request(GooseRequest::builder().set_request_builder(rb).name("auth.token").expect_status_code(201).build())
        .await
        .map_err(|e| format!("{e:?}"))?;
    let resp = g.response.map_err(|e| e.to_string())?;
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    let t = v["access_token"]
        .as_str()
        .ok_or_else(|| format!("no access_token in {v}"))?
        .to_string();
    *w = Some((t.clone(), now_ns()));
    Ok(t)
}

async fn call(
    user: &mut GooseUser,
    cx: &Context,
    method: GooseMethod,
    path: &str,
    name: &str,
    body: Option<Value>,
    expect: u16,
) -> Result<(u16, Value), String> {
    let r = run();
    let tok = token(user).await?;
    let (rcx, headers) = otel::request_span(cx, &format!("airflow {name}"));
    let mut rb = user
        .get_request_builder(&method, path)
        .map_err(|e| format!("{e:?}"))?
        .header("authorization", format!("Bearer {tok}"));
    for (k, v) in &headers {
        rb = rb.header(k, v);
    }
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    let t0 = now_ns();
    let g = user
        .request(GooseRequest::builder().set_request_builder(rb).name(name).expect_status_code(expect).build())
        .await
        .map_err(|e| format!("{e:?}"))?;
    let (st, v) = match g.response {
        Ok(resp) => {
            let st = resp.status().as_u16();
            (st, resp.json::<Value>().await.unwrap_or(Value::Null))
        }
        Err(e) => (0, json!(e.to_string())),
    };
    r.col.req(name, t0, st);
    otel::end_request(&rcx, st);
    Ok((st, v))
}

fn ts_ns(v: &Value) -> Option<u128> {
    let s = v.as_str()?;
    let t = chrono::DateTime::parse_from_rfc3339(s).ok()?;
    Some(t.timestamp_nanos_opt()? as u128)
}

async fn workflow(user: &mut GooseUser) -> Result<(), String> {
    let r = run();
    let n = r.seq.fetch_add(1, Ordering::Relaxed);
    let run_id = format!("{}-{}", r.cfg.run_id, n);
    let dag_id = dag(&r.cfg.workload);
    let cx = otel::workflow_span(&r.cfg.run_id, &r.cfg.workload, &run_id, &r.cfg.system);
    let t0 = now_ns();
    let mut conf = json!({"k": r.cfg.k, "l": r.cfg.l, "sleep_ms": r.cfg.sleep_ms});
    if let Some(tp) = otel::traceparent(&cx) {
        conf["traceparent"] = json!(tp);
    }
    let mut retries: u32 = 0;
    let res: Result<(), String> = async {
        // Retried like rbench's Resonate requests (up to 3 times on a transport
        // error, 429 or 5xx); a 409 on a retry means the earlier attempt created it.
        let mut attempt = 0;
        loop {
            let (st, v) = call(
                user,
                &cx,
                GooseMethod::Post,
                &format!("/api/v2/dags/{dag_id}/dagRuns"),
                "dagRuns.create",
                Some(json!({"dag_run_id": run_id, "logical_date": null, "conf": conf})),
                200,
            )
            .await?;
            if (200..300).contains(&st) || (st == 409 && attempt > 0) {
                break;
            }
            if (st == 0 || st == 429 || st >= 500) && attempt < 3 {
                attempt += 1;
                retries += 1;
                tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
                continue;
            }
            return Err(format!("dagRuns.create {st}: {v}"));
        }
        let deadline = t0 + r.cfg.timeout_s as u128 * 1_000_000_000;
        let state = loop {
            tokio::time::sleep(Duration::from_millis(r.cfg.poll_ms)).await;
            let (st, v) = call(
                user,
                &cx,
                GooseMethod::Get,
                &format!("/api/v2/dags/{dag_id}/dagRuns/{run_id}"),
                "dagRuns.get",
                None,
                200,
            )
            .await?;
            let s = v["state"].as_str().unwrap_or("").to_string();
            if st == 200 && (s == "success" || s == "failed") {
                break s;
            }
            if now_ns() > deadline {
                return Err(format!("run not terminal after {} s (state {s})", r.cfg.timeout_s));
            }
        };
        let end = now_ns();
        let (_, v) = call(
            user,
            &cx,
            GooseMethod::Get,
            &format!("/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances?limit=100"),
            "taskInstances.list",
            None,
            200,
        )
        .await?;
        for ti in v["task_instances"].as_array().cloned().unwrap_or_default() {
            let id = ti["task_id"].as_str().unwrap_or("?");
            let mi = ti["map_index"].as_i64().unwrap_or(-1);
            let kind = if id.starts_with("sleep") || id.starts_with("wait") { "sleep" } else { "task" };
            if ti["state"] == "skipped" {
                continue;
            }
            let (Some(start), Some(stop)) = (ts_ns(&ti["start_date"]), ts_ns(&ti["end_date"])) else {
                continue;
            };
            // A sleep's start_ns is its due time (as for Resonate and Temporal),
            // so end - start is how late the wake-up was.
            let (sched, start) = if kind == "sleep" {
                (Some(start), start + r.cfg.sleep_ms as u128 * 1_000_000)
            } else {
                (ts_ns(&ti["queued_when"]).or_else(|| ts_ns(&ti["scheduled_when"])), start)
            };
            r.col.step(StepRec {
                wf_id: run_id.clone(),
                step: if mi >= 0 { format!("{id}[{mi}]") } else { id.to_string() },
                kind: kind.into(),
                sched_ns: sched,
                start_ns: start,
                end_ns: stop,
                ok: ti["state"] == "success",
            });
        }
        r.col.wfs.lock().unwrap().push(WfRec {
            wf_id: run_id.clone(),
            start_ns: t0,
            end_ns: end,
            ok: state == "success",
            error: (state != "success").then(|| format!("dag run {state}")),
            retries: retries
                + v["task_instances"]
                    .as_array()
                    .map(|a| a.iter().map(|t| t["try_number"].as_u64().unwrap_or(1).saturating_sub(1) as u32).sum::<u32>())
                    .unwrap_or(0),
        });
        if state != "success" {
            r.col.error(&format!("dag run {state}"));
        }
        Ok(())
    }
    .await;
    if let Err(e) = &res {
        r.col.wf(WfRec {
            wf_id: run_id.clone(),
            start_ns: t0,
            end_ns: now_ns(),
            ok: false,
            error: Some(e.clone()),
            retries,
        });
    }
    otel::end_workflow(&cx, res.is_ok(), res.as_ref().err().map(String::as_str));
    res
}

pub async fn transaction(user: &mut GooseUser) -> TransactionResult {
    if let Err(e) = workflow(user).await {
        eprintln!("rbench: dag run failed: {e}");
    }
    Ok(())
}
