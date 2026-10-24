//! rbench: one harness, one metrics format, for durable-execution systems.
//!
//! - `rbench resonate`: raw Resonate (or the Loams `/durable/` endpoint) over
//!   HTTP, driven by Goose.
//! - `rbench airflow`: Apache Airflow 3 over its REST API, driven by Goose.
//! - `rbench exec -- <driver>`: an SDK-level driver (Temporal) that prints the
//!   records of `bench/drivers/README.md`.
//! - `rbench report <dir>`: every run's summary in one CSV and one table.
//! - `rbench rtt <host:port>`: TCP connect round trips, to separate WAN time.

mod airflow;
mod exec;
mod otel;
mod record;
mod report;
mod resonate;
mod sampler;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use goose::prelude::*;
use opentelemetry::KeyValue;
use serde_json::json;

use airflow::transaction as airflow_tx;
use record::{Collector, Summary};
use resonate::transaction as resonate_tx;

#[derive(Parser)]
#[command(name = "rbench", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Resonate(ResonateArgs),
    Airflow(AirflowArgs),
    Exec(ExecArgs),
    Report(ReportArgs),
    Rtt(RttArgs),
}

#[derive(Args, Clone)]
struct Common {
    /// fanout, chain or burst
    #[arg(long, env = "RBENCH_WORKLOAD")]
    workload: String,
    /// Workflows in total
    #[arg(short, long, env = "RBENCH_N", default_value_t = 200)]
    n: usize,
    /// Workflows in flight (Goose users); burst ignores it and uses N
    #[arg(short, long, env = "RBENCH_CONCURRENCY", default_value_t = 20)]
    concurrency: usize,
    #[arg(short, long, env = "RBENCH_K", default_value_t = 8)]
    k: usize,
    #[arg(short, long, env = "RBENCH_L", default_value_t = 5)]
    l: usize,
    #[arg(long, env = "RBENCH_SLEEP_MS", default_value_t = 1000)]
    sleep_ms: u64,
    /// The system label in results (e.g. resonate, loams-durable, temporal)
    #[arg(long, env = "RBENCH_SYSTEM")]
    system: String,
    /// The backend label in results (e.g. sqlite, postgres, tidb-aws-ap-southeast-1)
    #[arg(long, env = "RBENCH_BACKEND")]
    backend: String,
    /// Results root; the run goes to <out>/<run-id>/
    #[arg(long, default_value = "bench/results/2026-10-24/runs")]
    out: PathBuf,
    /// Run id (default: <system>-<backend>-<workload>-<unix seconds>)
    #[arg(long)]
    run_id: Option<String>,
    /// Processes to sample: pid:<n>, proc:<comm>, container:<name> (repeatable)
    #[arg(long)]
    watch: Vec<String>,
    /// Free-form notes stored in the summary (repeatable)
    #[arg(long)]
    note: Vec<String>,
    /// Goose hatch rate (users started per second)
    #[arg(long, default_value = "100")]
    hatch_rate: String,
}

impl Common {
    fn run_id(&self) -> String {
        self.run_id.clone().unwrap_or_else(|| {
            format!(
                "{}-{}-{}-{}",
                self.system,
                self.backend,
                self.workload,
                record::now_ns() / 1_000_000_000
            )
        })
    }
    /// Goose users and iterations per user for N workflows.
    fn shape(&self) -> (usize, usize) {
        if self.workload == "burst" {
            (self.n, 1)
        } else {
            let c = self.concurrency.max(1).min(self.n.max(1));
            (c, self.n.div_ceil(c))
        }
    }
    fn params(&self) -> BTreeMap<String, serde_json::Value> {
        let (users, iters) = self.shape();
        let mut p = BTreeMap::new();
        p.insert("n".into(), json!(users * iters));
        p.insert("concurrency".into(), json!(users));
        match self.workload.as_str() {
            "fanout" => {
                p.insert("k".into(), json!(self.k));
            }
            "chain" => {
                p.insert("l".into(), json!(self.l));
                p.insert("sleep_ms".into(), json!(self.sleep_ms));
            }
            _ => {}
        }
        p
    }
}

#[derive(Args)]
struct ResonateArgs {
    #[command(flatten)]
    common: Common,
    /// The server, e.g. http://127.0.0.1:18001
    #[arg(long, default_value = "http://127.0.0.1:18001")]
    url: String,
    /// The protocol path under the server (`/`, or `/durable/` for Loams)
    #[arg(long, default_value = "/")]
    path: String,
    /// An Authorization header value, from this environment variable
    #[arg(long)]
    auth_env: Option<String>,
    #[arg(long, value_enum, default_value_t = resonate::Wake::Push)]
    wake: resonate::Wake,
    /// Where the push listener binds (the server must reach it)
    #[arg(long, default_value = "127.0.0.1:19190")]
    push_listen: String,
    #[arg(long, default_value = "poll://any@rbench")]
    poll_target: String,
    #[arg(long, default_value_t = 60_000)]
    ttl_ms: u64,
    #[arg(long, default_value_t = 3)]
    max_retries: u32,
    /// Give timer promises a target, as the TypeScript SDK does (SQLite only)
    #[arg(long)]
    timer_target: bool,
}

#[derive(Args)]
struct AirflowArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long, default_value = "http://127.0.0.1:18080")]
    url: String,
    #[arg(long, default_value = "admin")]
    user: String,
    #[arg(long, env = "RBENCH_AIRFLOW_PASSWORD", default_value = "admin")]
    password: String,
    #[arg(long, default_value_t = 100)]
    poll_ms: u64,
    #[arg(long, default_value_t = 600)]
    timeout_s: u64,
}

#[derive(Args)]
struct ExecArgs {
    #[command(flatten)]
    common: Common,
    /// Also sample the driver process itself
    #[arg(long)]
    watch_driver: bool,
    #[arg(last = true, required = true)]
    cmd: Vec<String>,
}

#[derive(Args)]
struct ReportArgs {
    /// A results directory holding runs/<run-id>/summary.json
    dir: PathBuf,
}

#[derive(Args)]
struct RttArgs {
    /// host:port, repeatable
    targets: Vec<String>,
    #[arg(short, long, default_value_t = 20)]
    count: usize,
}

fn summary_base(c: &Common, run_id: &str) -> Summary {
    Summary {
        run_id: run_id.to_string(),
        system: c.system.clone(),
        backend: c.backend.clone(),
        workload: c.workload.clone(),
        params: c.params(),
        notes: c.note.clone(),
        ..Default::default()
    }
}

fn otel_for(c: &Common, run_id: &str) -> otel::Otel {
    otel::init(
        &format!("rbench-{}", c.system),
        vec![
            KeyValue::new("rbench.run", run_id.to_string()),
            KeyValue::new("rbench.backend", c.backend.clone()),
            KeyValue::new("rbench.workload", c.workload.clone()),
        ],
    )
}

/// The host's state, so a reader can tell a quiet run from a busy one.
fn host_env() -> serde_json::Value {
    let read = |p: &str| std::fs::read_to_string(p).unwrap_or_default();
    let cpu = read("/proc/cpuinfo")
        .lines()
        .find_map(|l| l.strip_prefix("model name").map(|v| v.trim_start_matches([' ', '\t', ':']).to_string()))
        .unwrap_or_default();
    let mem_kb: u64 = read("/proc/meminfo")
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0);
    let avail_kb: u64 = read("/proc/meminfo")
        .lines()
        .find_map(|l| l.strip_prefix("MemAvailable:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0);
    json!({
        "time": chrono::Utc::now().to_rfc3339(),
        "loadavg": read("/proc/loadavg").trim(),
        "cpu": cpu,
        "nproc": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "mem_total_mb": mem_kb / 1024,
        "mem_available_mb": avail_kb / 1024,
        "kernel": read("/proc/sys/kernel/osrelease").trim(),
        "argv": std::env::args().collect::<Vec<_>>(),
    })
}

fn finish(c: &Common, dir: &std::path::Path, col: &Collector, mut s: Summary) -> anyhow::Result<()> {
    col.write(dir)?;
    let mut env: serde_json::Value = std::fs::read(dir.join("env.json"))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(json!({}));
    env["end"] = host_env();
    std::fs::write(dir.join("env.json"), serde_json::to_vec_pretty(&env)?)?;
    s = col.summarize(s);
    std::fs::write(dir.join("summary.json"), serde_json::to_vec_pretty(&s)?)?;
    eprintln!(
        "rbench: {} {} {}: {}/{} ok, {:.1} wf/s, p50 {:.1} ms, p99 {:.1} ms, {} failed -> {}",
        c.system,
        c.backend,
        c.workload,
        s.completed,
        s.started,
        s.throughput_wf_s,
        s.latency.p50_ms,
        s.latency.p99_ms,
        s.failed,
        dir.display()
    );
    Ok(())
}

async fn goose_attack(
    c: &Common,
    host: &str,
    dir: &std::path::Path,
    tx: Transaction,
) -> anyhow::Result<()> {
    let (users, iters) = c.shape();
    let hatch = if c.workload == "burst" { "100000".to_string() } else { c.hatch_rate.clone() };
    let report = dir.join("goose-report.html");
    // Goose 0.18.1 resolves `iterations` from the configuration's own value
    // first, and a default configuration's 0 is a value, so
    // `GooseDefault::Iterations` never applies: set it on the configuration.
    let mut config = goose::config::GooseConfiguration::default();
    config.iterations = iters;
    // No controllers: they bind 0.0.0.0:5116/5117, and two runs at once collide.
    config.no_telnet = true;
    config.no_websocket = true;
    GooseAttack::initialize_with_config(config)?
        .register_scenario(scenario!("workflow").register_transaction(tx))
        .set_default(GooseDefault::Host, host)?
        .set_default(GooseDefault::Users, users)?
        .set_default(GooseDefault::HatchRate, hatch.as_str())?
        .set_default(GooseDefault::NoPrintMetrics, true)?
        .set_default(GooseDefault::ReportFile, report.to_str().unwrap())?
        .execute()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Resonate(a) => {
            let c = &a.common;
            let run_id = c.run_id();
            let dir = c.out.join(&run_id);
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join("env.json"), serde_json::to_vec_pretty(&json!({"start": host_env()}))?)?;
            let ot = otel_for(c, &run_id);
            let col = Arc::new(Collector::default());
            let auth = a.auth_env.as_ref().and_then(|v| std::env::var(v).ok());
            let cfg = resonate::Cfg {
                run_id: run_id.clone(),
                system: c.system.clone(),
                workload: c.workload.clone(),
                k: c.k,
                l: c.l,
                sleep_ms: c.sleep_ms,
                path: a.path.clone(),
                auth,
                wake: a.wake,
                push_url: format!("http://{}/", a.push_listen),
                poll_target: a.poll_target.clone(),
                ttl_ms: a.ttl_ms,
                max_retries: a.max_retries,
                timer_target: a.timer_target,
            };
            resonate::install(cfg, col.clone(), a.url.trim_end_matches('/').to_string());
            if a.wake == resonate::Wake::Push {
                let app = axum::Router::new().route("/", axum::routing::post(resonate::on_push));
                let l = tokio::net::TcpListener::bind(&a.push_listen).await?;
                tokio::spawn(async move { axum::serve(l, app).await });
            }
            let sampler = sampler::Sampler::start(&c.watch, &dir.join("resources.csv"))?;
            goose_attack(c, &a.url, &dir, transaction!(resonate_tx)).await?;
            let mut s = summary_base(c, &run_id);
            s.params.insert("wake".into(), json!(format!("{:?}", a.wake).to_lowercase()));
            if a.timer_target {
                s.params.insert("timer_target".into(), json!(true));
            }
            s.resources = sampler.finish();
            finish(c, &dir, &col, s)?;
            ot.shutdown();
        }
        Cmd::Airflow(a) => {
            let c = &a.common;
            let run_id = c.run_id();
            let dir = c.out.join(&run_id);
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join("env.json"), serde_json::to_vec_pretty(&json!({"start": host_env()}))?)?;
            let ot = otel_for(c, &run_id);
            let col = Arc::new(Collector::default());
            airflow::install(
                airflow::Cfg {
                    run_id: run_id.clone(),
                    system: c.system.clone(),
                    workload: c.workload.clone(),
                    k: c.k,
                    l: c.l,
                    sleep_ms: c.sleep_ms,
                    user: a.user.clone(),
                    password: a.password.clone(),
                    poll_ms: a.poll_ms,
                    timeout_s: a.timeout_s,
                },
                col.clone(),
            );
            let sampler = sampler::Sampler::start(&c.watch, &dir.join("resources.csv"))?;
            goose_attack(c, &a.url, &dir, transaction!(airflow_tx)).await?;
            let mut s = summary_base(c, &run_id);
            s.params.insert("poll_ms".into(), json!(a.poll_ms));
            s.resources = sampler.finish();
            finish(c, &dir, &col, s)?;
            ot.shutdown();
        }
        Cmd::Exec(a) => {
            let c = &a.common;
            let run_id = c.run_id();
            let dir = c.out.join(&run_id);
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join("env.json"), serde_json::to_vec_pretty(&json!({"start": host_env()}))?)?;
            let col = Arc::new(Collector::default());
            let (users, iters) = c.shape();
            let env = vec![
                ("RBENCH_WORKLOAD".into(), c.workload.clone()),
                ("RBENCH_N".into(), (users * iters).to_string()),
                ("RBENCH_CONCURRENCY".into(), users.to_string()),
                ("RBENCH_K".into(), c.k.to_string()),
                ("RBENCH_L".into(), c.l.to_string()),
                ("RBENCH_SLEEP_MS".into(), c.sleep_ms.to_string()),
                ("RBENCH_RUN_ID".into(), run_id.clone()),
                ("RBENCH_SYSTEM".into(), c.system.clone()),
                ("RBENCH_BACKEND".into(), c.backend.clone()),
                ("OTEL_SERVICE_NAME".into(), format!("rbench-{}", c.system)),
            ];
            let mut watch = c.watch.clone();
            let driver_pid = std::sync::Arc::new(std::sync::Mutex::new(None));
            let dp = driver_pid.clone();
            // The sampler needs the driver's pid, which exists only once it runs.
            let (pid_tx, pid_rx) = std::sync::mpsc::channel::<u32>();
            let want_driver = a.watch_driver;
            let col2 = col.clone();
            let cmd = a.cmd.clone();
            let log = dir.join("driver.log");
            let handle = tokio::spawn(async move {
                exec::run(&cmd, env, col2, &log, move |p| {
                    *dp.lock().unwrap() = Some(p);
                    let _ = pid_tx.send(p);
                })
                .await
            });
            if want_driver {
                if let Ok(p) = pid_rx.recv_timeout(std::time::Duration::from_secs(10)) {
                    watch.push(format!("pid:{p}"));
                }
            }
            let sampler = sampler::Sampler::start(&watch, &dir.join("resources.csv"))?;
            let out = handle.await??;
            let mut s = summary_base(c, &run_id);
            s.resources = sampler.finish();
            if out.exit != Some(0) {
                s.notes.push(format!("driver exited with {:?}", out.exit));
            }
            if out.done.is_none() {
                s.notes.push("driver printed no done record".into());
            }
            let _ = driver_pid;
            finish(c, &dir, &col, s)?;
        }
        Cmd::Report(a) => report::run(&a.dir)?,
        Cmd::Rtt(a) => {
            for t in &a.targets {
                let mut v = vec![];
                for _ in 0..a.count {
                    let t0 = std::time::Instant::now();
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        tokio::net::TcpStream::connect(t),
                    )
                    .await
                    {
                        Ok(Ok(_)) => v.push(t0.elapsed().as_nanos()),
                        _ => {}
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                let min_ms = v.iter().min().map(|m| *m as f64 / 1e6).unwrap_or(0.0);
                let d = record::dist(v);
                println!(
                    "{}",
                    json!({"target": t, "n": d.n, "min_ms": min_ms, "p50_ms": d.p50_ms, "p95_ms": d.p95_ms, "max_ms": d.max_ms})
                );
            }
        }
    }
    Ok(())
}
