//! `rbench report`: every run's summary.json in one CSV and one Markdown table.

use std::io::Write;
use std::path::Path;

use crate::record::Summary;

pub fn run(dir: &Path) -> anyhow::Result<()> {
    let runs = dir.join("runs");
    let mut all: Vec<Summary> = vec![];
    let mut load: std::collections::HashMap<String, String> = Default::default();
    for e in std::fs::read_dir(&runs)? {
        let dir = e?.path();
        let p = dir.join("summary.json");
        if let Ok(b) = std::fs::read(dir.join("env.json")) {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                let l1 = |k: &str| v[k]["loadavg"].as_str().and_then(|s| s.split_whitespace().next()).unwrap_or("?").to_string();
                load.insert(dir.file_name().unwrap().to_string_lossy().to_string(), format!("{}→{}", l1("start"), l1("end")));
            }
        }
        if let Ok(b) = std::fs::read(&p) {
            match serde_json::from_slice::<Summary>(&b) {
                Ok(s) => all.push(s),
                Err(err) => eprintln!("rbench: skipping {}: {err}", p.display()),
            }
        }
    }
    all.sort_by(|a, b| (&a.workload, &a.system, &a.backend, &a.run_id).cmp(&(&b.workload, &b.system, &b.backend, &b.run_id)));
    let mut csv = std::fs::File::create(dir.join("results.csv"))?;
    writeln!(csv, "run_id,workload,system,backend,params,started,completed,failed,retries,request_errors,wall_s,throughput_wf_s,p50_ms,p95_ms,p99_ms,max_ms,step_kind,step_p50_ms,step_p99_ms,sleep_late_p50_ms,sleep_late_p99_ms,cpu_avg_pct,rss_max_mb,load1")?;
    let mut md = std::fs::File::create(dir.join("results.md"))?;
    writeln!(md, "| Workload | System | Backend | Params | OK/started | wf/s | p50 ms | p95 ms | p99 ms | Step p50/p99 ms | Sleep late p50/p99 ms | Errors | Retries | CPU avg % (sum) | RSS max MB (sum) | Host load1 start→end |")?;
    writeln!(md, "|---|---|---|---|---|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---|")?;
    for s in &all {
        let params = s
            .params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");
        let (sk, sd) = s
            .steps
            .iter()
            .find(|(k, _)| k.as_str() != "sleep")
            .map(|(k, d)| (k.clone(), d.clone()))
            .unwrap_or_default();
        let late = s.sleep_lateness.clone().unwrap_or_default();
        let l = load.get(&s.run_id).cloned().unwrap_or_default();
        let cpu: f64 = s.resources.iter().map(|r| r.cpu_avg_pct).sum();
        let rss: f64 = s.resources.iter().map(|r| r.rss_max_mb).sum();
        writeln!(
            csv,
            "{},{},{},{},\"{}\",{},{},{},{},{},{:.2},{:.3},{:.2},{:.2},{:.2},{:.2},{},{:.2},{:.2},{:.2},{:.2},{:.1},{:.1},{}",
            s.run_id, s.workload, s.system, s.backend, params, s.started, s.completed, s.failed, s.retries, s.request_errors,
            s.wall_s, s.throughput_wf_s, s.latency.p50_ms, s.latency.p95_ms, s.latency.p99_ms, s.latency.max_ms,
            sk, sd.p50_ms, sd.p99_ms, late.p50_ms, late.p99_ms, cpu, rss, l
        )?;
        let late_s = if s.sleep_lateness.is_some() { format!("{:.1} / {:.1}", late.p50_ms, late.p99_ms) } else { "–".into() };
        writeln!(
            md,
            "| {} | {} | {} | {} | {}/{} | {:.2} | {:.0} | {:.0} | {:.0} | {:.1} / {:.1} | {} | {} | {} | {:.0} | {:.0} | {} |",
            s.workload, s.system, s.backend, params, s.completed, s.started, s.throughput_wf_s, s.latency.p50_ms,
            s.latency.p95_ms, s.latency.p99_ms, sd.p50_ms, sd.p99_ms, late_s, s.request_errors + s.failed as u64, s.retries, cpu, rss, l
        )?;
    }
    eprintln!("rbench: {} runs -> {}/results.{{csv,md}}", all.len(), dir.display());
    Ok(())
}
