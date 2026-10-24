//! The one metrics format every system reports in: workflow, step and
//! request records, collected in memory and written as CSV plus a summary.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// Wall-clock nanoseconds since the Unix epoch.
pub fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WfRec {
    pub wf_id: String,
    pub start_ns: u128,
    pub end_ns: u128,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRec {
    pub wf_id: String,
    pub step: String,
    pub kind: String,
    #[serde(default)]
    pub sched_ns: Option<u128>,
    pub start_ns: u128,
    pub end_ns: u128,
    #[serde(default = "yes")]
    pub ok: bool,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Serialize)]
pub struct ReqRec {
    pub kind: String,
    pub start_ns: u128,
    pub dur_ns: u128,
    pub status: u16,
}

#[derive(Default)]
pub struct Collector {
    pub wfs: Mutex<Vec<WfRec>>,
    pub steps: Mutex<Vec<StepRec>>,
    pub reqs: Mutex<Vec<ReqRec>>,
    pub errors: Mutex<BTreeMap<String, u64>>,
}

impl Collector {
    pub fn wf(&self, r: WfRec) {
        if let Some(e) = &r.error {
            self.error(e);
        }
        self.wfs.lock().unwrap().push(r);
    }
    pub fn step(&self, r: StepRec) {
        self.steps.lock().unwrap().push(r);
    }
    pub fn req(&self, kind: &str, start_ns: u128, status: u16) {
        let dur_ns = now_ns().saturating_sub(start_ns);
        self.reqs.lock().unwrap().push(ReqRec {
            kind: kind.to_string(),
            start_ns,
            dur_ns,
            status,
        });
    }
    pub fn error(&self, e: &str) {
        // Bucket by the first 80 characters so ids in messages don't explode it.
        let key: String = e.chars().take(80).collect();
        *self.errors.lock().unwrap().entry(key).or_default() += 1;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Dist {
    pub n: usize,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

pub fn dist(mut ns: Vec<u128>) -> Dist {
    if ns.is_empty() {
        return Dist::default();
    }
    ns.sort_unstable();
    let ms = |v: u128| v as f64 / 1e6;
    let pct = |p: f64| {
        let idx = ((p / 100.0) * (ns.len() as f64 - 1.0)).round() as usize;
        ms(ns[idx.min(ns.len() - 1)])
    };
    Dist {
        n: ns.len(),
        mean_ms: ns.iter().map(|v| *v as f64).sum::<f64>() / ns.len() as f64 / 1e6,
        p50_ms: pct(50.0),
        p95_ms: pct(95.0),
        p99_ms: pct(99.0),
        max_ms: ms(*ns.last().unwrap()),
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceSummary {
    pub target: String,
    pub cpu_avg_pct: f64,
    pub cpu_max_pct: f64,
    pub rss_avg_mb: f64,
    pub rss_max_mb: f64,
    pub samples: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub run_id: String,
    pub system: String,
    pub backend: String,
    pub workload: String,
    pub params: BTreeMap<String, serde_json::Value>,
    pub started: usize,
    pub completed: usize,
    pub failed: usize,
    pub retries: u64,
    pub wall_s: f64,
    pub throughput_wf_s: f64,
    pub latency: Dist,
    pub steps: BTreeMap<String, Dist>,
    /// Sleep steps only: how late the wake-up was past the timer's due time.
    pub sleep_lateness: Option<Dist>,
    pub requests: BTreeMap<String, Dist>,
    pub request_errors: u64,
    pub errors: BTreeMap<String, u64>,
    pub resources: Vec<ResourceSummary>,
    pub notes: Vec<String>,
}

impl Collector {
    pub fn summarize(&self, mut s: Summary) -> Summary {
        let wfs = self.wfs.lock().unwrap();
        s.started = wfs.len();
        s.completed = wfs.iter().filter(|w| w.ok).count();
        s.failed = wfs.len() - s.completed;
        s.retries = wfs.iter().map(|w| w.retries as u64).sum();
        if let (Some(first), Some(last)) = (
            wfs.iter().map(|w| w.start_ns).min(),
            wfs.iter().map(|w| w.end_ns).max(),
        ) {
            s.wall_s = (last.saturating_sub(first)) as f64 / 1e9;
        }
        if s.wall_s > 0.0 {
            s.throughput_wf_s = s.completed as f64 / s.wall_s;
        }
        s.latency = dist(
            wfs.iter()
                .filter(|w| w.ok)
                .map(|w| w.end_ns.saturating_sub(w.start_ns))
                .collect(),
        );
        let steps = self.steps.lock().unwrap();
        let mut by_kind: BTreeMap<String, Vec<u128>> = BTreeMap::new();
        let mut late = vec![];
        for st in steps.iter().filter(|st| st.ok) {
            let from = st.sched_ns.unwrap_or(st.start_ns);
            by_kind
                .entry(st.kind.clone())
                .or_default()
                .push(st.end_ns.saturating_sub(from));
            if st.kind == "sleep" {
                // For a sleep, start_ns is the due time.
                late.push(st.end_ns.saturating_sub(st.start_ns));
            }
        }
        s.steps = by_kind.into_iter().map(|(k, v)| (k, dist(v))).collect();
        if !late.is_empty() {
            s.sleep_lateness = Some(dist(late));
        }
        let reqs = self.reqs.lock().unwrap();
        let mut by_req: BTreeMap<String, Vec<u128>> = BTreeMap::new();
        for r in reqs.iter() {
            by_req.entry(r.kind.clone()).or_default().push(r.dur_ns);
        }
        s.request_errors = reqs
            .iter()
            .filter(|r| r.status == 0 || r.status >= 400)
            .count() as u64;
        s.requests = by_req.into_iter().map(|(k, v)| (k, dist(v))).collect();
        s.errors = self.errors.lock().unwrap().clone();
        s
    }

    pub fn write(&self, dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut f = std::io::BufWriter::new(std::fs::File::create(dir.join("workflows.csv"))?);
        writeln!(f, "wf_id,start_ns,end_ns,latency_ms,ok,retries,error")?;
        for w in self.wfs.lock().unwrap().iter() {
            writeln!(
                f,
                "{},{},{},{:.3},{},{},{}",
                w.wf_id,
                w.start_ns,
                w.end_ns,
                w.end_ns.saturating_sub(w.start_ns) as f64 / 1e6,
                w.ok,
                w.retries,
                csv_escape(w.error.as_deref().unwrap_or(""))
            )?;
        }
        let mut f = std::io::BufWriter::new(std::fs::File::create(dir.join("steps.csv"))?);
        writeln!(f, "wf_id,step,kind,sched_ns,start_ns,end_ns,latency_ms,ok")?;
        for st in self.steps.lock().unwrap().iter() {
            let from = st.sched_ns.unwrap_or(st.start_ns);
            writeln!(
                f,
                "{},{},{},{},{},{},{:.3},{}",
                st.wf_id,
                st.step,
                st.kind,
                st.sched_ns.map(|v| v.to_string()).unwrap_or_default(),
                st.start_ns,
                st.end_ns,
                st.end_ns.saturating_sub(from) as f64 / 1e6,
                st.ok
            )?;
        }
        let mut f = std::io::BufWriter::new(std::fs::File::create(dir.join("requests.csv"))?);
        writeln!(f, "kind,start_ns,dur_ms,status")?;
        for r in self.reqs.lock().unwrap().iter() {
            writeln!(
                f,
                "{},{},{:.3},{}",
                r.kind,
                r.start_ns,
                r.dur_ns as f64 / 1e6,
                r.status
            )?;
        }
        Ok(())
    }
}

fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\"").replace('\n', " "))
    } else {
        s.to_string()
    }
}
