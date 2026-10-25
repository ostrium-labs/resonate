//! CPU and RSS of each process under test, sampled once a second.
//!
//! Targets: `pid:<n>` (a host process, threads included), `proc:<name>` (every
//! host process whose comm is `<name>`, re-resolved each sample, summed), and
//! `container:<name>` (a podman container's cgroup: `cpu.stat` and
//! `memory.current`, so every process in it counts).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::record::{now_ns, ResourceSummary};

#[derive(Clone, Debug)]
enum Target {
    Pid(u32),
    Proc(String),
    Container(String, Option<PathBuf>),
}

impl Target {
    fn parse(s: &str) -> anyhow::Result<Self> {
        let (k, v) = s
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("watch target {s:?} is not kind:value"))?;
        Ok(match k {
            "pid" => Target::Pid(v.parse()?),
            "proc" => Target::Proc(v.to_string()),
            "container" => Target::Container(v.to_string(), cgroup_of(v)),
            _ => anyhow::bail!("unknown watch kind {k:?} (pid, proc, container)"),
        })
    }
    fn label(&self) -> String {
        match self {
            Target::Pid(p) => format!("pid:{p}"),
            Target::Proc(n) => format!("proc:{n}"),
            Target::Container(n, _) => format!("container:{n}"),
        }
    }
    /// (cpu time in microseconds, rss in bytes)
    fn read(&self) -> Option<(u64, u64)> {
        match self {
            Target::Pid(p) => read_pid(*p),
            Target::Proc(name) => {
                let mut cpu = 0;
                let mut rss = 0;
                let mut any = false;
                for pid in pids_named(name) {
                    if let Some((c, r)) = read_pid(pid) {
                        cpu += c;
                        rss += r;
                        any = true;
                    }
                }
                any.then_some((cpu, rss))
            }
            Target::Container(_, Some(cg)) => {
                let stat = std::fs::read_to_string(cg.join("cpu.stat")).ok()?;
                let usage = stat
                    .lines()
                    .find_map(|l| l.strip_prefix("usage_usec "))
                    .and_then(|v| v.trim().parse().ok())?;
                let mem = std::fs::read_to_string(cg.join("memory.current"))
                    .ok()
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                Some((usage, mem))
            }
            Target::Container(_, None) => None,
        }
    }
}

fn cgroup_of(name: &str) -> Option<PathBuf> {
    let out = std::process::Command::new("podman")
        .args(["inspect", "--format", "{{.State.CgroupPath}}", name])
        .output()
        .ok()?;
    let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if p.is_empty() {
        eprintln!("rbench: no cgroup for container {name}");
        return None;
    }
    // A container's own cgroup may hold only sub-cgroups (podman's "container"
    // leaf); the parent's files already count them.
    Some(PathBuf::from(format!("/sys/fs/cgroup{p}")))
}

fn clk_tck() -> u64 {
    100
}

fn read_pid(pid: u32) -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm may contain spaces; fields after the closing paren.
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = f.get(11)?.parse().ok()?;
    let stime: u64 = f.get(12)?.parse().ok()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    Some(((utime + stime) * 1_000_000 / clk_tck(), rss_kb * 1024))
}

fn pids_named(name: &str) -> Vec<u32> {
    let mut v = vec![];
    if let Ok(rd) = std::fs::read_dir("/proc") {
        for e in rd.flatten() {
            if let Some(pid) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) {
                if let Ok(comm) = std::fs::read_to_string(format!("/proc/{pid}/comm")) {
                    if comm.trim() == name {
                        v.push(pid);
                    }
                }
            }
        }
    }
    v
}

pub struct Sampler {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Vec<ResourceSummary>>>,
}

impl Sampler {
    pub fn start(targets: &[String], csv: &Path) -> anyhow::Result<Self> {
        let targets: Vec<Target> = targets
            .iter()
            .map(|t| Target::parse(t))
            .collect::<anyhow::Result<_>>()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        std::fs::create_dir_all(csv.parent().unwrap_or(Path::new(".")))?;
        let mut out = std::io::BufWriter::new(std::fs::File::create(csv)?);
        let handle = std::thread::spawn(move || {
            let _ = writeln!(out, "t_ns,target,cpu_pct,rss_mb");
            let mut prev: Vec<Option<(u64, Instant)>> = vec![None; targets.len()];
            let mut acc: Vec<(f64, f64, f64, f64, usize)> = vec![(0.0, 0.0, 0.0, 0.0, 0); targets.len()];
            while !stop2.load(Ordering::Relaxed) {
                for (i, t) in targets.iter().enumerate() {
                    if let Some((cpu_us, rss)) = t.read() {
                        let now = Instant::now();
                        if let Some((p_us, p_t)) = prev[i] {
                            let dt = now.duration_since(p_t).as_secs_f64();
                            if dt > 0.0 {
                                let pct = (cpu_us.saturating_sub(p_us)) as f64 / 1e6 / dt * 100.0;
                                let mb = rss as f64 / 1048576.0;
                                let _ = writeln!(out, "{},{},{:.1},{:.1}", now_ns(), t.label(), pct, mb);
                                let a = &mut acc[i];
                                a.0 += pct;
                                a.1 = a.1.max(pct);
                                a.2 += mb;
                                a.3 = a.3.max(mb);
                                a.4 += 1;
                            }
                        }
                        prev[i] = Some((cpu_us, now));
                    }
                }
                std::thread::sleep(Duration::from_millis(1000));
            }
            let _ = out.flush();
            targets
                .iter()
                .zip(acc)
                .map(|(t, a)| ResourceSummary {
                    target: t.label(),
                    cpu_avg_pct: if a.4 > 0 { a.0 / a.4 as f64 } else { 0.0 },
                    cpu_max_pct: a.1,
                    rss_avg_mb: if a.4 > 0 { a.2 / a.4 as f64 } else { 0.0 },
                    rss_max_mb: a.3,
                    samples: a.4,
                })
                .collect()
        });
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    pub fn finish(mut self) -> Vec<ResourceSummary> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or_default()
    }
}
