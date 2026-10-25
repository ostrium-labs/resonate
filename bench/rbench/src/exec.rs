//! `rbench exec`: run an SDK-level driver (see `bench/drivers/README.md`) and
//! read its JSON Lines records, so it reports in the same format as the Goose
//! workloads.

use std::process::Stdio;
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::record::{Collector, StepRec, WfRec};

pub struct Outcome {
    pub done: Option<Value>,
    pub exit: Option<i32>,
    pub pid: Option<u32>,
}

pub async fn run(
    cmd: &[String],
    env: Vec<(String, String)>,
    col: Arc<Collector>,
    log: &std::path::Path,
    on_spawn: impl FnOnce(u32),
) -> anyhow::Result<Outcome> {
    let (prog, args) = cmd
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("exec needs a command after --"))?;
    let stderr = std::fs::File::create(log)?;
    let mut child = tokio::process::Command::new(prog)
        .args(args)
        .envs(env)
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .spawn()?;
    let pid = child.id();
    if let Some(p) = pid {
        on_spawn(p);
    }
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut done = None;
    while let Some(line) = lines.next_line().await? {
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match v["type"].as_str() {
            Some("wf") => {
                if let Ok(r) = serde_json::from_value::<WfRec>(v) {
                    col.wf(r);
                }
            }
            Some("step") => {
                if let Ok(r) = serde_json::from_value::<StepRec>(v) {
                    col.step(r);
                }
            }
            Some("req") => {
                let kind = v["kind"].as_str().unwrap_or("?");
                let start = v["start_ns"].as_u64().unwrap_or(0) as u128;
                let dur = v["dur_ns"].as_u64().unwrap_or(0) as u128;
                col.reqs.lock().unwrap().push(crate::record::ReqRec {
                    kind: kind.to_string(),
                    start_ns: start,
                    dur_ns: dur,
                    status: v["status"].as_u64().unwrap_or(200) as u16,
                });
            }
            Some("done") => done = Some(v),
            _ => {}
        }
    }
    let status = child.wait().await?;
    Ok(Outcome {
        done,
        exit: status.code(),
        pid,
    })
}

#[allow(dead_code)]
fn _types(_: StepRec, _: WfRec) {}
