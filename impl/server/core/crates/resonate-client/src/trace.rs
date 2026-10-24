//! Watching what a client exchanges, and recording it for the conformance
//! checkers.
//!
//! `spec/valid` checks a recorded run against the specification: the Lean
//! checker (`lake exe checktrace < trace.ndjson`) and the porcupine port
//! (`go run ./cmd/lincheck < trace.ndjson`). Both read one NDJSON line per
//! external call:
//!
//! ```json
//! {"kind":"promise.create","now":1000,"req":{…},"res":{"kind":…,"head":{…},"data":…}}
//! ```
//!
//! [`TraceRecorder`] writes exactly that from a client's own calls, so any
//! workload driven through a [`Client`](crate::Client) is a trace producer.
//! For `now` to mean anything to the checkers it must be the clock the
//! server judged deadlines by, so record against a server started in debug
//! mode and give the client a [`debug clock`](crate::Builder::debug_clock):
//! every request then carries `resonate:debug_time`, and that same instant is
//! the line's `now`.

use std::io::Write;
use std::sync::Mutex;

use serde_json::{json, Value};

/// One completed exchange, as an [`Observer`] sees it.
#[derive(Debug, Clone, Copy)]
pub struct Exchange<'a> {
    /// The request kind.
    pub kind: &'a str,
    /// The instant of the request: the debug clock when the client has one,
    /// otherwise the wall clock, in milliseconds.
    pub now: i64,
    /// The request's `data`.
    pub request: &'a Value,
    /// The whole response envelope, `{kind, head, data}`.
    pub response: &'a Value,
}

/// Sees every exchange that produced an answer, once, with its final answer.
///
/// An attempt that got no answer is not an exchange and is not reported; if
/// it was applied anyway, the record will show its effect without its call.
/// That is the [`Unavailable`](resonate_core::Unavailable) contract showing
/// through, and a trace meant for the checkers should come from a run that
/// needed no retries.
pub trait Observer: Send + Sync {
    fn observe(&self, exchange: &Exchange<'_>);
}

/// The kinds the `spec/valid` checkers model. Searches and `debug.*` are not
/// part of the specification, so a recorder leaves them out.
pub const SPEC_KINDS: &[&str] = &[
    "promise.get",
    "promise.create",
    "promise.settle",
    "promise.register_callback",
    "promise.register_listener",
    "task.get",
    "task.create",
    "task.acquire",
    "task.fence",
    "task.heartbeat",
    "task.suspend",
    "task.fulfill",
    "task.release",
    "task.halt",
    "task.continue",
    "schedule.get",
    "schedule.create",
    "schedule.delete",
];

/// Records exchanges as `spec/valid` trace lines, in observation order.
#[derive(Debug, Default)]
pub struct TraceRecorder {
    lines: Mutex<Vec<Value>>,
}

impl TraceRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The trace lines so far.
    pub fn lines(&self) -> Vec<Value> {
        self.lines.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The trace as NDJSON, one line per call, newline-terminated.
    pub fn to_ndjson(&self) -> String {
        let mut out = String::new();
        for line in self.lines() {
            out.push_str(&line.to_string());
            out.push('\n');
        }
        out
    }

    /// Write the trace as NDJSON.
    pub fn write_to(&self, mut w: impl Write) -> std::io::Result<()> {
        w.write_all(self.to_ndjson().as_bytes())
    }
}

impl Observer for TraceRecorder {
    fn observe(&self, x: &Exchange<'_>) {
        if !SPEC_KINDS.contains(&x.kind) {
            return;
        }
        self.lines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(json!({
                "kind": x.kind,
                "now": x.now,
                "req": x.request,
                "res": x.response,
            }));
    }
}
