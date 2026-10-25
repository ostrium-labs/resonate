//! `resonate:delay` through the running timer, not a debug tick.
//!
//! `tests/delay.rs` drives the engine with `tick`. In a running server the
//! held-back dispatch comes from the timer instead: the create arms the task's
//! retry deadline at the delay, the wheel holds it, and `Server::fire` runs the
//! `Internal` that dispatches it. This checks that path end to end, with enough
//! tasks due at once that they fire side by side.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{Message, RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_core::{util, ResonateServer, Unavailable};
use resonate_server_sqlite::SqliteEngine;
use resonate_sql::engine::Engine;
use resonate_sql::server::{Options, Server};
use serde_json::json;

const W: &str = "poll://any@w";
const TASKS: usize = 40;

/// Records when each task's execute message was routed.
#[derive(Default)]
struct Executes(Mutex<HashMap<String, i64>>);

#[async_trait]
impl ResonateRouter for Executes {
    async fn route(&self, _address: &str, msg: &Message) -> Result<(), Unavailable> {
        if let Message::Execute(m) = msg {
            self.0
                .lock()
                .unwrap()
                .entry(m.data.task.id.clone())
                .or_insert_with(util::system_time_ms);
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_dispatch_goes_through_the_timer() {
    let routed = Arc::new(Executes::default());
    let server = Server::new(
        Box::new(|debug| {
            Box::pin(async move {
                let e = SqliteEngine::open(":memory:", 30_000, 10, true, debug)
                    .map_err(|e| Unavailable::new(e.to_string()))?;
                Ok(Arc::new(e) as Arc<dyn Engine>)
            })
        }),
        routed.clone(),
        Options {
            server_url: String::new(),
            wheel_capacity: 8192,
            wheel_refresh: 30_000,
            sweep_interval: 60_000,
        },
    );
    // Not debug: the timer and the sweep run, on the real clock.
    server.init(false).await.unwrap();

    let due = util::system_time_ms() + 500;
    for i in 0..TASKS {
        let req = RequestEnvelope {
            kind: "promise.create".to_string(),
            head: RequestHead {
                corr_id: format!("c{i}"),
                version: SUPPORTED_VERSIONS[0].to_string(),
                auth: None,
                debug_time: None,
            },
            data: json!({
                "id": format!("delay-timer:{i}"),
                "timeoutAt": due + 600_000,
                "param": {},
                "tags": { "resonate:target": W, "resonate:delay": due.to_string() },
            }),
        };
        let resp = server.process(&req).await.unwrap();
        assert_eq!(resp.head.status, 200);
    }
    assert!(
        routed.0.lock().unwrap().is_empty(),
        "a delayed task was dispatched at create"
    );

    // The sweep runs every 60 s, so within this window only the timer can
    // dispatch them.
    let start = Instant::now();
    while routed.0.lock().unwrap().len() < TASKS && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    server.stop().await.unwrap();

    let at = routed.0.lock().unwrap().clone();
    assert_eq!(at.len(), TASKS, "dispatched {} of {TASKS}", at.len());
    for (id, t) in &at {
        assert!(*t >= due, "{id} dispatched {} ms before its delay", due - t);
        assert!(*t - due < 2_000, "{id} dispatched {} ms late", t - due);
    }
}
