//! Timeout processing — background loop.
//!
//! Periodically processes expired timeouts (promise, task retry, task lease)
//! and expired schedules.

use std::sync::Weak;
use std::time::Duration;

use crate::metrics;
use crate::server::Server;
use resonate_core::util;
use tracing::Instrument;

/// The backstop scan, started by [`Server::init`] and ended by
/// [`Server::stop`](crate::server::Server::stop).
///
/// The last resort, and no longer the mechanism: the timer fires a deadline the
/// moment it comes due, and its backfill re-reads the durable ones more often
/// than this runs. What is left for this is what neither sees — a deadline past
/// the wheel's horizon on a server holding more than the wheel's capacity.
///
/// Weak, because the server owns the task that runs this: a strong handle would
/// keep the server alive for as long as the loop, which is forever.
pub async fn run(
    server: Weak<Server>,
    interval_ms: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_millis(interval_ms);
    // Retention rides the same loop at its own cadence: a pass is a bounded
    // transaction, so it waits for nothing the sweep holds and nothing waits
    // for it. Read once — the policy is the engine's, fixed at open.
    let retention_every = server
        .upgrade()
        .and_then(|s| s.engine().ok().and_then(|e| e.retention().map(|r| r.interval)))
        .map(Duration::from_millis);
    let start = tokio::time::Instant::now();
    let mut next_sweep = start + interval;
    let mut next_prune = retention_every.map(|every| start + every);

    loop {
        let wake = match next_prune {
            Some(p) if p < next_sweep => p,
            _ => next_sweep,
        };
        tokio::select! {
            _ = tokio::time::sleep_until(wake) => {}
            _ = shutdown.changed() => {
                tracing::info!("Timeout processing loop shutting down");
                return;
            }
        }

        let Some(state) = server.upgrade() else {
            return;
        };
        let Ok(engine) = state.engine() else {
            return;
        };
        let now_instant = tokio::time::Instant::now();
        if let (Some(due), Some(every)) = (next_prune, retention_every) {
            if now_instant >= due {
                prune(engine.as_ref()).await;
                next_prune = Some(tokio::time::Instant::now() + every);
            }
        }
        if now_instant < next_sweep {
            continue;
        }
        next_sweep = tokio::time::Instant::now() + interval;
        let now = util::system_time_ms();
        let span = tracing::info_span!(
            "resonate.timer.sweep",
            resonate.schedules_fired = tracing::field::Empty,
            resonate.messages = tracing::field::Empty,
        );
        let tick = engine.tick(now).instrument(span.clone()).await;
        async {
            match tick {
                // The engine reports what happened; recording it is the
                // caller's job, the same way the router counts deliveries and
                // the workers do not. The messages the sweep emitted go out
                // here — a redispatched task and a fired schedule both owe
                // one, and there is no longer a pump to find them later.
                Ok((fired, messages, armed)) => {
                    let span = tracing::Span::current();
                    span.record("resonate.schedules_fired", fired);
                    span.record("resonate.messages", messages.len());
                    metrics::SCHEDULE_PROMISES_TOTAL.inc_by(fired as f64);
                    state.deliver(messages).await;
                    // A sweep arms too — a redispatched task gets a fresh
                    // retry deadline — and those are the deadlines most
                    // likely to come due soon. Dropping them would leave the
                    // timer knowing only what the request path told it.
                    state.arm(armed);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Background timeout processing failed: storage error");
                }
            }
        }
        .instrument(span)
        .await;
    }
}

/// The most passes one wake-up runs back to back. Each is one transaction of
/// at most `retention.batch` promises; a backlog drains this many batches per
/// interval and then yields to the sweep.
const MAX_PASSES_PER_WAKE: usize = 16;

/// Retention passes until one comes back short of a full batch.
async fn prune(engine: &dyn crate::engine::Engine) {
    let Some(batch) = engine.retention().map(|r| r.batch) else {
        return;
    };
    for _ in 0..MAX_PASSES_PER_WAKE {
        let now = util::system_time_ms();
        let span = tracing::info_span!(
            "resonate.retention.pass",
            resonate.pruned = tracing::field::Empty,
            resonate.tombstones_expired = tracing::field::Empty,
        );
        match engine.prune(now).instrument(span.clone()).await {
            Ok(report) => {
                span.record("resonate.pruned", report.pruned);
                span.record("resonate.tombstones_expired", report.expired);
                crate::retention::record(&report);
                if !report.full(batch) {
                    return;
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "Retention pass failed: storage error");
                return;
            }
        }
    }
}
