//! One span and one histogram sample per storage operation.

use std::future::Future;
use std::time::Instant;

use lazy_static::lazy_static;
use resonate_plugin::prometheus::{histogram_opts, register_histogram_vec, HistogramVec};
use tracing::Instrument;

lazy_static! {
    /// Storage latency, by backend, operation and outcome.
    ///
    /// No tenant, promise or key label: the cardinality is the backends times
    /// their operations times two, whatever the load.
    pub static ref STORE_OP_DURATION: HistogramVec = register_histogram_vec!(
        histogram_opts!(
            "resonate_store_op_duration_seconds",
            "Storage operation latency in seconds, by store, operation and outcome",
            vec![
                0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
                0.5, 1.0, 2.5, 5.0,
            ]
        ),
        &["store", "op", "outcome"]
    )
    .unwrap();
}

/// Run one storage operation inside a `resonate.store` span, and time it.
///
/// `store` is the backend (`sqlite`, `postgres`, `mysql`, `blob`, …) and `op`
/// the operation (`transact`, `query`, `get`, `put_if_match`, …); both are
/// static, because both become label values. `ok` says which outcome a result
/// is — a lost compare-and-swap is an answer, not a failure, so the caller
/// decides.
pub async fn store_op<F, T>(
    store: &'static str,
    op: &'static str,
    ok: impl FnOnce(&T) -> bool,
    f: F,
) -> T
where
    F: Future<Output = T>,
{
    let span = tracing::info_span!(
        "resonate.store",
        otel.name = %format_args!("{store}.{op}"),
        db.system = store,
        db.operation = op,
        outcome = tracing::field::Empty,
    );
    let start = Instant::now();
    let out = f.instrument(span.clone()).await;
    let outcome = if ok(&out) { "ok" } else { "error" };
    span.record("outcome", outcome);
    STORE_OP_DURATION
        .with_label_values(&[store, op, outcome])
        .observe(start.elapsed().as_secs_f64());
    out
}
