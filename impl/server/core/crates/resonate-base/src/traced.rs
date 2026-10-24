//! The server, traced: one span per request kind, and the trace each promise
//! is filed under.
//!
//! A decorator rather than a span in every server plugin, because what it
//! records is the same for all of them — the kind, the correlation id, the
//! status — and because filing a promise's trace has to happen beside the
//! router, which is this crate's. [`build`](crate::build) wraps whichever
//! server was chosen, so every caller of the port — the HTTP gateway, a worker
//! calling back in process, an embedder — goes through it.

use std::sync::Arc;

use async_trait::async_trait;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};
use resonate_trace::{TraceContexts, TraceParent};
use tracing::Instrument;

/// A [`ResonateServer`] that traces each request and files the trace of each
/// promise it creates with a task.
pub struct Traced {
    inner: Arc<dyn ResonateServer>,
    contexts: Arc<TraceContexts>,
}

impl Traced {
    pub fn new(inner: Arc<dyn ResonateServer>, contexts: Arc<TraceContexts>) -> Self {
        Self { inner, contexts }
    }
}

/// The id of the promise a request creates, for the kinds that create one.
fn created_id(req: &RequestEnvelope) -> Option<&str> {
    match req.kind.as_str() {
        "promise.create" => req.data.get("id")?.as_str(),
        "task.create" => req.data.get("action")?.get("data")?.get("id")?.as_str(),
        _ => None,
    }
}

/// The id of the task a request finishes, after which nothing is sent for it.
fn finished_id(req: &RequestEnvelope) -> Option<&str> {
    match req.kind.as_str() {
        "task.fulfill" | "promise.settle" => req.data.get("id")?.as_str(),
        _ => None,
    }
}

#[async_trait]
impl ResonateServer for Traced {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        self.inner.init(debug).await
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        self.inner.stop().await
    }

    async fn ready(&self) -> bool {
        self.inner.ready().await
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        let span = tracing::info_span!(
            "resonate.process",
            otel.name = %format_args!("process {}", req.kind),
            resonate.kind = %req.kind,
            resonate.corr_id = %req.head.corr_id,
            resonate.status = tracing::field::Empty,
        );
        // A context in the tags parents the request when nothing above it is
        // in a trace already: a caller in process, with no gateway to have
        // read a header. Under a gateway the request span has done this.
        let tagged = resonate_trace::from_request(req);
        if let Some(tp) = &tagged {
            if resonate_trace::current().is_none() {
                resonate_trace::set_parent(&span, tp);
            }
        }

        // Filed before the request runs, because the first execute message is
        // routed inside it. The trace is this request's own when tracing is
        // on — so the task hangs off the create — and otherwise the context
        // that came in, as it came.
        let filed = created_id(req).and_then(|id| {
            let tp: TraceParent = resonate_trace::of(&span)
                .or_else(|| tagged.clone())
                .or_else(resonate_trace::incoming)?;
            let before = self.contexts.get(id);
            self.contexts.insert(id, tp);
            Some((id, before))
        });

        let result = self.inner.process(req).instrument(span.clone()).await;

        let status = match &result {
            Ok(resp) => resp.head.status,
            Err(_) => 503,
        };
        span.record("resonate.status", status);
        if status >= 400 {
            // Nothing was created, so nothing is filed — put back what was.
            if let Some((id, before)) = filed {
                match before {
                    Some(tp) => self.contexts.insert(id, tp),
                    None => self.contexts.remove(id),
                }
            }
        } else if let Some(id) = finished_id(req) {
            self.contexts.remove(id);
        }
        result
    }
}
