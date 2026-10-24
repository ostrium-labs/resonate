//! The router: one worker per address scheme, and the counter that sees every
//! message.
//!
//! Not an `axum::Router`, which routes an HTTP path to a handler. This one
//! routes an *address* to a worker: `poll://any@default` reaches the poll
//! transport. Bare `Router` in this workspace is always this one; axum's is
//! always written `axum::Router`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use lazy_static::lazy_static;
use resonate_plugin::prometheus::{register_counter_vec, CounterVec};

use resonate_core::types::Message;
use resonate_core::{scheme_of, Cause, ResonateRouter, ResonateWorker, Unavailable};
use resonate_trace::{TraceContexts, TraceParent};
use tracing::Instrument;

lazy_static! {
    /// Hand-offs to a worker, by outcome. Declared here because this is the one
    /// place that sees every message — and "never reached a worker" is an
    /// outcome only visible from here.
    pub static ref DELIVERIES_TOTAL: CounterVec = register_counter_vec!(
        "resonate_deliveries_total",
        "Total number of message deliveries by status",
        &["status"]
    )
    .unwrap();
}

// ---- Router ----

/// Routes a message to the worker registered for its address scheme.
///
/// The [`ResonateRouter`] this binary uses, and the only one there is. Not a
/// plugin: there is one of it, it is a dozen lines, and nothing about it varies
/// per deployment.
///
/// The router's knowledge of an address stops at the scheme: it reads the
/// scheme, looks up a worker, and hands over the untouched address string.
/// Registering a new scheme is therefore the whole cost of adding a worker —
/// nothing here, and nothing in `core`, has to change.
pub struct Router {
    /// Written once, at startup, and read on every message after.
    ///
    /// The router is the only participant that starts incomplete, and that is
    /// what breaks the cycle: a worker needs the server, the server is built
    /// from the router, so something has to exist before what it holds. Making
    /// it the router costs one `OnceLock` in a type no plugin ever sees — the
    /// alternatives were a handle to a server that did not exist yet, or
    /// interior mutability inside every server plugin.
    workers: OnceLock<HashMap<String, Arc<dyn ResonateWorker>>>,
    /// The trace each promise was created in, filed by the traced server and
    /// read here, so a message carries the trace of the work it starts.
    contexts: Arc<TraceContexts>,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    /// A router with nothing to route to yet. Built first, so the server has
    /// something to be handed.
    pub fn new() -> Self {
        Self {
            workers: OnceLock::new(),
            contexts: Arc::new(TraceContexts::default()),
        }
    }

    /// Where promises' traces are filed. The traced server writes it, and
    /// [`route`](ResonateRouter::route) reads it.
    pub fn contexts(&self) -> Arc<TraceContexts> {
        Arc::clone(&self.contexts)
    }

    /// Hand it the workers. Complete from here, and never changed again.
    ///
    /// A second call is refused rather than ignored: the losing set would be
    /// silently unreachable, which is a wiring bug worth hearing about.
    pub fn install(
        &self,
        workers: HashMap<String, Arc<dyn ResonateWorker>>,
    ) -> Result<(), &'static str> {
        self.workers
            .set(workers)
            .map_err(|_| "the router already has its workers")
    }

    /// What it can route to. Empty until `install`, so an address that arrives
    /// before startup finishes is reported undeliverable rather than panicking.
    fn workers(&self) -> Option<&HashMap<String, Arc<dyn ResonateWorker>>> {
        self.workers.get()
    }
}

/// The promise a message is about: an execute message's task, which shares its
/// promise's id, or an unblock message's promise.
fn subject(msg: &Message) -> &str {
    match msg {
        Message::Execute(m) => &m.data.task.id,
        Message::Unblock(m) => &m.data.promise.id,
    }
}

/// `msg`, carrying `tp` as the trace of the work it starts.
fn with_trace(msg: &Message, tp: &TraceParent) -> Message {
    let mut msg = msg.clone();
    let traceparent = Some(tp.traceparent().to_string());
    let tracestate = tp.tracestate().map(str::to_string);
    match &mut msg {
        Message::Execute(m) => {
            m.head.traceparent = traceparent;
            m.head.tracestate = tracestate;
        }
        Message::Unblock(m) => {
            m.head.traceparent = traceparent;
            m.head.tracestate = tracestate;
        }
    }
    msg
}

impl Router {
    /// Route one message inside its own span, parented on the trace its
    /// promise was created in.
    ///
    /// The span's parent is the filed trace when there is one and it is not
    /// the trace already current — a retry fired by a timer, a resume caused
    /// by another promise settling — and whatever is current otherwise. The
    /// message then carries this span as its parent, so the worker's requests
    /// continue from the hand-off. Without an OpenTelemetry layer there is no
    /// span to name, and the filed context is passed on as it was filed.
    async fn route_traced(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let (kind, id) = match msg {
            Message::Execute(_) => ("execute", subject(msg)),
            Message::Unblock(_) => ("unblock", subject(msg)),
        };
        let filed = self.contexts.get(id).or_else(|| match msg {
            Message::Unblock(m) => resonate_trace::from_tags(&m.data.promise.tags),
            Message::Execute(_) => None,
        });
        let span = tracing::info_span!(
            "resonate.route",
            otel.name = %format_args!("route {kind}"),
            otel.kind = "producer",
            resonate.message.kind = kind,
            resonate.promise.id = %id,
            resonate.address.scheme = scheme_of(address).as_deref().unwrap_or(""),
        );
        if let Some(tp) = &filed {
            let current = resonate_trace::current();
            if current.as_ref().map(|c| c.trace_id()) != Some(tp.trace_id()) {
                resonate_trace::set_parent(&span, tp);
            }
        }
        let carried = resonate_trace::of(&span).or(filed);
        let traced;
        let msg = match &carried {
            Some(tp) => {
                traced = with_trace(msg, tp);
                &traced
            }
            None => msg,
        };
        self.route_inner(address, msg).instrument(span).await
    }

    async fn route_inner(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let scheme = scheme_of(address)
            .ok_or_else(|| Unavailable::unroutable(format!("address is not a URI: {address}")))?;
        let worker = self.workers().and_then(|w| w.get(&scheme)).ok_or_else(|| {
            Unavailable::unroutable(format!("no worker registered for scheme '{scheme}'"))
        })?;
        worker.process(address, msg).await
    }
}

#[async_trait]
impl ResonateRouter for Router {
    // No `init` and no `stop`: the router routes. It holds the workers, but
    // holding a thing is not owning its lifecycle, and the routing table is the
    // wrong thing to drive one from — a worker claiming two schemes is in there
    // twice and would be started twice. The composition root starts each worker
    // once, from the list of what it built.

    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Delivery outcomes are recorded here rather than in the workers: the
        // router is the one place that sees every message, so a worker needs no
        // opinion about metrics — and "never reached a worker" is a distinct
        // outcome only visible from here.
        //
        // Note this counts the hand-off, not the eventual result. The HTTP push
        // and Pub/Sub workers enqueue onto a bounded channel and deliver
        // asynchronously past it, so `success` here means accepted for
        // delivery.
        let outcome = self.route_traced(address, msg).await;
        DELIVERIES_TOTAL
            .with_label_values(&[match &outcome {
                Ok(()) => "success",
                Err(e) if e.cause == Cause::Unroutable => "dropped",
                Err(_) => "error",
            }])
            .inc();
        outcome
    }
}

// ---- Test stubs ----

#[cfg(test)]
pub mod stubs {
    use super::*;
    use std::sync::Mutex;

    /// Records every `(address, message)` pair it is asked to deliver.
    ///
    /// One stub serves every scheme now that workers share a trait — register
    /// it under whichever scheme the test is exercising.
    pub struct RecordingWorker {
        pub calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl RecordingWorker {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(vec![]),
            }
        }

        /// Delivered messages as `(address, serialized message)`.
        pub fn calls(&self) -> Vec<(String, serde_json::Value)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Default for RecordingWorker {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ResonateWorker for RecordingWorker {
        async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
            let value = serde_json::to_value(msg).expect("message serializes");
            self.calls
                .lock()
                .unwrap()
                .push((address.to_string(), value));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::stubs::*;
    use super::*;
    use resonate_core::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};

    fn execute_msg() -> Message {
        Message::Execute(ExecuteMsg {
            kind: "execute".to_string(),
            head: MessageHead::new("http://localhost:8001".to_string()),
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: "t1".to_string(),
                    version: 1,
                },
            },
        })
    }

    fn router_with(scheme: &str, stub: Arc<RecordingWorker>) -> Router {
        let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
        workers.insert(scheme.to_string(), stub);
        let router = Router::new();
        router.install(workers).unwrap();
        router
    }

    fn empty_router() -> Router {
        let router = Router::new();
        router.install(HashMap::new()).unwrap();
        router
    }

    #[tokio::test]
    async fn routes_by_scheme_to_the_registered_worker() {
        for (scheme, address) in [
            ("http", "http://example.com/callback"),
            ("https", "https://example.com/secure"),
            ("poll", "poll://any@default"),
            ("gcps", "gcps://my-project/my-topic"),
            ("bash", "bash://docker/alpine"),
        ] {
            let stub = Arc::new(RecordingWorker::new());
            let router = router_with(scheme, stub.clone());
            router.route(address, &execute_msg()).await.unwrap();

            let calls = stub.calls();
            assert_eq!(calls.len(), 1, "for {address}");
            // The worker receives the address verbatim — the router does not
            // decompose it.
            assert_eq!(calls[0].0, address);
            assert_eq!(calls[0].1["kind"], "execute");
        }
    }

    #[tokio::test]
    async fn unregistered_scheme_is_reported_not_dropped() {
        for address in [
            "http://example.com/callback",
            "poll://any@default",
            "gcps://my-project/my-topic",
            "bash://docker/alpine",
        ] {
            let err = empty_router()
                .route(address, &execute_msg())
                .await
                .expect_err("no worker is registered");
            assert!(
                err.to_string().contains("no worker registered"),
                "for {address}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn unroutable_is_distinguishable_from_a_worker_failure() {
        // The router records delivery outcomes, and "never reached a worker"
        // is a different outcome from "a worker tried and failed" — the first
        // will never succeed on a retry, the second may.
        let err = empty_router()
            .route("poll://any@g", &execute_msg())
            .await
            .expect_err("no worker registered");
        assert_eq!(err.cause, Cause::Unroutable);

        let err = empty_router()
            .route("not a url", &execute_msg())
            .await
            .expect_err("not a URI");
        assert_eq!(err.cause, Cause::Unroutable);

        // A registered worker that fails is a delivery failure, not unroutable.
        struct FailingWorker;
        #[async_trait]
        impl ResonateWorker for FailingWorker {
            async fn process(&self, _a: &str, _m: &Message) -> Result<(), Unavailable> {
                Err(Unavailable::new("worker said no"))
            }
        }
        let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
        workers.insert("poll".to_string(), Arc::new(FailingWorker));
        let router = Router::new();
        router.install(workers).unwrap();
        let err = router
            .route("poll://any@g", &execute_msg())
            .await
            .expect_err("worker failed");
        assert_eq!(err.cause, Cause::Delivery);
    }

    #[tokio::test]
    async fn non_uri_address_is_reported() {
        let stub = Arc::new(RecordingWorker::new());
        let router = router_with("http", stub.clone());
        let err = router
            .route("not a url", &execute_msg())
            .await
            .expect_err("not a URI");
        assert!(err.to_string().contains("not a URI"), "{err}");
        assert!(stub.calls().is_empty());
    }

    #[tokio::test]
    async fn a_worker_serves_every_address_of_its_scheme() {
        let stub = Arc::new(RecordingWorker::new());
        let router = router_with("poll", stub.clone());
        for address in ["poll://any@a", "poll://uni@b/id", "poll://malformed"] {
            router.route(address, &execute_msg()).await.unwrap();
        }
        let calls = stub.calls();
        assert_eq!(calls.len(), 3);
        // Including the malformed one: rejecting it is the worker's job, not
        // the router's.
        assert_eq!(calls[2].0, "poll://malformed");
    }

    #[tokio::test]
    async fn a_router_with_nothing_installed_reports_undeliverable() {
        // The window between the router being built and its workers arriving.
        // Nothing routes then — no gateway is listening — but it must report
        // rather than panic if anything does.
        let err = Router::new()
            .route("http://example.com/", &execute_msg())
            .await
            .expect_err("no workers yet");
        assert_eq!(err.cause, Cause::Unroutable);
    }

    #[tokio::test]
    async fn a_filed_trace_rides_on_the_message_without_any_exporter() {
        // No subscriber at all: the context is passed on as it was filed.
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let stub = Arc::new(RecordingWorker::new());
        let router = router_with("poll", stub.clone());
        router
            .contexts()
            .insert("t1", TraceParent::parse(tp, Some("k=v")).unwrap());
        router.route("poll://any@g", &execute_msg()).await.unwrap();
        let calls = stub.calls();
        assert_eq!(calls[0].1["head"]["traceparent"], tp);
        assert_eq!(calls[0].1["head"]["tracestate"], "k=v");
    }

    #[tokio::test]
    async fn an_untraced_message_reads_as_it_always_has() {
        let stub = Arc::new(RecordingWorker::new());
        let router = router_with("poll", stub.clone());
        router.route("poll://any@g", &execute_msg()).await.unwrap();
        let head = &stub.calls()[0].1["head"];
        assert_eq!(
            head,
            &serde_json::json!({"serverUrl": "http://localhost:8001"})
        );
    }

    #[tokio::test]
    async fn workers_are_installed_once() {
        let router = Router::new();
        router.install(HashMap::new()).unwrap();
        router
            .install(HashMap::new())
            .expect_err("the losing set would be silently unreachable");
    }
}
