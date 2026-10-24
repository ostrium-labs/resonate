//! One trace, create → task → settle, across an asynchronous hop.
//!
//! The server as `resonate_base::build` assembles it — the SQLite server, the
//! HTTP gateway's routes, and a worker that records what it is handed — with
//! an OpenTelemetry layer exporting to memory. A client creates a promise with
//! a `traceparent`; the execute message the worker receives carries the same
//! trace; the worker acquires and fulfills the task sending that context back
//! as the header; every span of all three requests is in the one trace.
//!
//! One test function, because a global subscriber can be installed once per
//! process and the SQLite engine needs the multi-threaded runtime, where a
//! thread-local default would miss the worker threads.

use std::sync::{Arc, Mutex, OnceLock};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use resonate_base::{build, Options, Registry};
use resonate_gateway_http::routes::{api_routes, AppState};
use resonate_plugin::axum;
use resonate_plugin::types::Message;
use resonate_plugin::{Loader, ResonateServer, ResonateWorker, Unavailable, WorkerPlugin};
use serde_json::{json, Value};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const TAG_TRACE: &str = "0af7651916cd43dd8448eb211c80319c";

// --- the worker: records each message, and nothing else ----------------------

fn messages() -> &'static Mutex<Vec<(String, Value)>> {
    static MESSAGES: OnceLock<Mutex<Vec<(String, Value)>>> = OnceLock::new();
    MESSAGES.get_or_init(Default::default)
}

struct Recorder;

#[async_trait::async_trait]
impl ResonateWorker for Recorder {
    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let value = serde_json::to_value(msg).expect("serializes");
        messages()
            .lock()
            .unwrap()
            .push((address.to_string(), value));
        Ok(())
    }
}

static RECORDER: WorkerPlugin = WorkerPlugin::new("resonate-test-recorder", &["test"], |_, _| {
    Ok(Some(Arc::new(Recorder) as Arc<dyn ResonateWorker>))
});

/// The execute message for `id`, once the server has routed it.
async fn execute_for(id: &str) -> Value {
    for _ in 0..200 {
        let found = messages()
            .lock()
            .unwrap()
            .iter()
            .find(|(_, m)| m["kind"] == "execute" && m["data"]["task"]["id"] == id)
            .map(|(_, m)| m.clone());
        if let Some(m) = found {
            return m;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("no execute message for {id}");
}

// --- the client --------------------------------------------------------------

async fn call(app: &axum::Router, traceparent: Option<&str>, kind: &str, data: Value) -> Value {
    let mut req = axum::http::Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json");
    if let Some(tp) = traceparent {
        req = req.header("traceparent", tp);
    }
    let body = json!({
        "kind": kind,
        "head": {"corrId": format!("{kind}-{}", data["id"].as_str().unwrap_or("")), "version": "2026-04-01"},
        "data": data,
    });
    let res = app
        .clone()
        .oneshot(req.body(axum::body::Body::from(body.to_string())).unwrap())
        .await
        .expect("infallible");
    let bytes = http_body_util::BodyExt::collect(res.into_body())
        .await
        .unwrap()
        .to_bytes();
    serde_json::from_slice(&bytes).expect("a response envelope")
}

fn far() -> i64 {
    resonate_core::util::system_time_ms() + 3_600_000
}

fn trace_of(span: &SpanData) -> String {
    span.span_context.trace_id().to_string()
}

fn attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_trace_crosses_create_task_and_settle() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
    tracing::subscriber::set_global_default(subscriber).expect("one subscriber");

    let registry = Registry::new()
        .server(&resonate_server_sqlite::PLUGIN)
        .worker(&RECORDER);
    let config = Loader::new()
        .set("servers.server_sqlite.path", ":memory:")
        .unwrap()
        .load();
    let running = build(&registry, &config, &Options::default()).expect("builds");
    running.start(false).await.expect("starts");
    let server: Arc<dyn ResonateServer> = Arc::clone(running.server());
    let app = |trace_tags| {
        api_routes().with_state(AppState {
            server: Arc::clone(&server),
            auth: None,
            trace_tags,
        })
    };
    let plain = app(false);

    // 1. Create, in the client's trace.
    let client = format!("00-{TRACE}-00f067aa0ba902b7-01");
    let created = call(
        &plain,
        Some(&client),
        "promise.create",
        json!({"id": "p1", "timeoutAt": far(), "tags": {"resonate:target": "test://worker"}}),
    )
    .await;
    assert_eq!(created["head"]["status"], 200, "{created}");
    // The header is not a tag the client asked for, so it is not one.
    assert!(created["data"]["promise"]["tags"]
        .get("traceparent")
        .is_none());

    // 2. The execute message carries the trace across the hop.
    let msg = execute_for("p1").await;
    let carried = msg["head"]["traceparent"]
        .as_str()
        .expect("the execute message carries a trace")
        .to_string();
    assert_eq!(&carried[3..35], TRACE, "{msg}");
    let version = msg["data"]["task"]["version"].clone();

    // 3. The worker acquires and fulfills, sending the context back.
    let acquired = call(
        &plain,
        Some(&carried),
        "task.acquire",
        json!({"id": "p1", "version": version, "pid": "w1", "ttl": 60_000}),
    )
    .await;
    assert_eq!(acquired["head"]["status"], 200, "{acquired}");
    let version = acquired["data"]["task"]["version"].clone();
    let fulfilled = call(
        &plain,
        Some(&carried),
        "task.fulfill",
        json!({"id": "p1", "version": version, "action": {"kind": "promise.settle", "head": {},
            "data": {"id": "p1", "state": "resolved", "value": {}}}}),
    )
    .await;
    assert_eq!(fulfilled["head"]["status"], 200, "{fulfilled}");

    // 4. Every span of the three requests is in the client's trace.
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    for corr in ["promise.create-p1", "task.acquire-p1", "task.fulfill-p1"] {
        let request = spans
            .iter()
            .find(|s| {
                attr(s, "resonate.corr_id").as_deref() == Some(corr)
                    && s.name == corr.split('-').next().unwrap()
            })
            .unwrap_or_else(|| panic!("a span for {corr}"));
        assert_eq!(trace_of(request), TRACE, "{corr}");
    }
    let in_trace: Vec<&SpanData> = spans.iter().filter(|s| trace_of(s) == TRACE).collect();
    for name in ["route execute", "sqlite.transact"] {
        assert!(
            in_trace.iter().any(|s| s.name == name),
            "{name} in the trace: {:?}",
            in_trace
                .iter()
                .map(|s| s.name.to_string())
                .collect::<Vec<_>>()
        );
    }
    // The worker's requests are children of the hand-off, not of the create.
    let route = in_trace.iter().find(|s| s.name == "route execute").unwrap();
    assert_eq!(&carried[36..52], route.span_context.span_id().to_string());

    // 5. A client that sets the tag and no header gets the same: the tag is
    //    the trace, and it is kept, because the client wrote it.
    let tagged = format!("00-{TAG_TRACE}-b7ad6b7169203331-01");
    let created = call(
        &plain,
        None,
        "promise.create",
        json!({"id": "p2", "timeoutAt": far(),
            "tags": {"resonate:target": "test://worker", "traceparent": tagged}}),
    )
    .await;
    assert_eq!(created["head"]["status"], 200, "{created}");
    assert_eq!(created["data"]["promise"]["tags"]["traceparent"], tagged);
    let msg = execute_for("p2").await;
    assert_eq!(
        &msg["head"]["traceparent"].as_str().unwrap()[3..35],
        TAG_TRACE
    );

    // 6. With `trace_tags` on, the header is kept as the tag.
    let created = call(
        &app(true),
        Some(&client),
        "promise.create",
        json!({"id": "p3", "timeoutAt": far(), "tags": {}}),
    )
    .await;
    assert_eq!(
        created["data"]["promise"]["tags"]["traceparent"], client,
        "{created}"
    );

    running.stop(std::time::Duration::from_secs(5)).await;
    let _ = provider.shutdown();
}
