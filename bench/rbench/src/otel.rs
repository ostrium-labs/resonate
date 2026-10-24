//! OpenTelemetry: one `rbench.workflow` root span per workflow, a client span
//! per request, and the W3C `traceparent` of that span on the request.

use std::collections::HashMap;

use opentelemetry::global::{self, BoxedTracer};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{Span, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

pub struct Otel {
    provider: Option<SdkTracerProvider>,
}

/// Starts the exporter when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the default
/// `http://127.0.0.1:4317`) is reachable; with `RBENCH_OTEL=off` it is off.
pub fn init(service: &str, attrs: Vec<KeyValue>) -> Otel {
    if std::env::var("RBENCH_OTEL").as_deref() == Ok("off") {
        return Otel { provider: None };
    }
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());
    use opentelemetry_otlp::WithExportConfig;
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.clone())
        .build()
    {
        Ok(e) => e,
        Err(err) => {
            eprintln!("rbench: OTLP exporter off ({err})");
            return Otel { provider: None };
        }
    };
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service.to_string())
                .with_attributes(attrs)
                .build(),
        )
        .build();
    global::set_tracer_provider(provider.clone());
    eprintln!("rbench: OTLP traces to {endpoint} as service {service}");
    Otel {
        provider: Some(provider),
    }
}

impl Otel {
    pub fn shutdown(self) {
        if let Some(p) = self.provider {
            let _ = p.force_flush();
            let _ = p.shutdown();
        }
    }
}

pub fn tracer() -> BoxedTracer {
    global::tracer("rbench")
}

/// A workflow's root span, as a context to hang request spans off.
pub fn workflow_span(run: &str, workload: &str, wf_id: &str, system: &str) -> Context {
    let span = tracer()
        .span_builder("rbench.workflow")
        .with_kind(SpanKind::Client)
        .with_attributes(vec![
            KeyValue::new("rbench.run", run.to_string()),
            KeyValue::new("rbench.workload", workload.to_string()),
            KeyValue::new("rbench.wf_id", wf_id.to_string()),
            KeyValue::new("rbench.system", system.to_string()),
        ])
        .start(&tracer());
    Context::current_with_span(span)
}

pub fn end_workflow(cx: &Context, ok: bool, err: Option<&str>) {
    let span = cx.span();
    if !ok {
        span.set_status(Status::error(err.unwrap_or("failed").to_string()));
    }
    span.end();
}

/// A child span for one request; returns it and the headers to send.
pub fn request_span(parent: &Context, name: &str) -> (Context, HashMap<String, String>) {
    let span = tracer()
        .span_builder(name.to_string())
        .with_kind(SpanKind::Client)
        .start_with_context(&tracer(), parent);
    let cx = parent.with_span(span);
    let mut headers = HashMap::new();
    TraceContextPropagator::new().inject_context(&cx, &mut headers);
    (cx, headers)
}

pub fn end_request(cx: &Context, status: u16) {
    let span = cx.span();
    span.set_attribute(KeyValue::new("http.response.status_code", status as i64));
    if status == 0 || status >= 400 {
        span.set_status(Status::error(format!("status {status}")));
    }
    span.end();
}

pub fn traceparent(cx: &Context) -> Option<String> {
    let mut headers = HashMap::new();
    TraceContextPropagator::new().inject_context(cx, &mut headers);
    headers.remove("traceparent")
}

// Keep the trait in scope for `span.end()` on a `SpanRef`.
#[allow(unused_imports)]
use Span as _;
