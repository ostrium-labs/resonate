//! The OTLP exporter, from the standard OpenTelemetry environment.
//!
//! Off unless an endpoint is set. These are OpenTelemetry's variables, read
//! the way every OpenTelemetry SDK reads them, and none of them is a
//! `RESONATE_*` key — the server reads every one of those as configuration:
//!
//! | variable | meaning |
//! |---|---|
//! | `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | where to send; either one turns export on |
//! | `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `http/protobuf` (default) or `grpc` |
//! | `OTEL_EXPORTER_OTLP_HEADERS`, `…_TIMEOUT`, `…_COMPRESSION` | read by the exporter itself |
//! | `OTEL_SERVICE_NAME` | the service name; `resonate` when unset |
//! | `OTEL_RESOURCE_ATTRIBUTES` | further resource attributes |
//! | `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | the sampler, read by the SDK |
//! | `OTEL_SDK_DISABLED=true`, `OTEL_TRACES_EXPORTER=none` | off, whatever else is set |

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;

/// The service name when `OTEL_SERVICE_NAME` does not give one.
pub const DEFAULT_SERVICE_NAME: &str = "resonate";

/// An installed exporter. Hold it for the life of the process and
/// [`shutdown`](Exporter::shutdown) it last, which is what flushes the spans
/// still batched.
pub struct Exporter {
    provider: SdkTracerProvider,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// Whether the environment asks for traces to be exported.
pub fn enabled() -> bool {
    if env("OTEL_SDK_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return false;
    }
    if env("OTEL_TRACES_EXPORTER").is_some_and(|v| v.eq_ignore_ascii_case("none")) {
        return false;
    }
    env("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
}

impl Exporter {
    /// Build the exporter the environment describes, or `None` if it describes
    /// none.
    ///
    /// Call from inside the Tokio runtime: the gRPC channel is bound to the
    /// runtime it is built on.
    pub fn from_env() -> Result<Option<Self>, String> {
        if !enabled() {
            return Ok(None);
        }
        let protocol = env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .or_else(|| env("OTEL_EXPORTER_OTLP_PROTOCOL"))
            .unwrap_or_else(|| "http/protobuf".to_string());
        let exporter = match protocol.as_str() {
            "grpc" => SpanExporter::builder()
                .with_tonic()
                .build()
                .map_err(|e| format!("OTLP gRPC exporter: {e}"))?,
            "http/protobuf" => SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .build()
                .map_err(|e| format!("OTLP HTTP exporter: {e}"))?,
            other => {
                return Err(format!(
                    "OTEL_EXPORTER_OTLP_PROTOCOL={other} is not supported: use \
                     `http/protobuf` or `grpc`"
                ))
            }
        };
        Ok(Some(Self::with_exporter(exporter)))
    }

    /// An exporter over any span exporter — a test's in-memory one, say —
    /// with the resource and sampler the environment describes.
    pub fn with_exporter<E>(exporter: E) -> Self
    where
        E: opentelemetry_sdk::trace::SpanExporter + 'static,
    {
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource())
            .build();
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        opentelemetry::global::set_tracer_provider(provider.clone());
        Self { provider }
    }

    /// The `tracing` layer that turns this workspace's spans into
    /// OpenTelemetry spans.
    ///
    /// Filtered to Resonate's own crates at `info`, independently of the log
    /// level: an operator who logs at `warn` still gets traces, and the HTTP
    /// client and gRPC stack the exporter itself runs on are never traced into
    /// the exporter.
    pub fn layer<S>(&self) -> impl Layer<S> + Send + Sync + 'static
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
    {
        tracing_opentelemetry::layer()
            .with_tracer(self.provider.tracer("resonate"))
            .with_filter(
                // A prefix, so `resonate` is every `resonate_*` crate too.
                Targets::new().with_target("resonate", tracing::Level::INFO),
            )
    }

    /// Flush what is batched and stop. Blocks: call it from a blocking context
    /// (`spawn_blocking`) or after the runtime is gone.
    pub fn shutdown(self) {
        if let Err(e) = self.provider.force_flush() {
            eprintln!("OpenTelemetry flush failed: {e}");
        }
        if let Err(e) = self.provider.shutdown() {
            eprintln!("OpenTelemetry shutdown failed: {e}");
        }
    }
}

/// The SDK's resource from the environment, named `resonate` unless the
/// environment names it.
fn resource() -> Resource {
    let named = env("OTEL_SERVICE_NAME").is_some()
        || env("OTEL_RESOURCE_ATTRIBUTES").is_some_and(|a| {
            a.split(',')
                .any(|kv| kv.trim_start().starts_with("service.name="))
        });
    let builder = Resource::builder().with_attribute(opentelemetry::KeyValue::new(
        "service.version",
        env!("CARGO_PKG_VERSION"),
    ));
    if named {
        builder.build()
    } else {
        builder.with_service_name(DEFAULT_SERVICE_NAME).build()
    }
}
