//! Tracing for a Resonate server.
//!
//! Spans everywhere in this workspace come from the `tracing` crate and nothing
//! else, so whoever owns the process decides where they go. This crate is the
//! part of tracing that is more than `tracing::info_span!`:
//!
//! - [`TraceParent`]: a W3C trace context, read from a header or a tag, and
//!   attached to a span as its remote parent.
//! - [`incoming`]: the context a request arrived with, for code below the edge
//!   that has no headers to read.
//! - [`TraceContexts`]: which trace a promise belongs to, so a message emitted
//!   for it later — a retry, a resume — continues that trace.
//! - [`store_op`]: one span and one histogram sample per storage operation.
//! - `otlp` (feature `otlp`): the OpenTelemetry exporter, configured from the
//!   standard `OTEL_*` variables. Only a binary that owns the process installs
//!   it; a library embedding the server brings its own subscriber, and nothing
//!   here requires one. Without an OpenTelemetry layer every function in this
//!   crate still works: a context is carried as the string it arrived as.

mod context;
mod contexts;
mod metrics;
#[cfg(feature = "otlp")]
pub mod otlp;

pub use context::{
    current, from_request, from_tags, incoming, of, set_parent, with_incoming, TraceParent,
    TRACEPARENT, TRACESTATE,
};
pub use contexts::TraceContexts;
pub use metrics::{store_op, STORE_OP_DURATION};
