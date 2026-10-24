//! The W3C trace context: what a `traceparent` says, and how a span takes it.

use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use resonate_core::types::RequestEnvelope;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The header, and the tag, that carry a trace context. One name for both, so
/// a client that knows the header already knows the tag.
pub const TRACEPARENT: &str = "traceparent";
/// Vendor-specific trace state, carried beside [`TRACEPARENT`].
pub const TRACESTATE: &str = "tracestate";

/// The W3C limit a receiver must accept; anything longer is dropped rather
/// than truncated, since a truncated list is a different list.
const TRACESTATE_MAX: usize = 512;

/// A validated W3C trace context.
///
/// Holds the strings it was read from, not a parsed form, so that passing one
/// through a process with no OpenTelemetry installed hands on exactly what
/// arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceParent {
    traceparent: String,
    tracestate: Option<String>,
}

impl TraceParent {
    /// Read a `traceparent` (and its `tracestate`), or `None` if it is not one.
    ///
    /// The rules of W3C Trace Context level 1: lowercase hex, version `ff`
    /// refused, all-zero trace and parent ids refused, and a later version
    /// read for its first four fields. An invalid `tracestate` is dropped on
    /// its own; it never invalidates the `traceparent` beside it.
    pub fn parse(traceparent: &str, tracestate: Option<&str>) -> Option<Self> {
        let tp = traceparent.trim();
        if tp.len() < 55 {
            return None;
        }
        let b = tp.as_bytes();
        if b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return None;
        }
        let version = &tp[0..2];
        if !is_lower_hex(version) || version == "ff" {
            return None;
        }
        if version == "00" && tp.len() != 55 {
            return None;
        }
        if tp.len() > 55 && b[55] != b'-' {
            return None;
        }
        let (trace_id, parent_id, flags) = (&tp[3..35], &tp[36..52], &tp[53..55]);
        if !is_lower_hex(trace_id) || !is_lower_hex(parent_id) || !is_lower_hex(flags) {
            return None;
        }
        if trace_id.bytes().all(|c| c == b'0') || parent_id.bytes().all(|c| c == b'0') {
            return None;
        }
        let tracestate = tracestate
            .map(str::trim)
            .filter(|s| !s.is_empty() && s.len() <= TRACESTATE_MAX)
            .filter(|s| TraceState::from_str(s).is_ok())
            .map(str::to_string);
        Some(Self {
            // A later version is read for the four fields this version knows.
            traceparent: tp[..55].to_string(),
            tracestate,
        })
    }

    /// The `traceparent` value.
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// The `tracestate` value, when there is one.
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    /// The trace id, as 32 hex digits.
    pub fn trace_id(&self) -> &str {
        &self.traceparent[3..35]
    }

    /// The span this context names as the parent, as 16 hex digits.
    pub fn parent_id(&self) -> &str {
        &self.traceparent[36..52]
    }

    /// The context as OpenTelemetry's remote span context.
    pub fn span_context(&self) -> SpanContext {
        let trace_id = TraceId::from_hex(self.trace_id()).unwrap_or(TraceId::INVALID);
        let span_id = SpanId::from_hex(self.parent_id()).unwrap_or(SpanId::INVALID);
        let flags = u8::from_str_radix(&self.traceparent[53..55], 16).unwrap_or(0);
        let state = self
            .tracestate
            .as_deref()
            .and_then(|s| TraceState::from_str(s).ok())
            .unwrap_or_default();
        SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::new(flags) & TraceFlags::SAMPLED,
            true,
            state,
        )
    }

    /// The context naming `sc` as the parent, if `sc` is a real span.
    pub fn from_span_context(sc: &SpanContext) -> Option<Self> {
        if !sc.is_valid() {
            return None;
        }
        let traceparent = format!(
            "00-{}-{}-{:02x}",
            sc.trace_id(),
            sc.span_id(),
            sc.trace_flags() & TraceFlags::SAMPLED
        );
        let state = sc.trace_state().header();
        Some(Self {
            traceparent,
            tracestate: (!state.is_empty()).then_some(state),
        })
    }
}

fn is_lower_hex(s: &str) -> bool {
    s.bytes()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// Make `tp` the parent of `span`.
///
/// Must come before the span is first entered. A no-op when no OpenTelemetry
/// layer is installed — there is no OpenTelemetry span to give a parent to —
/// which is what lets the same code run in a process that exports nothing.
pub fn set_parent(span: &tracing::Span, tp: &TraceParent) {
    let cx = Context::new().with_remote_span_context(tp.span_context());
    let _ = span.set_parent(cx);
}

/// The context of `span`, as a child would name it: its trace, and itself as
/// the parent.
///
/// `None` without an OpenTelemetry layer, or for a span that is disabled.
pub fn of(span: &tracing::Span) -> Option<TraceParent> {
    let cx = span.context();
    let otel = cx.span();
    TraceParent::from_span_context(otel.span_context())
}

/// [`of`] the current span.
pub fn current() -> Option<TraceParent> {
    of(&tracing::Span::current())
}

tokio::task_local! {
    static INCOMING: Option<TraceParent>;
}

/// Run `f` with `tp` as the context its request arrived with.
///
/// The edge has headers and the code under it does not. A gateway reads the
/// header, and whatever runs inside `f` — the server's own span, the trace a
/// promise is filed under — can ask [`incoming`] without a parameter on every
/// port in between.
pub async fn with_incoming<F: Future>(tp: Option<TraceParent>, f: F) -> F::Output {
    INCOMING.scope(tp, f).await
}

/// The context the current request arrived with, if a gateway said.
pub fn incoming() -> Option<TraceParent> {
    INCOMING.try_with(|tp| tp.clone()).ok().flatten()
}

/// The context a promise's tags carry: the `traceparent` tag, with a
/// `tracestate` tag beside it if the client set one.
pub fn from_tags(tags: &HashMap<String, String>) -> Option<TraceParent> {
    TraceParent::parse(
        tags.get(TRACEPARENT)?,
        tags.get(TRACESTATE).map(String::as_str),
    )
}

/// The context a request carries in its body: the tags of the promise it
/// creates, for the two kinds that create one with a task.
///
/// Read from the JSON rather than the typed request, so it costs nothing for
/// every other kind and needs no validation the server does not already do.
pub fn from_request(req: &RequestEnvelope) -> Option<TraceParent> {
    let tags = match req.kind.as_str() {
        "promise.create" => req.data.get("tags"),
        "task.create" => req.data.get("action")?.get("data")?.get("tags"),
        _ => None,
    }?;
    TraceParent::parse(
        tags.get(TRACEPARENT)?.as_str()?,
        tags.get(TRACESTATE).and_then(|v| v.as_str()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn a_valid_traceparent_reads_back_as_written() {
        let tp = TraceParent::parse(TP, Some("congo=t61rcWkgMzE")).expect("valid");
        assert_eq!(tp.traceparent(), TP);
        assert_eq!(tp.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tp.parent_id(), "00f067aa0ba902b7");
        assert_eq!(tp.tracestate(), Some("congo=t61rcWkgMzE"));
        let back = TraceParent::from_span_context(&tp.span_context()).expect("valid");
        assert_eq!(back, tp);
    }

    #[test]
    fn what_the_spec_refuses_is_refused() {
        for bad in [
            "",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert!(TraceParent::parse(bad, None).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn a_later_version_is_read_for_the_fields_this_one_knows() {
        let tp = TraceParent::parse(
            "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-what-comes-next",
            None,
        )
        .expect("forward compatible");
        assert_eq!(tp.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn a_bad_tracestate_is_dropped_on_its_own() {
        let tp = TraceParent::parse(TP, Some("not a list entry")).expect("traceparent stands");
        assert_eq!(tp.tracestate(), None);
    }

    #[test]
    fn a_request_carries_its_context_in_the_tags_it_creates() {
        let req: RequestEnvelope = serde_json::from_value(serde_json::json!({
            "kind": "task.create",
            "head": {"corrId": "c", "version": "2026-04-01"},
            "data": {"pid": "p", "ttl": 1, "action": {"kind": "promise.create", "head": {},
                "data": {"id": "a", "timeoutAt": 1, "tags": {"traceparent": TP}}}}
        }))
        .unwrap();
        assert_eq!(
            from_request(&req).map(|t| t.trace_id().to_string()),
            Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string())
        );

        let other: RequestEnvelope = serde_json::from_value(serde_json::json!({
            "kind": "promise.get",
            "head": {"corrId": "c", "version": "2026-04-01"},
            "data": {"id": "a", "tags": {"traceparent": TP}}
        }))
        .unwrap();
        assert!(from_request(&other).is_none());
    }

    #[test]
    fn without_a_layer_nothing_is_current_and_nothing_breaks() {
        let span = tracing::info_span!("probe");
        set_parent(&span, &TraceParent::parse(TP, None).unwrap());
        assert!(of(&span).is_none());
        assert!(incoming().is_none());
    }

    #[tokio::test]
    async fn the_incoming_context_is_scoped_to_its_request() {
        let tp = TraceParent::parse(TP, None);
        let seen = with_incoming(tp.clone(), async { incoming() }).await;
        assert_eq!(seen, tp);
        assert!(incoming().is_none());
    }
}
