//! The routes, their state, and the handlers behind them.
//!
//! Everything here is the translation between HTTP and the Resonate protocol:
//! bytes to an envelope, an envelope to a
//! [`ResonateServer::process`](resonate_core::ResonateServer::process) call,
//! and a response envelope back to a status and a body. No protocol decisions
//! are made here — what the protocol admits is `core`'s, and what an operation
//! does is the server's.

use crate::axum;

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{any, get, post},
    Json, Router,
};
use lazy_static::lazy_static;
// Through the ABI's re-export, so there is one default registry in the build.
use resonate_plugin::prometheus::{
    register_counter_vec, register_histogram_vec, CounterVec, HistogramVec,
};

use resonate_auth::AuthMode;
use resonate_core::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_core::{ui, ResonateServer};

lazy_static! {
    /// Requests by kind and status. Registered into prometheus' default
    /// registry, which is what the binary's `/metrics` endpoint gathers — so
    /// these appear alongside the engine's counters without the two crates
    /// having to share a registry handle.
    pub static ref REQUEST_TOTAL: CounterVec = register_counter_vec!(
        "resonate_request_total",
        "Total number of requests by kind and status",
        &["kind", "status"]
    )
    .unwrap();

    /// Request latency by kind.
    pub static ref REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "resonate_request_duration_seconds",
        "Request duration in seconds by kind",
        &["kind"]
    )
    .unwrap();
}

// === Shared application state ===

/// What every handler can reach.
///
/// Ports, not implementations: the server is `dyn ResonateServer`, so this
/// crate fronts the in-process engine, the reference model or a client for a
/// remote server without knowing which.
///
#[derive(Clone)]
pub struct AppState {
    pub server: Arc<dyn ResonateServer>,
    pub auth: Option<AuthMode>,
}

// Sub-state for API handlers — the server, and whether to authenticate.
#[derive(Clone)]
pub struct ApiState {
    pub server: Arc<dyn ResonateServer>,
    pub auth: Option<AuthMode>,
}

impl axum::extract::FromRef<AppState> for ApiState {
    fn from_ref(state: &AppState) -> Self {
        ApiState {
            server: state.server.clone(),
            auth: state.auth.clone(),
        }
    }
}

/// API routes: the RPC endpoint, the readiness probe, and the legacy paths.
pub fn api_routes() -> Router<AppState> {
    axum::Router::new()
        .route("/", post(handle_api))
        .route("/ready", get(handle_ready))
        .route("/promises", any(handle_legacy))
        .route("/promises/*path", any(handle_legacy))
        .route("/schedules", any(handle_legacy))
        .route("/schedules/*path", any(handle_legacy))
        .route("/tasks", any(handle_legacy))
        .route("/tasks/*path", any(handle_legacy))
}

async fn handle_legacy() -> impl IntoResponse {
    tracing::warn!(
        "Legacy endpoint hit — this path is no longer supported. \
        Please update to the latest SDK."
    );
    (
        StatusCode::GONE,
        Json(serde_json::json!({
            "error": "This endpoint is no longer supported. Please update to the latest SDK."
        })),
    )
}

/// The one probe: this process is running, and the server behind it says it
/// can serve.
///
/// `/health` alongside this said the same thing twice, so there is one. What
/// it answers is the server's to decide: `ready` defaults to true, and a
/// server that can actually tell — the blob server asks its bucket — answers
/// for itself, which is how a pod whose storage went away reports 503 rather
/// than taking traffic it cannot serve.
async fn handle_ready(State(state): State<AppState>) -> StatusCode {
    if state.server.ready().await {
        StatusCode::OK
    } else {
        tracing::error!("Readiness check failed: server reports not ready");
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn into_response(resp: ResponseEnvelope) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let code = axum::http::StatusCode::from_u16(resp.head.status as u16)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    (code, Json(resp))
}

/// The bearer token from `Authorization`, when there is one.
///
/// Exact `Bearer ` prefix — another scheme is not a token this endpoint can
/// read, so it is `None` rather than a guess. `Bearer ` with nothing after it
/// is `Some("")`, which the auth check rejects like any other bad token.
///
/// The poll transport does this for its own endpoint, in its own crate. It is
/// three lines against a header name fixed by the protocol; sharing them would
/// buy a dependency between two plugins to save nothing.
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

async fn handle_api(
    State(api_state): State<ApiState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let options = ProtocolOptions::gateway(api_state.auth);
    handle_envelope(api_state.server.as_ref(), &headers, &body, &options).await
}

// === The protocol endpoint, for embedders ===

/// What the protocol endpoint does beyond the protocol.
///
/// The endpoint's protocol behaviour is fixed — parse and validate at the
/// edge, refuse `ui.*`, render `Unavailable` as `503`, answer the server's
/// envelope with its `head.status` — and these are the parts a host decides.
/// This gateway runs with [`ProtocolOptions::gateway`]; a process that serves
/// the endpoint behind authentication of its own wants
/// [`ProtocolOptions::embedded`].
#[derive(Clone, Default)]
pub struct ProtocolOptions {
    /// Copy a `Bearer` `Authorization` header into a request whose envelope
    /// left `head.auth` empty.
    ///
    /// Right when the header carries a Resonate token. Wrong when it carries
    /// the host's own credential (an API key, a session), which the host has
    /// already checked and which is no token a Resonate server should see.
    pub forward_authorization: bool,
    /// Count and time requests in `resonate_request_total` and
    /// `resonate_request_duration_seconds`, in prometheus' default registry.
    pub metrics: bool,
    /// Verify `head.auth` before the server sees the request. `None` admits
    /// every request — for a host that authenticated it already.
    pub auth: Option<AuthMode>,
}

impl std::fmt::Debug for ProtocolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolOptions")
            .field("forward_authorization", &self.forward_authorization)
            .field("metrics", &self.metrics)
            .field("auth", &self.auth.as_ref().map(|_| ".."))
            .finish()
    }
}

impl ProtocolOptions {
    /// What this gateway does: forward the header, record metrics, and verify
    /// with `auth` when there is a policy.
    pub fn gateway(auth: Option<AuthMode>) -> Self {
        Self {
            forward_authorization: true,
            metrics: true,
            auth,
        }
    }

    /// For a host that authenticates callers itself: nothing forwarded, no
    /// metrics of this crate's, no Resonate token check.
    pub fn embedded() -> Self {
        Self::default()
    }
}

/// The envelope the protocol endpoint answers a `POST` of `body` with.
///
/// The whole of the endpoint short of HTTP: what this gateway's `POST /`
/// answers, for the same bytes and the same options, is this envelope with its
/// `head.status` as the HTTP status ([`handle_envelope`]). For a host whose
/// HTTP is not axum, or that wants the envelope before it renders it.
///
/// `headers` is read only for `Authorization`, and only when
/// [`ProtocolOptions::forward_authorization`] is set.
pub async fn answer_envelope(
    server: &dyn ResonateServer,
    headers: &axum::http::HeaderMap,
    body: &[u8],
    options: &ProtocolOptions,
) -> ResponseEnvelope {
    let start = std::time::Instant::now();

    // Parse and validate at the edge. A body that is not a request never
    // reaches the server: `core` decides what the protocol admits and how the
    // rejection reads, and this renders it — which is the only part that is
    // HTTP's. `salvage_context` digs out what it can from bytes that would not
    // parse, so even that answer can be correlated.
    let mut req: RequestEnvelope = match types::parse_and_validate(body) {
        Ok(req) => req,
        Err(invalid) => {
            let (kind, corr_id) = types::salvage_context(body);
            tracing::warn!(kind = %kind, corr_id = %corr_id, reason = %invalid, "Invalid request");
            return invalid.to_response(kind, corr_id);
        }
    };

    // Where the token came from is HTTP's business; that there is one is the
    // protocol's. The envelope carries it, so the header only fills the field
    // when the envelope left it empty — a caller holding a bearer token and
    // nothing else. Nothing downstream can tell the two apart, which is the
    // point: `auth_check` still reads one field.
    if options.forward_authorization && req.head.auth.is_none() {
        if let Some(token) = bearer_token(headers) {
            req.head.auth = Some(token.to_string());
        }
    }

    let kind = req.kind.clone();
    let corr_id = req.head.corr_id.clone();

    // The console's namespace is not served here.
    //
    // `ui.*` is a read model shaped for one screen at a time, answered on the
    // console's own route (`resonate_gateway_web`). Refusing it at the worker
    // endpoint is what keeps the two apart: an SDK cannot come to depend on a
    // request that exists to draw a table, and the read model can change
    // without touching the protocol workers speak. The engines would answer it
    // — this is the boundary, not a missing implementation, so it says so.
    if ui::is_ui_kind(&kind) {
        tracing::warn!(kind = %kind, corr_id = %corr_id, "Console request refused on the worker endpoint");
        return ResponseEnvelope::error(
            kind,
            corr_id,
            404,
            "Console requests ('ui.*') are served on the console's own endpoint, not here",
        );
    }

    // Log incoming request at the application protocol level
    tracing::info!(
        kind = %kind,
        corr_id = %corr_id,
        "Received request"
    );

    if let Some(auth) = &options.auth {
        if let Err(err_response) = auth.check_envelope(&req).await {
            let status = err_response.head.status.to_string();
            let elapsed_ms = start.elapsed().as_millis();
            tracing::warn!(
                kind = %kind,
                corr_id = %corr_id,
                status = %status,
                elapsed_ms = elapsed_ms,
                "Request rejected by auth"
            );
            if options.metrics {
                REQUEST_TOTAL.with_label_values(&[&kind, &status]).inc();
                REQUEST_DURATION
                    .with_label_values(&[&kind])
                    .observe(start.elapsed().as_secs_f64());
            }
            return *err_response;
        }
    }

    let response = match server.process(&req).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!(kind = %kind, corr_id = %corr_id, error = %e, "Server unavailable");
            ResponseEnvelope::error(kind.clone(), corr_id.clone(), 503, &e.to_string())
        }
    };
    let status = response.head.status.to_string();
    let elapsed_ms = start.elapsed().as_millis();

    // Log response outcome — level depends on status
    if response.head.status >= 500 {
        tracing::error!(
            kind = %kind,
            corr_id = %corr_id,
            status = response.head.status,
            elapsed_ms = elapsed_ms,
            "Request failed with internal error"
        );
    } else if response.head.status >= 400 {
        tracing::warn!(
            kind = %kind,
            corr_id = %corr_id,
            status = response.head.status,
            elapsed_ms = elapsed_ms,
            "Request rejected"
        );
    } else {
        tracing::info!(
            kind = %kind,
            corr_id = %corr_id,
            status = response.head.status,
            elapsed_ms = elapsed_ms,
            "Request completed"
        );
    }

    if options.metrics {
        REQUEST_TOTAL.with_label_values(&[&kind, &status]).inc();
        REQUEST_DURATION
            .with_label_values(&[&kind])
            .observe(start.elapsed().as_secs_f64());
    }
    response
}

/// The protocol endpoint's HTTP answer to a `POST` of `body`: the envelope
/// [`answer_envelope`] gives, with its `head.status` as the HTTP status.
///
/// This gateway's own `POST /` is this function with
/// [`ProtocolOptions::gateway`], so an embedder that calls it answers exactly
/// what the gateway would for the same bytes and options.
pub async fn handle_envelope(
    server: &dyn ResonateServer,
    headers: &axum::http::HeaderMap,
    body: &[u8],
    options: &ProtocolOptions,
) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    into_response(answer_envelope(server, headers, body, options).await)
}

/// The protocol endpoint as a router, with the server chosen per request.
///
/// `POST /` of one envelope, answered by [`handle_envelope`] against whatever
/// server `resolve` picks from the request's head — by host, by a path
/// parameter's value an outer layer put in an extension, by the tenant an
/// authentication middleware attached. Mount it where the host wants it:
///
/// ```ignore
/// let app = axum::Router::new().nest(
///     "/durable",
///     protocol_router(
///         |parts: &Parts| {
///             let tenant = parts.extensions.get::<Tenant>()?;
///             servers.get(tenant)
///         },
///         ProtocolOptions::embedded(),
///     ),
/// );
/// ```
///
/// Nested at `/durable`, the endpoint is `POST /durable` (axum 0.7 does not
/// also route `/durable/`), and that is the URL a client is given.
///
/// `resolve` is synchronous: a lookup that has to wait belongs in a
/// middleware that leaves its answer in an extension for `resolve` to read.
/// A request it resolves to nothing is answered `404` with whatever `kind`
/// and `corrId` could be read from the body, before the body is parsed any
/// further.
///
/// No `/ready` and no legacy paths: those are this gateway's, and a host has
/// its own probes.
pub fn protocol_router<F>(resolve: F, options: ProtocolOptions) -> Router
where
    F: Fn(&axum::http::request::Parts) -> Option<Arc<dyn ResonateServer>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let options = Arc::new(options);
    Router::new().route(
        "/",
        post(
            move |parts: axum::http::request::Parts, body: axum::body::Bytes| {
                let resolve = resolve.clone();
                let options = Arc::clone(&options);
                async move {
                    let Some(server) = resolve(&parts) else {
                        let (kind, corr_id) = types::salvage_context(&body);
                        tracing::warn!(kind = %kind, corr_id = %corr_id, "No server for this request");
                        return into_response(ResponseEnvelope::error(
                            kind,
                            corr_id,
                            404,
                            "No Resonate server serves this request",
                        ));
                    };
                    handle_envelope(server.as_ref(), &parts.headers, &body, &options).await
                }
            },
        ),
    )
}
