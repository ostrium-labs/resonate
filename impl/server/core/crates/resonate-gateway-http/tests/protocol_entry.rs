//! The public protocol entry: the gateway's `POST /` answers what it answered
//! before the entry was made public, byte for byte, and an embedder can mount
//! it per tenant with its own options.

use std::collections::HashMap;
use std::sync::Arc;

use resonate_auth::{AuthConfig, AuthMode};
use resonate_core::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_core::{ui, ResonateServer, Unavailable};
use resonate_gateway_http::routes::{
    api_routes, ApiState, AppState, REQUEST_DURATION, REQUEST_TOTAL,
};
use resonate_gateway_http::{answer_envelope, protocol_router, ProtocolOptions};
use resonate_plugin::axum;
use resonate_plugin::axum::body::Body;
use resonate_plugin::axum::extract::State;
use resonate_plugin::axum::http::{HeaderMap, Request, StatusCode};
use resonate_plugin::axum::routing::post;
use resonate_plugin::axum::{Json, Router};
use serde_json::{json, Value};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// The handler as it was
// ---------------------------------------------------------------------------

// `handle_api` and its two helpers, verbatim from `routes.rs` before the
// protocol entry was extracted (ostrium-labs/resonate loams/main f419b8be).
// The comparison below is only worth something if this stays untouched.

fn legacy_into_response(resp: ResponseEnvelope) -> (StatusCode, Json<ResponseEnvelope>) {
    let code = axum::http::StatusCode::from_u16(resp.head.status as u16)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    (code, Json(resp))
}

fn legacy_bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

async fn legacy_handle_api(
    State(api_state): State<ApiState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let server = &api_state.server;
    let start = std::time::Instant::now();

    let mut req: RequestEnvelope = match types::parse_and_validate(&body) {
        Ok(req) => req,
        Err(invalid) => {
            let (kind, corr_id) = types::salvage_context(&body);
            tracing::warn!(kind = %kind, corr_id = %corr_id, reason = %invalid, "Invalid request");
            return legacy_into_response(invalid.to_response(kind, corr_id));
        }
    };

    if req.head.auth.is_none() {
        if let Some(token) = legacy_bearer_token(&headers) {
            req.head.auth = Some(token.to_string());
        }
    }

    let kind = req.kind.clone();
    let corr_id = req.head.corr_id.clone();

    if ui::is_ui_kind(&kind) {
        tracing::warn!(kind = %kind, corr_id = %corr_id, "Console request refused on the worker endpoint");
        return legacy_into_response(ResponseEnvelope::error(
            kind,
            corr_id,
            404,
            "Console requests ('ui.*') are served on the console's own endpoint, not here",
        ));
    }

    tracing::info!(
        kind = %kind,
        corr_id = %corr_id,
        "Received request"
    );

    if let Some(auth) = &api_state.auth {
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
            REQUEST_TOTAL.with_label_values(&[&kind, &status]).inc();
            REQUEST_DURATION
                .with_label_values(&[&kind])
                .observe(start.elapsed().as_secs_f64());
            return legacy_into_response(*err_response);
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

    REQUEST_TOTAL.with_label_values(&[&kind, &status]).inc();
    REQUEST_DURATION
        .with_label_values(&[&kind])
        .observe(start.elapsed().as_secs_f64());
    legacy_into_response(response)
}

fn legacy_routes() -> Router<AppState> {
    Router::new().route("/", post(legacy_handle_api))
}

// ---------------------------------------------------------------------------
// A server whose answer is a function of the request
// ---------------------------------------------------------------------------

/// Answers with what it was handed, so two endpoints that hand it different
/// things answer differently. `data.status` picks the status and
/// `data.unavailable` makes it unavailable.
struct Echo;

#[async_trait::async_trait]
impl ResonateServer for Echo {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        if req.data.get("unavailable").is_some() {
            return Err(Unavailable::new("the store is down"));
        }
        let status = req
            .data
            .get("status")
            .and_then(Value::as_i64)
            .unwrap_or(200) as i32;
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            status,
            json!({
                "seen": {
                    "kind": req.kind,
                    "corrId": req.head.corr_id,
                    "auth": req.head.auth,
                    "debugTime": req.head.debug_time,
                    "data": req.data,
                }
            }),
        ))
    }
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn jwt(claims: Value) -> String {
    let header = json!({ "alg": "HS256", "typ": "JWT" });
    format!(
        "{}.{}.c2ln",
        b64(header.to_string().as_bytes()),
        b64(claims.to_string().as_bytes())
    )
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn unsigned_mode() -> AuthMode {
    AuthMode::Jwt(Arc::new(AuthConfig {
        key: None,
        iss: None,
        aud: None,
    }))
}

async fn post_to(
    app: &Router,
    body: &[u8],
    authorization: Option<&str>,
) -> (StatusCode, String, Vec<u8>) {
    let mut req = Request::builder().method("POST").uri("/");
    if let Some(value) = authorization {
        req = req.header("authorization", value);
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body.to_vec())).unwrap())
        .await
        .expect("infallible");
    let status = res.status();
    let content_type = res
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, content_type, bytes)
}

fn envelope(kind: &str, head: Value, data: Value) -> Vec<u8> {
    json!({ "kind": kind, "head": head, "data": data })
        .to_string()
        .into_bytes()
}

fn head(corr_id: &str) -> Value {
    json!({ "corrId": corr_id, "version": "2026-04-01" })
}

/// Bodies that walk every branch of the handler.
fn corpus() -> Vec<(String, Vec<u8>)> {
    let admin = jwt(json!({ "exp": now_s() + 60, "role": "admin" }));
    let tenant = jwt(json!({ "exp": now_s() + 60, "prefix": "a." }));
    let expired = jwt(json!({ "exp": 1, "role": "admin" }));
    let with_auth = |token: &str| {
        let mut h = head("c-auth");
        h["auth"] = json!(token);
        h
    };
    let mut debug_head = head("c-debug");
    debug_head["resonate:debug_time"] = json!(42);
    vec![
        ("not json".into(), b"nope".to_vec()),
        (
            "no head".into(),
            br#"{"kind":"eq.nohead","data":{}}"#.to_vec(),
        ),
        (
            "salvageable".into(),
            br#"{"kind":"eq.salvage","head":{"corrId":"c9"}}"#.to_vec(),
        ),
        ("empty kind".into(), envelope("", head("c1"), json!({}))),
        (
            "data not object".into(),
            envelope("eq.data", head("c2"), json!([1])),
        ),
        (
            "bad version".into(),
            envelope(
                "eq.version",
                json!({ "corrId": "c3", "version": "1.0" }),
                json!({}),
            ),
        ),
        (
            "console kind".into(),
            envelope("ui.executions.search", head("c4"), json!({})),
        ),
        (
            "plain".into(),
            envelope("eq.plain", head("c5"), json!({ "id": "a.p" })),
        ),
        (
            "debug time".into(),
            envelope("eq.debug", debug_head, json!({ "id": "a.p" })),
        ),
        (
            "envelope admin token".into(),
            envelope("eq.admin", with_auth(&admin), json!({ "id": "x" })),
        ),
        (
            "envelope tenant token".into(),
            envelope("promise.get", with_auth(&tenant), json!({ "id": "a.p" })),
        ),
        (
            "envelope tenant token, other id".into(),
            envelope("promise.get", with_auth(&tenant), json!({ "id": "b.p" })),
        ),
        (
            "envelope expired token".into(),
            envelope("eq.expired", with_auth(&expired), json!({})),
        ),
        (
            "404".into(),
            envelope("eq.404", head("c6"), json!({ "id": "a.p", "status": 404 })),
        ),
        (
            "409".into(),
            envelope("eq.409", head("c7"), json!({ "id": "a.p", "status": 409 })),
        ),
        (
            "300".into(),
            envelope("eq.300", head("c8"), json!({ "id": "a.p", "status": 300 })),
        ),
        (
            "500".into(),
            envelope("eq.500", head("c10"), json!({ "id": "a.p", "status": 500 })),
        ),
        (
            "unavailable".into(),
            envelope(
                "eq.unavailable",
                head("c11"),
                json!({ "id": "a.p", "unavailable": true }),
            ),
        ),
    ]
}

fn headers() -> Vec<Option<String>> {
    vec![
        None,
        Some(format!(
            "Bearer {}",
            jwt(json!({ "exp": now_s() + 60, "role": "admin" }))
        )),
        Some(format!(
            "Bearer {}",
            jwt(json!({ "exp": now_s() + 60, "prefix": "b." }))
        )),
        Some("Bearer ".into()),
        Some("Basic dXNlcjpwYXNz".into()),
    ]
}

/// `resonate_request_total` for every (kind, status) in `labels`.
fn counts(labels: &[(String, String)]) -> HashMap<(String, String), u64> {
    labels
        .iter()
        .map(|(k, s)| {
            (
                (k.clone(), s.clone()),
                REQUEST_TOTAL.with_label_values(&[k, s]).get() as u64,
            )
        })
        .collect()
}

fn label_of(status: StatusCode, body: &[u8]) -> Option<(String, String)> {
    let v: Value = serde_json::from_slice(body).ok()?;
    Some((v["kind"].as_str()?.to_string(), status.as_u16().to_string()))
}

#[tokio::test]
async fn the_gateway_answers_what_it_answered_before_byte_for_byte() {
    for auth in [None, Some(unsigned_mode())] {
        let state = AppState {
            server: Arc::new(Echo),
            auth: auth.clone(),
        };
        let old: Router = legacy_routes().with_state(state.clone());
        let new: Router = api_routes().with_state(state);
        for (name, body) in corpus() {
            for header in headers() {
                let header = header.as_deref();
                let was = post_to(&old, &body, header).await;
                let label: Vec<_> = label_of(was.0, &was.2).into_iter().collect();
                let before = counts(&label);
                let again = post_to(&old, &body, header).await;
                let old_delta = counts(&label);
                let is = post_to(&new, &body, header).await;
                let new_delta = counts(&label);

                let case = format!("{name} / header {header:?} / auth {}", auth.is_some());
                assert_eq!(was, again, "the legacy handler is deterministic: {case}");
                assert_eq!(was.0, is.0, "status: {case}");
                assert_eq!(was.1, is.1, "content type: {case}");
                assert_eq!(
                    String::from_utf8_lossy(&was.2),
                    String::from_utf8_lossy(&is.2),
                    "body: {case}"
                );
                // The same metrics, too: whatever the old handler counted for
                // one request, the new one counts for one request.
                for l in &label {
                    let old_step = old_delta[l] - before[l];
                    let new_step = new_delta[l] - old_delta[l];
                    assert_eq!(old_step, new_step, "metrics: {case}");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The entry, embedded
// ---------------------------------------------------------------------------

fn seen(body: &[u8]) -> Value {
    serde_json::from_slice::<Value>(body).unwrap()["data"]["seen"].clone()
}

#[tokio::test]
async fn an_embedder_mounts_the_endpoint_per_tenant() {
    #[derive(Clone)]
    struct Tenant(&'static str);

    /// A server that says which tenant it is.
    struct Named(&'static str);
    #[async_trait::async_trait]
    impl ResonateServer for Named {
        async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
            Ok(ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                json!({ "tenant": self.0, "auth": req.head.auth }),
            ))
        }
    }

    let servers: Arc<HashMap<&'static str, Arc<dyn ResonateServer>>> = Arc::new(HashMap::from([
        ("a", Arc::new(Named("a")) as Arc<dyn ResonateServer>),
        ("b", Arc::new(Named("b")) as Arc<dyn ResonateServer>),
    ]));
    let protocol = protocol_router(
        move |parts| {
            let tenant = parts.extensions.get::<Tenant>()?;
            servers.get(tenant.0).cloned()
        },
        ProtocolOptions::embedded(),
    );
    // The host's own authentication decides the tenant; here, a header.
    let app: Router = Router::new().nest(
        "/durable",
        protocol.layer(axum::middleware::from_fn(
            |mut req: Request<Body>, next: axum::middleware::Next| async move {
                match req.headers().get("x-tenant").and_then(|v| v.to_str().ok()) {
                    Some("a") => {
                        req.extensions_mut().insert(Tenant("a"));
                    }
                    Some("b") => {
                        req.extensions_mut().insert(Tenant("b"));
                    }
                    _ => {}
                }
                next.run(req).await
            },
        )),
    );

    let call = |tenant: Option<&'static str>| {
        let app = app.clone();
        async move {
            let mut req = Request::builder()
                .method("POST")
                .uri("/durable")
                .header("authorization", "Bearer host-session-key");
            if let Some(t) = tenant {
                req = req.header("x-tenant", t);
            }
            let body = envelope("promise.get", head("c1"), json!({ "id": "p" }));
            let res = app
                .oneshot(req.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };

    let (status, a) = call(Some("a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(a["data"]["tenant"], "a");
    assert_eq!(
        a["data"]["auth"],
        Value::Null,
        "the host's credential is not forwarded into head.auth"
    );
    assert_eq!(call(Some("b")).await.1["data"]["tenant"], "b");

    let (status, none) = call(None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(none["kind"], "promise.get", "the refusal can be correlated");
    assert_eq!(none["head"]["corrId"], "c1");
}

#[tokio::test]
async fn options_choose_forwarding_metrics_and_auth() {
    let token = jwt(json!({ "exp": now_s() + 60, "role": "admin" }));
    let mut headers = HeaderMap::new();
    headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
    let body = envelope("eq.options", head("c1"), json!({ "id": "x" }));
    let label = [("eq.options".to_string(), "200".to_string())];

    // Embedded: nothing forwarded, nothing counted.
    let before = counts(&label)[&label[0]];
    let r = answer_envelope(&Echo, &headers, &body, &ProtocolOptions::embedded()).await;
    assert_eq!(r.head.status, 200);
    assert_eq!(r.data["seen"]["auth"], Value::Null);
    assert_eq!(counts(&label)[&label[0]], before, "no metrics");

    // Forwarding alone.
    let forward = ProtocolOptions {
        forward_authorization: true,
        ..ProtocolOptions::embedded()
    };
    let r = answer_envelope(&Echo, &headers, &body, &forward).await;
    assert_eq!(r.data["seen"]["auth"], json!(token));

    // Metrics alone.
    let metrics = ProtocolOptions {
        metrics: true,
        ..ProtocolOptions::embedded()
    };
    answer_envelope(&Echo, &headers, &body, &metrics).await;
    assert_eq!(counts(&label)[&label[0]], before + 1);

    // Verification without forwarding: the host's header is not a Resonate
    // token, so a request that carries none in its envelope is refused.
    let verify = ProtocolOptions {
        auth: Some(unsigned_mode()),
        ..ProtocolOptions::embedded()
    };
    let r = answer_envelope(&Echo, &headers, &body, &verify).await;
    assert_eq!(r.head.status, 401);

    // The gateway's own options are the old behaviour.
    let r = answer_envelope(
        &Echo,
        &headers,
        &body,
        &ProtocolOptions::gateway(Some(unsigned_mode())),
    )
    .await;
    assert_eq!(r.head.status, 200);
    assert_eq!(seen(&serde_json::to_vec(&r).unwrap())["auth"], json!(token));

    // Not a request at all: the same 400 whatever the options.
    let r = answer_envelope(&Echo, &headers, b"nope", &ProtocolOptions::embedded()).await;
    assert_eq!(r.head.status, 400);
    // A server that cannot answer: 503.
    let down = envelope("eq.options", head("c2"), json!({ "unavailable": true }));
    let r = answer_envelope(&Echo, &headers, &down, &ProtocolOptions::embedded()).await;
    assert_eq!(r.head.status, 503);
}
