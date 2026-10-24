//! The two ways a request reaches a server: HTTP to a gateway, or a call on
//! an `Arc<dyn ResonateServer>` in this process.
//!
//! Both take the same [`Outgoing`] and give back the same [`Raw`] answer, so
//! retries, auth, correlation and observation sit above them once.

use std::sync::Arc;
use std::time::Duration;

use resonate_core::types::{RequestEnvelope, RequestHead, PROTOCOL_VERSION};
use resonate_core::ResonateServer;
use serde::Deserialize;
use serde_json::{json, Map, Value};

/// A request as the client sends it, before transport.
#[derive(Debug, Clone)]
pub(crate) struct Outgoing {
    pub kind: String,
    pub corr_id: String,
    /// `head.auth` as the caller set it. The token source's token is added by
    /// the transport, per attempt.
    pub auth: Option<String>,
    pub debug_time: Option<i64>,
    pub data: Value,
}

/// A response envelope, whatever transport carried it.
#[derive(Debug, Clone)]
pub(crate) struct Raw {
    pub kind: String,
    pub corr_id: String,
    pub status: u16,
    pub version: String,
    pub data: Value,
}

impl Raw {
    /// The envelope as JSON, the shape a trace line records.
    pub(crate) fn envelope(&self) -> Value {
        json!({
            "kind": self.kind,
            "head": { "corrId": self.corr_id, "status": self.status, "version": self.version },
            "data": self.data,
        })
    }
}

/// Why an attempt produced no envelope.
#[derive(Debug)]
pub(crate) enum Failure {
    /// Worth retrying: no connection, a timeout, a proxy's 502/503/504, or a
    /// server that returned `Unavailable`.
    Transient(String),
    /// Not worth retrying: the peer answered with something that is not a
    /// protocol response.
    Protocol(String),
}

#[derive(Clone)]
pub(crate) enum Transport {
    Http(Http),
    Local(Arc<dyn ResonateServer>),
}

impl Transport {
    /// One attempt.
    pub(crate) async fn send(
        &self,
        out: &Outgoing,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<Raw, Failure> {
        match self {
            Transport::Http(http) => http.send(out, token, timeout).await,
            Transport::Local(server) => local_send(server.as_ref(), out, token, timeout).await,
        }
    }

    pub(crate) async fn ready(&self, timeout: Duration) -> bool {
        match self {
            Transport::Http(http) => http.ready(timeout).await,
            Transport::Local(server) => server.ready().await,
        }
    }
}

// --- In process ---

async fn local_send(
    server: &dyn ResonateServer,
    out: &Outgoing,
    token: Option<String>,
    timeout: Duration,
) -> Result<Raw, Failure> {
    // An envelope the caller set keeps its token; the source's fills an empty
    // one. That is the HTTP gateway's precedence between the envelope and the
    // header, so a request answers the same in process as it would over HTTP.
    let req = RequestEnvelope {
        kind: out.kind.clone(),
        head: RequestHead {
            corr_id: out.corr_id.clone(),
            version: PROTOCOL_VERSION.to_string(),
            auth: out.auth.clone().or(token),
            debug_time: out.debug_time,
        },
        data: out.data.clone(),
    };
    let resp = match tokio::time::timeout(timeout, server.process(&req)).await {
        Err(_) => return Err(Failure::Transient(format!("timed out after {timeout:?}"))),
        Ok(Err(unavailable)) => return Err(Failure::Transient(unavailable.to_string())),
        Ok(Ok(resp)) => resp,
    };
    let status = u16::try_from(resp.head.status)
        .map_err(|_| Failure::Protocol(format!("status {} is not a status", resp.head.status)))?;
    if status == 503 {
        return Err(Failure::Transient(describe_503(&resp.data)));
    }
    Ok(Raw {
        kind: resp.kind,
        corr_id: resp.head.corr_id,
        status,
        version: resp.head.version,
        data: resp.data,
    })
}

// --- HTTP ---

#[derive(Clone)]
pub(crate) struct Http {
    pub client: reqwest::Client,
    /// Where envelopes are POSTed.
    pub url: reqwest::Url,
    /// The gateway's readiness probe, beside it.
    pub ready_url: reqwest::Url,
}

#[derive(Deserialize)]
struct WireResponse {
    kind: String,
    head: WireHead,
    data: Value,
}

#[derive(Deserialize)]
struct WireHead {
    #[serde(rename = "corrId")]
    corr_id: String,
    status: i64,
    #[serde(default)]
    version: String,
}

impl Http {
    pub(crate) fn new(client: reqwest::Client, url: reqwest::Url) -> Self {
        // `/ready` sits beside `/` on the gateway, and beside wherever an
        // embedder mounted it.
        let mut base = url.clone();
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let ready_url = base.join("ready").unwrap_or_else(|_| base.clone());
        Self {
            client,
            url,
            ready_url,
        }
    }

    async fn send(
        &self,
        out: &Outgoing,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<Raw, Failure> {
        let mut head = Map::new();
        head.insert("corrId".into(), Value::String(out.corr_id.clone()));
        head.insert("version".into(), Value::String(PROTOCOL_VERSION.into()));
        if let Some(auth) = &out.auth {
            head.insert("auth".into(), Value::String(auth.clone()));
        }
        if let Some(t) = out.debug_time {
            head.insert("resonate:debug_time".into(), Value::from(t));
        }
        let body = json!({ "kind": out.kind, "head": head, "data": out.data });

        let mut request = self
            .client
            .post(self.url.clone())
            .timeout(timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string());
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(transient)?;
        let http_status = response.status().as_u16();
        let bytes = response.bytes().await.map_err(transient)?;

        let Ok(wire) = serde_json::from_slice::<WireResponse>(&bytes) else {
            // No envelope. From a proxy in front of the gateway this is a
            // gateway-level failure and worth another try; from anything else
            // it is not a Resonate endpoint.
            let snippet = String::from_utf8_lossy(&bytes[..bytes.len().min(200)]).into_owned();
            return Err(if matches!(http_status, 502..=504) {
                Failure::Transient(format!("HTTP {http_status}: {snippet}"))
            } else {
                Failure::Protocol(format!(
                    "HTTP {http_status} without a response envelope: {snippet}"
                ))
            });
        };
        let status = u16::try_from(wire.head.status).map_err(|_| {
            Failure::Protocol(format!("status {} is not a status", wire.head.status))
        })?;
        if status == 503 || http_status == 503 {
            return Err(Failure::Transient(describe_503(&wire.data)));
        }
        Ok(Raw {
            kind: wire.kind,
            corr_id: wire.head.corr_id,
            status,
            version: wire.head.version,
            data: wire.data,
        })
    }

    async fn ready(&self, timeout: Duration) -> bool {
        match self
            .client
            .get(self.ready_url.clone())
            .timeout(timeout)
            .send()
            .await
        {
            Ok(r) => r.status().is_success(),
            Err(_) => false,
        }
    }
}

fn transient(e: reqwest::Error) -> Failure {
    if e.is_builder() {
        // A request that could not be built (a token that is not a valid
        // header value, say) fails the same way on every attempt.
        return Failure::Protocol(e.to_string());
    }
    // Whatever else kept the request from completing — refused, reset, timed
    // out, a body cut short — is "no answer", which is what a retry is for.
    Failure::Transient(e.to_string())
}

fn describe_503(data: &Value) -> String {
    match data.as_str() {
        Some(m) => format!("503: {m}"),
        None => format!("503: {data}"),
    }
}
