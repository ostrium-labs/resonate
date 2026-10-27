//! The consumer side of the poll transport: `GET {endpoint}/poll/{group}/{id}`.
//!
//! A worker that cannot be dialled dials in. It opens one long-lived HTTP
//! request on the gateway's listener (`resonate-transport-http-poll`), and the
//! server writes each message routed to `poll://any@<group>` (or
//! `poll://uni@<group>/<id>`) down it as one server-sent event whose data is
//! the message's JSON.
//!
//! [`Client::poll`](crate::Client::poll) opens that request and hands back a
//! [`Subscription`]:
//!
//! - It returns once the server has answered the request's headers. The
//!   server registers the connection before it answers, so every message
//!   routed after `poll` returns reaches this subscription.
//! - Each frame becomes a [`PollEvent::Message`]: [`PollMessage::Execute`]
//!   (a task to acquire), [`PollMessage::Unblock`] (a promise a listener
//!   waited on has settled), or [`PollMessage::Unknown`] for a kind this
//!   client does not know yet. A frame that is not a message at all is
//!   skipped.
//! - A connection that ends or fails is reopened with jittered backoff, for
//!   as long as the subscription lives, and [`PollEvent::Reconnected`] is
//!   sent once it is back. **Anything routed to this connection while it was
//!   down is lost** for an `unblock` (the server sends it once) and comes
//!   back for an `execute` only with the server's redispatch of a pending
//!   task. A consumer that waits on promises re-reads them on `Reconnected`.
//! - Dropping the [`Subscription`] closes the connection, which takes it out
//!   of its group on the server.
//!
//! Auth is the client's token source, read for every connection attempt and
//! sent as `Authorization: Bearer …`: the poll route admits whom the protocol
//! endpoint admits.

use std::sync::Arc;
use std::time::{Duration, Instant};

use resonate_core::types::PromiseRecord;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::auth::TokenSource;
use crate::error::Error;
use crate::retry::{Jitter, RetryPolicy};

/// The kind every poll error carries.
const KIND: &str = "poll";

/// How a [`Subscription`] behaves. [`PollOptions::default`] suits a gateway
/// worker; the fields are public so a caller can change one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollOptions {
    /// Retries for opening the first connection, inside
    /// [`Client::poll`](crate::Client::poll). `None` uses the client's
    /// [`RetryPolicy`].
    pub open: Option<RetryPolicy>,
    /// The backoff between reconnects after the first connection. Only
    /// `base` and `cap` are read: reconnecting goes on until the
    /// subscription is dropped.
    pub reconnect: RetryPolicy,
    /// Treat a connection that has been silent this long as dead and reopen
    /// it. Only useful when the server sends keepalives
    /// (`keepalive_interval_ms`); set it to a few intervals. `None` waits
    /// for the connection to end.
    pub idle_timeout: Option<Duration>,
    /// Events buffered between the connection and [`Subscription::recv`].
    /// When the buffer is full the connection is not read until there is
    /// room, so the server's own per-connection buffer applies backpressure.
    pub buffer: usize,
}

impl Default for PollOptions {
    /// The client's retries to open; reconnects from 100 ms doubling to 5 s;
    /// no idle timeout; 1024 events buffered.
    fn default() -> Self {
        Self {
            open: None,
            reconnect: RetryPolicy {
                max_attempts: u32::MAX,
                base: Duration::from_millis(100),
                cap: Duration::from_secs(5),
                deadline: Duration::MAX,
            },
            idle_timeout: None,
            buffer: 1024,
        }
    }
}

/// What a [`Subscription`] yields.
// Unboxed: an event is moved once, from the connection to `recv`.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollEvent {
    /// One message the server routed to this connection.
    Message(PollMessage),
    /// The connection was lost and is open again. Messages routed while it
    /// was down are lost (see the module docs).
    Reconnected {
        /// Attempts it took to reopen.
        attempts: u32,
    },
}

/// One poll message, as the server's router built it.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollMessage {
    /// `execute`: the task is pending at `version`. Acquire it with that
    /// version. Sent when the task is created (or its delay ends), after a
    /// release or a lease expiry, and again on the server's redispatch, so
    /// duplicates are normal.
    Execute {
        /// The task's id, which is its promise's id.
        id: String,
        version: i64,
        /// Where the server says to call back (`head.serverUrl`).
        server_url: String,
        trace: TraceContext,
    },
    /// `unblock`: a promise this group listened on has settled. Sent once.
    Unblock {
        promise: PromiseRecord,
        trace: TraceContext,
    },
    /// A kind this client does not know: the message as it came.
    Unknown { kind: String, message: Value },
}

/// The W3C trace context a message carries in its `head`, if the work it
/// belongs to was traced.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct TraceContext {
    #[serde(default)]
    pub traceparent: Option<String>,
    #[serde(default)]
    pub tracestate: Option<String>,
}

#[derive(Deserialize)]
struct WireMessage {
    kind: String,
    #[serde(default)]
    head: Value,
    #[serde(default)]
    data: Value,
}

#[derive(Deserialize)]
struct WireExecute {
    task: WireTask,
}

#[derive(Deserialize)]
struct WireTask {
    id: String,
    version: i64,
}

#[derive(Deserialize)]
struct WireUnblock {
    promise: PromiseRecord,
}

/// One event's data as a message; `None` if it is not one (the frame is
/// skipped).
pub(crate) fn parse_message(text: &str) -> Option<PollMessage> {
    let raw: Value = serde_json::from_str(text).ok()?;
    let wire: WireMessage = serde_json::from_value(raw.clone()).ok()?;
    let trace: TraceContext = serde_json::from_value(wire.head.clone()).unwrap_or_default();
    match wire.kind.as_str() {
        "execute" => {
            let data: WireExecute = serde_json::from_value(wire.data).ok()?;
            let server_url = wire
                .head
                .get("serverUrl")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some(PollMessage::Execute {
                id: data.task.id,
                version: data.task.version,
                server_url,
                trace,
            })
        }
        "unblock" => {
            let data: WireUnblock = serde_json::from_value(wire.data).ok()?;
            Some(PollMessage::Unblock {
                promise: data.promise,
                trace,
            })
        }
        _ => Some(PollMessage::Unknown {
            kind: wire.kind,
            message: raw,
        }),
    }
}

/// Server-sent events, fed bytes as they arrive, yielding each event's data.
/// Comments (the `: ping` keepalive) and fields other than `data` are
/// ignored; a multi-line `data` is joined with `\n`, as the format says.
#[derive(Debug, Default)]
pub(crate) struct SseParser {
    pending: Vec<u8>,
    data: Vec<String>,
    has_data: bool,
}

impl SseParser {
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        self.pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        // Split on `\n`; a `\r` before it is dropped. Bytes after the last
        // `\n` wait for the next chunk, so a UTF-8 sequence split across
        // chunks is never decoded in halves.
        while let Some(nl) = self.pending.iter().position(|&b| b == b'\n') {
            let mut line: Vec<u8> = self.pending.drain(..=nl).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line);
            if line.is_empty() {
                if self.has_data {
                    out.push(self.data.join("\n"));
                }
                self.data.clear();
                self.has_data = false;
            } else if let Some(rest) = line.strip_prefix("data:") {
                self.data
                    .push(rest.strip_prefix(' ').unwrap_or(rest).to_string());
                self.has_data = true;
            } else if line == "data" {
                self.data.push(String::new());
                self.has_data = true;
            }
        }
        out
    }
}

/// The URL a consumer of `group` with connection id `id` opens: `poll`,
/// `group` and `id` appended as path segments (percent-encoded) to the
/// protocol endpoint's path.
pub(crate) fn poll_url(
    endpoint: &reqwest::Url,
    group: &str,
    id: &str,
) -> Result<reqwest::Url, Error> {
    let mut url = endpoint.clone();
    url.set_query(None);
    url.set_fragment(None);
    url.path_segments_mut()
        .map_err(|()| Error::Build(format!("{endpoint} cannot take a path")))?
        .pop_if_empty()
        .push("poll")
        .push(group)
        .push(id);
    Ok(url)
}

/// What opening a connection needs; cloned into the background task.
#[derive(Clone)]
pub(crate) struct Dialer {
    pub http: reqwest::Client,
    pub url: reqwest::Url,
    pub auth: Option<Arc<dyn TokenSource>>,
}

/// Why one attempt to open failed.
enum OpenFailure {
    /// Worth another try: no connection, a timeout, `5xx`.
    Transient(String),
    /// An answer: `401`, `403`, `404`, any other `4xx`.
    Rejected { status: u16, message: String },
    /// The token source failed.
    Auth(String),
}

impl Dialer {
    async fn open(&self) -> Result<reqwest::Response, OpenFailure> {
        let mut request = self
            .http
            .get(self.url.clone())
            .header(reqwest::header::ACCEPT, "text/event-stream");
        if let Some(source) = &self.auth {
            if let Some(token) = source.token().await.map_err(OpenFailure::Auth)? {
                request = request.bearer_auth(token);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|e| OpenFailure::Transient(e.to_string()))?;
        let status = response.status().as_u16();
        if response.status().is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        let snippet: String = body.chars().take(200).collect();
        let message = format!("HTTP {status}: {snippet}");
        if status >= 500 || status == 429 || status == 408 {
            Err(OpenFailure::Transient(message))
        } else {
            Err(OpenFailure::Rejected { status, message })
        }
    }

    /// Open, retrying transient failures within `policy`.
    pub(crate) async fn open_within(
        &self,
        policy: &RetryPolicy,
        jitter: &Jitter,
    ) -> Result<reqwest::Response, Error> {
        let started = Instant::now();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let last = match self.open().await {
                Ok(r) => return Ok(r),
                Err(OpenFailure::Auth(m)) => return Err(Error::Auth(m)),
                Err(OpenFailure::Rejected { status, message }) => {
                    return Err(Error::Rejected {
                        kind: KIND.to_string(),
                        corr_id: String::new(),
                        status,
                        message,
                        attempts: attempt,
                    })
                }
                Err(OpenFailure::Transient(m)) => m,
            };
            let wait = jitter.backoff(policy, attempt);
            if attempt >= policy.max_attempts || started.elapsed() + wait >= policy.deadline {
                return Err(Error::Unavailable {
                    kind: KIND.to_string(),
                    corr_id: String::new(),
                    attempts: attempt,
                    message: last,
                });
            }
            tokio::time::sleep(wait).await;
        }
    }
}

/// An open poll connection: [`recv`](Self::recv) its events; drop it to
/// close the connection.
#[derive(Debug)]
pub struct Subscription {
    rx: mpsc::Receiver<PollEvent>,
    task: JoinHandle<()>,
    group: String,
    id: String,
}

impl Subscription {
    /// The next event. `None` only after [`close`](Self::close).
    pub async fn recv(&mut self) -> Option<PollEvent> {
        self.rx.recv().await
    }

    /// The group this connection consumes.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// This connection's id within its group.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Close the connection now; [`recv`](Self::recv) drains what was
    /// already received, then answers `None`.
    pub fn close(&mut self) {
        self.task.abort();
        self.rx.close();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Dropping the response body closes the connection, and that is
        // what takes it out of its group on the server.
        self.task.abort();
    }
}

/// Open a subscription: the first connection within the open policy, then
/// a task that reads it and reopens it.
pub(crate) async fn subscribe(
    dialer: Dialer,
    group: &str,
    id: &str,
    open_policy: RetryPolicy,
    options: PollOptions,
) -> Result<Subscription, Error> {
    if group.is_empty() || id.is_empty() {
        return Err(Error::Build("poll group and id must not be empty".into()));
    }
    let jitter = Jitter::new();
    let first = dialer.open_within(&open_policy, &jitter).await?;
    let (tx, rx) = mpsc::channel(options.buffer.max(1));
    let task = tokio::spawn(run(dialer, first, tx, options, jitter));
    Ok(Subscription {
        rx,
        task,
        group: group.to_string(),
        id: id.to_string(),
    })
}

/// Read connections until the subscription goes.
async fn run(
    dialer: Dialer,
    mut response: reqwest::Response,
    tx: mpsc::Sender<PollEvent>,
    options: PollOptions,
    jitter: Jitter,
) {
    loop {
        if !pump(&mut response, &tx, options.idle_timeout).await {
            return;
        }
        let mut attempts: u32 = 0;
        response = loop {
            attempts += 1;
            let wait = jitter.backoff(&options.reconnect, attempts.min(32));
            tokio::select! {
                _ = tx.closed() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            // A rejection (a token that expired, say) is retried like any
            // failure: the token source is read again on the next attempt.
            if let Ok(r) = dialer.open().await {
                break r;
            }
        };
        if tx.send(PollEvent::Reconnected { attempts }).await.is_err() {
            return;
        }
    }
}

/// Read one connection into `tx` until it ends, fails or goes idle.
/// `false` when the receiver is gone.
async fn pump(
    response: &mut reqwest::Response,
    tx: &mpsc::Sender<PollEvent>,
    idle_timeout: Option<Duration>,
) -> bool {
    let mut sse = SseParser::default();
    loop {
        let next = async {
            match idle_timeout {
                Some(idle) => tokio::time::timeout(idle, response.chunk())
                    .await
                    .unwrap_or(Ok(None)),
                None => response.chunk().await,
            }
        };
        let chunk = tokio::select! {
            _ = tx.closed() => return false,
            c = next => c,
        };
        match chunk {
            Ok(Some(bytes)) => {
                for data in sse.feed(&bytes) {
                    if let Some(message) = parse_message(&data) {
                        if tx.send(PollEvent::Message(message)).await.is_err() {
                            return false;
                        }
                    }
                }
            }
            // Ended, failed, or idle past the timeout: reopen.
            Ok(None) | Err(_) => return true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::{
        ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, PromiseState,
        PromiseValue, UnblockMsg, UnblockMsgData, UnblockMsgHead,
    };

    #[test]
    fn events_split_across_chunks_and_lines() {
        let mut p = SseParser::default();
        assert!(p.feed(b": ping\n\ndata: {\"a\"").is_empty());
        assert_eq!(p.feed(b":1}\r\n\r\ndata: x\n\n"), vec!["{\"a\":1}", "x"]);
        assert_eq!(p.feed(b"data: one\ndata: two\n\n"), vec!["one\ntwo"]);
        // A comment alone is no event.
        assert!(p.feed(b": ping\n\n").is_empty());
    }

    #[test]
    fn a_utf8_sequence_split_across_chunks_survives() {
        let mut p = SseParser::default();
        let bytes = "data: é\n\n".as_bytes();
        // Split inside the two-byte é.
        assert!(p.feed(&bytes[..7]).is_empty());
        assert_eq!(p.feed(&bytes[7..]), vec!["é"]);
    }

    fn record(id: &str) -> PromiseRecord {
        PromiseRecord {
            id: id.to_string(),
            state: PromiseState::Resolved,
            param: PromiseValue::default(),
            value: PromiseValue::default(),
            tags: [("resonate:timer".to_string(), "true".to_string())].into(),
            timeout_at: 2,
            created_at: 1,
            settled_at: Some(2),
        }
    }

    /// The bytes the poll transport writes: it serializes `Message` through
    /// a `Value`, so keys come out sorted.
    fn as_transport_writes(msg: &Message) -> String {
        serde_json::to_string(&serde_json::to_value(msg).unwrap()).unwrap()
    }

    #[test]
    fn execute_as_the_server_writes_it() {
        let msg = Message::Execute(ExecuteMsg {
            kind: "execute".into(),
            head: MessageHead::new("http://localhost:8001"),
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: "n.w.r:wft.1".into(),
                    version: 3,
                },
            },
        });
        assert_eq!(
            parse_message(&as_transport_writes(&msg)),
            Some(PollMessage::Execute {
                id: "n.w.r:wft.1".into(),
                version: 3,
                server_url: "http://localhost:8001".into(),
                trace: TraceContext::default(),
            })
        );
    }

    #[test]
    fn unblock_with_a_trace_as_the_server_writes_it() {
        let msg = Message::Unblock(UnblockMsg {
            kind: "unblock".into(),
            head: UnblockMsgHead {
                traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
                tracestate: None,
            },
            data: UnblockMsgData {
                promise: record("tmr:a"),
            },
        });
        match parse_message(&as_transport_writes(&msg)) {
            Some(PollMessage::Unblock { promise, trace }) => {
                assert_eq!(promise, record("tmr:a"));
                assert!(trace.traceparent.is_some());
                assert_eq!(trace.tracestate, None);
            }
            other => panic!("expected unblock, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_kind_is_handed_over_and_garbage_is_skipped() {
        match parse_message(r#"{"kind":"resume","head":{},"data":{"x":1}}"#) {
            Some(PollMessage::Unknown { kind, message }) => {
                assert_eq!(kind, "resume");
                assert_eq!(message["data"]["x"], 1);
            }
            other => panic!("expected unknown, got {other:?}"),
        }
        assert_eq!(parse_message("ping"), None);
        assert_eq!(parse_message(r#"{"kind":"execute","data":{}}"#), None);
    }

    #[test]
    fn poll_urls_sit_beside_the_protocol_endpoint() {
        let at = |s: &str| reqwest::Url::parse(s).unwrap();
        assert_eq!(
            poll_url(&at("http://h:8001/"), "workers", "p1")
                .unwrap()
                .as_str(),
            "http://h:8001/poll/workers/p1"
        );
        assert_eq!(
            poll_url(&at("https://h/durable"), "g", "p")
                .unwrap()
                .as_str(),
            "https://h/durable/poll/g/p"
        );
        assert_eq!(
            poll_url(&at("https://h/durable/?x=1"), "a b", "p/q")
                .unwrap()
                .as_str(),
            "https://h/durable/poll/a%20b/p%2Fq"
        );
    }
}
