//! The client, its builder, and the one loop every call goes through.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};
use serde_json::Value;

use crate::auth::TokenSource;
use crate::corr::{CorrIds, Sequential};
use crate::error::Error;
use crate::retry::{Jitter, RetryPolicy};
use crate::trace::{Exchange, Observer};
use crate::transport::{Failure, Http, Outgoing, Raw, Transport};

/// A Resonate protocol client.
///
/// One type over two transports: [`Client::http`] talks to a gateway's
/// protocol endpoint, and [`Client::local`] calls an `Arc<dyn ResonateServer>`
/// in this process. Everything above the transport — typed methods, retries,
/// the auth hook, correlation ids, observation — is the same for both, so
/// code written against a `Client` does not know which it has.
///
/// Cheap to clone; clones share the connection pool and the id generator.
///
/// `Client` is itself a [`ResonateServer`], so it plugs in anywhere the port
/// is taken: put one behind a gateway and that gateway fronts a remote server.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    transport: Transport,
    auth: Option<Arc<dyn TokenSource>>,
    corr_ids: Arc<dyn CorrIds>,
    retry: RetryPolicy,
    jitter: Jitter,
    timeout: Duration,
    debug_clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    observer: Option<Arc<dyn Observer>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let transport = match &self.inner.transport {
            Transport::Http(h) => format!("http({})", h.url),
            Transport::Local(_) => "local".to_string(),
        };
        f.debug_struct("Client")
            .field("transport", &transport)
            .field("retry", &self.inner.retry)
            .field("timeout", &self.inner.timeout)
            .finish_non_exhaustive()
    }
}

enum Target {
    Http {
        url: String,
        client: Option<reqwest::Client>,
        connect_timeout: Duration,
    },
    Local(Arc<dyn ResonateServer>),
}

/// Configures a [`Client`].
pub struct Builder {
    target: Target,
    auth: Option<Arc<dyn TokenSource>>,
    corr_ids: Option<Arc<dyn CorrIds>>,
    retry: RetryPolicy,
    timeout: Duration,
    debug_clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    observer: Option<Arc<dyn Observer>>,
}

impl Client {
    /// A client for the protocol endpoint at `url`: the gateway's `POST /`
    /// (`http://host:8001/`), or wherever an embedder mounted it
    /// (`https://host/durable`). Envelopes are POSTed to `url` exactly.
    pub fn http(url: impl Into<String>) -> Builder {
        Builder::new(Target::Http {
            url: url.into(),
            client: None,
            connect_timeout: Duration::from_secs(5),
        })
    }

    /// A client that calls `server` in this process.
    ///
    /// The server is called as it is: the client does not `init` or `stop`
    /// it, because whoever built it owns its lifecycle.
    pub fn local(server: Arc<dyn ResonateServer>) -> Builder {
        Builder::new(Target::Local(server))
    }

    /// Send one request of any kind and return its envelope, whatever the
    /// status. The typed methods are built on this; it is here for kinds they
    /// do not cover.
    ///
    /// Only "no answer" is an error: a `404` comes back as a [`Response`]
    /// with that status and the server's message as `data`. The typed
    /// methods are the ones that check the status.
    ///
    /// [`Response`]: crate::Response
    pub async fn send(
        &self,
        kind: &str,
        data: Value,
    ) -> Result<crate::model::Response<Value>, Error> {
        let (raw, attempts) = self.exchange(self.outgoing(kind, data)).await?;
        Ok(crate::model::Response {
            status: raw.status,
            corr_id: raw.corr_id,
            data: raw.data,
            attempts,
        })
    }

    /// A request with a fresh correlation id and the debug clock's instant.
    pub(crate) fn outgoing(&self, kind: &str, data: Value) -> Outgoing {
        Outgoing {
            kind: kind.to_string(),
            corr_id: self.inner.corr_ids.next_id(kind),
            auth: None,
            debug_time: self.inner.debug_clock.as_ref().map(|c| c()),
            data,
        }
    }

    /// Send, retrying what got no answer, and check that the answer is to
    /// this request.
    pub(crate) async fn exchange(&self, out: Outgoing) -> Result<(Raw, u32), Error> {
        let inner = &self.inner;
        let started = Instant::now();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let token = match &inner.auth {
                Some(source) => source.token().await.map_err(Error::Auth)?,
                None => None,
            };
            // Never let one attempt outlive the call's deadline.
            let remaining = inner.retry.deadline.saturating_sub(started.elapsed());
            let timeout = inner.timeout.min(remaining).max(Duration::from_millis(1));

            let last = match inner.transport.send(&out, token, timeout).await {
                Ok(raw) => {
                    if raw.kind != out.kind || raw.corr_id != out.corr_id {
                        return Err(Error::Protocol {
                            kind: out.kind.clone(),
                            message: format!(
                                "answer is for {} ({}), not {} ({})",
                                raw.kind, raw.corr_id, out.kind, out.corr_id
                            ),
                        });
                    }
                    if let Some(observer) = &inner.observer {
                        let now = out.debug_time.unwrap_or_else(wall_ms);
                        observer.observe(&Exchange {
                            kind: &out.kind,
                            now,
                            request: &out.data,
                            response: &raw.envelope(),
                        });
                    }
                    return Ok((raw, attempt));
                }
                Err(Failure::Protocol(message)) => {
                    return Err(Error::Protocol {
                        kind: out.kind.clone(),
                        message,
                    })
                }
                Err(Failure::Transient(message)) => message,
            };

            let wait = inner.jitter.backoff(&inner.retry, attempt);
            if attempt >= inner.retry.max_attempts
                || started.elapsed() + wait >= inner.retry.deadline
            {
                return Err(Error::Unavailable {
                    kind: out.kind.clone(),
                    corr_id: out.corr_id.clone(),
                    attempts: attempt,
                    message: last,
                });
            }
            tokio::time::sleep(wait).await;
        }
    }
}

impl Client {
    /// Consume `poll://` messages for `group` as connection `id`: open
    /// `GET {endpoint}/poll/{group}/{id}` on the gateway and return once the
    /// server has registered it. See [`crate::poll`] for what the
    /// [`Subscription`](crate::Subscription) yields and how it reconnects.
    ///
    /// Opening is retried within the client's [`RetryPolicy`]; a `4xx` (`401`
    /// for a token the gateway refuses) is [`Error::Rejected`] with kind
    /// `poll`. HTTP only: a client over [`Client::local`] answers
    /// [`Error::Unsupported`], because a poll connection lives on a gateway.
    pub async fn poll(&self, group: &str, id: &str) -> Result<crate::Subscription, Error> {
        self.poll_with(group, id, crate::PollOptions::default())
            .await
    }

    /// [`Client::poll`] with [`PollOptions`](crate::PollOptions).
    pub async fn poll_with(
        &self,
        group: &str,
        id: &str,
        options: crate::PollOptions,
    ) -> Result<crate::Subscription, Error> {
        let Transport::Http(http) = &self.inner.transport else {
            return Err(Error::Unsupported(
                "poll needs an HTTP client: poll connections are served by a gateway".into(),
            ));
        };
        let dialer = crate::poll::Dialer {
            http: http.client.clone(),
            url: crate::poll::poll_url(&http.url, group, id)?,
            auth: self.inner.auth.clone(),
        };
        let open = options.open.unwrap_or(self.inner.retry);
        crate::poll::subscribe(dialer, group, id, open, options).await
    }
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

impl Builder {
    fn new(target: Target) -> Self {
        Self {
            target,
            auth: None,
            corr_ids: None,
            retry: RetryPolicy::default(),
            timeout: Duration::from_secs(10),
            debug_clock: None,
            observer: None,
        }
    }

    /// Where each request's token comes from (see [`TokenSource`]).
    pub fn auth(mut self, source: impl TokenSource + 'static) -> Self {
        self.auth = Some(Arc::new(source));
        self
    }

    /// How correlation ids are made. Defaults to [`Sequential`].
    pub fn corr_ids(mut self, ids: impl CorrIds + 'static) -> Self {
        self.corr_ids = Some(Arc::new(ids));
        self
    }

    /// The retry policy. Defaults to [`RetryPolicy::default`].
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// How long one attempt may take, end to end. Defaults to 10 s.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// How long an HTTP connection may take to open. Defaults to 5 s. Has no
    /// effect in process, or with [`Builder::http_client`].
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        if let Target::Http {
            connect_timeout, ..
        } = &mut self.target
        {
            *connect_timeout = timeout;
        }
        self
    }

    /// Use this `reqwest` client, to share its pool or set TLS roots. Its
    /// own connect timeout applies. Has no effect in process.
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        if let Target::Http { client, .. } = &mut self.target {
            *client = Some(http);
        }
        self
    }

    /// Stamp every request with `resonate:debug_time` from this clock.
    ///
    /// For a server started in debug mode, where the clock belongs to the
    /// caller: tests, simulations, and recording traces for `spec/valid`.
    /// A server not in debug mode ignores it.
    pub fn debug_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.debug_clock = Some(Arc::new(clock));
        self
    }

    /// Hand every answered exchange to `observer` (see [`Observer`]).
    pub fn observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        let transport = match self.target {
            Target::Local(server) => Transport::Local(server),
            Target::Http {
                url,
                client,
                connect_timeout,
            } => {
                let url = reqwest::Url::parse(&url)
                    .map_err(|e| Error::Build(format!("bad URL {url:?}: {e}")))?;
                if !matches!(url.scheme(), "http" | "https") {
                    return Err(Error::Build(format!(
                        "URL scheme must be http or https, not {:?}",
                        url.scheme()
                    )));
                }
                let client = match client {
                    Some(c) => c,
                    None => reqwest::Client::builder()
                        .connect_timeout(connect_timeout)
                        .build()
                        .map_err(|e| Error::Build(e.to_string()))?,
                };
                Transport::Http(Http::new(client, url))
            }
        };
        Ok(Client {
            inner: Arc::new(Inner {
                transport,
                auth: self.auth,
                corr_ids: self.corr_ids.unwrap_or_else(|| Arc::new(Sequential::new())),
                retry: self.retry,
                jitter: Jitter::new(),
                timeout: self.timeout,
                debug_clock: self.debug_clock,
                observer: self.observer,
            }),
        })
    }
}

/// The client as the port: one envelope in, one envelope out.
///
/// The envelope is sent as the caller built it — its `corrId`, its
/// `head.auth`, its `resonate:debug_time` — with the token source adding a
/// token where the envelope has none, and the debug clock stamping a time
/// where it has none. The retry policy applies. A call that gets no answer
/// within it, an answer of `503`, or an answer that is not a protocol response
/// is [`Unavailable`]: the gateway renders that as `503` again, so a gateway
/// in front of a `Client` passes a remote server's unavailability through.
#[async_trait]
impl ResonateServer for Client {
    async fn ready(&self) -> bool {
        self.inner.transport.ready(self.inner.timeout).await
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        let out = Outgoing {
            kind: req.kind.clone(),
            corr_id: req.head.corr_id.clone(),
            auth: req.head.auth.clone(),
            debug_time: req
                .head
                .debug_time
                .or_else(|| self.inner.debug_clock.as_ref().map(|c| c())),
            data: req.data.clone(),
        };
        match self.exchange(out).await {
            Ok((raw, _)) => Ok(ResponseEnvelope::new(
                raw.kind,
                raw.corr_id,
                i32::from(raw.status),
                raw.data,
            )),
            Err(e) => Err(Unavailable::new(e.to_string())),
        }
    }
}
