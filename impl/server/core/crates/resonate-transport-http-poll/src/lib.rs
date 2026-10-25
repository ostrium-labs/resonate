//! Resonate transport: HTTP poll (SSE).
//!
//! For workers that cannot be dialled: the worker opens an SSE connection to
//! the server and messages are pushed down it. A transport rather than a
//! router — it knows how to reach a worker, not what the message means.
//!
//! Unlike the dialled transports this one has an inbound half: the endpoint
//! workers connect to. It binds no socket for it — it registers the route, and
//! the HTTP gateway that owns the listener serves it. One listener, whatever
//! the set of plugins that have put routes on it.
//!
//! Workers connect via `GET /poll/{group}/{id}` and receive messages as
//! Server-Sent Events; [`PollRegistry`] holds those connections open and
//! pushes to them based on `poll://` address routing.
//!
//! # Groups with many consumers
//!
//! Any number of workers, or gateway replicas, may share a group. A message
//! for `poll://any@<group>` goes to exactly one of them: one connection,
//! picked at random, and the next one along if that one is full or has gone.
//! A connection leaves its group the moment its stream ends, and whatever was
//! queued on it unread is routed again. What has been written to the socket is
//! the worker's: if it dies before acting on it, or the connection is
//! half-open and the writes go nowhere, the task's retry or lease deadline on
//! the server is what brings it back. There is no acknowledgement in this
//! transport; acquiring the task is the acknowledgement.

// axum comes from `resonate-plugin`, so a build has one of it — see that
// crate's re-export for why.
use resonate_plugin::axum;

/// The address scheme this transport serves.
pub const SCHEME: &str = "poll";

/// This transport, as a plugin. The one thing a binary names to get `poll://`
/// addresses delivered.
pub static PLUGIN: resonate_plugin::WorkerPlugin =
    resonate_plugin::WorkerPlugin::new(env!("CARGO_PKG_NAME"), &[SCHEME], configure);

/// Read `[workers.transport_http_poll]`, and build the registry unless it is turned off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::WorkerDependencies,
) -> Result<Option<std::sync::Arc<dyn ResonateWorker>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    // `tokio::sync::mpsc::channel` panics on a zero capacity, and a connection
    // limit of zero would accept nothing while still holding the scheme.
    if config.buffer_size == 0 {
        return Err(settings.reject("buffer_size", "must be at least 1 (got 0)"));
    }
    if config.max_connections == 0 {
        return Err(settings.reject("max_connections", "must be at least 1 (got 0)"));
    }
    // Not `0` meaning off: off is `unset`. A zero interval would be a timer
    // that fires as fast as it can, which is not what anyone writing it means.
    if config.keepalive_interval_ms == Some(0) {
        return Err(settings.reject(
            "keepalive_interval_ms",
            "must be at least 1, or unset for no keepalive (got 0)",
        ));
    }
    let registry = PollRegistry::new(deps.server, config);

    // The inbound half. A worker that cannot be dialled dials in, so this
    // transport needs an HTTP endpoint — but it does not need a *listener*.
    // The gateway has one, and one port is what makes the endpoint reachable
    // wherever the protocol is.
    let handler = Arc::clone(&registry);
    deps.routes.add(PLUGIN.id(), move |auth| {
        axum::Router::new()
            .route(POLL_PATH, get(handle))
            .with_state((handler, auth))
    });

    Ok(Some(registry))
}

/// Where workers connect. On the gateway's listener, alongside the protocol.
pub const POLL_PATH: &str = "/poll/:group/:id";

/// Everything under `[workers.transport_http_poll]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Enable the poll:// address scheme [default: true]
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Maximum concurrent SSE connections held open [default: 1000]
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Per-connection message buffer [default: 100]
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// How often to send a keepalive down an idle connection, in
    /// milliseconds. A proxy or load balancer between the worker and here
    /// closes a connection that has been quiet too long, and an SSE comment
    /// frame is enough to stop it. Unset sends none, which is right when
    /// nothing sits in the middle [default: unset]
    #[serde(default)]
    pub keepalive_interval_ms: Option<u64>,
    // No `bind` and no `auth`. This transport serves a route, not a socket:
    // the gateway that owns the listener owns the address it is reachable at
    // and the policy that admits a connection to it. One door, one lock.
}

fn default_enabled() -> bool {
    true
}
fn default_max_connections() -> usize {
    1000
}
fn default_buffer_size() -> usize {
    100
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_connections: default_max_connections(),
            buffer_size: default_buffer_size(),
            keepalive_interval_ms: None,
        }
    }
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

use resonate_plugin::types::Message;
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};

/// A `poll://` destination: `poll://<cast>@<group>[/<id>]`.
#[derive(Debug, Clone)]
pub struct PollAddress {
    pub cast: PollCast,
    pub group: String,
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PollCast {
    Uni,
    Any,
}

impl PollAddress {
    /// Parse a `poll://` address. This worker owns the syntax; the router has
    /// only checked the scheme.
    pub fn parse(address: &str) -> Result<Self, Unavailable> {
        let bad = || Unavailable::new(format!("malformed poll address: {address}"));
        let parsed = url::Url::parse(address).map_err(|_| bad())?;
        let cast = match parsed.username() {
            "uni" => PollCast::Uni,
            "any" => PollCast::Any,
            _ => return Err(bad()),
        };
        let group = parsed.host_str().ok_or_else(bad)?.to_string();
        let path = parsed.path();
        let id = if path.len() > 1 {
            Some(path[1..].to_string())
        } else {
            None
        };
        Ok(PollAddress { cast, group, id })
    }
}

/// A message waiting on a connection, with the address it was routed by.
///
/// The address travels with it so that a message a consumer never read can be
/// routed again when that consumer goes: see [`PollReceiver`]'s `Drop`.
struct Queued {
    address: Arc<PollAddress>,
    payload: String,
}

/// A single SSE connection to a worker: the sending half of its stream.
pub struct PollConnection {
    /// Unique identifier for this specific connection instance.
    pub conn_id: u64,
    pub id: String,
    tx: mpsc::Sender<Queued>,
}

impl PollConnection {
    /// Its stream has ended: the receiver is gone, and nothing sent here will
    /// ever be read.
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// The receiving half of a connection: what its SSE stream reads from.
///
/// Dropping it is how a connection leaves its group. That happens when the
/// stream ends for any reason — the worker disconnected, a write failed, the
/// transport stopped — and it happens synchronously, so there is no window in
/// which the group still offers a connection nobody is reading. Whatever was
/// queued here and not yet read is routed again by its original address: no
/// consumer has seen it, so handing it to another is not a second delivery.
pub struct PollReceiver {
    registry: Weak<PollRegistry>,
    group: String,
    conn_id: u64,
    rx: mpsc::Receiver<Queued>,
}

impl PollReceiver {
    /// The next message, or `None` once the transport has dropped this
    /// connection (on [`stop`](ResonateWorker::stop)).
    pub async fn recv(&mut self) -> Option<String> {
        self.rx.recv().await.map(|q| q.payload)
    }

    /// The next message if one is waiting.
    pub fn try_recv(&mut self) -> Result<String, TryRecvError> {
        self.rx.try_recv().map(|q| q.payload)
    }
}

impl Drop for PollReceiver {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        registry.remove(&self.group, self.conn_id);
        // Closed first, so nothing new lands here while it is emptied.
        self.rx.close();
        let (mut moved, mut lost) = (0usize, 0usize);
        while let Ok(q) = self.rx.try_recv() {
            match registry.route(&q.address, q.payload) {
                Ok(()) => moved += 1,
                Err(_) => lost += 1,
            }
        }
        if moved + lost > 0 {
            tracing::info!(
                group = %self.group,
                conn_id = self.conn_id,
                moved,
                lost,
                "Unread poll messages of a closed connection routed again"
            );
        }
    }
}

/// Manages all active poll connections, grouped by group name.
pub struct PollRegistry {
    /// group -> [connection], oldest first.
    ///
    /// A plain mutex: it is never held across an `await`, and that is what
    /// lets a [`PollReceiver`] leave its group from `Drop`.
    connections: Mutex<HashMap<String, Vec<PollConnection>>>,
    /// Monotonically increasing counter for unique connection IDs.
    next_conn_id: AtomicU64,
    pub max_connections: usize,
    pub buffer_size: usize,
    pub keepalive_interval_ms: Option<u64>,
    /// Handed to each [`PollReceiver`], so it can leave its group when dropped.
    me: Weak<PollRegistry>,
    /// Held so a delivery failure can be reported back to the server (e.g.
    /// releasing the task instead of dropping it). Not used yet.
    ///
    /// Weak: the server holds the router and the router holds this worker, so
    /// a strong handle back would close a reference cycle.
    #[allow(dead_code)]
    server: Weak<dyn ResonateServer>,
}

impl PollRegistry {
    /// `Arc`, because the route registered for it holds one.
    pub fn new(server: Weak<dyn ResonateServer>, config: Config) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            connections: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            max_connections: config.max_connections,
            buffer_size: config.buffer_size,
            keepalive_interval_ms: config.keepalive_interval_ms,
            me: me.clone(),
            server,
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<PollConnection>>> {
        // Nothing done under the lock can leave the map half-written.
        self.connections.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// How many connections are open, across every group.
    pub async fn connection_count(&self) -> usize {
        self.lock().values().map(|v| v.len()).sum()
    }

    /// Register a new connection. Returns its id and the receiving end of the
    /// message channel. Returns None if max connections exceeded.
    ///
    /// The registry is the sole owner of every sender. That is what makes
    /// [`stop`](ResonateWorker::stop) able to end the streams by clearing the
    /// map: the receiver sees its channel close. The receiver, for its part,
    /// takes the connection out of its group when it is dropped.
    pub async fn register(&self, group: &str, id: &str) -> Option<(u64, PollReceiver)> {
        let mut conns = self.lock();

        // Check total connection count
        let total: usize = conns.values().map(|v| v.len()).sum();
        if total >= self.max_connections {
            return None;
        }

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(self.buffer_size);
        conns
            .entry(group.to_string())
            .or_default()
            .push(PollConnection {
                conn_id,
                id: id.to_string(),
                tx,
            });

        tracing::info!(
            group = %group,
            id = %id,
            conn_id = conn_id,
            total_connections = total + 1,
            "Poll connection registered"
        );

        let receiver = PollReceiver {
            registry: self.me.clone(),
            group: group.to_string(),
            conn_id,
            rx,
        };
        Some((conn_id, receiver))
    }

    /// Deregister a specific connection by its unique connection ID.
    ///
    /// Dropping its [`PollReceiver`] does this already; this is for a caller
    /// that wants it gone while still holding the receiver.
    pub async fn deregister(&self, group: &str, conn_id: u64) {
        self.remove(group, conn_id);
    }

    fn remove(&self, group: &str, conn_id: u64) {
        let mut conns = self.lock();
        if let Some(group_conns) = conns.get_mut(group) {
            group_conns.retain(|c| c.conn_id != conn_id);
            if group_conns.is_empty() {
                conns.remove(group);
            }
        }
        tracing::info!(group = %group, conn_id = conn_id, "Poll connection deregistered");
    }

    /// Send a message to the appropriate connection based on the poll address.
    /// Returns true if a connection accepted it.
    pub async fn send_poll(&self, address: &PollAddress, payload: &str) -> bool {
        self.route(&Arc::new(address.clone()), payload.to_string())
            .is_ok()
    }

    /// Put `payload` on exactly one connection, or hand it back.
    ///
    /// The candidates, in the order they are tried:
    ///
    /// - `uni@group/id`: the connections for `id`, newest first. A worker
    ///   that reconnected has its old connection in the group until the
    ///   server notices it is gone, and the new one is the one being read.
    /// - `any@group/id`: the same, then every other connection in the group.
    /// - `any@group`: every connection, from a random one onwards. Random is
    ///   what spreads the load; going on from there is what makes a full or
    ///   closed connection cost a skip rather than the message.
    ///
    /// The first that accepts it has it, so a message is never on two
    /// connections. A connection whose stream has ended is removed from the
    /// group as soon as a send finds it.
    fn route(&self, address: &Arc<PollAddress>, payload: String) -> Result<(), String> {
        let mut conns = self.lock();
        let Some(group) = conns.get_mut(&address.group) else {
            tracing::warn!(
                group = %address.group,
                "Poll send failed: no active connections for group"
            );
            return Err(payload);
        };
        group.retain(|c| !c.is_closed());

        let len = group.len();
        let named = |c: &PollConnection| address.id.as_deref() == Some(c.id.as_str());
        let mut order: Vec<usize> = (0..len).rev().filter(|&i| named(&group[i])).collect();
        if address.cast == PollCast::Any && len > 0 {
            let start = fastrand::usize(..len);
            order.extend(
                (0..len)
                    .map(|k| (start + k) % len)
                    .filter(|&i| !named(&group[i])),
            );
        }

        let mut item = Queued {
            address: Arc::clone(address),
            payload,
        };
        for i in order {
            match group[i].tx.try_send(item) {
                Ok(()) => {
                    tracing::debug!(
                        group = %address.group,
                        selected_id = %group[i].id,
                        conn_id = group[i].conn_id,
                        "Poll message delivered"
                    );
                    return Ok(());
                }
                Err(TrySendError::Full(q)) | Err(TrySendError::Closed(q)) => item = q,
            }
        }

        if group.is_empty() {
            conns.remove(&address.group);
        }
        tracing::warn!(
            group = %address.group,
            id = ?address.id,
            connections = len,
            "Poll message delivery failed: no connection could take it"
        );
        Err(item.payload)
    }
}

#[async_trait::async_trait]
impl ResonateWorker for PollRegistry {
    // No `init`. There is nothing to open: the route was registered during
    // `configure` and the gateway binds the socket it lives on.

    /// Drop every connection, which is what ends the SSE streams.
    ///
    /// Each registered connection owns the sending half of its stream's
    /// channel. Clearing the map drops them all, the handler's `recv` returns
    /// `None`, and the stream finishes on its own — so this transport tears
    /// down its own connections and nothing outside it needs a say.
    ///
    /// That is also what lets the gateway stop *after* the workers: axum's
    /// graceful shutdown waits for every in-flight response, and an SSE
    /// response is in flight until its stream ends. By the time the gateway
    /// drains, there are none left for it to wait on.
    /// `a_drained_registry_ends_its_streams` says so.
    async fn stop(&self) -> Result<(), Unavailable> {
        let mut conns = self.lock();
        let n: usize = conns.values().map(|v| v.len()).sum();
        conns.clear();
        if n > 0 {
            tracing::info!(connections = n, "Poll connections closed");
        }
        Ok(())
    }

    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let addr = Arc::new(PollAddress::parse(address)?);
        // Serialize via `Value` rather than straight from the struct. serde_json
        // has no `preserve_order`, so a `Value` map is a BTreeMap and emits keys
        // alphabetically — which is what SSE consumers have always received.
        // Going direct would emit declaration order instead and change the bytes.
        let mut value = serde_json::to_value(msg)
            .map_err(|e| Unavailable::new(format!("cannot serialize message: {e}")))?;
        let span = tracing::info_span!(
            "resonate.deliver",
            otel.name = "deliver poll",
            otel.kind = "producer",
            resonate.message.kind = value["kind"].as_str().unwrap_or(""),
            resonate.poll.group = %addr.group,
            resonate.delivered = tracing::field::Empty,
        );
        // A message that carries a trace carries this delivery as its parent
        // when spans are exported. The frame is the only channel to the
        // worker, so the context goes in its `head`, where the router put it.
        if value["head"].get("traceparent").is_some() {
            if let Some(tp) = resonate_trace::of(&span) {
                value["head"]["traceparent"] = tp.traceparent().into();
                if let Some(state) = tp.tracestate() {
                    value["head"]["tracestate"] = state.into();
                }
            }
        }
        let body = serde_json::to_string(&value)
            .map_err(|e| Unavailable::new(format!("cannot serialize message: {e}")))?;
        let routed = span.in_scope(|| self.route(&addr, body));
        span.record("resonate.delivered", routed.is_ok());
        routed.map_err(|_| {
            Unavailable::new(format!(
                "no poll connection accepted delivery for {address}"
            ))
        })
    }
}

// ─── The endpoint workers dial ───────────────────────────────────────────────
//
// It used to be a route the HTTP gateway hosted, which meant the gateway named
// `PollRegistry` — one plugin reaching into another's concrete type, and the
// reason the gateway could not be a plugin itself.

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

/// Serve `GET /poll/{group}/{id}` until `stop`.
/// What the handler needs: the registry to register into, and the gateway's
/// auth policy — handed over when it built these routes, so this endpoint
/// admits exactly whom the protocol endpoint admits.
type PollState = (Arc<PollRegistry>, Option<resonate_auth::AuthMode>);

async fn handle(
    State((registry, auth)): State<PollState>,
    headers: axum::http::HeaderMap,
    Path((group, id)): Path<(String, String)>,
) -> Response {
    if let Some(auth) = &auth {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if !auth.check_token(token).await {
            tracing::warn!(group = %group, id = %id, "Poll connection rejected: unauthorized");
            return (axum::http::StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

    tracing::info!(group = %group, id = %id, "Poll SSE connection requested");
    match registry.register(&group, &id).await {
        Some((conn_id, mut rx)) => {
            tracing::info!(group = %group, id = %id, conn_id, "Poll SSE connection established");
            // The stream ends when the channel closes, and nothing else. A
            // client disconnecting drops this response; the transport stopping
            // clears its registry, which drops the only sender. There is no
            // shutdown signal to keep in step with, because the thing that owns
            // the connection is the thing that ends it.
            let keepalive = registry.keepalive_interval_ms;
            // Dropping the stream drops `rx`, and that is what takes this
            // connection out of its group — see `PollReceiver`.
            let stream = async_stream::stream! {
                while let Some(msg) = rx.recv().await {
                    yield Ok::<_, std::convert::Infallible>(Event::default().data(msg));
                }
            };
            let sse = Sse::new(stream);
            match keepalive {
                Some(ms) => sse
                    .keep_alive(
                        KeepAlive::new()
                            .interval(std::time::Duration::from_millis(ms))
                            .text("ping"),
                    )
                    .into_response(),
                None => sse.into_response(),
            }
        }
        None => {
            tracing::warn!(group = %group, id = %id, "Poll connection rejected: at capacity");
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Poll registration at capacity",
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopServer;

    #[async_trait::async_trait]
    impl ResonateServer for NoopServer {
        async fn process(
            &self,
            _req: &resonate_plugin::types::RequestEnvelope,
        ) -> Result<resonate_plugin::types::ResponseEnvelope, Unavailable> {
            unreachable!("never called")
        }
    }

    fn no_server() -> resonate_plugin::WorkerDependencies {
        resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>,
            resonate_plugin::Routes::new(),
        )
    }

    /// What `configure` builds, without the deps it does not touch here.
    fn registry(config: Config) -> Arc<PollRegistry> {
        PollRegistry::new(
            std::sync::Weak::<NoopServer>::new() as Weak<dyn ResonateServer>,
            config,
        )
    }

    fn settings(pairs: &[(&str, &str)]) -> resonate_plugin::Configuration {
        let mut loader = resonate_plugin::Loader::new();
        for (k, v) in pairs {
            loader = loader.set(k, v).unwrap();
        }
        loader.load()
    }

    #[test]
    fn a_section_nobody_wrote_gets_this_crate_s_defaults() {
        let config = settings(&[]);
        let worker = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()).unwrap();
        assert!(worker.is_some(), "poll is on unless turned off");
        assert_eq!(PLUGIN.schemes, &["poll"]);
        assert_eq!(
            config.worker(&PLUGIN.id()).key(),
            "workers.transport_http_poll"
        );
    }

    #[test]
    fn turning_it_off_is_its_own_setting() {
        let config = settings(&[("workers.transport_http_poll.enabled", "false")]);
        assert!(
            (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_zero_sized_buffer_is_refused_at_startup() {
        // tokio::sync::mpsc::channel panics on a zero capacity, so this used to
        // be a check in the server's main, three crates away from the channel.
        let config = settings(&[("workers.transport_http_poll.buffer_size", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()) else {
            panic!("channel construction would panic");
        };
        assert_eq!(err.key, "workers.transport_http_poll.buffer_size");
    }

    /// Nothing between the worker and here by default, so nothing to keep
    /// alive. A deployment that has a proxy in the middle is the one that
    /// knows it.
    #[test]
    fn no_keepalive_unless_a_deployment_asks_for_one() {
        let config = settings(&[]);
        assert_eq!(Config::default().keepalive_interval_ms, None);
        let settings = config.worker(&PLUGIN.id());
        let extracted: Config = settings.extract().unwrap();
        assert_eq!(extracted.keepalive_interval_ms, None);
        assert_eq!(registry(extracted).keepalive_interval_ms, None);
    }

    /// The interval reaches the registry, which is what the handler reads it
    /// from when it builds the SSE response.
    #[test]
    fn an_interval_reaches_the_thing_that_serves_the_stream() {
        let config = settings(&[("workers.transport_http_poll.keepalive_interval_ms", "30000")]);
        let extracted: Config = config.worker(&PLUGIN.id()).extract().unwrap();
        assert_eq!(extracted.keepalive_interval_ms, Some(30_000));
        assert_eq!(registry(extracted).keepalive_interval_ms, Some(30_000));
    }

    /// Off is `unset`, not `0` — so `0` is a mistake, and startup says so
    /// rather than spinning a timer as fast as it can fire.
    #[test]
    fn a_zero_interval_is_refused_rather_than_read_as_off() {
        let config = settings(&[("workers.transport_http_poll.keepalive_interval_ms", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()) else {
            panic!("a zero interval must not be taken as \"no keepalive\"");
        };
        assert_eq!(err.key, "workers.transport_http_poll.keepalive_interval_ms");
    }

    /// Registering a route is what this transport does instead of binding.
    ///
    /// The gateway serves it, so `configure` has to have put it there before
    /// any gateway's `init` runs — which is why registration is here and not in
    /// an `init` of its own.
    #[test]
    fn it_registers_a_route_rather_than_taking_a_port() {
        let config = settings(&[]);
        let routes = resonate_plugin::Routes::new();
        let deps = resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>,
            Arc::clone(&routes),
        );
        (PLUGIN.configure)(&config.worker(&PLUGIN.id()), deps).unwrap();

        let registered = routes.take();
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].0, "transport_http_poll");
    }

    /// A transport that is off registers nothing — no dangling endpoint on the
    /// gateway for a scheme nothing routes to.
    #[test]
    fn turning_it_off_takes_the_route_with_it() {
        let config = settings(&[("workers.transport_http_poll.enabled", "false")]);
        let routes = resonate_plugin::Routes::new();
        let deps = resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>,
            Arc::clone(&routes),
        );
        assert!((PLUGIN.configure)(&config.worker(&PLUGIN.id()), deps)
            .unwrap()
            .is_none());
        assert!(routes.is_empty());
    }

    /// `stop` ends every stream, which is what lets the gateway drain after it.
    ///
    /// axum's graceful shutdown waits for in-flight responses, and an SSE
    /// response is in flight until its stream ends — and these streams end when
    /// this registry drops its senders. If `stop` left one alive, the gateway
    /// would wait on it until the shutdown timeout.
    #[tokio::test]
    async fn a_drained_registry_ends_its_streams() {
        let poll = PollRegistry::new(
            std::sync::Weak::<NoopServer>::new() as Weak<dyn ResonateServer>,
            Config::default(),
        );
        let (_conn_id, mut rx) = poll.register("default", "w1").await.expect("capacity");
        assert_eq!(poll.connection_count().await, 1);

        poll.stop().await.expect("stops");

        assert_eq!(poll.connection_count().await, 0);
        assert!(
            rx.recv().await.is_none(),
            "the stream must see its channel close, or the gateway waits on it"
        );
    }

    // ─── Groups with many consumers ──────────────────────────────────────────
    //
    // Several workers (or gateway replicas) share one `poll://any@<group>`.
    // What a group owes them: each message reaches one live consumer, a
    // consumer that is gone stops being chosen at once, and a message is not
    // given up on while another consumer could take it.

    fn execute(task: &str) -> Message {
        use resonate_plugin::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};
        Message::Execute(ExecuteMsg {
            kind: "execute".to_string(),
            head: MessageHead {
                server_url: "http://server".to_string(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: task.to_string(),
                    version: 0,
                },
            },
        })
    }

    /// The task id an SSE frame carries.
    fn task_of(frame: &str) -> String {
        let v: serde_json::Value = serde_json::from_str(frame).expect("a frame is JSON");
        v["data"]["task"]["id"]
            .as_str()
            .expect("an execute")
            .to_string()
    }

    /// Everything waiting on a receiver right now.
    fn drain_now(rx: &mut PollReceiver) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            out.push(task_of(&frame));
        }
        out
    }

    /// One `process` puts a message on one connection: never two, never none
    /// while a consumer is there. And the choice is spread over the group.
    #[tokio::test]
    async fn an_anycast_reaches_exactly_one_consumer_and_the_load_is_spread() {
        let poll = registry(Config {
            buffer_size: 10_000,
            ..Config::default()
        });
        let mut rxs = Vec::new();
        for id in ["a", "b", "c"] {
            rxs.push(poll.register("g", id).await.expect("capacity").1);
        }
        let n = 3_000;
        for i in 0..n {
            poll.process("poll://any@g", &execute(&format!("t{i}")))
                .await
                .expect("a live consumer takes it");
        }
        let mut seen = std::collections::HashSet::new();
        for rx in &mut rxs {
            let got = drain_now(rx);
            // Uniform over three: 1000 each, and 800 is more than six
            // standard deviations below that.
            assert!(got.len() > 800, "one consumer got only {}", got.len());
            for t in got {
                assert!(seen.insert(t.clone()), "{t} delivered twice");
            }
        }
        assert_eq!(seen.len(), n);
    }

    /// A consumer whose stream has ended is not chosen, and its message goes
    /// to one that is still there rather than being reported undeliverable.
    #[tokio::test]
    async fn a_consumer_that_went_away_is_skipped_and_dropped() {
        let poll = registry(Config::default());
        let (_, mut live) = poll.register("g", "live").await.expect("capacity");
        let (_, gone) = poll.register("g", "gone").await.expect("capacity");
        drop(gone);

        for i in 0..50 {
            poll.process("poll://any@g", &execute(&format!("t{i}")))
                .await
                .expect("the live consumer takes every one");
        }
        assert_eq!(drain_now(&mut live).len(), 50);
        assert_eq!(
            poll.connection_count().await,
            1,
            "the closed connection is no longer in the group"
        );
    }

    /// A full consumer is busy, not gone: the message goes to another, and
    /// only when every consumer is full is it refused.
    #[tokio::test]
    async fn a_full_consumer_passes_the_message_on() {
        let poll = registry(Config {
            buffer_size: 1,
            ..Config::default()
        });
        let (_, mut a) = poll.register("g", "a").await.expect("capacity");
        let (_, mut b) = poll.register("g", "b").await.expect("capacity");
        poll.process("poll://any@g", &execute("t0"))
            .await
            .expect("one takes it");
        poll.process("poll://any@g", &execute("t1"))
            .await
            .expect("the other takes it");
        poll.process("poll://any@g", &execute("t2"))
            .await
            .expect_err("both full: refused, and the retry deadline takes over");
        let mut got = drain_now(&mut a);
        got.extend(drain_now(&mut b));
        got.sort();
        assert_eq!(got, ["t0", "t1"]);
    }

    /// `any@g/<id>` prefers `<id>`, and falls back to the group when that
    /// consumer cannot take it.
    #[tokio::test]
    async fn a_preferred_consumer_that_is_gone_falls_back_to_the_group() {
        let poll = registry(Config::default());
        let (_, mut other) = poll.register("g", "other").await.expect("capacity");
        let (_, preferred) = poll.register("g", "preferred").await.expect("capacity");
        drop(preferred);
        poll.process("poll://any@g/preferred", &execute("t0"))
            .await
            .expect("another consumer takes it");
        assert_eq!(drain_now(&mut other), ["t0"]);
    }

    /// Messages queued for a consumer that disconnects before reading them
    /// were never seen by anyone, so they go to another consumer instead of
    /// waiting out the server's retry deadline. Read ones are not resent.
    #[tokio::test]
    async fn what_a_dropped_consumer_had_not_read_goes_to_another() {
        let poll = registry(Config::default());
        let (_, mut stays) = poll.register("g", "stays").await.expect("capacity");
        let (_, mut leaves) = poll.register("g", "leaves").await.expect("capacity");
        for i in 0..5 {
            poll.process("poll://any@g/leaves", &execute(&format!("t{i}")))
                .await
                .expect("queued for the preferred consumer");
        }
        assert_eq!(task_of(&leaves.recv().await.expect("one read")), "t0");
        drop(leaves);
        assert_eq!(drain_now(&mut stays), ["t1", "t2", "t3", "t4"]);
    }

    /// A unicast to an id that has reconnected reaches the newer connection:
    /// the older one is the one more likely to be dead.
    #[tokio::test]
    async fn a_unicast_reaches_the_newest_connection_for_an_id() {
        let poll = registry(Config::default());
        let (_, mut older) = poll.register("g", "w").await.expect("capacity");
        let (_, mut newer) = poll.register("g", "w").await.expect("capacity");
        poll.process("poll://uni@g/w", &execute("t0"))
            .await
            .expect("delivered");
        assert_eq!(drain_now(&mut newer), ["t0"]);
        assert!(drain_now(&mut older).is_empty());
    }

    // ─── The same, over a real socket ────────────────────────────────────────

    /// Serve the poll route on a loopback port, the way a gateway would.
    async fn serve(poll: &Arc<PollRegistry>) -> std::net::SocketAddr {
        let app = axum::Router::new()
            .route(POLL_PATH, get(handle))
            .with_state((Arc::clone(poll), None::<resonate_auth::AuthMode>));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    /// Open `GET /poll/<group>/<id>` and read up to the end of the headers.
    async fn connect(addr: std::net::SocketAddr, group: &str, id: &str) -> tokio::net::TcpStream {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        s.write_all(format!("GET /poll/{group}/{id} HTTP/1.1\r\nHost: t\r\n\r\n").as_bytes())
            .await
            .expect("request");
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut byte).await.expect("headers");
            head.push(byte[0]);
        }
        assert!(
            head.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&head)
        );
        s
    }

    async fn wait_for_count(poll: &PollRegistry, want: usize) -> std::time::Duration {
        let start = std::time::Instant::now();
        while poll.connection_count().await != want {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(5),
                "still {} connections, want {want}",
                poll.connection_count().await
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        start.elapsed()
    }

    /// A consumer that closes its socket leaves the group without anything
    /// being sent to it first: the server notices the end of the connection.
    #[tokio::test]
    async fn a_closed_socket_leaves_the_group_promptly() {
        let poll = registry(Config::default());
        let addr = serve(&poll).await;
        let a = connect(addr, "g", "a").await;
        let _b = connect(addr, "g", "b").await;
        wait_for_count(&poll, 2).await;
        drop(a);
        let took = wait_for_count(&poll, 1).await;
        assert!(
            took < std::time::Duration::from_millis(500),
            "took {took:?}"
        );
    }

    /// A consumer that stops reading but keeps its socket open — hung, or on
    /// the far side of a half-open connection — cannot be told from a slow
    /// one. The server's writes still succeed into the socket buffer, so
    /// what is sent there is lost to the transport; the task's retry
    /// deadline is what brings it back. This pins that down.
    #[tokio::test]
    async fn a_consumer_that_stops_reading_still_looks_alive() {
        let poll = registry(Config::default());
        let addr = serve(&poll).await;
        let _stalled = connect(addr, "g", "stalled").await;
        wait_for_count(&poll, 1).await;
        // Ten buffers' worth, in batches the stream can drain into the socket
        // between: the channel never fills, because the kernel takes it all.
        for batch in 0..10 {
            for i in 0..100 {
                poll.process("poll://any@g", &execute(&format!("t{batch}-{i}")))
                    .await
                    .expect("accepted: the transport cannot tell it is not read");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(poll.connection_count().await, 1);
    }

    #[test]
    fn a_zero_connection_limit_is_refused_at_startup() {
        let config = settings(&[("workers.transport_http_poll.max_connections", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()) else {
            panic!("it would hold the scheme and accept nothing");
        };
        assert_eq!(err.key, "workers.transport_http_poll.max_connections");
    }
}
