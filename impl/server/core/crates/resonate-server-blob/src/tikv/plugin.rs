//! `resonate-server-tikv`: the blob server on TiKV, as a server plugin.
//!
//! It reads `[servers.server_tikv]`, opens the keyspace in `init`, and runs the
//! blob server's timer loop plus a sweep of expired commit tokens. The keys
//! are the ones Loams 0.1.0's embedded plugin of the same id read, so a Loams
//! configuration carries over unchanged.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};

use super::keyspace::{ensure_keyspace, pd_http_url};
use super::store::{TikvStore, TikvStoreCfg};
use crate::applier::{ApplierCfg, KeySpace};
use crate::kernel::state::KernelCfg;
use crate::server::{Server, ServerCfg};
use crate::store::Store;

/// The plugin. Its id, `server_tikv`, names the config section.
pub static PLUGIN: resonate_plugin::ServerPlugin =
    resonate_plugin::ServerPlugin::new("resonate-server-tikv", configure);

const TOKEN_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// How long `stop` waits for each background task before aborting it.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// A root given as a string (`"resonate-durable/"`) or as bytes
/// (`[114, 101, ...]`, what Loams 0.1.0 writes).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Root {
    Text(String),
    Bytes(Vec<u8>),
}

impl Default for Root {
    fn default() -> Self {
        Root::Text(String::new())
    }
}

impl Root {
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Root::Text(s) => s.as_bytes().to_vec(),
            Root::Bytes(b) => b.clone(),
        }
    }
}

/// Everything under `[servers.server_tikv]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// PD endpoints, `host:port`.
    pub pd: Vec<String>,
    /// The API v2 keyspace. Empty means API v1, a cluster without
    /// keyspaces, where `root` is the only isolation.
    #[serde(default)]
    pub keyspace: String,
    /// The prefix every key lives under inside the keyspace. Every instance
    /// of one deployment must use the same root. Unrelated deployments on
    /// one keyspace must use different ones.
    #[serde(default)]
    pub root: Root,
    /// Create the keyspace through PD's HTTP API when it is missing.
    #[serde(default = "yes")]
    pub create_keyspace: bool,
    /// The URL a worker is told to call back on.
    #[serde(default)]
    pub server_url: String,
    /// How long a task may be held before it is handed to someone else.
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: i64,
    /// Answer `promise.search`, `task.search` and `schedule.search`. Each one
    /// scans the store, but a console needs them, so they are on.
    #[serde(default = "yes")]
    pub search: bool,
    /// How many prefixes the timer keys are spread across.
    #[serde(default = "default_timer_shards")]
    pub timer_shards: u32,
    /// Documents held in the read cache.
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize,
    /// How many times a batch is re-decided after losing a race.
    #[serde(default = "default_max_cas_retries")]
    pub max_cas_retries: u32,
    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,
    /// The TiKV client's per-request timeout, in milliseconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Async commit and one-phase commit (off: classic two-phase commit).
    #[serde(default = "yes")]
    pub async_commit: bool,
}

fn yes() -> bool {
    true
}
fn default_retry_timeout() -> i64 {
    30_000
}
fn default_timer_shards() -> u32 {
    4
}
fn default_cache_capacity() -> usize {
    10_000
}
fn default_max_cas_retries() -> u32 {
    8
}
fn default_preload_limit() -> u32 {
    10
}
fn default_request_timeout_ms() -> u64 {
    5_000
}

impl Config {
    /// The store configuration this section names.
    pub fn store(&self) -> TikvStoreCfg {
        TikvStoreCfg {
            pd: self.pd.clone(),
            keyspace: self.keyspace.clone(),
            root: self.root.bytes(),
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            async_commit: self.async_commit,
        }
    }
}

fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::ServerDependencies,
) -> Result<Arc<dyn ResonateServer>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if config.pd.is_empty() || config.pd.iter().any(|p| p.trim().is_empty()) {
        return Err(settings.reject("pd", "at least one PD endpoint is required"));
    }
    if config.timer_shards == 0 {
        return Err(settings.reject("timer_shards", "must be at least 1 (got 0)"));
    }
    if config.cache_capacity == 0 {
        return Err(settings.reject("cache_capacity", "must be at least 1 (got 0)"));
    }
    if config.request_timeout_ms == 0 {
        return Err(settings.reject("request_timeout_ms", "must be positive"));
    }
    Ok(Arc::new(TikvServer::new(config, deps.router)))
}

/// The plugin's server: config and a router now, a keyspace after `init`.
pub struct TikvServer {
    config: Config,
    router: Arc<dyn ResonateRouter>,
    inner: OnceLock<Arc<Server>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    token_sweeper: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TikvServer {
    pub fn new(config: Config, router: Arc<dyn ResonateRouter>) -> Self {
        Self {
            config,
            router,
            inner: OnceLock::new(),
            shutdown: tokio::sync::watch::channel(false).0,
            timer: Mutex::new(None),
            token_sweeper: Mutex::new(None),
        }
    }

    /// The blob server over `store`, as this section configures it.
    pub fn build(&self, store: Arc<dyn Store>, debug: bool) -> Arc<Server> {
        Server::build(
            store,
            Arc::clone(&self.router),
            ServerCfg {
                // The root isolates deployments, so the blob prefix is empty:
                // that is the layout 0.1.0 wrote.
                keys: KeySpace::new("", self.config.timer_shards),
                applier: ApplierCfg {
                    max_cas_retries: self.config.max_cas_retries,
                    kernel: KernelCfg {
                        retry_timeout: self.config.retry_timeout,
                        preload_limit: self.config.preload_limit,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                timerd: Default::default(),
                cache_capacity: self.config.cache_capacity,
                debug,
                search: self.config.search,
                server_url: self.config.server_url.clone(),
            },
        )
    }
}

#[async_trait]
impl ResonateServer for TikvServer {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let config = self.config.store();
        if self.config.create_keyspace && !config.keyspace.is_empty() {
            ensure_keyspace(&pd_http_url(&config.pd[0]), &config.keyspace)
                .await
                .map_err(|e| {
                    Unavailable::new(format!(
                        "cannot open the TiKV keyspace {}: {e}",
                        config.keyspace
                    ))
                })?;
        }
        tracing::info!(
            pd = ?config.pd,
            keyspace = %config.keyspace,
            root = %String::from_utf8_lossy(&config.root),
            "Using the TiKV backend"
        );
        let store = TikvStore::connect(config)
            .await
            .map_err(|e| Unavailable::new(format!("cannot open the TiKV store: {e}")))?;
        let server = self.build(Arc::new(store.clone()), debug);
        self.inner
            .set(Arc::clone(&server))
            .map_err(|_| Unavailable::new("the TiKV server was initialized twice"))?;
        if debug {
            return Ok(());
        }
        let timer = Arc::clone(server.timerd()).spawn(self.shutdown.subscribe());
        *self.timer.lock().expect("TiKV timer mutex") = Some(timer);
        let mut shutdown = self.shutdown.subscribe();
        let sweeper = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TOKEN_SWEEP_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = interval.tick() => {
                        tokio::select! {
                            _ = shutdown.changed() => break,
                            result = store.sweep_expired_tokens() => {
                                if let Err(error) = result {
                                    tracing::warn!(%error, "TiKV commit-token sweep failed");
                                }
                            }
                        }
                    }
                }
            }
        });
        *self.token_sweeper.lock().expect("TiKV sweeper mutex") = Some(sweeper);
        Ok(())
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        let _ = self.shutdown.send(true);
        let timer = self.timer.lock().expect("TiKV timer mutex").take();
        if let Some(timer) = timer {
            join_within(timer, "timer").await;
        }
        let sweeper = self
            .token_sweeper
            .lock()
            .expect("TiKV sweeper mutex")
            .take();
        if let Some(sweeper) = sweeper {
            join_within(sweeper, "token sweeper").await;
        }
        Ok(())
    }

    async fn process(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.inner
            .get()
            .ok_or_else(|| Unavailable::new("the TiKV server has not been started"))?
            .process(request)
            .await
    }

    async fn ready(&self) -> bool {
        match self.inner.get() {
            Some(server) => server.ready().await,
            None => false,
        }
    }
}

/// Waits up to [`STOP_GRACE`] for a task told to stop, then aborts it, so a
/// stalled TiKV call cannot hold up shutdown.
async fn join_within(task: tokio::task::JoinHandle<()>, name: &str) {
    let abort = task.abort_handle();
    if tokio::time::timeout(STOP_GRACE, task).await.is_err() {
        tracing::warn!(task = name, "a TiKV task did not stop in time; aborting it");
        abort.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_root_reads_as_text_or_as_bytes() {
        let text: Config =
            serde_json::from_str(r#"{"pd":["pd:2379"],"keyspace":"k","root":"rd/"}"#).unwrap();
        let bytes: Config =
            serde_json::from_str(r#"{"pd":["pd:2379"],"keyspace":"k","root":[114,100,47]}"#)
                .unwrap();
        assert_eq!(text.root.bytes(), b"rd/");
        assert_eq!(bytes.root.bytes(), b"rd/");
        let none: Config = serde_json::from_str(r#"{"pd":["pd:2379"]}"#).unwrap();
        assert!(none.root.bytes().is_empty());
        assert!(none.search && none.create_keyspace && none.async_commit);
        assert_eq!(none.retry_timeout, 30_000);
    }
}
