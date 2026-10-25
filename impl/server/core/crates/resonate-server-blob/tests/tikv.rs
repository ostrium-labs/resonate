//! The TiKV store against a live cluster: `TEST_TIKV_PD` names PD
//! (`host:port`), `TEST_TIKV_KEYSPACE` the keyspace (default
//! `resonate_test`, created if missing). Skipped when `TEST_TIKV_PD` is unset.
//!
//! ```text
//! test/tikv/up.sh 23790 20260
//! TEST_TIKV_PD=127.0.0.1:23790 \
//!   cargo test -p resonate-server-blob --features tikv --test tikv
//! ```
#![cfg(feature = "tikv")]

use std::sync::Arc;

use resonate_core::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_core::ResonateServer;
use resonate_server_blob::sender::NullRouter;
use resonate_server_blob::server::{Server, ServerCfg};
use resonate_server_blob::store::Store;
use resonate_server_blob::tikv::store::{OBJECT_PREFIX, REVISION_BYTES, TOKEN_PREFIX};
use resonate_server_blob::tikv::{ensure_keyspace, keyspace::pd_http_url, TikvStore, TikvStoreCfg};
use serde_json::{json, Value};

struct Cluster {
    pd: Vec<String>,
    keyspace: String,
}

async fn cluster() -> Option<Cluster> {
    let pd = std::env::var("TEST_TIKV_PD")
        .ok()
        .filter(|s| !s.is_empty())?;
    let keyspace = std::env::var("TEST_TIKV_KEYSPACE").unwrap_or_else(|_| "resonate_test".into());
    let pd: Vec<String> = pd.split(',').map(str::to_owned).collect();
    ensure_keyspace(&pd_http_url(&pd[0]), &keyspace)
        .await
        .expect("TEST_TIKV_PD keyspace");
    Some(Cluster { pd, keyspace })
}

impl Cluster {
    fn cfg(&self, root: &[u8]) -> TikvStoreCfg {
        let mut cfg = TikvStoreCfg::new(self.pd.clone(), self.keyspace.clone());
        cfg.root = root.to_vec();
        cfg
    }

    async fn store(&self, root: &[u8]) -> TikvStore {
        TikvStore::connect(self.cfg(root)).await.expect("connect")
    }

    /// The raw client on the same keyspace, to write and read bytes as they
    /// are on disk.
    async fn raw(&self) -> tikv_client::TransactionClient {
        tikv_client::TransactionClient::new_with_config(
            self.pd.clone(),
            tikv_client::Config::default().with_keyspace(&self.keyspace),
        )
        .await
        .expect("raw client")
    }
}

fn fresh_root(purpose: &str) -> Vec<u8> {
    format!("{purpose}-{:016x}/", fastrand::u64(..)).into_bytes()
}

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: fastrand::u64(..).to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

fn server(store: TikvStore, debug: bool) -> Arc<Server> {
    Server::build(
        Arc::new(store),
        Arc::new(NullRouter),
        ServerCfg {
            debug,
            search: true,
            ..Default::default()
        },
    )
}

async fn call(server: &Arc<Server>, kind: &str, data: Value, now: i64) -> (u16, Value) {
    let mut envelope = req(kind, data);
    envelope.head.debug_time = Some(now);
    let response = server.process(&envelope).await.expect("available");
    let response = serde_json::to_value(&response).expect("response json");
    let status = response["head"]["status"].as_u64().unwrap_or(0) as u16;
    (status, response["data"].clone())
}

/// Keys go under `root ‖ resonate/blob/`, values are a 16-byte revision then
/// the body, and every write leaves a commit token under `root ‖ t/`.
#[tokio::test(flavor = "multi_thread")]
async fn the_layout_on_disk_is_loams_0_1() {
    let Some(cluster) = cluster().await else {
        eprintln!("TEST_TIKV_PD not set: skipped");
        return;
    };
    let root = fresh_root("layout");
    let store = cluster.store(&root).await;
    let etag = store
        .put_if_none_match("promise/a", b"body".to_vec())
        .await
        .unwrap();
    let raw = cluster.raw().await;
    let ts = raw.current_timestamp().await.unwrap();
    let mut snapshot = raw.snapshot(
        ts,
        tikv_client::TransactionOptions::new_optimistic().read_only(),
    );
    let key = [&root[..], OBJECT_PREFIX, b"promise/a"].concat();
    let value = snapshot.get(key).await.unwrap().expect("object on disk");
    assert_eq!(&value[REVISION_BYTES..], b"body");
    assert_eq!(
        uuid::Uuid::from_slice(&value[..REVISION_BYTES])
            .unwrap()
            .to_string(),
        etag.0
    );
    let start = [&root[..], TOKEN_PREFIX].concat();
    let mut end = start.clone();
    *end.last_mut().unwrap() += 1;
    let tokens: Vec<_> = snapshot.scan(start..end, 10).await.unwrap().collect();
    assert_eq!(tokens.len(), 1, "one commit token per write");
    assert_eq!(tokens[0].value().len(), 8, "a token holds its expiry");
    // Nothing is swept before its 30 minutes are up.
    assert_eq!(store.sweep_expired_tokens().await.unwrap(), 0);
}

/// Two roots on one keyspace never see each other's keys.
#[tokio::test(flavor = "multi_thread")]
async fn roots_isolate_deployments() {
    let Some(cluster) = cluster().await else {
        return;
    };
    let a = cluster.store(&fresh_root("iso-a")).await;
    let b = cluster.store(&fresh_root("iso-b")).await;
    a.put("promise/x", b"a".to_vec()).await.unwrap();
    assert_eq!(b.get("promise/x").await.unwrap(), None);
    assert!(b.list("", 10).await.unwrap().is_empty());
    assert_eq!(a.list("", 10).await.unwrap(), vec!["promise/x".to_string()]);
}

/// State outlives the server: a promise created through one server is read,
/// and settled, through another on the same root.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_server_reads_and_settles_what_an_old_one_wrote() {
    let Some(cluster) = cluster().await else {
        return;
    };
    let root = fresh_root("restart");
    let now = 1_000_000;
    let first = server(cluster.store(&root).await, true);
    let (status, _) = call(
        &first,
        "promise.create",
        json!({"id": "p1", "timeoutAt": now + 3_600_000, "param": {"data": "aW4="}, "tags": {}}),
        now,
    )
    .await;
    assert!(matches!(status, 200 | 201), "create answered {status}");
    drop(first);

    let second = server(cluster.store(&root).await, true);
    let (status, data) = call(&second, "promise.get", json!({"id": "p1"}), now + 1).await;
    assert_eq!(status, 200);
    assert_eq!(data["promise"]["state"], "pending");
    let (status, data) = call(
        &second,
        "promise.settle",
        json!({"id": "p1", "state": "resolved", "value": {"data": "b3V0"}}),
        now + 2,
    )
    .await;
    assert_eq!(status, 200, "settle answered {status}: {data}");
    let (_, data) = call(&second, "promise.get", json!({"id": "p1"}), now + 3).await;
    assert_eq!(data["promise"]["state"], "resolved");
}

/// A keyspace written by Loams 0.1.x (`tests/fixtures/loams-0.1.1-durable`,
/// dumped from a running `ghcr.io/ostrium-labs/loams:0.1.1` with
/// `--durable-store tikv://…/loams_durable`): loaded byte for byte under a
/// fresh root, every promise reads back as Loams answered it, a pending one
/// settles over its 0.1.x revision, and the expired commit tokens are swept.
#[tokio::test(flavor = "multi_thread")]
async fn tikv_store_reads_loams_010_fixture() {
    let Some(cluster) = cluster().await else {
        return;
    };
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/loams-0.1.1-durable");
    let keys = std::fs::read_to_string(dir.join("keys.ndjson")).expect("fixture keys");
    let expect: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("expect.json")).expect("fixture expect"),
    )
    .expect("fixture json");

    let root = fresh_root("fixture");
    let raw = cluster.raw().await;
    let mut txn = raw.begin_optimistic().await.unwrap();
    let mut token_expiries = Vec::new();
    for line in keys.lines().filter(|l| !l.trim().is_empty()) {
        let pair: Value = serde_json::from_str(line).expect("fixture line");
        let key = hex(pair["k"].as_str().unwrap());
        let value = hex(pair["v"].as_str().unwrap());
        if key.starts_with(TOKEN_PREFIX) {
            token_expiries.push(u64::from_be_bytes(value[..8].try_into().unwrap()));
        }
        txn.put([&root[..], &key].concat(), value).await.unwrap();
    }
    txn.commit().await.unwrap();

    let store = cluster.store(&root).await;
    let now = chrono_now();
    let server = server(store.clone(), true);
    for promise in expect["promises"].as_array().expect("promises") {
        let id = promise["id"].as_str().unwrap();
        let (status, data) = call(&server, "promise.get", json!({"id": id}), now).await;
        assert_eq!(status, 200, "promise.get {id}: {data}");
        for (field, want) in promise.as_object().expect("promise") {
            assert_eq!(&data["promise"][field], want, "{id}.{field}");
        }
    }
    if let Some(pending) = expect["settle"].as_str() {
        let (status, data) = call(
            &server,
            "promise.settle",
            json!({"id": pending, "state": "resolved", "value": {"data": "ZnJvbS1mb3Jr"}}),
            now,
        )
        .await;
        assert_eq!(
            status, 200,
            "settle {pending} over a 0.1.x revision: {data}"
        );
    }
    // Every fixture token whose 30 minutes are up is swept, and the others
    // are kept.
    let now = chrono_now() as u64;
    let expired = token_expiries.iter().filter(|&&e| e < now).count() as u64;
    let swept = store.sweep_expired_tokens().await.unwrap();
    assert!(
        swept >= expired,
        "swept {swept} of {expired} expired tokens"
    );
    assert!(swept <= token_expiries.len() as u64);
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Not a test: dumps every key under `DUMP_ROOT` in keyspace
/// `TEST_TIKV_KEYSPACE` to `DUMP_OUT` as the fixture's `keys.ndjson` (keys
/// relative to the root, hex). How `tests/fixtures/loams-0.1.1-durable` was
/// taken; see its README.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn dump_root_as_fixture() {
    let Some(cluster) = cluster().await else {
        return;
    };
    let root = std::env::var("DUMP_ROOT").expect("DUMP_ROOT").into_bytes();
    let out = std::env::var("DUMP_OUT").expect("DUMP_OUT");
    let raw = cluster.raw().await;
    let ts = raw.current_timestamp().await.unwrap();
    let mut snapshot = raw.snapshot(
        ts,
        tikv_client::TransactionOptions::new_optimistic().read_only(),
    );
    let mut end = root.clone();
    *end.last_mut().unwrap() += 1;
    let mut lines = String::new();
    for pair in snapshot.scan(root.clone()..end, 100_000).await.unwrap() {
        let key: Vec<u8> = pair.key().clone().into();
        let rel = &key[root.len()..];
        lines.push_str(&format!(
            "{{\"k\":\"{}\",\"v\":\"{}\"}}\n",
            to_hex(rel),
            to_hex(pair.value())
        ));
    }
    std::fs::write(out, lines).unwrap();
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
