//! The poll consumer against the real poll transport, served on the real HTTP
//! gateway, and against a hand-written server for what the real one does not
//! do on demand (closing a connection).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use resonate_client::*;
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, PromiseState, PromiseValue,
    UnblockMsg, UnblockMsgData, UnblockMsgHead,
};
use resonate_core::{ResonateGateway, ResonateRouter, ResonateWorker, Unavailable};
use resonate_server_sqlite::SqliteEngine;
use resonate_sql::engine::Engine;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct NullRouter;

#[async_trait]
impl ResonateRouter for NullRouter {
    async fn route(&self, _address: &str, _msg: &Message) -> Result<(), Unavailable> {
        Ok(())
    }
}

async fn sqlite_server() -> Arc<dyn ResonateServer> {
    let server = resonate_sql::server::Server::new(
        Box::new(|debug| {
            Box::pin(async move {
                let engine = SqliteEngine::open(":memory:", 30_000, 10, true, debug)
                    .map_err(|e| Unavailable::new(format!("cannot open sqlite: {e}")))?;
                Ok(Arc::new(engine) as Arc<dyn Engine>)
            })
        }),
        Arc::new(NullRouter),
        resonate_sql::server::Options {
            server_url: "http://localhost:8001".to_string(),
            wheel_capacity: 1024,
            wheel_refresh: 30_000,
            sweep_interval: 60_000,
        },
    );
    server.init(true).await.expect("sqlite init");
    server
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The gateway with the poll transport's route on it, and the transport as
/// the worker the server's router would hand `poll://` messages to.
async fn gateway_with_poll(
    auth: Option<resonate_auth::Config>,
) -> (
    resonate_gateway_http::HttpGateway,
    Arc<dyn ResonateWorker>,
    String,
) {
    let server = sqlite_server().await;
    let routes = resonate_plugin::Routes::new();
    let plugin = &resonate_transport_http_poll::PLUGIN;
    let config = resonate_plugin::Loader::new().load();
    let worker = (plugin.configure)(
        &config.worker(&plugin.id()),
        resonate_plugin::WorkerDependencies::new(Arc::downgrade(&server), Arc::clone(&routes)),
    )
    .expect("poll transport configures")
    .expect("poll transport is on by default");

    let port = free_port();
    let gw = resonate_gateway_http::HttpGateway::new(
        server,
        routes,
        resonate_gateway_http::Config {
            bind: format!("127.0.0.1:{port}"),
            auth,
            ..Default::default()
        },
    );
    gw.init(false).await.expect("gateway binds");
    (gw, worker, format!("http://127.0.0.1:{port}/"))
}

fn execute(id: &str, version: i64) -> Message {
    Message::Execute(ExecuteMsg {
        kind: "execute".into(),
        head: MessageHead::new("http://localhost:8001"),
        data: ExecuteMsgData {
            task: ExecuteMsgTask {
                id: id.into(),
                version,
            },
        },
    })
}

fn unblock(id: &str) -> Message {
    Message::Unblock(UnblockMsg {
        kind: "unblock".into(),
        head: UnblockMsgHead::default(),
        data: UnblockMsgData {
            promise: PromiseRecord {
                id: id.into(),
                state: PromiseState::RejectedTimedout,
                param: PromiseValue::default(),
                value: PromiseValue::default(),
                tags: [("resonate:scope".to_string(), "global".to_string())].into(),
                timeout_at: 7,
                created_at: 1,
                settled_at: Some(7),
            },
        },
    })
}

async fn next(sub: &mut Subscription) -> PollEvent {
    tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("an event within 10 s")
        .expect("the subscription is open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_and_unblock_arrive_through_the_gateway() {
    let (gw, worker, url) = gateway_with_poll(None).await;
    let client = Client::http(&url).build().unwrap();
    let mut sub = client.poll("workers", "p1").await.expect("poll opens");
    assert_eq!((sub.group(), sub.id()), ("workers", "p1"));

    // Registered by the time `poll` returned, so this is delivered.
    worker
        .process("poll://any@workers", &execute("n.w.r:wft.1", 3))
        .await
        .expect("a connection takes it");
    match next(&mut sub).await {
        PollEvent::Message(PollMessage::Execute {
            id,
            version,
            server_url,
            ..
        }) => {
            assert_eq!((id.as_str(), version), ("n.w.r:wft.1", 3));
            assert_eq!(server_url, "http://localhost:8001");
        }
        other => panic!("expected execute, got {other:?}"),
    }

    worker
        .process("poll://any@workers", &unblock("n.w.r:to.4"))
        .await
        .expect("a connection takes it");
    match next(&mut sub).await {
        PollEvent::Message(PollMessage::Unblock { promise, .. }) => {
            assert_eq!(promise.id, "n.w.r:to.4");
            assert_eq!(promise.state, PromiseState::RejectedTimedout);
        }
        other => panic!("expected unblock, got {other:?}"),
    }

    // A group nobody consumes is not this subscription's.
    assert!(worker
        .process("poll://any@others", &execute("x", 1))
        .await
        .is_err());
    drop(sub);
    gw.stop().await.unwrap();
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn unsigned_jwt() -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let header = json!({ "alg": "HS256", "typ": "JWT" });
    let claims = json!({ "exp": exp });
    format!(
        "{}.{}.c2ln",
        b64(header.to_string().as_bytes()),
        b64(claims.to_string().as_bytes())
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_poll_route_admits_whom_the_protocol_endpoint_admits() {
    let (gw, _worker, url) = gateway_with_poll(Some(resonate_auth::Config {
        publickey: "none".into(),
        iss: None,
        aud: None,
    }))
    .await;

    let anonymous = Client::http(&url).build().unwrap();
    let refused = anonymous.poll("workers", "p1").await.unwrap_err();
    assert_eq!(refused.status(), Some(401), "{refused}");

    let client = Client::http(&url)
        .auth(|| Some(unsigned_jwt()))
        .build()
        .unwrap();
    let sub = client.poll("workers", "p2").await.expect("admitted");
    drop(sub);
    gw.stop().await.unwrap();
}

#[tokio::test]
async fn a_local_client_cannot_poll() {
    let client = Client::local(sqlite_server().await).build().unwrap();
    assert!(matches!(
        client.poll("workers", "p1").await,
        Err(Error::Unsupported(_))
    ));
}

#[tokio::test]
async fn an_empty_group_is_refused() {
    let client = Client::http("http://127.0.0.1:1/").build().unwrap();
    assert!(matches!(client.poll("", "p1").await, Err(Error::Build(_))));
}

/// A server that answers each connection with one `execute` and then closes
/// it: what a gateway restart or a proxy's idle cut looks like to a worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_connection_is_reopened_and_says_so() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (paths_tx, mut paths) = tokio::sync::mpsc::unbounded_channel::<String>();
    let server = tokio::spawn(async move {
        for n in 0..2 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                let k = sock.read(&mut buf).await.unwrap();
                if k == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..k]);
            }
            let request = String::from_utf8_lossy(&got).into_owned();
            let _ = paths_tx.send(request.lines().next().unwrap_or("").to_string());
            let body = format!(
                ": ping\n\ndata: {{\"data\":{{\"task\":{{\"id\":\"t{n}\",\"version\":1}}}},\"head\":{{\"serverUrl\":\"\"}},\"kind\":\"execute\"}}\n\n"
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}"
            );
            sock.write_all(response.as_bytes()).await.unwrap();
            let _ = sock.shutdown().await;
        }
        // Hold the port; later connections wait in the backlog.
        std::future::pending::<()>().await;
        drop(listener);
    });

    let client = Client::http(format!("http://{addr}/")).build().unwrap();
    let options = PollOptions {
        reconnect: RetryPolicy {
            max_attempts: u32::MAX,
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            deadline: Duration::MAX,
        },
        ..PollOptions::default()
    };
    let mut sub = client
        .poll_with("g", "p1", options)
        .await
        .expect("poll opens");

    let id_of = |e: PollEvent| match e {
        PollEvent::Message(PollMessage::Execute { id, .. }) => id,
        other => panic!("expected execute, got {other:?}"),
    };
    assert_eq!(id_of(next(&mut sub).await), "t0");
    assert!(matches!(
        next(&mut sub).await,
        PollEvent::Reconnected { attempts } if attempts >= 1
    ));
    assert_eq!(id_of(next(&mut sub).await), "t1");

    for _ in 0..2 {
        let line = paths.recv().await.unwrap();
        assert_eq!(line, "GET /poll/g/p1 HTTP/1.1");
    }
    drop(sub);
    server.abort();
}
