# resonate-client

A typed Rust client for the Resonate protocol (`2026-04-01`).

One `Client` type, two transports:

- **HTTP**: `POST` of one envelope to a gateway's protocol endpoint
  (`resonate-gateway-http`'s `POST /`, or wherever an embedder mounted
  `protocol_router`).
- **In process**: a call on an `Arc<dyn ResonateServer>`.

Above the transport, both get the same typed methods, retries, auth hook,
correlation ids and observation. `Client` also implements `ResonateServer`,
so it plugs in wherever the port is taken, including behind another gateway.

The crate depends on `resonate-core` for the records and the port, `reqwest`
(rustls, no default features), `tokio`, `serde`, `serde_json` and
`async-trait`. It does not depend on any engine.

## Use

```toml
[dependencies]
resonate-client = { git = "https://github.com/ostrium-labs/resonate", rev = "<commit>" }
```

```rust
use std::time::Duration;
use resonate_client::{
    Client, PromiseCreate, PromiseSettle, RetryPolicy, SettleState,
};

let client = Client::http("http://localhost:8001/")
    .auth(|| Some(mint_short_lived_token()))  // per attempt
    .timeout(Duration::from_secs(5))           // per attempt
    .retry(RetryPolicy::default())             // 6 attempts within 30 s
    .build()?;

// 200 whether the promise is new or already existed.
let p = client
    .promise_create(&PromiseCreate::new("order-42", deadline_ms).data("e30="))
    .await?;

client
    .promise_settle(&PromiseSettle::new("order-42", SettleState::Resolved))
    .await?;

match client.promise_get("order-41").await {
    Err(e) if e.is_not_found() => {}
    other => { other?; }
}
```

In process, over a server you already hold:

```rust
let server: Arc<dyn ResonateServer> = /* the engine */;
let client = Client::local(server).build()?;
```

A worker's lease loop:

```rust
let claim = client
    .task_create("worker-1", 30_000, &PromiseCreate::new("wf", deadline).target("poll://any@workers"))
    .await?;
let (id, v) = (claim.data.task.id.clone(), claim.data.task.version);
client.task_heartbeat("worker-1", &[(&id, v)]).await?;
client
    .task_fence(&id, v, &FenceAction::Create(PromiseCreate::new("wf:1", deadline)))
    .await?;
match client.task_suspend(&id, v, &["wf:1"]).await?.data {
    Suspension::Suspended => { /* wait for an execute message */ }
    Suspension::Resume { preload } => { /* carry on with the lease */ }
}
client.task_fulfill(&id, v, SettleState::Resolved, PromiseValue::default()).await?;
```

## Methods

One per request kind (`resonate_client::KINDS` lists them):

| kind | method | success data |
|---|---|---|
| `promise.get` | `promise_get(id)` | `PromiseRecord` |
| `promise.create` | `promise_create(&PromiseCreate)` | `PromiseRecord` |
| `promise.settle` | `promise_settle(&PromiseSettle)` | `PromiseRecord` |
| `promise.register_callback` | `promise_register_callback(awaited, awaiter)` | `PromiseRecord` (awaited) |
| `promise.register_listener` | `promise_register_listener(awaited, address)` | `PromiseRecord` |
| `promise.search` | `promise_search(&PromiseSearch)` | `Page<PromiseRecord>` |
| `task.get` | `task_get(id)` | `TaskRecord` |
| `task.create` | `task_create(pid, ttl, &PromiseCreate)` | `TaskClaim` |
| `task.acquire` | `task_acquire(id, version, pid, ttl)` | `TaskClaim` |
| `task.release` | `task_release(id, version)` | `()` |
| `task.fulfill` | `task_fulfill(id, version, SettleState, PromiseValue)` | `PromiseRecord` |
| `task.suspend` | `task_suspend(id, version, &[awaited])` | `Suspension` (`200` suspended, `300` resume) |
| `task.fence` | `task_fence(id, version, &FenceAction)` | `Fenced` |
| `task.heartbeat` | `task_heartbeat(pid, &[(id, version)])` | `()` |
| `task.halt` | `task_halt(id)` | `()` |
| `task.continue` | `task_continue(id)` | `()` |
| `task.search` | `task_search(&TaskSearch)` | `Page<TaskRecord>` |
| `schedule.get` | `schedule_get(id)` | `ScheduleRecord` |
| `schedule.create` | `schedule_create(&ScheduleCreate)` | `ScheduleRecord` |
| `schedule.delete` | `schedule_delete(id)` | `()` |
| `schedule.search` | `schedule_search(&ScheduleSearch)` | `Page<ScheduleRecord>` |
| `debug.reset` / `debug.snap` / `debug.tick` | `debug_reset()`, `debug_snap()`, `debug_tick(time)` | servers in debug mode only |

`send(kind, data)` sends anything else and returns the envelope whatever
its status.

## Answers

Every method returns `Result<Response<T>, Error>`. `Response` carries
`status`, `corr_id`, the typed `data`, and `attempts`.

- `Error::Rejected { status, message, attempts, .. }`: the server said no
  (`400`, `401`, `403`, `404`, `409`, `422`, `500`). `is_not_found()`,
  `is_conflict()`, `is_unprocessable()` name the common ones.
- `Error::Unavailable { attempts, message, .. }`: no answer within the retry
  policy. The request **may have been applied**.
- `Error::Auth`, `Error::Protocol`, `Error::Build`: the token source failed,
  the peer did not answer with a protocol response for this request, or the
  client could not be built.

What the server answers (SQLite, checked by `tests/roundtrip.rs`):

- `promise.create` on an existing id: `200` and the existing record,
  unchanged. There is no `201`.
- `promise.settle` on a settled promise: `200` and the record as it is.
- `task.create` on an acquired task: `409`; on a fulfilled one: `200` and the
  task as it is; on a promise without a task: `422`.
- `task.acquire`, `task.release`, `task.fulfill`, `task.fence` against a task
  not held at that version: `409`.
- `task.halt` on a halted task: `200`; on a fulfilled one: `409`.
  `task.continue` on a task that is not halted: `409`.
- `promise.register_listener` on an internal promise: `422`;
  `promise.register_callback` with a missing awaiter: `422`.

## Retries and timeouts

`timeout` bounds one attempt (default 10 s); `connect_timeout` bounds opening
an HTTP connection (default 5 s). `RetryPolicy { max_attempts, base, cap,
deadline }` retries connection failures, timeouts, `503` and a proxy's bare
`502`/`504`, waiting a uniformly random time up to
`min(cap, base * 2^(n-1))` between attempts, and never past `deadline`.
`RetryPolicy::none()` turns retries off. An answer is never retried.

Retrying assumes idempotence, which is the protocol's contract for
`Unavailable`: an attempt that got no answer may have been applied. For
`task.acquire`, `task.fulfill`, `task.suspend` and `task.fence` a repeat of an
applied attempt answers `409`; `attempts > 1` on the error says that may be
what happened.

## Auth

`.auth(source)` takes a `TokenSource`: a closure `Fn() -> Option<String>`,
`StaticToken`, or your own async implementation. It is asked once per
attempt. Over HTTP the token goes in `Authorization: Bearer`; in process it
goes in `head.auth` when the request does not carry one.

## Correlation ids

`Sequential` by default: `rc-<16 random hex>-<n>`. `.corr_ids(...)` takes any
`CorrIds`, including a closure `Fn(&str) -> String` given the kind. A retry
reuses its request's id. The client checks that the answer echoes the
request's `kind` and `corrId`.

## Traces for spec/valid

`.debug_clock(f)` stamps `resonate:debug_time` on every request, for a server
in debug mode. `.observer(Arc<dyn Observer>)` sees every answered exchange.
`trace::TraceRecorder` is an observer that writes the NDJSON
`spec/valid` reads (`{"kind","now","req","res"}` per call), so any workload
driven through a client can be checked with
`lake exe checktrace < trace.ndjson` or
`go run ./cmd/lincheck < trace.ndjson`. The test
`the_workload_records_a_spec_trace` writes one to
`$CARGO_TARGET_TMPDIR/resonate-client-trace.ndjson` (and to
`$RESONATE_CLIENT_TRACE` when set).
