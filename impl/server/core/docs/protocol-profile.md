# The gateway-facing protocol profile

The subset of the Resonate protocol (version `2026-04-01`) that a gateway may
build on: a Temporal-protocol gateway, an Airflow executor, or anything else
that keeps its own history and uses Resonate as its durability layer. It is
the profile asked for as FW7 in Loams design 74 (§5.3, §6.2, §7.4).

Every statement here comes from the code on this branch, the specification in
`spec/`, or a run of `crates/resonate-profile`. That crate runs each pattern
below as a scenario through the `ResonateServer` port against every store, so
the profile and the stores cannot drift apart without a test failing. Where
stores differ, this document says so and the tests pin each store's answer.

Stores, as named below:

| name | server | run by the tests |
|---|---|---|
| oracle | the in-memory reference model (`resonate-oracle`) | always |
| SQLite | the SQL server (`resonate-sql`) over `resonate-server-sqlite` | always |
| blob | `resonate-server-blob` over an in-memory object store | always |
| Postgres | the SQL server over `resonate-server-postgres` | with `TEST_POSTGRES_URL` |
| MySQL/TiDB | the SQL server over `resonate-server-mysql` | with `TEST_MYSQL_URL` (not run for FW7) |
| S3 | `resonate-server-blob` over a bucket | not run (no bucket); from the code |
| TiKV | `resonate-server-blob` over `resonate_server_blob::tikv` (FW1) | with feature `tikv` and `TEST_TIKV_PD` (§11) |

Contents:

1. [Transport and envelope](#1-transport-and-envelope)
2. [Answer codes, in general](#2-answer-codes-in-general)
3. [Ids](#3-ids)
4. [Tags](#4-tags)
5. [Records](#5-records)
6. [Requests](#6-requests)
7. [Messages the server sends](#7-messages-the-server-sends)
8. [Patterns](#8-patterns)
9. [Store differences](#9-store-differences)
10. [What this changes in design 74](#10-what-this-changes-in-design-74)
11. [Running the scenarios](#11-running-the-scenarios)

---

## 1. Transport and envelope

One request per HTTP `POST /` on the server's HTTP gateway
(`resonate-gateway-http`), body = one JSON envelope:

```json
{
  "kind": "promise.create",
  "head": { "corrId": "c-1", "version": "2026-04-01", "auth": "<token, optional>" },
  "data": { "...": "the request's fields" }
}
```

The answer is an envelope of the same kind. The HTTP status equals
`head.status`:

```json
{ "kind": "promise.create", "head": { "corrId": "c-1", "status": 200, "version": "2026-04-01" }, "data": { "promise": { } } }
```

- `head.version` must be `2026-04-01` (`SUPPORTED_VERSIONS` in
  `resonate-core/src/types.rs`); anything else is 400.
- `head.auth` may be omitted and sent as `Authorization: Bearer <token>`
  instead; the gateway copies the header into the envelope.
- `head["resonate:debug_time"]` (ms) sets the request's `now`, and only on a
  server started with the debug flag. The tests use it; a production gateway
  never sends it.
- An envelope that does not parse, has an empty `kind`, a non-object `data`
  or another version is answered 400 at the edge
  (`types::parse_and_validate`). `ui.*` kinds are 404 on this endpoint.
- **Body size:** the endpoint reads the body with axum 0.7's `Bytes`
  extractor and sets no `DefaultBodyLimit`, so axum's default of 2 MiB
  applies. A larger body is rejected by axum before it reaches the server,
  and that answer is not an envelope. (From the code; not run.)
- The Rust client for all of this is FW3 (`resonate-client`).

## 2. Answer codes, in general

| status | meaning | data |
|---|---|---|
| 200 | done, or already done (see each request) | the record(s) |
| 300 | `task.suspend` only: an awaited promise is already settled | `{ "preload": [...] }` |
| 400 | the request is malformed or fails validation | a string |
| 401 / 403 | auth is on and the token is missing, invalid, or its `prefix` claim does not cover the id (`resonate-auth`); 403 also for search on a blob/TiKV server with search off, and for `debug.*` on a non-debug blob server | a string |
| 404 | the named promise / task / schedule does not exist | a string |
| 409 | a task is not in the state, or not at the version, the request needs | a string |
| 422 | the request is well formed but the state forbids it (not awaitable, no target) | a string |
| 500 | the store refused the write (e.g. a Postgres constraint, an index row too large) or failed | a string |
| 503 | no answer: the store is unreachable, Postgres had a serialization failure it could not retry, or the blob server lost too many compare-and-set races | a string |

Retry rules for a gateway:

- **503 and a lost connection may have been applied.** Retry with the same
  request: every request in this profile is safe to repeat (see each one),
  except `task.create` (§6.6).
- **500 is not transient** in this profile's uses. On Postgres it is what an
  id or tag that breaks the schema gets (§3.3). Do not retry it blindly.
- **Most "it was already so" cases are 200, not an error.** `promise.create`
  on an existing id, `promise.settle` on a settled promise and
  `task.heartbeat` that matches nothing all answer 200. The gateway must read
  the returned record to know what happened.

## 3. Ids

### 3.1 Shape

A promise id is a string. The protocol gives one character a meaning: the
**origin** of an id is everything before its first `:` (the whole id if it
has none). The rest is the lineage.

The origin matters to a gateway in four ways:

- **The blob and TiKV stores keep one document per origin.** Every request on
  an id is a compare-and-set of its origin's whole document, serialized by one
  actor per origin, and the whole document is rewritten on each change. An
  origin with many promises is a hot, growing object.
- `task.heartbeat` refuses (400) a request whose tasks span more than one
  origin.
- The console groups an execution by origin.
- `promise.register_callback`, `task.suspend` and `task.fence` require their
  ids to share an origin (not used in this profile).

So: **one origin per unit that is mutated together**, and never a constant
origin. Design 74 §5.4's ids (`wft:{ns}:…`, `act:{ns}:…`, `tmr:…`, `to:…`)
put every promise of a kind, across every namespace, in one origin (`wft`,
`act`, …), and have up to five `:`; see §10.

### 3.2 Characters

- Any UTF-8 string of at least one byte, **except** one containing NUL
  (`\0`), which is 400 (`validate_promise_create_data`). Run: `/`, `%`,
  space, `ä€😀`, U+0001 and `.-_~!*'()` round-trip unchanged on every store.
- **At most one `:` on Postgres.** The schema's
  `well_formed_promise_id_at_most_one_separator` check (`^[^:]*(:[^:]*)?$`)
  makes `a:b:c` a 500 there; the other stores accept it.
- `resonate:origin`, `resonate:branch`, `resonate:parent` tags, when given,
  must be a prefix of the id (400 otherwise); `resonate:origin` and
  `resonate:prefix` may not contain `:`.
- Schedule ids may not contain `:` (400).
- With auth on, a token with a `prefix` claim may only touch ids that start
  with it, and may not search. Put the tenant's namespace first in the id.

### 3.3 Length

There is no length check in the protocol. Each store has its own limit:

| store | promise id limit | what enforces it | how it fails | evidence |
|---|---|---|---|---|
| oracle | none | — | — | run: 100 000 bytes accepted |
| SQLite | none in practice (`TEXT`; SQLite's 1 GB string limit) | — | — | run: 100 000 bytes accepted |
| Postgres | **2 692 bytes** for an incompressible id | btree index rows may not exceed 2 704 bytes (`promises_pkey`, `idx_promises_origin_id`, `idx_promises_task (task_state, id)`, `promises_task_key_unique`) | 500 `index row size … exceeds btree version 4 maximum 2704` | run: 2 692 accepted, 2 693 refused. A compressible id (`aaaa…`) is compressed in the index and goes further (100 000 accepted), so the limit depends on content |
| MySQL / TiDB | **255 characters** | `promises.id VARCHAR(255)` (also `callbacks`, `listeners.promise_id`, `schedules.id`/`promise_id`, and the generated `origin_id`, `target`, `parent_id`, `branch_id`, and `listeners.address`, all `VARCHAR(255)`) | a strict-mode "data too long" error, a 500 | from `resonate-server-mysql/migrations/0001_initial.sql`; not run for FW7 |
| blob, in memory | none | — | — | run: 100 000 bytes accepted |
| blob on S3 | only the **origin** is limited: the encoded origin must fit S3's 1 024-byte key | the timer key `<prefix>t/NN/<20 digits>_<enc(origin)>` is the longest key, so `len(enc(origin)) ≤ 998 − len(prefix)`; `enc` keeps `A–Z a–z 0–9 . -` and writes every other byte as `%XX` (3 bytes) | the bucket refuses the write | from `resonate-server-blob/src/applier.rs` (`KeySpace`, `encode_key`); not run |
| TiKV (FW1) | origin as for S3 but against TiKV's key limit; the lineage is inside the document | the key is `root ‖ "resonate/blob/" ‖ <object key>` | not measured | from FW1's `tikv/store.rs`; not run |

**Profile limit: 255 bytes**, as ASCII, for every promise id, schedule id,
target and listener address. That fits every store (it is MySQL's limit,
and far under Postgres's), and the scenario tests check that a 255-byte id,
root or `origin:lineage`, targeted or not, is accepted everywhere. A gateway
that needs more hashes (design 74's `h` + 32 hex rule, with 255 as the limit).

Other limits, all from the code:

- `timeoutAt` ≥ 0 (400 otherwise); it is an absolute time in ms since the
  epoch, not a duration.
- `ttl` ≥ 1 on `task.create` and `task.acquire` (400 otherwise).
- `limit` on searches: 1–1000 (400 otherwise); default 100 for promises and
  tasks, 10 for schedules.
- `param.data` and `value.data`: no server limit beyond the 2 MiB body; a
  1 MiB `param` was accepted by every store in the run. On blob and TiKV it
  also counts towards its origin's document, which is rewritten whole on each
  change.

## 4. Tags

Tags are a string-to-string map. Keys starting with `resonate:` are the
server's; any other key (`loams:ns`, `loams:run`, `loams:kind`, `loams:wf`,
`traceparent`) is stored and returned untouched and can be searched on.

| tag | value | effect |
|---|---|---|
| `resonate:target` | an address (any URI with a scheme, `url::Url::parse`) | the promise **has a task**: created `pending` at version 0 and dispatched (`execute`) to this address. Invalid address: 400 |
| `resonate:timer` | exactly `"true"` | at its deadline the promise **resolves** (not `rejected_timedout`). Any other value (`"1"`, `"false"`) is not a timer (run) |
| `resonate:scope` | `"global"` | makes the promise **external** (see below) |
| `resonate:external` | `"true"` | makes the promise external |
| `resonate:delay` | an absolute time in ms, as a decimal string | hold back the first dispatch of the promise's task until then (§6.1); requires `resonate:target`, must be < `timeoutAt`, ≥ 0 and an integer (400 otherwise); forbidden in `task.create` (400). **Ignored by the SQL stores** (§9) |
| `resonate:origin`, `resonate:branch`, `resonate:parent`, `resonate:prefix` | ids | lineage bookkeeping for the SDKs; validated as in §3.2. `resonate:branch` also drives `preload` in task answers |
| `resonate:schedule` | the schedule id | set by the server on promises a schedule creates |

**External** (`resonate_core::types::otype`): a promise is external if it has
`resonate:target`, `resonate:timer = "true"`, `resonate:scope = "global"` or
`resonate:external = "true"`. Only an external promise **arms its deadline**
(fires at `timeoutAt` and notifies its listeners) and may take a listener.
An internal one expires lazily, when it is next read, and a listener on it
is 422. **Every promise the gateway waits on must be external.**

A timer may not also have a target on Postgres
(`well_formed_promise_timer_not_targeted`: 500); the other stores accept the
pair. Do not combine them.

## 5. Records

**Promise** (`PromiseRecord`):

```json
{
  "id": "n1.order-42.r1:tmr.9",
  "state": "pending | resolved | rejected | rejected_canceled | rejected_timedout",
  "param": { "headers": { "k": "v" }, "data": "<string>" },
  "value": { "headers": { }, "data": "<string>" },
  "tags": { "resonate:timer": "true" },
  "timeoutAt": 1700000005000,
  "createdAt": 1700000000000,
  "settledAt": 1700000005000
}
```

- `param` and `value` omit `headers`/`data` when unset; a pending promise
  has `value: {}`.
- `settledAt` is absent while pending. A promise settled by its deadline has
  `settledAt = timeoutAt` and an empty value.
- A promise created with `timeoutAt ≤ now` is created settled, with
  `createdAt = settledAt = timeoutAt` (run).
- **What every answer shows is the promise as it is at `now`**: a pending
  promise past its deadline reads as expired even if the store has not
  written the expiry yet (`PromiseRecord::project`).

**Task** (`TaskRecord`): one per promise with a target, same id.

```json
{ "id": "n1.order-42.r1:act.5.1", "state": "acquired", "version": 1, "resumes": 0, "ttl": 10000, "pid": "gw-a" }
```

- `state`: `pending | acquired | suspended | halted | fulfilled`.
- `version` counts claims: 0 at create, +1 on each acquire. `ttl` and `pid`
  are present only while `acquired`.
- A task is `fulfilled` as soon as its promise is settled, by anyone.

**Schedule** (`ScheduleRecord`):

```json
{
  "id": "n1.sched-a", "cron": "* * * * *",
  "promiseId": "n1.sched-a.{{.timestamp}}", "promiseTimeout": 60000,
  "promiseParam": { "data": "e30=" }, "promiseTags": { "resonate:target": "poll://any@gw-sched" },
  "createdAt": 1700000000000, "nextRunAt": 1700000040000, "lastRunAt": 1699999980000
}
```

## 6. Requests

Each section gives the request `data`, every status it can answer, and when.
Statuses common to all (401/403 auth, 500, 503) are in §2 and not repeated.

### 6.1 `promise.create`

```json
{ "id": "n1.order-42.r1:tmr.9", "timeoutAt": 1700000005000,
  "param": { "headers": {}, "data": "aW5wdXQ=" },
  "tags": { "resonate:timer": "true", "loams:kind": "tmr" } }
```

`param` and `tags` may be omitted (empty).

| status | when | data |
|---|---|---|
| 200 | created | `{ "promise": <record> }`, `pending`, `createdAt = now` |
| 200 | created with `timeoutAt ≤ now` | settled (`resolved` if a timer, else `rejected_timedout`), `createdAt = settledAt = timeoutAt`; a target's task is created `fulfilled` |
| 200 | **the id exists, whatever its state and whatever this request's param, tags and deadline** | `{ "promise": <the stored record> }`. Nothing is changed and nothing is dispatched. There is no conflict answer: compare the record with what you sent |
| 400 | missing/empty `id`, NUL in `id`, missing or negative `timeoutAt`, invalid `resonate:target` address, any tag rule in §3.2 and §4 broken | string |
| 500 | Postgres: id with two `:`, timer with a target, id or indexed tag over the index limit; MySQL: over 255 | string |

Side effects of a create with a target: the task is created `pending` at
version 0, and unless `resonate:delay` is in the future, an `execute`
message goes to the target at once. A pending task's dispatch is repeated
every `retry_timeout` (§7.2) until someone acquires it.

`resonate:delay` (oracle, blob, TiKV; the spec's `createPromise`): the task
is still created `pending` at once; only the first `execute` is held back
until the delay. **Delay holds back dispatch, not claiming:** a caller that
knows the id can `task.acquire` it at version 0 before the delay (run), and
then nothing is dispatched at the delay. A delay already past dispatches at
once.

### 6.2 `promise.get`

```json
{ "id": "n1.order-42.r1:tmr.9" }
```

| status | when |
|---|---|
| 200 | `{ "promise": <record as of now> }` |
| 400 | empty id |
| 404 | no such promise |

### 6.3 `promise.settle`

```json
{ "id": "n1.order-42.r1:sig.approve.1", "state": "resolved", "value": { "headers": {}, "data": "eWVz" } }
```

`state` ∈ `resolved | rejected | rejected_canceled`. Only the server sets
`rejected_timedout`.

| status | when | data |
|---|---|---|
| 200 | the promise was pending (or expired just now: see below) | the settled record, `settledAt = now` |
| 200 | **already settled** (by anyone, including its deadline) | the stored record, unchanged. The first settle wins; nothing is sent |
| 400 | empty id, unknown state (including `rejected_timedout`) | string |
| 404 | no such promise | string |

A settle at or after the deadline finds the promise already expired: the
answer is the expired record (`resolved` for a timer, `rejected_timedout`
otherwise). Settling a promise with a task fulfils the task; its holder's
next `task.fulfill` is 409.

Effects of a settle that lands: every listener gets one `unblock` (§7.1),
the promise's task (if any) becomes `fulfilled`, and callbacks resume their
awaiters.

### 6.4 `promise.register_listener`

```json
{ "awaited": "n1.order-42.r1:tmr.9", "address": "poll://any@gw" }
```

| status | when | data |
|---|---|---|
| 200 | the promise is pending: the listener is stored (once per address; a repeat is 200 and changes nothing) | the record |
| 200 | the promise is settled: nothing is stored and **nothing is sent** | the settled record. The caller learns the outcome from this answer |
| 400 | empty field, or `address` is not a URI with a scheme | string |
| 404 | no such promise | string |
| 422 | the promise is internal (§4) | string |

A gateway registers a listener and **reads the answer**: if the state is not
`pending`, that is the event, and no message will follow.

### 6.5 `promise.search`

```json
{ "state": "pending", "tags": { "loams:run": "r1" }, "limit": 100, "cursor": "<from the last page>" }
```

All fields optional. 200 `{ "promises": [...], "cursor": "<next>" }` sorted
by id, `cursor` absent on the last page; 400 for a bad `limit`. The state
filter reads the state as of now. On blob and TiKV each search reads every
document in the store (FW6 adds an index); blob's plugin answers 403 unless
`search_enabled` is set (TiKV's plugin turns it on by default). Not for a hot
path.

### 6.6 `task.create` (create and claim in one step)

```json
{ "pid": "gw-a", "ttl": 10000,
  "action": { "kind": "promise.create", "head": {},
              "data": { "id": "n1.order-42.r1:wft.3", "timeoutAt": 1700000600000, "param": {},
                        "tags": { "resonate:target": "poll://any@ltq.0.q" } } } }
```

Creates the promise and its task **already acquired by the caller**
(`version: 1`, `pid`, `ttl`); no `execute` is sent. This is for a worker
that starts its own work. It is not how a gateway hands work to a poller:
that is `promise.create` with a target (§8.2).

| status | when | data |
|---|---|---|
| 200 | created: task `acquired` v1 | `{ "task", "promise", "preload" }` |
| 200 | the task exists and is `pending` (e.g. its lease lapsed): **claimed** at version + 1 | same |
| 200 | the task exists and is `fulfilled` | the fulfilled task and settled promise |
| 200 | created with `timeoutAt ≤ now` | task `fulfilled` v0, promise expired |
| 400 | no `resonate:target`, a `resonate:delay`, `ttl < 1`, empty `pid`, or any `promise.create` rule | string |
| 409 | the task exists and is `acquired`, `suspended` or `halted`, **including by the same caller**: a retried `task.create` whose first attempt landed is 409 | string |
| 422 | the promise exists without a target | string |

### 6.7 `task.get`

```json
{ "id": "n1.order-42.r1:act.5.1" }
```

200 `{ "task": <record> }` (as of now: a task whose promise has settled or
expired reads `fulfilled`); 400 empty id; 404 no such task (also for a
promise without a target, e.g. a timer).

### 6.8 `task.acquire`

```json
{ "id": "n1.order-42.r1:act.5.1", "version": 0, "pid": "gw-a", "ttl": 10000 }
```

`version` is the one in the `execute` message (or `task.get`). The lease
runs to `now + ttl`.

| status | when | data |
|---|---|---|
| 200 | the task was `pending` at this version | `{ "task": acquired, version + 1, "promise", "preload" }` |
| 400 | empty id/pid, `ttl < 1`, negative version | string |
| 404 | no such task | string |
| 409 | not `pending` (held, suspended, halted, fulfilled), or another version | string |

`preload` lists up to `preload_limit` (default 10) other promises with the
same `resonate:branch` tag; empty without one.

### 6.9 `task.heartbeat`

```json
{ "pid": "gw-a", "tasks": [ { "id": "n1.order-42.r1:act.5.1", "version": 1 } ] }
```

For each listed task that is `acquired` by this `pid` at this version, and
whose promise has not passed its deadline, the lease moves to `now + ttl`
(the ttl given at acquire).

| status | when | data |
|---|---|---|
| 200 | always, once valid: **including when it extends nothing** (wrong pid, old version, lease already lost, task fulfilled or unknown) | `{}` |
| 400 | empty `pid`, empty `tasks`, or tasks from more than one origin | string |

A heartbeat cannot tell the holder it has lost the lease. The holder learns
it from `task.fulfill` (409) or `task.get`. One heartbeat request per
origin (so per run, with this profile's ids).

### 6.10 `task.fulfill`

```json
{ "id": "n1.order-42.r1:act.5.1", "version": 2,
  "action": { "kind": "promise.settle", "head": {},
              "data": { "id": "n1.order-42.r1:act.5.1", "state": "resolved", "value": { "data": "cmVzdWx0" } } } }
```

Settles the task's own promise under the lease.

| status | when | data |
|---|---|---|
| 200 | `acquired` at this version: promise settled, task `fulfilled` | `{ "promise": <settled record> }` |
| 200 | `acquired` at this version but the promise had already settled | the stored record; task becomes `fulfilled` |
| 400 | `action.data.id ≠ id`, bad state, empty id | string |
| 404 | no such task, or no such promise | string |
| 409 | not `acquired` at this version: lease lost and re-acquired or released, already fulfilled, or the promise was settled by someone else (cancel) | string |

### 6.11 `task.release`

```json
{ "id": "n1.order-42.r1:act.6.1", "version": 1 }
```

Gives a held task back: `pending` at the **same** version, `execute` sent
to the target at once, and again every `retry_timeout` until acquired.

200 `{}`; 400 bad fields; 404 no such task; 409 not `acquired` at this
version.

### 6.12 `schedule.create`, `schedule.get`, `schedule.delete`, `schedule.search`

```json
{ "id": "n1.sched-a", "cron": "* * * * *",
  "promiseId": "n1.sched-a.{{.timestamp}}", "promiseTimeout": 60000,
  "promiseParam": { "data": "e30=" },
  "promiseTags": { "resonate:target": "poll://any@gw-sched", "loams:kind": "start" } }
```

- `cron`: 5 fields (`min hour dom month dow`, seconds = 0) or 6–7 fields
  (`sec min hour dom month dow [year]`), in **UTC** (`util::compute_next_cron`).
- `promiseId`: a template; `{{.id}}` is replaced by the schedule id and
  `{{.timestamp}}` by the run's nominal time in ms. Keep the result within
  §3 (no second `:` on Postgres).
- `promiseTags` **must** contain `resonate:target`: every run is a task
  dispatched to that address. To be told of runs, the gateway is the target.
- Each run creates promise `promiseId` with `createdAt` = the nominal time,
  `timeoutAt` = nominal + `promiseTimeout`, the schedule's param, and its tags
  plus `resonate:schedule = <schedule id>` and `resonate:origin`,
  `resonate:branch`, `resonate:parent`, `resonate:prefix` = the promise id.
  An id that already exists is left alone (idempotent per nominal time).

| request | status | when |
|---|---|---|
| `schedule.create` | 200 | created: `nextRunAt` = the first cron time after now, `createdAt` = now |
| | 200 | **the id exists**: the stored schedule, unchanged, whatever this request says |
| | 400 | `:` in the id, invalid cron, no `resonate:target` in `promiseTags`, `:` in a `resonate:origin`/`resonate:prefix` tag, negative `promiseTimeout`, empty fields |
| | 500 | Postgres: `promiseTags` with a timer and a target (schema check `well_formed_schedule_promise_tags_not_timer_targeted`; from the schema, not run) |
| `schedule.get` `{ "id" }` | 200 / 404 | `{ "schedule" }` / no such schedule |
| `schedule.delete` `{ "id" }` | 200 `{}` / 404 | deleted, no further runs / no such schedule (also a second delete) |
| `schedule.search` `{ "tags", "limit", "cursor" }` | 200 / 400 | by `promiseTags`; same caveats as `promise.search` on blob/TiKV |

**Missed runs differ by store** (run, a tick three runs late with
`promiseTimeout` = one period):

| store | what a late sweep does |
|---|---|
| oracle | creates every missed run; those past `promiseTimeout` are created `rejected_timedout`; dispatches all of them |
| blob (and TiKV, same server) | creates every missed run; those past `promiseTimeout` are created `rejected_timedout` and **not** dispatched; the live one is dispatched |
| SQLite, Postgres (MySQL: same SQL server) | creates **one** missed run per sweep, the oldest; if it is past `promiseTimeout` it is created `rejected_timedout` and not dispatched; `nextRunAt` advances one period per sweep, so a backlog drains one run per sweep |

So the profile does not rely on catch-up. A gateway that needs Temporal's
catch-up window sets `promiseTimeout` to it and treats a
`rejected_timedout` run as skipped.

### 6.13 Not in the profile

`promise.register_callback`, `task.suspend`, `task.fence`, `task.halt` and
`task.continue` are the SDKs' requests for running code inside Resonate.
A gateway that keeps its own history does not need them, and they carry
same-origin rules (§3.1). `debug.*` exists only on a debug server.

## 7. Messages the server sends

Messages go through the router to a worker chosen by the address scheme:
`poll://` (workers that hold an SSE connection on `GET /poll/{group}/{id}`;
`poll://any@<group>` reaches one connection of the group,
`poll://uni@<group>/<id>` one named connection), `http(s)://` (push), and
others per plugin. Many consumers sharing one group is FW8.

### 7.1 Shapes

```json
{ "kind": "execute", "head": { "serverUrl": "<the server's URL>" },
  "data": { "task": { "id": "n1.order-42.r1:act.5.1", "version": 0 } } }

{ "kind": "unblock", "head": {},
  "data": { "promise": <the settled record> } }
```

- `execute` goes to the task's `resonate:target` when a task becomes
  `pending`: at create (or at its delay), on `task.release`, on lease
  expiry, and every `retry_timeout` while it stays pending. Its `version` is
  the one to acquire with.
- `unblock` goes to each listener address once, when the promise settles
  (by a settle, a `task.fulfill`, or its deadline). The listeners are then
  dropped.

### 7.2 Delivery

- **At most once.** Every store routes a message once, after its commit,
  and drops it if delivery fails (`resonate-sql/src/server.rs` `deliver`;
  `resonate-server-blob/src/sender.rs`: "a lost `Execute` is recovered by
  the task's retry deadline; a lost `Unblock` is lost").
- So an `execute` is retried by the server (every `retry_timeout`: default
  30 s on SQLite, Postgres, MySQL and the TiKV plugin, 60 s on the blob
  plugin; the tests set 30 s everywhere). **An `unblock` is not.** A gateway
  that relies on listeners also needs a backstop: re-read with
  `promise.get` the promises it is waiting on after a reconnect, and on a
  timer of its own.
- Deadlines fire from the server's own timer. On the SQL stores an armed
  deadline is held in an in-memory wheel (`wheel_capacity`, default 8192)
  refreshed from the database every `wheel_refresh` (30 s), with a sweep
  every `sweep_interval` (60 s) as the backstop; a deadline armed by another
  instance may be seen up to `wheel_refresh` late. Lateness is measured by
  FW10's bench.

## 8. Patterns

The rows of design 74 §5.3, as this profile does them. Ids use the grammar
of §8.0. Each pattern is a scenario in `crates/resonate-profile/tests/scenarios.rs`
(named in brackets) and passes on every store the run reached.

### 8.0 Ids for a gateway

```
<run>      = <ns>.<wf>.<run-id>                  the origin: no ':'
<promise>  = <run>:<kind>.<event>[.<attempt|timeout-kind>]
start      = <run>                               the workflow's own promise
schedule   = <ns>.<schedule-id>                   no ':'; runs <ns>.<schedule-id>.{{.timestamp}}
```

- One origin per workflow run: everything of a run serializes on one blob
  document and nothing else does, and one `task.heartbeat` covers a run.
- Exactly one `:` on Postgres, none in schedule ids.
- `<wf>` percent-encoded so that `:` and `.` cannot appear raw (`%3A`,
  `%2E`); `<ns>` the namespace id first, for token prefixes.
- 255 bytes at most (§3.3); past that, hash `<wf>` and keep the full id in a
  tag.

### 8.1 Workflow start, idempotent start [`workflow_start_is_an_idempotent_promise`]

1. `promise.create { id: <run>, timeoutAt: <run deadline>, param, tags: { "resonate:scope": "global", ... } }` → 200 `pending`.
2. A retry of the same request: 200 with the identical record.
3. A create with other param/tags/deadline under the same id: 200 with the
   **stored** record. Detecting the conflict is the gateway's (compare).
4. `promise.register_listener { awaited: <run>, address }` → 200; the
   completion arrives as `unblock`.
5. Completion: `promise.settle` → 200, listeners get `unblock` with the
   value. A start after completion answers with the completed record.

### 8.2 Work for a poller: workflow task, activity [`activity_is_a_task_with_a_lease`, `release_redispatches_at_once_and_then_every_retry_timeout`]

1. `promise.create { id: <run>:act.5.1, timeoutAt: <schedule-to-close>, tags: { "resonate:target": "poll://any@<queue>" } }`
   → 200; `execute { id, version: 0 }` to the queue.
2. The poller's claim: `task.acquire { id, version: 0, pid, ttl: <start-to-close or heartbeat timeout> }` → 200 v1. A second claimant: 409.
3. `task.heartbeat { pid, tasks: [{ id, version: 1 }] }` → 200, lease to `now + ttl`.
4. Lease expiry: the task returns to `pending` **at the same version** (1),
   and `execute { version: 1 }` is sent again. The old holder's
   `task.fulfill` is 409; its heartbeat is still 200 and does nothing.
5. Another claimant: `task.acquire { version: 1 }` → 200 v2.
6. `task.fulfill { version: 2, action: settle resolved|rejected }` → 200;
   task `fulfilled`; a second fulfill or acquire is 409.
7. `task.release { version }` → `pending`, same version, `execute` at once.

Design 74 says the gateway "never lets Resonate re-deliver the same
attempt" of a workflow task. Resonate will re-deliver on lease expiry
whatever the gateway intends; the gateway prevents it by settling the
attempt's promise (which fulfils the task, so no further `execute`) when it
times the attempt out, and creating the next attempt's promise.

### 8.3 Timer [`timer_resolves_at_its_deadline_and_unblocks_its_listener`]

1. `promise.create { id: <run>:tmr.9, timeoutAt: <fire time>, tags: { "resonate:timer": "true" } }` → 200 `pending`; nothing dispatched; `task.get` 404.
2. `promise.register_listener { awaited, address }` → 200.
3. At `timeoutAt` the promise **resolves**, `settledAt = timeoutAt`, empty
   value, and each listener gets `unblock`. Before it, nothing.
4. A listener registered after the fire: 200 with the resolved record, no
   message.

### 8.4 Timeout (schedule-to-start, start-to-close, heartbeat, …) [`timeout_rejects_timedout_at_its_deadline`]

A deadline that **rejects** is a promise that is external but not a timer:
`tags: { "resonate:scope": "global" }`. At `timeoutAt` it becomes
`rejected_timedout` (`settledAt = timeoutAt`) and listeners get `unblock`.
A read at or after the deadline sees `rejected_timedout` even before the
sweep. A deadline already past at create is created `rejected_timedout`.

(Design 74's `to:` promises carry `resonate:timer` and expect
`rejectedTimedout`; with that tag they resolve. Use one or the other: §10.)

### 8.5 Retry with backoff [`retry_with_backoff_is_a_delayed_create`, `delay_holds_back_dispatch_but_not_acquire`]

1. Attempt *n* fails: `task.fulfill … state: "rejected"`.
2. Attempt *n+1*: `promise.create { id: <run>:act.5.<n+1>, tags: { "resonate:target": …, "resonate:delay": "<now + backoff, absolute ms>" } }`.
   On oracle/blob/TiKV: `pending`, no `execute` until the delay, then
   `execute { version: 0 }`.
3. `resonate:delay` must be < `timeoutAt`, an integer ≥ 0, with a target.
4. **On the SQL stores the delay is ignored and the attempt is dispatched at
   create** (§9). Until that is fixed, a gateway on SQLite or Postgres does
   backoff with a timer promise (§8.3) and creates attempt *n+1* when the
   timer's `unblock` arrives (or its own backstop finds it resolved).

### 8.6 Signal [`signal_is_a_settle_of_a_waited_promise`]

Design 74 keeps signals out of Resonate (they are history events). Where a
gateway does wait on an outside event with Resonate, it is an external
promise with a listener, and the signal is `promise.settle`: 200, every
listener gets `unblock` with the value. A second settle is 200 with the
first value (the first wins) and sends nothing.

### 8.7 Cancel [`cancel_is_a_settle_rejected_canceled`]

- Cancel a timer or any pending promise: `promise.settle { state: "rejected_canceled" }` → 200; listeners get `unblock`; the deadline later does nothing.
- A cancel that loses to the deadline (or any earlier settle) is 200 with
  the stored record (`resolved` for a fired timer). Read the state.
- Cancel a running activity: settle its promise `rejected_canceled`. The task
  becomes `fulfilled`, nothing is redelivered; the holder's heartbeat is 200
  (no effect), its `task.fulfill` is 409.
- Terminate: the same settle on every outstanding promise of the run.

### 8.8 Schedules [`schedule_fires_start_promises`]

`schedule.create` with the gateway as target. Each run is a task promise
`<ns>.<schedule-id>.<nominal ms>` dispatched to the gateway, which acquires
it (or just reads the id from the `execute`), starts the workflow, and
fulfils it. See §6.12 for missed runs.

### 8.9 `task.create` [`task_create_claims_in_one_step`]

Only for a caller that does the work itself: 200 acquired v1, no dispatch;
retried while held, 409; after the lease lapses, it claims (v2); after
completion, 200 with the fulfilled task.

## 9. Store differences

Found by running the scenarios on oracle, SQLite, blob (in memory) and
Postgres 17, on this branch:

| behaviour | oracle | SQLite | Postgres | blob | MySQL/TiDB (code) |
|---|---|---|---|---|---|
| `resonate:delay` holds back the first dispatch (as the spec says) | yes | **no: dispatched at create** | **no** | yes | no (same SQL server; no delay handling in `resonate-server-mysql`) |
| id with two or more `:` | 200 | 200 | **500** | 200 | 200 |
| `resonate:timer` + `resonate:target` | 200 | 200 | **500** | 200 | 200 |
| promise id length | none | none | 2 692 B incompressible | none (S3: origin ≤ 998 − prefix encoded bytes) | 255 |
| missed schedule runs on a late sweep | all, all dispatched | one per sweep | one per sweep | all, live one dispatched | one per sweep |
| `retry_timeout` default | (30 s in the model) | 30 s | 30 s | 60 s (TiKV plugin: 30 s) | 30 s |
| searches | yes | yes | yes | off unless `search_enabled` (403) | yes |

Everything else in §6 was the same on every store in the run.

The `resonate:delay` gap is a divergence from the specification
(`createPromise` in `spec/spec/02-abstract/state.lean` arms the task's
first retry at `max(delay, now)`); the SQL engines never read the tag. The
port differential does not catch it because its generator never sends the
tag. It is a fork issue to fix in the SQL engines (separate change).

## 10. What this changes in design 74

For DX1 Task 0:

1. **Ids (§5.4).** `wft:{ns}:{wf}:{run}:{eid}` and its siblings do not
   work: they have up to five `:` (500 on Postgres), and their origin is the
   kind (`wft`, `act`, …), so on blob/TiKV every promise of a kind across
   all namespaces is one document, and `task.heartbeat` cannot batch across
   runs anyway. Use §8.0: `<ns>.<wf>.<run>:<kind>.<event>[.<n>]`.
2. **ID limit:** 255 bytes (§3.3). Hash past it.
3. **Timeouts (`to:`).** A promise with `resonate:timer` resolves at its
   deadline; it never becomes `rejectedTimedout`. Use
   `resonate:scope: global` for a timeout that rejects, `resonate:timer` for
   a sleep, never both on one promise with a target.
4. **Workflow task scheduled** is `promise.create` with
   `resonate:target`, not `task.create`: `task.create` hands the task to its
   own caller, acquired, and sends nothing to the poller.
5. **Idempotent create** has no conflict answer: a re-create with other
   params is 200 with the stored record. The gateway compares (design 74's
   `loams_temporal_outbox_conflict_total` must be computed by the gateway).
6. **Lease expiry re-delivers** at the same version; a heartbeat never says
   the lease is lost (always 200). The gateway learns from 409 on fulfill.
7. **Listener delivery is at most once.** A lost `unblock` is not resent.
   The gateway needs a reconcile loop (`promise.get` or `promise.search` by
   `loams:run`) behind the in-memory wake-ups of §5.6.
8. **Retry backoff via `resonate:delay`** does not work on the SQL stores
   today (§9). On SQLite (light compose) use a backoff timer until the SQL
   engines are fixed.
9. **Schedules** need a target, so the gateway is the target of its own
   schedules; missed runs are store-dependent (§6.12).

## 11. Running the scenarios

```text
cd impl/server/core
cargo test -p resonate-profile                                   # oracle, SQLite, blob

podman run -d --name pg -e POSTGRES_USER=resonate -e POSTGRES_PASSWORD=resonate \
  -e POSTGRES_DB=resonate -p 127.0.0.1:25432:5432 postgres:17
TEST_POSTGRES_URL=postgres://resonate:resonate@127.0.0.1:25432/resonate \
  cargo test -p resonate-profile                                 # + Postgres

TEST_BACKENDS=sqlite,postgres ...                                # only these
TEST_TRACE_DIR=/path ...                                         # NDJSON traces
```

TiKV (FW1's store): start the playground and run with the `tikv` feature

```text
test/tikv/up.sh 23790 20260
TEST_TIKV_PD=127.0.0.1:23790 cargo test -p resonate-profile --features tikv
test/tikv/down.sh
```

(`TEST_TIKV_KEYSPACE` defaults to `resonate_profile`; each run uses a fresh
root).

Traces: with `TEST_TRACE_DIR`, each scenario writes
`<scenario>.<store>.ndjson` in the `spec/valid` format (one line per protocol
request: `kind`, `now`, `req`, `res`; no `debug.*`, no searches). They are
meant for `lake exe checktrace` (`spec/valid/lean`) and
`go run ./cmd/lincheck` (`spec/valid/porc`).
