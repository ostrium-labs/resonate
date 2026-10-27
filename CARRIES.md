# Carries on `loams/main`

`ostrium-labs/resonate` is Ostrium Labs' fork of
[resonatehq/resonate](https://github.com/resonatehq/resonate) (Apache-2.0).
Loams embeds the server from this fork at a pinned revision.

## Branches

- `main` mirrors `resonatehq/resonate` `main`. It only ever fast-forwards and
  never carries a commit of its own.
- `loams/main` is the integration branch: `main` plus the carries below. Loams
  pins a commit of this branch. On an upstream bump it is rebuilt as `main` plus
  each carry that is still open, in the order of this table.
- One branch per upstream change (`feat/<topic>`, `fix/<topic>`, `deps/<topic>`),
  based on `main`, holding one logical change with DCO sign-off. This is the
  branch an upstream PR is opened from.
- Fork work items (FW*, from Loams design 74 §7.4) land in `loams/main`
  through a PR each. An item that is upstreamable gets its own branch on `main`
  when it is offered upstream.
- `loam/0.10.1` is the frozen branch that Loams 0.1.x pins (`31ffb201`).
  Leave it alone until Loams moves its pin to `loams/main`. Its tree is
  identical to the first `loams/main` (2026-10-24).
- `loam/tikv-dapr` is an archive of the September TiKV/redb blob stores, the
  Dapr bridge and the kind deploy scripts. FW1 replaces its TiKV store, and
  nothing else on it is carried.

## Carries

State is as of 2026-10-24. "Upstream" means a PR on `resonatehq/resonate`.

| Carry | Branch | Upstream | State | Why it is carried |
|---|---|---|---|---|
| MySQL: classify retryable errors by error number | `fix/mysql-retryable-errno` | [#1162](https://github.com/resonatehq/resonate/pull/1162) | **Merged** 2026-10-23 (`9c72d818`) | No longer a carry: it is in `main` |
| RustSec advisories, drop OpenSSL, version internal path deps | `deps/advisories-rustls` | [#1164](https://github.com/resonatehq/resonate/pull/1164) | Open, rebased on `9c72d818` | rustls only, with no OpenSSL in Loams' images. The versioned path deps let Loams depend on the crates by git |
| `transport-http-push`: GCP ID token behind `gcp-idtoken` | `feat/push-gcp-idtoken-feature` | [#1167](https://github.com/resonatehq/resonate/pull/1167) | Open, rebased | Loams builds without `google-cloud-auth` and reqwest 0.13 |
| MySQL plugin on TiDB (session pin, errnos, xtask and CI legs) | `feat/tidb-backend` | [#1163](https://github.com/resonatehq/resonate/pull/1163) (draft) | Open, rebased onto the merged #1162. FW2 adds `tests/locks.rs` on `loams/main` (no shared locks; every session READ COMMITTED and, on TiDB, pessimistic); it goes into #1163 when the owner approves updating it | Loams' legacy durable store on TiDB (FW2) |
| MySQL: refuse ids and addresses longer than their `VARCHAR(255)` column instead of storing them truncated (FW2) | `fix/mysql-address-truncation` | Candidate, not opened | Branch ready on `main`; on `loams/main` through `fw2/address-length` | `INSERT IGNORE` downgrades "Data too long" to a warning, so a target or listener address over 255 characters was cut and its messages went to the truncated address (found by FW7 on TiDB) |
| `sdk-rs`: reqwest without default TLS | `feat/sdk-rs-reqwest-default-feature` | [#1168](https://github.com/resonatehq/resonate/pull/1168) | Open, rebased | Loams embeds the Rust SDK with rustls only |
| `resonate-auth`: jsonwebtoken 10.3 on aws-lc-rs; unsigned mode without the deprecated switch | `fix/auth-jsonwebtoken-10` | [#1197](https://github.com/resonatehq/resonate/pull/1197) | Open | GHSA-h395-gr6q-cpjc |
| xtask: resolve the target directory through `cargo metadata` | in FW0 | Not opened | Carried | `porcupine` assumed `<workspace>/target`. It breaks under a configured `build.target-dir` or `CARGO_TARGET_DIR` |
| FW1: the TiKV store (`resonate-server-blob` feature `tikv`, plugin `resonate-server-tikv`). It keeps Loams 0.1.x's on-disk layout byte for byte: `root ‖ resonate/blob/<key>` → 16-byte random revision ‖ body, with commit tokens at `root ‖ t/`. It also adds `xtask --backend tikv` and `test/tikv/` | `fw1/tikv-store` | Not opened; upstreamable (it brings `tikv-client` from Ostrium Labs' client-rust fork; upstream would want crates.io `tikv-client` once its fixes land) | [#6](https://github.com/ostrium-labs/resonate/pull/6), merged | Loams' native durable store moves here from `crates/loams-durable/src/tikv.rs` (design 74 D261, D1513), and it replaces `loam/tikv-dapr`'s `store_tikv.rs` |
| FW3: `resonate-client` (typed Rust protocol client, HTTP and in process, retries, auth hook) and the public protocol entry in `resonate-gateway-http` (`protocol_router`, `handle_envelope`, `answer_envelope`, `ProtocolOptions`) | `fw3/resonate-client` | Not opened; both parts are upstreamable as they stand | [#4](https://github.com/ostrium-labs/resonate/pull/4) into `loams/main` | Loams' Temporal gateway (`loams-temporal-resonate`) and `loams-worker` call Resonate only through this client (design 74 D1516), and `loams-durable` can mount the endpoint per tenant instead of keeping a copy of `handle_api` |
| FW10: OpenTelemetry tracing (OTLP), `traceparent` across create → task → settle, store-op latency histogram, `cargo xtask bench` | `fw10/tracing-bench` | Not opened | [#5](https://github.com/ostrium-labs/resonate/pull/5) into `loams/main` | Benchmarks and traces for DX3 and the bench comparison (design 74 §7.4 FW10) |
| FW7: the gateway-facing protocol profile (`docs/protocol-profile.md`) and its scenario tests (`crates/resonate-profile`) | `fw7/protocol-profile` | Not opened (the doc is Loams-facing; the scenario crate could be offered) | [#3](https://github.com/ostrium-labs/resonate/pull/3), merged | Loams design 74 §7.4: what DX1's Temporal gateway may rely on, pinned per store |
| Postgres: accept promise ids with more than one `:` | `fix/postgres-multi-colon-ids` | [#1195](https://github.com/resonatehq/resonate/pull/1195), open | Branch ready on `main`; on `loams/main` through [#8](https://github.com/ostrium-labs/resonate/pull/8). A new migration (`0002_ids_with_several_separators.sql`) drops the check, so existing databases are migrated, not recreated | Design 74 ids and any caller id with several `:` were a 500 on Postgres only (schema check `well_formed_promise_id_at_most_one_separator`); every other store and the spec accept them |
| SQL engines: hold a task's first dispatch back to its `resonate:delay` | `fix/sql-resonate-delay` | [#1196](https://github.com/resonatehq/resonate/pull/1196), open | Branch ready on `main`; on `loams/main` through [#8](https://github.com/ostrium-labs/resonate/pull/8) | The spec's `createPromise`, the model and the blob server delay the first dispatch; SQLite, Postgres and MySQL dispatched at create. DX1 uses the delay for retry backoff |
| FW11: timer lateness under load, in four commits. `IndexedWheel` (`spec_merge` over a BTreeMap and a hash index, `O(log n)` per arrival, tested step by step against the proved `TimerWheel`, which is unchanged); the timer runs it; the timer fires due deadlines before draining more merges (bounded drain); `resonate-sql` fires up to 16 due deadlines at once. Plus `xtask bench --max-timer-p99-ms` and a harness fix. Supersedes FW8's `e90c0c2f` and `37fab6de` | `fw11/timer-wheel`; upstream-ready `fix/timer-wheel-merge` (the four commits only, on `main`) | [#1198](https://github.com/resonatehq/resonate/pull/1198), open. **Strong upstream candidate**: a latency bug in every SQL, Neo4j and ScyllaDB server under load | [#23](https://github.com/ostrium-labs/resonate/pull/23) | Under task load, SQL-backend timers fired tens of seconds late or not at all (SQLite: 0 of 500 within 30 s; Postgres: p99 21.5 s). After: SQLite p99 61 ms, Postgres p99 30 ms |
| FW8: poll groups with many consumers. `http-poll` skips a full or closed connection and tries the next, drops a closed one at once and re-routes what it had not read; `tests/poll_group.rs` (3 consumers, 10,000 tasks, one killed) on SQLite, blob and Postgres | `fw8/poll-groups` | Not opened (upstreamable; its timer-driver and SQL-fire commits moved into FW11) | [#9](https://github.com/ostrium-labs/resonate/pull/9) | Gateway replicas and worker agents share one `poll://any@<group>` (Loams design 74 §6.2) |
| FW3 follow-up: the poll consumer in `resonate-client` (`Client::poll`, `Subscription`, `PollEvent`/`PollMessage`; SSE reader, reconnect with backoff and a `Reconnected` event, auth per connection) | `loams/fw3-poll-consumer` | Not opened; upstreamable with FW3 | PR into `loams/main` | Loams' Temporal gateway reads its `poll://` deliveries through it and drops its own interim SSE reader (`wire_interim`, Loams D1591, D2041) |

Fork work items (Loams design 74 §7.4) are added to this table as their PRs
merge into `loams/main`.

## Not upstreamable

None yet. A carry that upstream declines goes here with the reviewer's reason.

## Tracked, not fixed yet

Findings from the fork work and the benchmarks that have no fix on
`loams/main` yet. Each one becomes a carry row when its fix lands.

| Finding | Found by | Next step |
|---|---|---|
| A timer with a target (`resonate:timer` with `resonate:target`) is outside the spec (`well_formed_promise_timer_not_targeted`). Postgres enforces this as a schema check and answers 500; the oracle, SQLite and blob accept it. The TS SDK's `ctx.sleep` (`impl/sdk/ts/src/context.ts`, `sleepCreateOpts`) sends exactly this, so a durable sleep fails on Postgres | bench agent; FW7 scenario `timer_with_target` | An issue is being drafted for the owner. Decide between fixing the SDK and relaxing the spec before changing any store |
| MySQL/TiDB: a target or listener address over 255 bytes is truncated to 255 bytes without an error, so messages go to the wrong address | FW7 | In progress as `fw2/address-length`, an FW2 follow-up. Upstream-ready branch: `fix/mysql-address-truncation` |
| Trace continuity: the promise → trace map that carries `traceparent` onto execute messages lives in one process. A message emitted by another instance, or after a restart, starts a new trace unless the promise has a `traceparent` tag | FW10 | Make stores read the `traceparent` tag when they emit messages, so the trace survives restarts and multiple instances |
| A half-open poll connection costs one retry timeout per message sent to it before the consumer is dropped | FW8 | A transport-level ack, or TCP keepalive on the SSE stream |
| Postgres throughput falls as the database grows: 94 creates/s on an empty database, about 20/s at 38k rows (debug build) | FW8 | Profile the queries (index use on `promises`), with FW10's per-store-op histograms |
| TiKV refuses keys over 8 KiB, so on TiKV a promise id is limited by its encoded origin (see `docs/protocol-profile.md`). Such a request has no answer (503) rather than a 4xx | FW7 on TiKV | Validate the id length at the protocol edge (a 400), with the limit taken from the store |

## Remaining fork work (2026-10-25)

These items from Loams design 74 §7.4 are not on `loams/main` yet. Pick
them up from the branches listed here. Each WIP commit message has a
handoff.

| Item | State | Branch | Next steps |
|---|---|---|---|
| FW5: settled-promise retention | WIP, pushed, no PR | `fw5/retention` (`8f832410`, `aaccff1a`) | **Done:** the `retention` setting (tag-selector rules, age, `tombstone_horizon`, batch, interval; expired re-create = 410) and SQLite and Postgres pruning with tombstones and metrics. The Lean spec draft is in `spec/spec/02-abstract/retention.lean` and `04-theorems/retention.lean`. It has 3 `sorry` and was typechecked on a stray Lean 4.34.1 only, so it needs `lake build` on the pinned 4.31.0. **Open question:** pruned ids left in outbox entries break `consistent_outbox_unblock_names_settled_promise`. **To do:** MySQL/TiDB, the blob/TiKV store (use FW6's `ix/p/s/<state>/` index once it lands), `tlap/Retention.tla`, profile scenarios, a scaled soak test, `docs/protocol-profile.md`, the PR |
| FW6: tag-indexed search on blob/TiKV | WIP, pushed, no PR | `fw6/tag-index` (`a67f4fb7`) | **Done:** an optional `Store::index()` port (`StoreIndex`, `IndexView`, `IndexDelta`), a TiKV implementation in the same pessimistic transaction as the document (`root ‖ resonate/ix/`), `MemStore` and `CountingStore`, index-backed `promise.search`/`task.search` with cursors, a one-time rebuild on a missing marker (`ix/v1`), and extended differentials. **To do:** compile and run the port differential, read-count tests, crash and race tests, the 0.1.1 fixture backfill test, a TiKV differential and porcupine, the PR |
| FW9: several server instances on one TiKV keyspace | Not started | | Origin ownership, timers and delivery with 2–3 instances. Porcupine and checktrace with one instance killed. Measure the worst-case added timer delay. Needs a 3-node TiKV (`test/tikv/` is single-node), which also covers FW1's TiKV leader-transfer chaos case |
| FW4: batch request | Not built, by design | | Only if DX3 measures that per-request overhead dominates (design 74 FW4). The bench agent's numbers decide |
| `checktrace` (Lean trace checker) for every store | Not run anywhere | | Needs the Lean toolchain (`spec/lean-toolchain`) in CI. Porcupine runs instead (SQLite, TiDB, TiKV legs) |
| Upstream PRs | Opened on 2026-10-25 with the owner's approval: [#1195](https://github.com/resonatehq/resonate/pull/1195) (Postgres multi-`:` ids), [#1196](https://github.com/resonatehq/resonate/pull/1196) (SQL `resonate:delay`), [#1197](https://github.com/resonatehq/resonate/pull/1197) (jsonwebtoken 10.3), [#1198](https://github.com/resonatehq/resonate/pull/1198) (timer wheel). Issues #1188–#1194 and #1199 are open | Not opened yet: `fix/mysql-address-truncation` (issue #1193), the xtask target-dir fix (issue #1194), FW1 (TiKV), FW3 (client and router; issue #1165), FW5 (issue #1166), FW8 | Anything further to resonatehq still needs the owner's OK. #1163 (TiDB) would also take FW2's `tests/locks.rs` |

## Rebuilding `loams/main` after an upstream bump

```sh
git fetch upstream
git push origin upstream/main:main            # fast-forward only
git switch -C loams/main upstream/main
# cherry-pick each still-open carry, in table order; resolve; then
cd impl/server/core && cargo xtask check
git push --force-with-lease origin loams/main
```

Loams moves its pin in a PR that changes only `rev` in `Cargo.toml`, plus
any code the new revision requires.
