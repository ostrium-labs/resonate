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
| `sdk-rs`: reqwest without default TLS | `feat/sdk-rs-reqwest-default-feature` | [#1168](https://github.com/resonatehq/resonate/pull/1168) | Open, rebased | Loams embeds the Rust SDK with rustls only |
| `resonate-auth`: jsonwebtoken 10.3 on aws-lc-rs; unsigned mode without the deprecated switch | `fix/auth-jsonwebtoken-10` | Not opened (needs the owner's approval) | Branch ready on `main` | GHSA-h395-gr6q-cpjc |
| xtask: resolve the target directory through `cargo metadata` | in FW0 | Not opened | Carried | `porcupine` assumed `<workspace>/target`. It breaks under a configured `build.target-dir` or `CARGO_TARGET_DIR` |
| FW1: the TiKV store (`resonate-server-blob` feature `tikv`, plugin `resonate-server-tikv`). It keeps Loams 0.1.x's on-disk layout byte for byte: `root ‖ resonate/blob/<key>` → 16-byte random revision ‖ body, with commit tokens at `root ‖ t/`. It also adds `xtask --backend tikv` and `test/tikv/` | `fw1/tikv-store` | Not opened; upstreamable (it brings `tikv-client` from Ostrium Labs' client-rust fork; upstream would want crates.io `tikv-client` once its fixes land) | PR into `loams/main` | Loams' native durable store moves here from `crates/loams-durable/src/tikv.rs` (design 74 D261, D1513), and it replaces `loam/tikv-dapr`'s `store_tikv.rs` |
| FW3: `resonate-client` (typed Rust protocol client, HTTP and in process, retries, auth hook) and the public protocol entry in `resonate-gateway-http` (`protocol_router`, `handle_envelope`, `answer_envelope`, `ProtocolOptions`) | `fw3/resonate-client` | Not opened; both parts are upstreamable as they stand | [#4](https://github.com/ostrium-labs/resonate/pull/4) into `loams/main` | Loams' Temporal gateway (`loams-temporal-resonate`) and `loams-worker` call Resonate only through this client (design 74 D1516), and `loams-durable` can mount the endpoint per tenant instead of keeping a copy of `handle_api` |
| FW10: OpenTelemetry tracing (OTLP), `traceparent` across create → task → settle, store-op latency histogram, `cargo xtask bench` | `fw10/tracing-bench` | Not opened | [#5](https://github.com/ostrium-labs/resonate/pull/5) into `loams/main` | Benchmarks and traces for DX3 and the bench comparison (design 74 §7.4 FW10) |
| FW7: the gateway-facing protocol profile (`docs/protocol-profile.md`) and its scenario tests (`crates/resonate-profile`) | `fw7/protocol-profile` | Not opened (the doc is Loams-facing; the scenario crate could be offered) | In review | Loams design 74 §7.4: what DX1's Temporal gateway may rely on, pinned per store |

Fork work items (Loams design 74 §7.4) are added to this table as their PRs
merge into `loams/main`.

## Not upstreamable

None yet. A carry that upstream declines goes here with the reviewer's reason.

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
