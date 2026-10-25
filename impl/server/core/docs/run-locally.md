# Run the protocol gateway locally

*Ostrium Labs fork (`loams/main`). The TiKV store and the trace export are
fork additions; see `CARRIES.md`.*

The Resonate server is one binary, `resonate`. Its HTTP gateway serves the
Resonate protocol, so "the protocol gateway" is just the server's HTTP port.
Each request is one `POST /` with one envelope (`{"kind", "head", "data"}`).
The same port serves `/poll/<group>/<id>` for poll workers, `/ready`, and the
console at `/`.

This page runs the binary on each store this fork supports: SQLite, Postgres,
TiDB (through the MySQL plugin) and TiKV (the `resonate-server-tikv` plugin).
It shows two ways to run it: directly, or with compose.

| Port | What |
|---|---|
| 8001 | protocol (`POST /`), `/poll`, `/ready`, console |
| 9090 | Prometheus `/metrics` |

## 1. Build

```sh
cd impl/server/core
cargo build --release --bin resonate
```

The binary is `target/release/resonate`. If a `.cargo/config.toml` sets
`build.target-dir`, it is under that directory instead. Every store plugin is
compiled in. The store is picked at run time with `servers.active`.

## 2. Pick a store and start it

Configuration comes from `resonate.toml` (`--config <file>`) or from `RESONATE_*`
environment variables. Nested keys are joined with `__`, so
`servers.server_tikv.pd` is `RESONATE_SERVERS__SERVER_TIKV__PD`.

### SQLite (default, no dependencies)

```sh
RESONATE_SERVERS__SERVER_SQLITE__PATH=./resonate.db ./resonate serve
```

### Postgres

```sh
podman run -d --name resonate-pg -p 5432:5432 \
  -e POSTGRES_USER=resonate -e POSTGRES_PASSWORD=resonate -e POSTGRES_DB=resonate postgres:16
RESONATE_SERVERS__ACTIVE=server_postgres \
RESONATE_SERVERS__SERVER_POSTGRES__URL=postgres://resonate:resonate@127.0.0.1:5432/resonate \
  ./resonate serve
```

### TiDB

TiDB goes through the MySQL plugin. On TiDB the plugin pins
`tidb_txn_mode = 'pessimistic'` for every session, so `FOR UPDATE` locks the
rows it reads.

```sh
podman run -d --name resonate-tidb -p 4000:4000 pingcap/tidb:v8.5.8   # unistore, for tests
RESONATE_SERVERS__ACTIVE=server_mysql \
RESONATE_SERVERS__SERVER_MYSQL__URL=mysql://root@127.0.0.1:4000/test \
  ./resonate serve
```

For TiDB Cloud, use the cluster's URL with TLS, for example
`mysql://<user>:<pw>@gateway01.<region>.prod.aws.tidbcloud.com:4000/resonate?ssl-mode=required`.

### TiKV

TiKV needs PD and a TiKV node on API v2 (keyspaces). The repository ships a
single-node cluster for local use:

```sh
test/tikv/up.sh 2379 20160     # PD on 2379, TiKV on 20160, host network
RESONATE_SERVERS__ACTIVE=server_tikv \
RESONATE_SERVERS__SERVER_TIKV__PD='["127.0.0.1:2379"]' \
RESONATE_SERVERS__SERVER_TIKV__KEYSPACE=resonate \
RESONATE_SERVERS__SERVER_TIKV__ROOT=dev/ \
  ./resonate serve
test/tikv/down.sh              # when done
```

If the keyspace does not exist, the server creates it through PD's HTTP API.
Every instance of one deployment uses the same `keyspace` and `root`; an
unrelated deployment uses another root. The other `server_tikv` keys are
described in `crates/resonate-server-blob/src/tikv/plugin.rs`. They are
`search`, `timer_shards`, `cache_capacity`, `retry_timeout`,
`request_timeout_ms`, `async_commit` and `create_keyspace`.

## 3. Talk to it

```sh
curl -fsS localhost:8001/ready && echo ready
curl -s localhost:8001/ -H 'content-type: application/json' -d '{
  "kind": "promise.create",
  "head": {"corrId": "1", "version": "2026-04-01"},
  "data": {"id": "hello", "timeoutAt": 9999999999999, "param": {}, "tags": {}}
}'
./resonate promises get hello --server http://localhost:8001
```

The protocol version string the server speaks is in
`crates/resonate-core/src/types.rs` (`SUPPORTED_VERSIONS`).

## 4. Or use compose

`docker-compose.yml` has one profile per store. Each profile starts the store
and the server, with the server on 8001 and 9090:

```sh
podman compose --profile sqlite up -d --build   # or postgres, mysql, tidb, tikv
podman compose --profile tikv down -v
```

The server image is built from `Dockerfile`, which takes a release build.
The `tidb` profile runs TiDB with embedded storage. The `tikv` profile
runs one PD and one TiKV on API v2, configured from `test/tikv/`. Both are
for development only, not production layouts.

## 5. Embedding the endpoint

A Rust host that serves the protocol on its own port, behind its own
authentication, mounts `resonate_gateway_http::protocol_router` (one
`ResonateServer` per request, e.g. per tenant) or calls `handle_envelope`.
This is what Loams does for its `/durable/` endpoint. Calling it from Rust
is `resonate-client` (`crates/resonate-client`), over HTTP or in process.
The subset a gateway may rely on, with exact answer codes and limits per
store, is `docs/protocol-profile.md`.

## 6. Traces

Set `OTEL_EXPORTER_OTLP_ENDPOINT` (for example `http://127.0.0.1:4317`) and the
server exports OpenTelemetry spans for each request, store operation and
delivery. See `docs/tracing.md`.
