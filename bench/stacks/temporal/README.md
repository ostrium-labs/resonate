# rbench stack: Temporal

The stock Temporal server on plain Postgres, for the Temporal leg of rbench. The driver is
`bench/drivers/temporal-go`.

| Service | Container | Image (pinned as DX0 pins them) |
|---|---|---|
| `postgres` | `rbench-temporal-postgres` | `docker.io/library/postgres:17.10@sha256:7958605b474b3d264a969cb3a123d6aa00ad1e1fe9da8a69984dabb704d93317` |
| `schema` (one-shot) | `rbench-temporal-schema` | `docker.io/temporalio/admin-tools:1.32.1@sha256:9ef0dc5b063bf4ffd5c1512de72e0e73e225bd2c7006a18ce69bd592a8862a83` |
| `temporal` | `rbench-temporal-server` | `docker.io/temporalio/server:1.32.1@sha256:acecbab5b0b8ae9cf4012b508866ca97feda3709577bf157b537239f40029049` |
| `namespace` (one-shot) | `rbench-temporal-namespace` | the admin-tools image above |

Host ports, bound to 127.0.0.1 only: Temporal frontend gRPC on `127.0.0.1:17233`, Postgres on
`127.0.0.1:15433` (user and password `temporal`). Compose project `rbench-temporal`.

```sh
bench/stacks/temporal/up.sh                                   # up, waits for namespace default
podman compose -f bench/stacks/temporal/compose.yaml down -v  # down, drops the volume
```

## How it is wired

- **Same shape as DX0.** The service env, plugin `postgres12`, 16 history shards, the
  dynamic config (`dynamicconfig/development-sql.yaml`, upstream's at v1.32.1) and the schema
  step follow `deploy/release/compose.compute.yaml` and `compute/durable-init.sh` in
  ostrium-labs/loams. DX0 ran on Loams Postgres; this stack uses **plain Postgres 17.10**, so
  the same Postgres container (image, settings) can serve Resonate's Postgres leg and the
  two durable engines are compared on the same database.
- **One server process.** `TEMPORAL_SERVICES=frontend,history,matching,worker` runs all four
  services in one container (the image's `temporal-server start` takes the list), to save
  RAM. DX0 runs four containers; the code paths are the same.
- **Schema.** `init.sh schema` creates `temporal` and `temporal_visibility`, then
  `temporal-sql-tool setup-schema -v 0.0` and
  `update-schema -d /etc/temporal/schema/postgresql/v12/{temporal,visibility}/versioned`.
- **Namespace.** `init.sh namespace` waits until the frontend reports SERVING, then
  `temporal operator namespace create --retention 72h -n default`, and waits until the
  namespace answers a list call. Both steps are idempotent.
- **Postgres settings.** Stock, except `max_connections=300` (four services with their pools
  need more than the default 100) and `synchronous_commit`, which stays `on` unless
  `RBENCH_PG_SYNCHRONOUS_COMMIT` is set. Whatever it is set to must be the same for the
  Resonate Postgres leg.
- **Temporal knobs.** `TEMPORAL_SQL_MAX_CONNS` (default 20, the image template's) sets the
  persistence pool per service; `TEMPORAL_LOG_LEVEL` (default `warn`).

## Server tracing (OTel)

Temporal 1.32 exports server spans when `OTEL_TRACES_EXPORTER=otlp`
(`temporal/fx.go`), with the OTel SDK's `OTEL_EXPORTER_OTLP_*` env for the endpoint. There is
no `otel:` block in the env-only config template. The compose file wires it up but leaves it
**off by default** (`TEMPORAL_OTEL_TRACES_EXPORTER=none`; an empty value makes the server
refuse to start: `unsupported OTEL exporter`).

To turn it on, run a collector that the container can reach, then:

```sh
TEMPORAL_OTEL_TRACES_EXPORTER=otlp \
TEMPORAL_OTEL_ENDPOINT=http://host.containers.internal:4317 \
  bench/stacks/temporal/up.sh
```

Two things found in testing (2026-10-24):

- With the exporter on and **no collector reachable**, the server got roughly 10x slower
  (chain, N=20: 33-47 s against 5-7 s), not only noisy. So turn it on only while a collector is
  listening.
- `host.containers.internal` (169.254.1.2 on the rootless bridge network) does **not** reach
  a collector bound to the host's `127.0.0.1:4317`; the connection is refused. The collector
  must listen on an address the containers can reach, or run as a container on the
  `rbench-temporal_default` network (`TEMPORAL_OTEL_ENDPOINT=http://<name>:4317`). With a
  reachable collector, server spans arrived (service `temporal-server`) with no export errors
  and no slowdown.

## Host notes

On this laptop the Postgres volume sits on btrfs, where an 8 KiB `O_DSYNC` write took about
50 ms under the other agents' load. Temporal commits several transactions per workflow step,
so slow fsync shows up as `Failed to start transaction: context deadline exceeded` in the
server log (pool waits) and as long tail latencies. The Resonate Postgres leg will see the
same disk, so compare the two on the same volume and the same `synchronous_commit`.
