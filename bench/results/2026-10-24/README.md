# Durable-execution benchmarks, 2026-10-24/25

Resonate on TiKV, TiDB Cloud Starter (seven regions), SQLite and Postgres, next to stock Temporal and Apache
Airflow, with the same three workloads, one harness (`bench/rbench`, Goose-driven), and OpenTelemetry traces for
every run. Everything here ran on one laptop; read the caveats before quoting a number.

## Summary

**TiKV is the backend to run Resonate on.** With the fork's TiKV store (FW1, now on `loams/main` `b5177606`, with
FW10 tracing on), raw Resonate was the fastest store when the three stores ran back to back on the final build:
fan-out **49 workflows/s, p50 291 ms, p99 1.4 s** (500 workflows × 8 parallel steps, 20 in flight) against 18/s on
Postgres and 15/s on SQLite; a 500-workflow burst **123/s** against 83/s and 37/s. That hour the host was very busy
(load 26-147 from other agents' builds). In a quieter hour an earlier FW1 build reached **121 fan-out workflows/s,
p50 147 ms, p99 278 ms**, the best number of the whole benchmark, against 49/s for SQLite, 19/s for Postgres, 9.1/s
for stock Temporal on the same Postgres image and 0.3-0.4/s for Airflow. Turning the server's tracing off did not
change the TiKV result (31 vs 37-49/s, within the host's noise). Two things are not ready:

- **durable timers fire late** after a 1 s sleep: TiKV store p50 2.5 s / p99 16 s on the final build (0.6 / 9.5 s on
  the WIP build in the quieter hour), SQLite 0.3 / 17 s, Postgres 0.1 / 0.8 s, the Loams 0.1.1 engine on TiKV
  0.25 / 3.2 s, Temporal 0.04 / 4.6 s. The SQL stores' figures are affected by the timer-wheel bug FW10 found (fork
  task FW11, not landed when these ran); the TiKV store's lateness is a separate open question for the fork;
- **the WIP FW1 build failed on a fresh TiKV**: the TiKV client gave up on region errors while the keyspace's first
  regions split (342 in one run: 3.7/s, p99 117 s, 3 failed workflows). The merged build had **no** region errors
  on a fresh cluster.

**TiDB Cloud Starter is the free, zero-ops option.** All seven regions the API offers ran, twice, for $0: 19
clusters were created with a spending limit of 0 and all 19 deleted, and the October bill reads $0. From this laptop
in India it is slow: a fan-out workflow took 35-140 s at p50 and a one-step workflow 19-48 s, because every
protocol request takes several MySQL round trips over the WAN and the traces show the 8 parallel step settles of a
workflow serializing on the workflow's task row (each `mysql.transact` holding its lock for seconds). Those numbers
say little about a Resonate server in the same cloud region as its Starter instance, which is how a hosted service
would run.

**Cost** ("if a hosted Resonate service were deployed on TiDB Starter vs on TiKV"): a fan-out workflow costs about
740 Request Units on Starter, so the 50 M free RUs cover ~68,000 of them a month; beyond that 1 M cost $74
(us-east-1) to $118 (São Paulo). A three-node PD+TiKV cluster at loams.dev/cost's prices costs $162-225 a month, so it
is cheaper than Starter above roughly 2-3 M fan-out workflows a month (1-1.5 M chains, 11-15 M one-step workflows).
Details and assumptions in [Cost](#cost).

**Baselines.** SQLite and Postgres are Resonate's baselines. Stock Temporal 1.32.1 on the same Postgres image did
the fan-out at 9.1/s (p50 1.6 s) and the burst at 38/s. Airflow 3.3.2 with the Edge executor did 0.3-0.4 fan-out
workflows/s and could not take 500 simultaneous Dag-run triggers (64 and 105 of 500 completed in two passes).

## What ran, and what could not

| Leg | Status |
|---|---|
| Raw Resonate on TiKV (fork FW1 store) | ran on `loams/main` `b5177606` (FW1 merged, FW10 tracing: server spans down to `blob.get`/`blob.put`), and earlier on the WIP branch `fw1/tikv-store` `4196c06c` (no server spans) |
| Raw Resonate on TiDB Cloud Starter, 7 regions | ran twice: an untraced pass (`loams/main` `f419b8be`) and a traced pass (FW10 build); see the TiDB section |
| Raw Resonate on SQLite and Postgres | ran, FW10 builds (server spans: gateway → `process` → `sqlite.transact`/`postgres.transact`) |
| "TiKV + protocol gateway": the Loams 0.1.1 engine's Resonate endpoint `/durable/` on TiKV | ran (`ghcr.io/ostrium-labs/loams:0.1.1`), the gateway Loams ships today |
| Stock Temporal 1.32.1 on Postgres (DX0 pins) | ran, Go SDK 1.49.0 |
| **Temporal SDK through a Temporal-protocol gateway on Resonate** | **not run**: that gateway is DX1 work in ostrium-labs/loams and did not run an SDK workflow when these ran |
| Airflow 3.3.2 + Edge executor (DX0 pins) | ran |
| **Airflow with a Resonate-backed executor** | **not run**: `LoamsResonateExecutor` (design 74 §9.3) does not exist yet |
| SigNoz on Loams House | **not used**: loams#1176 and #1181 (the House changes SigNoz needs) were open; SigNoz ran on its own bundled ClickHouse instead |

## Method

**Workloads** (identical shape for every system; `bench/drivers/README.md`):

- **fanout**: one workflow = 8 parallel no-op steps, then a join. 500 workflows, 20 in flight.
- **chain**: 5 sequential no-op steps, each followed by a durable sleep of 1 s. 100 workflows, 20 in flight.
- **burst**: 500 one-step workflows started at once (500 Goose users, one iteration each), then awaited.

TiDB Cloud runs were smaller to fit the WAN and the free quota: fanout 40 (4 in flight), chain 20 (10 in flight),
burst 50.

**Drivers.** Raw Resonate and the Loams endpoint are driven over HTTP by Goose 0.18.1. Each Goose user is the
caller and a co-located worker, and sends exactly the requests the TypeScript SDK sends for the same code
(`task.create`; per step `task.fence`+`promise.create` then `task.fence`+`promise.settle`; per sleep a timer promise,
`task.suspend`, a wake-up from the HTTP push transport, `task.acquire`; `task.heartbeat` every ttl/3; `task.fulfill`).
Airflow is driven over its REST API by Goose (trigger, poll every 100 ms, read task instances). Temporal is driven by
its Go SDK (`bench/drivers/temporal-go`) under `rbench exec`. Every system reports the same records: per workflow
start/end, per step scheduled/started/ended, per request latency, and CPU/RSS of each process or container sampled
every second.

**Metrics.** Throughput = completed workflows / (last end − first start). Latency = start-to-complete per workflow.
Step latency = step scheduled (or sent) to completion observed. Sleep lateness = wake-up observed − timer due.
Errors = failed workflows + requests answered 5xx/transport errors. Retries = re-sent requests and activity
attempts > 1.

**Traces.** Every process exports OTLP to an OpenTelemetry Collector (contrib 0.162.0), which writes every span to
files and forwards it to SigNoz. Client spans (`rbench.workflow` → `resonate <kind>`) carry `traceparent` into the
server; the FW10 server continues the trace (gateway span → `process <kind>` → store transaction). The exported
spans are in [`traces/`](traces/) (`.jsonl.zst`, one OTLP/JSON export request per line), other host processes'
spans removed.

## Environment

| | |
|---|---|
| Host | one laptop: Intel Core Ultra 5 225H (14 cores), 15 GB RAM, NVMe; CachyOS, Linux 7.2.4; podman 6.1.1 rootless |
| Shared host | other agents were building Rust code at the same time: load average 14-30 during most runs (each run's `env.json` records load at start and end; the tables show it) |
| Disk | all local data on ext4 `/mnt/Projects`; an 8 KiB O_DSYNC write took 5 ms when quiet and up to 170 ms under the other agents' builds |
| Network | home connection in India. TCP connect times to the TiDB Cloud endpoints (`tidbcloud-rtt.json`) are not the true round trip: some regions answered the handshake in 18 ms, which only a terminating proxy on the path can do, so read them as a lower bound; the traces' `mysql.transact` spans show the real cost |
| Resonate | `loams/main` `b5177606` (final: FW1 + FW2 + FW10), the FW10 tracing build of `fw10/tracing-bench` (2026-10-24 21:43 IST; SQLite/Postgres table B and the traced TiDB pass), `fw1/tikv-store` `4196c06c` (WIP TiKV runs), `loams/main` `f419b8be` (untraced TiDB pass) |
| Loams | `ghcr.io/ostrium-labs/loams:0.1.1@sha256:9fa00b99…` |
| TiKV / PD | v8.5.8 (`docker.io/pingcap/{pd,tikv}:v8.5.8` by digest), one node each, API v2 keyspaces |
| TiDB (RU measurement) | `ghcr.io/ostrium-labs/tidb:v8.5.8-loams.1@sha256:ea62f135…` |
| TiDB Cloud | Starter, v8.5.3-serverless, spending limit 0 |
| Postgres | 17.10 (`postgres:17.10@sha256:7958605b…`), `max_connections=300`, `synchronous_commit=on` |
| Temporal | server and admin-tools 1.32.1 by digest (DX0 R0.2), Go SDK 1.49.0, OTel contrib 0.8.1 |
| Airflow | 3.3.2-python3.12 by digest + `apache-airflow-providers-edge3==4.3.2` (DX0 R0.14), one scheduler, 2 Edge workers × 16 slots |
| Harness | rbench 0.1.0, Goose 0.18.1, opentelemetry 0.33.0, Rust 1.97.1 |
| Traces | otelcol-contrib 0.162.0; SigNoz community 0.145.0, signoz-otel-collector 0.144.12, ClickHouse 25.12.5 |

## Results: local backends

Two sets. **Table A** is the cleanest comparison of Resonate's stores: the final build (`loams/main` `b5177606`,
tracing on) on TiKV, Postgres and SQLite, run back to back within 17 minutes, with the extra TiKV runs of that build
(fresh cluster, tracing off). **Table B** has every system, from a quieter hour (load 14-30) on the builds of that
hour. Both lists, and why each row is there, are in [`headline-runs.txt`](headline-runs.txt); every run, including the
ones not listed, is in [`results.md`](results.md) and [`runs/`](runs/). "server CPU %" sums the server and store
processes or containers (100 = one core); for Temporal it includes the Go SDK worker process. The load column is the
host's 1-minute load average when the run started: compare rows within a table, and distrust rows with a high load.

#### A · fanout, raw Resonate stores back to back (500 workflows x 8 parallel steps, 20 in flight)

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | TiKV (FW1 store) | 500 / 20 | 500/500 | 49.44 | 291 | 821 | 1,414 | 133.9 | – | 0 | 105 | 39.78 | [resonate-tikv-fanout-1792916938](runs/resonate-tikv-fanout-1792916938/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, 6 min earlier_ | 500 / 20 | 500/500 | 48.39 | 290 | 789 | 2,054 | 140.0 | – | 0 | 113 | 28.77 | [resonate-tikv-fanout-1792916248](runs/resonate-tikv-fanout-1792916248/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, first load on a fresh TiKV_ | 500 / 20 | 500/500 | 37.48 | 279 | 1,438 | 2,582 | 136.2 | – | 0 | 79 | 26.27 | [resonate-tikv-fanout-1792916070](runs/resonate-tikv-fanout-1792916070/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, server tracing off_ | 500 / 20 | 500/500 | 30.62 | 329 | 1,871 | 2,311 | 156.1 | – | 0 | 80 | 41.74 | [resonate-tikv-fanout-1792916438](runs/resonate-tikv-fanout-1792916438/) |
| Resonate (raw) | postgres | 500 / 20 | 500/500 | 17.86 | 813 | 2,990 | 4,692 | 365.0 | – | 0 | 102 | 25.99 | [resonate-postgres-fanout-1792916826](runs/resonate-postgres-fanout-1792916826/) |
| Resonate (raw) | sqlite | 500 / 20 | 500/500 | 15.12 | 889 | 3,174 | 10,577 | 129.2 | – | 0 | 17 | 52.83 | [resonate-sqlite-fanout-1792916650](runs/resonate-sqlite-fanout-1792916650/) |

#### A · chain, raw Resonate stores back to back (100 workflows x 5 steps + 1 s sleeps, 20 in flight)

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | TiKV (FW1 store) | 100 / 20 | 100/100 | 0.80 | 19,363 | 47,161 | 55,753 | 67.1 | 2,541 / 16,271 | 0 | 23 | 35.08 | [resonate-tikv-chain-1792916950](runs/resonate-tikv-chain-1792916950/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, 6 min earlier_ | 100 / 20 | 100/100 | 1.16 | 14,535 | 30,739 | 31,175 | 56.2 | 1,588 / 14,553 | 0 | 33 | 29.92 | [resonate-tikv-chain-1792916260](runs/resonate-tikv-chain-1792916260/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, fresh TiKV_ | 100 / 20 | 100/100 | 1.07 | 17,283 | 26,767 | 32,852 | 59.4 | 2,003 / 12,850 | 0 | 31 | 26.33 | [resonate-tikv-chain-1792916086](runs/resonate-tikv-chain-1792916086/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, server tracing off_ | 100 / 20 | 100/100 | 1.17 | 15,835 | 26,508 | 28,476 | 51.4 | 1,562 / 7,809 | 0 | 33 | 37.39 | [resonate-tikv-chain-1792916456](runs/resonate-tikv-chain-1792916456/) |
| Resonate (raw) | postgres | 100 / 20 | 100/100 | 3.02 | 6,224 | 7,527 | 7,863 | 40.4 | 114 / 811 | 0 | 61 | 30.67 | [resonate-postgres-chain-1792916856](runs/resonate-postgres-chain-1792916856/) |
| Resonate (raw) | sqlite | 100 / 20 | 100/100 | 1.25 | 14,352 | 34,547 | 38,674 | 7.1 | 333 / 16,937 | 0 | 6 | 39.68 | [resonate-sqlite-chain-1792916684](runs/resonate-sqlite-chain-1792916684/) |

#### A · burst, raw Resonate stores back to back (500 one-step workflows at once)

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | TiKV (FW1 store) | 500 / 500 | 500/500 | 123.39 | 2,681 | 3,862 | 3,977 | 764.2 | – | 0 | 68 | 146.61 | [resonate-tikv-burst-1792917077](runs/resonate-tikv-burst-1792917077/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, 6 min earlier_ | 500 / 500 | 500/500 | 139.54 | 2,379 | 3,444 | 3,530 | 700.8 | – | 0 | 72 | 28.51 | [resonate-tikv-burst-1792916349](runs/resonate-tikv-burst-1792916349/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, fresh TiKV_ | 500 / 500 | 500/500 | 146.99 | 2,120 | 3,214 | 3,320 | 214.0 | – | 0 | 60 | 39.79 | [resonate-tikv-burst-1792916183](runs/resonate-tikv-burst-1792916183/) |
| Resonate (raw) | TiKV (FW1 store) — _same build, server tracing off_ | 500 / 500 | 500/500 | 148.50 | 2,179 | 3,269 | 3,351 | 199.2 | – | 0 | 67 | 29.30 | [resonate-tikv-burst-1792916545](runs/resonate-tikv-burst-1792916545/) |
| Resonate (raw) | postgres | 500 / 500 | 500/500 | 82.54 | 4,830 | 5,911 | 6,024 | 2,969.4 | – | 0 | 147 | 27.06 | [resonate-postgres-burst-1792916890](runs/resonate-postgres-burst-1792916890/) |
| Resonate (raw) | sqlite | 500 / 500 | 500/500 | 36.62 | 12,588 | 13,564 | 13,638 | 2,281.6 | – | 0 | 15 | 33.30 | [resonate-sqlite-burst-1792916765](runs/resonate-sqlite-burst-1792916765/) |

#### B · fanout, every system, quieter hour

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | TiKV (FW1 store) — _WIP FW1 4196c06c, warm cluster_ | 500 / 20 | 500/500 | 120.99 | 147 | 222 | 278 | 74.0 | – | 0 | 220 | 19.72 | [resonate-tikv-fanout-1792880201](runs/resonate-tikv-fanout-1792880201/) |
| Resonate (raw) | TiKV (FW1 store) — _WIP FW1, fresh TiKV: region errors_ | 500 / 20 | 497/500 | 3.70 | 162 | 938 | 116,763 | 75.0 | – | 36 | 32 | 17.51 | [resonate-tikv-fanout-1792879851](runs/resonate-tikv-fanout-1792879851/) |
| Loams 0.1.1 /durable/ | tikv | 500 / 20 | 500/500 | 62.37 | 244 | 763 | 1,093 | 124.9 | – | 0 | 118 | 24.06 | [loams-durable-tikv-fanout-1792876595](runs/loams-durable-tikv-fanout-1792876595/) |
| Resonate (raw) | sqlite | 500 / 20 | 500/500 | 48.99 | 280 | 1,128 | 1,843 | 119.2 | – | 0 | 78 | 17.50 | [resonate-sqlite-fanout-1792876507](runs/resonate-sqlite-fanout-1792876507/) |
| Resonate (raw) | postgres | 500 / 20 | 500/500 | 18.96 | 669 | 2,303 | 5,115 | 328.2 | – | 0 | 121 | 29.46 | [resonate-postgres-fanout-1792879054](runs/resonate-postgres-fanout-1792879054/) |
| Temporal 1.32.1 | postgres | 500 / 20 | 500/500 | 9.09 | 1,555 | 6,050 | 8,096 | 287.9 | – | 0 | 141 | 18.39 | [temporal-postgres-fanout-1792876726](runs/temporal-postgres-fanout-1792876726/) |
| Airflow 3.3.2 + Edge | postgres — _polling each run every 1 s_ | 500 / 20 | 496/500 | 0.27 | 39,931 | 63,041 | 146,046 | 9,705.0 | – | 451 | 391 | 31.55 | [airflow-postgres-fanout-1792897057](runs/airflow-postgres-fanout-1792897057/) |
| Airflow 3.3.2 + Edge | postgres — _polling every 100 ms (earlier pass)_ | 500 / 20 | 497/500 | 0.42 | 25,309 | 45,653 | 93,020 | 6,119.8 | – | 2171 | 467 | 16.76 | [airflow-postgres-fanout-1792876931](runs/airflow-postgres-fanout-1792876931/) |

#### B · chain, every system, quieter hour

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | TiKV (FW1 store) — _WIP FW1, warm_ | 100 / 20 | 100/100 | 1.56 | 11,065 | 20,420 | 20,520 | 30.3 | 567 / 9,543 | 0 | 41 | 19.26 | [resonate-tikv-chain-1792880206](runs/resonate-tikv-chain-1792880206/) |
| Resonate (raw) | TiKV (FW1 store) — _WIP FW1, fresh TiKV_ | 100 / 20 | 100/100 | 0.87 | 15,384 | 42,199 | 44,323 | 38.5 | 1,846 / 22,863 | 0 | 28 | 23.15 | [resonate-tikv-chain-1792879987](runs/resonate-tikv-chain-1792879987/) |
| Loams 0.1.1 /durable/ | tikv | 100 / 20 | 100/100 | 2.39 | 7,222 | 12,100 | 12,177 | 31.7 | 251 / 3,168 | 0 | 73 | 24.28 | [loams-durable-tikv-chain-1792876605](runs/loams-durable-tikv-chain-1792876605/) |
| Resonate (raw) | sqlite | 100 / 20 | 100/100 | 2.31 | 6,557 | 12,601 | 12,964 | 4.5 | 18 / 6,954 | 0 | 16 | 18.29 | [resonate-sqlite-chain-1792876519](runs/resonate-sqlite-chain-1792876519/) |
| Resonate (raw) | postgres | 100 / 20 | 100/100 | 1.37 | 7,323 | 18,036 | 18,884 | 72.3 | 358 / 7,164 | 0 | 32 | 27.97 | [resonate-postgres-chain-1792879084](runs/resonate-postgres-chain-1792879084/) |
| Temporal 1.32.1 | postgres | 100 / 20 | 100/100 | 2.10 | 6,452 | 17,250 | 17,539 | 16.1 | 42 / 4,637 | 0 | 56 | 21.79 | [temporal-postgres-chain-1792876782](runs/temporal-postgres-chain-1792876782/) |
| Airflow 3.3.2 + Edge | postgres — _polling every 1 s_ | 100 / 20 | 100/100 | 0.36 | 43,143 | 114,369 | 115,769 | 1,762.4 | 2,386 / 6,070 | 0 | 347 | 34.87 | [airflow-postgres-chain-1792913275](runs/airflow-postgres-chain-1792913275/) |
| Airflow 3.3.2 + Edge | postgres — _polling every 100 ms (earlier pass)_ | 100 / 20 | 100/100 | 0.35 | 50,516 | 70,116 | 72,379 | 2,908.3 | 3,301 / 6,712 | 0 | 585 | 22.06 | [airflow-postgres-chain-1792878109](runs/airflow-postgres-chain-1792878109/) |

#### B · burst, every system, quieter hour

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | TiKV (FW1 store) — _WIP FW1, warm_ | 500 / 500 | 500/500 | 135.51 | 1,596 | 3,528 | 3,655 | 172.2 | – | 0 | 50 | 14.28 | [resonate-tikv-burst-1792880271](runs/resonate-tikv-burst-1792880271/) |
| Resonate (raw) | TiKV (FW1 store) — _WIP FW1, fresh TiKV_ | 500 / 500 | 500/500 | 62.48 | 6,277 | 7,264 | 7,989 | 2,104.7 | – | 0 | 37 | 19.39 | [resonate-tikv-burst-1792880103](runs/resonate-tikv-burst-1792880103/) |
| Loams 0.1.1 /durable/ | tikv | 500 / 500 | 500/500 | 113.15 | 3,081 | 4,223 | 4,298 | 311.0 | – | 0 | 65 | 18.86 | [loams-durable-tikv-burst-1792876649](runs/loams-durable-tikv-burst-1792876649/) |
| Resonate (raw) | sqlite | 500 / 500 | 500/500 | 184.22 | 2,188 | 2,635 | 2,677 | 584.8 | – | 0 | 25 | 22.15 | [resonate-sqlite-burst-1792876563](runs/resonate-sqlite-burst-1792876563/) |
| Resonate (raw) | postgres | 500 / 500 | 500/500 | 114.70 | 3,233 | 4,231 | 4,311 | 1,169.9 | – | 0 | 176 | 25.12 | [resonate-postgres-burst-1792879159](runs/resonate-postgres-burst-1792879159/) |
| Temporal 1.32.1 | postgres | 500 / 500 | 500/500 | 37.62 | 7,888 | 11,405 | 11,775 | 32.7 | – | 0 | 181 | 22.29 | [temporal-postgres-burst-1792876832](runs/temporal-postgres-burst-1792876832/) |
| Airflow 3.3.2 + Edge | postgres — _polling every 1 s_ | 500 / 500 | 64/500 | 0.15 | 389,849 | 421,564 | 421,569 | 343,945.8 | – | 2537 | 136 | 37.59 | [airflow-postgres-burst-1792913572](runs/airflow-postgres-burst-1792913572/) |
| Airflow 3.3.2 + Edge | postgres — _polling every 100 ms (earlier pass)_ | 500 / 500 | 105/500 | 0.20 | 519,501 | 520,424 | 520,501 | 359,883.4 | – | 5031 | 104 | 57.69 | [airflow-postgres-burst-1792878403](runs/airflow-postgres-burst-1792878403/) |

Charts (table B): [fanout](charts/fanout-throughput.svg) · [chain](charts/chain-throughput.svg) ·
[burst](charts/burst-throughput.svg) throughput; [fanout](charts/fanout-latency.svg) ·
[chain](charts/chain-latency.svg) · [burst](charts/burst-latency.svg) latency.

![fanout throughput](charts/fanout-throughput.svg)
![fanout latency](charts/fanout-latency.svg)

## Results: TiDB Cloud Starter, seven regions

The API offers Starter in seven regions (`GET /v1beta1/regions`: AWS Oregon, N. Virginia, Singapore, Tokyo,
Frankfurt, São Paulo; Alibaba Cloud Singapore), not five; all seven ran, one cluster at a time. The Resonate server
ran on this laptop against each region over TLS, so **every number includes the WAN**. Tables: the traced pass
(FW10 build, 2026-10-25); the untraced pass of 2026-10-24 (`loams/main` `f419b8be`) is in `results.md` and agrees
within the noise. Sizes: fanout 40 workflows (4 in flight), chain 20 (10 in flight), burst 50.

#### TiDB fanout: 40 workflows x 8 parallel steps, 4 in flight (server on the laptop, store over the WAN, traced FW10 build)

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | tidb-aws-ap-southeast-1 | 40 / 4 | 40/40 | 0.10 | 34,810 | 77,718 | 80,606 | 21,675.0 | – | 0 | 1 | 30.81 | [resonate-tidb-aws-ap-southeast-1-fanout-1792878546](runs/resonate-tidb-aws-ap-southeast-1-fanout-1792878546/) |
| Resonate (raw) | tidb-aws-ap-northeast-1 | 40 / 4 | 37/40 | 0.06 | 50,273 | 90,539 | 96,101 | 29,697.6 | – | 8 | 0 | 33.14 | [resonate-tidb-aws-ap-northeast-1-fanout-1792879201](runs/resonate-tidb-aws-ap-northeast-1-fanout-1792879201/) |
| Resonate (raw) | tidb-aws-eu-central-1 | 40 / 4 | 34/40 | 0.04 | 83,337 | 134,109 | 144,347 | 47,374.1 | – | 26 | 0 | 20.30 | [resonate-tidb-aws-eu-central-1-fanout-1792895084](runs/resonate-tidb-aws-eu-central-1-fanout-1792895084/) |
| Resonate (raw) | tidb-aws-us-east-1 | 40 / 4 | 30/40 | 0.02 | 141,260 | 177,667 | 180,058 | 71,450.1 | – | 171 | 0 | 23.76 | [resonate-tidb-aws-us-east-1-fanout-1792896380](runs/resonate-tidb-aws-us-east-1-fanout-1792896380/) |
| Resonate (raw) | tidb-aws-us-west-2 | 40 / 4 | 33/40 | 0.04 | 64,357 | 83,057 | 168,286 | 41,938.0 | – | 39 | 0 | 23.68 | [resonate-tidb-aws-us-west-2-fanout-1792913083](runs/resonate-tidb-aws-us-west-2-fanout-1792913083/) |
| Resonate (raw) | tidb-aws-sa-east-1 | 40 / 4 | 31/40 | 0.04 | 84,093 | 134,241 | 134,877 | 50,790.1 | – | 32 | 0 | 33.44 | [resonate-tidb-aws-sa-east-1-fanout-1792914237](runs/resonate-tidb-aws-sa-east-1-fanout-1792914237/) |
| Resonate (raw) | tidb-alicloud-ap-southeast-1 | 40 / 4 | 35/40 | 0.05 | 49,023 | 97,697 | 138,865 | 31,603.6 | – | 40 | 0 | 19.87 | [resonate-tidb-alicloud-ap-southeast-1-fanout-1792915614](runs/resonate-tidb-alicloud-ap-southeast-1-fanout-1792915614/) |

#### TiDB chain: 20 workflows x 5 steps + 1 s sleeps, 10 in flight

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | tidb-aws-ap-southeast-1 | 20 / 10 | 20/20 | 0.14 | 66,792 | 71,878 | 74,554 | 5,119.1 | 5,667 / 9,763 | 0 | 1 | 24.17 | [resonate-tidb-aws-ap-southeast-1-chain-1792878945](runs/resonate-tidb-aws-ap-southeast-1-chain-1792878945/) |
| Resonate (raw) | tidb-aws-ap-northeast-1 | 20 / 10 | 20/20 | 0.14 | 68,604 | 69,522 | 74,989 | 5,531.7 | 5,359 / 8,215 | 0 | 1 | 18.20 | [resonate-tidb-aws-ap-northeast-1-chain-1792879780](runs/resonate-tidb-aws-ap-northeast-1-chain-1792879780/) |
| Resonate (raw) | tidb-aws-eu-central-1 | 20 / 10 | 20/20 | 0.09 | 100,119 | 109,000 | 114,190 | 7,135.2 | 7,542 / 17,440 | 0 | 1 | 26.25 | [resonate-tidb-aws-eu-central-1-chain-1792895930](runs/resonate-tidb-aws-eu-central-1-chain-1792895930/) |
| Resonate (raw) | tidb-aws-us-east-1 | 20 / 10 | 20/20 | 0.04 | 280,030 | 292,243 | 293,087 | 13,768.5 | 23,401 / 104,761 | 3 | 0 | 92.66 | [resonate-tidb-aws-us-east-1-chain-1792897772](runs/resonate-tidb-aws-us-east-1-chain-1792897772/) |
| Resonate (raw) | tidb-aws-us-west-2 | 20 / 10 | 20/20 | 0.08 | 121,425 | 129,075 | 129,912 | 10,034.8 | 10,678 / 16,279 | 0 | 1 | 24.20 | [resonate-tidb-aws-us-west-2-chain-1792913854](runs/resonate-tidb-aws-us-west-2-chain-1792913854/) |
| Resonate (raw) | tidb-aws-sa-east-1 | 20 / 10 | 20/20 | 0.06 | 164,261 | 175,901 | 177,875 | 12,354.2 | 14,962 / 29,943 | 0 | 0 | 28.65 | [resonate-tidb-aws-sa-east-1-chain-1792915115](runs/resonate-tidb-aws-sa-east-1-chain-1792915115/) |
| Resonate (raw) | tidb-alicloud-ap-southeast-1 | 20 / 10 | 20/20 | 0.13 | 74,255 | 83,486 | 84,916 | 5,732.1 | 5,759 / 12,086 | 0 | 1 | 28.04 | [resonate-tidb-alicloud-ap-southeast-1-chain-1792916270](runs/resonate-tidb-alicloud-ap-southeast-1-chain-1792916270/) |

#### TiDB burst: 50 one-step workflows at once

| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | sleep late p50 / p99 ms | errors | server CPU % | load1 | run |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| Resonate (raw) | tidb-aws-ap-southeast-1 | 50 / 50 | 50/50 | 1.25 | 36,124 | 39,993 | 39,996 | 18,735.7 | – | 0 | 1 | 27.97 | [resonate-tidb-aws-ap-southeast-1-burst-1792879087](runs/resonate-tidb-aws-ap-southeast-1-burst-1792879087/) |
| Resonate (raw) | tidb-aws-ap-northeast-1 | 50 / 50 | 50/50 | 2.34 | 19,128 | 20,759 | 21,374 | 8,402.2 | – | 0 | 2 | 20.37 | [resonate-tidb-aws-ap-northeast-1-burst-1792879924](runs/resonate-tidb-aws-ap-northeast-1-burst-1792879924/) |
| Resonate (raw) | tidb-aws-eu-central-1 | 50 / 50 | 18/50 | 0.14 | 121,019 | 124,093 | 124,526 | 59,580.3 | – | 75 | 1 | 23.48 | [resonate-tidb-aws-eu-central-1-burst-1792896144](runs/resonate-tidb-aws-eu-central-1-burst-1792896144/) |
| Resonate (raw) | tidb-aws-us-east-1 | 50 / 50 | 50/50 | 0.99 | 44,141 | 50,152 | 50,393 | 19,354.7 | – | 0 | 1 | 57.72 | [resonate-tidb-aws-us-east-1-burst-1792898250](runs/resonate-tidb-aws-us-east-1-burst-1792898250/) |
| Resonate (raw) | tidb-aws-us-west-2 | 50 / 50 | 50/50 | 1.10 | 30,712 | 37,265 | 45,300 | 14,948.4 | – | 0 | 1 | 52.51 | [resonate-tidb-aws-us-west-2-burst-1792914105](runs/resonate-tidb-aws-us-west-2-burst-1792914105/) |
| Resonate (raw) | tidb-aws-sa-east-1 | 50 / 50 | 50/50 | 0.87 | 47,929 | 56,706 | 57,181 | 23,172.1 | – | 0 | 1 | 19.47 | [resonate-tidb-aws-sa-east-1-burst-1792915452](runs/resonate-tidb-aws-sa-east-1-burst-1792915452/) |
| Resonate (raw) | tidb-alicloud-ap-southeast-1 | 50 / 50 | 50/50 | 2.37 | 18,814 | 20,861 | 21,071 | 9,001.0 | – | 0 | 2 | 44.88 | [resonate-tidb-alicloud-ap-southeast-1-burst-1792916424](runs/resonate-tidb-alicloud-ap-southeast-1-burst-1792916424/) |

Round trips:

| Region | TCP connect min / p50 ms (each pass) |
|---|---|
| alicloud-ap-southeast-1 | 97 / 302; 113 / 378 |
| aws-ap-northeast-1 | 117 / 512; 77 / 295 |
| aws-ap-southeast-1 | 50 / 308; 120 / 305; 80 / 277 |
| aws-eu-central-1 | 52 / 291; 24 / 76; 38 / 261 |
| aws-sa-east-1 | 106 / 303; 18 / 120 |
| aws-us-east-1 | 61 / 301; 18 / 226 |
| aws-us-west-2 | 50 / 303; 22 / 136; 18 / 125 |

## Timers

Sleep lateness (wake-up observed − due time) from the chain runs, p50 / p99:

| System | Backend | Run note | FW11 (SQL timer wheel) applies | late p50 ms | late p95 ms | late p99 ms | max ms | run |
|---|---|---|---|---:|---:|---:|---:|---|
| resonate | tikv |  | no | 2,541 | 8,836 | 16,271 | 17,085 | resonate-tikv-chain-1792916950 |
| resonate | tikv | same build, 6 min earlier | no | 1,588 | 4,837 | 14,553 | 14,806 | resonate-tikv-chain-1792916260 |
| resonate | tikv | same build, fresh TiKV | no | 2,003 | 6,724 | 12,850 | 15,212 | resonate-tikv-chain-1792916086 |
| resonate | tikv | same build, server tracing off | no | 1,562 | 6,379 | 7,809 | 9,077 | resonate-tikv-chain-1792916456 |
| resonate | postgres |  | yes | 114 | 640 | 811 | 913 | resonate-postgres-chain-1792916856 |
| resonate | sqlite |  | yes | 333 | 6,229 | 16,937 | 18,622 | resonate-sqlite-chain-1792916684 |
| resonate | tikv | WIP FW1, warm | no | 567 | 8,498 | 9,543 | 9,840 | resonate-tikv-chain-1792880206 |
| resonate | tikv | WIP FW1, fresh TiKV | no | 1,846 | 8,580 | 22,863 | 32,410 | resonate-tikv-chain-1792879987 |
| loams-durable | tikv |  | no | 251 | 2,216 | 3,168 | 3,306 | loams-durable-tikv-chain-1792876605 |
| resonate | sqlite |  | yes | 18 | 4,455 | 6,954 | 7,591 | resonate-sqlite-chain-1792876519 |
| resonate | postgres |  | yes | 358 | 3,838 | 7,164 | 30,168 | resonate-postgres-chain-1792879084 |
| temporal | postgres |  | no | 42 | 1,427 | 4,637 | 12,571 | temporal-postgres-chain-1792876782 |
| airflow | postgres | polling every 1 s | no | 2,386 | 4,007 | 6,070 | 6,481 | airflow-postgres-chain-1792913275 |
| airflow | postgres | polling every 100 ms (earlier pass) | no | 3,301 | 5,310 | 6,712 | 8,620 | airflow-postgres-chain-1792878109 |
| resonate | tidb-aws-ap-southeast-1 | includes WAN round trips | yes | 5,667 | 7,622 | 9,763 | 12,849 | resonate-tidb-aws-ap-southeast-1-chain-1792878945 |
| resonate | tidb-aws-ap-northeast-1 | includes WAN round trips | yes | 5,359 | 8,028 | 8,215 | 8,216 | resonate-tidb-aws-ap-northeast-1-chain-1792879780 |
| resonate | tidb-aws-eu-central-1 | includes WAN round trips | yes | 7,542 | 16,609 | 17,440 | 17,647 | resonate-tidb-aws-eu-central-1-chain-1792895930 |
| resonate | tidb-aws-us-east-1 | includes WAN round trips | yes | 23,401 | 63,176 | 104,761 | 105,571 | resonate-tidb-aws-us-east-1-chain-1792897772 |
| resonate | tidb-aws-us-west-2 | includes WAN round trips | yes | 10,678 | 15,580 | 16,279 | 25,223 | resonate-tidb-aws-us-west-2-chain-1792913854 |
| resonate | tidb-aws-sa-east-1 | includes WAN round trips | yes | 14,962 | 28,514 | 29,943 | 30,422 | resonate-tidb-aws-sa-east-1-chain-1792915115 |
| resonate | tidb-alicloud-ap-southeast-1 | includes WAN round trips | yes | 5,759 | 9,202 | 12,086 | 12,511 | resonate-tidb-alicloud-ap-southeast-1-chain-1792916270 |

The rows are the chain runs of tables A and B. The SQL stores (SQLite, Postgres, MySQL/TiDB) are affected by the timer-wheel bug FW10 found: after task load the
deadline wheel falls behind (`resonate-timer-wheel`, default `WHEEL_CAPACITY=8192`); the fix is fork task FW11 and
had not landed when these ran, so these SQL timer numbers are not a property of the design. The TiKV store and the
Loams 0.1.1 engine (TiKV) also fire late; that is not explained by FW11 and is reported to the fork lead.
Temporal's timers fire about 1 s late at minimum by design (sub-second sleeps round up), which is why S = 1 s here.

## Cost

**Request Units.** TiDB Cloud Starter bills Request Units but does not show them to an API key: the cluster API
has no usage view and `information_schema.statements_summary` answers "SELECT command denied" (checked 2026-10-25).
So `bench/scripts/measure-ru.sh` ran the same Resonate server against a local TiDB v8.5.8 with resource control and
summed `statements_summary` RU (read and write separately) per workload, then scaled them to Starter with ratios
measured statement by statement on a Starter instance (`ru-spot-check.txt`, 20 of each through
`@@tidb_last_query_info`): writes ×2.26 (INSERT 2.90 → 6.73, UPDATE 3.28 → 7.23), reads ×1.5
(`SELECT … FOR UPDATE` 1.70 → 2.55; point gets equal). Storage per workflow is the table-size growth after
`ANALYZE`. Treat both as estimates within a factor of about 1.5.

**Prices.** Starter, read 2026-10-25 from <https://www.pingcap.com/tidb-cloud-starter-pricing-details/>: $0.10 per
1 M RUs (Oregon, N. Virginia), $0.12 (Singapore, Tokyo, Frankfurt, Alibaba Cloud Singapore), $0.16 (São Paulo); row
storage $0.20-0.38 per GiB-month. Free per instance per month, for the first five instances of an organization:
50 M RUs, 5 GiB row storage, 5 GiB columnar. TiKV: three nodes of the 2 vCPU / 8 GiB shape with a 50 GB disk, PD and
TiKV on each, at the unit prices loams.dev/cost uses (`lib/cloud-prices.ts` in ostrium-labs/loams-site, prices as
of 2026-10-23): AWS t3.large + gp3, GCP e2-standard-2 + pd-balanced, Azure D2s v5 + E6. A single-VM row is the
laptop-like, unreplicated case. The Resonate server's own VM is the same in both and is left out.

| Workload | RU / workflow | Stored KiB / workflow | Free 50M RU covers (workflows/month) | Starter $ per 1M workflows (us-east-1 … sa-east-1) |
|---|---:|---:|---:|---|
| fanout | 737 | 3.6 | 67,802 | $73.74 – $117.99 |
| chain | 1,564 | 4.3 | 31,975 | $156.37 – $250.19 |
| burst | 150 | 0.8 | 332,258 | $15.05 – $24.08 |

| Workload | TiKV deployment | $ / month | Break-even vs Starter us-east-1 (workflows/month) | vs Starter Singapore | TiKV capacity at the measured request rate (workflows/month) |
|---|---|---:|---:|---:|---:|
| fanout | aws (us-east-1, t3.large + 50 GB gp3) ×3 | 194 | 2,701,349 | 2,262,424 | 317,949,773 |
| fanout | gcp (us-central1, e2-standard-2 + 50 GB pd-balanced) ×3 | 162 | 2,261,272 | 1,895,694 | 317,949,773 |
| fanout | azure (East US, D2s v5 + E6 64 GiB) ×3 | 225 | 3,114,020 | 2,606,317 | 317,949,773 |
| fanout | single VM, aws t3.large + 50 GB gp3 (no replication, not HA) | 65 | 945,651 | 799,343 | 317,949,773 |
| chain | aws (us-east-1, t3.large + 50 GB gp3) ×3 | 194 | 1,273,954 | 1,066,958 | 212,123,644 |
| chain | gcp (us-central1, e2-standard-2 + 50 GB pd-balanced) ×3 | 162 | 1,066,414 | 894,008 | 212,123,644 |
| chain | azure (East US, D2s v5 + E6 64 GiB) ×3 | 225 | 1,468,570 | 1,229,138 | 212,123,644 |
| chain | single VM, aws t3.large + 50 GB gp3 (no replication, not HA) | 65 | 445,968 | 376,970 | 212,123,644 |
| burst | aws (us-east-1, t3.large + 50 GB gp3) ×3 | 194 | 13,237,672 | 11,086,770 | 1,430,773,979 |
| burst | gcp (us-central1, e2-standard-2 + 50 GB pd-balanced) ×3 | 162 | 11,081,122 | 9,289,645 | 1,430,773,979 |
| burst | azure (East US, D2s v5 + E6 64 GiB) ×3 | 225 | 15,259,924 | 12,771,980 | 1,430,773,979 |
| burst | single VM, aws t3.large + 50 GB gp3 (no replication, not HA) | 65 | 4,634,062 | 3,917,095 | 1,430,773,979 |

TiKV capacity basis: 2,178 protocol requests/s (the best raw-Resonate-on-TiKV run, one laptop node); requests per workflow: burst 4.0, chain 27.0, fanout 18.0.

**How to read it.** Below the free allowance Starter costs nothing. Above it, Starter grows linearly with work;
TiKV is flat until it runs out of capacity. Break-even is the workflow count per month where Starter's RU bill
equals the TiKV cluster's monthly price (storage left out on both sides: without the fork's retention, FW5, both
grow with history — ~3.6 KiB per fan-out workflow, so 1 M retained fan-out workflows are ~3.4 GiB, inside Starter's
5 GiB free row storage). The TiKV capacity column applies the best request rate one laptop TiKV node sustained (2,178
protocol requests/s, the WIP FW1 build in the quieter hour; the final build reached 870-890 requests/s with the host
at load 26-40) to each workload; a 3 × 2 vCPU cluster with replication factor 3 has not been measured and could be
lower. Either way the break-even points sit far below the capacity.

## Traces and SigNoz

Representative traces (the run's median workflow), from SigNoz:

| | |
|---|---|
| Raw Resonate on SQLite, fan-out: client → gateway → `process` → `sqlite.transact` | ![](screenshots/trace-resonate-sqlite-fanout-1792876507.png) |
| Raw Resonate on TiDB Cloud Singapore, fan-out: the 8 parallel settles serialize (18-20 s `mysql.transact` each) | ![](screenshots/trace-resonate-tidb-aws-ap-southeast-1-fanout-1792878546.png) |
| Raw Resonate on TiDB Cloud Singapore, chain | ![](screenshots/trace-resonate-tidb-aws-ap-southeast-1-chain-1792878945.png) |
| Raw Resonate on Postgres, chain (timers, suspend, re-acquire) | ![](screenshots/trace-resonate-postgres-chain-1792879084.png) |
| Raw Resonate on TiKV (final build), fan-out: client → gateway → `process` → `resonate.blob.batch` → `blob.get`/`blob.put` | ![](screenshots/trace-resonate-tikv-fanout-1792916938.png) |
| Raw Resonate on TiKV (final build), chain: each wake-up (`route execute` → `deliver http`) lands seconds after its 1 s timer | ![](screenshots/trace-resonate-tikv-chain-1792916950.png) |
| Raw Resonate on TiKV (WIP FW1 build), fan-out (client spans only) | ![](screenshots/trace-resonate-tikv-fanout-1792880201.png) |
| Raw Resonate on TiDB Cloud N. Virginia, fan-out | ![](screenshots/trace-resonate-tidb-aws-us-east-1-fanout-1792896380.png) |
| Loams 0.1.1 `/durable/` on TiKV, chain (client spans) | ![](screenshots/trace-loams-durable-tikv-chain-1792876605.png) |
| Stock Temporal, fan-out (Go SDK interceptor spans) | ![](screenshots/trace-temporal-postgres-fanout-1792876726.png) |
| Airflow, fan-out (rbench's REST calls) | ![](screenshots/trace-airflow-postgres-fanout-1792876931.png) |
| Service map | ![](screenshots/signoz-service-map.png) |
| Services (p99, error rate, ops/s) | ![](screenshots/signoz-services.png) |

There is no "Temporal via gateway" trace: that leg did not run. Temporal's server exported its own spans
(`temporal-server.{frontend,history,matching,worker}`, about 37,000 traces), but none of them joined the SDK's
traces in these runs, so the Temporal screenshot shows the SDK side only. Airflow exported its own spans as service
`airflow`; they do not continue rbench's trace either (rbench passes `traceparent` in the Dag run's `conf`, which
Airflow does not read).

**Reproduce the views.** `podman compose -p rbench-signoz -f bench/stacks/signoz/compose.yaml up -d`, open
<http://127.0.0.1:13301>, create the first user, then either run the legs (they export to 127.0.0.1:4317) or replay
the files in `traces/` into the collector. The screenshots come from `bench/scripts/pick-traces.py` (the median
`rbench.workflow` of each run) and `bench/scripts/shots.js` (playwright-core 1.63.0); `screenshots/spec.json` lists
the trace IDs. SigNoz's own counts can include duplicates (the collector retries into it); the span files do not.

## Caveats

- **One laptop, shared.** Client, server, store and the trace pipeline share 14 cores with other agents' builds.
  The same leg varied a lot between a busier and a quieter hour (SQLite burst: 19/s with the server at 5% CPU
  waiting on fsync, then 184/s). Compare legs within one table, and check the load column.
- **One node of everything.** TiKV, PD, Postgres and TiDB ran as single nodes; replication factor 1 for TiKV.
- **WAN to TiDB Cloud.** The Starter numbers are dominated by ~300 ms round trips from India; they are not what a
  Resonate server in the same cloud region would see. Free-tier throttling could not be told apart from WAN latency
  (Starter exposes no RU or throttling metrics to an API key).
- **Request Units are estimated** (see Cost): Starter does not let a user read RU consumption, so they were measured
  on a local TiDB and scaled by per-statement ratios measured on Starter.
- **The host's load varied from 14 to over 200** while these ran (other agents' builds; one reboot). The same leg
  moved a lot between hours (raw Resonate on TiKV fan-out: 121/s at load 20, 37-49/s at load 26-40; SQLite burst
  19/s to 184/s). Runs cut by a reboot or a network outage are in `runs-interrupted/` and were run again.
- **The WIP FW1 build** hit region errors on a fresh TiKV; the merged build did not.
- **Airflow** answered 500 to many of rbench's status polls (2,168 in the 100 ms pass, 451 in the 1 s pass) and
  could not take a 500-run burst in either pass; a 100-run burst completed 92/100 with a p50 of 21 minutes (100 ms
  polling, `results.md`).
- **Not like for like everywhere.** Raw Resonate here is a protocol-level worker in the load generator (no SDK
  overhead); Temporal includes its SDK worker; Airflow includes its scheduler and task start-up per step.

## Cloud resources

Every TiDB Cloud Starter cluster this benchmark created (name `rbench-<region>-<time>`, label `rbench=1`,
spending limit 0), and when it was deleted. After the last deletion the organization's cluster list was empty, and
the billing API's October bill read $0.

| Cluster ID | Name | Region | Deleted |
|---|---|---|---|
| 10304496918315519915 | rbench-aws-ap-southeast-1-10241815 | aws-ap-southeast-1 | deleted 2026-10-24T16:12:15Z (confirmed by list; GET answers 403) |
| 10597552624216414813 | rbench-aws-ap-southeast-1-10242156 | aws-ap-southeast-1 | deleted 2026-10-24T16:40:02Z |
| 10523604401741989246 | rbench-aws-ap-southeast-1-10242212 | aws-ap-southeast-1 | deleted 2026-10-24T16:58:16Z |
| 10393562956217459868 | rbench-aws-ap-northeast-1-10242228 | aws-ap-northeast-1 | deleted 2026-10-24T17:26:17Z |
| 10630155938895170100 | rbench-aws-eu-central-1-10242256 | aws-eu-central-1 | deleted 2026-10-24T17:49:22Z |
| 10376215671306572278 | rbench-aws-us-east-1-10242319 | aws-us-east-1 | deleted 2026-10-24T18:11:10Z |
| 10881850573502748041 | rbench-aws-us-west-2-10242341 | aws-us-west-2 | deleted 2026-10-24T18:32:24Z |
| 10545335814766925086 | rbench-aws-sa-east-1-10250002 | aws-sa-east-1 | deleted 2026-10-24T18:54:35Z |
| 10611905280523180695 | rbench-alicloud-ap-southeast-1-10250024 | alicloud-ap-southeast-1 | deleted 2026-10-24T19:05:20Z |
| 10440154324872932374 | rbench-aws-us-west-2-10250313 | aws-us-west-2 | deleted 2026-10-25T02:23:37Z |
| 10507940613578680152 | rbench-aws-ap-southeast-1-10250318 | aws-ap-southeast-1 | deleted 2026-10-24T21:59:09Z |
| 10978188805670432127 | rbench-aws-ap-northeast-1-10250329 | aws-ap-northeast-1 | deleted 2026-10-24T22:12:39Z |
| 10830883098691749847 | rbench-aws-eu-central-1-10250342 | aws-eu-central-1 | deleted 2026-10-25T02:23:02Z |
| 10530897403818315396 | rbench-aws-eu-central-1-10250753 | aws-eu-central-1 | deleted 2026-10-25T02:45:05Z |
| 10316009973155987711 | rbench-aws-us-east-1-10250815 | aws-us-east-1 | deleted 2026-10-25T03:18:51Z |
| 10884622247644669918 | rbench-aws-us-west-2-10250848 | aws-us-west-2 | deleted 2026-10-25T07:22:06Z |
| 10374824742992233932 | rbench-aws-us-west-2-10251253 | aws-us-west-2 | deleted 2026-10-25T07:43:00Z |
| 10520422919636842564 | rbench-aws-sa-east-1-10251313 | aws-sa-east-1 | deleted 2026-10-25T08:05:36Z |
| 10106305941070618337 | rbench-alicloud-ap-southeast-1-10251335 | alicloud-ap-southeast-1 | deleted 2026-10-25T08:21:08Z |

## Files

- `runs/<run-id>/`: `summary.json`, `workflows.csv`, `steps.csv`, `requests.csv`, `resources.csv`, `env.json`,
  (Goose's HTML report came out empty with `NoPrintMetrics` set and is not included.)
- `runs-interrupted/`: runs cut by a network outage or a reboot of the host, excluded and run again.
- `results.csv`, `results.md`: every run. `headline-runs.txt`: the runs in the tables above.
- `tidbcloud-rtt.json`, `ru-per-workflow.json`, `ru-spot-check.{sql,txt}`, `cost.json`, `cost.md`.
- `traces/`: spans, `manifest.json` with counts per service. `charts/`, `screenshots/`.

Reproduce: `bench/scripts/run-local-all.sh`, `bench/scripts/run-tidbcloud.sh <region>`,
`bench/scripts/measure-ru.sh`, then `rbench report`, `bench/scripts/{headline,cost,charts}.py`.
