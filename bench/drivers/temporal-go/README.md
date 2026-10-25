# rbench driver: Temporal (Go SDK)

The native driver for the Temporal leg, following `../README.md`. One binary runs the worker,
the starter, or both in one process. The stack is `bench/stacks/temporal`.

## Build

The SDK needs Go 1.26, so build in the pinned Go image (module and build caches are the
host's defaults):

```sh
cd bench/drivers/temporal-go
podman run --rm -v $PWD:/src:z -v $HOME/go/pkg/mod:/go/pkg/mod:z \
  -v $HOME/.cache/go-build:/root/.cache/go-build:z -w /src \
  -e GOFLAGS=-p=4 -e GOTOOLCHAIN=local -e CGO_ENABLED=0 \
  docker.io/library/golang:1.26.8@sha256:eb36c1664dd974cde625f736e02c204383deebe03977365caaec5bf49f794348 \
  sh -c 'go build -trimpath -o bin/rbench-temporal . && go build -trimpath -o bin/otlp-sink ./cmd/otlp-sink'
```

Pins (go.mod, go.sum committed): `go.temporal.io/sdk v1.49.0`,
`go.temporal.io/sdk/contrib/opentelemetry v0.8.1`, `go.opentelemetry.io/otel`, `/sdk`,
`/trace` and `/exporters/otlp/otlptrace/otlptracegrpc` `v1.44.0` (1.44.0, not 1.43.0: the
SDK's dependency graph raises otel to 1.44.0, so all OTel modules are pinned there).

## Run

```sh
RBENCH_WORKLOAD=fanout RBENCH_N=20 RBENCH_K=4 RBENCH_CONCURRENCY=10 RBENCH_RUN_ID=r1 \
  bin/rbench-temporal -mode=all > r1.jsonl
```

Environment, as the contract: `RBENCH_WORKLOAD` (`fanout`, `chain`, `burst`), `RBENCH_N`,
`RBENCH_K` (8), `RBENCH_L` (5), `RBENCH_SLEEP_MS` (1000), `RBENCH_CONCURRENCY` (10),
`RBENCH_RUN_ID` (default `r<unix time>`), plus `TEMPORAL_ADDRESS` (`127.0.0.1:17233`),
`TEMPORAL_NAMESPACE` (`default`), `OTEL_EXPORTER_OTLP_ENDPOINT` (`http://127.0.0.1:4317`),
`OTEL_SERVICE_NAME` (`rbench-temporal-go`).

Flags (logged to stderr at start): `-mode` (`all`, `worker`, `start`), `-task-queue`
(`rbench`), `-max-activities` (200), `-max-workflow-tasks` (200), `-activity-pollers` (8),
`-workflow-pollers` (8), `-sticky-cache` (10000), `-burst-start-workers` (256),
`-activity-timeout` (10s, the activities' StartToCloseTimeout), `-wf-timeout` (10m).

Exit status: 0 after a `done` line (failures are counted in it), 2 when it cannot start
(bad env, Temporal unreachable).

## What it does

- Workflows: `FanOut(k)` runs k no-op activities in parallel and joins; `Chain(l, sleepMs)`
  runs l no-op activities in sequence, each followed by `workflow.Sleep` (a durable timer);
  `Single()` runs one (for `burst`). Workflow IDs are `<run>-<workload>-<i>`.
- Activities set only `StartToCloseTimeout`; the retry policy is Temporal's default.
- `fanout`/`chain`: at most `RBENCH_CONCURRENCY` workflows in flight, each start then `Get`.
  `burst`: all N started first (by `-burst-start-workers` goroutines), then all awaited, so a
  burst workflow's `end_ns` is no earlier than the end of the start phase.
- Records:
  - `wf`: `start_ns` just before `ExecuteWorkflow`, `end_ns` when `run.Get` returns,
    `retries` = sum of (attempt - 1) over its activities (each activity returns its attempt).
  - `step` `activity`: written by the activity itself: `sched_ns` = `ActivityInfo.ScheduledTime`,
    `start_ns` = `ActivityInfo.StartedTime`, `end_ns` when the body returns. These come from the
    process that runs the worker (in `-mode=start` they are on the worker's stdout).
  - `step` `sleep`: the workflow returns `workflow.Now()` before and after each sleep, and the
    starter writes them after `Get` (`sched_ns` = `start_ns`). `workflow.Now()` is the
    workflow task's time, so `end_ns` is when the workflow task after the timer started.
  - `done`: last line.
- Tracing: the Temporal OTel tracing interceptor on the client (the SDK applies it to the
  worker too), W3C `traceparent` in the Temporal headers, one root span `rbench.workflow` per
  workflow with `rbench.run`, `rbench.workload`, `rbench.wf_id`. Export is the batch
  processor over OTLP/gRPC: with no collector listening the run does not slow down or fail;
  the error is logged once and spans are dropped. Flushed on exit with a 5 s bound.

## Observed on Temporal 1.32.1

- Durable timers under one second fire after about one second (200 ms sleeps took
  1008-1480 ms). Use `RBENCH_SLEEP_MS` of 1000 or more when comparing timer cost.
- `ScheduledTime` and `StartedTime` are usually microseconds apart when the task is
  sync-matched to a waiting poller, so `start_ns - sched_ns` is not a queue-wait measure there.

## otlp-sink

`cmd/otlp-sink` is a minimal OTLP/gRPC trace receiver for checking that spans arrive: it counts
spans per service and name and prints them on SIGINT. Not a collector.
