# Workload drivers

Every system runs the same three workload shapes and reports the same records, so `rbench`
(the Rust harness in `../rbench`) computes the same metrics for all of them.

HTTP-level systems (raw Resonate, the Loams `/durable/` endpoint, Airflow's REST API) are
driven by `rbench` itself with Goose. SDK-level systems (Temporal) have a native driver here,
run under `rbench exec`, which passes the workload through the environment and reads records
from the driver's stdout.

## Workloads

| Name | Shape | Parameters |
|---|---|---|
| `fanout` | one workflow = K parallel no-op steps, then a join | `RBENCH_K` (default 8) |
| `chain` | one workflow = L sequential steps, each followed by a durable sleep of S ms | `RBENCH_L` (default 5), `RBENCH_SLEEP_MS` (default 1000) |
| `burst` | N one-step workflows all started at once, then awaited | (N is `RBENCH_N`) |

Common: `RBENCH_N` workflows in total, at most `RBENCH_CONCURRENCY` in flight (`burst`
ignores it: all N are started before any is awaited), `RBENCH_RUN_ID` (a prefix for workflow
IDs, unique per run), `RBENCH_SYSTEM`, `RBENCH_BACKEND`. A no-op step returns its input and
does no I/O. Steps have no retries configured beyond the system's default.

Tracing: drivers export OpenTelemetry spans over OTLP to `OTEL_EXPORTER_OTLP_ENDPOINT`
(default `http://127.0.0.1:4317`, gRPC) with `service.name` from `OTEL_SERVICE_NAME`, one
root span per workflow (`rbench.workflow`, attributes `rbench.run`, `rbench.workload`,
`rbench.wf_id`), and propagate W3C `traceparent` into the system's own requests (the SDK's
interceptor where it has one).

## Records (stdout, JSON Lines)

Timestamps are Unix epoch **nanoseconds** from the host's wall clock (all processes run on
one host). One object per line; any other stdout line is ignored, logs go to stderr.

```json
{"type":"wf","wf_id":"r1-fanout-17","start_ns":1,"end_ns":2,"ok":true,"error":null,"retries":0}
{"type":"step","wf_id":"r1-fanout-17","step":"s3","kind":"activity","sched_ns":1,"start_ns":1,"end_ns":2,"ok":true}
{"type":"done","started":100,"completed":100,"failed":0}
```

- `wf`: `start_ns` is just before the start request is sent; `end_ns` is when the driver
  observes the workflow's result. `retries` counts retried attempts the driver saw (activity
  attempts > 1, re-sent requests).
- `step`: `kind` is `activity`, `sleep` or `task`. `sched_ns` is when the step was scheduled
  (as the system reports it, or null), `start_ns` when its body started, `end_ns` when the
  workflow observed its completion (or the body's end when that is all that is visible).
- `done`: the last line, a self-check of the counts.
