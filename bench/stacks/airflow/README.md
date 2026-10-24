# rbench stack: Apache Airflow 3.3.2 + Edge executor

The Airflow leg of the durable-execution benchmark. `rbench` drives it over Airflow's
public REST API (`/api/v2`); the workload contract is `../../drivers/README.md`.

| Piece | Pin |
|---|---|
| Airflow | `docker.io/apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1` |
| Edge executor | `apache-airflow-providers-edge3==4.3.2`, installed with `constraints-3.3.2/constraints-3.12.txt` |
| Metadata DB | `docker.io/library/postgres:17.10@sha256:7958605b474b3d264a969cb3a123d6aa00ad1e1fe9da8a69984dabb704d93317` |

Services (compose project `rbench-airflow`, containers `rbench-airflow-*`): `postgres`,
`airflow-init` (one-shot `airflow db migrate`), `airflow-api-server`, `airflow-scheduler`
(one; DX0 runs two), `airflow-dag-processor`, `airflow-triggerer`, `airflow-edge-worker-1`
and `-2` (queue `default`). The wiring follows DX0's airflow profile in
`ostrium-labs/loams` `deploy/release/compose.compute.yaml`.

**edge3 install: Containerfile, not `_PIP_ADDITIONAL_REQUIREMENTS`.** DX0 installs edge3 at
container start; here that would run pip in six containers on every `up`. `Containerfile`
is `FROM` the pinned digest and pip-installs `apache-airflow==3.3.2` +
`apache-airflow-providers-edge3==4.3.2` with the same constraints file, once, locally.

## Up / down

```sh
cd bench/stacks/airflow
podman build -t localhost/rbench-airflow:3.3.2-edge3-4.3.2 -f Containerfile .   # once
podman compose -p rbench-airflow up -d
# ready when the API is healthy and the three DAGs are registered (observed 70 s to 8 min on a loaded host):
until curl -fsS http://127.0.0.1:18080/api/v2/monitor/health >/dev/null; do sleep 2; done
podman compose -p rbench-airflow down -v      # stop and drop the metadata DB volume
```

Host ports (127.0.0.1 only): API server `127.0.0.1:18080`, Postgres `127.0.0.1:15434`
(user/password/db `airflow`). Memory limits: Postgres 1g, api-server 2g, scheduler,
dag-processor, triggerer 1g each, edge workers 3g each.

`GET /api/v2/monitor/health` (no auth) reports `metadatabase`, `scheduler`, `triggerer`,
`dag_processor`, each `{"status": "healthy", ...}`. DAG registration lags the health
check: wait until `GET /api/v2/dags?dag_id_pattern=rbench` returns `total_entries: 3`
(DAGs are created unpaused).

## Auth (simple auth manager)

Fixed admin: username `admin`, password `admin` (test-only; the api-server writes
`{"admin": "admin"}` to the passwords file at start). Every `/api/v2` call except
`/monitor/health` needs a bearer token; without one it returns 401.

```sh
curl -sS -X POST http://127.0.0.1:18080/auth/token \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'
# 201 Created  {"access_token": "<JWT>"}
```

The body may also be `application/x-www-form-urlencoded`. The token is an HS512 JWT,
valid 24 h (`[api_auth] jwt_expiration_time` = 86400): fetch one per bench run and send
`Authorization: Bearer <JWT>`.

## REST calls (observed on 3.3.2)

Timestamps are ISO-8601 UTC with microseconds and a `Z` suffix, e.g.
`"2026-10-24T12:23:30.185298Z"`, or `null`.

### Trigger a run

```
POST /api/v2/dags/{dag_id}/dagRuns
{"dag_run_id": "r1-fanout-17", "logical_date": null, "conf": {"k": 8}}
```

- `logical_date` is **required** (send `null`); omitting it returns **422**.
- `dag_run_id` is optional (Airflow then generates `manual__<run_after ISO>`); a repeated
  `dag_run_id` returns **409**. Using the rbench `wf_id` as `dag_run_id` works and avoids
  parsing the response.
- Success is **200** with the DAG run object (below), `state: "queued"`.

### Poll a run

```
GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}
```

DAG run object fields: `dag_run_id`, `dag_id`, `logical_date` (null), `queued_at`,
`start_date`, `end_date`, `duration` (seconds, float, null until done), `run_after`,
`last_scheduling_decision`, `run_type` (`"manual"`), `state`, `triggered_by`
(`"rest_api"`), `triggering_user_name`, `conf`, `note`, `dag_versions`, `bundle_version`,
`dag_display_name`, `partition_key`, `partition_date`.

`state`: `queued` → `running` → `success` | `failed`. Terminal = `success` or `failed`.
A chain run with `l` < 5 ends in `success` (the skipped tail does not fail it).

### List a run's task instances

```
GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances?limit=100
```

Response: `{"task_instances": [...], "total_entries": N, "total_entries_limit": ...,
"next_cursor": ..., "previous_cursor": ...}`. Page size is capped at 100
(`[api] maximum_page_limit`); for fanout `k` > 97 page with `offset`. Per task instance:

| Field | Meaning for rbench |
|---|---|
| `task_id` | `plan`, `noop`, `join`; `step_0..4`, `sleep_0..4`; `noop` (single) |
| `map_index` | mapped index for fanout `noop` (0..k-1), else `-1` |
| `state` | `success`, `failed`, `skipped`, `upstream_failed`, `deferred`, `running`, `queued`, `scheduled`, or `null` (not yet scheduled) |
| `scheduled_when` | when the scheduler marked it scheduled |
| `queued_when` | when it was queued to the executor (`sched_ns` in the record) |
| `start_date` | body start (`start_ns`) |
| `end_date` | end (`end_ns`) |
| `duration` | seconds, float |
| `try_number` | attempts; `retries` = `try_number - 1` |
| `hostname` | `edge-worker-1` / `edge-worker-2` (a `sleep_i` shows the worker that deferred it); `""` for tasks never run on a worker |
| `trigger`, `triggerer_job` | set while a `sleep_i` is `deferred` |

Other fields present: `id`, `dag_id`, `dag_run_id`, `logical_date`, `run_after`,
`max_tries`, `task_display_name`, `dag_display_name`, `unixname`, `pool`, `pool_slots`,
`queue`, `priority_weight`, `operator`, `operator_name`, `pid`, `executor`,
`executor_config`, `note`, `rendered_map_index`, `rendered_fields`, `dag_version`.

Notes for the harness:
- Mapped `noop` instances only appear after `plan` succeeds (dynamic task mapping
  expands at run time); before that the list holds `plan` and `join`.
- `sleep_i` (`kind: sleep`) never returns to a worker: it defers on a worker
  (`start_date` = sleep start) and the triggerer completes it (`end_date`).
- Tasks skipped by a chain short-circuit have `start_date == end_date` and no
  `queued_when`; drop `skipped` instances from step records.

## DAGs (`dags/`)

All `schedule=None`, unpaused at creation, triggered by REST with `conf`. Steps use the
Task SDK (`airflow.sdk`) and do no I/O.

| DAG | conf (defaults) | Graph |
|---|---|---|
| `rbench_fanout` | `k` (8) | `plan` → `noop.expand(i=plan())` (k mapped instances) → `join` |
| `rbench_chain` | `l` (5), `sleep_ms` (1000) | `step_0` → `sleep_0` → … → `step_4` → `sleep_4` |
| `rbench_single` | none | `noop` (burst unit) |

- **fanout:** dynamic task mapping needs the list at run time, so `plan` (one extra
  task) turns `conf.k` into `range(k)`. Each `noop(i)` returns `i`; `join` returns the count.
- **chain, fixed max L = 5:** the task graph is fixed at parse time. Each `step_i` is a
  short-circuit task that returns `{"i": i}` for `i < l`; `step_l` (when `l` < 5) returns
  `False` and skips the rest of the chain in one go. So `l` < 5 costs one extra worker task
  (`step_l`); `l` > 5 is clamped to 5.
- **durable sleep:** `sleep_i` is a small `DurableSleep` operator that defers to
  `DateTimeTrigger(moment=now + sleep_ms, end_from_trigger=True)`. The wait lives in the
  triggerer, with the trigger row in Postgres; it survives a triggerer restart (verified: a
  triggerer recreated mid-sleep picked the triggers up and the runs succeeded). `S` is
  measured from the sleep task's start, which is just after `step_i` ended plus scheduling
  latency. `TimeDeltaSensor(deferrable=True)` was not used: its delta counts from the run's
  `run_after`, not from the previous step.

## Knobs

| Knob (env) | Value | Default | Why |
|---|---|---|---|
| `[core] parallelism` | 256 | 32 | scheduler-wide running-task cap; above worker slots (32) so workers bound it |
| `[core] max_active_tasks_per_dag` | 1024 | 16 | fanout with C concurrent runs × k tasks in one DAG |
| `[core] max_active_runs_per_dag` | 256 | 16 | ≥ C concurrent runs (64 planned) and burst N queued; DAGs also set `max_active_runs` (256; single 1024) |
| `[core] default_pool_task_slot_count` | 1024 | 128 | the default pool would otherwise cap running+queued tasks at 128 |
| `[core] dags_are_paused_at_creation` | false | true | DAGs runnable as soon as parsed |
| `[scheduler] scheduler_heartbeat_sec` | 1 | 5 | scheduler job heartbeat |
| `[scheduler] max_tis_per_query` | 512 | 16 | tasks examined/queued per scheduler loop (burst) |
| `[scheduler] max_dagruns_per_loop_to_schedule` | 256 | 20 | runs advanced per scheduler loop |
| `[scheduler] scheduler_idle_sleep_time` | 1 (default) | 1 | loop sleep when idle |
| `[dag_processor] refresh_interval` | 5 | 300 | bundle re-list (new DAG files picked up) |
| `[dag_processor] min_file_process_interval` | 30 (default) | 30 | re-parse interval; DAG files are static |
| `[edge] job_poll_interval` | 1 | 5 | worker poll for new jobs and job-done check, in whole seconds (int; 1 is the minimum) |
| `[edge] heartbeat_interval` | 10 | 30 | worker status heartbeat |
| `[edge] worker_concurrency` / `--concurrency` | 16 per worker (32 total) | 8 | parallel task processes per edge worker |
| `[edge] push_logs` | false | true | no task-log upload to the api-server (logs stay in the worker container) |
| `[triggerer] capacity` | 1000 (default) | 1000 | concurrent triggers (deferred sleeps) |
| `[workers] execution_api_timeout` | 30 | 5.0 | see "Known issues": a 5 s timeout makes the task runner retry a non-idempotent XCom write under load |
| `[api] workers` | 4 (default) | 4 | uvicorn workers serving REST, Execution API and Edge API |
| Postgres `max_connections` | 400 | 100 | 8 Airflow processes plus 4 api workers and up to 32 task processes |

Fixed, not tuning: `[core] executor` = `airflow.providers.edge3.executors.EdgeExecutor`,
`[core] execution_api_server_url` = `http://airflow-api-server:8080/execution/`,
`[edge] api_enabled` = true, `[edge] api_url` =
`http://airflow-api-server:8080/edge_worker/v1/rpcapi`, `[api_auth] jwt_secret` (fixed
test value), `[core] auth_manager` = SimpleAuthManager.

## OpenTelemetry

Traces are on and export OTLP over **HTTP/protobuf** to
`http://host.containers.internal:4318/v1/traces`, `service.name=rbench-airflow`.

On 3.3.2 `[traces] otel_on=True` switches tracing on, but the exporter is chosen by the
standard OTel env: `OTEL_TRACES_EXPORTER` (default `otlp` = **gRPC**),
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_SERVICE_NAME`. `[traces] otel_host` /
`otel_port` / `otel_ssl_active` / `otel_service` are a deprecated back-compat path: they only
set the endpoint, and the exporter stays gRPC. The compose file therefore sets
`OTEL_TRACES_EXPORTER=otlp_proto_http` and the endpoint env. No extra package: the image
already has `opentelemetry-exporter-otlp-proto-http` 1.44.0.

Do not switch to the gRPC exporter: with it, forked children (dag-processor parsers,
triggerer runner, edge-worker task processes) hang in `futex_wait` (gRPC is not fork-safe),
DAGs never register and tasks never start. With HTTP, all services fork cleanly.

The collector must listen on a non-loopback host address (for example `0.0.0.0:4318`):
`host.containers.internal` resolves to 169.254.1.2, which does not reach a listener bound to
`127.0.0.1` only. If nothing listens, Airflow logs export errors and carries on.

Propagating rbench's W3C `traceparent` into Airflow's spans through the REST trigger
call was not tested; until it is, treat rbench's `rbench.workflow` span and Airflow's
spans as separate traces and join them on the DAG run id.

## Known issues

- **Non-idempotent XCom write on retry.** Under heavy host load (load average 15-26
  during verification), a task's Execution API call to store its return value took more
  than the default `[workers] execution_api_timeout` of 5 s; the task runner retried, the
  retry hit `Unique constraint violation` on `xcom`, and the task (and its run) failed.
  Raising the timeout to 30 s avoids the spurious retry. rbench should still report such
  failures, not hide them.
- **Task start cost.** Every task body runs in a fresh forked task process that talks to
  the Execution API; on a loaded laptop a no-op step takes seconds from `start_date` to
  `end_date`. That is Airflow's per-task cost, not the DAG.
- Edge workers and the dag-processor log `This process is multi-threaded, use of fork()
  may lead to deadlocks` on every fork; harmless with the HTTP exporter.

## Verification (2026-10-24)

Final config, fresh stack (`down -v` then `up -d`), runs triggered one at a time by curl
(`POST` trigger, then `GET` poll every 0.1 s). End-to-end = trigger request sent to
`state: success` observed. The host was heavily loaded by other work (load average
19-27 on 16 GB), so treat these as a smoke test, not baseline numbers.

| DAG, conf | Runs | Result | End-to-end, ms (each run) |
|---|---|---|---|
| `rbench_single` | 5 | 5 success | 8766, 10049, 2947, 7305, 2072 |
| `rbench_fanout`, `{"k":4}` | 5 | 5 success | 14702, 11783, 15879, 8843, 9310 |
| `rbench_chain`, `{"l":2,"sleep_ms":200}` | 5 | 5 success | 28415, 22442, 20652, 23761, 27032 |

Last chain run's timeline: `step_0` 5.0 s body, `sleep_0` 3.5 s (200 ms trigger plus the
worker-side defer), `step_1` 3.4 s, `sleep_1` 3.1 s, `step_2` (short-circuit) 1.8 s,
then `sleep_2..step_4` all skipped at the same instant. About 1 s of scheduler/queue gap
between consecutive tasks.

An earlier pass, before `execution_api_timeout` was raised, had 1 of 5 fanout runs fail on
the XCom retry described above. OTLP/HTTP span exports reached a test listener on
`0.0.0.0:4318` (278 export requests over the runs).

The stack was removed afterwards (`podman compose -p rbench-airflow down -v`).
