# Benchmarks

`cargo xtask bench` measures a live server: promise throughput, how long a
worker waits to claim a task, and how late the server's timer fires. It starts
a release server on a fresh database, the way `cargo xtask porcupine` does,
or measures one that is already running.

## Running it

```sh
cd impl/server/core

# SQLite: builds the release server, starts it on a fresh file, runs, stops it.
cargo xtask bench --backend sqlite

# Postgres, MySQL, TiDB: a fresh database is created through the admin URL and
# dropped afterwards (see `cargo xtask --help` for the XTASK_*_ADMIN_URL variables).
cargo xtask bench --backend postgres --postgres-admin-url postgres://postgres:pw@127.0.0.1:5432/postgres

# Blob: the in-process object store.
cargo xtask bench --backend blob

# Any server that is already running. Point it at a fresh database first.
cargo xtask bench --backend tikv --server-url http://127.0.0.1:8001
```

| Option | Default | Meaning |
|---|---|---|
| `--clients N` | 16 | Concurrent clients in each phase |
| `--duration S` | 10 | Seconds each of the create/settle and task phases runs |
| `--payload B` | 256 | Bytes in each promise's param and value |
| `--timers N` | 500 | Promises in the timer phase |
| `--timer-ms T` | 2000 | How far ahead each timer promise's deadline is |
| `--server-url URL` | none | Measure this server instead of starting one |
| `--binary PATH` | none | Start this server binary instead of building one |
| `--timer-grace-ms G` | 30000 | How long after the last deadline a timer may still land |
| `--work-dir DIR` | `<target>/xtask-bench` | Where the started server keeps its database. Not `/tmp`, which may be a tmpfs |
| `--log-level L` | `warn` | Log level of the server the bench starts |
| `--skip cs,task,timer` | none | Phases to leave out |

`tikv` has no server plugin in this build yet (FW1). Until it lands,
`--backend tikv` needs `--server-url` and only labels the report.

The server the bench starts logs at `warn`, so the numbers measure the server
and not the terminal. It inherits the environment, so the `OTEL_*` variables
in [tracing.md](tracing.md) trace a bench run.

The bench prints one JSON line on stdout, which is the record to keep, and a
table on stderr.

## What each number means

**create/settle**: each client creates a promise with a `--payload`-byte
param and settles it with a value of the same size, in a loop, for
`--duration` seconds. Every promise has its own id and origin, so a backend
that serializes per origin is not measured on a single one.

- `create_per_s`, `settle_per_s`: successful requests per second across all
  clients.
- `create_p50_ms` / `create_p99_ms`, `settle_p50_ms` / `settle_p99_ms`:
  client-side latency of one request.

**task claim**: the harness opens one `GET /poll/<group>/<id>` stream per
client. Each client creates a promise targeted at `poll://any@<group>` and
waits for it to run. Whichever stream receives the execute message acquires
the task and fulfills it.

- `claim_p50_ms`, `claim_p95_ms`, `claim_p99_ms`: time from sending the
  create to the answer to `task.acquire`. This is what a worker waits between
  a task being requested and owning it: the create commits, the message is
  routed and delivered, and the acquire commits.
- `dispatch_p50_ms`: the same clock, stopped when the execute message
  arrives.
- `task_e2e_p50_ms` / `task_e2e_p99_ms`: stopped when `task.fulfill` answers.
- `tasks_per_s`: tasks completed per second, with `--clients` in flight.

**timers**: `--timers` promises (tag `resonate:scope=global`, so the deadline
is armed) with `timeoutAt` `--timer-ms` ahead, each with a listener on a
`poll://` group. When the server's timer rejects one as timed out, the unblock
message arrives on the stream.

- `timer_late_p50_ms` … `timer_late_max_ms`: arrival time minus `timeoutAt`,
  on the harness's clock. This covers the timer firing, the transition
  committing and the message being delivered. Against `--server-url` on
  another host, clock skew adds to it.
- `timers_fired` / `timers_created`: a deadline that has not fired
  `--timer-grace-ms` (30 s) after the last one is missing from the count.

`*_errors` counts requests that did not answer 200/201 and tasks that did not
complete within 30 s. A run with errors is not a throughput number.

## Results

Fill one row per run, with the JSON line kept beside it. The numbers are only
comparable between rows from the same machine and load.

| Date | Commit | Backend | Machine | Clients / s / payload | create/s | settle/s | claim p50 / p95 / p99 ms | tasks/s | timer late p50 / p99 / max ms (T) | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-10-24 | `5ea4215e` (fw10) | sqlite | A | 16 / 10 / 256 B | 519 | 519 | 17.4 / 148.7 / 305.1 | 284 | none fired within 30 s (2000) | Defaults. Load average 13 to 41 from other builds. See "Timer finding" |
| 2026-10-24 | `5ea4215e` (fw10) | sqlite | A | 16 / 10 / 256 B | 449 | 449 | 21.5 / 204.9 / 647.6 | 192 | 97 / 30006 / 30012 (2000) | `WHEEL_CAPACITY=256`. Load average 37 to 40 |

Machine A: Intel Core Ultra 5 225H (14 cores, 1 thread per core), 15 GiB RAM,
NVMe with ext4 (`/mnt/Projects`, where the database lived), Linux
7.2.4-cachyos. This is a shared development machine, and other agents' builds
were running during both runs, so treat these as smoke numbers rather than a
baseline. The JSON lines:

```json
{"backend":"sqlite","bench":"resonate","claim_p50_ms":17.37,"claim_p95_ms":148.68,"claim_p99_ms":305.11,"clients":16,"commit":"5ea4215ec558","create_p50_ms":6.82,"create_p99_ms":188.82,"create_per_s":518.75,"cs_errors":0,"dispatch_p50_ms":8.13,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:46503","settle_p50_ms":6.53,"settle_p99_ms":190.29,"settle_per_s":518.75,"task_e2e_p50_ms":27.4,"task_e2e_p99_ms":426.46,"task_errors":0,"tasks_per_s":283.8,"timer_deadline_ms":2000,"timer_late_max_ms":null,"timer_late_p50_ms":null,"timer_late_p95_ms":null,"timer_late_p99_ms":null,"timers_created":500,"timers_fired":0,"unix_ms":1792862868965}
{"backend":"sqlite","bench":"resonate","claim_p50_ms":21.48,"claim_p95_ms":204.91,"claim_p99_ms":647.57,"clients":16,"commit":"5ea4215ec558","create_p50_ms":6.55,"create_p99_ms":237.69,"create_per_s":448.81,"cs_errors":0,"dispatch_p50_ms":9.83,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:38957","settle_p50_ms":6.57,"settle_p99_ms":219.1,"settle_per_s":448.81,"task_e2e_p50_ms":33.94,"task_e2e_p99_ms":776.85,"task_errors":0,"tasks_per_s":191.89,"timer_deadline_ms":2000,"timer_late_max_ms":30012.0,"timer_late_p50_ms":97.0,"timer_late_p95_ms":29988.0,"timer_late_p99_ms":30006.0,"timers_created":500,"timers_fired":500,"unix_ms":1792863013942}
```

## Timer finding (SQL servers)

On SQLite, a timer phase run on its own fires every deadline 2 to 8 ms late.
A timer phase run after the task phase does not:

- **Default wheel (8192 entries):** after 3 s of task load (about 2,500 tasks,
  4 clients), deadlines fired about 11.5 s late. After the default 10 s with
  16 clients, none of 500 had fired 30 s after the last deadline.
- **`RESONATE_SERVERS__SERVER_SQLITE__WHEEL_CAPACITY=256`:** after the same
  3 s of task load, they fired 2 to 3 ms late. After the default run, the
  p50 was 97 ms but the p95 was 30 s. That 30 s is the deadlines evicted from
  the small wheel coming back on the 30 s backfill (`wheel_refresh`).

The likely cause is `resonate-timer-wheel`'s merge. It rebuilds the whole
wheel on every batch, using `Vec::remove(0)` loops and an insertion sort,
which is quadratic in the wheel's size. Every request that arms a deadline
sends one batch, and a task arms two deadlines (its promise timeout and its
retry). So a wheel of a few thousand entries makes the timer task fall behind
its unbounded merge queue, and firing waits behind the backlog. Shrinking the
wheel removes the backlog but evicts near deadlines. Making the merge
`O(n log n)` (or `O(m log n)` per batch) is a change to the verified wheel,
and is not part of this PR.
