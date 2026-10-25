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
| `--max-timer-p99-ms B` | none | Fail the run unless every timer fired and the timer p99 lateness is under `B` ms (the FW11 regression gate) |

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
| 2026-10-25 | `08f73b0d` (fw10, before FW11) | sqlite | A | 16 / 10 / 256 B | 366 | 366 | 17.1 / 482.6 / 6180.6 | 61 | 1212 / 6445 / 6448 (2000), 494 of 500 fired | Defaults, `--max-timer-p99-ms 2000`: fails. Load average 20 to 36 |
| 2026-10-25 | `08f73b0d` (fw10, before FW11) | postgres | A | 16 / 10 / 256 B | 124 | 124 | 98.6 / 631.7 / 1167.4 | 68 | 13395 / 21541 / 21561 (2000) | Postgres 17 in podman, default pool (10). Gate fails. Load average 12 to 20 |
| 2026-10-25 | `9d56a23b` (fw11) | sqlite | A | 16 / 10 / 256 B | 515 | 515 | 8.9 / 58.1 / 101.4 | 681 | 4 / 61 / 70 (2000), 500 of 500 fired | Gate passes. Load average 19 to 33. A run just before it, at load 15 to 35: p99 739 ms, claim p99 2550 ms |
| 2026-10-25 | `9d56a23b` (fw11) | postgres | A | 16 / 10 / 256 B | 144 | 144 | 26.0 / 118.7 / 193.3 | 198 | 11 / 30 / 32 (2000), 500 of 500 fired | As the Postgres row above. Gate passes. Load average 26 to 46 |

Machine A: Intel Core Ultra 5 225H (14 cores, 1 thread per core), 15 GiB RAM,
NVMe with ext4 (`/mnt/Projects`, where the database lived), Linux
7.2.4-cachyos. This is a shared development machine, and other agents' builds
were running during both runs, so treat these as smoke numbers rather than a
baseline. The JSON lines:

```json
{"backend":"sqlite","bench":"resonate","claim_p50_ms":17.37,"claim_p95_ms":148.68,"claim_p99_ms":305.11,"clients":16,"commit":"5ea4215ec558","create_p50_ms":6.82,"create_p99_ms":188.82,"create_per_s":518.75,"cs_errors":0,"dispatch_p50_ms":8.13,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:46503","settle_p50_ms":6.53,"settle_p99_ms":190.29,"settle_per_s":518.75,"task_e2e_p50_ms":27.4,"task_e2e_p99_ms":426.46,"task_errors":0,"tasks_per_s":283.8,"timer_deadline_ms":2000,"timer_late_max_ms":null,"timer_late_p50_ms":null,"timer_late_p95_ms":null,"timer_late_p99_ms":null,"timers_created":500,"timers_fired":0,"unix_ms":1792862868965}
{"backend":"sqlite","bench":"resonate","claim_p50_ms":21.48,"claim_p95_ms":204.91,"claim_p99_ms":647.57,"clients":16,"commit":"5ea4215ec558","create_p50_ms":6.55,"create_p99_ms":237.69,"create_per_s":448.81,"cs_errors":0,"dispatch_p50_ms":9.83,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:38957","settle_p50_ms":6.57,"settle_p99_ms":219.1,"settle_per_s":448.81,"task_e2e_p50_ms":33.94,"task_e2e_p99_ms":776.85,"task_errors":0,"tasks_per_s":191.89,"timer_deadline_ms":2000,"timer_late_max_ms":30012.0,"timer_late_p50_ms":97.0,"timer_late_p95_ms":29988.0,"timer_late_p99_ms":30006.0,"timers_created":500,"timers_fired":500,"unix_ms":1792863013942}
```

The 2026-10-25 rows. The `commit` field is the xtask's checkout; the server
binaries were built at `08f73b0d` (before) and `791bc915` (after, the same
server code as `9d56a23b`). Before on SQLite and Postgres, then after:

```json
{"backend":"sqlite","bench":"resonate","claim_p50_ms":17.06,"claim_p95_ms":482.62,"claim_p99_ms":6180.6,"clients":16,"commit":"b11af1b16734","create_p50_ms":5.14,"create_p99_ms":385.16,"create_per_s":366.23,"cs_errors":0,"dispatch_p50_ms":7.99,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:41293","settle_p50_ms":5.22,"settle_p99_ms":427.09,"settle_per_s":366.23,"task_e2e_p50_ms":28.37,"task_e2e_p99_ms":6674.15,"task_errors":0,"tasks_per_s":61.09,"timer_deadline_ms":2000,"timer_late_max_ms":6448.0,"timer_late_p50_ms":1212.0,"timer_late_p95_ms":6270.0,"timer_late_p99_ms":6445.0,"timers_created":500,"timers_fired":494,"unix_ms":1792897050123}
{"backend":"postgres","bench":"resonate","claim_p50_ms":98.58,"claim_p95_ms":631.68,"claim_p99_ms":1167.38,"clients":16,"commit":"8c8db1e119a2","create_p50_ms":39.79,"create_p99_ms":406.77,"create_per_s":124.28,"cs_errors":0,"dispatch_p50_ms":46.44,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:42915","settle_p50_ms":40.31,"settle_p99_ms":400.0,"settle_per_s":124.28,"task_e2e_p50_ms":150.87,"task_e2e_p99_ms":1547.0,"task_errors":0,"tasks_per_s":67.85,"timer_deadline_ms":2000,"timer_late_max_ms":21561.0,"timer_late_p50_ms":13395.0,"timer_late_p95_ms":21396.0,"timer_late_p99_ms":21541.0,"timers_created":500,"timers_fired":500,"unix_ms":1792932117639}
{"backend":"sqlite","bench":"resonate","claim_p50_ms":8.89,"claim_p95_ms":58.05,"claim_p99_ms":101.41,"clients":16,"commit":"791bc915a2fd","create_p50_ms":3.57,"create_p99_ms":555.68,"create_per_s":514.68,"cs_errors":0,"dispatch_p50_ms":4.31,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:34917","settle_p50_ms":3.54,"settle_p99_ms":363.06,"settle_per_s":514.68,"task_e2e_p50_ms":13.58,"task_e2e_p99_ms":128.8,"task_errors":0,"tasks_per_s":680.84,"timer_deadline_ms":2000,"timer_late_max_ms":70.0,"timer_late_p50_ms":4.0,"timer_late_p95_ms":60.0,"timer_late_p99_ms":61.0,"timers_created":500,"timers_fired":500,"unix_ms":1792932814870}
{"backend":"postgres","bench":"resonate","claim_p50_ms":25.97,"claim_p95_ms":118.74,"claim_p99_ms":193.29,"clients":16,"commit":"791bc915a2fd","create_p50_ms":11.59,"create_p99_ms":1073.55,"create_per_s":144.37,"cs_errors":0,"dispatch_p50_ms":13.06,"duration_s":10,"host_cpus":14,"payload_bytes":256,"server_url":"http://127.0.0.1:34491","settle_p50_ms":12.57,"settle_p99_ms":1222.87,"settle_per_s":144.37,"task_e2e_p50_ms":39.53,"task_e2e_p99_ms":570.12,"task_errors":0,"tasks_per_s":198.13,"timer_deadline_ms":2000,"timer_late_max_ms":32.0,"timer_late_p50_ms":11.0,"timer_late_p95_ms":20.0,"timer_late_p99_ms":30.0,"timers_created":500,"timers_fired":500,"unix_ms":1792932644565}
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

The likely cause was `resonate-timer-wheel`'s merge, and FW11 confirmed it
(next section).

## FW11: the timer fix

Timer lateness under load had three causes. FW11 fixes each in its own
commit, with its own regression test. The FW8 branch found the second and
third independently; its two timer commits are folded in here.

**1. The wheel's merge is quadratic** (`resonate-timer-wheel`). The proved
`TimerWheel::merge` moves every untouched entry out with `Vec::remove(0)` and
insertion-sorts the whole union back in. One single-entry merge into a full
wheel costs (release build, machine A,
`cargo run --release -p resonate-timer-wheel --example merge_cost`):

| wheel size | 256 | 1024 | 2048 | 4096 | 8192 |
|---|---|---|---|---|---|
| `TimerWheel` µs per merge | 22.2 | 226.8 | 777.7 | 4164 | 19832 |
| `IndexedWheel` µs per merge | 0.090 | 0.105 | 0.088 | 0.128 | 0.150 |

At the SQL default of 8192, the timer task could take about 50 batches a
second, and every request that arms a deadline sends one. The fix is
`IndexedWheel`, the same `spec_merge` over a `BTreeMap` keyed by
`(deadline, arrival sequence)` plus a hash index on identity, `O(log n)` per
arrival. The proved wheel is unchanged and still verified, and `IndexedWheel`
is tested against it step by step (crate README, "Cost"). Regression:
`deadlines_fire_on_time_while_the_wheel_is_full_and_busy` (16 producers keep
a wheel of 8192 full while 500 near deadlines come due) fired 0 of 500
before and fires all 500 after, p99 1 to 3 ms.

**2. The timer merged ahead of firing.** Its `select!` was biased to the
merge queue and fired only from the sleep branch, so a queue that never
emptied kept every due deadline from firing, however cheap the merges
were. Each pass now merges at most 1024 queued batches, fires what is due,
then sleeps. The batches are merged one at a time rather than concatenated,
because the two differ once the wheel is full. Regression:
`a_stream_of_merges_does_not_starve_firing` (adapted from FW8) fails on cause
1's fix alone and passes with this one.

**3. Firing was serial** (`resonate-sql`). The timer looks at its wheel again
only after the whole batch it fired is done, and each fired deadline is a
transaction. On Postgres, 500 timers due together took seconds back to back.
With fixes 1 and 2 alone the bench's timer p99 was still 14.1 s. Firing now
runs up to 16 at a time. Regression: `crates/resonate-sql/tests/fire.rs`.

Choosing the bound, `cargo xtask bench` with 16 clients and 500 timers, on one
binary with the bound overridden per run (single runs on a shared machine, so
treat small differences as noise):

| backend | fire bound | timer p50 / p95 / p99 ms | claim p99 ms | create/s | tasks/s |
|---|---|---|---|---|---|
| postgres | before FW11 | 13395 / 21396 / 21541 | 1167 | 124 | 68 |
| postgres | 1 (fixes 1 and 2 only) | 7990 / 13584 / 14085 | 2382 | 161 | 75 |
| postgres | 8 | 928 / 1500 / 1561 | 590 | 120 | 74 |
| postgres | 16 | 97 / 486 / 612 | 859 | 180 | 62 |
| sqlite | before FW11 (load 21) | none of 500 fired within 30 s | 298 | 289 | 180 |
| sqlite | 1 | 13 / 177 / 179; repeats p99 150, 706 | 94; 936, 236 | 872; 643, 1019 | 347; 174, 419 |
| sqlite | 8 | 5 / 22 / 28 | 384 | 632 | 262 |
| sqlite | 16 | 7 / 104 / 117; repeats p99 321, 278 | 615; 531, 808 | 701; 993, 655 | 239; 213, 159 |

On Postgres, 16 is the only setting that keeps the p99 well under the
2 s gate, and request throughput is no worse. On SQLite, the bound's effect
is inside the run-to-run noise. A fire waits for a pool connection like any
request, so a bound above the default pool of 10 queues fires rather than
taking connections from requests.

**A bench harness fix came with it.** The timer phase registers its listener
after creating the promise. If the deadline passed in between,
`register_listener` answered with the settled promise and kept no listener,
and the harness waited for an unblock that never came. Once timers fired on
time, a loaded run lost a handful this way (493 of 500, every promise
`rejected_timedout` in the database). That answer now counts as the
observation.

**Regression gates.**

- `cargo test -p resonate-timer-wheel` (the two timer tests above, plus
  `tests/indexed.rs`) and `cargo test -p resonate-sql --test fire`.
- `cargo xtask bench --backend sqlite --max-timer-p99-ms 2000` (and
  `--backend postgres`) end to end. Results are the 2026-10-25 rows above.

## Known issues

Found by FW8 (poll groups) and not fixed by FW11:

- A half-open poll connection still costs one retry timeout per message: the
  server writes into a connection whose peer is gone, and only the task's
  retry deadline redelivers. Fixing it needs a transport-level ack or TCP
  keepalive on the poll stream.
- Postgres throughput degrades as the database grows: 94 requests/s on an
  empty database, about 20/s at 38k rows (debug build).
