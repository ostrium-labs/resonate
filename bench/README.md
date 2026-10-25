# bench: durable-execution benchmarks

One harness, one metrics format, for Resonate and the systems people compare it with.

| Path | What it is |
|---|---|
| `rbench/` | The harness (Rust). `rbench resonate` and `rbench airflow` drive HTTP-level workloads with [Goose](https://github.com/tag1consulting/goose) 0.18.1; `rbench exec` wraps an SDK-level driver and reads its records; `rbench report` makes `results.csv`/`results.md`; `rbench rtt` measures TCP round trips |
| `drivers/` | The workload contract (`drivers/README.md`) and the Temporal Go SDK driver (`drivers/temporal-go`) |
| `stacks/` | Pinned podman compose stacks: `temporal`, `airflow`, `postgres`, `tikv` (+ the Loams engine), `signoz` (trace viewer + collector) |
| `scripts/` | One script per leg (`run-*.sh`), the TiDB Cloud helper (`tidbcloud.sh`), charts (`charts.py`) and SigNoz screenshots (`shots.js`) |
| `results/<date>/` | Raw data, traces, tables, charts and the write-up of one benchmark day |

Build the harness: `cd bench/rbench && cargo build --release` (its own workspace; it does not build the server).

The workloads (same shape for every system):

- **fanout**: one workflow = K parallel no-op steps, then a join;
- **chain**: L sequential no-op steps, each followed by a durable sleep of S ms;
- **burst**: N one-step workflows started at once, then awaited.

Each run writes `runs/<run-id>/` with `workflows.csv`, `steps.csv`, `requests.csv`, `resources.csv`
(CPU and RSS of each process or container, 1 s samples), `env.json` (host load at start and end), and `summary.json`. See `results/2026-10-24/README.md` for the first full run.
