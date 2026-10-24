"""Pick one representative trace per published run for the SigNoz screenshots:
the run's `rbench.workflow` root span closest to the run's median duration.

    python3 -I bench/scripts/pick-traces.py <results dir> <run-id>... > spec.json

Reads SigNoz's ClickHouse through `podman exec rbench-signoz-clickhouse`.
Writes the screenshot spec for bench/scripts/shots.js on stdout.
"""

import json
import subprocess
import sys


def ch(q):
    out = subprocess.run(
        ["podman", "exec", "rbench-signoz-clickhouse", "clickhouse-client", "-q", q],
        check=True, capture_output=True, text=True,
    ).stdout
    return [line.split("\t") for line in out.splitlines() if line]


def main():
    runs = sys.argv[2:]
    spec = [
        {"name": "signoz-services", "path": "/services?relativeTime=6h", "wait": 6000},
        {"name": "signoz-service-map", "path": "/service-map?relativeTime=6h", "wait": 8000},
    ]
    for run in runs:
        rows = ch(
            "SELECT traceID, durationNano FROM signoz_traces.distributed_signoz_index_v3 "
            f"WHERE name = 'rbench.workflow' AND attributes_string['rbench.run'] = '{run}' "
            "ORDER BY durationNano"
        )
        if not rows:
            print(f"no trace for {run}", file=sys.stderr)
            continue
        tid, dur = rows[len(rows) // 2]
        spec.append({"name": f"trace-{run}", "path": f"/trace/{tid}", "wait": 6000,
                     "trace_id": tid, "duration_ms": int(dur) / 1e6, "run": run})
    json.dump(spec, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
