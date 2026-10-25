"""Combine one step of the five-instance TiDB Starter run.

    python3 -I bench/scripts/tidb5x.py <out dir> <workload> <label>

Reads runs/tidb5x-<workload>-<label>-i{1..5}/ and appends one JSON line to
combined.jsonl: per-instance and combined throughput, latency and errors.
Combined throughput = all completed workflows / (last end - first start).
"""

import csv
import json
import os
import sys


def pct(v, p):
    if not v:
        return 0.0
    v = sorted(v)
    return v[min(len(v) - 1, round(p / 100 * (len(v) - 1)))]


def main():
    out, w, label = sys.argv[1:4]
    starts, ends, lat, per = [], [], [], []
    failed = errors = 0
    for i in range(1, 6):
        d = os.path.join(out, "runs", f"tidb5x-{w}-{label}-i{i}")
        try:
            s = json.load(open(os.path.join(d, "summary.json")))
        except OSError:
            per.append({"instance": i, "missing": True})
            continue
        with open(os.path.join(d, "workflows.csv")) as f:
            for r in csv.DictReader(f):
                a, b = int(r["start_ns"]), int(r["end_ns"])
                starts.append(a)
                ends.append(b)
                if r["ok"] == "true":
                    lat.append((b - a) / 1e6)
        failed += s["failed"]
        errors += s["request_errors"]
        per.append({"instance": i, "completed": s["completed"], "started": s["started"],
                    "wf_s": round(s["throughput_wf_s"], 3), "p50_ms": round(s["latency"]["p50_ms"]),
                    "p99_ms": round(s["latency"]["p99_ms"]), "request_errors": s["request_errors"],
                    "errors": s["errors"], "params": s["params"]})
    wall = (max(ends) - min(starts)) / 1e9 if starts else 0
    res = {
        "workload": w, "step": label, "completed": len(lat), "started": len(starts), "failed": failed,
        "request_errors": errors, "wall_s": round(wall, 1),
        "combined_wf_s": round(len(lat) / wall, 3) if wall else 0,
        "p50_ms": round(pct(lat, 50)), "p95_ms": round(pct(lat, 95)), "p99_ms": round(pct(lat, 99)),
        "per_instance": per,
    }
    with open(os.path.join(out, "combined.jsonl"), "a") as f:
        f.write(json.dumps(res) + "\n")
    print(f"COMBINED {w} {label}: {res['completed']}/{res['started']} ok, {res['combined_wf_s']} wf/s, "
          f"p50 {res['p50_ms']} ms, p99 {res['p99_ms']} ms, failed {failed}, request errors {errors}, "
          f"per instance " + " ".join(str(p.get('wf_s')) for p in per))


if __name__ == "__main__":
    main()
