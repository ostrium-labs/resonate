"""The headline tables: one row per run listed in headline-runs.txt (in order).

    python3 -I bench/scripts/headline.py bench/results/2026-10-24 > table.md

headline-runs.txt holds run ids, one per line; '#' starts a comment and a line
'## <title>' starts a new table. Every run in runs/ stays in results.md.
"""

import json
import os
import sys


def load(d, run):
    with open(os.path.join(d, "runs", run, "summary.json")) as f:
        s = json.load(f)
    try:
        with open(os.path.join(d, "runs", run, "env.json")) as f:
            env = json.load(f)
        load1 = env["start"]["loadavg"].split()[0]
    except (OSError, KeyError):
        load1 = "?"
    return s, load1


def label(s):
    sysname = {
        "resonate": "Resonate (raw)",
        "loams-durable": "Loams 0.1.1 /durable/",
        "temporal": "Temporal 1.32.1",
        "airflow": "Airflow 3.3.2 + Edge",
    }.get(s["system"], s["system"])
    be = s["backend"]
    if s["system"] == "resonate" and be == "tikv":
        be = "TiKV (FW1 store)"
    return sysname, be


HEAD = ("| System | Backend | N / concurrency | OK | wf/s | p50 ms | p95 ms | p99 ms | step p50 ms | "
        "sleep late p50 / p99 ms | errors | server CPU % | load1 | run |\n"
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---|")


def row(d, run, note=""):
    s, l1 = load(d, run)
    sysname, be = label(s)
    if note:
        be = f"{be} — _{note}_"
    p = s["params"]
    step = next((v for k, v in s["steps"].items() if k != "sleep"), {"p50_ms": 0})
    late = s.get("sleep_lateness")
    late_s = f"{late['p50_ms']:,.0f} / {late['p99_ms']:,.0f}" if late else "–"
    cpu = sum(r["cpu_avg_pct"] for r in s["resources"] if not r["target"].startswith("proc:rbench"))
    errs = s["failed"] + s["request_errors"]
    return (f"| {sysname} | {be} | {p.get('n')} / {p.get('concurrency')} | {s['completed']}/{s['started']} | "
            f"{s['throughput_wf_s']:,.2f} | {s['latency']['p50_ms']:,.0f} | {s['latency']['p95_ms']:,.0f} | "
            f"{s['latency']['p99_ms']:,.0f} | {step['p50_ms']:,.1f} | {late_s} | {errs} | {cpu:,.0f} | {l1} | "
            f"[{run}](runs/{run}/) |")


def main():
    d = sys.argv[1]
    out = []
    with open(os.path.join(d, "headline-runs.txt")) as f:
        for line in f:
            line = line.strip()
            if line.startswith("## "):
                out.append("")
                out.append("#### " + line[3:])
                out.append("")
                out.append(HEAD)
                continue
            run, _, note = line.partition("#")
            run, note = run.strip(), note.strip()
            if run:
                if os.path.exists(os.path.join(d, "runs", run, "summary.json")):
                    out.append(row(d, run, note))
                else:
                    print(f"missing {run}", file=sys.stderr)
    print("\n".join(out))


if __name__ == "__main__":
    main()
