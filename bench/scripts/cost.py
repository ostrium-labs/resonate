"""Cost model: a hosted Resonate service on TiDB Cloud Starter vs on self-hosted TiKV.

    python3 -I bench/scripts/cost.py bench/results/2026-10-24

Inputs (in the results directory):
  ru-per-workflow.json   Request Units and stored bytes per workflow, per workload,
                         measured by bench/scripts/measure-ru.sh
  results.csv            measured throughput (for the TiKV capacity check)
Prices are written out below with their sources and the date they were read.
Writes cost.json and cost.md.
"""

import csv
import json
import os
import sys

SECONDS_PER_MONTH = 730 * 3600

# TiDB Cloud Starter, https://www.pingcap.com/tidb-cloud-starter-pricing-details/
# read 2026-10-25. Per 1M RUs and per GiB-month of row-based storage.
STARTER = {
    "aws-us-west-2": (0.10, 0.20),
    "aws-us-east-1": (0.10, 0.20),
    "aws-ap-southeast-1": (0.12, 0.24),
    "aws-ap-northeast-1": (0.12, 0.24),
    "aws-eu-central-1": (0.12, 0.24),
    "aws-sa-east-1": (0.16, 0.38),
    "alicloud-ap-southeast-1": (0.12, 0.24),
}
# Free per instance per month (first five instances of an organization).
FREE_RU = 50_000_000
FREE_ROW_GIB = 5

# Self-hosted TiKV: three nodes, PD and TiKV on each, priced with the unit
# prices loams.dev/cost uses (ostrium-labs/loams-site lib/cloud-prices.ts,
# pricesAsOf 2026-10-23, the "full" 2 vCPU / 8 GiB shape and its 50 GB disk).
HOURS = 730
NODE = {
    "aws (us-east-1, t3.large + 50 GB gp3)": 0.0832 * HOURS + 0.08 * 50,
    "gcp (us-central1, e2-standard-2 + 50 GB pd-balanced)": 0.06701142 * HOURS + 0.10 * 50,
    "azure (East US, D2s v5 + E6 64 GiB)": 0.096 * HOURS + 4.8,
}
NODES = 3


def main():
    d = sys.argv[1]
    with open(os.path.join(d, "ru-per-workflow.json")) as f:
        ru = json.load(f)
    # Capacity of the TiKV store: the highest protocol request rate a raw
    # Resonate run on TiKV sustained (requests / wall time), divided by each
    # workload's requests per workflow. Concurrency-bound wall clocks (a chain
    # spends most of its time asleep) say nothing about capacity; requests do.
    req_rate, req_per_wf = 0.0, {}
    runs = os.path.join(d, "runs")
    for name in os.listdir(runs):
        try:
            with open(os.path.join(runs, name, "summary.json")) as f:
                sm = json.load(f)
        except OSError:
            continue
        if sm["system"] != "resonate" or sm["backend"] != "tikv" or not sm["wall_s"] or sm["failed"]:
            continue
        n = sum(v["n"] for v in sm["requests"].values())
        req_rate = max(req_rate, n / sm["wall_s"])
        req_per_wf.setdefault(sm["workload"], n / sm["started"])
    tikv = {k: v * NODES for k, v in NODE.items()}
    tikv["single VM, aws t3.large + 50 GB gp3 (no replication, not HA)"] = NODE["aws (us-east-1, t3.large + 50 GB gp3)"]
    out = {"starter": {}, "tikv_monthly_usd": tikv, "breakeven": {}, "inputs": ru}
    out_cap = {}
    md = []
    md.append("| Workload | RU / workflow | Stored KiB / workflow | Free 50M RU covers (workflows/month) | Starter $ per 1M workflows (us-east-1 … sa-east-1) |")
    md.append("|---|---:|---:|---:|---|")
    for w, m in ru["workloads"].items():
        r = m["ru_per_wf"]
        kib = m["bytes_per_wf"] / 1024
        per_m = {reg: r * p_ru for reg, (p_ru, _) in STARTER.items()}
        out["starter"][w] = {
            "ru_per_wf": r,
            "free_workflows_per_month": FREE_RU / r,
            "usd_per_1m_workflows_compute": per_m,
            "usd_per_1m_workflows_storage_month": {reg: m["bytes_per_wf"] * 1e6 / 2**30 * p_st for reg, (_, p_st) in STARTER.items()},
            "retained_workflows_in_free_storage": FREE_ROW_GIB * 2**30 / m["bytes_per_wf"],
        }
        lo, hi = min(per_m.values()), max(per_m.values())
        md.append(f"| {w} | {r:,.0f} | {kib:,.1f} | {FREE_RU / r:,.0f} | ${lo:,.2f} – ${hi:,.2f} |")
    md.append("")
    md.append("| Workload | TiKV deployment | $ / month | Break-even vs Starter us-east-1 (workflows/month) | vs Starter Singapore | TiKV capacity at the measured request rate (workflows/month) |")
    md.append("|---|---|---:|---:|---:|---:|")
    for w, m in ru["workloads"].items():
        r = m["ru_per_wf"]
        cap = req_rate / req_per_wf[w] * SECONDS_PER_MONTH if req_per_wf.get(w) else 0
        for name, usd in tikv.items():
            be = {}
            for reg in ("aws-us-east-1", "aws-ap-southeast-1"):
                p_ru = STARTER[reg][0]
                # Starter compute cost = max(0, N*r - FREE_RU) * p_ru / 1e6; storage left out (FW5 retention).
                be[reg] = (usd / p_ru * 1e6 + FREE_RU) / r
            out["breakeven"].setdefault(w, {})[name] = be
            md.append(f"| {w} | {name if name.startswith('single') else name + ' ×' + str(NODES)} | {usd:,.0f} | {be['aws-us-east-1']:,.0f} | {be['aws-ap-southeast-1']:,.0f} | {cap:,.0f} |")
    out["tikv_capacity"] = {"max_request_rate_per_s": req_rate, "requests_per_workflow": req_per_wf}
    md.append("")
    md.append(f"TiKV capacity basis: {req_rate:,.0f} protocol requests/s (the best raw-Resonate-on-TiKV run, one laptop node); requests per workflow: " + ", ".join(f"{k} {v:.1f}" for k, v in sorted(req_per_wf.items())) + ".")
    with open(os.path.join(d, "cost.json"), "w") as f:
        json.dump(out, f, indent=2)
    with open(os.path.join(d, "cost.md"), "w") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
