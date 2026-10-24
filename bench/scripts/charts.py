"""Charts for a results directory, from its results.csv (written by `rbench report`).

    uv run --no-project --with matplotlib==3.10.7 python -I bench/scripts/charts.py bench/results/2026-10-24

Writes charts/*.svg: throughput and latency per (system, backend) for each
workload, and, for the TiDB Cloud legs, p50 latency against the region's TCP
round trip. Palette: the dataviz reference instance (light surface).
"""

import csv
import json
import os
import sys

import matplotlib

matplotlib.use("svg")
import matplotlib.pyplot as plt  # noqa: E402

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#e4e3df"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.size": 10,
        "axes.edgecolor": GRID,
        "axes.labelcolor": INK2,
        "xtick.color": INK2,
        "ytick.color": INK2,
        "text.color": INK,
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "svg.fonttype": "none",
    }
)


def load(d):
    with open(os.path.join(d, "results.csv")) as f:
        return list(csv.DictReader(f))


def label(r):
    return f"{r['system']} · {r['backend']}"


def best(rows):
    """One row per (workload, system, backend): the last run (rows are in run-id order)."""
    out = {}
    for r in rows:
        out[(r["workload"], r["system"], r["backend"])] = r
    return list(out.values())


def bars(rows, workload, out):
    rs = sorted([r for r in rows if r["workload"] == workload], key=lambda r: float(r["throughput_wf_s"]))
    if not rs:
        return
    h = 0.9 + 0.32 * len(rs)
    fig, ax = plt.subplots(figsize=(7.5, h))
    y = range(len(rs))
    v = [float(r["throughput_wf_s"]) for r in rs]
    ax.barh(list(y), v, color=S1, height=0.6)
    for i, val in zip(y, v):
        ax.text(val, i, f"  {val:.1f}", va="center", color=INK2, fontsize=9)
    ax.set_yticks(list(y), [label(r) for r in rs])
    ax.set_xlabel("completed workflows per second")
    ax.set_title(f"{workload}: throughput", loc="left", color=INK, fontsize=11)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_xlim(0, max(v) * 1.18 if max(v) > 0 else 1)
    fig.tight_layout()
    fig.savefig(os.path.join(out, f"{workload}-throughput.svg"))
    plt.close(fig)


def latency(rows, workload, out):
    rs = sorted([r for r in rows if r["workload"] == workload], key=lambda r: float(r["p50_ms"]))
    if not rs:
        return
    h = 1.1 + 0.32 * len(rs)
    fig, ax = plt.subplots(figsize=(7.5, h))
    for i, r in enumerate(rs):
        p50, p95, p99 = (float(r[k]) for k in ("p50_ms", "p95_ms", "p99_ms"))
        ax.plot([p50, p99], [i, i], color=GRID, linewidth=2, zorder=1)
        ax.scatter([p50], [i], color=S1, s=36, zorder=3, label="p50" if i == 0 else None)
        ax.scatter([p95], [i], color=S2, s=36, zorder=3, label="p95" if i == 0 else None)
        ax.scatter([p99], [i], color=S3, s=36, zorder=3, label="p99" if i == 0 else None)
    ax.set_yticks(range(len(rs)), [label(r) for r in rs])
    ax.set_xscale("log")
    ax.set_xlabel("start-to-complete latency, ms (log scale)")
    ax.set_title(f"{workload}: workflow latency", loc="left", color=INK, fontsize=11)
    ax.grid(axis="x", color=GRID, linewidth=0.8, which="both")
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.legend(frameon=False, ncol=3, loc="lower right", bbox_to_anchor=(1, 1.0))
    fig.tight_layout()
    fig.savefig(os.path.join(out, f"{workload}-latency.svg"))
    plt.close(fig)


def rtt(rows, d, out):
    p = os.path.join(d, "tidbcloud-rtt.json")
    if not os.path.exists(p):
        return
    rtts = {}
    with open(p) as f:
        for line in f:
            if line.strip():
                o = json.loads(line)
                rtts[o["backend"]] = o["p50_ms"]
    for workload in ("fanout", "chain", "burst"):
        pts = [(rtts[r["backend"]], float(r["p50_ms"]), r["backend"]) for r in rows
               if r["workload"] == workload and r["backend"] in rtts and r["system"] == "resonate"]
        if not pts:
            continue
        fig, ax = plt.subplots(figsize=(6.5, 4))
        ax.scatter([x for x, _, _ in pts], [y for _, y, _ in pts], color=S1, s=40)
        for x, y, b in pts:
            ax.annotate(b.replace("tidb-", ""), (x, y), textcoords="offset points", xytext=(6, 4), color=INK2, fontsize=8)
        ax.set_xlabel("TCP connect round trip from the laptop (India), ms")
        ax.set_ylabel("workflow p50, ms")
        ax.set_title(f"{workload}: Resonate on TiDB Cloud Starter, by region", loc="left", color=INK, fontsize=11)
        ax.grid(color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        fig.tight_layout()
        fig.savefig(os.path.join(out, f"{workload}-tidbcloud-rtt.svg"))
        plt.close(fig)


def main():
    d = sys.argv[1]
    out = os.path.join(d, "charts")
    os.makedirs(out, exist_ok=True)
    rows = best(load(d))
    for w in ("fanout", "chain", "burst"):
        bars(rows, w, out)
        latency(rows, w, out)
    rtt(rows, d, out)
    print("charts ->", out)


if __name__ == "__main__":
    main()
