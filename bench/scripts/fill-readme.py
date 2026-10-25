"""Fill the generated sections of a results README from the data next to it.

    python3 -I bench/scripts/fill-readme.py bench/results/2026-10-24 <tidbcloud state dir>

Replaces HEADLINE_LOCAL, TIDB_SECTION, TIMERS_TABLE, COST_SECTION and
TIDB_RESOURCES in README.md (keeps a README.template.md the first time).
"""

import json
import os
import subprocess
import sys

d, state = sys.argv[1], sys.argv[2]
here = os.path.dirname(os.path.abspath(__file__))
tmpl = os.path.join(d, "README.template.md")
if not os.path.exists(tmpl):
    os.rename(os.path.join(d, "README.md"), tmpl)
text = open(tmpl).read()

headline = subprocess.run([sys.executable, "-I", os.path.join(here, "headline.py"), d], check=True,
                          capture_output=True, text=True).stdout
local, _, tidb = headline.partition("#### TiDB")
text = text.replace("HEADLINE_LOCAL", local.strip())

# TiDB: per region, the traced pass's runs (headline) plus the RTT.
rtt = {}
with open(os.path.join(d, "tidbcloud-rtt.json")) as f:
    for line in f:
        if line.strip():
            o = json.loads(line)
            rtt.setdefault(o["backend"], []).append(o)
rows = ["| Region | TCP connect min / p50 ms (each pass) |", "|---|---|"]
for be, os_ in sorted(rtt.items()):
    rows.append(f"| {be.replace('tidb-', '')} | " + "; ".join(f"{o['min_ms']:.0f} / {o['p50_ms']:.0f}" for o in os_) + " |")
text = text.replace("TIDB_SECTION", ("#### TiDB" + tidb).strip() + "\n\nRound trips:\n\n" + "\n".join(rows))

# Timers: every chain run in the headline list.
trows = ["| System | Backend | late p50 ms | late p95 ms | late p99 ms | max ms | run |", "|---|---|---:|---:|---:|---:|---|"]
for line in open(os.path.join(d, "headline-runs.txt")):
    run = line.split("#", 1)[0].strip()
    if not run or "-chain-" not in run:
        continue
    s = json.load(open(os.path.join(d, "runs", run, "summary.json")))
    l = s.get("sleep_lateness") or {}
    if not l:
        continue
    trows.append(f"| {s['system']} | {s['backend']} | {l['p50_ms']:,.0f} | {l['p95_ms']:,.0f} | {l['p99_ms']:,.0f} | {l['max_ms']:,.0f} | {run} |")
text = text.replace("TIMERS_TABLE", "\n".join(trows))

cost = open(os.path.join(d, "cost.md")).read().strip()
ru = json.load(open(os.path.join(d, "ru-per-workflow.json")))
sc = ru["starter_scaling"]
cost_text = f"""**Request Units.** TiDB Cloud Starter bills Request Units but does not show them to an API key: the cluster API
has no usage view and `information_schema.statements_summary` answers "SELECT command denied" (checked 2026-10-25).
So `bench/scripts/measure-ru.sh` ran the same Resonate server against a local TiDB v8.5.8 with resource control and
summed `statements_summary` RU (read and write separately) per workload, then scaled them to Starter with ratios
measured statement by statement on a Starter instance (`ru-spot-check.txt`, 20 of each through
`@@tidb_last_query_info`): writes ×{sc['write']} (INSERT 2.90 → 6.73, UPDATE 3.28 → 7.23), reads ×{sc['read']}
(`SELECT … FOR UPDATE` 1.70 → 2.55; point gets equal). Storage per workflow is the table-size growth after
`ANALYZE`. Treat both as estimates within a factor of about 1.5.

**Prices.** Starter, read 2026-10-25 from <https://www.pingcap.com/tidb-cloud-starter-pricing-details/>: $0.10 per
1 M RUs (Oregon, N. Virginia), $0.12 (Singapore, Tokyo, Frankfurt, Alibaba Cloud Singapore), $0.16 (São Paulo); row
storage $0.20-0.38 per GiB-month. Free per instance per month, for the first five instances of an organization:
50 M RUs, 5 GiB row storage, 5 GiB columnar. TiKV: three nodes of the 2 vCPU / 8 GiB shape with a 50 GB disk, PD and
TiKV on each, at the unit prices loams.dev/cost uses (`lib/cloud-prices.ts` in ostrium-labs/loams-site, prices as
of 2026-10-23): AWS t3.large + gp3, GCP e2-standard-2 + pd-balanced, Azure D2s v5 + E6. A single-VM row is the
laptop-like, unreplicated case. The Resonate server's own VM is the same in both and is left out.

{cost}

**How to read it.** Below the free allowance Starter costs nothing. Above it, Starter grows linearly with work;
TiKV is flat until it runs out of capacity. Break-even is the workflow count per month where Starter's RU bill
equals the TiKV cluster's monthly price (storage left out on both sides: without the fork's retention, FW5, both
grow with history — ~3.6 KiB per fan-out workflow, so 1 M retained fan-out workflows are ~3.4 GiB, inside Starter's
5 GiB free row storage). The TiKV capacity column applies the request rate one laptop TiKV node sustained (2,178
protocol requests/s) to each workload; a 3 × 2 vCPU cluster with replication factor 3 has not been measured and
could be lower."""
text = text.replace("COST_SECTION", cost_text)

created = [l.rstrip("\n").split("\t") for l in open(os.path.join(state, "created.tsv")) if l.strip()]
deleted = {l.split("\t")[0]: l.rstrip("\n").split("\t", 2)[2] for l in open(os.path.join(state, "deleted.tsv")) if l.strip()}
rrows = ["| Cluster ID | Name | Region | Deleted |", "|---|---|---|---|"]
for cid, name, region in created:
    rrows.append(f"| {cid} | {name} | {region} | {deleted.get(cid, 'NOT DELETED')} |")
text = text.replace("TIDB_RESOURCES", f"""Every TiDB Cloud Starter cluster this benchmark created (name `rbench-<region>-<time>`, label `rbench=1`,
spending limit 0), and when it was deleted. After the last deletion the organization's cluster list was empty, and
the billing API's October bill read $0.

""" + "\n".join(rrows))
open(os.path.join(d, "README.md"), "w").write(text)
print("README filled")
