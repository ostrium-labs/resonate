#!/usr/bin/env bash
# The Loams 0.1.1 engine's Resonate endpoint (/durable/) on TiKV
# (bench/stacks/tikv with --profile loams, up), the same workloads and sizes as
# the raw Resonate legs.
set -euo pipefail
OUT=${OUT:-bench/results/2026-10-24}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs}
RBENCH_BIN=${RBENCH_BIN:?}
FAN_N=${FAN_N:-500} FAN_C=${FAN_C:-20} FAN_K=${FAN_K:-8}
CH_N=${CH_N:-100} CH_C=${CH_C:-20} CH_L=${CH_L:-5} CH_S=${CH_S:-1000}
BU_N=${BU_N:-500}
WORKLOADS=${WORKLOADS:-fanout chain burst}
mkdir -p "$OUT/runs" "$LOGS"
common=(--url http://127.0.0.1:18090 --path /durable/ --push-listen 127.0.0.1:19191
  --system loams-durable --backend tikv --out "$OUT/runs"
  --watch container:rbench-loams --watch container:rbench-tikv-tikv --watch container:rbench-tikv-pd --watch proc:rbench
  --note "Loams 0.1.1 engine (ghcr image), Resonate endpoint /durable/ on its main listener, embedded Resonate on TiKV v8.5.8 (one PD, one TiKV, API v2 keyspace loams_durable), data on ext4 (/mnt/Projects); --durable-push")
for w in $WORKLOADS; do
  case $w in
  fanout) args=(-n "$FAN_N" -c "$FAN_C" -k "$FAN_K") ;;
  chain) args=(-n "$CH_N" -c "$CH_C" -l "$CH_L" --sleep-ms "$CH_S") ;;
  burst) args=(-n "$BU_N") ;;
  esac
  "$RBENCH_BIN" resonate --workload "$w" "${args[@]}" "${common[@]}" 2>"$LOGS/rbench-loams-$w.log" || true
  grep -h '^rbench: loams' "$LOGS/rbench-loams-$w.log" || tail -3 "$LOGS/rbench-loams-$w.log"
done
