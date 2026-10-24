#!/usr/bin/env bash
# Airflow 3.3.2 + Edge executor (bench/stacks/airflow, up) driven over its REST
# API by rbench (Goose), the same workloads and sizes as the other legs.
set -euo pipefail
OUT=${OUT:-bench/results/2026-10-24}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs}
RBENCH_BIN=${RBENCH_BIN:?}
FAN_N=${FAN_N:-500} FAN_C=${FAN_C:-20} FAN_K=${FAN_K:-8}
CH_N=${CH_N:-100} CH_C=${CH_C:-20} CH_L=${CH_L:-5} CH_S=${CH_S:-1000}
BU_N=${BU_N:-500}
WORKLOADS=${WORKLOADS:-fanout chain burst}
mkdir -p "$OUT/runs" "$LOGS"
watch=()
for c in api-server scheduler dag-processor triggerer edge-worker-1 edge-worker-2 postgres; do watch+=(--watch "container:rbench-airflow-$c"); done
common=(--system airflow --backend postgres --out "$OUT/runs" "${watch[@]}" --watch proc:rbench --timeout-s 1800
  --note "Airflow 3.3.2 + edge3 4.3.2 (EdgeExecutor, 2 workers x 16 slots), one scheduler, Postgres 17.10 metadata DB on ext4 (/mnt/Projects); run end observed by polling every 100 ms")
for w in $WORKLOADS; do
  case $w in
  fanout) args=(-n "$FAN_N" -c "$FAN_C" -k "$FAN_K") ;;
  chain) args=(-n "$CH_N" -c "$CH_C" -l "$CH_L" --sleep-ms "$CH_S") ;;
  burst) args=(-n "$BU_N") ;;
  esac
  "$RBENCH_BIN" airflow --workload "$w" "${args[@]}" "${common[@]}" 2>"$LOGS/rbench-airflow-$w.log" || true
  grep -h '^rbench: airflow' "$LOGS/rbench-airflow-$w.log" || tail -3 "$LOGS/rbench-airflow-$w.log"
done
