#!/usr/bin/env bash
# Stock Temporal 1.32.1 (bench/stacks/temporal, up) driven by the Go SDK driver
# under `rbench exec`, the same three workloads and sizes as the Resonate legs.
set -euo pipefail
OUT=${OUT:-bench/results/2026-10-24}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs}
RBENCH_BIN=${RBENCH_BIN:?}
DRIVER=${DRIVER:-bench/drivers/temporal-go/bin/rbench-temporal}
FAN_N=${FAN_N:-500} FAN_C=${FAN_C:-20} FAN_K=${FAN_K:-8}
CH_N=${CH_N:-100} CH_C=${CH_C:-20} CH_L=${CH_L:-5} CH_S=${CH_S:-1000}
BU_N=${BU_N:-500}
WORKLOADS=${WORKLOADS:-fanout chain burst}
mkdir -p "$OUT/runs" "$LOGS"
common=(--system temporal --backend postgres --out "$OUT/runs" --watch-driver
  --watch container:rbench-temporal-server --watch container:rbench-temporal-postgres
  --note "stock Temporal 1.32.1, four services in one container, Postgres 17.10 in podman, data on ext4 (/mnt/Projects); Go SDK 1.49.0 worker and starter in one process")
for w in $WORKLOADS; do
  case $w in
  fanout) args=(-n "$FAN_N" -c "$FAN_C" -k "$FAN_K") ;;
  chain) args=(-n "$CH_N" -c "$CH_C" -l "$CH_L" --sleep-ms "$CH_S") ;;
  burst) args=(-n "$BU_N") ;;
  esac
  TEMPORAL_ADDRESS=127.0.0.1:17233 OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317} \
    "$RBENCH_BIN" exec --workload "$w" "${args[@]}" "${common[@]}" -- "$DRIVER" -mode=all 2>"$LOGS/rbench-temporal-$w.log" || true
  grep -h '^rbench: temporal' "$LOGS/rbench-temporal-$w.log" || tail -3 "$LOGS/rbench-temporal-$w.log"
done
