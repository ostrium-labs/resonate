#!/usr/bin/env bash
# One TiDB Cloud Starter region, end to end: create a cluster, measure the
# round trip, run the Resonate server (MySQL plugin) on this laptop against it,
# run the three workloads, stop the server, delete the cluster (always, via the
# trap) and check it is gone.
#
#   bench/scripts/run-tidbcloud.sh <region-id>
#
# Env: SERVER_BIN, RBENCH_BIN, OUT (results dir), and the workload sizes below.
set -euo pipefail
region=$1
here=$(cd "$(dirname "$0")" && pwd)
tc="$here/tidbcloud.sh"
OUT=${OUT:-bench/results/2026-10-24}
DATA=${DATA:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data}
SERVER_BIN=${SERVER_BIN:?}
RBENCH_BIN=${RBENCH_BIN:?}
FAN_N=${FAN_N:-60} FAN_C=${FAN_C:-10} FAN_K=${FAN_K:-8}
CH_N=${CH_N:-20} CH_C=${CH_C:-10} CH_L=${CH_L:-3} CH_S=${CH_S:-1000}
BU_N=${BU_N:-60}
POOL=${POOL:-32}
backend="tidb-$region"
mkdir -p "$OUT/runs" "$DATA/logs"

# Nothing else may answer on the bench port, or the run measures that instead.
if curl -s -o /dev/null http://127.0.0.1:18001/ready; then echo "port 18001 is taken" >&2; exit 1; fi
# The server reads RESONATE_* as configuration: keep none of ours in its env.
id=$("$tc" create "$region")
echo "cluster $id in $region"
server=""
cleanup() {
  [ -n "$server" ] && kill "$server" 2>/dev/null && wait "$server" 2>/dev/null || true
  "$tc" delete "$id" || echo "DELETE FAILED for $id ($region): delete it by hand" >&2
}
trap cleanup EXIT

hp=$("$tc" host "$id")
"$RBENCH_BIN" rtt "$hp" -c 30 | jq -c --arg b "$backend" --arg r "$region" '. + {backend:$b, region:$r, target:"<cluster-endpoint>:4000"}' | tee -a "$OUT/tidbcloud-rtt.json"

url=$("$tc" url "$id")
RESONATE_SERVERS__ACTIVE=server_mysql \
RESONATE_SERVERS__SERVER_MYSQL__URL="$url" \
RESONATE_SERVERS__SERVER_MYSQL__POOL_SIZE=$POOL \
RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=127.0.0.1:18001 \
RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=127.0.0.1:18002 \
OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317} \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
  OTEL_SERVICE_NAME=resonate-server \
  env -u SERVER_BIN -u RBENCH_BIN "$SERVER_BIN" serve --level warn >"$DATA/logs/server-$backend.log" 2>&1 &
server=$!
sleep 2
kill -0 "$server" 2>/dev/null || { tail -20 "$DATA/logs/server-$backend.log" >&2; exit 1; }
for _ in $(seq 1 120); do
  curl -fsS -o /dev/null http://127.0.0.1:18001/ready 2>/dev/null && break
  kill -0 "$server" 2>/dev/null || { tail -20 "$DATA/logs/server-$backend.log" >&2; exit 1; }
  sleep 1
done
curl -fsS -o /dev/null http://127.0.0.1:18001/ready

common=(--system resonate --backend "$backend" --out "$OUT/runs" --watch "pid:$server" --note "server on this laptop; store TiDB Cloud Starter $region over WAN (TLS); pool $POOL")
"$RBENCH_BIN" resonate --workload fanout -n "$FAN_N" -c "$FAN_C" -k "$FAN_K" "${common[@]}" 2>"$DATA/logs/rbench-$backend-fanout.log" || true
"$RBENCH_BIN" resonate --workload chain -n "$CH_N" -c "$CH_C" -l "$CH_L" --sleep-ms "$CH_S" "${common[@]}" 2>"$DATA/logs/rbench-$backend-chain.log" || true
"$RBENCH_BIN" resonate --workload burst -n "$BU_N" "${common[@]}" 2>"$DATA/logs/rbench-$backend-burst.log" || true
grep -h '^rbench: resonate' "$DATA/logs/rbench-$backend-"*.log || true
