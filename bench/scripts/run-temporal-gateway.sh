#!/usr/bin/env bash
# The Temporal SDK on Resonate through the Loams Temporal gateway (DX1,
# `loams-temporal`): the same Go SDK driver as the stock-Temporal leg, pointed
# at the gateway. The gateway keeps Temporal history in its own Postgres
# database; Resonate (the fork's server) runs on Postgres too, both in the same
# Postgres 17.10 container as the other Postgres legs (bench/stacks/postgres).
#
#   GATEWAY_BIN=... SERVER_BIN=... RBENCH_BIN=... bench/scripts/run-temporal-gateway.sh
set -euo pipefail
OUT=${OUT:-bench/results/2026-10-24}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs}
export RBENCH_DATA=${RBENCH_DATA:-/mnt/Projects/rbench-data}
GATEWAY_BIN=${GATEWAY_BIN:?}
SERVER_BIN=${SERVER_BIN:?}
RBENCH_BIN=${RBENCH_BIN:?}
DRIVER=${DRIVER:-bench/drivers/temporal-go/bin/rbench-temporal}
FAN_N=${FAN_N:-500} FAN_C=${FAN_C:-20} FAN_K=${FAN_K:-8}
CH_N=${CH_N:-100} CH_C=${CH_C:-20} CH_L=${CH_L:-5} CH_S=${CH_S:-1000}
BU_N=${BU_N:-500}
WORKLOADS=${WORKLOADS:-fanout burst chain}
GW_STORE=${GW_STORE:-postgres}
mkdir -p "$OUT/runs" "$LOGS"
pg=(podman compose -p rbench-pg -f bench/stacks/postgres/compose.yaml)

"${pg[@]}" down >/dev/null 2>&1 || true
podman unshare rm -rf "$RBENCH_DATA/pg"; mkdir -p "$RBENCH_DATA/pg"
RBENCH_DATA=$RBENCH_DATA "${pg[@]}" up -d >/dev/null 2>&1
until podman healthcheck run rbench-pg >/dev/null 2>&1; do sleep 2; done
sleep 3
podman exec rbench-pg psql -U resonate -d postgres -qc 'CREATE DATABASE resonate' -c 'CREATE DATABASE temporal_gw' >/dev/null

server="" gw=""
trap '[ -n "$gw" ] && kill "$gw" 2>/dev/null; [ -n "$server" ] && kill "$server" 2>/dev/null; wait 2>/dev/null; "${pg[@]}" down >/dev/null 2>&1 || true' EXIT
env -i HOME="$HOME" PATH="$PATH" \
  RESONATE_SERVERS__ACTIVE=server_postgres \
  RESONATE_SERVERS__SERVER_POSTGRES__URL=postgres://resonate:resonate@127.0.0.1:15432/resonate \
  RESONATE_SERVERS__SERVER_POSTGRES__POOL_SIZE=32 \
  RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=127.0.0.1:18041 \
  RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=127.0.0.1:18042 \
  OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317} \
  OTEL_EXPORTER_OTLP_PROTOCOL=grpc OTEL_SERVICE_NAME=resonate-server-gw \
  "$SERVER_BIN" serve --level warn >"$LOGS/server-gw.log" 2>&1 &
server=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null http://127.0.0.1:18041/ready 2>/dev/null && break; sleep 1; done

cfg="$LOGS/gateway.toml"
if [ "$GW_STORE" = postgres ]; then
  store='kind = "postgres"
url_env = "LOAMS_TEMPORAL_DATABASE_URL"'
else
  store='kind = "memory"'
fi
cat >"$cfg" <<EOF
listen = "127.0.0.1:17234"
insecure_dev = true
pid = "bench-gw-1"
dev_loams_namespace = "local"
[store]
$store
[durability]
kind = "resonate"
url = "http://127.0.0.1:18041"
[[bootstrap_namespaces]]
loams_namespace_id = "local"
name = "default"
EOF
LOAMS_TEMPORAL_DATABASE_URL=postgres://resonate:resonate@127.0.0.1:15432/temporal_gw \
  "$GATEWAY_BIN" --config "$cfg" >"$LOGS/gateway.log" 2>&1 &
gw=$!
for _ in $(seq 1 60); do (exec 3<>/dev/tcp/127.0.0.1/17234) 2>/dev/null && break; sleep 1; done
sleep 2
kill -0 "$gw" || { tail -20 "$LOGS/gateway.log" >&2; exit 1; }

common=(--system temporal-gateway --backend "resonate-postgres" --out "$OUT/runs" --watch-driver
  --watch "pid:$server" --watch "pid:$gw" --watch container:rbench-pg
  --note "Temporal Go SDK 1.49.0 through the Loams Temporal gateway (DX1, loams dx1-t9-binary $(basename "$GATEWAY_BIN")), history store $GW_STORE; Resonate $(basename "$SERVER_BIN") on Postgres 17.10; both databases in one rbench-pg container, data on ext4 (/mnt/Projects)")
for w in $WORKLOADS; do
  case $w in
  fanout) args=(-n "$FAN_N" -c "$FAN_C" -k "$FAN_K") ;;
  chain) args=(-n "$CH_N" -c "$CH_C" -l "$CH_L" --sleep-ms "$CH_S") ;;
  burst) args=(-n "$BU_N") ;;
  esac
  TEMPORAL_ADDRESS=127.0.0.1:17234 OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317} \
    "$RBENCH_BIN" exec --workload "$w" "${args[@]}" "${common[@]}" -- "$DRIVER" -mode=all 2>"$LOGS/rbench-temporal-gw-$w.log" || true
  grep -h '^rbench: temporal-gateway' "$LOGS/rbench-temporal-gw-$w.log" || tail -3 "$LOGS/rbench-temporal-gw-$w.log"
done
