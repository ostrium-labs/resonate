#!/usr/bin/env bash
# Raw Resonate on a local store: start the server on a fresh store, run the
# three workloads with rbench, stop the server.
#
#   bench/scripts/run-resonate-local.sh sqlite
#   bench/scripts/run-resonate-local.sh postgres     # needs rbench-pg (bench/stacks/postgres)
#
# Store data lives on $RBENCH_DATA (ext4 /mnt/Projects here, same disk for every local leg).
set -euo pipefail
store=$1
OUT=${OUT:-bench/results/2026-10-24}
RBENCH_DATA=${RBENCH_DATA:-/mnt/Projects/rbench-data}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs}
SERVER_BIN=${SERVER_BIN:?}
RBENCH_BIN=${RBENCH_BIN:?}
FAN_N=${FAN_N:-500} FAN_C=${FAN_C:-20} FAN_K=${FAN_K:-8}
CH_N=${CH_N:-100} CH_C=${CH_C:-20} CH_L=${CH_L:-5} CH_S=${CH_S:-1000}
BU_N=${BU_N:-500}
POOL=${POOL:-32}
WORKLOADS=${WORKLOADS:-fanout chain burst}
mkdir -p "$OUT/runs" "$LOGS" "$RBENCH_DATA"
if curl -s -o /dev/null http://127.0.0.1:18001/ready; then echo "port 18001 is taken" >&2; exit 1; fi

case $store in
sqlite)
  rm -rf "$RBENCH_DATA/sqlite"; mkdir -p "$RBENCH_DATA/sqlite"
  storeenv=(RESONATE_SERVERS__ACTIVE=server_sqlite "RESONATE_SERVERS__SERVER_SQLITE__PATH=$RBENCH_DATA/sqlite/resonate.db")
  watch=() ; note="SQLite file on ext4 (/mnt/Projects)" ;;
postgres)
  podman exec rbench-pg psql -U resonate -d postgres -qc 'DROP DATABASE IF EXISTS resonate' -c 'CREATE DATABASE resonate' >/dev/null
  storeenv=(RESONATE_SERVERS__ACTIVE=server_postgres "RESONATE_SERVERS__SERVER_POSTGRES__URL=postgres://resonate:resonate@127.0.0.1:15432/resonate" "RESONATE_SERVERS__SERVER_POSTGRES__POOL_SIZE=$POOL")
  watch=(--watch container:rbench-pg); note="Postgres 17.10 in podman (rbench-pg), data on ext4 (/mnt/Projects), pool $POOL" ;;
*) echo "store: sqlite | postgres" >&2; exit 2 ;;
esac

server=""
trap '[ -n "$server" ] && kill "$server" 2>/dev/null; wait 2>/dev/null || true' EXIT
env -i HOME="$HOME" PATH="$PATH" "${storeenv[@]}" \
  RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=127.0.0.1:18001 \
  RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=127.0.0.1:18002 \
  OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317} \
  OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
  OTEL_SERVICE_NAME=resonate-server \
  "$SERVER_BIN" serve --level warn >"$LOGS/server-$store.log" 2>&1 &
server=$!
for _ in $(seq 1 60); do
  curl -fsS -o /dev/null http://127.0.0.1:18001/ready 2>/dev/null && break
  kill -0 "$server" 2>/dev/null || { tail -20 "$LOGS/server-$store.log" >&2; exit 1; }
  sleep 1
done
curl -fsS -o /dev/null http://127.0.0.1:18001/ready

common=(--system resonate --backend "$store" --out "$OUT/runs" --watch "pid:$server" "${watch[@]}" --watch proc:rbench --note "$note")
for w in $WORKLOADS; do
  case $w in
  fanout) args=(-n "$FAN_N" -c "$FAN_C" -k "$FAN_K") ;;
  chain) args=(-n "$CH_N" -c "$CH_C" -l "$CH_L" --sleep-ms "$CH_S") ;;
  burst) args=(-n "$BU_N") ;;
  esac
  "$RBENCH_BIN" resonate --workload "$w" "${args[@]}" "${common[@]}" 2>"$LOGS/rbench-resonate-$store-$w.log" || true
  grep -h '^rbench: resonate' "$LOGS/rbench-resonate-$store-$w.log" || tail -3 "$LOGS/rbench-resonate-$store-$w.log"
done
