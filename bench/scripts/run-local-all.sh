#!/usr/bin/env bash
# Every local leg, one after another, each stack up only for its own leg, fresh
# data each time: raw Resonate (SQLite, Postgres), the Loams engine's /durable/
# on TiKV, stock Temporal, Airflow. The SigNoz stack stays up throughout.
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
cd "$here/.."
export RBENCH_DATA=${RBENCH_DATA:-/mnt/Projects/rbench-data}
export SERVER_BIN RBENCH_BIN
stacks=bench/stacks
wipe() { podman unshare rm -rf "$@"; mkdir -p "$@"; }
log() { echo "== $(date -u +%T) $* (load $(cut -d' ' -f1-3 /proc/loadavg))"; }

podman compose -p rbench-airflow -f $stacks/airflow/compose.yaml stop >/dev/null 2>&1

log "resonate sqlite"; bench/scripts/run-resonate-local.sh sqlite

log "resonate postgres"; wipe "$RBENCH_DATA/pg"
podman compose -p rbench-pg -f $stacks/postgres/compose.yaml up -d >/dev/null 2>&1
until podman healthcheck run rbench-pg >/dev/null 2>&1; do sleep 2; done
bench/scripts/run-resonate-local.sh postgres
podman compose -p rbench-pg -f $stacks/postgres/compose.yaml stop >/dev/null 2>&1

log "loams /durable/ on tikv"; wipe "$RBENCH_DATA/tikv/pd" "$RBENCH_DATA/tikv/tikv" "$RBENCH_DATA/loams"
podman compose -p rbench-tikv -f $stacks/tikv/compose.yaml --profile loams up -d >/dev/null 2>&1
for _ in $(seq 1 90); do curl -fs -o /dev/null http://127.0.0.1:18090/health && break; sleep 2; done
sleep 10
bench/scripts/run-loams.sh
podman compose -p rbench-tikv -f $stacks/tikv/compose.yaml --profile loams stop >/dev/null 2>&1

log "temporal"; wipe "$RBENCH_DATA/temporal-pg"
( cd $stacks/temporal && podman compose -f compose.yaml -f compose.data.yaml -f compose.otel.yaml down >/dev/null 2>&1
  podman compose -f compose.yaml -f compose.data.yaml -f compose.otel.yaml up -d postgres >/dev/null 2>&1
  podman network connect rbench-temporal_default rbench-otelcol 2>/dev/null
  podman compose -f compose.yaml -f compose.data.yaml -f compose.otel.yaml up -d >/dev/null 2>&1
  podman wait rbench-temporal-namespace )
sleep 10
bench/scripts/run-temporal.sh
( cd $stacks/temporal && podman compose -f compose.yaml -f compose.data.yaml -f compose.otel.yaml stop >/dev/null 2>&1 )

log "airflow"; wipe "$RBENCH_DATA/airflow-pg"
( cd $stacks/airflow && podman compose -p rbench-airflow -f compose.yaml -f compose.data.yaml -f compose.otel.yaml down >/dev/null 2>&1
  podman compose -p rbench-airflow -f compose.yaml -f compose.data.yaml -f compose.otel.yaml up -d postgres >/dev/null 2>&1
  podman network connect rbench-airflow_default rbench-otelcol 2>/dev/null
  podman compose -p rbench-airflow -f compose.yaml -f compose.data.yaml -f compose.otel.yaml up -d >/dev/null 2>&1 )
for _ in $(seq 1 120); do
  t=$(curl -s -XPOST http://127.0.0.1:18080/auth/token -H 'content-type: application/json' -d '{"username":"admin","password":"admin"}' | jq -r .access_token 2>/dev/null)
  [ -n "$t" ] && [ "$t" != null ] && [ "$(curl -s -H "Authorization: Bearer $t" 'http://127.0.0.1:18080/api/v2/dags?dag_id_pattern=rbench' | jq -r .total_entries)" = 3 ] && break
  sleep 5
done
sleep 20
bench/scripts/run-airflow.sh
( cd $stacks/airflow && podman compose -p rbench-airflow -f compose.yaml -f compose.data.yaml -f compose.otel.yaml stop >/dev/null 2>&1 )
log "done"
