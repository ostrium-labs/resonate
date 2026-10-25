#!/usr/bin/env bash
# Every local leg, one after another, each stack up only for its own leg, fresh
# data each time: raw Resonate (SQLite, Postgres), the Loams engine's /durable/
# on TiKV, stock Temporal, Airflow. The SigNoz stack stays up throughout.
#   LEGS="sqlite postgres loams temporal airflow" SERVER_BIN=... RBENCH_BIN=... bench/scripts/run-local-all.sh
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
cd "$here/.."
export RBENCH_DATA=${RBENCH_DATA:-/mnt/Projects/rbench-data}
export SERVER_BIN RBENCH_BIN
LEGS=${LEGS:-sqlite postgres tikv loams temporal airflow}
want() { case " $LEGS " in *" $1 "*) return 0 ;; esac; return 1; }
stacks=bench/stacks
wipe() { podman unshare rm -rf "$@"; mkdir -p "$@"; }
log() { echo "== $(date -u +%T) $* (load $(cut -d' ' -f1-3 /proc/loadavg))"; }
pg=(podman compose -p rbench-pg -f $stacks/postgres/compose.yaml)
kv=(podman compose -p rbench-tikv -f $stacks/tikv/compose.yaml --profile loams)
tp=(podman compose -f $stacks/temporal/compose.yaml -f $stacks/temporal/compose.data.yaml -f $stacks/temporal/compose.otel.yaml)
af=(podman compose -p rbench-airflow -f $stacks/airflow/compose.yaml -f $stacks/airflow/compose.data.yaml -f $stacks/airflow/compose.otel.yaml)

"${af[@]}" stop >/dev/null 2>&1

if want sqlite; then log "resonate sqlite"; bench/scripts/run-resonate-local.sh sqlite; fi

if want postgres; then
  log "resonate postgres"
  "${pg[@]}" down >/dev/null 2>&1; wipe "$RBENCH_DATA/pg"
  "${pg[@]}" up -d >/dev/null 2>&1
  until podman healthcheck run rbench-pg >/dev/null 2>&1; do sleep 2; done
  sleep 3
  bench/scripts/run-resonate-local.sh postgres
  "${pg[@]}" down >/dev/null 2>&1
fi

if want tikv; then
  log "resonate tikv (FW1 store)"
  kvp=(podman compose -p rbench-tikv -f $stacks/tikv/compose.yaml)
  "${kv[@]}" down >/dev/null 2>&1; wipe "$RBENCH_DATA/tikv/pd" "$RBENCH_DATA/tikv/tikv"
  "${kvp[@]}" up -d >/dev/null 2>&1
  for _ in $(seq 1 90); do curl -fs -o /dev/null http://127.0.0.1:20180/status && break; sleep 2; done
  sleep 10
  SERVER_BIN=${TIKV_SERVER_BIN:-$SERVER_BIN} bench/scripts/run-resonate-local.sh tikv
  "${kvp[@]}" down >/dev/null 2>&1
fi

if want loams; then
  log "loams /durable/ on tikv"
  "${kv[@]}" down >/dev/null 2>&1; wipe "$RBENCH_DATA/tikv/pd" "$RBENCH_DATA/tikv/tikv" "$RBENCH_DATA/loams"
  "${kv[@]}" up -d >/dev/null 2>&1
  for _ in $(seq 1 90); do curl -fs -o /dev/null http://127.0.0.1:18090/health && break; sleep 2; done
  sleep 10
  bench/scripts/run-loams.sh
  "${kv[@]}" down >/dev/null 2>&1
fi

if want temporal; then
  log "temporal"
  "${tp[@]}" down >/dev/null 2>&1; wipe "$RBENCH_DATA/temporal-pg"
  "${tp[@]}" up -d postgres >/dev/null 2>&1
  podman network connect rbench-temporal_default rbench-otelcol 2>/dev/null
  "${tp[@]}" up -d >/dev/null 2>&1
  podman wait rbench-temporal-namespace >/dev/null
  sleep 10
  bench/scripts/run-temporal.sh
  "${tp[@]}" down >/dev/null 2>&1
fi

if want airflow; then
  log "airflow"
  "${af[@]}" down >/dev/null 2>&1; wipe "$RBENCH_DATA/airflow-pg"
  "${af[@]}" up -d postgres >/dev/null 2>&1
  podman network connect rbench-airflow_default rbench-otelcol 2>/dev/null
  "${af[@]}" up -d >/dev/null 2>&1
  for _ in $(seq 1 120); do
    t=$(curl -s -XPOST http://127.0.0.1:18080/auth/token -H 'content-type: application/json' -d '{"username":"admin","password":"admin"}' | jq -r .access_token 2>/dev/null)
    [ -n "$t" ] && [ "$t" != null ] && [ "$(curl -s -H "Authorization: Bearer $t" 'http://127.0.0.1:18080/api/v2/dags?dag_id_pattern=rbench' | jq -r .total_entries)" = 3 ] && break
    sleep 5
  done
  sleep 20
  bench/scripts/run-airflow.sh
  "${af[@]}" down >/dev/null 2>&1
fi
log "done"
