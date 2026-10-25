#!/usr/bin/env bash
# Request Units per workflow, for the cost model.
#
# TiDB Cloud Starter bills Request Units but does not let a user read them:
# its API has no usage view for an instance, and statements_summary answers
# "SELECT command denied" (checked 2026-10-25). So this runs the same Resonate
# server (MySQL plugin) against a local TiDB v8.5.8 with resource control on
# (bench/stacks/tikv, profile tidb) and reads RU from statements_summary,
# before and after each workload. Stored bytes per workflow come from
# information_schema.tables after ANALYZE.
#
#   SERVER_BIN=... RBENCH_BIN=... bench/scripts/measure-ru.sh
set -euo pipefail
OUT=${OUT:-bench/results/2026-10-24}
RBENCH_DATA=${RBENCH_DATA:-/mnt/Projects/rbench-data}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs}
SERVER_BIN=${SERVER_BIN:?}
RBENCH_BIN=${RBENCH_BIN:?}
stack=(podman compose -p rbench-tikv -f bench/stacks/tikv/compose.yaml --profile tidb)
sql() { mariadb -h 127.0.0.1 -P 14000 -u root -N -B -e "$1"; }
ru() {
  sql "SELECT COALESCE(SUM(EXEC_COUNT*AVG_REQUEST_UNIT_READ),0), COALESCE(SUM(EXEC_COUNT*AVG_REQUEST_UNIT_WRITE),0) FROM (
         SELECT EXEC_COUNT, AVG_REQUEST_UNIT_READ, AVG_REQUEST_UNIT_WRITE, SCHEMA_NAME FROM information_schema.CLUSTER_STATEMENTS_SUMMARY
         UNION ALL
         SELECT EXEC_COUNT, AVG_REQUEST_UNIT_READ, AVG_REQUEST_UNIT_WRITE, SCHEMA_NAME FROM information_schema.CLUSTER_STATEMENTS_SUMMARY_HISTORY
       ) s WHERE SCHEMA_NAME = 'resonate'"
}
bytes() {
  for t in $(sql "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='resonate'"); do sql "ANALYZE TABLE resonate.\`$t\`" >/dev/null; done
  sql "SELECT COALESCE(SUM(DATA_LENGTH+INDEX_LENGTH),0) FROM information_schema.TABLES WHERE TABLE_SCHEMA='resonate'"
}

"${stack[@]}" down >/dev/null 2>&1 || true
podman unshare rm -rf "$RBENCH_DATA/tikv"; mkdir -p "$RBENCH_DATA/tikv/pd" "$RBENCH_DATA/tikv/tikv" "$RBENCH_DATA/loams"
"${stack[@]}" up -d >/dev/null 2>&1
for _ in $(seq 1 120); do sql "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
sql "CREATE DATABASE IF NOT EXISTS resonate"
sql "SET GLOBAL tidb_stmt_summary_max_stmt_count = 20000; SET GLOBAL tidb_stmt_summary_refresh_interval = 86400; SET GLOBAL tidb_stmt_summary_history_size = 255"

server=""
trap '[ -n "$server" ] && kill "$server" 2>/dev/null; wait 2>/dev/null; "${stack[@]}" down >/dev/null 2>&1 || true' EXIT
env -i HOME="$HOME" PATH="$PATH" \
  RESONATE_SERVERS__ACTIVE=server_mysql \
  RESONATE_SERVERS__SERVER_MYSQL__URL=mysql://root@127.0.0.1:14000/resonate \
  RESONATE_SERVERS__SERVER_MYSQL__POOL_SIZE=32 \
  RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=127.0.0.1:18001 \
  RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=127.0.0.1:18002 \
  "$SERVER_BIN" serve --level warn >"$LOGS/server-ru-tidb.log" 2>&1 &
server=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null http://127.0.0.1:18001/ready 2>/dev/null && break; sleep 1; done

READ_X=${READ_X:-1.5} WRITE_X=${WRITE_X:-2.26}
result="{\"starter_scaling\": {\"read\": $READ_X, \"write\": $WRITE_X, \"why\": \"per-statement RU from @@tidb_last_query_info, 20 of each, local TiDB v8.5.8 vs Starter aws-us-west-2 (2026-10-25): INSERT 2.90 vs 6.73, autocommit UPDATE 3.28 vs 7.23, SELECT FOR UPDATE 1.70 vs 2.55, point get 0.52 vs 0.50; bench-data ru-spot-*.txt\"}, \"method\": \"Resonate server (MySQL plugin) on a local TiDB v8.5.8 (ghcr.io/ostrium-labs/tidb:v8.5.8-loams.1, keyspace mode) over one PD + one TiKV v8.5.8; RU = sum over statements_summary(+history) of EXEC_COUNT*(AVG_REQUEST_UNIT_READ+AVG_REQUEST_UNIT_WRITE) for schema resonate, before/after each workload; bytes = sum(DATA_LENGTH+INDEX_LENGTH) after ANALYZE, delta per workload divided by its workflows\", \"workloads\": {"
first=1; total_wf=0; prev_b=0
for spec in "fanout 100 10" "chain 20 10" "burst 100 100"; do
  set -- $spec; w=$1 n=$2 c=$3
  read -r br bw <<<"$(ru)"
  "$RBENCH_BIN" resonate --workload "$w" -n "$n" -c "$c" -k 8 -l 5 --sleep-ms 1000 --system resonate --backend tidb-local \
    --out "$OUT/runs" --watch "pid:$server" --note "RU measurement run (local TiDB v8.5.8)" 2>"$LOGS/rbench-ru-$w.log" || true
  grep -h '^rbench: resonate' "$LOGS/rbench-ru-$w.log" >&2 || true
  sleep 2
  read -r ar aw <<<"$(ru)"
  b=$(bytes)
  [ $first = 1 ] || result+=", "; first=0
  result+=$(python3 -c "
import json
r=($ar-$br)/$n; w=($aw-$bw)/$n
print(json.dumps('$w')+': '+json.dumps({'workflows': $n, 'ru_read_per_wf_local': r, 'ru_write_per_wf_local': w,
  'ru_per_wf_local': r+w, 'ru_per_wf': r*$READ_X+w*$WRITE_X, 'bytes_total_after': $b, 'bytes_per_wf': max($b-$prev_b,0)/$n}))")
  prev_b=$b
done
result+="}}"
echo "$result" | python3 -m json.tool > "$OUT/ru-per-workflow.json"
cat "$OUT/ru-per-workflow.json"
