#!/usr/bin/env bash
# Five TiDB Cloud Starter instances "at full": five independent Resonate
# servers (MySQL plugin), one per instance, driven in parallel by five rbench
# processes, with a concurrency sweep for fan-out and burst. Every instance is
# deleted at the end (the trap), whatever happens.
#
#   bench/scripts/run-tidb-5x.sh <region> [<existing cluster id> ...]
#
# Env: SERVER_BIN, RBENCH_BIN, OUT (default bench/results/2026-10-24/tidb-starter-5x),
#      FAN_SWEEP ("C:N ..." per server), BURST_SWEEP ("N ..." per server), POOL.
set -euo pipefail
region=$1; shift
here=$(cd "$(dirname "$0")" && pwd)
tc="$here/tidbcloud.sh"
OUT=${OUT:-bench/results/2026-10-24/tidb-starter-5x}
LOGS=${LOGS:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/logs/tidb5x}
SERVER_BIN=${SERVER_BIN:?}
RBENCH_BIN=${RBENCH_BIN:?}
POOL=${POOL:-64}
FAN_SWEEP=${FAN_SWEEP:-"4:8 16:32 64:128"}
BURST_SWEEP=${BURST_SWEEP:-"50 200 500"}
mkdir -p "$OUT/runs" "$LOGS"

ids=("$@")
servers=()
cleanup() {
  for p in "${servers[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  for id in "${ids[@]}"; do "$tc" delete "$id" || echo "DELETE FAILED for $id: delete it by hand" >&2; done
}
trap cleanup EXIT

# Create what is missing, in parallel.
need=$((5 - ${#ids[@]}))
tmp=$(mktemp -d -p "$LOGS")
for i in $(seq 1 $need); do ( "$tc" create "$region" >"$tmp/$i" 2>"$tmp/$i.err" ) & done
wait
bad=0
for i in $(seq 1 $need); do
  id=$(cat "$tmp/$i" 2>/dev/null || true)
  # Track every instance that was made before failing, so the trap deletes it.
  if [ -n "$id" ]; then ids+=("$id"); else echo "create $i failed: $(cat "$tmp/$i.err")" >&2; bad=1; fi
done
[ $bad = 0 ] || exit 1
printf '%s\n' "${ids[@]}" >"$OUT/instances.txt"
echo "instances: ${ids[*]}"

# A server per instance: ports 1802i (HTTP), 1803i (metrics), push 1920i.
for i in 1 2 3 4 5; do
  id=${ids[$((i - 1))]}
  url=$("$tc" url "$id")
  env -i HOME="$HOME" PATH="$PATH" \
    RESONATE_SERVERS__ACTIVE=server_mysql \
    RESONATE_SERVERS__SERVER_MYSQL__URL="$url" \
    RESONATE_SERVERS__SERVER_MYSQL__POOL_SIZE=$POOL \
    RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=127.0.0.1:1802$i \
    RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=127.0.0.1:1803$i \
    OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317} \
    OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
    OTEL_SERVICE_NAME=resonate-server-tidb5x-$i \
    "$SERVER_BIN" serve --level warn >"$LOGS/server-$i.log" 2>&1 &
  servers+=($!)
done
for i in 1 2 3 4 5; do
  for _ in $(seq 1 120); do curl -fsS -o /dev/null "http://127.0.0.1:1802$i/ready" 2>/dev/null && break; sleep 1; done
  curl -fsS -o /dev/null "http://127.0.0.1:1802$i/ready"
done
echo "servers up: ${servers[*]}"

step() { # workload, per-server args..., label
  local w=$1 label=$2; shift 2
  local pids=()
  for i in 1 2 3 4 5; do
    "$RBENCH_BIN" resonate --workload "$w" "$@" -k 8 \
      --url "http://127.0.0.1:1802$i" --push-listen "127.0.0.1:1920$i" \
      --system resonate --backend "tidb5x-$region-i$i" --out "$OUT/runs" --run-id "tidb5x-$w-$label-i$i" \
      --watch "pid:${servers[$((i - 1))]}" \
      --note "5 Starter instances at once ($region), instance $i of 5, pool $POOL; step $label" \
      2>"$LOGS/rbench-$w-$label-i$i.log" &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || true; done
  for i in 1 2 3 4 5; do grep -h '^rbench: resonate' "$LOGS/rbench-$w-$label-i$i.log" || true; done
  python3 -I "$here/tidb5x.py" "$OUT" "$w" "$label"
}

TAG=${TAG:-}
for s in $FAN_SWEEP; do c=${s%%:*} n=${s##*:}; step fanout "${TAG}c$c" -n "$n" -c "$c"; done
for n in $BURST_SWEEP; do step burst "${TAG}n$n" -n "$n"; done

# Starter throttling shows as MySQL error 8252 ("Exceeded resource group quota
# limitation") or refused connections; count those, not digits in timings.
grep -hcE '(Error|ERROR|error|code)[: ]+8252|[Qq]uota limitation|[Ee]xceeded resource group|Too many connections|max_user_connections' "$LOGS"/server-*.log | paste -sd' ' | sed 's/^/throttling lines per server: /' || true
grep -hc 'pool timed out' "$LOGS"/server-*.log | paste -sd' ' | sed 's/^/client pool timeouts per server: /' || true
