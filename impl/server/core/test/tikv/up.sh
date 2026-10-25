#!/usr/bin/env bash
# A single-node PD + TiKV (API v2) for tests and local runs, on the host
# network. Usage: up.sh [pd-port] [tikv-port]   (defaults 2379, 20160)
#
#   TEST_TIKV_PD=127.0.0.1:2379 cargo test -p resonate-server-blob --features tikv
#
# Uses podman when it is on PATH, else docker. Stop with down.sh.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
pd_port="${1:-2379}"
tikv_port="${2:-20160}"
peer_port=$((pd_port + 1))
status_port=$((tikv_port + 20))
tag="${TIKV_TAG:-v8.5.8}"
name="${TIKV_NAME:-resonate-tikv}"
run="$(command -v podman || command -v docker)"

"$run" run -d --name "$name-pd" --network host \
  -v "$here/pd.toml:/etc/pd.toml:ro,z" \
  "docker.io/pingcap/pd:$tag" \
  --name=pd --data-dir=/data/pd --config=/etc/pd.toml \
  --client-urls="http://127.0.0.1:$pd_port" \
  --peer-urls="http://127.0.0.1:$peer_port" \
  --initial-cluster="pd=http://127.0.0.1:$peer_port" >/dev/null

"$run" run -d --name "$name-tikv" --network host \
  -v "$here/tikv.toml:/etc/tikv.toml:ro,z" \
  "docker.io/pingcap/tikv:$tag" \
  --addr="127.0.0.1:$tikv_port" --status-addr="127.0.0.1:$status_port" \
  --data-dir=/data/tikv --pd="127.0.0.1:$pd_port" --config=/etc/tikv.toml >/dev/null

# Ready when PD reports one store Up and a keyspace can be created.
for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:$pd_port/pd/api/v1/stores" 2>/dev/null | grep -q '"state_name": "Up"'; then
    if curl -fsS -X POST "http://127.0.0.1:$pd_port/pd/api/v2/keyspaces" \
        -H 'content-type: application/json' -d '{"name":"resonate_ready_probe"}' >/dev/null 2>&1 ||
       curl -fsS "http://127.0.0.1:$pd_port/pd/api/v2/keyspaces/resonate_ready_probe" >/dev/null 2>&1; then
      echo "PD 127.0.0.1:$pd_port, TiKV 127.0.0.1:$tikv_port: up"
      exit 0
    fi
  fi
  sleep 1
done
echo "TiKV did not come up" >&2
"$run" logs --tail 30 "$name-tikv" >&2 || true
exit 1
