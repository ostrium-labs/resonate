#!/usr/bin/env bash
# TiDB Cloud Starter clusters for the bench, through the v1beta1 serverless API.
# Every cluster this script makes is named rbench-<region>-<date> and labelled
# rbench=1; delete refuses any cluster without both. Keys come from
# $TIDBCLOUD_ENV (default ~/tidbcloud-lc0.env) and are never printed.
#
#   tidbcloud.sh regions                 Starter regions the API offers
#   tidbcloud.sh list                    clusters made by this script
#   tidbcloud.sh create <region-id>      create (spending limit 0), wait ACTIVE, print id
#   tidbcloud.sh url <cluster-id>        mysql:// URL (password from the state dir)
#   tidbcloud.sh host <cluster-id>       host:port of the public endpoint
#   tidbcloud.sh delete <cluster-id>     delete one of ours, wait until gone
set -euo pipefail
API=https://serverless.tidbapi.com/v1beta1
STATE=${RBENCH_STATE:-$HOME/Documents/Ostriumlabs/resonate-wt/bench-data/tidbcloud}
mkdir -p "$STATE"; chmod 700 "$STATE"
set -a; source "${TIDBCLOUD_ENV:-$HOME/tidbcloud-lc0.env}"; set +a
api() { curl -sS --fail-with-body --digest -u "$TIDB_CLOUD_PUBLIC_KEY:$TIDB_CLOUD_PRIVATE_KEY" -H 'content-type: application/json' "$@"; }

case ${1:-} in
regions)
  api "$API/regions" | jq -r '.regions[] | select(.servicePlans | index("Starter")) | "\(.regionId)\t\(.displayName)"' ;;
list)
  api "$API/clusters?pageSize=100" | jq -r '.clusters[]? | select(.labels.rbench=="1") | "\(.clusterId)\t\(.displayName)\t\(.region.regionId)\t\(.state)"' ;;
create)
  region=$2; name="rbench-${region}-$(date +%m%d%H%M)-$(od -An -N2 -tu2 /dev/urandom | tr -d " ")"
  pw=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)
  body=$(jq -n --arg n "$name" --arg r "regions/$region" --arg p "$pw" \
    '{displayName:$n, region:{name:$r}, spendingLimit:{monthly:0}, rootPassword:$p, labels:{rbench:"1"}}')
  id=$(api -X POST "$API/clusters" -d "$body" | jq -r .clusterId)
  [ -n "$id" ] && [ "$id" != null ] || { echo "create failed" >&2; exit 1; }
  umask 077; printf '%s' "$pw" >"$STATE/$id.pw"; printf '%s\t%s\t%s\n' "$id" "$name" "$region" >>"$STATE/created.tsv"
  for _ in $(seq 1 120); do
    s=$(api "$API/clusters/$id" | jq -r .state)
    [ "$s" = ACTIVE ] && { echo "$id"; exit 0; }
    sleep 5
  done
  echo "cluster $id not ACTIVE after 10 min (state $s)" >&2; exit 1 ;;
host)
  api "$API/clusters/$2" | jq -r '"\(.endpoints.public.host):\(.endpoints.public.port)"' ;;
url)
  c=$(api "$API/clusters/$2")
  host=$(jq -r .endpoints.public.host <<<"$c"); port=$(jq -r .endpoints.public.port <<<"$c"); pre=$(jq -r .userPrefix <<<"$c")
  printf 'mysql://%s.root:%s@%s:%s/test?ssl-mode=required' "$pre" "$(cat "$STATE/$2.pw")" "$host" "$port" ;;
delete)
  id=$2; c=$(api "$API/clusters/$id")
  name=$(jq -r .displayName <<<"$c"); lab=$(jq -r '.labels.rbench // ""' <<<"$c")
  case $name in rbench-*) ;; *) echo "refusing: $id is $name, not ours" >&2; exit 1;; esac
  [ "$lab" = 1 ] || { echo "refusing: $id has no rbench label" >&2; exit 1; }
  grep -q "^$id	" "$STATE/created.tsv" || { echo "refusing: $id not in $STATE/created.tsv" >&2; exit 1; }
  api -X DELETE "$API/clusters/$id" >/dev/null
  for _ in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --digest -u "$TIDB_CLOUD_PUBLIC_KEY:$TIDB_CLOUD_PRIVATE_KEY" "$API/clusters/$id")
    s=$(api "$API/clusters/$id" 2>/dev/null | jq -r '.state // ""' || true)
    # A deleted cluster answers 403 or 404 and drops out of the list.
    gone=$(api "$API/clusters?pageSize=100" | jq --arg id "$id" '[.clusters[]? | select(.clusterId==$id)] | length')
    if { [ "$code" = 404 ] || [ "$code" = 403 ] || [ "$s" = DELETED ]; } && [ "$gone" = 0 ]; then printf '%s\t%s\tdeleted %s\n' "$id" "$name" "$(date -u +%FT%TZ)" >>"$STATE/deleted.tsv"; echo "deleted $id $name"; exit 0; fi
    sleep 5
  done
  echo "delete of $id not confirmed" >&2; exit 1 ;;
*) sed -n '2,13p' "$0"; exit 2 ;;
esac
