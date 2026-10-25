#!/bin/sh
# One-shot init for the rbench Temporal stack, run in the pinned admin-tools
# image (as DX0's deploy/release/compute/durable-init.sh in ostrium-labs/loams).
#
#   init.sh schema     # databases + schema (temporal, temporal_visibility)
#   init.sh namespace  # waits for the frontend, creates namespace `default`
#
# Idempotent: running it again on an existing volume changes nothing.
set -eu

PG_HOST=${PG_HOST:-postgres}
PG_PORT=${PG_PORT:-5432}
PG_USER=${PG_USER:-temporal}
PG_PASSWORD=${PG_PASSWORD:-temporal}
FRONTEND=${FRONTEND:-temporal:7233}
NAMESPACE=${NAMESPACE:-default}
WAIT_SECONDS=${WAIT_SECONDS:-180}

log() { echo "rbench-temporal-init: $*"; }

sql() {
  db=$1
  shift
  temporal-sql-tool --plugin postgres12 --ep "$PG_HOST" -p "$PG_PORT" \
    -u "$PG_USER" --pw "$PG_PASSWORD" --db "$db" "$@"
}

schema() {
  # compose starts this after Postgres reports healthy (pg_isready).
  schema_dir=/etc/temporal/schema/postgresql/v12
  for pair in temporal:temporal temporal_visibility:visibility; do
    db=${pair%%:*}
    dir=${pair#*:}
    sql "$db" create-database >/dev/null 2>&1 || log "$db: database already there"
    # setup-schema fails on a database that already has the versioning
    # tables; the update below (which needs them) is the real check.
    sql "$db" setup-schema -v 0.0 >/dev/null 2>&1 || log "$db: versioning tables already there"
    sql "$db" update-schema -d "$schema_dir/$dir/versioned"
    log "$db: schema up to date"
  done
}

namespace() {
  # The CLI's gRPC dial by name timed out on podman's DNS while the address
  # worked (2026-10-24): dial the resolved address.
  host=${FRONTEND%:*}; port=${FRONTEND##*:}
  ip=$(getent hosts "$host" | awk '{print $1; exit}')
  [ -n "$ip" ] && FRONTEND="$ip:$port"
  i=0
  until temporal operator cluster health --address "$FRONTEND" 2>/dev/null | grep -q SERVING; do
    i=$((i + 2))
    [ "$i" -ge "$WAIT_SECONDS" ] && { log "frontend $FRONTEND not SERVING in ${WAIT_SECONDS}s"; exit 1; }
    sleep 2
  done
  if temporal operator namespace describe --address "$FRONTEND" -n "$NAMESPACE" >/dev/null 2>&1; then
    log "namespace $NAMESPACE already there"
  else
    temporal operator namespace create --address "$FRONTEND" --retention 72h -n "$NAMESPACE"
    log "namespace $NAMESPACE created"
  fi
  # A new namespace takes a moment to reach every service's cache.
  i=0
  until temporal workflow list --address "$FRONTEND" --namespace "$NAMESPACE" --limit 1 >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge "$WAIT_SECONDS" ] && { log "namespace $NAMESPACE not usable in ${WAIT_SECONDS}s"; exit 1; }
    sleep 1
  done
  log "ready"
}

case ${1:-} in
  schema) schema ;;
  namespace) namespace ;;
  *) echo "usage: init.sh schema | namespace" >&2; exit 2 ;;
esac
