#!/usr/bin/env bash
# Stops and removes what up.sh started (and its data).
set -uo pipefail
name="${TIKV_NAME:-resonate-tikv}"
run="$(command -v podman || command -v docker)"
"$run" rm -f "$name-tikv" "$name-pd" >/dev/null 2>&1
echo "stopped $name"
