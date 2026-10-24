#!/bin/sh
# Bring the rbench Temporal stack up and wait until namespace `default` is
# usable. Extra environment (TEMPORAL_OTEL_TRACES_EXPORTER, ...) passes
# through to compose. Tear down with:
#   podman compose -f bench/stacks/temporal/compose.yaml down -v
set -eu
here=$(cd "$(dirname "$0")" && pwd)
podman compose -f "$here/compose.yaml" up -d
code=$(podman wait rbench-temporal-namespace)
if [ "$code" != 0 ]; then
  podman logs rbench-temporal-namespace >&2 || true
  echo "rbench-temporal: namespace init failed (exit $code)" >&2
  exit 1
fi
echo "rbench-temporal: ready on 127.0.0.1:17233 (namespace default)"
