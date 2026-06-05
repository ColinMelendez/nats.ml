#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
matrix_modes=${NATS_INTEROP_KEY_VALUE_MATRIX_MODES:-${NATS_INTEROP_JETSTREAM_MATRIX_MODES:-anonymous}}
NATS_INTEROP_JETSTREAM_MATRIX_SCENARIOS=kv \
  NATS_INTEROP_JETSTREAM_MATRIX_MODES="$matrix_modes" \
  exec "$script_dir/runtest-interop-jetstream-matrix.sh" "$@"
