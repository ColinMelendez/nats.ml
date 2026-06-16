#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
NATS_TEST_JS_CLUSTER_SCENARIO=kv \
NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_FAILURE_MODES="${NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_FAILURE_MODES:-node-a,node-b,node-c,leader,restart,multi-node}" \
  exec \
  "$script_dir/runtest-interop-auth-jetstream-cluster-matrix.sh" "$@"
