#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
NATS_TEST_JS_CLUSTER_SCENARIO=object exec \
  "$script_dir/runtest-interop-auth-jetstream-cluster-matrix.sh" "$@"
