#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
NATS_TEST_INTEROP_MODE=service-parent-close exec "$script_dir/runtest-interop.sh" "$@"
