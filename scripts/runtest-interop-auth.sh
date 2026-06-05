#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
auth_mode=${NATS_TEST_INTEROP_AUTH_MODE:-nkey}
case "$auth_mode" in
  nkey|jwt|mtls)
    ;;
  *)
    echo "NATS_TEST_INTEROP_AUTH_MODE must be nkey, jwt, or mtls" >&2
    exit 1
    ;;
esac

unset NATS_TEST_INTEROP_AUTH_NEGATIVE
NATS_TEST_INTEROP_AUTH_MODE="$auth_mode" \
  exec "$script_dir/runtest-interop.sh" "$@"
