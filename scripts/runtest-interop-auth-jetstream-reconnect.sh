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
if [ "$auth_mode" = mtls ]; then
  NATS_TEST_TLS=1 NATS_TEST_INTEROP_AUTH_MODE="$auth_mode" \
    exec "$script_dir/runtest-interop-jetstream-reconnect.sh" "$@"
else
  NATS_TEST_INTEROP_AUTH_MODE="$auth_mode" \
    exec "$script_dir/runtest-interop-jetstream-reconnect.sh" "$@"
fi
