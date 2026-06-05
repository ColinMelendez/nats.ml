#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
auth_mode=${NATS_TEST_INTEROP_AUTH_MODE:-nkey}
negative_mode=${NATS_TEST_INTEROP_AUTH_NEGATIVE-}

case "$auth_mode" in
  nkey|jwt|mtls)
    ;;
  *)
    echo "NATS_TEST_INTEROP_AUTH_MODE must be nkey, jwt, or mtls" >&2
    exit 1
    ;;
esac

if [ -z "$negative_mode" ]; then
  case "$auth_mode" in
    nkey|jwt) negative_mode=credentials ;;
    mtls) negative_mode=certificate ;;
  esac
fi

case "$auth_mode:$negative_mode" in
  nkey:credentials|jwt:credentials|mtls:certificate)
    ;;
  *)
    echo "unsupported authentication negative combination: $auth_mode/$negative_mode" >&2
    exit 1
    ;;
esac

if [ "$auth_mode" = mtls ]; then
  tls_enabled=1
else
  tls_enabled=${NATS_TEST_TLS-0}
fi

NATS_TEST_TLS="$tls_enabled" NATS_TEST_INTEROP_AUTH_MODE="$auth_mode" \
  NATS_TEST_INTEROP_AUTH_NEGATIVE="$negative_mode" \
  exec "$script_dir/runtest-interop.sh" "$@"
