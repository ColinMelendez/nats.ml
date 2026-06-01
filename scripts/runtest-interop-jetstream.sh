#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-jetstream.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
container=
peer_pid=
ready=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-ready.XXXXXX")
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-ocaml.XXXXXX")
rm -f "$ready"
prefix="ocaml.interop.jetstream.$$"
stream="OCAML_INTEROP_JS_$$"

if [ -n "${NATS_TEST_TLS_CA+x}" ] || [ -n "${NATS_TEST_TOKEN+x}" ] ||
  [ -n "${NATS_TEST_USER+x}" ] || [ -n "${NATS_TEST_PASS+x}" ]; then
  echo "JetStream interop currently supports anonymous plaintext connections only" >&2
  exit 1
fi

cleanup() {
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" >/dev/null 2>&1 || true
    wait "$peer_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -f "$ready" "$peer_log" "$ocaml_log"
}

trap cleanup EXIT INT TERM

container=$(docker run --detach --rm --publish 127.0.0.1::4222 "$image" -js)
port=
attempt=0
while [ "$attempt" -lt 30 ]; do
  port=$(docker port "$container" 4222/tcp 2>/dev/null | sed -n 's/.*://p' | head -n 1) || port=
  if [ -n "$port" ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ -z "$port" ]; then
  echo "could not determine the published NATS port" >&2
  exit 1
fi

attempt=0
while [ "$attempt" -lt 20 ]; do
  if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if ! docker logs "$container" 2>&1 | grep -q "Server is ready"; then
  echo "NATS JetStream server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

server="nats://127.0.0.1:$port"
NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
  NATS_TEST_INTEROP_STREAM="$stream" \
  nix develop .#integration -c nats-ocaml-interop-peer \
  --mode jetstream --server "$server" --prefix "$prefix" --stream "$stream" \
  --ready-file "$ready" >"$peer_log" 2>&1 &
peer_pid=$!

attempt=0
while [ ! -e "$ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Go JetStream interop peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$ready" ]; then
  echo "Go JetStream interop peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

status=0
if NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" nix develop .#integration -c dune exec \
    test/interop/interop_jetstream_acceptance.exe >"$ocaml_log" 2>&1
then
  :
else
  status=$?
fi

if wait "$peer_pid"; then
  peer_status=0
else
  peer_status=$?
fi
if [ "$status" -eq 0 ] && [ "$peer_status" -ne 0 ]; then
  status=$peer_status
fi

if [ "$status" -eq 0 ]; then
  cat "$ocaml_log"
  cat "$peer_log"
else
  cat "$ocaml_log" >&2 || true
  cat "$peer_log" >&2 || true
fi
trap - EXIT
cleanup
exit "$status"
