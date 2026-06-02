#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-jetstream-reconnect.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
container=
peer_pid=
watcher=
data_dir=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-data.XXXXXX")
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect.XXXXXX")
peer_ready="$signal.peer"
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-ocaml.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-docker.XXXXXX")
rm -f "$signal"
prefix="ocaml.interop.jetstream.reconnect.$$"
stream="OCAML_INTEROP_JS_RECONNECT_$$"

if [ -n "${NATS_TEST_TOKEN+x}" ] || [ -n "${NATS_TEST_USER+x}" ] ||
  [ -n "${NATS_TEST_PASS+x}" ] ||
  [ -n "${NATS_TEST_TLS_CA+x}" ]; then
  echo "JetStream reconnect interop currently supports anonymous plaintext only" >&2
  exit 1
fi
if [ -n "${NATS_TEST_TLS+x}" ] && [ "${NATS_TEST_TLS}" != 0 ]; then
  echo "JetStream reconnect interop currently supports anonymous plaintext only" >&2
  exit 1
fi

cleanup() {
  if [ -n "$watcher" ]; then
    kill "$watcher" >/dev/null 2>&1 || true
    wait "$watcher" >/dev/null 2>&1 || true
  fi
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" >/dev/null 2>&1 || true
    wait "$peer_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$data_dir"
  rm -f "$signal" "$signal.1" "$signal.failed" "$peer_ready" "$peer_log" \
    "$ocaml_log" "$docker_error"
}

trap cleanup EXIT INT TERM

port=$((16000 + ($$ % 1000)))
attempt=0
while [ "$attempt" -lt 30 ]; do
  candidate_name="ocaml-nats-interop-js-reconnect-$$-$attempt"
  if container=$(docker run --detach --name "$candidate_name" \
    --volume "$data_dir:/data" --publish "127.0.0.1:$port:4222" "$image" \
    -js -sd /data 2>"$docker_error"); then
    break
  fi
  docker rm -f "$candidate_name" >/dev/null 2>&1 || true
  container=
  attempt=$((attempt + 1))
  port=$((port + 1))
done
if [ -z "$container" ]; then
  echo "could not bind a local NATS port" >&2
  cat "$docker_error" >&2 || true
  exit 1
fi

wait_until_ready_count() {
  minimum=$1
  attempt=0
  while [ "$attempt" -lt 90 ]; do
    ready_count=$(docker logs "$container" 2>&1 \
      | grep -c "Server is ready" || true)
    if [ "$ready_count" -ge "$minimum" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS JetStream server did not become ready $minimum time(s)" >&2
  docker logs "$container" >&2 || true
  return 1
}

wait_until_ready_count 1

(
  while [ ! -e "$signal.1" ]; do
    sleep 1
  done
  if ! docker kill "$container" >/dev/null 2>&1; then
    touch "$signal.failed"
    exit 1
  fi
  if ! docker start "$container" >/dev/null 2>&1; then
    touch "$signal.failed"
    exit 1
  fi
  if ! wait_until_ready_count 2; then
    touch "$signal.failed"
    exit 1
  fi
) &
watcher=$!

NATS_TEST_SERVER="nats://127.0.0.1:$port" \
  NATS_TEST_INTEROP_PREFIX="$prefix" \
  NATS_TEST_INTEROP_STREAM="$stream" \
  NATS_TEST_INTEROP_SIGNAL="$signal" \
  nix develop .#integration -c nats-ocaml-interop-peer \
  --mode jetstream-push-reconnect --server "nats://127.0.0.1:$port" \
  --prefix "$prefix" --stream "$stream" --ready-file "$peer_ready" \
  --signal-file "$signal" >"$peer_log" 2>&1 &
peer_pid=$!

attempt=0
while [ ! -e "$peer_ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Go JetStream reconnect peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$peer_ready" ]; then
  echo "Go JetStream reconnect peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

status=0
if NATS_TEST_SERVER="nats://127.0.0.1:$port" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    nix develop .#integration -c dune exec \
    test/interop/interop_jetstream_push_reconnect_acceptance.exe \
    >"$ocaml_log" 2>&1
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

if [ "$status" -ne 0 ]; then
  kill "$watcher" >/dev/null 2>&1 || true
fi
if wait "$watcher"; then
  watcher_status=0
else
  watcher_status=$?
fi
if [ "$status" -eq 0 ] && [ "$peer_status" -eq 0 ] &&
  [ "$watcher_status" -ne 0 ]; then
  status=$watcher_status
fi

if [ "$status" -eq 0 ]; then
  cat "$ocaml_log"
  cat "$peer_log"
else
  cat "$ocaml_log" >&2 || true
  cat "$peer_log" >&2 || true
  docker logs "$container" >&2 || true
fi
trap - EXIT
cleanup
exit "$status"
