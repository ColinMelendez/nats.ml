#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-lameduck.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
container=
watcher=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-lameduck.XXXXXX")
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-lameduck-log.XXXXXX")
rm -f "$signal"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT/INT/TERM trap.
cleanup() {
  if [ -n "$watcher" ]; then
    kill "$watcher" >/dev/null 2>&1 || true
  fi
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -f "$signal" "$log"
}

trap cleanup EXIT INT TERM

wait_for_port() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 30 ]; do
    port=$(docker port "$container" 4222/tcp 2>/dev/null | sed -n 's/.*://p' | head -n 1) || port=
    if [ -n "$port" ]; then
      printf '%s\n' "$port"
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "could not determine the published NATS port" >&2
  return 1
}

wait_until_ready() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 20 ]; do
    if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS server did not become ready" >&2
  docker logs "$container" >&2 || true
  return 1
}

container=$(docker run --detach --rm --publish 127.0.0.1::4222 "$image")
port=$(wait_for_port "$container")
wait_until_ready "$container"

(
  while [ ! -e "$signal" ]; do
    sleep 0.05
  done
  docker kill --signal=SIGUSR2 "$container"
) &
watcher=$!

status=0
if NATS_TEST_SERVER="nats://127.0.0.1:$port" \
    NATS_TEST_LAMEDUCK_SIGNAL="$signal" nix develop .#integration -c dune exec \
    test/server/server_lameduck.exe >"$log" 2>&1
then
  cat "$log"
else
  status=$?
  cat "$log" >&2
  docker logs "$container" >&2 || true
fi
exit "$status"
