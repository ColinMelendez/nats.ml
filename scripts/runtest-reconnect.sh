#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
primary=
secondary=
watcher=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-reconnect.XXXXXX")
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-reconnect-log.XXXXXX")
rm -f "$signal"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT/INT/TERM trap.
cleanup() {
  if [ -n "$watcher" ]; then
    kill "$watcher" >/dev/null 2>&1 || true
  fi
  if [ -n "$primary" ]; then
    docker rm -f "$primary" >/dev/null 2>&1 || true
  fi
  if [ -n "$secondary" ]; then
    docker rm -f "$secondary" >/dev/null 2>&1 || true
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
  echo "could not determine the published NATS port for $container" >&2
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
  echo "NATS server $container did not become ready" >&2
  docker logs "$container" >&2 || true
  return 1
}

primary=$(docker run --detach --publish 127.0.0.1::4222 "$image")
secondary=$(docker run --detach --publish 127.0.0.1::4222 "$image")
primary_port=$(wait_for_port "$primary")
secondary_port=$(wait_for_port "$secondary")
wait_until_ready "$primary"
wait_until_ready "$secondary"

(
  while [ ! -e "$signal" ]; do
    sleep 0.05
  done
  docker kill "$primary" >/dev/null 2>&1 || true
) &
watcher=$!

status=0
if NATS_TEST_SERVERS="nats://127.0.0.1:$primary_port,nats://127.0.0.1:$secondary_port" \
    NATS_TEST_RECONNECT_SIGNAL="$signal" nix develop .#integration -c dune exec \
    test/server/server_reconnect.exe >"$log" 2>&1
then
  cat "$log"
else
  status=$?
  cat "$log" >&2
  docker logs "$primary" >&2 || true
  docker logs "$secondary" >&2 || true
fi
exit "$status"
