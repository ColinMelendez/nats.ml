#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

image=${NATS_SERVER_IMAGE:-nats:2.14.5}
container=
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system.XXXXXX")

cleanup() {
  status=$?
  if [ -n "$container" ]; then
    docker logs "$container" >"$log" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ "$status" -eq 0 ]; then
    cat "$log"
  else
    cat "$log" >&2
  fi
  rm -f "$log"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

container=$(docker run --detach --rm \
  --volume "$script_dir/nats-server-system.conf:/etc/nats/nats.conf:ro" \
  --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf)

port=
attempt=0
while [ "$attempt" -lt 30 ]; do
  port=$(docker port "$container" 4222/tcp 2>/dev/null | sed -n 's/.*://p' | head -n 1) || port=
  if [ -n "$port" ]; then break; fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ -z "$port" ]; then
  echo "could not determine the published NATS port" >&2
  exit 1
fi

ready=0
attempt=0
while [ "$attempt" -lt 20 ]; do
  if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
    ready=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "NATS system-account server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

NATS_TEST_SERVER="nats://127.0.0.1:$port" \
NATS_TEST_SYSTEM_USER=sys NATS_TEST_SYSTEM_PASS=sys \
nix develop .#integration -c dune exec test/server/server_system.exe
