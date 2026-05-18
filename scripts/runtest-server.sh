#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
container=
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-server.XXXXXX")

cleanup() {
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -f "$log"
}

trap cleanup EXIT INT TERM

container=$(docker run --detach --rm --publish 127.0.0.1::4222 "$image")
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

server="nats://127.0.0.1:$port"
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
  echo "NATS server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

if NATS_TEST_SERVER="$server" nix develop -c dune exec \
    test/server/server_acceptance.exe >"$log" 2>&1
then
  cat "$log"
else
  cat "$log" >&2
  exit 1
fi
