#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
primary=
secondary=
watcher=
peer_pid=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-reconnect.XXXXXX")
peer_signal="$signal.peer"
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-reconnect-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-reconnect-ocaml.XXXXXX")
rm -f "$signal"
prefix="ocaml.interop.reconnect.$$"

cleanup() {
  if [ -n "$watcher" ]; then
    kill "$watcher" >/dev/null 2>&1 || true
    wait "$watcher" >/dev/null 2>&1 || true
  fi
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" >/dev/null 2>&1 || true
    wait "$peer_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$primary" ]; then
    docker rm -f "$primary" >/dev/null 2>&1 || true
  fi
  if [ -n "$secondary" ]; then
    docker rm -f "$secondary" >/dev/null 2>&1 || true
  fi
  rm -f "$signal" "$peer_signal" "$peer_log" "$ocaml_log"
}

trap 'cleanup' EXIT INT TERM

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

run_server() {
  if [ -n "${NATS_TEST_TOKEN+x}" ]; then
    if [ -n "${NATS_TEST_USER+x}" ] || [ -n "${NATS_TEST_PASS+x}" ]; then
      echo "NATS_TEST_TOKEN cannot be combined with NATS_TEST_USER or NATS_TEST_PASS" >&2
      exit 1
    fi
    if [ -z "$auth_token" ]; then
      echo "NATS_TEST_TOKEN must be non-empty" >&2
      exit 1
    fi
    case "$auth_token" in
      *[!A-Za-z0-9_-]*)
        echo "NATS_TEST_TOKEN may use only ASCII letters, digits, underscores, or hyphens" >&2
        exit 1
        ;;
    esac
    docker run --detach --publish 127.0.0.1::4222 "$image" --auth "$auth_token"
  elif [ -n "${NATS_TEST_USER+x}" ] || [ -n "${NATS_TEST_PASS+x}" ]; then
    if [ -z "$auth_user" ] || [ -z "$auth_pass" ]; then
      echo "NATS_TEST_USER and NATS_TEST_PASS must both be non-empty" >&2
      exit 1
    fi
    case "$auth_user$auth_pass" in
      *[!A-Za-z0-9_-]*)
        echo "NATS_TEST_USER and NATS_TEST_PASS may use only ASCII letters, digits, underscores, or hyphens" >&2
        exit 1
        ;;
    esac
    docker run --detach \
      --env NATS_TEST_USER --env NATS_TEST_PASS \
      --volume "$script_dir/nats-server-auth.conf:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf
  else
    docker run --detach --publish 127.0.0.1::4222 "$image"
  fi
}

primary=$(run_server)
secondary=$(run_server)
primary_port=$(wait_for_port "$primary")
secondary_port=$(wait_for_port "$secondary")
wait_until_ready "$primary"
wait_until_ready "$secondary"

servers="nats://127.0.0.1:$primary_port,nats://127.0.0.1:$secondary_port"
(
  while [ ! -e "$signal" ]; do
    sleep 0.05
  done
  docker kill "$primary" >/dev/null 2>&1 || true
) &
watcher=$!

nix develop .#integration -c true
NATS_TEST_SERVER="$servers" NATS_TEST_SERVERS="$servers" \
  NATS_TEST_INTEROP_PREFIX="$prefix" \
  nix develop .#integration -c nats-ocaml-interop-peer \
  --mode reconnect --server "$servers" --prefix "$prefix" \
  --ready-file "$peer_signal" --signal-file "$signal" \
  >"$peer_log" 2>&1 &
peer_pid=$!

attempt=0
while [ ! -e "$peer_signal" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Go reconnect peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$peer_signal" ]; then
  echo "Go reconnect peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

status=0
if NATS_TEST_SERVER="$servers" NATS_TEST_SERVERS="$servers" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    nix develop .#integration -c dune exec \
    test/interop/interop_reconnect_acceptance.exe >"$ocaml_log" 2>&1
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
