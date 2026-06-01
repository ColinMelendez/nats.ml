#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-cluster.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
run_id=$$
network="ocaml-nats-cluster-$run_id"
cluster_name="ocaml-nats-integration-$run_id"
primary_name="$network-a"
secondary_name="$network-b"
tertiary_name="$network-c"
primary=
secondary=
tertiary=
watcher=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster.XXXXXX")
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster-log.XXXXXX")
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
  if [ -n "$tertiary" ]; then
    docker rm -f "$tertiary" >/dev/null 2>&1 || true
  fi
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -f "$signal" "$signal.1" "$signal.2.cluster-b" \
    "$signal.2.cluster-c" "$log"
}

trap cleanup EXIT INT TERM

cluster_base_port=${NATS_TEST_CLUSTER_BASE_PORT:-14222}
case "$cluster_base_port" in
  ""|*[!0-9]*)
    echo "NATS_TEST_CLUSTER_BASE_PORT must be a decimal port" >&2
    exit 1
    ;;
esac
if [ "$cluster_base_port" -lt 1 ] || [ "$cluster_base_port" -gt 65533 ]; then
  echo "NATS_TEST_CLUSTER_BASE_PORT must leave room for three ports" >&2
  exit 1
fi
secondary_port=$((cluster_base_port + 1))
tertiary_port=$((cluster_base_port + 2))

docker network create "$network" >/dev/null

primary=$(docker run --detach --name "$primary_name" --network "$network" \
  --hostname "$primary_name" \
  --publish "127.0.0.1:$cluster_base_port:4222" "$image" \
  -p 4222 -n cluster-a -cluster nats://0.0.0.0:6222 \
  -cluster_advertise "$primary_name:6222" -cluster_name "$cluster_name" \
  -client_advertise "127.0.0.1:$cluster_base_port")
secondary=$(docker run --detach --name "$secondary_name" --network "$network" \
  --hostname "$secondary_name" \
  --publish "127.0.0.1:$secondary_port:4222" "$image" \
  -p 4222 -n cluster-b -cluster nats://0.0.0.0:6222 \
  -cluster_advertise "$secondary_name:6222" \
  -routes "nats://$primary_name:6222" -cluster_name "$cluster_name" \
  -client_advertise "127.0.0.1:$secondary_port")
tertiary=$(docker run --detach --name "$tertiary_name" --network "$network" \
  --hostname "$tertiary_name" \
  --publish "127.0.0.1:$tertiary_port:4222" "$image" \
  -p 4222 -n cluster-c -cluster nats://0.0.0.0:6222 \
  -cluster_advertise "$tertiary_name:6222" \
  -routes "nats://$primary_name:6222" -cluster_name "$cluster_name" \
  -client_advertise "127.0.0.1:$tertiary_port")

wait_until_ready() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 30 ]; do
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

wait_for_routes() {
  container=$1
  minimum=${2:-1}
  attempt=0
  while [ "$attempt" -lt 30 ]; do
    route_count=$(docker logs "$container" 2>&1 \
      | grep -c "Route connection created" || true)
    if [ "$route_count" -ge "$minimum" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS cluster did not form the required route mesh" >&2
  docker logs "$container" >&2 || true
  return 1
}

wait_until_ready "$primary"
wait_until_ready "$secondary"
wait_until_ready "$tertiary"
wait_for_routes "$primary" 2
wait_for_routes "$secondary"
wait_for_routes "$tertiary"
sleep 1

(
  while [ ! -e "$signal.1" ]; do
    sleep 1
  done
  docker kill "$primary" >/dev/null 2>&1 || true
  while [ ! -e "$signal.2.cluster-b" ] &&
    [ ! -e "$signal.2.cluster-c" ]; do
    sleep 1
  done
  if [ -e "$signal.2.cluster-b" ]; then
    docker kill "$secondary" >/dev/null 2>&1 || true
  else
    docker kill "$tertiary" >/dev/null 2>&1 || true
  fi
) &
watcher=$!

status=0
if NATS_TEST_CLUSTER_SERVER="nats://127.0.0.1:$cluster_base_port" \
    NATS_TEST_CLUSTER_SIGNAL="$signal" \
    NATS_TEST_CLUSTER_INITIAL_SERVERS=cluster-a \
    NATS_TEST_CLUSTER_RECOVERED_SERVERS=cluster-b,cluster-c \
    NATS_TEST_CLUSTER_DISCOVERED="127.0.0.1:$secondary_port,127.0.0.1:$tertiary_port" \
    nix develop .#integration -c dune exec test/server/server_cluster.exe \
    >"$log" 2>&1
then
  cat "$log"
else
  status=$?
  cat "$log" >&2
  docker logs "$primary" >&2 || true
  docker logs "$secondary" >&2 || true
  docker logs "$tertiary" >&2 || true
fi
exit "$status"
