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
cluster_base_port=
network="ocaml-nats-cluster-$run_id"
cluster_name="ocaml-nats-integration-$run_id"
primary=
secondary=
tertiary=
watcher=
network_created=0
attempt_log=
launch_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster-launch.XXXXXX")
start_cidfile=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster-cid.XXXXXX")
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster.XXXXXX")
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster-log.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-cluster-docker.XXXXXX")
rm -f "$signal" "$start_cidfile"
# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/test-artifacts.sh"
artifact_init cluster "$run_id"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT/INT/TERM trap.
cleanup() {
  status=$?
  if [ -n "$watcher" ]; then
    kill "$watcher" >/dev/null 2>&1 || true
    wait "$watcher" >/dev/null 2>&1 || true
  fi
  artifact_save_file "$status" "$log" ocaml.log
  artifact_save_file "$status" "$attempt_log" dune-exec.log
  artifact_save_file "$status" "$docker_error" docker-launcher.log
  artifact_save_docker_log "$status" "$primary" cluster-a.log
  artifact_save_docker_log "$status" "$secondary" cluster-b.log
  artifact_save_docker_log "$status" "$tertiary" cluster-c.log
  artifact_save_docker_state "$status" "$primary" cluster-a.state
  artifact_save_docker_state "$status" "$secondary" cluster-b.state
  artifact_save_docker_state "$status" "$tertiary" cluster-c.state
  artifact_save_image "$status" "$image" nats-server.image
  artifact_save_text "$status" run.txt \
    "runner=cluster" "image=$image" "cluster_port=$cluster_base_port" \
    "status=$status"
  if [ -n "$primary" ]; then
    docker rm -f "$primary" >/dev/null 2>&1 || true
  fi
  if [ -n "$secondary" ]; then
    docker rm -f "$secondary" >/dev/null 2>&1 || true
  fi
  if [ -n "$tertiary" ]; then
    docker rm -f "$tertiary" >/dev/null 2>&1 || true
  fi
  if [ "$network_created" -eq 1 ]; then
    docker network rm "$network" >/dev/null 2>&1 || true
  fi
  rm -f "$signal" "$signal.1" "$signal.2.cluster-b" \
    "$signal.2.cluster-c" "$signal.failed" "$log" "$docker_error" \
    "$attempt_log" "$launch_error" "$start_cidfile"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cluster_base_port=${NATS_TEST_CLUSTER_BASE_PORT:-}
if [ -z "$cluster_base_port" ]; then
  cluster_base_port=$((14000 + ($$ % 1000) * 3))
fi
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

if ! docker network create "$network" >"$docker_error" 2>&1; then
  cat "$docker_error" >&2 || true
  exit 1
fi
network_created=1

cleanup_failed_start() {
  failed_container=$(sed -n '1p' "$start_cidfile" 2>/dev/null || true)
  case "$failed_container" in
    ""|*[!0-9a-f]*) ;;
    *) docker rm -f "$failed_container" >/dev/null 2>&1 || true ;;
  esac
  rm -f "$start_cidfile"
}

retryable_launch_error() {
  grep -Eiq 'port (is )?already allocated|address already in use|failed to bind|bind .* failed' \
    "$launch_error"
}

start_cluster() {
  attempt=$1
  primary_name="$network-a-$attempt"
  secondary_name="$network-b-$attempt"
  tertiary_name="$network-c-$attempt"
  secondary_port=$((cluster_base_port + 1))
  tertiary_port=$((cluster_base_port + 2))
  if [ "$tertiary_port" -gt 65535 ]; then
    return 1
  fi
  primary=
  secondary=
  tertiary=
  : >"$launch_error"
  if ! primary=$(docker run --detach --name "$primary_name" \
    --cidfile "$start_cidfile" \
    --network "$network" --hostname "$primary_name" \
    --publish "127.0.0.1:$cluster_base_port:4222" "$image" \
    -p 4222 -n cluster-a -cluster nats://0.0.0.0:6222 \
    -cluster_advertise "$primary_name:6222" -cluster_name "$cluster_name" \
    -client_advertise "127.0.0.1:$cluster_base_port" \
    2>"$launch_error"); then
    cat "$launch_error" >>"$docker_error"
    cleanup_failed_start
    primary=
    if retryable_launch_error; then return 1; else return 2; fi
  fi
  rm -f "$start_cidfile"
  : >"$launch_error"
  if ! secondary=$(docker run --detach --name "$secondary_name" \
    --cidfile "$start_cidfile" \
    --network "$network" --hostname "$secondary_name" \
    --publish "127.0.0.1:$secondary_port:4222" "$image" \
    -p 4222 -n cluster-b -cluster nats://0.0.0.0:6222 \
    -cluster_advertise "$secondary_name:6222" \
    -routes "nats://$primary_name:6222" -cluster_name "$cluster_name" \
    -client_advertise "127.0.0.1:$secondary_port" \
    2>"$launch_error"); then
    cat "$launch_error" >>"$docker_error"
    cleanup_failed_start
    secondary=
    docker rm -f "$primary" >/dev/null 2>&1 || true
    primary=
    if retryable_launch_error; then return 1; else return 2; fi
  fi
  rm -f "$start_cidfile"
  : >"$launch_error"
  if ! tertiary=$(docker run --detach --name "$tertiary_name" \
    --cidfile "$start_cidfile" \
    --network "$network" --hostname "$tertiary_name" \
    --publish "127.0.0.1:$tertiary_port:4222" "$image" \
    -p 4222 -n cluster-c -cluster nats://0.0.0.0:6222 \
    -cluster_advertise "$tertiary_name:6222" \
    -routes "nats://$primary_name:6222" -cluster_name "$cluster_name" \
    -client_advertise "127.0.0.1:$tertiary_port" \
    2>"$launch_error"); then
    cat "$launch_error" >>"$docker_error"
    cleanup_failed_start
    tertiary=
    docker rm -f "$secondary" >/dev/null 2>&1 || true
    docker rm -f "$primary" >/dev/null 2>&1 || true
    secondary=
    primary=
    if retryable_launch_error; then return 1; else return 2; fi
  fi
  rm -f "$start_cidfile"
  return 0
}

attempt=0
started=0
while [ "$attempt" -lt 30 ]; do
  if start_cluster "$attempt"; then
    started=1
    break
  else
    launch_status=$?
    if [ "$launch_status" -ne 1 ]; then
      break
    fi
    cluster_base_port=$((cluster_base_port + 3))
    attempt=$((attempt + 1))
  fi
done
if [ "$started" -ne 1 ]; then
  echo "could not allocate three local NATS cluster ports" >&2
  cat "$docker_error" >&2 || true
  exit 1
fi

wait_until_ready() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 30 ]; do
    if ! state=$(docker inspect --format '{{.State.Running}}' "$container" \
      2>/dev/null); then
      attempt=$((attempt + 1))
      sleep 1
      continue
    fi
    if [ "$state" != true ]; then
      echo "NATS server $container stopped before becoming ready" >&2
      docker logs "$container" >&2 || true
      return 1
    fi
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
    if ! state=$(docker inspect --format '{{.State.Running}}' "$container" \
      2>/dev/null); then
      attempt=$((attempt + 1))
      sleep 1
      continue
    fi
    if [ "$state" != true ]; then
      echo "NATS server $container stopped while forming routes" >&2
      docker logs "$container" >&2 || true
      return 1
    fi
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

run_dune_exec() {
  target=$1
  attempt=0
  while [ "$attempt" -lt 120 ]; do
    attempt_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-dune-exec.XXXXXX")
    if nix develop .#integration -c dune exec "$target" >"$attempt_log" 2>&1;
    then
      cat "$attempt_log"
      rm -f "$attempt_log"
      attempt_log=
      return 0
    else
      result=$?
      if grep -q "Another Dune instance is currently running" "$attempt_log";
      then
        rm -f "$attempt_log"
        attempt_log=
        attempt=$((attempt + 1))
        sleep 1
      else
        cat "$attempt_log"
        rm -f "$attempt_log"
        attempt_log=
        return "$result"
      fi
    fi
  done
  echo "Dune remained busy while starting $target" >&2
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
  if ! docker kill "$primary" >/dev/null 2>>"$docker_error"; then
    touch "$signal.failed"
    exit 1
  fi
  while [ ! -e "$signal.2.cluster-b" ] &&
    [ ! -e "$signal.2.cluster-c" ]; do
    sleep 1
  done
  if [ -e "$signal.2.cluster-b" ]; then
    if ! docker kill "$secondary" >/dev/null 2>>"$docker_error"; then
      touch "$signal.failed"
      exit 1
    fi
  else
    if ! docker kill "$tertiary" >/dev/null 2>>"$docker_error"; then
      touch "$signal.failed"
      exit 1
    fi
  fi
) &
watcher=$!

status=0
if NATS_TEST_CLUSTER_SERVER="nats://127.0.0.1:$cluster_base_port" \
    NATS_TEST_CLUSTER_SIGNAL="$signal" \
    NATS_TEST_CLUSTER_INITIAL_SERVERS=cluster-a \
    NATS_TEST_CLUSTER_RECOVERED_SERVERS=cluster-b,cluster-c \
    NATS_TEST_CLUSTER_DISCOVERED="127.0.0.1:$secondary_port,127.0.0.1:$tertiary_port" \
    run_dune_exec test/server/server_cluster.exe \
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
if [ "$status" -ne 0 ]; then
  kill "$watcher" >/dev/null 2>&1 || true
fi
if wait "$watcher"; then
  watcher_status=0
else
  watcher_status=$?
fi
if [ "$status" -eq 0 ] && [ "$watcher_status" -ne 0 ]; then
  status=$watcher_status
fi
exit "$status"
