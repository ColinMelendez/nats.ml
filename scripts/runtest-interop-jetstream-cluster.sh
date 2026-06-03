#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-jetstream-cluster.sh" "$@"
fi

integration_command() {
  if [ "${NATS_INTEGRATION_SHELL-}" = 1 ]; then
    "$@"
  else
    nix develop .#integration -c "$@"
  fi
}

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
dune_build_dir=${NATS_TEST_DUNE_BUILD_DIR:-_build-interop}
if [ "${NATS_TEST_JS_INTEROP_DEBUG-}" = 1 ]; then
  server_debug_args="-DV"
else
  server_debug_args=
fi
run_id=$$
network="ocaml-nats-js-interop-cluster-$run_id"
cluster_name="ocaml-nats-js-interop-$run_id"
primary_name="$network-a"
secondary_name="$network-b"
tertiary_name="$network-c"
primary=
secondary=
tertiary=
peer_pid=
watcher=
resolver=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster.XXXXXX")
peer_ready="$signal.peer"
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster-ocaml.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster-docker.XXXXXX")
rm -f "$signal"
prefix="ocaml.interop.jetstream.cluster.$$"
stream="OCAML_INTEROP_JS_CLUSTER_$$"
failure_mode=${NATS_TEST_JS_CLUSTER_FAILURE_MODE:-seed}
leader_file="$signal.leader"
survivor_file="$signal.survivor"
survivor_tmp="$survivor_file.tmp"
killed_file="$signal.killed"
kill_ready_file="$signal.kill-ready"

case "$failure_mode" in
  seed|leader) ;;
  *)
    echo "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed or leader" >&2
    exit 1
    ;;
esac

if [ -n "${NATS_TEST_TOKEN+x}" ] || [ -n "${NATS_TEST_USER+x}" ] ||
  [ -n "${NATS_TEST_PASS+x}" ] || [ -n "${NATS_TEST_TLS_CA+x}" ]; then
  echo "JetStream cluster interop currently supports anonymous plaintext only" >&2
  exit 1
fi
if [ -n "${NATS_TEST_TLS+x}" ] && [ "${NATS_TEST_TLS}" != 0 ]; then
  echo "JetStream cluster interop currently supports anonymous plaintext only" >&2
  exit 1
fi

if ! integration_command dune build \
    --build-dir "$dune_build_dir" \
    test/interop/interop_jetstream_ordered_reconnect_acceptance.exe
then
  echo "JetStream ordered reconnect acceptance executable did not build" >&2
  exit 1
fi

cleanup() {
  if [ -n "$resolver" ]; then
    kill "$resolver" >/dev/null 2>&1 || true
    wait "$resolver" >/dev/null 2>&1 || true
  fi
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
  if [ -n "$tertiary" ]; then
    docker rm -f "$tertiary" >/dev/null 2>&1 || true
  fi
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -f "$signal" "$signal.1" "$signal.failed" "$leader_file" \
    "$survivor_file" "$survivor_tmp" "$killed_file" "$kill_ready_file" \
    "$peer_ready" "$peer_log" "$ocaml_log" "$docker_error"
}

trap cleanup EXIT INT TERM

cluster_base_port=${NATS_TEST_JS_INTEROP_CLUSTER_BASE_PORT:-16222}
case "$cluster_base_port" in
  ""|*[!0-9]*)
    echo "NATS_TEST_JS_INTEROP_CLUSTER_BASE_PORT must be a decimal port" >&2
    exit 1
    ;;
esac
if [ "$cluster_base_port" -lt 1 ] || [ "$cluster_base_port" -gt 65533 ]; then
  echo "NATS_TEST_JS_INTEROP_CLUSTER_BASE_PORT must leave room for three ports" >&2
  exit 1
fi
secondary_port=$((cluster_base_port + 1))
tertiary_port=$((cluster_base_port + 2))

docker network create "$network" >/dev/null

# shellcheck disable=SC2086 # the debug setting is a fixed optional flag.
primary=$(docker run --detach --name "$primary_name" --network "$network" \
  --hostname "$primary_name" --tmpfs /data \
  --publish "127.0.0.1:$cluster_base_port:4222" "$image" $server_debug_args \
  -js -sd /data -p 4222 -n cluster-a -cluster nats://0.0.0.0:6222 \
  -cluster_advertise "$primary_name:6222" \
  -routes "nats://$secondary_name:6222,nats://$tertiary_name:6222" \
  -cluster_name "$cluster_name" \
  -client_advertise "127.0.0.1:$cluster_base_port" 2>"$docker_error")
secondary=$(docker run --detach --name "$secondary_name" --network "$network" \
  --hostname "$secondary_name" --tmpfs /data \
  --publish "127.0.0.1:$secondary_port:4222" "$image" $server_debug_args \
  -js -sd /data -p 4222 -n cluster-b -cluster nats://0.0.0.0:6222 \
  -cluster_advertise "$secondary_name:6222" \
  -routes "nats://$primary_name:6222,nats://$tertiary_name:6222" \
  -cluster_name "$cluster_name" \
  -client_advertise "127.0.0.1:$secondary_port" 2>"$docker_error")
tertiary=$(docker run --detach --name "$tertiary_name" --network "$network" \
  --hostname "$tertiary_name" --tmpfs /data \
  --publish "127.0.0.1:$tertiary_port:4222" "$image" $server_debug_args \
  -js -sd /data -p 4222 -n cluster-c -cluster nats://0.0.0.0:6222 \
  -cluster_advertise "$tertiary_name:6222" \
  -routes "nats://$primary_name:6222,nats://$secondary_name:6222" \
  -cluster_name "$cluster_name" \
  -client_advertise "127.0.0.1:$tertiary_port" 2>"$docker_error")

wait_until_ready() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 45 ]; do
    if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS JetStream server $container did not become ready" >&2
  docker logs "$container" >&2 || true
  return 1
}

wait_for_routes() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 45 ]; do
    route_count=$(docker logs "$container" 2>&1 \
      | grep -c "Route connection created" || true)
    if [ "$route_count" -ge 2 ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS JetStream interop cluster did not form the required route mesh" >&2
  docker logs "$container" >&2 || true
  return 1
}

wait_until_ready "$primary"
wait_until_ready "$secondary"
wait_until_ready "$tertiary"
wait_for_routes "$primary"
wait_for_routes "$secondary"
wait_for_routes "$tertiary"
# Route connections can precede JetStream meta placement; the Go peer also
# retries stream creation until the cluster can place all replicas.
sleep 2

if [ "$failure_mode" = leader ]; then
  (
    leader=
    while [ -z "$leader" ]; do
      if [ -e "$signal.failed" ]; then
        exit 1
      fi
      leader=$(sed -n '1p' "$leader_file" 2>/dev/null || true)
      if [ -z "$leader" ]; then
        sleep 1
      fi
    done
    case "$leader" in
      cluster-a)
        survivor="nats://127.0.0.1:$secondary_port,nats://127.0.0.1:$tertiary_port"
        ;;
      cluster-b)
        survivor="nats://127.0.0.1:$cluster_base_port,nats://127.0.0.1:$tertiary_port"
        ;;
      cluster-c)
        survivor="nats://127.0.0.1:$cluster_base_port,nats://127.0.0.1:$secondary_port"
        ;;
      *)
        touch "$signal.failed"
        exit 1
        ;;
    esac
    if ! printf '%s\n' "$survivor" >"$survivor_tmp" ||
      ! mv "$survivor_tmp" "$survivor_file"; then
      touch "$signal.failed"
      exit 1
    fi
  ) &
  resolver=$!
fi

(
  while [ ! -e "$signal.1" ]; do
    if [ -e "$signal.failed" ]; then
      exit 1
    fi
    sleep 1
  done

  if [ "$failure_mode" = leader ]; then
    while [ ! -e "$kill_ready_file" ]; do
      if [ -e "$signal.failed" ]; then
        exit 1
      fi
      sleep 1
    done
    leader=$(sed -n '1p' "$leader_file")
    case "$leader" in
      cluster-a) target=$primary ;;
      cluster-b) target=$secondary ;;
      cluster-c) target=$tertiary ;;
      *)
        touch "$signal.failed"
        exit 1
        ;;
    esac
  else
    target=$primary
  fi

  if ! docker kill "$target" >/dev/null 2>&1; then
    touch "$signal.failed"
    exit 1
  fi
  if [ "$failure_mode" = leader ]; then
    touch "$killed_file"
  fi
) &
watcher=$!

if [ "$failure_mode" = leader ]; then
  peer_mode=jetstream-ordered-leader-failover
  peer_server="nats://127.0.0.1:$secondary_port,nats://127.0.0.1:$cluster_base_port,nats://127.0.0.1:$tertiary_port"
else
  peer_mode=jetstream-ordered-reconnect
  peer_server="nats://127.0.0.1:$cluster_base_port,nats://127.0.0.1:$secondary_port,nats://127.0.0.1:$tertiary_port"
fi

if [ "$failure_mode" = leader ]; then
  integration_command env \
    NATS_TEST_SERVER="nats://127.0.0.1:$cluster_base_port" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    nats-ocaml-interop-peer \
    --mode "$peer_mode" \
    --server "$peer_server" \
    --prefix "$prefix" --stream "$stream" --ready-file "$peer_ready" \
    --signal-file "$signal" --leader-file "$leader_file" \
    --survivor-file "$survivor_file" >"$peer_log" 2>&1 &
else
  integration_command env \
    NATS_TEST_SERVER="nats://127.0.0.1:$cluster_base_port" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    nats-ocaml-interop-peer \
    --mode "$peer_mode" \
    --server "$peer_server" \
    --prefix "$prefix" --stream "$stream" --ready-file "$peer_ready" \
    --signal-file "$signal" >"$peer_log" 2>&1 &
fi
peer_pid=$!

attempt=0
while [ ! -e "$peer_ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  # Nix may need to realize the Go peer on a fresh machine.
  if [ "$attempt" -ge 180 ]; then
    echo "Go JetStream ordered reconnect peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$peer_ready" ]; then
  echo "Go JetStream ordered reconnect peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi
if [ "$failure_mode" = leader ] && [ ! -e "$leader_file" ]; then
  echo "Go JetStream ordered leader failover peer did not report a leader" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi
if [ "$failure_mode" = leader ] && [ ! -e "$survivor_file" ]; then
  echo "Go JetStream ordered leader failover peer did not resolve survivors" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

ocaml_server="nats://127.0.0.1:$cluster_base_port"
ocaml_initial_name=cluster-a
ocaml_recovered_names=cluster-b,cluster-c
ocaml_discovered="127.0.0.1:$secondary_port,127.0.0.1:$tertiary_port"
if [ "$failure_mode" = leader ]; then
  leader=$(sed -n '1p' "$leader_file")
  case "$leader" in
    cluster-a)
      ocaml_server="nats://127.0.0.1:$secondary_port"
      ocaml_initial_name=cluster-b
      ocaml_recovered_names=cluster-a,cluster-c
      ocaml_discovered="127.0.0.1:$cluster_base_port,127.0.0.1:$tertiary_port"
      ;;
    cluster-b|cluster-c) ;;
    *)
      echo "Go JetStream ordered leader failover peer reported unknown leader $leader" >&2
      cat "$peer_log" >&2 || true
      exit 1
      ;;
  esac
fi

status=0
if integration_command env \
    NATS_TEST_SERVER="$ocaml_server" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    NATS_TEST_JS_CLUSTER_FAILURE_MODE="$failure_mode" \
    NATS_TEST_JS_CLUSTER_INITIAL_NAME="$ocaml_initial_name" \
    NATS_TEST_JS_CLUSTER_RECOVERED_NAMES="$ocaml_recovered_names" \
    NATS_TEST_JS_CLUSTER_DISCOVERED="$ocaml_discovered" \
    dune exec \
    --build-dir "$dune_build_dir" \
    test/interop/interop_jetstream_ordered_reconnect_acceptance.exe \
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
  if [ -n "$resolver" ]; then
    kill "$resolver" >/dev/null 2>&1 || true
  fi
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
if [ "$failure_mode" = leader ]; then
  if wait "$resolver"; then
    resolver_status=0
  else
    resolver_status=$?
  fi
  if [ "$status" -eq 0 ] && [ "$resolver_status" -ne 0 ]; then
    status=$resolver_status
  fi
fi

if [ "$status" -eq 0 ]; then
  cat "$ocaml_log"
  cat "$peer_log"
else
  cat "$ocaml_log" >&2 || true
  cat "$peer_log" >&2 || true
  docker logs "$primary" >&2 || true
  docker logs "$secondary" >&2 || true
  docker logs "$tertiary" >&2 || true
fi
trap - EXIT
cleanup
exit "$status"
