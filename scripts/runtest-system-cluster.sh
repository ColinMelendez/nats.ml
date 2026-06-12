#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-system-cluster.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.14.5}
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "system cluster requires cached image $image; refusing to pull it" >&2
  exit 1
fi

auth_mode=${NATS_TEST_SYSTEM_AUTH_MODE:-user-pass}
case "$auth_mode" in
  user-pass|user-pass-tls|nkey|nkey-tls|jwt|jwt-tls|mtls)
    ;;
  *)
    echo "NATS_TEST_SYSTEM_AUTH_MODE must be user-pass, user-pass-tls, nkey, nkey-tls, jwt, jwt-tls, or mtls" >&2
    exit 1
    ;;
esac

tls_enabled=0
case "$auth_mode" in
  user-pass-tls|nkey-tls|jwt-tls|mtls) tls_enabled=1 ;;
esac

config_source=
generated_config=0
case "$auth_mode" in
  user-pass|user-pass-tls) config_source="$script_dir/nats-server-system.conf" ;;
  nkey|nkey-tls) config_source="$script_dir/nats-server-system-nkey.conf" ;;
  jwt|jwt-tls) generated_config=1 ;;
  mtls) config_source="$script_dir/nats-server-system-mtls.conf" ;;
esac

run_id=$$
cluster_base_port=${NATS_TEST_SYSTEM_CLUSTER_BASE_PORT:-}
network="ocaml-nats-system-cluster-$run_id"
cluster_name="ocaml-nats-system-$run_id"
system_account=SYS
system_account_event=SYS
primary=
secondary=
tertiary=
watcher=
network_created=0
attempt_log=
auth_dir=
cert_dir=
launch_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system-launch.XXXXXX")
start_cidfile=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system-cid.XXXXXX")
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system-signal.XXXXXX")
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system-log.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system-docker.XXXXXX")
config_dir="$script_dir/.nats-system-config-$run_id"
# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/test-artifacts.sh"
artifact_init system-cluster "$run_id"

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
    "runner=system-cluster" "image=$image" \
    "cluster_port=$cluster_base_port" "auth_mode=$auth_mode" \
    "tls=$tls_enabled" "status=$status"
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
  rm -f "$signal" "$signal.failed" "$log" "$docker_error" \
    "$attempt_log" "$launch_error" "$start_cidfile"
  rm -rf "$config_dir"
  if [ -n "$cert_dir" ]; then
    rm -rf "$cert_dir"
  fi
  if [ -n "$auth_dir" ]; then
    rm -rf "$auth_dir"
  fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

rm -f "$signal" "$start_cidfile"
if ! mkdir "$config_dir"; then
  echo "could not create temporary system cluster configuration directory" >&2
  exit 1
fi

unset NATS_TEST_SYSTEM_ACCOUNT NATS_TEST_SYSTEM_NKEY_PUBLIC \
  NATS_TEST_SYSTEM_NKEY_SEED_FILE NATS_TEST_SYSTEM_USER_JWT_FILE \
  NATS_TEST_SYSTEM_USER_SEED_FILE NATS_TEST_TLS_CA NATS_TEST_TLS_CERT \
  NATS_TEST_TLS_KEY

# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/system-auth-material.sh"
if [ "$auth_mode" = nkey ] || [ "$auth_mode" = nkey-tls ] || \
  [ "$auth_mode" = jwt ] || [ "$auth_mode" = jwt-tls ]; then
  prepare_system_nkey_material
fi
if [ "$tls_enabled" -eq 1 ]; then
  prepare_system_tls_material
fi
if [ "$generated_config" -eq 1 ]; then
  config_source="$auth_dir/nats.conf"
fi
if [ "$auth_mode" = jwt ] || [ "$auth_mode" = jwt-tls ]; then
  system_account_event="$system_account"
fi

write_node_config() {
  name=$1
  target=$2
  if [ "$generated_config" -eq 1 ]; then
    {
      printf 'server_name: %s\n\n' "$name"
      cat "$config_source"
    } >"$target"
  else
    sed "s/server_name: system-account/server_name: $name/" \
      "$config_source" >"$target"
  fi
  if [ "$tls_enabled" -eq 1 ] && [ "$auth_mode" != mtls ]; then
    cat >>"$target" <<'EOF'

port: 4222

tls {
  cert_file: "/etc/nats/certs/server.pem"
  key_file: "/etc/nats/certs/server-key.pem"
}
EOF
  fi
}

write_node_config cluster-a "$config_dir/cluster-a.conf"
write_node_config cluster-b "$config_dir/cluster-b.conf"
write_node_config cluster-c "$config_dir/cluster-c.conf"

if [ -z "$cluster_base_port" ]; then
  cluster_base_port=$((15000 + ($$ % 1000) * 3))
fi
case "$cluster_base_port" in
  ""|*[!0-9]*)
    echo "NATS_TEST_SYSTEM_CLUSTER_BASE_PORT must be a decimal port" >&2
    exit 1
    ;;
esac
if [ "$cluster_base_port" -lt 1 ] || [ "$cluster_base_port" -gt 65533 ]; then
  echo "NATS_TEST_SYSTEM_CLUSTER_BASE_PORT must leave room for three ports" >&2
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

run_node() {
  node_name=$1
  server_name=$2
  host_port=$3
  config=$4
  route=$5
  set -- --detach --name "$node_name" \
    --cidfile "$start_cidfile" \
    --network "$network" --hostname "$node_name" \
    --volume "$config:/etc/nats/nats.conf:ro"
  if [ "$tls_enabled" -eq 1 ]; then
    set -- "$@" --volume "$cert_dir:/etc/nats/certs:ro"
  fi
  case "$auth_mode" in
    nkey|nkey-tls) set -- "$@" --env NATS_TEST_SYSTEM_NKEY_PUBLIC ;;
  esac
  set -- "$@" --publish "127.0.0.1:$host_port:4222"
  if [ -n "$route" ]; then
    docker run "$@" "$image" --config /etc/nats/nats.conf -p 4222 \
      -n "$server_name" -cluster nats://0.0.0.0:6222 \
      -cluster_advertise "$node_name:6222" -cluster_name "$cluster_name" \
      -routes "$route" -client_advertise "127.0.0.1:$host_port"
  else
    docker run "$@" "$image" --config /etc/nats/nats.conf -p 4222 \
      -n "$server_name" -cluster nats://0.0.0.0:6222 \
      -cluster_advertise "$node_name:6222" -cluster_name "$cluster_name" \
      -client_advertise "127.0.0.1:$host_port"
  fi
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
  if ! primary=$(run_node "$primary_name" cluster-a "$cluster_base_port" \
    "$config_dir/cluster-a.conf" "" 2>"$launch_error"); then
    cat "$launch_error" >>"$docker_error"
    cleanup_failed_start
    primary=
    if retryable_launch_error; then return 1; else return 2; fi
  fi
  rm -f "$start_cidfile"
  : >"$launch_error"
  if ! secondary=$(run_node "$secondary_name" cluster-b "$secondary_port" \
    "$config_dir/cluster-b.conf" "nats://$primary_name:6222" \
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
  if ! tertiary=$(run_node "$tertiary_name" cluster-c "$tertiary_port" \
    "$config_dir/cluster-c.conf" "nats://$primary_name:6222" \
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
  echo "could not allocate three local NATS system cluster ports" >&2
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
  echo "NATS system cluster did not form the required route mesh" >&2
  docker logs "$container" >&2 || true
  return 1
}

run_dune_exec() {
  target=$1
  attempt=0
  while [ "$attempt" -lt 120 ]; do
    attempt_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-system-dune.XXXXXX")
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
  while [ ! -e "$signal" ]; do
    sleep 1
  done
  if ! docker kill "$primary" >/dev/null 2>>"$docker_error"; then
    touch "$signal.failed"
    exit 1
  fi
) &
watcher=$!

client_host=127.0.0.1
if [ "$tls_enabled" -eq 1 ]; then
  client_host=localhost
fi

status=0
if NATS_TEST_SERVERS="nats://$client_host:$cluster_base_port,nats://$client_host:$secondary_port,nats://$client_host:$tertiary_port" \
    NATS_TEST_EXPECTED_SERVERS=3 \
    NATS_TEST_SYSTEM_AUTH_MODE="$auth_mode" \
    NATS_TEST_SYSTEM_ACCOUNT="$system_account" \
    NATS_TEST_SYSTEM_ACCOUNT_NAME="$system_account_event" \
    NATS_TEST_SYSTEM_USER=sys NATS_TEST_SYSTEM_PASS=sys \
    NATS_TEST_SYSTEM_SECONDARY_SERVER="nats://$client_host:$secondary_port" \
    NATS_TEST_SYSTEM_RECOVERED_SERVER="nats://$client_host:$tertiary_port" \
    NATS_TEST_SYSTEM_SIGNAL="$signal" \
    run_dune_exec test/server/server_system.exe \
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
