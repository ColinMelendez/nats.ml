#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

runner_timeout=${NATS_TEST_RUN_TIMEOUT:-300}
case "$runner_timeout" in
  ""|*[!0-9]*)
    echo "NATS_TEST_RUN_TIMEOUT must be a positive number of seconds" >&2
    exit 1
    ;;
esac
if [ "$runner_timeout" -lt 1 ]; then
  echo "NATS_TEST_RUN_TIMEOUT must be a positive number of seconds" >&2
  exit 1
fi

cluster_scenario=${NATS_TEST_JS_CLUSTER_SCENARIO:-ordered}
case "$cluster_scenario" in
  ordered)
    acceptance_executable=test/interop/interop_jetstream_ordered_reconnect_acceptance.exe
    prefix="ocaml.interop.jetstream.cluster.$$"
    stream="OCAML_INTEROP_JS_CLUSTER_$$"
    bucket=
    ;;
  kv)
    acceptance_executable=test/interop/interop_key_value_ordered_reconnect_acceptance.exe
    prefix="ocaml.interop.key-value.cluster.$$"
    bucket="OCAML_INTEROP_KV_$$"
    stream="KV_$bucket"
    ;;
  object)
    acceptance_executable=test/interop/interop_object_store_reconnect_acceptance.exe
    prefix="ocaml.interop.object-store.cluster.$$"
    bucket="OCAML_INTEROP_OBJ_$$"
    stream="OBJ_$bucket"
    ;;
  *)
    echo "NATS_TEST_JS_CLUSTER_SCENARIO must be ordered, kv, or object" >&2
    exit 1
    ;;
esac

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c timeout --signal=TERM --kill-after=5s \
    "${runner_timeout}s" env NATS_INTEGRATION_SHELL=1 \
    "$script_dir/runtest-interop-jetstream-cluster.sh" "$@"
fi

integration_command() {
  if [ "${NATS_INTEGRATION_SHELL-}" = 1 ]; then
    "$@"
  else
    nix develop .#integration -c "$@"
  fi
}

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
tls_enabled=${NATS_TEST_TLS-0}
interop_auth_mode=${NATS_TEST_INTEROP_AUTH_MODE-}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
auth_mode=anonymous
cert_dir=
auth_dir=
if [ "${NATS_TEST_JS_INTEROP_DEBUG-}" = 1 ]; then
  server_debug_args="-DV"
else
  server_debug_args=
fi
run_id=$$
dune_build_dir=${NATS_TEST_DUNE_BUILD_DIR:-_build-interop-$run_id}
network="ocaml-nats-js-interop-cluster-$run_id"
cluster_name="ocaml-nats-js-interop-$run_id"
primary_name="$network-a"
secondary_name="$network-b"
tertiary_name="$network-c"
primary=
secondary=
tertiary=
cluster_base_port=
primary_data_volume=
secondary_data_volume=
tertiary_data_volume=
peer_pid=
watcher=
resolver=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster.XXXXXX")
peer_ready="$signal.peer"
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster-ocaml.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-js-interop-cluster-docker.XXXXXX")
rm -f "$signal"
failure_mode=${NATS_TEST_JS_CLUSTER_FAILURE_MODE:-seed}
volume_suffix=${signal##*/}
leader_file="$signal.leader"
survivor_file="$signal.survivor"
survivor_tmp="$survivor_file.tmp"
killed_file="$signal.killed"
kill_ready_file="$signal.kill-ready"
go_reconnected_file="$signal.go-reconnected"
ocaml_reconnected_file="$signal.ocaml-reconnected"

# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/test-artifacts.sh"
artifact_init "interop-$cluster_scenario-cluster" "$run_id"

case "$failure_mode" in
  seed|node-a|node-b|node-c|leader|restart) ;;
  *)
    echo "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, node-a, node-b, node-c, leader, or restart" >&2
    exit 1
    ;;
esac

if [ "$failure_mode" = restart ]; then
  primary_data_volume="$primary_name-data-$volume_suffix"
  secondary_data_volume="$secondary_name-data-$volume_suffix"
  tertiary_data_volume="$tertiary_name-data-$volume_suffix"
fi

case "$tls_enabled" in
  0|1) ;;
  *)
    echo "NATS_TEST_TLS must be 0 or 1" >&2
    exit 1
    ;;
esac

if [ -n "$interop_auth_mode" ]; then
  case "$interop_auth_mode" in
    nkey|jwt|mtls)
      if [ -n "${NATS_TEST_TOKEN+x}" ] || [ -n "${NATS_TEST_USER+x}" ] ||
        [ -n "${NATS_TEST_PASS+x}" ]; then
        echo "NATS_TEST_INTEROP_AUTH_MODE cannot be combined with token or username/password credentials" >&2
        exit 1
      fi
      auth_mode=$interop_auth_mode
      ;;
    *)
      echo "NATS_TEST_INTEROP_AUTH_MODE must be nkey, jwt, or mtls" >&2
      exit 1
      ;;
  esac
elif [ -n "${NATS_TEST_TOKEN+x}" ]; then
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
  auth_mode=token
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
  auth_mode=user_pass
fi

if [ "$auth_mode" = mtls ]; then
  tls_enabled=1
fi

if ! integration_command timeout --signal=TERM --kill-after=5s \
    "${runner_timeout}s" dune build \
    --build-dir "$dune_build_dir" \
    "$acceptance_executable"
then
  echo "$cluster_scenario cluster acceptance executable did not build" >&2
  exit 1
fi

remove_container() {
  container=$1
  name=$2
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ "$container" != "$name" ]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
  fi
}

remove_cluster() {
  remove_container "$tertiary" "$tertiary_name"
  remove_container "$secondary" "$secondary_name"
  remove_container "$primary" "$primary_name"
  tertiary=
  secondary=
  primary=
}

cleanup() {
  status=$?
  if [ "$#" -eq 1 ]; then
    status=$1
  fi
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
  artifact_save_file "$status" "$peer_log" go-peer.log
  artifact_save_file "$status" "$ocaml_log" ocaml.log
  artifact_save_file "$status" "$docker_error" docker-launcher.log
  artifact_save_file "$status" "$peer_ready" peer-ready
  artifact_save_docker_log "$status" "$primary" cluster-a.log
  artifact_save_docker_log "$status" "$secondary" cluster-b.log
  artifact_save_docker_log "$status" "$tertiary" cluster-c.log
  artifact_save_docker_state "$status" "$primary" cluster-a.state
  artifact_save_docker_state "$status" "$secondary" cluster-b.state
  artifact_save_docker_state "$status" "$tertiary" cluster-c.state
  artifact_save_image "$status" "$image" nats-server.image
  artifact_save_text "$status" run.txt \
    "runner=interop-jetstream-cluster" "scenario=$cluster_scenario" \
    "image=$image" \
    "failure_mode=$failure_mode" "tls=$tls_enabled" "auth_mode=$auth_mode" \
    "cluster_port=$cluster_base_port" "build_dir=$dune_build_dir" \
    "status=$status"
  remove_cluster
  if [ -n "$primary_data_volume" ]; then
    docker volume rm "$primary_data_volume" >/dev/null 2>&1 || true
  fi
  if [ -n "$secondary_data_volume" ]; then
    docker volume rm "$secondary_data_volume" >/dev/null 2>&1 || true
  fi
  if [ -n "$tertiary_data_volume" ]; then
    docker volume rm "$tertiary_data_volume" >/dev/null 2>&1 || true
  fi
  docker network rm "$network" >/dev/null 2>&1 || true
  if [ -n "$cert_dir" ]; then
    rm -rf "$cert_dir"
  fi
  if [ -n "$auth_dir" ]; then
    rm -rf "$auth_dir"
  fi
  rm -f "$signal" "$signal.1" "$signal.failed" "$leader_file" \
    "$survivor_file" "$survivor_tmp" "$killed_file" "$kill_ready_file" \
    "$go_reconnected_file" "$ocaml_reconnected_file" "$peer_ready" \
    "$peer_log" "$ocaml_log" "$docker_error"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/interop-auth-material.sh"

if [ "$auth_mode" = nkey ] || [ "$auth_mode" = jwt ]; then
  prepare_nkey_material
else
  unset NATS_TEST_NKEY_PUBLIC NATS_TEST_NKEY_SEED_FILE
  unset NATS_TEST_USER_JWT_FILE NATS_TEST_USER_SEED_FILE
fi

if [ "$tls_enabled" -eq 1 ]; then
  prepare_tls_material
else
  unset NATS_TEST_TLS_CA NATS_TEST_TLS_CERT NATS_TEST_TLS_KEY
fi

if [ "$auth_mode" = jwt ] && [ "$tls_enabled" -eq 1 ]; then
  cp "$auth_dir/nats.conf" "$auth_dir/nats-tls.conf"
  cat >>"$auth_dir/nats-tls.conf" <<'EOF'
port: 4222

tls {
  cert_file: "/etc/nats/certs/server.pem"
  key_file: "/etc/nats/certs/server-key.pem"
}
EOF
fi

cluster_base_port=${NATS_TEST_JS_INTEROP_CLUSTER_BASE_PORT:-}
if [ -z "$cluster_base_port" ]; then
  cluster_base_port=$((16222 + (run_id % 1000) * 3))
fi
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

client_host=127.0.0.1

endpoint_for_port() {
  # NATS servers configured with TLS required advertise a plaintext INFO and
  # upgrade the connection during the protocol handshake. Keep the nats
  # scheme even for TLS cases so both clients exercise that negotiation.
  printf 'nats://%s:%s' "$client_host" "$1"
}

host_port_for_port() {
  printf '%s:%s' "$client_host" "$1"
}

run_server() {
  node_name=$1
  node_label=$2
  client_port=$3
  routes=$4
  data_option="--tmpfs /data"
  if [ "$failure_mode" = restart ]; then
    data_volume="$node_name-data-$volume_suffix"
    data_option="--volume $data_volume:/data"
  fi
  config_file=
  docker_options=
  server_options=
  case "$auth_mode" in
    anonymous)
      if [ "$tls_enabled" -eq 1 ]; then
        config_file="$script_dir/nats-server-tls.conf"
      fi
      ;;
    token)
      docker_options="--env NATS_TEST_TOKEN"
      if [ "$tls_enabled" -eq 1 ]; then
        config_file="$script_dir/nats-server-token-tls.conf"
      else
        server_options="--auth $auth_token"
      fi
      ;;
    user_pass)
      config_file="$script_dir/nats-server-auth.conf"
      if [ "$tls_enabled" -eq 1 ]; then
        config_file="$script_dir/nats-server-auth-tls.conf"
      fi
      docker_options="--env NATS_TEST_USER --env NATS_TEST_PASS"
      ;;
    nkey)
      if [ "$tls_enabled" -eq 1 ]; then
        config_file="$script_dir/nats-server-nkey-tls.conf"
      else
        config_file="$script_dir/nats-server-nkey.conf"
      fi
      docker_options="--env NATS_TEST_NKEY_PUBLIC"
      ;;
    jwt)
      if [ "$tls_enabled" -eq 1 ]; then
        config_file="$auth_dir/nats-tls.conf"
      else
        config_file="$auth_dir/nats.conf"
      fi
      ;;
    mtls)
      config_file="$script_dir/nats-server-mtls.conf"
      ;;
  esac
  if [ -n "$config_file" ]; then
    docker_options="$docker_options --volume $config_file:/etc/nats/nats.conf:ro"
    server_options="$server_options -c /etc/nats/nats.conf"
  fi
  if [ -n "$cert_dir" ]; then
    docker_options="$docker_options --volume $cert_dir:/etc/nats/certs:ro"
  fi
  # shellcheck disable=SC2086 # validated auth and fixed path options expand into words.
  docker run --detach --name "$node_name" --network "$network" \
    --hostname "$node_name" $data_option $docker_options \
    --publish "127.0.0.1:$client_port:4222" "$image" $server_debug_args \
    $server_options -js -sd /data -p 4222 -n "$node_label" \
    -cluster nats://0.0.0.0:6222 \
    -cluster_advertise "$node_name:6222" -routes "$routes" \
    -cluster_name "$cluster_name" \
    -client_advertise "$client_host:$client_port" 2>>"$docker_error"
}

retryable_launch_error() {
  grep -Eiq 'port (is )?already allocated|address already in use|failed to bind|bind .* failed' \
    "$docker_error"
}

docker network create "$network" > /dev/null 2>>"$docker_error"
if [ "$failure_mode" = restart ]; then
  {
    docker volume create "$primary_data_volume"
    docker volume create "$secondary_data_volume"
    docker volume create "$tertiary_data_volume"
  } >>"$docker_error" 2>&1
fi

start_cluster() {
  launch_attempt=0
  while [ "$launch_attempt" -lt 30 ]; do
    primary=
    secondary=
    tertiary=
    secondary_port=$((cluster_base_port + 1))
    tertiary_port=$((cluster_base_port + 2))
    if [ "$tertiary_port" -gt 65535 ]; then
      echo "could not allocate three local NATS cluster ports" >&2
      return 1
    fi
    : >"$docker_error"
    if primary=$(run_server "$primary_name" cluster-a "$cluster_base_port" \
      "nats://$secondary_name:6222,nats://$tertiary_name:6222"); then
      :
    else
      remove_cluster
      if retryable_launch_error; then
        cluster_base_port=$((cluster_base_port + 3))
        launch_attempt=$((launch_attempt + 1))
        continue
      fi
      return 1
    fi
    if secondary=$(run_server "$secondary_name" cluster-b "$secondary_port" \
      "nats://$primary_name:6222"); then
      :
    else
      remove_cluster
      if retryable_launch_error; then
        cluster_base_port=$((cluster_base_port + 3))
        launch_attempt=$((launch_attempt + 1))
        continue
      fi
      return 1
    fi
    if tertiary=$(run_server "$tertiary_name" cluster-c "$tertiary_port" \
      "nats://$primary_name:6222,nats://$secondary_name:6222"); then
      return 0
    else
      remove_cluster
      if retryable_launch_error; then
        cluster_base_port=$((cluster_base_port + 3))
        launch_attempt=$((launch_attempt + 1))
        continue
      fi
      return 1
    fi
  done
  echo "could not allocate three local NATS cluster ports" >&2
  return 1
}

start_cluster

wait_until_ready() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 45 ]; do
    if ! state=$(docker inspect --format '{{.State.Running}}' "$container" \
      2>/dev/null); then
      attempt=$((attempt + 1))
      sleep 1
      continue
    fi
    if [ "$state" != true ]; then
      echo "NATS JetStream server $container stopped before becoming ready" >&2
      docker logs "$container" >&2 || true
      return 1
    fi
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

wait_until_ready_count() {
  container=$1
  minimum=$2
  attempt=0
  while [ "$attempt" -lt 45 ]; do
    if ! state=$(docker inspect --format '{{.State.Running}}' "$container" \
      2>/dev/null); then
      attempt=$((attempt + 1))
      sleep 1
      continue
    fi
    if [ "$state" != true ]; then
      echo "NATS JetStream server $container stopped while restarting" >&2
      docker logs "$container" >&2 || true
      return 1
    fi
    ready_count=$(docker logs "$container" 2>&1 \
      | grep -c "Server is ready" || true)
    if [ "$ready_count" -ge "$minimum" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS JetStream server $container did not become ready $minimum time(s)" >&2
  docker logs "$container" >&2 || true
  return 1
}

wait_for_barrier() {
  path=$1
  label=$2
  attempt=0
  while [ "$attempt" -lt 900 ]; do
    if [ -e "$signal.failed" ]; then
      echo "$label failed (see $signal.failed)" >&2
      return 1
    fi
    if [ -e "$path" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 0.1
  done
  echo "timed out waiting for $label" >&2
  return 1
}

wait_for_file() {
  path=$1
  label=$2
  attempt=0
  while [ "$attempt" -lt "$runner_timeout" ]; do
    if [ -e "$signal.failed" ]; then
      echo "$label failed (see $signal.failed)" >&2
      return 1
    fi
    if [ -e "$path" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "timed out waiting for $label" >&2
  return 1
}

wait_for_nonempty_file() {
  path=$1
  label=$2
  attempt=0
  while [ "$attempt" -lt "$runner_timeout" ]; do
    if [ -e "$signal.failed" ]; then
      echo "$label failed (see $signal.failed)" >&2
      return 1
    fi
    if [ -s "$path" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "timed out waiting for $label" >&2
  return 1
}

wait_for_routes() {
  container=$1
  attempt=0
  while [ "$attempt" -lt 45 ]; do
    if ! state=$(docker inspect --format '{{.State.Running}}' "$container" \
      2>/dev/null); then
      attempt=$((attempt + 1))
      sleep 1
      continue
    fi
    if [ "$state" != true ]; then
      echo "NATS JetStream server $container stopped while forming routes" >&2
      docker logs "$container" >&2 || true
      return 1
    fi
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
    if ! wait_for_nonempty_file "$leader_file" "JetStream leader report"; then
      exit 1
    fi
    leader=$(sed -n '1p' "$leader_file" 2>/dev/null || true)
    case "$leader" in
      cluster-a)
        survivor="$(endpoint_for_port "$secondary_port"),$(endpoint_for_port "$tertiary_port")"
        ;;
      cluster-b)
        survivor="$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$tertiary_port")"
        ;;
      cluster-c)
        survivor="$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$secondary_port")"
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
  if ! wait_for_file "$signal.1" "cluster failure trigger"; then
    exit 1
  fi

  # Let both clients finish processing the baseline barrier before removing
  # the selected server. Authentication and TLS handshakes can otherwise make
  # the kill race the final control response rather than exercise recovery.
  sleep 2

  if [ "$failure_mode" = leader ]; then
    if ! wait_for_file "$kill_ready_file" "leader failure trigger"; then
      exit 1
    fi
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
    case "$failure_mode" in
      seed|node-a|restart) target=$primary ;;
      node-b) target=$secondary ;;
      node-c) target=$tertiary ;;
      *)
        touch "$signal.failed"
        exit 1
        ;;
    esac
  fi

  if ! docker kill "$target" >/dev/null 2>&1; then
    touch "$signal.failed"
    exit 1
  fi
  if [ "$failure_mode" = restart ]; then
    if ! wait_for_barrier "$go_reconnected_file" "Go reconnect barrier" ||
      ! wait_for_barrier "$ocaml_reconnected_file" "OCaml reconnect barrier"; then
      touch "$signal.failed"
      exit 1
    fi
    if ! docker start "$target" >/dev/null 2>&1; then
      touch "$signal.failed"
      exit 1
    fi
    if ! wait_until_ready_count "$target" 2; then
      touch "$signal.failed"
      exit 1
    fi
  fi
  if [ "$failure_mode" = leader ]; then
    touch "$killed_file"
  fi
) &
watcher=$!

if [ "$failure_mode" = leader ]; then
  if [ "$cluster_scenario" = kv ]; then
    peer_mode=jetstream-kv-leader-failover
  elif [ "$cluster_scenario" = object ]; then
    peer_mode=jetstream-object-leader-failover
  else
    peer_mode=jetstream-ordered-leader-failover
  fi
  peer_server="$(endpoint_for_port "$secondary_port"),$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$tertiary_port")"
elif [ "$failure_mode" = restart ]; then
  if [ "$cluster_scenario" = kv ]; then
    peer_mode=jetstream-kv-restart
  elif [ "$cluster_scenario" = object ]; then
    peer_mode=jetstream-object-restart
  else
    peer_mode=jetstream-ordered-restart
  fi
  peer_server="$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$secondary_port"),$(endpoint_for_port "$tertiary_port")"
else
  if [ "$cluster_scenario" = kv ]; then
    peer_mode=jetstream-kv-reconnect
  elif [ "$cluster_scenario" = object ]; then
    peer_mode=jetstream-object-reconnect
  else
    peer_mode=jetstream-ordered-reconnect
  fi
  case "$failure_mode" in
    node-b)
      peer_server="$(endpoint_for_port "$secondary_port"),$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$tertiary_port")"
      ;;
    node-c)
      peer_server="$(endpoint_for_port "$tertiary_port"),$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$secondary_port")"
      ;;
    seed|node-a)
      peer_server="$(endpoint_for_port "$cluster_base_port"),$(endpoint_for_port "$secondary_port"),$(endpoint_for_port "$tertiary_port")"
      ;;
    *)
      echo "unknown non-leader JetStream cluster failure mode: $failure_mode" >&2
      exit 1
      ;;
  esac
fi

if [ "$failure_mode" = leader ]; then
  timeout --signal=TERM --kill-after=5s "${runner_timeout}s" env \
    NATS_TEST_SERVER="$(endpoint_for_port "$cluster_base_port")" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_BUCKET="$bucket" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    nats-ocaml-interop-peer \
    --mode "$peer_mode" \
    --server "$peer_server" \
    --prefix "$prefix" --stream "$stream" --bucket "$bucket" \
    --ready-file "$peer_ready" \
    --signal-file "$signal" --leader-file "$leader_file" \
    --survivor-file "$survivor_file" >"$peer_log" 2>&1 &
else
  timeout --signal=TERM --kill-after=5s "${runner_timeout}s" env \
    NATS_TEST_SERVER="$(endpoint_for_port "$cluster_base_port")" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_BUCKET="$bucket" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    nats-ocaml-interop-peer \
    --mode "$peer_mode" \
    --server "$peer_server" \
    --prefix "$prefix" --stream "$stream" --bucket "$bucket" \
    --ready-file "$peer_ready" \
    --signal-file "$signal" >"$peer_log" 2>&1 &
fi
peer_pid=$!

attempt=0
while [ ! -e "$peer_ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  # Nix may need to realize the Go peer on a fresh machine.
  if [ "$attempt" -ge "$runner_timeout" ]; then
    echo "Go $cluster_scenario cluster peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$peer_ready" ]; then
  echo "Go $cluster_scenario cluster peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi
if [ "$failure_mode" = leader ] && [ ! -s "$leader_file" ]; then
  echo "Go $cluster_scenario leader failover peer did not report a leader" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi
if [ "$failure_mode" = leader ] && [ ! -e "$survivor_file" ]; then
  echo "Go $cluster_scenario leader failover peer did not resolve survivors" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

ocaml_server="$(endpoint_for_port "$cluster_base_port")"
ocaml_initial_name=cluster-a
ocaml_recovered_names=cluster-b,cluster-c
ocaml_discovered="$(host_port_for_port "$secondary_port"),$(host_port_for_port "$tertiary_port")"
if [ "$failure_mode" = leader ]; then
  leader=$(sed -n '1p' "$leader_file")
  case "$leader" in
    cluster-a)
      ocaml_server="$(endpoint_for_port "$secondary_port")"
      ocaml_initial_name=cluster-b
      ocaml_recovered_names=cluster-a,cluster-c
      ocaml_discovered="$(host_port_for_port "$cluster_base_port"),$(host_port_for_port "$tertiary_port")"
      ;;
    cluster-b|cluster-c) ;;
    *)
      echo "Go JetStream ordered leader failover peer reported unknown leader $leader" >&2
      cat "$peer_log" >&2 || true
      exit 1
      ;;
  esac
else
  case "$failure_mode" in
    node-b)
      ocaml_server="$(endpoint_for_port "$secondary_port")"
      ocaml_initial_name=cluster-b
      ocaml_recovered_names=cluster-a,cluster-c
      ocaml_discovered="$(host_port_for_port "$cluster_base_port"),$(host_port_for_port "$tertiary_port")"
      ;;
    node-c)
      ocaml_server="$(endpoint_for_port "$tertiary_port")"
      ocaml_initial_name=cluster-c
      ocaml_recovered_names=cluster-a,cluster-b
      ocaml_discovered="$(host_port_for_port "$cluster_base_port"),$(host_port_for_port "$secondary_port")"
      ;;
    seed|node-a) ;;
    *)
      echo "unknown non-leader JetStream cluster failure mode: $failure_mode" >&2
      exit 1
      ;;
  esac
fi

status=0
if integration_command timeout --signal=TERM --kill-after=5s \
    "${runner_timeout}s" env \
    NATS_TEST_SERVER="$ocaml_server" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_BUCKET="$bucket" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    NATS_TEST_JS_CLUSTER_FAILURE_MODE="$failure_mode" \
    NATS_TEST_JS_CLUSTER_INITIAL_NAME="$ocaml_initial_name" \
    NATS_TEST_JS_CLUSTER_RECOVERED_NAMES="$ocaml_recovered_names" \
    NATS_TEST_JS_CLUSTER_DISCOVERED="$ocaml_discovered" \
    dune exec \
    --build-dir "$dune_build_dir" \
    "$acceptance_executable" \
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
cleanup "$status"
exit "$status"
