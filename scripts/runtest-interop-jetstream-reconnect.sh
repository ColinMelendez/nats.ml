#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-jetstream-reconnect.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
tls_enabled=${NATS_TEST_TLS-0}
interop_auth_mode=${NATS_TEST_INTEROP_AUTH_MODE-}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
auth_mode=anonymous
container=
peer_pid=
watcher=
cert_dir=
auth_dir=
data_dir=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-data.XXXXXX")
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect.XXXXXX")
peer_ready="$signal.peer"
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-ocaml.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-js-reconnect-docker.XXXXXX")
rm -f "$signal"
prefix="ocaml.interop.jetstream.reconnect.$$"
stream="OCAML_INTEROP_JS_RECONNECT_$$"

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

cleanup() {
  if [ -n "$watcher" ]; then
    kill "$watcher" >/dev/null 2>&1 || true
    wait "$watcher" >/dev/null 2>&1 || true
  fi
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" >/dev/null 2>&1 || true
    wait "$peer_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$data_dir"
  if [ -n "$cert_dir" ]; then
    rm -rf "$cert_dir"
  fi
  if [ -n "$auth_dir" ]; then
    rm -rf "$auth_dir"
  fi
  rm -f "$signal" "$signal.1" "$signal.failed" "$peer_ready" "$peer_log" \
    "$ocaml_log" "$docker_error"
}

trap cleanup EXIT INT TERM

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

run_server() {
  config_source=
  docker_options=
  server_options=
  case "$auth_mode" in
    anonymous)
      if [ "$tls_enabled" -eq 1 ]; then
        config_source="$script_dir/nats-server-tls.conf"
      fi
      ;;
    token)
      docker_options="--env NATS_TEST_TOKEN"
      if [ "$tls_enabled" -eq 1 ]; then
        config_source="$script_dir/nats-server-token-tls.conf"
      else
        server_options="--auth $auth_token"
      fi
      ;;
    user_pass)
      config_source="$script_dir/nats-server-auth.conf"
      if [ "$tls_enabled" -eq 1 ]; then
        config_source="$script_dir/nats-server-auth-tls.conf"
      fi
      docker_options="--env NATS_TEST_USER --env NATS_TEST_PASS"
      ;;
    nkey)
      if [ "$tls_enabled" -eq 1 ]; then
        config_source="$script_dir/nats-server-nkey-tls.conf"
      else
        config_source="$script_dir/nats-server-nkey.conf"
      fi
      docker_options="--env NATS_TEST_NKEY_PUBLIC"
      ;;
    jwt)
      if [ "$tls_enabled" -eq 1 ]; then
        config_source="$auth_dir/nats-tls.conf"
      else
        config_source="$auth_dir/nats.conf"
      fi
      ;;
    mtls)
      config_source="$script_dir/nats-server-mtls.conf"
      ;;
  esac
  runtime_config="$data_dir/nats.conf"
  if [ -n "$config_source" ]; then
    cp "$config_source" "$runtime_config"
  else
    : >"$runtime_config"
  fi
  # This runner deliberately uses SIGKILL and then requires acknowledged
  # messages not to reappear. Make the server sync each file-store write so a
  # completed consumer-state flush is durable when the process is killed.
  printf '\njetstream {\n  store_dir: "/data"\n  sync_interval: always\n}\n' \
    >>"$runtime_config"
  server_options="$server_options -c /data/nats.conf"
  if [ -n "$cert_dir" ]; then
    docker_options="$docker_options --volume $cert_dir:/etc/nats/certs:ro"
  fi
  # shellcheck disable=SC2086 # validated auth and fixed path options expand into words.
  docker run --detach --name "$candidate_name" --volume "$data_dir:/data" \
    --publish "127.0.0.1:$port:4222" $docker_options "$image" \
    $server_options 2>"$docker_error"
}

port=$((16000 + ($$ % 1000)))
attempt=0
while [ "$attempt" -lt 30 ]; do
  candidate_name="ocaml-nats-interop-js-reconnect-$$-$attempt"
  if container=$(run_server); then
    break
  fi
  docker rm -f "$candidate_name" >/dev/null 2>&1 || true
  container=
  attempt=$((attempt + 1))
  port=$((port + 1))
done
if [ -z "$container" ]; then
  echo "could not bind a local NATS port" >&2
  cat "$docker_error" >&2 || true
  exit 1
fi

wait_until_ready_count() {
  minimum=$1
  attempt=0
  while [ "$attempt" -lt 90 ]; do
    ready_count=$(docker logs "$container" 2>&1 \
      | grep -c "Server is ready" || true)
    if [ "$ready_count" -ge "$minimum" ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "NATS JetStream server did not become ready $minimum time(s)" >&2
  docker logs "$container" >&2 || true
  return 1
}

wait_until_ready_count 1

if [ "$tls_enabled" -eq 1 ]; then
  server="nats://localhost:$port"
else
  server="nats://127.0.0.1:$port"
fi

(
  while [ ! -e "$signal.1" ]; do
    sleep 1
  done
  # JetStream batches consumer-state writes at approximately 10 updates per
  # second. AckSync and ConsumerInfo observe the in-memory state, so leave more
  # than one batch interval for the acknowledged state to reach the file store
  # before testing hard-restart recovery.
  sleep 1
  if ! docker kill "$container" >/dev/null 2>&1; then
    touch "$signal.failed"
    exit 1
  fi
  if ! docker start "$container" >/dev/null 2>&1; then
    touch "$signal.failed"
    exit 1
  fi
  if ! wait_until_ready_count 2; then
    touch "$signal.failed"
    exit 1
  fi
) &
watcher=$!

NATS_TEST_SERVER="$server" \
  NATS_TEST_INTEROP_PREFIX="$prefix" \
  NATS_TEST_INTEROP_STREAM="$stream" \
  NATS_TEST_INTEROP_SIGNAL="$signal" \
  nix develop .#integration -c nats-ocaml-interop-peer \
  --mode jetstream-push-reconnect --server "$server" \
  --prefix "$prefix" --stream "$stream" --ready-file "$peer_ready" \
  --signal-file "$signal" >"$peer_log" 2>&1 &
peer_pid=$!

attempt=0
while [ ! -e "$peer_ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Go JetStream reconnect peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$peer_ready" ]; then
  echo "Go JetStream reconnect peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

status=0
if NATS_TEST_SERVER="$server" \
    NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" \
    NATS_TEST_INTEROP_SIGNAL="$signal" \
    nix develop .#integration -c dune exec \
    test/interop/interop_jetstream_push_reconnect_acceptance.exe \
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

if [ "$status" -eq 0 ]; then
  cat "$ocaml_log"
  cat "$peer_log"
else
  cat "$ocaml_log" >&2 || true
  cat "$peer_log" >&2 || true
  docker logs "$container" >&2 || true
fi
trap - EXIT
cleanup
exit "$status"
