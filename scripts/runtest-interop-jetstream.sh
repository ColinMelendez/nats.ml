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

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c timeout --preserve-status \
    --signal=TERM --kill-after=5s \
    "${runner_timeout}s" env NATS_INTEGRATION_SHELL=1 \
    "$script_dir/runtest-interop-jetstream.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
jetstream_mode=${NATS_TEST_INTEROP_JETSTREAM_MODE:-pull}
tls_enabled=${NATS_TEST_TLS-0}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
auth_mode=anonymous
container=
peer_pid=
cert_dir=
ready=
peer_log=
ocaml_log=
docker_error=
server=
container_image=
configured_image=$image
prefix="ocaml.interop.jetstream.$$"
stream="OCAML_INTEROP_JS_$$"
bucket="OCAML_INTEROP_KV_$$"

case "$jetstream_mode" in
  pull) peer_mode=jetstream ;;
  admin)
    peer_mode=jetstream-admin
    # Consumer reset support requires the JetStream API in NATS 2.14.
    if [ -z "${NATS_SERVER_IMAGE+x}" ]; then
      image=nats:2.14.6
    fi
    ;;
  push) peer_mode=jetstream-push ;;
  ordered) peer_mode=jetstream-ordered ;;
  kv)
    peer_mode=jetstream-kv
    # LimitMarkerTTL and per-message TTL support require the JetStream API
    # level provided by NATS 2.11. Keep the older default for the other modes,
    # while allowing callers to override the image explicitly.
    if [ -z "${NATS_SERVER_IMAGE+x}" ]; then
      image=nats:2.11.0
    fi
    ;;
  object) peer_mode=jetstream-object ;;
  *)
    echo "NATS_TEST_INTEROP_JETSTREAM_MODE must be pull, admin, push, ordered, kv, or object" >&2
    exit 1
    ;;
esac

# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/test-artifacts.sh"
artifact_init "interop-jetstream-$jetstream_mode" "$$"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT trap.
cleanup() {
  status=$?
  save_artifacts
  stop_peer
  save_artifacts
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ -n "$cert_dir" ]; then
    rm -rf "$cert_dir"
  fi
  for temporary in "$ready" "$peer_log" "$ocaml_log" "$docker_error"; do
    if [ -n "$temporary" ]; then
      rm -f "$temporary"
    fi
  done
}

# shellcheck disable=SC2329 # Invoked indirectly by cleanup.
save_artifacts() {
  if [ "$status" -eq 0 ]; then
    return 0
  fi
  if [ -n "$container" ]; then
    container_image=$(docker inspect --format '{{.Image}}' "$container" \
      2>/dev/null || true)
  fi
  artifact_save_file "$status" "$peer_log" go-peer.log
  artifact_save_file "$status" "$ocaml_log" ocaml.log
  artifact_save_file "$status" "$docker_error" docker-launcher.log
  artifact_save_docker_log "$status" "$container" nats-server.log
  artifact_save_docker_state "$status" "$container" nats-server.state
  artifact_save_image "$status" "${container_image:-$configured_image}" \
    nats-server.image
  artifact_save_text "$status" run.txt \
    "runner=interop-jetstream" "mode=$jetstream_mode" \
    "image=$configured_image" "container_image=${container_image-}" \
    "tls=$tls_enabled" "auth_mode=$auth_mode" \
    "server=${server-}" "status=$status" \
    "timeout_seconds=$runner_timeout"
  if [ -n "$docker_error" ]; then
    cat "$docker_error" >&2 || true
  fi
}

# shellcheck disable=SC2329 # Invoked indirectly by cleanup.
stop_peer() {
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" >/dev/null 2>&1 || true
    attempt=0
    while kill -0 "$peer_pid" >/dev/null 2>&1 && [ "$attempt" -lt 2 ]; do
      attempt=$((attempt + 1))
      sleep 1
    done
    if kill -0 "$peer_pid" >/dev/null 2>&1; then
      kill -KILL "$peer_pid" >/dev/null 2>&1 || true
    fi
    wait "$peer_pid" >/dev/null 2>&1 || true
    peer_pid=
  fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ready=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-ready.XXXXXX")
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-ocaml.XXXXXX")
docker_error=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-jetstream-docker.XXXXXX")
rm -f "$ready"

case "$tls_enabled" in
  0|1) ;;
  *)
    echo "NATS_TEST_TLS must be 0 or 1" >&2
    exit 1
    ;;
esac

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
else
  auth_mode=anonymous
fi

run_server() {
  if [ "$tls_enabled" -eq 1 ]; then
    case "$auth_mode" in
      anonymous)
        config_file="$script_dir/nats-server-tls.conf"
        auth_options=
        ;;
      token)
        config_file="$script_dir/nats-server-token-tls.conf"
        auth_options="--env NATS_TEST_TOKEN"
        ;;
      user_pass)
        config_file="$script_dir/nats-server-auth-tls.conf"
        auth_options="--env NATS_TEST_USER --env NATS_TEST_PASS"
        ;;
    esac
    # shellcheck disable=SC2086 # auth_options intentionally expands to option words.
    docker run --detach $auth_options \
      --volume "$cert_dir:/etc/nats/certs:ro" \
      --volume "$config_file:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf -js
  elif [ "$auth_mode" = token ]; then
    docker run --detach --publish 127.0.0.1::4222 \
      "$image" --auth "$auth_token" -js
  elif [ "$auth_mode" = user_pass ]; then
    docker run --detach \
      --env NATS_TEST_USER --env NATS_TEST_PASS \
      --volume "$script_dir/nats-server-auth.conf:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf -js
  else
    docker run --detach --publish 127.0.0.1::4222 "$image" -js
  fi
}

if [ "$tls_enabled" -eq 1 ]; then
  cert_dir=$(mktemp -d "$script_dir/.nats-interop-tls.XXXXXX")
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" \
    -subj "/CN=ocaml-nats-interop-test-ca" >/dev/null
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" \
    >/dev/null
  openssl x509 -req -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" \
    -CAcreateserial -out "$cert_dir/server.pem" -days 1 -sha256 \
    -copy_extensions copy >/dev/null
  export NATS_TEST_TLS_CA="$cert_dir/ca.pem"
else
  unset NATS_TEST_TLS_CA
fi

container=$(run_server 2>"$docker_error")
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

attempt=0
while [ "$attempt" -lt 20 ]; do
  if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if ! docker logs "$container" 2>&1 | grep -q "Server is ready"; then
  echo "NATS JetStream server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

if [ "$tls_enabled" -eq 1 ]; then
  server="nats://localhost:$port"
else
  server="nats://127.0.0.1:$port"
fi
NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
  NATS_TEST_INTEROP_STREAM="$stream" \
  NATS_TEST_INTEROP_BUCKET="$bucket" \
  nix develop .#integration -c nats-ocaml-interop-peer \
  --mode "$peer_mode" --server "$server" --prefix "$prefix" \
  --stream "$stream" --bucket "$bucket" \
  --ready-file "$ready" >"$peer_log" 2>&1 &
peer_pid=$!

attempt=0
while [ ! -e "$ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Go JetStream interop peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$ready" ]; then
  echo "Go JetStream interop peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

status=0
case "$jetstream_mode" in
  pull) acceptance_executable=test/interop/interop_jetstream_acceptance.exe ;;
  admin) acceptance_executable=test/interop/interop_jetstream_admin_acceptance.exe ;;
  push) acceptance_executable=test/interop/interop_jetstream_push_acceptance.exe ;;
  ordered) acceptance_executable=test/interop/interop_jetstream_ordered_acceptance.exe ;;
  kv) acceptance_executable=test/interop/interop_key_value_acceptance.exe ;;
  object) acceptance_executable=test/interop/interop_object_store_acceptance.exe ;;
esac
if NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
    NATS_TEST_INTEROP_STREAM="$stream" NATS_TEST_INTEROP_BUCKET="$bucket" \
    nix develop .#integration -c dune exec \
    "$acceptance_executable" >"$ocaml_log" 2>&1
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
peer_pid=
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
exit "$status"
