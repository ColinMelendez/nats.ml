#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-reconnect.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
tls_enabled=${NATS_TEST_TLS-0}
interop_mode=${NATS_TEST_INTEROP_MODE:-core}
interop_auth_mode=${NATS_TEST_INTEROP_AUTH_MODE-}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
auth_mode=anonymous
primary=
secondary=
tertiary=
watcher=
peer_pid=
cert_dir=
auth_dir=
signal=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-reconnect.XXXXXX")
peer_signal="$signal.peer"
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-reconnect-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-reconnect-ocaml.XXXXXX")
rm -f "$signal"
prefix="ocaml.interop.reconnect.$$"

case "$interop_mode" in
  core)
    acceptance_executable=test/interop/interop_reconnect_acceptance.exe
    peer_mode=reconnect
    ;;
  service)
    acceptance_executable=test/interop/interop_service_reconnect_acceptance.exe
    peer_mode=service-reconnect
    ;;
  *)
    echo "NATS_TEST_INTEROP_MODE must be core or service" >&2
    exit 1
    ;;
esac

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
  if [ -n "$tertiary" ]; then
    docker rm -f "$tertiary" >/dev/null 2>&1 || true
  fi
  if [ -n "$cert_dir" ]; then
    rm -rf "$cert_dir"
  fi
  if [ -n "$auth_dir" ]; then
    rm -rf "$auth_dir"
  fi
  rm -f "$signal" "$signal.1" "$signal.2" "$peer_signal" "$peer_log" "$ocaml_log"
}

trap 'cleanup' EXIT INT TERM

# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/interop-auth-material.sh"

if [ "$auth_mode" = nkey ] || [ "$auth_mode" = jwt ]; then
  prepare_nkey_material
else
  unset NATS_TEST_NKEY_PUBLIC NATS_TEST_NKEY_SEED_FILE
  unset NATS_TEST_USER_JWT_FILE NATS_TEST_USER_SEED_FILE
fi

if [ "$auth_mode" = mtls ]; then
  tls_enabled=1
fi

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
      nkey)
        config_file="$script_dir/nats-server-nkey-tls.conf"
        auth_options="--env NATS_TEST_NKEY_PUBLIC"
        ;;
      jwt)
        config_file="$auth_dir/nats-tls.conf"
        auth_options=
        ;;
      mtls)
        config_file="$script_dir/nats-server-mtls.conf"
        auth_options=
        ;;
    esac
    # shellcheck disable=SC2086 # auth_options intentionally expands to option words.
    docker run --detach --rm $auth_options \
      --volume "$cert_dir:/etc/nats/certs:ro" \
      --volume "$config_file:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf
  elif [ "$auth_mode" = token ]; then
    docker run --detach --rm --publish 127.0.0.1::4222 "$image" --auth "$auth_token"
  elif [ "$auth_mode" = user_pass ]; then
    docker run --detach --rm \
      --env NATS_TEST_USER --env NATS_TEST_PASS \
      --volume "$script_dir/nats-server-auth.conf:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf
  elif [ "$auth_mode" = nkey ]; then
    docker run --detach --rm \
      --env NATS_TEST_NKEY_PUBLIC \
      --volume "$script_dir/nats-server-nkey.conf:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf
  elif [ "$auth_mode" = jwt ]; then
    docker run --detach --rm \
      --volume "$auth_dir/nats.conf:/etc/nats/nats.conf:ro" \
      --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf
  else
    docker run --detach --rm --publish 127.0.0.1::4222 "$image"
  fi
}

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

primary=$(run_server)
secondary=$(run_server)
tertiary=$(run_server)
primary_port=$(wait_for_port "$primary")
secondary_port=$(wait_for_port "$secondary")
tertiary_port=$(wait_for_port "$tertiary")
wait_until_ready "$primary"
wait_until_ready "$secondary"
wait_until_ready "$tertiary"

if [ "$tls_enabled" -eq 1 ]; then
  servers="nats://localhost:$primary_port,nats://localhost:$secondary_port,nats://localhost:$tertiary_port"
else
  servers="nats://127.0.0.1:$primary_port,nats://127.0.0.1:$secondary_port,nats://127.0.0.1:$tertiary_port"
fi
(
  while [ ! -e "$signal.1" ]; do
    sleep 1
  done
  docker kill "$primary" >/dev/null 2>&1 || true
  while [ ! -e "$signal.2" ]; do
    sleep 1
  done
  docker kill "$secondary" >/dev/null 2>&1 || true
) &
watcher=$!

nix develop .#integration -c true
NATS_TEST_SERVER="$servers" NATS_TEST_SERVERS="$servers" \
  NATS_TEST_INTEROP_PREFIX="$prefix" \
  nix develop .#integration -c nats-ocaml-interop-peer \
  --mode "$peer_mode" --server "$servers" --prefix "$prefix" \
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
