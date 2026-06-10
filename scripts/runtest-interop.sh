#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
tls_enabled=${NATS_TEST_TLS-0}
interop_mode=${NATS_TEST_INTEROP_MODE:-core}
interop_auth_mode=${NATS_TEST_INTEROP_AUTH_MODE-}
negative_mode=${NATS_TEST_INTEROP_AUTH_NEGATIVE-}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
auth_mode=anonymous
container=
peer_pid=
cert_dir=
auth_dir=
ready=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-ready.XXXXXX")
peer_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-peer.XXXXXX")
ocaml_log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-ocaml.XXXXXX")
parent_close_file=
if [ "$interop_mode" = service-parent-close ]; then
  parent_close_file=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-interop-parent-close.XXXXXX")
  rm -f "$parent_close_file"
fi
rm -f "$ready"
prefix="ocaml.interop.$$"
# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/test-artifacts.sh"
artifact_init interop "$$"

case "$interop_mode" in
  core)
    acceptance_executable=test/interop/interop_acceptance.exe
    peer_mode=core
    ;;
  service)
    acceptance_executable=test/interop/interop_service_acceptance.exe
    peer_mode=service
    ;;
  service-failure)
    acceptance_executable=test/interop/interop_service_failure_acceptance.exe
    peer_mode=service-failure
    ;;
  service-parent-close)
    acceptance_executable=test/interop/interop_service_parent_close_acceptance.exe
    peer_mode=service-parent-close
    ;;
  *)
    echo "NATS_TEST_INTEROP_MODE must be core, service, service-failure, or service-parent-close" >&2
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

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT trap.
cleanup() {
  status=$?
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" >/dev/null 2>&1 || true
    wait "$peer_pid" >/dev/null 2>&1 || true
  fi
  artifact_save_file "$status" "$peer_log" go-peer.log
  artifact_save_file "$status" "$ocaml_log" ocaml.log
  artifact_save_docker_log "$status" "$container" nats-server.log
  artifact_save_docker_state "$status" "$container" nats-server.state
  artifact_save_image "$status" "$image" nats-server.image
  artifact_save_text "$status" run.txt \
    "runner=interop" "mode=$interop_mode" "image=$image" "tls=$tls_enabled" \
    "auth_mode=$auth_mode" "negative=$negative_mode" "status=$status"
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ -n "$cert_dir" ]; then
    rm -rf "$cert_dir"
  fi
  if [ -n "$auth_dir" ]; then
    rm -rf "$auth_dir"
  fi
  if [ -n "$parent_close_file" ]; then
    rm -f "$parent_close_file"
  fi
  rm -f "$ready" "$peer_log" "$ocaml_log"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ -n "$interop_auth_mode" ]; then
  case "$interop_auth_mode" in
    nkey|jwt|mtls)
      if [ -n "${NATS_TEST_TOKEN+x}" ] || [ -n "${NATS_TEST_USER+x}" ] || \
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

case "$negative_mode" in
  "")
    ;;
  credentials)
    case "$auth_mode" in
      nkey|jwt)
        ;;
      *)
        echo "credential negatives require nkey or jwt authentication" >&2
        exit 1
        ;;
    esac
    ;;
  certificate)
    if [ "$auth_mode" != mtls ]; then
      echo "certificate negatives require mtls authentication" >&2
      exit 1
    fi
    ;;
  *)
    echo "NATS_TEST_INTEROP_AUTH_NEGATIVE must be credentials or certificate" >&2
    exit 1
    ;;
esac

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
    docker run --detach --rm --publish 127.0.0.1::4222 \
      "$image" --auth "$auth_token"
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

container=$(run_server)

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
  echo "NATS server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

nix develop .#integration -c true

if [ "$tls_enabled" -eq 1 ]; then
  server="nats://localhost:$port"
else
  server="nats://127.0.0.1:$port"
fi

run_negative_go() {
  case "$negative_mode" in
    credentials)
      if [ "$auth_mode" = nkey ]; then
        NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
          NATS_TEST_NKEY_SEED_FILE="$auth_dir/bad.seed" \
          nix develop .#integration -c nats-ocaml-interop-peer \
          --server "$server" --prefix "$prefix" --ready-file "$ready" \
          --mode "$peer_mode" >"$peer_log" 2>&1
      else
        NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
          NATS_TEST_USER_SEED_FILE="$auth_dir/bad.seed" \
          nix develop .#integration -c nats-ocaml-interop-peer \
          --server "$server" --prefix "$prefix" --ready-file "$ready" \
          --mode "$peer_mode" >"$peer_log" 2>&1
      fi
      ;;
    certificate)
      (
        unset NATS_TEST_TLS_CERT NATS_TEST_TLS_KEY
        NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
          nix develop .#integration -c nats-ocaml-interop-peer \
          --server "$server" --prefix "$prefix" --ready-file "$ready" \
          --mode "$peer_mode" >"$peer_log" 2>&1
      )
      ;;
  esac
}

run_negative_ocaml() {
  case "$negative_mode" in
    credentials)
      if [ "$auth_mode" = nkey ]; then
        NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
          NATS_TEST_NKEY_SEED_FILE="$auth_dir/bad.seed" \
          nix develop .#integration -c dune exec "$acceptance_executable" \
          >"$ocaml_log" 2>&1
      else
        NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
          NATS_TEST_USER_SEED_FILE="$auth_dir/bad.seed" \
          nix develop .#integration -c dune exec "$acceptance_executable" \
          >"$ocaml_log" 2>&1
      fi
      ;;
    certificate)
      (
        unset NATS_TEST_TLS_CERT NATS_TEST_TLS_KEY
        NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
          nix develop .#integration -c dune exec "$acceptance_executable" \
          >"$ocaml_log" 2>&1
      )
      ;;
  esac
}

if [ -n "$negative_mode" ]; then
  status=0
  if run_negative_go; then
    echo "Go peer unexpectedly connected with invalid $negative_mode material" >&2
    status=1
  fi
  if run_negative_ocaml; then
    echo "OCaml client unexpectedly connected with invalid $negative_mode material" >&2
    status=1
  fi
  if [ "$negative_mode" = credentials ]; then
    if ! grep -q "Authorization Violation" "$peer_log"; then
      echo "Go peer did not report an authorization violation" >&2
      status=1
    fi
    if ! grep -q "connection disconnected" "$ocaml_log"; then
      echo "OCaml client did not report an authentication disconnect" >&2
      status=1
    fi
  else
    if ! grep -q "certificate required" "$peer_log"; then
      echo "Go peer did not report a missing client certificate" >&2
      status=1
    fi
    if ! grep -q "TLS error" "$ocaml_log"; then
      echo "OCaml client did not report a TLS failure" >&2
      status=1
    fi
  fi
  cat "$ocaml_log" "$peer_log" >&2 || true
  exit "$status"
fi

if [ "$interop_mode" = service-parent-close ]; then
  NATS_TEST_INTEROP_PARENT_CLOSE_FILE="$parent_close_file" \
    NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
    nix develop .#integration -c nats-ocaml-interop-peer \
    --server "$server" --prefix "$prefix" --ready-file "$ready" \
    --mode "$peer_mode" \
    >"$peer_log" 2>&1 &
else
  NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
    nix develop .#integration -c nats-ocaml-interop-peer \
    --server "$server" --prefix "$prefix" --ready-file "$ready" \
    --mode "$peer_mode" \
    >"$peer_log" 2>&1 &
fi
peer_pid=$!

attempt=0
while [ ! -e "$ready" ] && kill -0 "$peer_pid" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Go interop peer did not become ready" >&2
    cat "$peer_log" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ ! -e "$ready" ]; then
  echo "Go interop peer exited before becoming ready" >&2
  cat "$peer_log" >&2 || true
  exit 1
fi

status=0
if [ "$interop_mode" = service-parent-close ]; then
  if NATS_TEST_INTEROP_PARENT_CLOSE_FILE="$parent_close_file" \
      NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
      nix develop .#integration -c dune exec "$acceptance_executable" \
      >"$ocaml_log" 2>&1
  then
    :
  else
    status=$?
  fi
elif NATS_TEST_SERVER="$server" NATS_TEST_INTEROP_PREFIX="$prefix" \
    nix develop .#integration -c dune exec "$acceptance_executable" \
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

if [ "$status" -eq 0 ]; then
  cat "$ocaml_log"
  cat "$peer_log"
else
  cat "$ocaml_log" >&2 || true
  cat "$peer_log" >&2 || true
fi
exit "$status"
