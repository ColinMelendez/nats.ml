#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
auth_user=${NATS_TEST_USER-}
auth_pass=${NATS_TEST_PASS-}
auth_token=${NATS_TEST_TOKEN-}
jetstream=${NATS_TEST_JETSTREAM-}
jetstream_run_id=${NATS_TEST_JETSTREAM_RUN_ID:-$$}
container=
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-server.XXXXXX")
# shellcheck disable=SC1091 # script_dir points at this file's directory.
. "$script_dir/test-artifacts.sh"
artifact_init server "$$"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT/INT/TERM trap.
cleanup() {
  status=$?
  artifact_save_file "$status" "$log" ocaml.log
  artifact_save_docker_log "$status" "$container" nats-server.log
  artifact_save_docker_state "$status" "$container" nats-server.state
  artifact_save_image "$status" "$image" nats-server.image
  artifact_save_text "$status" run.txt \
    "runner=server" "image=$image" "jetstream=$jetstream" \
    "status=$status"
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -f "$log"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

case "$jetstream" in
  ""|0) jetstream_arg= ;;
  1) jetstream_arg=-js ;;
  *)
    echo "NATS_TEST_JETSTREAM must be 1 when set" >&2
    exit 1
    ;;
esac

case "$jetstream_run_id" in
  ""|*[!A-Za-z0-9_-]*)
    echo "NATS_TEST_JETSTREAM_RUN_ID may use only ASCII letters, digits, underscores, or hyphens" >&2
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
  container=$(docker run --detach --rm --publish 127.0.0.1::4222 \
    "$image" --auth "$auth_token" $jetstream_arg)
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
  container=$(docker run --detach --rm \
    --env NATS_TEST_USER --env NATS_TEST_PASS \
    --volume "$script_dir/nats-server-auth.conf:/etc/nats/nats.conf:ro" \
    --publish 127.0.0.1::4222 "$image" --config /etc/nats/nats.conf $jetstream_arg)
else
  container=$(docker run --detach --rm --publish 127.0.0.1::4222 "$image" $jetstream_arg)
fi
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

server="nats://127.0.0.1:$port"
ready=0
attempt=0
while [ "$attempt" -lt 20 ]; do
  if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
    ready=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "NATS server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

status=0
if NATS_TEST_SERVER="$server" NATS_TEST_JETSTREAM="$jetstream" \
    NATS_TEST_JETSTREAM_RUN_ID="$jetstream_run_id" nix develop .#integration -c dune exec \
    test/server/server_acceptance.exe >"$log" 2>&1
then
  if NATS_TEST_SERVER="$server" nix develop .#integration -c dune exec \
      test/server/server_lifecycle.exe >>"$log" 2>&1
  then
    if NATS_TEST_SERVER="$server" \
        NATS_TEST_JETSTREAM_RUN_ID="$jetstream_run_id" nix develop .#integration \
        -c dune exec test/server/server_service.exe >>"$log" 2>&1
    then
      if NATS_TEST_SERVER="$server" \
          NATS_TEST_JETSTREAM_RUN_ID="$jetstream_run_id" nix develop .#integration \
          -c dune exec test/server/server_service_queue.exe >>"$log" 2>&1
      then
        if [ "$jetstream" = 1 ]; then
          if NATS_TEST_SERVER="$server" \
              NATS_TEST_JETSTREAM_RUN_ID="$jetstream_run_id" nix develop .#integration \
              -c dune exec test/server/server_jetstream_consumers.exe >>"$log" 2>&1
          then
            if NATS_TEST_SERVER="$server" \
                NATS_TEST_JETSTREAM_RUN_ID="$jetstream_run_id" nix develop .#integration \
                -c dune exec test/server/server_key_value.exe >>"$log" 2>&1
            then
              if NATS_TEST_SERVER="$server" \
                  NATS_TEST_JETSTREAM_RUN_ID="$jetstream_run_id" nix develop .#integration \
                  -c dune exec test/server/server_object_store.exe >>"$log" 2>&1
              then
                :
              else
                status=$?
              fi
            else
              status=$?
            fi
          else
            status=$?
          fi
        fi
      else
        status=$?
      fi
    else
      status=$?
    fi
  else
    status=$?
  fi
else
  status=$?
fi
if [ "$status" -eq 0 ]; then
  cat "$log"
else
  cat "$log" >&2
fi
exit "$status"
