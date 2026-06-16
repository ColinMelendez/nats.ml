#!/bin/sh
set -eu

profile=${NATS_TEST_COLIMA_PROFILE:-nats-tests}
cpus=${NATS_TEST_COLIMA_CPUS:-4}
memory=${NATS_TEST_COLIMA_MEMORY_GIB:-6}
disk=${NATS_TEST_COLIMA_DISK_GIB:-12}

case "$cpus" in
  ''|*[!0-9]*) echo "NATS_TEST_COLIMA_CPUS must be a positive integer" >&2; exit 2 ;;
esac
case "$memory" in
  ''|*[!0-9]*) echo "NATS_TEST_COLIMA_MEMORY_GIB must be a positive integer" >&2; exit 2 ;;
esac
case "$disk" in
  ''|*[!0-9]*) echo "NATS_TEST_COLIMA_DISK_GIB must be a positive integer" >&2; exit 2 ;;
esac
if [ "$cpus" -le 0 ] || [ "$memory" -le 0 ] || [ "$disk" -le 0 ]; then
  echo "NATS_TEST_COLIMA_CPUS, NATS_TEST_COLIMA_MEMORY_GIB, and NATS_TEST_COLIMA_DISK_GIB must be positive" >&2
  exit 2
fi
case "$profile" in
  ''|[-.]*|*[!A-Za-z0-9._-]*)
    echo "NATS_TEST_COLIMA_PROFILE must contain only letters, digits, '.', '_' or '-'" >&2
    exit 2
    ;;
esac

if [ "$#" -eq 0 ]; then
  echo "usage: $0 COMMAND [ARG ...]" >&2
  exit 2
fi
if [ -n "${DOCKER_HOST-}" ]; then
  echo "DOCKER_HOST must be unset so the selected Colima profile owns Docker access" >&2
  exit 2
fi

profile_context=colima
if [ "$profile" != default ]; then
  profile_context=colima-$profile
fi

lock_root=${TMPDIR:-/tmp}
lock_dir=$lock_root/ocaml-nats-colima-$profile.lock
lock_pid_file=$lock_dir/pid
started=0
status=0
lock_owned=0
previous_context=
context_known=0
child_pid=

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT trap.
cleanup() {
  status=$?
  trap '' HUP INT TERM

  if [ "$started" -eq 1 ]; then
    if ! colima stop "$profile"; then
      echo "failed to stop Colima profile $profile" >&2
      if [ "$status" -eq 0 ]; then
        status=1
      fi
    fi
  fi

  if [ "$context_known" -eq 1 ]; then
    current_context=$(env -u DOCKER_CONTEXT docker context show) || current_context=
    if [ -z "$current_context" ]; then
      echo "failed to inspect the active Docker context" >&2
      if [ "$status" -eq 0 ]; then
        status=1
      fi
    elif [ "$current_context" != "$previous_context" ]; then
      if ! env -u DOCKER_CONTEXT docker context use "$previous_context" >/dev/null; then
        echo "failed to restore Docker context $previous_context" >&2
        if [ "$status" -eq 0 ]; then
          status=1
        fi
      fi
    fi
  fi

  if [ "$lock_owned" -eq 1 ]; then
    if ! rm -f "$lock_pid_file" || ! rmdir "$lock_dir"; then
      echo "failed to release Colima profile lock $lock_dir" >&2
      if [ "$status" -eq 0 ]; then
        status=1
      fi
    fi
  fi

  exit "$status"
}

# shellcheck disable=SC2329 # Invoked indirectly by the signal traps.
forward_signal() {
  signal=$1
  signal_status=$2
  if [ -n "$child_pid" ]; then
    kill "-$signal" "$child_pid" 2>/dev/null || :
    wait "$child_pid" 2>/dev/null || :
  fi
  exit "$signal_status"
}

# shellcheck disable=SC2329 # Invoked indirectly by the HUP trap.
on_hup() { forward_signal HUP 129; }
# shellcheck disable=SC2329 # Invoked indirectly by the INT trap.
on_int() { forward_signal INT 130; }
# shellcheck disable=SC2329 # Invoked indirectly by the TERM trap.
on_term() { forward_signal TERM 143; }

install_signal_traps() {
  trap on_hup HUP
  trap on_int INT
  trap on_term TERM
}

trap cleanup EXIT
trap '' HUP INT TERM

lock_acquired=0
if mkdir "$lock_dir" 2>/dev/null; then
  lock_acquired=1
elif [ ! -d "$lock_dir" ]; then
  echo "could not create Colima profile lock $lock_dir" >&2
  exit 1
else
  lock_pid=
  if [ -r "$lock_pid_file" ]; then
    IFS= read -r lock_pid < "$lock_pid_file" || lock_pid=
  fi
  case "$lock_pid" in
    ''|*[!0-9]*)
      echo "Colima profile $profile has an unowned lock at $lock_dir; remove it only after confirming no test wrapper is running" >&2
      exit 2
      ;;
    *)
      if kill -0 "$lock_pid" 2>/dev/null; then
        echo "Colima profile $profile is already in use by process $lock_pid (lock: $lock_dir)" >&2
      else
        echo "Colima profile $profile has a possibly stale lock for process $lock_pid at $lock_dir; remove it only after confirming no test wrapper is running" >&2
      fi
      exit 2
      ;;
  esac
fi
if [ "$lock_acquired" -ne 1 ]; then
  echo "could not acquire Colima profile lock $lock_dir" >&2
  exit 1
fi
lock_owned=1
if ! printf '%s\n' "$$" > "$lock_pid_file"; then
  echo "could not record Colima profile lock owner $lock_dir" >&2
  exit 1
fi
install_signal_traps

if ! previous_context=$(env -u DOCKER_CONTEXT docker context show); then
  echo "could not determine the active Docker context" >&2
  exit 1
fi
context_known=1

profile_entry=$(colima list --json | grep -F "\"name\":\"$profile\"" || true)

case "$profile_entry" in
*'"status":"Running"'*)
  echo "using already-running Colima profile $profile" >&2
  ;;
*'"status":"Stopped"'*)
  echo "starting existing Colima profile $profile with its stored allocation" >&2
  started=1
  colima start "$profile" --activate=false
  ;;
'')
  echo "starting Colima profile $profile (${cpus} CPUs, ${memory} GiB RAM, ${disk} GiB disk)" >&2
  started=1
  colima start "$profile" --cpus "$cpus" --memory "$memory" --disk "$disk" --activate=false
  ;;
*)
  echo "could not determine the state of Colima profile $profile" >&2
  exit 1
  ;;
esac

export DOCKER_CONTEXT="$profile_context"
docker info >/dev/null
"$@" &
child_pid=$!
if wait "$child_pid"; then
  child_status=0
else
  child_status=$?
fi
child_pid=
exit "$child_status"
