#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-auth-jetstream-cluster-matrix.sh" "$@"
fi

images=${NATS_SERVER_IMAGES:-nats:2.10.22,nats:2.12.15,nats:2.14.5}
modes=${NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_MATRIX_MODES:-nkey,nkey-tls,jwt,jwt-tls,mtls}
failure_modes=${NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_FAILURE_MODES:-seed,leader,restart}

case "$modes" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_MATRIX_MODES must contain supported modes with no empty entries" >&2
    exit 1
    ;;
esac
case "$failure_modes" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_FAILURE_MODES must contain seed, leader, and/or restart with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $modes
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_MATRIX_MODES must contain at least one mode" >&2
  exit 1
fi

mode_list=
for mode do
  case "$mode" in
    nkey|nkey-tls|jwt|jwt-tls|mtls)
      ;;
    *)
      echo "unknown authenticated JetStream cluster mode: $mode" >&2
      exit 1
      ;;
  esac
  mode_list="$mode_list${mode_list:+ }$mode"
done

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $failure_modes
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_AUTH_JETSTREAM_CLUSTER_FAILURE_MODES must contain at least one mode" >&2
  exit 1
fi

failure_mode_list=
for failure_mode do
  case "$failure_mode" in
    seed|leader|restart)
      ;;
    *)
      echo "unknown authenticated JetStream cluster failure mode: $failure_mode" >&2
      exit 1
      ;;
  esac
  failure_mode_list="$failure_mode_list${failure_mode_list:+ }$failure_mode"
done

old_ifs=$IFS
IFS=', 	'
# shellcheck disable=SC2086
set -- $images
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_SERVER_IMAGES must contain at least one image" >&2
  exit 1
fi

run_case() {
  run_image=$1
  run_mode=$2
  run_failure_mode=$3
  case "$run_mode" in
    nkey)
      tls=0
      auth=nkey
      ;;
    nkey-tls)
      tls=1
      auth=nkey
      ;;
    jwt)
      tls=0
      auth=jwt
      ;;
    jwt-tls)
      tls=1
      auth=jwt
      ;;
    mtls)
      tls=1
      auth=mtls
      ;;
  esac
  (
    unset NATS_TEST_TOKEN NATS_TEST_USER NATS_TEST_PASS NATS_TEST_TLS_CA
    unset NATS_TEST_TLS_CERT NATS_TEST_TLS_KEY NATS_TEST_NKEY_SEED_FILE
    unset NATS_TEST_NKEY_PUBLIC NATS_TEST_USER_JWT_FILE NATS_TEST_USER_SEED_FILE
    unset NATS_TEST_INTEROP_AUTH_NEGATIVE
    NATS_SERVER_IMAGE="$run_image" NATS_TEST_TLS="$tls" \
      NATS_TEST_INTEROP_AUTH_MODE="$auth" \
      NATS_TEST_JS_CLUSTER_FAILURE_MODE="$run_failure_mode" \
      ./scripts/runtest-interop-auth-jetstream-cluster.sh
  )
}

status=0
case_number=0
echo "authenticated JetStream cluster matrix images: $*"
echo "authenticated JetStream cluster matrix modes: $mode_list"
echo "authenticated JetStream cluster matrix failure modes: $failure_mode_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "authenticated JetStream cluster matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "authenticated JetStream cluster matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # shellcheck disable=SC2086
  for mode in $mode_list; do
    # shellcheck disable=SC2086
    for failure_mode in $failure_mode_list; do
      case_number=$((case_number + 1))
      echo "authenticated JetStream cluster matrix case $case_number: $image ($mode, $failure_mode)"
      if run_case "$image" "$mode" "$failure_mode"; then
        :
      else
        status=1
        echo "authenticated JetStream cluster matrix case $case_number failed: $image ($mode, $failure_mode)" >&2
      fi
    done
  done
done

exit "$status"
