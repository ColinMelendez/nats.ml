#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-jetstream-cluster-matrix.sh" "$@"
fi

images=${NATS_SERVER_IMAGES:-nats:2.10.22,nats:2.12.15,nats:2.14.5}
modes=${NATS_INTEROP_JETSTREAM_CLUSTER_MATRIX_MODES:-seed,leader,restart}

case "$modes" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_JETSTREAM_CLUSTER_MATRIX_MODES must contain seed, leader, and/or restart with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $modes
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_JETSTREAM_CLUSTER_MATRIX_MODES must contain at least one mode" >&2
  exit 1
fi

mode_list=
for mode do
  case "$mode" in
    seed|leader|restart)
      ;;
    *)
      echo "unknown JetStream cluster interop matrix mode: $mode (expected seed, leader, or restart)" >&2
      exit 1
      ;;
  esac
  mode_list="$mode_list${mode_list:+ }$mode"
done

old_ifs=$IFS
IFS=', 	'
# The script has no positional interface; use positional parameters only as a
# compact, POSIX-compatible way to split the comma-separated image list.
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
  (
    unset NATS_TEST_TOKEN NATS_TEST_USER NATS_TEST_PASS NATS_TEST_TLS_CA
    NATS_TEST_TLS=0 NATS_SERVER_IMAGE="$run_image" \
      NATS_TEST_JS_CLUSTER_FAILURE_MODE="$run_mode" \
      ./scripts/runtest-interop-jetstream-cluster.sh
  )
}

status=0
case_number=0
echo "JetStream cluster interop matrix images: $*"
echo "JetStream cluster interop matrix modes: $mode_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "JetStream cluster interop matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "JetStream cluster interop matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # mode_list contains only the validated mode names above.
  # shellcheck disable=SC2086
  for mode in $mode_list; do
    case_number=$((case_number + 1))
    echo "JetStream cluster interop matrix case $case_number: $image ($mode)"
    if run_case "$image" "$mode"; then
      :
    else
      status=1
      echo "JetStream cluster interop matrix case $case_number failed: $image ($mode)" >&2
    fi
  done
done

exit "$status"
