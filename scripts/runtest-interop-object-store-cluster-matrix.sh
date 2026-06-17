#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
default_images=$("$script_dir/default-server-images.sh")
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-interop-object-store-cluster-matrix.sh" "$@"
fi

images=${NATS_SERVER_IMAGES:-$default_images}
modes=${NATS_INTEROP_OBJECT_STORE_CLUSTER_MODES:-node-a,node-b,node-c,leader,restart,multi-node}

case "$modes" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_OBJECT_STORE_CLUSTER_MODES must contain node-a, node-b, node-c, leader, restart, and/or multi-node (seed is an alias for node-a) with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $modes
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_OBJECT_STORE_CLUSTER_MODES must contain at least one mode" >&2
  exit 1
fi

mode_list=
for mode do
  case "$mode" in
    seed|node-a|node-b|node-c|leader|restart|multi-node)
      ;;
    *)
      echo "unknown Object Store cluster interop mode: $mode (expected node-a, node-b, node-c, leader, restart, or multi-node)" >&2
      exit 1
      ;;
  esac
  mode_list="$mode_list${mode_list:+ }$mode"
done

old_ifs=$IFS
IFS=', '
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
      ./scripts/runtest-interop-object-store-cluster.sh
  )
}

status=0
case_number=0
echo "Object Store cluster interop matrix images: $*"
echo "Object Store cluster interop matrix modes: $mode_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Object Store cluster interop matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "Object Store cluster interop matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # mode_list contains only the validated mode names above.
  # shellcheck disable=SC2086
  for mode in $mode_list; do
    case_number=$((case_number + 1))
    echo "Object Store cluster interop matrix case $case_number: $image ($mode)"
    if run_case "$image" "$mode"; then
      :
    else
      status=1
      echo "Object Store cluster interop matrix case $case_number failed: $image ($mode)" >&2
    fi
  done
done

exit "$status"
