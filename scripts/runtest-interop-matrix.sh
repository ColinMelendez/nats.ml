#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
default_images=$("$script_dir/default-server-images.sh")
cd "$script_dir/.."

images=${NATS_SERVER_IMAGES:-$default_images}
scenarios=${NATS_INTEROP_MATRIX_SCENARIOS:-core,reconnect,tls-core,tls-reconnect}

case "$scenarios" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_MATRIX_SCENARIOS must contain core, reconnect, tls-core, and/or tls-reconnect with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $scenarios
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_MATRIX_SCENARIOS must contain at least one scenario" >&2
  exit 1
fi

scenario_list=
for scenario do
  case "$scenario" in
    core|reconnect|tls-core|tls-reconnect)
      ;;
    *)
      echo "unknown interop matrix scenario: $scenario (expected core, reconnect, tls-core, or tls-reconnect)" >&2
      exit 1
      ;;
  esac
  scenario_list="$scenario_list${scenario_list:+ }$scenario"
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
  run_scenario=$2
  case "$run_scenario" in
    core)
      (
        unset NATS_TEST_TLS_CA
        NATS_TEST_TLS=0 NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop.sh
      )
      ;;
    tls-core)
      (
        unset NATS_TEST_TLS_CA
        NATS_TEST_TLS=1 NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop.sh
      )
      ;;
    reconnect)
      (
        unset NATS_TEST_TLS_CA
        NATS_TEST_TLS=0 NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop-reconnect.sh
      )
      ;;
    tls-reconnect)
      (
        unset NATS_TEST_TLS_CA
        NATS_TEST_TLS=1 NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop-reconnect.sh
      )
      ;;
    *)
      echo "unknown interop matrix scenario: $run_scenario" >&2
      return 2
      ;;
  esac
}

status=0
case_number=0
echo "interop matrix images: $*"
echo "interop matrix scenarios: $scenario_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "interop matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "interop matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # scenario_list contains only the validated scenario names above.
  # shellcheck disable=SC2086
  for scenario in $scenario_list; do
    case_number=$((case_number + 1))
    echo "interop matrix case $case_number: $image ($scenario)"
    if run_case "$image" "$scenario"; then
      :
    else
      status=1
      echo "interop matrix case $case_number failed: $image ($scenario)" >&2
    fi
  done
done

exit "$status"
