#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

images=${NATS_SERVER_IMAGES:-nats:2.10.22,nats:2.12.15,nats:2.14.5}
scenarios=${NATS_SERVER_MATRIX_SCENARIOS:-server,cluster,lameduck}

case "$scenarios" in
  ""|,*|*,|*,,*)
    echo "NATS_SERVER_MATRIX_SCENARIOS must contain server, cluster, and/or lameduck with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $scenarios
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_SERVER_MATRIX_SCENARIOS must contain at least one scenario" >&2
  exit 1
fi

scenario_list=
has_server=0
for scenario do
  case "$scenario" in
    server)
      has_server=1
      ;;
    cluster|lameduck)
      ;;
    *)
      echo "unknown server matrix scenario: $scenario (expected server, cluster, or lameduck)" >&2
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

if [ "$has_server" -eq 0 ] && {
  [ -n "${NATS_TEST_TOKEN+x}" ] ||
  [ -n "${NATS_TEST_USER+x}" ] ||
  [ -n "${NATS_TEST_PASS+x}" ] ||
  [ -n "${NATS_TEST_JETSTREAM+x}" ] ||
  [ -n "${NATS_TEST_JETSTREAM_RUN_ID+x}" ]
}; then
  echo "authentication and JetStream variables require the server scenario" >&2
  exit 1
fi

run_case() {
  run_image=$1
  run_scenario=$2
  case "$run_scenario" in
    server)
      NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-server.sh
      ;;
    cluster)
      (
        unset NATS_TEST_TOKEN NATS_TEST_USER NATS_TEST_PASS
        unset NATS_TEST_JETSTREAM NATS_TEST_JETSTREAM_RUN_ID
        NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-cluster.sh
      )
      ;;
    lameduck)
      (
        unset NATS_TEST_TOKEN NATS_TEST_USER NATS_TEST_PASS
        unset NATS_TEST_JETSTREAM NATS_TEST_JETSTREAM_RUN_ID
        NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-lameduck.sh
      )
      ;;
    *)
      echo "unknown server matrix scenario: $run_scenario" >&2
      return 2
      ;;
  esac
}

status=0
case_number=0
echo "server matrix images: $*"
echo "server matrix scenarios: $scenario_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "server matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "server matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # scenario_list contains only the validated scenario names above.
  # shellcheck disable=SC2086
  for scenario in $scenario_list; do
    case_number=$((case_number + 1))
    echo "server matrix case $case_number: $image ($scenario)"
    if run_case "$image" "$scenario"; then
      :
    else
      status=1
      echo "server matrix case $case_number failed: $image ($scenario)" >&2
    fi
  done
done

exit "$status"
