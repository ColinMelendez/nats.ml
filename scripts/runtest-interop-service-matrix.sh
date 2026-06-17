#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
default_images=$("$script_dir/default-server-images.sh")
cd "$script_dir/.."

images=${NATS_SERVER_IMAGES:-$default_images}
scenarios=${NATS_INTEROP_SERVICE_MATRIX_SCENARIOS:-service}
modes=${NATS_INTEROP_SERVICE_MATRIX_MODES:-anonymous,anonymous-tls,token,token-tls,user-pass,user-pass-tls,nkey,nkey-tls,jwt,jwt-tls,mtls}

case "$scenarios" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_SERVICE_MATRIX_SCENARIOS must contain service, service-failure, and/or service-parent-close with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $scenarios
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_SERVICE_MATRIX_SCENARIOS must contain at least one scenario" >&2
  exit 1
fi

scenario_list=
for scenario do
  case "$scenario" in
    service|service-failure|service-parent-close)
      ;;
    *)
      echo "unknown Service interop matrix scenario: $scenario (expected service, service-failure, or service-parent-close)" >&2
      exit 1
      ;;
  esac
  scenario_list="$scenario_list${scenario_list:+ }$scenario"
done

case "$modes" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_SERVICE_MATRIX_MODES must contain supported modes with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $modes
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_SERVICE_MATRIX_MODES must contain at least one mode" >&2
  exit 1
fi

mode_list=
for mode do
  case "$mode" in
    anonymous|anonymous-tls|token|token-tls|user-pass|user-pass-tls|\
    nkey|nkey-tls|jwt|jwt-tls|mtls)
      ;;
    *)
      echo "unknown Service interop matrix mode: $mode" >&2
      exit 1
      ;;
  esac
  mode_list="$mode_list${mode_list:+ }$mode"
done

if [ -n "${NATS_TEST_TOKEN+x}" ] && [ -z "$NATS_TEST_TOKEN" ]; then
  echo "NATS_TEST_TOKEN must be non-empty when supplied to the matrix" >&2
  exit 1
fi
if [ -n "${NATS_TEST_USER+x}" ] && [ -z "$NATS_TEST_USER" ]; then
  echo "NATS_TEST_USER must be non-empty when supplied to the matrix" >&2
  exit 1
fi
if [ -n "${NATS_TEST_PASS+x}" ] && [ -z "$NATS_TEST_PASS" ]; then
  echo "NATS_TEST_PASS must be non-empty when supplied to the matrix" >&2
  exit 1
fi
if [ -n "${NATS_TEST_TOKEN+x}" ] && {
  [ -n "${NATS_TEST_USER+x}" ] || [ -n "${NATS_TEST_PASS+x}" ];
}; then
  echo "NATS_TEST_TOKEN cannot be combined with NATS_TEST_USER or NATS_TEST_PASS" >&2
  exit 1
fi
if [ -n "${NATS_TEST_USER+x}" ] && [ -z "${NATS_TEST_PASS+x}" ]; then
  echo "NATS_TEST_USER and NATS_TEST_PASS must be supplied together" >&2
  exit 1
fi
if [ -n "${NATS_TEST_PASS+x}" ] && [ -z "${NATS_TEST_USER+x}" ]; then
  echo "NATS_TEST_USER and NATS_TEST_PASS must be supplied together" >&2
  exit 1
fi

matrix_token=${NATS_TEST_TOKEN:-ocaml_service_matrix_token}
matrix_user=${NATS_TEST_USER:-ocaml_service_matrix_user}
matrix_pass=${NATS_TEST_PASS:-ocaml_service_matrix_pass}

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

clear_credentials() {
  unset NATS_TEST_TOKEN NATS_TEST_USER NATS_TEST_PASS NATS_TEST_TLS_CA
  unset NATS_TEST_TLS_CERT NATS_TEST_TLS_KEY NATS_TEST_NKEY_SEED_FILE
  unset NATS_TEST_NKEY_PUBLIC NATS_TEST_USER_JWT_FILE NATS_TEST_USER_SEED_FILE
  unset NATS_TEST_INTEROP_AUTH_MODE NATS_TEST_INTEROP_AUTH_NEGATIVE
}

run_case() {
  run_image=$1
  run_scenario=$2
  run_mode=$3
  case "$run_mode" in
    anonymous)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_TLS=0 \
          NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop.sh
      )
      ;;
    anonymous-tls)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_TLS=1 \
          NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop.sh
      )
      ;;
    token)
      (
        clear_credentials
        NATS_TEST_TOKEN="$matrix_token" NATS_TEST_INTEROP_MODE="$run_scenario" \
          NATS_TEST_TLS=0 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    token-tls)
      (
        clear_credentials
        NATS_TEST_TOKEN="$matrix_token" NATS_TEST_INTEROP_MODE="$run_scenario" \
          NATS_TEST_TLS=1 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    user-pass)
      (
        clear_credentials
        NATS_TEST_USER="$matrix_user" NATS_TEST_PASS="$matrix_pass" \
          NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_TLS=0 \
          NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop.sh
      )
      ;;
    user-pass-tls)
      (
        clear_credentials
        NATS_TEST_USER="$matrix_user" NATS_TEST_PASS="$matrix_pass" \
          NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_TLS=1 \
          NATS_SERVER_IMAGE="$run_image" ./scripts/runtest-interop.sh
      )
      ;;
    nkey)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_INTEROP_AUTH_MODE=nkey \
          NATS_TEST_TLS=0 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    nkey-tls)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_INTEROP_AUTH_MODE=nkey \
          NATS_TEST_TLS=1 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    jwt)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_INTEROP_AUTH_MODE=jwt \
          NATS_TEST_TLS=0 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    jwt-tls)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_INTEROP_AUTH_MODE=jwt \
          NATS_TEST_TLS=1 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    mtls)
      (
        clear_credentials
        NATS_TEST_INTEROP_MODE="$run_scenario" NATS_TEST_INTEROP_AUTH_MODE=mtls \
          NATS_TEST_TLS=1 NATS_SERVER_IMAGE="$run_image" \
          ./scripts/runtest-interop.sh
      )
      ;;
    *)
      echo "unknown Service interop matrix mode: $run_mode" >&2
      return 2
      ;;
  esac
}

status=0
case_number=0
echo "Service interop matrix images: $*"
echo "Service interop matrix scenarios: $scenario_list"
echo "Service interop matrix modes: $mode_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Service interop matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "Service interop matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # scenario_list and mode_list contain only validated names above.
  # shellcheck disable=SC2086
  for scenario in $scenario_list; do
    # shellcheck disable=SC2086
    for mode in $mode_list; do
      case_number=$((case_number + 1))
      echo "Service interop matrix case $case_number: $image ($scenario, $mode)"
      if run_case "$image" "$scenario" "$mode"; then
        :
      else
        status=1
        echo "Service interop matrix case $case_number failed: $image ($scenario, $mode)" >&2
      fi
    done
  done
done

exit "$status"
