#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
default_images=$("$script_dir/default-server-images.sh")
cd "$script_dir/.."

images=${NATS_SERVER_IMAGES:-$default_images}
modes=${NATS_INTEROP_AUTH_JETSTREAM_RECONNECT_MATRIX_MODES:-nkey,nkey-tls,jwt,jwt-tls,mtls}

case "$modes" in
  ""|,*|*,|*,,*)
    echo "NATS_INTEROP_AUTH_JETSTREAM_RECONNECT_MATRIX_MODES must contain supported modes with no empty entries" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086
set -- $modes
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_INTEROP_AUTH_JETSTREAM_RECONNECT_MATRIX_MODES must contain at least one mode" >&2
  exit 1
fi

mode_list=
for mode do
  case "$mode" in
    nkey|nkey-tls|jwt|jwt-tls|mtls)
      ;;
    *)
      echo "unknown authenticated JetStream reconnect mode: $mode" >&2
      exit 1
      ;;
  esac
  mode_list="$mode_list${mode_list:+ }$mode"
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
      ./scripts/runtest-interop-auth-jetstream-reconnect.sh
  )
}

status=0
case_number=0
echo "authenticated JetStream reconnect matrix images: $*"
echo "authenticated JetStream reconnect matrix modes: $mode_list"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "authenticated JetStream reconnect matrix: pulling $image"
    if ! docker pull "$image"; then
      status=1
      echo "authenticated JetStream reconnect matrix could not pull image: $image" >&2
      continue
    fi
  fi
  # shellcheck disable=SC2086
  for mode in $mode_list; do
    case_number=$((case_number + 1))
    echo "authenticated JetStream reconnect matrix case $case_number: $image ($mode)"
    if run_case "$image" "$mode"; then
      :
    else
      status=1
      echo "authenticated JetStream reconnect matrix case $case_number failed: $image ($mode)" >&2
    fi
  done
done

exit "$status"
