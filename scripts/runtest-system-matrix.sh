#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
default_images=$("$script_dir/default-server-images.sh")
cd "$script_dir/.."

images=${NATS_SYSTEM_SERVER_IMAGES:-$default_images}
auth_modes=${NATS_SYSTEM_AUTH_MODES:-user-pass,user-pass-tls,nkey,nkey-tls,jwt,jwt-tls,mtls}
case "$images" in
  ""|,*|*,|*,,*)
    echo "NATS_SYSTEM_SERVER_IMAGES must contain non-empty comma-separated images" >&2
    exit 1
    ;;
esac
case "$auth_modes" in
  ""|,*|*,|*,,*)
    echo "NATS_SYSTEM_AUTH_MODES must contain non-empty comma-separated modes" >&2
    exit 1
    ;;
esac

old_ifs=$IFS
IFS=','
# shellcheck disable=SC2086 # the validated comma-separated image list is intentional.
set -- $images
IFS=$old_ifs

if [ "$#" -eq 0 ]; then
  echo "NATS_SYSTEM_SERVER_IMAGES must contain at least one image" >&2
  exit 1
fi

validate_auth_modes() {
  old_ifs=$IFS
  IFS=','
  # shellcheck disable=SC2086 # the validated comma-separated mode list is intentional.
  set -- $auth_modes
  IFS=$old_ifs
  if [ "$#" -eq 0 ]; then
    echo "NATS_SYSTEM_AUTH_MODES must contain at least one mode" >&2
    return 1
  fi
  for auth_mode do
    case "$auth_mode" in
      user-pass|user-pass-tls|nkey|nkey-tls|jwt|jwt-tls|mtls)
        ;;
      *)
        echo "NATS_SYSTEM_AUTH_MODES contains unsupported mode: $auth_mode" >&2
        return 1
        ;;
    esac
  done
}

if ! validate_auth_modes; then
  exit 1
fi

status=0
case_number=0
echo "system-account cluster matrix images: $*"
echo "system-account cluster matrix auth modes: $auth_modes"

run_image_modes() {
  image=$1
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "system-account matrix: image $image is not cached; refusing to pull it" >&2
    return 1
  fi
  image_status=0
  old_ifs=$IFS
  IFS=','
  # shellcheck disable=SC2086 # the validated comma-separated mode list is intentional.
  set -- $auth_modes
  IFS=$old_ifs
  for auth_mode do
    case_number=$((case_number + 1))
    echo "system-account matrix case $case_number: $image ($auth_mode)"
    if (
      unset NATS_TEST_SYSTEM_ACCOUNT NATS_TEST_SYSTEM_NKEY_PUBLIC \
        NATS_TEST_SYSTEM_NKEY_SEED_FILE NATS_TEST_SYSTEM_USER_JWT_FILE \
        NATS_TEST_SYSTEM_USER_SEED_FILE NATS_TEST_TLS_CA NATS_TEST_TLS_CERT \
        NATS_TEST_TLS_KEY NATS_TEST_SYSTEM_USER NATS_TEST_SYSTEM_PASS
      NATS_SERVER_IMAGE="$image" NATS_TEST_SYSTEM_AUTH_MODE="$auth_mode" \
        ./scripts/runtest-system-cluster.sh
    ); then
      :
    else
      image_status=1
      status=1
      echo "system-account matrix case $case_number failed: $image ($auth_mode)" >&2
    fi
  done
  return "$image_status"
}

for image do
  if [ -z "$image" ]; then
    echo "NATS_SYSTEM_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  run_image_modes "$image" || status=1
done

exit "$status"
