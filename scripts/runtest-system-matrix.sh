#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

images=${NATS_SYSTEM_SERVER_IMAGES:-nats:2.10.22,nats:2.12.15,nats:2.14.5}
case "$images" in
  ""|,*|*,|*,,*)
    echo "NATS_SYSTEM_SERVER_IMAGES must contain non-empty comma-separated images" >&2
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

status=0
case_number=0
echo "system-account cluster matrix images: $*"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SYSTEM_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "system-account matrix: image $image is not cached; refusing to pull it" >&2
    status=1
    continue
  fi
  case_number=$((case_number + 1))
  echo "system-account matrix case $case_number: $image"
  if NATS_SERVER_IMAGE="$image" ./scripts/runtest-system-cluster.sh; then
    :
  else
    status=1
    echo "system-account matrix case $case_number failed: $image" >&2
  fi
done

exit "$status"
