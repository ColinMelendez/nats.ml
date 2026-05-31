#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

images=${NATS_SERVER_IMAGES:-nats:2.10.22,nats:2.14.3}

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

status=0
case_number=0
echo "interop matrix images: $*"
for image do
  if [ -z "$image" ]; then
    echo "NATS_SERVER_IMAGES contains an empty image name" >&2
    status=1
    continue
  fi
  case_number=$((case_number + 1))
  echo "interop matrix case $case_number: $image"
  case_status=0
  if NATS_SERVER_IMAGE="$image" ./scripts/runtest-interop.sh; then
    :
  else
    case_status=$?
    status=$case_status
    echo "interop matrix case $case_number failed: $image" >&2
  fi
done

exit "$status"
