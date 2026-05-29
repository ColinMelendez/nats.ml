#!/bin/sh
set -eu

profile=${COLIMA_PROFILE:-default}
disk=${COLIMA_DISK_GIB:-10}

if colima status "$profile" >/dev/null 2>&1; then
  exec docker info >/dev/null
fi

colima start "$profile" --disk "$disk"
exec docker info >/dev/null
