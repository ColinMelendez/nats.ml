#!/bin/sh

# This file is sourced by acceptance runners. It deliberately records only
# image and container identity fields; Docker environment and command fields
# may contain credentials and are never copied into an artifact bundle.

artifact_root=${NATS_TEST_ARTIFACT_DIR-}
artifact_runner=
artifact_run_id=
artifact_run_dir=

artifact_init() {
  artifact_runner=$1
  artifact_run_id=$2
  if [ -n "$artifact_root" ]; then
    artifact_run_dir="$artifact_root/$artifact_runner-$artifact_run_id"
  else
    artifact_run_dir=
  fi
}

artifact_create_dir() {
  if [ -z "$artifact_run_dir" ]; then
    return 1
  fi
  if ! mkdir -p "$artifact_run_dir"; then
    echo "could not create acceptance artifact directory: $artifact_run_dir" >&2
    return 1
  fi
  chmod 700 "$artifact_run_dir" 2>/dev/null || true
  return 0
}

artifact_save_file() {
  status=$1
  source=$2
  target=$3
  if [ "$status" -eq 0 ] || [ -z "$artifact_run_dir" ] ||
    [ ! -f "$source" ]; then
    return 0
  fi
  if artifact_create_dir; then
    if ! cp "$source" "$artifact_run_dir/$target"; then
      echo "could not preserve acceptance artifact: $target" >&2
    fi
  fi
  return 0
}

artifact_save_text() {
  status=$1
  target=$2
  shift 2
  if [ "$status" -eq 0 ] || [ -z "$artifact_run_dir" ]; then
    return 0
  fi
  if artifact_create_dir; then
    if ! printf '%s\n' "$@" >"$artifact_run_dir/$target"; then
      echo "could not preserve acceptance artifact: $target" >&2
    fi
  fi
  return 0
}

artifact_save_docker_log() {
  status=$1
  container=$2
  target=$3
  if [ "$status" -eq 0 ] || [ -z "$artifact_run_dir" ] ||
    [ -z "$container" ]; then
    return 0
  fi
  if artifact_create_dir; then
    docker logs "$container" >"$artifact_run_dir/$target" 2>&1 || true
  fi
  return 0
}

artifact_save_docker_state() {
  status=$1
  container=$2
  target=$3
  if [ "$status" -eq 0 ] || [ -z "$artifact_run_dir" ] ||
    [ -z "$container" ]; then
    return 0
  fi
  if artifact_create_dir; then
    docker inspect --format 'id={{.Id}}
name={{.Name}}
image={{.Config.Image}}
state={{.State.Status}}
exit_code={{.State.ExitCode}}
started_at={{.State.StartedAt}}
finished_at={{.State.FinishedAt}}' "$container" \
      >"$artifact_run_dir/$target" 2>&1 || true
  fi
  return 0
}

artifact_save_image() {
  status=$1
  image=$2
  target=$3
  if [ "$status" -eq 0 ] || [ -z "$artifact_run_dir" ] ||
    [ -z "$image" ]; then
    return 0
  fi
  if artifact_create_dir; then
    docker image inspect --format 'id={{.Id}}
os={{.Os}}
architecture={{.Architecture}}
variant={{.Variant}}
repo_digests={{json .RepoDigests}}' "$image" \
      >"$artifact_run_dir/$target" 2>&1 || true
  fi
  return 0
}
