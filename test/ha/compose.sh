#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
compose_file="$script_dir/compose.yaml"
command=${1:-}
project=${HA_PROJECT_NAME:-streamd-ha-dev}

case "$project" in
  ''|*[!a-zA-Z0-9_-]*)
    echo "invalid HA_PROJECT_NAME: $project" >&2
    exit 2
    ;;
esac

run_parent="$repo_root/.tmp/ha"
mkdir -p "$run_parent"
run_dir=${HA_RUN_DIR:-"$run_parent/$project"}
mkdir -p "$run_dir"
run_dir=$(CDPATH= cd -- "$run_dir" && pwd)
case "$run_dir" in
  "$run_parent"/*) ;;
  *)
    echo "HA_RUN_DIR must be inside $run_parent" >&2
    exit 2
    ;;
esac

compose() {
  HA_RUN_DIR="$run_dir" docker compose -p "$project" -f "$compose_file" "$@"
}

prepare() {
  if [ -f "$run_dir/.streamd-ha-test" ]; then
    return
  fi
  if [ "$(find "$run_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo "HA run directory is non-empty and not owned by this harness: $run_dir" >&2
    exit 1
  fi
  (cd "$repo_root" && go run ./test/ha/cmd/prepare -out "$run_dir")
  : >"$run_dir/.streamd-ha-test"
}

down() {
  compose --profile test down -v --remove-orphans
}

cleanup_files() {
  if [ -f "$run_dir/.streamd-ha-test" ]; then
    rm -rf -- "$run_dir"
  fi
}

case "$command" in
  prepare)
    prepare
    ;;
  up)
    prepare
    compose up -d --build --wait --wait-timeout 120 etcd-1 etcd-2 etcd-3 toxiproxy proxy-init streamd-primary streamd-standby
    ;;
  test)
    prepare
    compose --profile test run --rm --build test-runner
    ;;
  logs)
    shift
    compose logs --no-color "$@"
    ;;
  down)
    down
    cleanup_files
    ;;
  all)
    prepare
    trap 'down >/dev/null 2>&1 || true; cleanup_files' EXIT INT TERM
    compose up -d --build --wait --wait-timeout 120 etcd-1 etcd-2 etcd-3 toxiproxy proxy-init streamd-primary streamd-standby
    compose --profile test run --rm --build test-runner
    ;;
  *)
    echo "usage: $0 {prepare|up|test|logs|down|all}" >&2
    exit 2
    ;;
esac
