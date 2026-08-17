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
  HA_RUN_DIR="$run_dir" \
  HA_PRIMARY_CONFIG="${HA_PRIMARY_CONFIG:-$run_dir/configs/primary.json}" \
  HA_STANDBY_CONFIG="${HA_STANDBY_CONFIG:-$run_dir/configs/standby.json}" \
    docker compose -p "$project" -f "$compose_file" "$@"
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
    compose stop -t 1 etcd-3
    compose --profile test run --rm --no-deps -e HA_SCENARIO=single-member-loss test-runner
    compose start etcd-3
    compose up -d --wait --wait-timeout 60 etcd-1 etcd-2 etcd-3
    compose stop -t 1 etcd-2 etcd-3
    compose --profile test run --rm --no-deps -e HA_SCENARIO=quorum-loss test-runner
    compose start etcd-2 etcd-3
    compose up -d --wait --wait-timeout 60 etcd-1 etcd-2 etcd-3
    compose restart -t 1 streamd-primary streamd-standby
    compose up -d --wait --wait-timeout 120 streamd-primary streamd-standby
    compose --profile test run --rm --no-deps -e HA_SCENARIO=quorum-recovered test-runner
    compose --profile test run --rm --no-deps -e HA_SCENARIO=before-failover test-runner
    compose kill -s KILL streamd-primary
    compose stop -t 1 streamd-standby
    sleep 16
    HA_PRIMARY_CONFIG="$run_dir/configs/primary-as-standby.json" \
    HA_STANDBY_CONFIG="$run_dir/configs/standby-as-primary.json" \
      compose up -d --force-recreate --wait --wait-timeout 120 streamd-primary streamd-standby
    compose --profile test run --rm --no-deps \
      -e HA_SCENARIO=after-failover \
      -e HA_PRIMARY_ADDRESS=streamd-standby:7443 \
      -e HA_PRIMARY_SERVER_NAME=streamd-standby \
      test-runner
    compose kill -s KILL streamd-standby
    compose stop -t 1 streamd-primary
    sleep 16
    compose up -d --force-recreate --wait --wait-timeout 120 streamd-primary streamd-standby
    compose --profile test run --rm --no-deps -e HA_SCENARIO=after-failback test-runner
    compose --profile test run --rm --no-deps -e HA_SCENARIO=before-snapshot test-runner
    compose stop -t 10 streamd-primary streamd-standby
    snapshot_json=$(compose --profile maintenance run --rm --no-deps --build maintenance-primary \
      snapshot -data /var/lib/streamd -out /var/lib/streamd/snapshots/checkpoint)
    snapshot_term=$(printf '%s\n' "$snapshot_json" | sed -n '/^{/,$p' | jq -er '.Term | select(. > 0)')
    compose --profile maintenance run --rm --no-deps maintenance-primary \
      verify-snapshot -path /var/lib/streamd/snapshots/checkpoint
    compose --profile maintenance run --rm --no-deps maintenance-primary \
      collect-wal -data /var/lib/streamd -snapshot /var/lib/streamd/snapshots/checkpoint
    compose --profile maintenance run --rm --no-deps reset-standby
    compose --profile maintenance run --rm --no-deps maintenance-standby \
      install-snapshot -data /var/lib/streamd -path /snapshots/checkpoint \
      -term "$snapshot_term" -leader-id 33333333-3333-3333-3333-333333333333
    compose up -d --force-recreate --wait --wait-timeout 120 streamd-primary streamd-standby
    compose --profile test run --rm --no-deps -e HA_SCENARIO=after-snapshot test-runner
    compose --profile test run --rm --no-deps -e HA_SCENARIO=standby-partition test-runner
    ;;
  *)
    echo "usage: $0 {prepare|up|test|logs|down|all}" >&2
    exit 2
    ;;
esac
