#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
streams=${SCALE_STREAMS:-1000000}
workers=${SCALE_WORKERS:-64}
duration=${SCALE_DURATION:-1s}
run_id=${SCALE_RUN_ID:-"$(date -u +%Y%m%dT%H%M%SZ)-$$"}
run_dir="$repo_root/.tmp/scale/$run_id"

case "$run_id" in
  ''|*[!a-zA-Z0-9_.-]*) echo "invalid SCALE_RUN_ID: $run_id" >&2; exit 2;;
esac
for value in "$streams" "$workers"; do
  case "$value" in ''|*[!0-9]*) echo "scale counts must be positive integers" >&2; exit 2;; esac
  [ "$value" -gt 0 ] || { echo "scale counts must be positive" >&2; exit 2; }
done

mkdir -p "$run_dir/bin" "$run_dir/data"
(cd "$repo_root" && go build -o "$run_dir/bin/streamd-bench" ./cmd/streamd-bench)

start_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
set +e
/usr/bin/time -v "$run_dir/bin/streamd-bench" \
  -duration "$duration" -mode single \
  -workers "$workers" -streams "$streams" -precreate-streams \
  -payload-bytes 0 -batch 1 -checkpoint-interval 0 \
  -verify=true -verify-reopen=true -data "$run_dir/data" \
  >"$run_dir/report.json" 2>"$run_dir/time-and-errors.log"
status=$?
set -e
end_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf '%s\n' "$status" >"$run_dir/exit-status"
printf 'start_utc=%s\nend_utc=%s\nstreams=%s\nworkers=%s\n' "$start_utc" "$end_utc" "$streams" "$workers" >"$run_dir/run.env"
[ "$status" -eq 0 ] || { echo "scale acceptance failed; artifacts: $run_dir" >&2; exit "$status"; }
test -s "$run_dir/report.json"
printf 'scale artifacts: %s\n' "$run_dir"
