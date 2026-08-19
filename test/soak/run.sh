#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
duration=${SOAK_DURATION:-10m}
workers=${SOAK_WORKERS:-16}
streams=${SOAK_STREAMS:-10000}
payload=${SOAK_PAYLOAD_BYTES:-1024}
rate=${SOAK_REQUESTS_PER_SECOND:-100}
sample_interval=${SOAK_SAMPLE_INTERVAL:-60}
run_id=${SOAK_RUN_ID:-"$(date -u +%Y%m%dT%H%M%SZ)-$$"}
run_dir="$repo_root/.tmp/soak/$run_id"

case "$run_id" in
  ''|*[!a-zA-Z0-9_.-]*) echo "invalid SOAK_RUN_ID: $run_id" >&2; exit 2;;
esac
case "$sample_interval" in
  ''|*[!0-9]*) echo "SOAK_SAMPLE_INTERVAL must be seconds" >&2; exit 2;;
esac
[ "$sample_interval" -gt 0 ] || { echo "SOAK_SAMPLE_INTERVAL must be positive" >&2; exit 2; }
case "$rate" in
  ''|*[!0-9]*) echo "SOAK_REQUESTS_PER_SECOND must be a positive integer" >&2; exit 2;;
esac
[ "$rate" -gt 0 ] || { echo "SOAK_REQUESTS_PER_SECOND must be positive" >&2; exit 2; }

mkdir -p "$run_dir/bin" "$run_dir/primary" "$run_dir/standby"
(cd "$repo_root" && go build -o "$run_dir/bin/streamd-bench" ./cmd/streamd-bench)

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ -n "${bench_pid:-}" ] && kill -0 "$bench_pid" 2>/dev/null; then
    kill -TERM "$bench_pid" 2>/dev/null || true
    wait "$bench_pid" 2>/dev/null || true
  fi
  printf '%s\n' "$status" >"$run_dir/exit-status"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$run_dir/bin/streamd-bench" \
  -duration "$duration" -mode strict \
  -workers "$workers" -streams "$streams" -precreate-streams \
  -payload-bytes "$payload" -batch 1 -checkpoint-interval 1m \
  -max-requests-per-second "$rate" -retention-interval 1h -max-retained-wal-bytes 4294967296 \
  -data "$run_dir/primary" -standby-data "$run_dir/standby" \
  -verify=true >"$run_dir/report.json" 2>"$run_dir/benchmark.log" &
bench_pid=$!

printf 'timestamp_utc,rss_kib,vsz_kib,fd_count,primary_bytes,standby_bytes\n' >"$run_dir/resources.csv"
while kill -0 "$bench_pid" 2>/dev/null; do
  rss=$(ps -o rss= -p "$bench_pid" | tr -d ' ' || true)
  vsz=$(ps -o vsz= -p "$bench_pid" | tr -d ' ' || true)
  fds=$(find "/proc/$bench_pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')
  primary_bytes=$(du -sb "$run_dir/primary" | awk '{print $1}')
  standby_bytes=$(du -sb "$run_dir/standby" | awk '{print $1}')
  printf '%s,%s,%s,%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${rss:-0}" "${vsz:-0}" "$fds" "$primary_bytes" "$standby_bytes" >>"$run_dir/resources.csv"
  sleep "$sample_interval" &
  wait $! || true
done
wait "$bench_pid"
bench_pid=
test -s "$run_dir/report.json"
printf 'soak artifacts: %s\n' "$run_dir"
