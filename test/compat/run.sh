#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
baseline=${COMPAT_BASELINE:-3555719}
work=$(mktemp -d)

cleanup() {
  status=$?
  trap - EXIT INT TERM
  rm -rf -- "$work"
  exit "$status"
}
trap cleanup EXIT INT TERM

git -C "$repo_root" cat-file -e "$baseline^{commit}"
mkdir -p "$work/old" "$work/bin" "$work/old-root" "$work/new-root"
git -C "$repo_root" archive "$baseline" | tar -x -C "$work/old"
(cd "$work/old" && go build -o "$work/bin/old-bench" ./cmd/streamd-bench && go build -o "$work/bin/old-tool" ./cmd/streamd-tool)
(cd "$repo_root" && go build -o "$work/bin/new-bench" ./cmd/streamd-bench && go build -o "$work/bin/new-tool" ./cmd/streamd-tool)

"$work/bin/old-bench" -duration 100ms -workers 4 -streams 32 -precreate-streams \
  -payload-bytes 64 -checkpoint-interval 0 -verify=true -data "$work/old-root" >"$work/old-report.json"
"$work/bin/new-tool" scrub -data "$work/old-root" >"$work/new-reads-old.json"

"$work/bin/new-bench" -duration 100ms -workers 4 -streams 32 -precreate-streams \
  -payload-bytes 64 -checkpoint-interval 0 -verify=true -data "$work/new-root" >"$work/new-report.json"
"$work/bin/old-tool" scrub -data "$work/new-root" >"$work/old-reads-new.json"

printf 'format compatibility passed against %s\n' "$baseline"
