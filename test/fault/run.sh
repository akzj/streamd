#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)

cd "$repo_root"
go test ./internal/storage/commit -count=1 -run 'Test(StorageFaultsFailClosed|CommitWaitsForSlowFsync)$'
go test ./internal/storage/fsutil -count=1 -run 'Test(WriteFullAtCompletesShortWrites|AtomicWriteCrashBeforeRename)$'
go test ./internal/storage/wal -count=1 -run 'Test(CreateAppendRecoverTruncatedTailAndSeal|HistoryBoundsAndCorruptionErrors)$'
go test ./internal/storage/engine -count=1 -run 'Test(CheckpointCrashRecovery|CapacityCriticalRejectsAppendButPreservesReadAndMaintenance)$'
