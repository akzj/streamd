# streamd soak test

The soak harness runs the real benchmark against independent Primary and
Standby WALs in strict durability mode. It retains the data roots, final JSON
report, stderr log, exit status, and periodic RSS/VSZ/FD/disk samples below
`.tmp/soak/<run-id>`.

Quick validation:

```bash
SOAK_DURATION=10m make test-soak
```

Release soak:

```bash
make test-soak-72h
```

The timed process defaults to a disk-budgeted 100 requests/s, performs periodic
Checkpoint and bounded Compaction, and every hour creates a verified linked
Snapshot before collecting covered Primary WAL. It finishes with checkpoint,
scrub, restart, and record-count verification through `streamd-bench -verify`.
Override the ceiling with `SOAK_REQUESTS_PER_SECOND` only after calculating the
72-hour Primary WAL, Segment, Snapshot, and Standby WAL space budget.
An exit status of zero is necessary but not sufficient: review `resources.csv`
for monotonic RSS, FD, and disk growth and retain the entire run directory as
the acceptance artifact.
