# Scale acceptance

`make test-scale` creates one million Streams by default, checkpoints them,
closes and reopens the store, verifies representative first/middle/last Stream
lookups, and performs a full scrub. The run preserves JSON, GNU time (including
peak RSS), stderr, configuration, data files, and exit status below
`.tmp/scale/<run-id>`.

Use `SCALE_STREAMS`, `SCALE_WORKERS`, `SCALE_DURATION`, and `SCALE_RUN_ID` to
produce smaller development runs or a stable artifact name. A reduced run is
only a harness smoke test and does not satisfy the million-Stream gate.
