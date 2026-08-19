# Format compatibility gate

`make test-compat` builds the pinned accepted V1 baseline (`3555719`) and the
current checkout. Each version creates and checkpoints a data root; the other
version then performs a full offline scrub. This is both an upgrade and a
rollback read-compatibility gate for the unchanged V1 disk format.

The baseline commit must be present in the Git clone. Override it only for an
explicitly reviewed format baseline with `COMPAT_BASELINE=<commit>`.
