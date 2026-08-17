# Local Strict HA integration test

This suite runs the real `streamd` binaries against a TLS-enabled three-member
etcd development cluster. Docker Compose owns etcd, Primary, Standby,
Toxiproxy, isolated networks, and disposable volumes. The Go test runner uses
the public mTLS API and never mounts a host data directory.

Run the complete test with automatic cleanup:

```bash
make test-ha
```

Repeat the complete isolated acceptance run (default 10 times):

```bash
make test-ha-repeat
HA_REPEAT=100 make test-ha-repeat
```

For interactive diagnosis:

```bash
make ha-up
make ha-test
make ha-logs
make ha-down
```

Set `HA_PROJECT_NAME` to run an additional isolated environment. Generated
certificates and configuration live only under `.tmp/ha/<project>` and
`ha-down` removes them after Compose has removed its named volumes.

The suite verifies a Strict append/read/idempotency path and then disables the
Primary-to-Standby Toxiproxy link to prove that a new Strict append is not
acknowledged. It deliberately runs serially because the partition is a
cluster-wide fault.

Each streamd node reaches all three etcd members through independent
Toxiproxy client links. The complete run also stops one etcd member to prove
continued writes, then removes quorum and waits for the Primary Lease safety
window to fence writes. After quorum recovery it performs a controlled node
restart and verifies Strict writes resume under a new safe runtime state.

The process failover drill then kills the Primary, waits for its Lease to
expire, promotes the former Standby with a new Term, and starts the old Primary
as the replacement Standby. It appends on the promoted node, then snapshots the
surviving Primary and reinstalls the former Primary before failback. This
deliberately covers the crash window where a newer local Manifest exists but
the durable committed checkpoint is older; the harness never truncates or
trusts that suffix. It reads the append after failback to prove Snapshot Rejoin
preserved the committed prefix.

Finally, the suite creates and verifies an offline installable Snapshot, runs
the guarded WAL collector, resets only the disposable Standby data volume,
and first starts the empty Standby without installing the Snapshot. It verifies
that the Primary exposes a deterministic `snapshot_required` recovery task,
keeps readiness false, and does not open its public gRPC service. The harness
then removes etcd quorum and verifies Lease loss changes the node to
`failed/recovering` without losing that task identity. After restoring quorum,
it stops both nodes, installs the named Snapshot using the task's current Term,
and proves the restored Standby can rejoin and sustain a new Strict append. WAL
files are never removed directly by the harness.

The suite then snapshots the healthy Primary and uses a dedicated offline fault
injector to append a valid but uncommitted conflicting WAL suffix to the stopped
disposable Standby. The injector has no network, requires the data-root lock and
a fully committed Standby checkpoint, and is not part of either production
binary. After restart, the suite proves that `LOG_DIVERGED` closes the public
gRPC listener and exposes a deterministic recovery task containing the exact
target Entry ID and CRC. It installs the verified Snapshot without pre-clearing
the Standby volume, restarts both nodes, verifies the committed prefix, and
performs another Strict append. This also guards Snapshot installation against
retaining a replaced active WAL as if it were a sealed history file.

On failure the harness still removes containers, networks, and volumes, but
first preserves service logs below `.tmp/ha-artifacts/`. CI uploads that
directory as a short-lived diagnostic artifact. The nightly workflow runs ten
fresh projects; the 100-run command is the release-candidate stability gate.
All offline maintenance containers use `network_mode: none`, and cleanup loads
both test and maintenance profiles so one-shot commands cannot leak Compose
default networks.
