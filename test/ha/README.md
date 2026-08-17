# Local Strict HA integration test

This suite runs the real `streamd` binaries against a TLS-enabled three-member
etcd development cluster. Docker Compose owns etcd, Primary, Standby,
Toxiproxy, isolated networks, and disposable volumes. The Go test runner uses
the public mTLS API and never mounts a host data directory.

Run the complete test with automatic cleanup:

```bash
make test-ha
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
as the replacement Standby. It appends on the promoted node, fails back, and
reads that append from the former Primary to prove Rejoin persisted the new
committed prefix.

Finally, the suite creates and verifies an offline installable Snapshot, runs
the guarded WAL collector, resets only the disposable Standby data volume,
installs the Snapshot, and proves the restored Standby can rejoin and sustain a
new Strict append. WAL files are never removed directly by the harness.
