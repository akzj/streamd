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
Toxiproxy client links so later acceptance cases can isolate one path without
weakening TLS identity checks.
