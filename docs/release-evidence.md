# Release acceptance evidence

This record ties release claims to explicit local runs. It supplements the
support contract in [`support.md`](support.md); it does not replace the
repeatable commands or make the test matrix a complete NATS conformance suite.

## 2026-09-03 current-line refresh

The release-candidate working tree was exercised with OCaml 5.5 and
`github.com/nats-io/nats.go v1.53.1`. Docker acceptance used the owned
`nats-tests` Colima profile with 4 CPUs, 6 GiB RAM, and a 12 GiB disk.

### Local gates

The following gates passed:

- formatting, complete package build, API documentation, and the deterministic
  Dune test suite;
- ShellCheck over every integration script; and
- the Go interoperability peer test suite.

### NATS 2.14.6 acceptance

The current-line refresh passed:

- server lifecycle, cluster discovery, and lame-duck handling;
- Core NATS, reconnect, TLS Core, and TLS reconnect interoperability;
- positive, negative, and reconnect authentication coverage;
- all 33 current-line Service cells: eleven credential/TLS modes in each of the
  base, slow-consumer failure, and parent-close scenarios;
- all seven privileged system-account credential/TLS modes;
- all 30 single-server JetStream cells: pull, Push, Ordered, Key-Value, and
  Object Store across six authentication/TLS modes;
- all 21 anonymous cluster/reconnect cases: eight Ordered, six Key-Value, six
  Object Store, and one durable Push restart case; and
- all 100 authenticated cluster cases: 40 Ordered, 30 Key-Value, and 30 Object
  Store cases across NKey, JWT, NKey-over-TLS, JWT-over-TLS, and mTLS.

The single-server Key-Value matrix was repeated after one transient peer-start
failure; all six focused repetitions and the subsequent complete 30-cell
matrix passed.

### Pinned reconnect compatibility

The durable Push hard-restart path passed anonymous mode plus all five advanced
credential/TLS modes on each pinned server:

- `nats:2.10.22`;
- `nats:2.12.15`; and
- `nats:2.14.6`.

This is 18 compatibility cases. The formerly intermittent NKey-over-TLS case
also passed five consecutive repetitions on 2.14.6. The reconnect harness
uses `sync_interval: always` and waits beyond the server's consumer-state
batch interval before `SIGKILL`, so it tests client session recovery without
racing the server's asynchronous consumer-state file write.

The wrapper stopped the owned VM after every matrix. No acceptance container,
test process, or generated acceptance directory remained after the run.

### Qualification

The source head after the harness correction was `903dcc7`. The checkout also
contained pre-existing, uncommitted build-environment, lock, benchmark, and
other unrelated worktree changes that were not part of that commit. This
evidence validates that exact working tree, which cannot be reconstructed from
`903dcc7` alone. Before tagging a release, land or remove those changes
deliberately and repeat every release gate in [`support.md`](support.md) from a
clean checkout. The broad 2.10.22 and 2.12.15 matrices retain their earlier
recorded evidence; this refresh reran the changed hard-restart path on all
three pins and the broader acceptance surface on the new 2.14.6 current-line
pin.
