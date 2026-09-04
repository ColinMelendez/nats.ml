# Release acceptance evidence

This record ties release claims to explicit local runs. It supplements the
support contract in [`support.md`](support.md); it does not replace the
repeatable commands or make the test matrix a complete NATS conformance suite.

## 2026-09-03 clean release candidate

Commit `dfc59817f5e6e50673de86d3d246b2ba7c3c0392` was exercised from a
clean, detached checkout with OCaml 5.5. The Go peer used
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

### Pinned reconnect compatibility

The durable Push hard-restart path passed anonymous mode plus all five advanced
credential/TLS modes on each pinned server:

- `nats:2.10.22`;
- `nats:2.12.15`; and
- `nats:2.14.6`.

This is 18 compatibility cases. The reconnect harness sets
`sync_interval: always` and waits beyond the server's consumer-state batch
interval before `SIGKILL`, so it tests client session recovery without racing
the server's asynchronous consumer-state file write.

### Environment notes

Persistent Nix profiles kept the compiler and integration tools rooted for the
run. `TMPDIR` was under the clean checkout parent so every host path used by a
container was available through Colima's shared filesystem.

The first anonymous cluster case was repeated after switching the matrix to a
prebuilt acceptance executable. The complete anonymous cluster matrices were
then run from their first case and passed. An authenticated reconnect batch was
also discarded when it was started from the default development environment,
which does not contain the credential generator. The complete 15-case batch
was rerun in the integration environment and passed; the three anonymous cases
were also repeated in a standalone run.

The owned VM was stopped after the final matrix. No acceptance container, test
process, or generated authentication or TLS directory remained after the run.
No release gate or scenario remains blocked.

The broad 2.10.22 and 2.12.15 matrices retain their earlier recorded evidence;
this clean refresh reran the changed hard-restart path on all three pins and
the broader acceptance surface on the 2.14.6 current-line pin.
