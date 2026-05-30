# NATS for OCaml

This repository is building a modern NATS client SDK for OCaml.

The implementation is organized around an I/O-neutral Core NATS protocol
state machine and runtime adapters. The first adapter will target Eio. The
long-term feature set includes Core NATS, JetStream, Key-Value, Object Store,
and Services; NATS Streaming/STAN is intentionally out of scope.

The research and architecture proposal is in
[`docs/design.md`](docs/design.md). The phased implementation roadmap is in
[`plan.md`](plan.md).

## Development

Enter the development shell and run the build or tests with Dune:

```sh
nix develop
dune build
dune runtest
```

The real-server tests use a smaller, separate shell so integration-only
tooling does not become part of the normal OCaml development environment:

```sh
nix develop .#integration
```

The current pure-core tests are portable. An opt-in Core NATS acceptance run
against a pinned `nats-server` Docker image is available with:

```sh
./scripts/start-colima.sh
./scripts/runtest-server.sh
./scripts/runtest-reconnect.sh
./scripts/runtest-cluster.sh
./scripts/runtest-tls.sh
./scripts/runtest-lameduck.sh
```

The script requires Docker and is intentionally outside `dune runtest`; Docker
is the one integration dependency supplied by the host rather than Nix. The
Colima helper creates or starts the `default` profile with a 10 GiB disk by
default; set `COLIMA_DISK_GIB` only when a larger disk is actually needed. Set
`NATS_SERVER_IMAGE` to try another server image. Cluster, TLS, and reconnect
scenarios are split into focused runners; the reconnect runner starts two
single-node servers, kills the active one, checks pending-request failure, and
checks subscription recovery.
The cluster runner starts a three-node route mesh, connects only to the seed,
checks the advertised client URLs, kills the seed, and checks failover to a
discovered peer with subscription recovery.
The lame-duck runner signals a live server through the container and checks the
dynamic `INFO` flag, typed `Lame_duck_mode` event, and continued use of the
existing connection.
The TLS runner generates an ephemeral CA and hostname-checked server
certificate, then verifies a real TLS connection. To
exercise username/password authentication, set both `NATS_TEST_USER` and
`NATS_TEST_PASS` to non-empty ephemeral credentials before running the script;
the credentials must use only ASCII letters, digits, underscores, and hyphens.
The harness then requires anonymous connection rejection as well as successful
authenticated traffic. To enable the JetStream acceptance slice, also set
`NATS_TEST_JETSTREAM=1`; this starts the server with JetStream enabled and
exercises stream management, publish acknowledgements, duplicate message ids,
and cleanup. The server runner also checks request timeout/cancellation
cleanup, auto-unsubscribe limits, bounded subscription slow-consumer behavior,
subscription drain, connection drain, and parent-switch cleanup.
The JetStream test uses a per-run stream name; set
`NATS_TEST_JETSTREAM_RUN_ID` only when a stable, safe identifier is useful for
debugging.

The first JetStream layer is available through `Nats_eio.Jetstream`: typed
stream and consumer configuration and management, including consumer metadata,
sampling, push rate limits, replica inheritance, singular and multi-subject
filters, redelivery backoff schedules, typed pause/resume control, and
read-modify-write updates that preserve unknown server fields, direct
stored-message
reads, one-shot fetch, durable publish and message acknowledgements, and
switch-owned Eio pull/push sessions over ordinary Core NATS request/reply. Push
sessions consume idle heartbeats, answer flow-control requests including
stalled-heartbeat replies, and restore replayable delivery subscriptions across
reconnects. Ordered sessions use client-managed ephemeral pull consumers,
validate consecutive consumer sequences, and resume from the next stream
sequence after recovery. `Nats_eio.Object_store` now provides validated
buckets, direct metadata, incremental Bytesrw put/get, digest verification,
metadata updates and links, snapshot/live watches, listing, deletion,
replacement cleanup, sealing, and bucket policy updates for replicas, placement,
compression, and stream metadata. `Nats_eio.Key_value` provides revisioned
values, compare-and-set mutations, finite scans, history, and cancellable
watches. `Nats_eio.Service` provides typed endpoint workers, queue groups,
`$SRV.*` monitoring and fan-out discovery, request/service-error replies,
statistics, replayable subscriptions, and service-local draining. Advanced
cluster failure scenarios and cross-SDK interoperability coverage remain in
the final acceptance phase.

The project uses Dune package management. No compatibility layer for NATS
Streaming is planned.
