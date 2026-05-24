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

The current pure-core tests are portable. An opt-in Core NATS acceptance run
against a pinned `nats-server` Docker image is available with:

```sh
./scripts/runtest-server.sh
```

The script requires Docker and is intentionally outside `dune runtest`; set
`NATS_SERVER_IMAGE` to try another server image. Cluster, TLS, and reconnect
scenarios will be added as the corresponding implementation phases land. To
exercise username/password authentication, set both `NATS_TEST_USER` and
`NATS_TEST_PASS` to non-empty ephemeral credentials before running the script;
the credentials must use only ASCII letters, digits, underscores, and hyphens.
The harness then requires anonymous connection rejection as well as successful
authenticated traffic. To enable the JetStream acceptance slice, also set
`NATS_TEST_JETSTREAM=1`; this starts the server with JetStream enabled and
exercises stream management, publish acknowledgements, duplicate message ids,
and cleanup. The JetStream test uses a per-run stream name; set
`NATS_TEST_JETSTREAM_RUN_ID` only when a stable, safe identifier is useful for
debugging.

The first JetStream layer is available through `Nats_eio.Jetstream`: typed
stream and consumer management, one-shot fetch, durable publish and message
acknowledgements, and switch-owned Eio pull/push sessions over ordinary Core
NATS request/reply. Push sessions consume idle heartbeats, answer flow-control
requests including stalled-heartbeat replies, and restore replayable delivery
subscriptions across reconnects. Ordered sessions use client-managed
ephemeral pull consumers, validate consecutive consumer sequences, and resume
from the next stream sequence after recovery. `Nats_eio.Key_value` provides
bucket operations, finite reads, history, and cancellable watches. The
`Nats_eio.Object_store` surface provides incremental put/get, metadata,
listing, watches, links, deletion, updates, and sealing. Services, bucket
inventory/configuration extensions, cluster-only Object Store options, and
real cluster/interop coverage remain later implementation phases.

The project uses Dune package management. No compatibility layer for NATS
Streaming is planned.
