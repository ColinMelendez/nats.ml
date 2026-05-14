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

The current pure-core tests are portable. Linux-backed integration testing
will be added with the Core NATS server harness.

The project uses Dune package management. No compatibility layer for NATS
Streaming is planned.
