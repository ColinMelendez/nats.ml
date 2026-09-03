# NATS for OCaml

This repository provides a NATS client SDK for OCaml 5.5 and newer. It combines
an I/O-neutral Core NATS protocol library with a direct-style Eio client for
TCP and TLS connections.

## Packages

| Package | Purpose |
| --- | --- |
| `nats` | Core protocol types, codecs, and the connection state machine. |
| `nats-eio` | Eio support for Core NATS, JetStream, Key-Value, Object Store, and Services. |
| `nats-eio-system` | Optional privileged server and account administration. |
| `nats-eio-opentelemetry` | Optional OpenTelemetry metrics and lifecycle spans. |

## Quickstart

The following example connects to a local server, publishes a message, waits
for the server to process it, and closes the connection:

```ocaml
let or_fail pp = function
  | Ok value -> value
  | Error error -> failwith (Format.asprintf "%a" pp error)

let run env =
  Eio.Switch.run @@ fun sw ->
  let endpoint =
    or_fail Nats.Endpoint.pp_error
      (Nats.Endpoint.of_string "nats://127.0.0.1:4222")
  in
  let connection =
    or_fail Nats_eio.Error.pp
      (Nats_eio.Connection.connect ~sw ~net:(Eio.Stdenv.net env)
         ~clock:(Eio.Stdenv.mono_clock env)
         [ endpoint ])
  in
  let subject = Nats.Subject.literal "hello" in
  or_fail Nats_eio.Error.pp
    (Nats_eio.Connection.publish connection subject "world");
  or_fail Nats_eio.Error.pp (Nats_eio.Connection.flush connection);
  or_fail Nats_eio.Error.pp (Nats_eio.Connection.close connection)

let () = Eio_main.run run
```

This example is also available as
[`examples/core_publish.ml`](examples/core_publish.ml).

## Features

The SDK includes:

- Core publish, subscribe, request/reply, queue groups, headers, and flush;
- authentication, TLS, discovery, reconnect, drain, and lifecycle events;
- JetStream streams, consumers, publishing, and acknowledgements;
- Key-Value and Object Store operations, watches, and streaming transfers;
- NATS Services and service discovery;
- optional system-account administration; and
- optional OpenTelemetry integration.

The Eio API uses direct iteration, structured results, and switch-owned
lifetimes. It does not copy callback-, channel-, or mutable-handle APIs from
other NATS clients.

For exact compatibility claims and known differences from the Go SDK, see the
[support policy](docs/support.md) and
[Go SDK parity audit](docs/go-sdk-parity.md).

## Documentation

- [Development guide](DEVELOPMENT.md): build, test, benchmark, and release
  workflows.
- [NATS by Example repository](https://github.com/ConnectEverything/nats-by-example):
  runnable, cross-client reference examples.
- [Design](docs/design.md): architecture and API decisions.
- [Implementation plan](docs/plan.md): completed phases and remaining work.
- [Support policy](docs/support.md): OCaml and NATS server compatibility.
- [Release evidence](docs/release-evidence.md): recorded acceptance runs.
- [Observability](docs/observability.md): metrics and lifecycle events.
- API reviews: [Core](docs/core-api-review.md),
  [JetStream](docs/jetstream-api-review.md), and
  [durable features](docs/durable-feature-api-review.md).

## Develop

Install the following tools before you begin:

- [Nix](https://nixos.org/) for the OCaml toolchain and dependencies.
- A Docker-compatible container runtime for live-server tests. The scripts call
  the `docker` command; Docker, Colima, or a compatible Podman setup can provide
  it.

Run the local build and deterministic tests:

```sh
nix develop
dune build
dune runtest
```

For live-server tests, benchmarks, and release checks, see the
[development guide](DEVELOPMENT.md).
