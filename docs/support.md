# Support policy

This document defines the compatibility contract for an initial release of
the OCaml NATS client. It separates API support from the exact server images
used as release evidence.

## Client baseline

- OCaml 5.5 or newer is required.
- `nats` contains the I/O-neutral Core NATS protocol types and state machine.
- `nats-eio` provides the TCP/TLS Eio connection, JetStream, Key-Value,
  Object Store, and Services APIs.
- `nats-eio-system` is an optional privileged system-account administration
  package.
- `nats-eio-opentelemetry` is an optional observability bridge.
- WebSocket and other alternative transports are not release goals. They do
  not affect the transport-neutral protocol core.

## Server compatibility

The release matrix uses one explicit patch release from each selected server
line:

| Server image | Role in the matrix | Support position |
| --- | --- | --- |
| `nats:2.10.22` | Compatibility floor | Tested by this project, but no longer an upstream-maintained server line |
| `nats:2.12.15` | Previous maintained line | Tested |
| `nats:2.14.6` | Current line | Release target; acceptance rerun pending |

The exact candidates live in `scripts/default-server-images.sh`, which is
shared by every version-matrix runner. Other patch releases in these minor
lines are expected to interoperate, but only completed runs against the pinned
images count as reproducible release evidence. Servers older than 2.10 are not
supported. Newer servers are best-effort until their release delta and
acceptance matrix have been audited.

NATS server support policy is described in the
[official release documentation](https://docs.nats.io/running-a-nats-service/nats_admin/upgrading_cluster).
The current server pin is tied to the
[2.14.6 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.6).

## Feature availability

Core NATS operations do not require JetStream. JetStream features are accepted
or rejected by the server. The client validates local invariants but does not
infer server policy from a version string or silently rewrite a requested
feature.

| Feature | Minimum server line |
| --- | --- |
| Stream metadata, compression, initial sequence, subject transforms, and consumer limits | 2.10 |
| Consumer pause and priority overflow/pinned policies | 2.11 |
| Per-message TTL and subject delete-marker TTL | 2.11 |
| Prioritized consumer policy | 2.12 |
| Atomic publishing, message counters, scheduled publishing, and persistence mode | 2.12 |
| Fast batch publishing and acknowledgement flow-control policy | 2.14 |

These minima follow the official
[stream](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md)
and
[consumer](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md)
references. A server can still reject a feature because of account limits,
permissions, topology, or local configuration.

Unsupported server behavior is returned as a structured JetStream API error,
including its HTTP-like code, NATS error code, description, and unrecognized
metadata. `INFO.version` remains available for diagnostics; it is not a
negotiated capability object.

Stream updates preserve fields returned by the server. Default values for
version-gated stream fields are omitted when an older server did not return
them, so an unrelated update does not accidentally request a newer feature.
One-way stream capabilities that the server reports as enabled are preserved
because the server does not permit disabling them. Consumer updates likewise
omit an absent priority timeout rather than injecting a newer field with a
zero value.

## Reference SDK

Cross-SDK tests build the Go peer against `github.com/nats-io/nats.go v1.53.1`.
The capability comparison is maintained in
[`go-sdk-parity.md`](go-sdk-parity.md). OCaml APIs intentionally use direct
style, Eio switch ownership, and structured results instead of copying Go
callbacks, channels, or mutable handles.

The reference pin is updated deliberately: review the upstream release notes,
audit new protocol-visible behavior, update the peer and Nix vendor hash, and
run the applicable interop matrix before changing the parity claim.

## Release evidence

A release candidate must pass:

1. the local build, unit/property tests, format check, API documentation build,
   shell checks, and Go peer check;
2. the live single-server Core, Services, JetStream, Key-Value, Object Store,
   authentication, and TLS matrix;
3. the cross-SDK Go peer matrix; and
4. the bounded cluster failure matrix, including fixed-node, leader, restart,
   multi-node, changed-advertisement, and management-unavailability scenarios.

The matrix is evidence for the documented contracts, not a claim of complete
NATS server conformance. Release notes must identify any gate that was not run
or any scenario that remains environment-blocked.
