# Go SDK capability parity audit

This audit compares the Eio SDK in this repository with the official Go SDK
pinned by the interop peer: `github.com/nats-io/nats.go v1.52.0`. Pinning the
reference makes the scope reproducible; a later server or SDK release must be
audited separately. The comparison is about user-visible capabilities, not a
one-for-one translation of Go method names. Eio's direct style, switch-owned
lifetimes, and typed OCaml values are intentional API differences.

The primary references are the [nats.go repository](https://github.com/nats-io/nats.go),
its [JetStream package](https://github.com/nats-io/nats.go/tree/v1.52.0/jetstream),
and its [Services package](https://github.com/nats-io/nats.go/tree/v1.52.0/micro).
The repository's interop runners provide behavioral evidence for the rows
marked as covered.

## Status at a glance

| Area | Current position | Material gaps or deliberate differences |
| --- | --- | --- |
| Core NATS | Covered for the Eio model | Go-specific dialers, proxy knobs, diagnostics, and alternative transports are not part of the Eio surface |
| Authentication and TLS | Covered | The callback/dialer breadth is narrower because credentials and TLS flows are explicit values |
| Reconnect, discovery, drain | Covered | Lifecycle callbacks use the Eio event stream; cumulative message and reconnect statistics are exposed as race-safe snapshots rather than mutable Go-style fields |
| JetStream management | Covered for the material v1.52.0 surface | No material protocol gap; future server-only fields remain an acceptance concern |
| Server-wide administration | Monitoring and selected controls covered by the optional `nats-eio-system` package | This privileged `$SYS` surface is separate from JetStream and outside the pinned Go JetStream package parity claim; claims, resolver, and user-management operations remain out of scope and explicit system-account authorization is required |
| JetStream publishing | Covered, including async futures, retries, TTL/schedule headers, atomic and fast batches | Shared async acknowledgement multiplexing is a throughput optimization, not a capability gap |
| JetStream consumption | Covered: pull, push, ordered, fetch, no-wait, heartbeats, flow control, priority, and continuous consumption | Go callback/channel receive shapes and threshold/error-handler tuning are represented by direct Eio iteration and structured results |
| Key-Value | Covered: CRUD, CAS, history, ordinary and ordered watches, listers, managers, policy fields, composition, TTL, purge-marker cleanup, the full 45-cell authenticated seed/leader/restart matrix, and the expanded 15-cell anonymous fixed-node/leader/restart matrix across three pinned releases | Ordinary and ordered watch behavior remain separate contracts; broader authenticated multi-node-loss and changed-advertisement evidence remains acceptance work |
| Object Store | Covered: streaming CRUD, links, metadata, watches, listing, sealing, managers, file helpers, Go interop, and the expanded 15-cell anonymous fixed-node/leader/restart cluster matrix; the six-mode single-server authentication/TLS matrix passes all 18 pinned-release cases | No material data-plane gap; authenticated multi-node-loss and changed-advertisement matrices remain acceptance work |
| Services | Covered for the pinned `micro` surface and lifecycle matrices | Future server/SDK versions and transport-specific integration hooks remain separate work |

## Core, authentication, and transport

The following Go capabilities have equivalent OCaml behavior, although the
ownership and receive APIs differ:

- Core publish, headers, request/reply, no-responders, queue groups, flush,
  unsubscribe, auto-unsubscribe, drain, and close;
- ordered configured and discovered server candidates, reconnect backoff and
  jitter, subscription replay, reconnect lifecycle events, and bounded pending
  resources; and
- token, username/password, NKey, JWT/NKey, TLS, server-required TLS, and mTLS
  authentication paths.

Reconnect follows the Go client's observable boundary: new publishes use a
bounded 8 MiB pending buffer by default and are flushed after the replacement
handshake and subscription replay; subscriptions created during recovery are
registered in local replay state; existing request waiters remain pending but
already-written request publishes are not re-emitted; and flushes already
waiting at the transport loss fail while new flushes can be queued. The OCaml
facade closes and returns `Connection_reconnecting` for a connection drain
requested during recovery. Subscription drains remain pending across a
transient transport loss and are re-established after the replacement
subscription replay. The OCaml facade does not attempt mutation reconciliation
or idempotency inference.

Go exposes synchronous subscriptions, callbacks, and channels because those
are natural Go concurrency forms. The OCaml surface uses `Subscription.next`,
`iter`, and Eio switches instead. This is a runtime adaptation, not a missing
wire capability.

Alternative transports, including WebSocket, are intentionally out of scope
for the current Eio-only SDK. The protocol core remains transport-neutral so a
future adapter can be added without changing the protocol semantics. Go-only
custom dialers, in-process servers, proxy headers, stale-connection tuning, and
server-introspection helpers are also not mirrored; they are adapter
conveniences rather than required NATS wire operations. Eio exposes the Go
baseline message and reconnect counters through `Connection.stats` and the
`Connection.Stats` accessors, while retaining direct-style ownership and
immutable snapshots.

## JetStream management

The OCaml client covers the material management surface exposed by the pinned
Go JetStream package:

- account information and domain-tier usage;
- stream create, bind, lookup, create-or-update, update, list, name listing,
  subject lookup, direct message reads, purge, ordinary and secure message
  deletion, and delete;
- consumer create, bind, lookup, create-or-update, update, list, name listing,
  pause/resume, reset, reset-to-sequence, and delete; and
- read-modify-write preservation of unknown fields in stream, source,
  transform, republish, external, and consumer configuration objects.

The typed stream configuration includes mirrors, sources and transforms,
republish, placement, compression, per-subject limits, consumer limits,
message TTL and counters, persistence mode, atomic and scheduled publishing,
fast batch publishing, direct reads, rollup, deletion policy, initial
sequence, and the other material v1.52.0 stream fields. Configuration
constructors enforce the local invariants before a request is sent.

This is a capability over Core NATS request/reply, not a second transport.
Name listers collect paged responses into ordered OCaml lists; callers that
already know a resource can use `bind` without an existence request.

This resource-management surface is not general server administration. NATS
server-wide monitoring and operational control use privileged system-account
subjects such as `$SYS.REQ.SERVER.<server-id>.*` and
`$SYS.REQ.ACCOUNT.<account-id>.*`, with separate versioned response schemas.
The optional `nats-eio-system` package now models those subjects separately,
including selectors, endpoint-specific monitoring options, targeted and
fan-out monitoring, reload, client kick/LDM, and classified system events. It
keeps complete JSON response values so version-specific monitor fields remain
available. Claims, resolver, and user-management operations remain outside
this package. See the [NATS system-account reference](https://github.com/nats-io/nats.docs/blob/master/running-a-nats-service/nats_admin/jwt.md).

## JetStream publishing

The publisher exposes synchronous acknowledgements and a switch-owned
asynchronous publisher with bounded pending state, per-future await/cancel,
completion waiting, no-responder retries, and stall timeouts. Publish options
cover message IDs, optimistic-concurrency expectations, retry policy,
per-message TTL, and scheduled-message headers. Acknowledgements retain the
server stream, sequence, duplicate, domain, batch, and count fields.

The server-side atomic and fast batch protocols are exposed as separate
`Atomic_batch` and `Batch` operations. They validate reserved control headers,
use the exact `Nats-Batch-*` headers and `$FI` reply grammar, consume fast-batch
flow acknowledgements/gaps/errors, and preserve batch/count metadata.

The current implementation gives each asynchronous publish future explicit
request ownership. Replacing those private reply subscriptions with one
shared wildcard acknowledgement subscription could reduce allocation and
subscription churn, but would not add a user-visible NATS capability.

## JetStream consumption

The normal delivery forms are covered:

- one-shot pull fetches, including max-bytes and no-wait requests;
- persistent pull sessions with bounded fetches, expiry, idle heartbeats,
  flow control, priority-group controls, timeout/resumption, and explicit
  cleanup;
- push sessions with queue groups, acknowledgements, heartbeats, flow-control
  responses, timeout/resumption, and reconnect restoration; and
- ordered sessions with client-managed ephemeral consumers, filters,
  headers-only delivery, reset limits, metadata/name-prefix controls, sequence
  validation, and recovery from gaps, liveness loss, deletion, or disconnect.

`Consumer.Consume` provides the continuous pull workflow as a switch-owned,
bounded queue with `next`, `iter`, `stop`, `drain`, and `close`. It accepts
message/byte bounds and a stop-after count while applying Eio backpressure.
That direct result/iteration shape replaces Go's callback and channel
variants. Go's `ThresholdMessages`, `ThresholdBytes`, and asynchronous error
handler are intentionally not copied as separate protocol abstractions:
queue capacity and the result returned by `next`/`iter` provide the same
backpressure and error ownership without hiding failures in a callback.

The acknowledgement policy includes flow control, and push/pull sessions
implement the corresponding control traffic. There is therefore no remaining
ack-flow-control capability gap; the difference is only the shape of the
consumer handle and error path.

## Key-Value

The OCaml module covers revisioned get/put/create/update, compare-and-set
delete/purge, exact revision reads, finite key scans, history, purge-marker
cleanup, and switch-owned ordinary and ordered watches with initial markers,
delivery policies, delete filtering, metadata-only delivery, multiple filters,
and revision resumption. Per-key and purge-marker TTLs are represented
explicitly.

`Key_value.Manager` covers account-wide create, update, create-or-update,
open, delete, name listing, and status listing. `Config` and `Status` project
description, history, limits, storage, replicas, placement, compression,
metadata, republish, mirrors, and sources onto the backing stream. The
existing typed JetStream source/republish codecs remain the single owner of
their wire representation; a KV mirror is represented with no local capture
subjects, and mirror/source combinations are rejected at the bucket boundary.

Go's convenience layer accepts bare bucket names when constructing mirror and
source relationships. The OCaml source value accepts an explicit backing
stream name and preserves caller-supplied transforms, so another bucket is
referenced as its `KV_` stream name. This is an intentional precision tradeoff
in favor of preserving the underlying JetStream composition model.

`Key_value.Watch` remains the ordinary, revision-resumable watch contract.
`Key_value.Ordered_watch` is the explicit stronger mode: it composes
`Consumer.Ordered` with bucket entry decoding, emits an initial marker, and
preserves filters, headers-only delivery, metadata, replay/heartbeat settings,
name prefixes, and reset limits while recovering from consumer-sequence gaps,
missing heartbeats, deletion, and non-replayed disconnects. It resumes each
new generation at the next expected stream revision. This separation keeps
the ordinary watch's duplicate and loss semantics stable.

The local mock suite covers retained/live and empty snapshots, metadata-only
delivery, and deterministic gap recovery. Live-server and cross-SDK failure
tests for the wrapper remain acceptance work; the lower-level ordered consumer
already has the corresponding live recovery harness.

The pinned Go interop runner covers revisions, stale CAS, tombstones, watches,
purge markers, and cleanup across the supported single-server matrix.

## Object Store

The OCaml module has the complete material data-plane shape: incremental
Bytesrw put/get, digest and chunk verification, metadata updates, ordinary and
bucket links, listing, tombstones, watches, deletion cleanup, status, sealing,
and bucket policy updates with read-modify-write preservation.

`Object_store.Manager` adds account-wide create, update, create-or-update,
open, delete, name listing, and status listing. `put_file` and `get_file`
provide the file conveniences while retaining incremental transfer and
structured filesystem errors.

The `scripts/runtest-interop-object-store.sh` runner exercises the pinned Go
peer for content and metadata exchange, updates, links, listing, tombstone
behavior, and sealing. Its matrix wrapper passes the same six anonymous,
token, and username/password plaintext/TLS modes across the three pinned
releases. The dedicated anonymous cluster runner also covers fixed
node-a/node-b/node-c loss, elected-leader loss, and durable seed restart across
all 15 pinned server-version/mode cells. Authenticated multi-node-loss and
changed-advertisement cases remain separate acceptance work rather than an
unimplemented Object Store API.

## Services

The OCaml service module covers typed identity/configuration, endpoint and
group composition, queue and metadata declarations, successful and error
replies, monitoring discovery, statistics, reset, stopped-state inspection,
pending message/byte limits, custom lifecycle/stat callbacks, and bidirectional
Go interop. The callbacks are immutable service options and are dispatched by
the service-owned Eio lifecycle; they are local API behavior and do not appear
on the NATS service wire.

The dedicated Service matrix covers the pinned server releases and the
anonymous, token, username/password, NKey, JWT, mTLS, and supported TLS
variants. Focused runners also cover reconnect, controlled subscription
failure, and parent-connection closure. Future server and SDK versions remain
acceptance work.

## Remaining work

The material Go parity gaps identified by the original audit are closed. The
remaining work is operational confidence and intentionally separate adapter
surface:

1. Extend the established live-server and Go-peer matrices to more cluster
   failure topologies and newer server/SDK releases.
2. The optional `nats-eio-opentelemetry` package now bridges the dependency-free
   observability boundary to cumulative connection metrics and payload-free
   lifecycle spans. Message context propagation and redaction remain
   application-owned rather than being inferred by the client.
3. Re-audit future JetStream fields and server feature gates without silently
   changing the pinned parity claim.
4. Keep alternative transports such as WebSocket out of this project scope
   unless that goal is explicitly changed.

These are acceptance and product-scope decisions, not unimplemented Core,
JetStream, KV, Object Store, or Services wire capabilities in the current
Eio surface.

The privileged system-account extension is intentionally separate from the
`nats-eio` JetStream parity claim. Remaining work there is broader live
cluster/failure coverage and any future operator-specific claims or
user-management APIs, not the monitoring/control slice documented above.
