# Go SDK capability parity audit

This audit compares the Eio SDK in this repository with the current official Go
SDK pinned by the interop peer: `github.com/nats-io/nats.go v1.52.0`. The Go
version is deliberately pinned so that a later server or SDK release does not
silently change the scope of this document. The comparison is about user-visible
capabilities, not a one-for-one translation of Go method names. Eio's direct
style, switch-owned lifetimes, and typed OCaml values are intentional API
differences.

The primary Go references are the [nats.go repository](https://github.com/nats-io/nats.go),
its current [JetStream package](https://github.com/nats-io/nats.go/tree/v1.52.0/jetstream),
and its [Services package](https://github.com/nats-io/nats.go/tree/v1.52.0/micro).
The repository's existing interop runners provide the behavioral evidence for
the capabilities marked as covered.

## Summary

| Area | Current position | Material gaps |
| --- | --- | --- |
| Core NATS | Covered for the Eio model | WebSocket transport and some Go-specific diagnostics/options |
| Authentication and TLS | Covered | Dynamic callback and transport-option breadth is narrower |
| Reconnect, discovery, drain | Covered | Go-style callback hooks and connection statistics are not mirrored |
| JetStream management | Account info, stream/consumer lookup, upsert, names, reset, detailed lists | Newer stream fields and system-level administration |
| JetStream publishing | Synchronous and switch-owned asynchronous acknowledgements; expectation/retry/TTL/schedule options; atomic and fast batch wire paths | Shared async reply multiplexing and a few newer server-only publish controls |
| JetStream consumption | Pull, push, ordered, heartbeats, flow control, priority, consumer reset | Some ordered/ack fields and Go's continuous batching controls |
| Key-Value | CRUD, CAS, history, finite keys, watches, per-key/marker TTL, purge-delete cleanup, resumable/multi-filter watches, listers | Bucket manager/listers |
| Object Store | Streaming CRUD, links, metadata, watches, list, seal, and Go interop | Bucket manager/listers and file helpers |
| Services | Registration, groups, requests, errors, discovery, stats | Reset/stopped state, pending limits, custom lifecycle/stat callbacks |

## Core and transport

The following Go capabilities have equivalent OCaml behavior, although the
ownership and receive APIs differ:

- Core publish, headers, request/reply, no-responders, queue groups, flush,
  unsubscribe, auto-unsubscribe, drain, and close.
- Ordered configured and discovered server candidates, reconnect backoff and
  jitter, subscription replay, reconnect lifecycle events, and bounded pending
  resources.
- Token, username/password, NKey, JWT/NKey, TLS, server-required TLS, and mTLS
  authentication paths.

Go exposes synchronous subscriptions, callbacks, and channels because those are
natural Go concurrency forms. The OCaml surface uses `Subscription.next`,
`iter`, and Eio switches instead; this is an intentional runtime adaptation,
not a missing capability.

The remaining transport/diagnostic gaps are:

- No WebSocket transport adapter. This is a real capability gap for browser or
  proxy deployments, but it should remain a transport package rather than enter
  the protocol core.
- No direct equivalent of Go's custom dialer, in-process server, WebSocket HTTP
  headers, proxy path, ping/stale-connection tuning, or connection statistics and
  server-introspection methods. Most are adapter-specific or observability
  conveniences and are lower priority than protocol features.

## JetStream management and publishing

### Covered

The OCaml client already covers synchronous publish acknowledgements, message
IDs, stream create/bind/update/list/info/delete, direct message reads by
sequence and subject, subject-filtered purge, consumer create/bind/update/list/
info/delete, pull and push consumers, ordered consumers, explicit/all/no-ack
acknowledgements, redelivery, backoff, filters, replay, idle heartbeats, flow
control, priority groups, pause/resume, and consumer recovery. These are backed
by the current Core request/reply and subscription primitives.

### Partial or missing stream capabilities

The Go `jetstream.StreamConfig` can also express:

- maximum consumers and discard-new-per-subject;
- no-ack streams and duplicate windows;
- mirrors, sources, source filters/transforms, and newer placement metadata/options;
- deny-purge, initial sequence, subject transforms, republish, and mirror-direct
  reads;
- stream-level consumer limits;
- per-message TTL/counter support, scheduled messages, atomic/batched publish
  feature flags, and newer persistence settings.

The OCaml codec preserves unknown fields when reading and read-modify-write
updates preserve fields outside the modeled projection. That prevents data loss,
but it does not make the fields configurable. Ordinary and secure stream message
deletion are now implemented and covered against the Go peer; the remaining
management gaps are the manager-level and newer configuration capabilities
listed above.

The Go SDK also exposes manager-level create/update/upsert operations, stream
and stream-name listers, account information, and consumer reset operations.
The OCaml surface now exposes those operations compositionally through the
JetStream capability and typed stream/consumer handles. Name listers eagerly
collect the server's paged responses into ordered lists, while `bind` remains
the explicit local-handle operation for callers that already know a resource
exists.

### Publishing

The OCaml publisher exposes both synchronous acknowledgements and a
switch-owned asynchronous `Publisher` with bounded pending state, per-future
await/cancel operations, completion waiting, no-responder retries, and stall
timeouts. `Publish_options` covers message IDs, optimistic-concurrency
expectations, retry policy, per-message TTL, and scheduled-message headers.
Publish acknowledgements retain the server stream, sequence, duplicate,
domain, batch, and count fields.

The server-side atomic and fast batch protocols are exposed as separate
`Atomic_batch` and `Batch` operations. They validate reserved control headers,
use the exact `Nats-Batch-*` headers and `$FI` reply grammar, consume fast-batch
flow acknowledgements/gaps/errors, and preserve batch/count metadata. The
stream configuration projection also models `allow_atomic`,
`allow_msg_schedules`, and `allow_batched`.

The remaining publishing optimization is to replace the current per-future
private request subscriptions with a shared wildcard acknowledgement
subscription. That is an allocation/throughput improvement, not a wire
capability gap; the current implementation retains explicit request ownership
and cancellation semantics.

### Consumption gaps

The normal pull/push/ordered workflows are present. The remaining Go-level gaps
are:

- `AckFlowControlPolicy` and the corresponding acknowledgement semantics;
- the separate consumer name field, where it is distinct from a durable name;
- ordered-consumer multi-subject filters, metadata, headers-only mode, inactive
  threshold, reset-attempt limit, and custom name prefix;
- Go's fetch-by-bytes/no-wait and continuously overlapping `Messages`/`Consume`
  helpers. OCaml `fetch` and `iter` cover the basic workflows, but not every
  throughput/control option exposed by Go.

## Key-Value

The OCaml module covers bucket configuration, revisioned get/put/create/update,
compare-and-set delete/purge, exact revision reads, finite key scans, history,
and switch-owned watches with initial markers, delivery policies, delete
filtering, metadata-only delivery, multiple filters, and revision resumption.
It also exposes per-key and purge-marker TTLs, marker age cleanup, and a
switch-owned streaming key lister. The local black-box suite covers the wire
contracts, while the KV interop runner exchanges TTL-bearing values and purge
markers with the pinned Go SDK and validates marker cleanup across SDKs.

The remaining Go KV capability gap is the bucket manager surface: create,
update/upsert, and name/status listers. Go's ordered watcher recovery is also
stronger than the current OCaml watch's ordinary ephemeral-consumer recovery;
the OCaml API documents that distinction rather than presenting a resumable
watch as an ordered consumer.

## Object Store

The OCaml module already has the important data-plane shape: incremental
Bytesrw put/get, digest and chunk verification, metadata updates, ordinary and
bucket links, listing, tombstones, watches, deletion cleanup, status, and
sealing. Its typed link representation is equivalent to the Go split between
`AddLink` and `AddBucketLink`.

The remaining API gaps are bucket-level create/update/create-or-update
distinctions, bucket-name/status listers, and file convenience helpers. The
cross-SDK data-plane contract is now exercised by
`scripts/runtest-interop-object-store.sh` against the pinned Go peer: both SDKs
exchange content and metadata, observe updates, resolve links, inspect list and
tombstone behavior, and validate sealing. The manager and convenience gaps do
not block the data-plane feature set.

## Services

The OCaml service module covers typed identity/configuration, endpoint and group
composition, queue and metadata declarations, successful and error replies,
monitoring discovery, statistics, and service-local stopping. Go additionally
offers `Reset`, `Stopped`, endpoint pending message/byte limits, and custom
statistics, error, and done callbacks. The first two are small state-surface
extensions; the callbacks need an explicit Eio ownership policy and should not be
added as unstructured mutable hooks.

## Prioritized follow-up

1. **Stream configuration expansion.** Add mirrors/sources and their transforms
   before the newer server-only configuration fields.
2. **Transport and service breadth.** Add WebSocket as a separate adapter and
   service reset/stopped/pending-limit behavior after the protocol gaps above.

This ordering keeps the narrow protocol waist intact, gives each addition a
behavioral test target, and avoids claiming parity merely because unknown JSON
fields survive a read-modify-write operation.
