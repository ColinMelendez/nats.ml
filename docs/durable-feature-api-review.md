# Key-Value and Object Store API stability review

This review covers the public Key-Value and Object Store surfaces in
`nats-eio`: bucket capabilities, revisioned entries, streaming transfers,
watch ownership, metadata commit behavior, structured errors, and cancellation.
It closes the API-shape portion of G5. It does not claim that the remaining
authenticated, multi-node, and changed-advertisement cluster/failure matrices
have passed.

## Method

The public `.mli` files were read together with their implementations, callers,
mock-transport tests, and the pinned NATS server behavior. The comparison point
was the official Go SDK pinned by this repository (`nats.go v1.52.0`) and the
server releases used by the acceptance matrix (`2.10.22`, `2.12.15`, and
`2.14.5`). The review questions were:

- Which values own a server consumer, a subscription, a worker, or a transfer?
- What does switch release, explicit close, timeout, and cancellation do to a
  blocked operation?
- Which revision and snapshot boundaries are observable to the caller?
- Does Object Store metadata remain the commit point for chunked content?
- Can failures leave a caller with an ambiguous partial result?

An OpenCode review was requested for the narrow Object Store changes in this
slice, but it returned no report within the ten-minute wait window. No outside
agent output is treated as evidence here; the conclusions below are based on
the source, tests, and pinned wire behavior.

## Public ownership model

| Surface | Ownership | Contract |
| --- | --- | --- |
| `Key_value.t`, `Object_store.t` | Borrowed capability over a JetStream connection | The handle names one bucket. It does not own a connection or a backing stream; bucket creation and deletion are explicit operations. |
| `Entry.t`, `Object_store.Info.t`, `Meta.t` | Immutable data values | They carry revisions, metadata, content identity, and policy values. They do not own subscriptions, files, or server resources. |
| `Key_value.Watch.t` | Caller switch owns an ephemeral push consumer and subscription | `next` is single-owner. `close` stops delivery and cleans up the owned consumer; switch release performs the same cleanup through the underlying session. |
| `Key_value.Ordered_watch.t` | Caller switch owns ordered-consumer generations | Consumer recreation, gap detection, and resume-at-next-revision are internal. The public handle remains one watch with one `next` ownership rule. |
| `Key_value.Key_lister.t` | Caller switch owns the underlying watch | The lister converts the retained snapshot into keys and returns `Ok None` at its initial boundary. |
| `Object_store.Watch.t` | Caller switch owns an ephemeral push consumer and subscription | Retained metadata is delivered before one `Initial_done`; a new-only watch emits the marker immediately. An empty retained snapshot also emits it immediately. |
| `Bytesrw.Bytes.Reader.t` passed to `put` | Caller owns the reader; the operation consumes it | The upload reads incrementally until end-of-data or error. It does not turn the reader into a long-lived library resource. |
| `Bytesrw.Bytes.Writer.t` passed to `get` | Caller owns the writer; the operation writes to it | Bytes are written incrementally. End-of-data is written only after size, chunk-count, digest, and subject checks succeed. |
| `Manager` operations | No long-lived ownership | Account-wide create, update, delete, names, and status operations use request/reply and return snapshots or capabilities. |

The narrow waist is therefore the existing JetStream request and delivery
layer. KV and Object Store add typed subjects and commit/revision rules without
introducing a second connection owner or a bucket-wide manager object.

## Representative callers

### Revisioned Key-Value mutation and watch

```ocaml
let revision = expect (Key_value.put bucket key "ready") in
let watch =
  expect
    (Key_value.Ordered_watch.v ~sw ~resume_from_revision:(Int64.succ revision)
       bucket)
in
match Key_value.Ordered_watch.next watch with
| Ok (Key_value.Ordered_watch.Entry entry) -> handle entry
| Ok Key_value.Ordered_watch.Initial_done -> begin_snapshot_done ()
| Error error -> report error
```

`Entry.revision` is the JetStream stream sequence, not a client-local counter.
Compare-and-set operations send the expected revision to the server and return a
structured mismatch when the server rejects it. A watch resumes inclusively at
the requested stream revision; callers can pass the last processed revision plus
one when replaying after a durable checkpoint.

### Incremental Object Store transfer

```ocaml
let info =
  expect
    (Object_store.put bucket meta
       (Bytesrw.Bytes.Reader.of_string payload))
in
let writer = Bytesrw.Bytes.Writer.of_buffer buffer in
ignore (expect (Object_store.get bucket (Object_store.Info.name info) writer))
```

The string helpers are convenience wrappers over the same reader/writer waist.
The primary API does not require an object to fit in one string or one mutable
buffer.

## Streaming, backpressure, and cancellation

KV watches and Object Store watches are pull-based. `next` blocks the caller
until an event, error, or closure; `iter` is a direct-style convenience over the
same single-owner operation. The underlying Eio session owns the subscription
queue and server consumer. This makes backpressure visible at the call site and
keeps user callbacks out of the protocol owner.

Object transfers use `Bytesrw` directly. `put` publishes bounded chunks and
publishes the metadata record only after the reader reaches end-of-data. `get`
reads chunks through an ordered consumer and writes them as they arrive. A
supplied timeout is one absolute operation deadline covering metadata requests,
link resolution, chunk delivery, verification, and cleanup; recovery cannot
silently extend it.

The writer contract is intentionally streaming rather than transactional: if a
metadata, subject, size, chunk-count, digest, or transport check fails after
some chunks have arrived, the writer may contain a valid prefix and does not
receive end-of-data. `get_file` opens the destination with truncation before
streaming, so a failed verification may leave a partial file. Callers that need
atomic replacement should stream to a temporary path and rename it after a
successful result.

Explicit `close` reports cleanup errors. Switch-release cleanup is still
best-effort at the lower JetStream session boundary, so callers that require
confirmation must call `close` and inspect its result. A timeout leaves a watch
open; it does not silently recreate or invalidate the public handle.

## Revision and snapshot semantics

Key-Value operations expose server revisions and operations directly:

- `put` appends a value and returns its stream revision;
- `create_key` and `update` use server compare-and-set rules;
- `delete` and `purge` append typed tombstones, with optional expected
  revisions; and
- `history` returns retained entries oldest first, including tombstones.

Ordinary watches preserve delivery order observed by the push session but do not
claim gap detection across reconnect. `Ordered_watch` is a deliberately
separate contract: it validates ordered consumer continuity and recreates the
consumer at the next expected stream revision after a gap, missing heartbeat,
deletion, or non-replayed disconnect. This prevents callers from accidentally
assuming ordered recovery from the weaker ordinary watch API.

Both KV and Object Store watches count retained deliveries when deciding when
to emit `Initial_done`, including entries filtered from the application result by
`ignore_deletes`. The server's pending count is the primary boundary; the final
message's `num_pending = 0` is the fallback. Empty retained snapshots emit the
marker without waiting for a future live message when the setup response reports
zero pending deliveries.

Object Store content has a separate revision boundary. Chunk subjects are
written under a fresh object NUID, then the rolled-up metadata record is
published as the commit point. The resulting record is read back before the
operation succeeds. Only after the new metadata is committed does the client
purge superseded chunks. A cleanup failure is returned as
`Error.Cleanup_failed` together with the committed object information.

Deletion publishes a tombstone with zero content counters and then purges the
old NUID. Repeating deletion of an existing tombstone is idempotent: it
republishes the tombstone and retries the purge. Replacing a tombstone likewise
purges the old NUID after the replacement metadata commits, so failed earlier
cleanup cannot leave stale content indefinitely.

## Alternatives considered

| Question | Considered | Decision and reason |
| --- | --- | --- |
| Transfer shape | Whole-object strings vs `Bytesrw` reader/writer | `Bytesrw` is the primary shape. It preserves bounded transfer and lets callers choose file, buffer, or network flow ownership. |
| Watch shape | Callback registry or one mutable bucket manager vs owned pull handles | Pull handles. Ownership, cancellation, and backpressure are visible and compose with Eio switches. |
| Recovery | Strengthen every watch to ordered recovery vs separate modes | Separate ordinary and ordered contracts. Ordered recovery has meaningful duplicate/loss and consumer-lifecycle costs that should not silently change ordinary watch behavior. |
| Listing | Return a live stream by default vs collect the metadata snapshot | `Object_store.list` returns an eager best-effort snapshot; `Watch` remains available when the caller needs live events. This keeps the common administrative operation simple and makes its memory cost explicit in the implementation. |
| Metadata commit | Expose chunks as committed content vs roll up metadata first | Metadata is the commit point. Readers never treat an uncommitted chunk prefix as an object, and replacement cleanup can be ordered after the new record is visible. |
| Cleanup errors | Hide purge failures vs return the committed value plus a structured error | Return `Cleanup_failed`. The content commit and cleanup outcome are distinct facts that callers may need to retry or audit. |

## Resolved findings

- Object Store watch setup now retains the server-reported pending count and a
  received count, with the delivery frame's pending field as a fallback. The
  empty-list path is covered by a mock-transport regression test.
- Repeated Object Store deletion no longer returns early for a tombstone. It
  republishes the deletion marker and retries purging its NUID; the behavior is
  covered by a focused regression test.
- Replacement cleanup purges any prior non-empty NUID, including a tombstone's
  NUID. This keeps cleanup ownership tied to content identity rather than the
  current visibility flag.
- Object Store stream read-modify-write updates omit version-gated default
  fields when the server's current INFO did not contain them. This preserves
  compatibility with older pinned servers without inventing unsupported fields.

## Deliberate follow-ups

The API review does not close the remaining operational evidence work:

- broader authenticated, multi-node, and changed-advertisement KV and Object
  Store cluster/failure-injection coverage remains outside the current matrix;
- ordinary watch reconnect remains weaker than ordered-watch recovery by design;
- a list is a best-effort snapshot while metadata may change concurrently;
- manager status/name listers eagerly retain their result lists; and
- the package still relies on structured server errors rather than exposing a
  proactive feature/version capability projection.

These are documented contracts or acceptance tasks, not unresolved ownership or
revision ambiguities in the current public API.

## Review result

The public KV and Object Store shapes are suitable for the next documentation
and acceptance phase. Streaming ownership, cancellation, snapshot boundaries,
revision semantics, metadata commit ordering, and cleanup outcomes are explicit
in the signatures and implementation. G5 is not a claim that every cluster
topology or future server version has passed.

## Evidence

- The focused Eio suite covers the KV and Object Store mock contracts, including
  retained/live and empty snapshots, ordered recovery, incremental transfers,
  replacement ordering, deletion tombstones, and tombstone cleanup retry.
- The full local build, documentation check, and test suite pass with the same
  implementation.
- The cross-SDK Object Store matrix passes anonymous, token,
  username/password, and server-required TLS variants across the three pinned
  NATS server releases. The dedicated anonymous cluster matrix also passes
  seed loss, elected-leader loss, and durable seed restart across all three
  pinned releases; broader topology coverage remains in `plan.md`.
- Reference behavior: [nats.go v1.52.0 Object Store implementation](https://raw.githubusercontent.com/nats-io/nats.go/v1.52.0/jetstream/object.go)
  and the [NATS JetStream Object Store documentation](https://docs.nats.io/using-nats/developer/develop_jetstream/object_store).
