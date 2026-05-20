# Implementation plan

## Status

This plan follows the research and design review in
[`docs/design.md`](docs/design.md). It incorporates an independent outside
review of that document. The review confirmed the core direction and surfaced
five corrections that are now treated as prerequisites:

1. NATS subscription ids are allocated by the client, not the server.
2. The application `Message` must be distinct from a wire-operation AST.
3. Protocol deliveries must be separate from lifecycle events.
4. Core NATS must receive the first API-stability gate; JetStream and its
   dependent APIs must not be promised as stable at the same time.
5. `Protocol` is not a separate public concept; the protocol waist is
   `Packet -> Op -> Client`.

The long-term goal is a modern, feature-complete NATS SDK. The first stable
release is deliberately smaller: Core NATS plus an Eio connection facade. The
remaining NATS product surface is added behind later stability gates.

NATS Streaming/STAN is not part of this plan.

### Current checkpoint

The repository scaffold, wire core, and immutable client transition slice are
in place. The
implementation currently provides validated subjects, filters, queue groups,
headers, and messages; the closed wire-operation vocabulary; CRLF and payload
framing over a caller-owned `Bytesrw.Bytes.Reader.t`; and a phase-blind codec.
The codec intentionally leaves `INFO`/`CONNECT` JSON opaque and its framing
errors stop the stream. `Nats.Client` now parses typed INFO, requires an
explicit CONNECT transition, allocates subscription ids, preserves HMSG status,
and exposes bounded incoming/timer transitions. The first Eio vertical slice
is now present as a separate `nats-eio` package:
it owns a serialized protocol fiber, a non-blocking pending-input buffer,
direct-style publish/subscribe/request/flush/drain/close operations, bounded
subscription and event streams, EOF/error shutdown, per-request inboxes,
structured no-responders results, local request deadlines, and
cancellation-safe waiter cleanup. Subscription handles also provide a
server-barrier drain that preserves queued terminal delivery, coalesces
concurrent callers, and keeps late PONGs associated with their original
barriers. Mock-transport coverage exercises fragmented/coalesced frames,
replies, no responders, timeouts, cancellation, sibling delivery during drain,
and multi-barrier ordering. The Eio adapter now also negotiates TLS-required
servers through a replaceable flow and reader lifecycle: it bounds the TLS
handshake, rejects buffered plaintext, waits for the post-TLS `INFO`, and
reports structured TLS/timeout/close failures. Transport recovery with
configurable retry/backoff is now implemented: the first redial is immediate,
later attempts use capped exponential waits, `Some 3` is the default attempt
limit, `None` permits unlimited attempts, and `Some 0` disables recovery.
Validated endpoint seeds and candidate selection are now part of that Eio
surface: each dial pass resolves every candidate again, tries all returned
stream addresses, prefers the endpoint that connected, and rotates failed
endpoints. Each `INFO.connect_urls` replaces the discovered set while
configured seeds remain sticky; both full endpoint URLs and bare `host[:port]`
advertisements are accepted. Initial handshake failures fail over across the
remaining configured seeds. Explicit `tls://` endpoint dialing now performs a
bounded TLS handshake before the NATS handshake, and bare advertisements
inherit the active session scheme. The caller-owned TLS configuration supplies
peer identity and SNI; the existing server-required TLS upgrade path remains
available through the same configuration. Reconnect jitter is configurable,
zero by default, and applied only to delayed retries. An opt-in Docker-backed
real-server acceptance harness now covers single-server pub/sub, headers, queue
groups, request/reply, no-responders, flush, close, and optional
username/password authentication; cluster, TLS, and reconnect acceptance remain
ahead of the G2 stability gate. Authentication capabilities now cover anonymous,
token, username/password, NKey, and JWT credentials; nonce signing is repeated
for every INFO, while private-key parsing and NKey/JWT server acceptance remain
later work. The JetStream foundation now adds a typed, resource-free capability
over the connection, stream configuration/info and create/bind/info/delete
operations, publish acknowledgements with message ids, API error envelopes, and
an opt-in real-server acceptance path for management, deduplication, and
cleanup. The consumer slice now includes consumer management, one-shot fetch,
typed message acknowledgements, and a persistent single-owner
`Consumer.Pull` session with batch accounting, local timeout/resumption,
server-expiry retries, structured terminal statuses, and switch-owned cleanup.
KV, Object Store, Services, and the remaining push/ordered/heartbeat consumer
features remain later work.
The recovery bridge
preserves live subscription handles, queues, and replay intent; fails
transport-bound requests, flushes, and drains; redials through the stored
connection seam; replays INFO/TLS/CONNECT and subscriptions; emits
non-terminal `Disconnected`/`Reconnected` events; and defers unsubscribe
commands until the replacement session is ready. It does not replay
arbitrary publishes or pending requests.

## Working principles

These are implementation invariants, not optional preferences.

### Pure protocol core

The `nats` package has no dependency on Eio, Lwt, Unix, TLS, DNS, or a socket
implementation. Its load-bearing transition is:

```text
incoming bytes -> Packet -> Op -> Client.t
                         Client.t -> wire output + events + deliveries
```

`Client.t` is immutable and opaque. It contains protocol state—connection
phase, server information, client-assigned subscription ids, subscription
intent, negotiated limits, and discovered server candidates—but never a
socket, fiber, mutex, clock, mutable buffer, random generator, or callback.

Time is passed to `timer`/`next_timeout`. Randomness is not needed by the Core
NATS state machine: inbox names and reconnect jitter are adapter concerns. Any
future core randomness must be an explicit input, never stored PRNG state.

### Clear protocol layers

The public core vocabulary is intentionally split:

- `Nats.Subject` and `Nats.Subject.Filter` validate ordinary subjects and
  wildcard filters;
- `Nats.Queue_group` validates queue group names;
- `Nats.Header` and `Nats.Message` model application data;
- `Nats.Op` is the closed wire AST for `INFO`, `CONNECT`, `PUB`, `SUB`, `MSG`,
  `PING`, and the other protocol operations;
- `Nats.Packet` owns CRLF framing, payload lengths, and incremental reader
  progress;
- `Nats.Codec` maps `Op` values to and from bytes without knowing connection
  phase;
- `Nats.Client` validates operations against connection state and owns the
  client-side protocol invariants.

There is no public `Nats.Protocol` module. The top-level `.mli` is curated;
implementation modules and intermediate decode types are not re-exported by
default.

### Explicit output channels

The pure transition must expose three different results:

```text
wire output       bytes the adapter writes to the peer
events            lifecycle/protocol observations
deliveries        { sid : int; message : Nats.Message.t }
```

Deliveries are not events. They have separate consumers and backpressure
policies. A transition consumes at most one complete operation from the
caller-owned `Bytesrw.Bytes.Reader.t`; the adapter loops around that bounded
step. Incomplete input remains in that reader, never in `Client.t`.

### Runtime ownership

`Nats_eio.Connection.t` owns a single serialized protocol owner. Concurrent
application operations enqueue commands; only the owner updates `Client.t`.
The connection owns sockets, TLS, dialing, reconnect waits, request waiters,
bounded subscription queues, the bounded lifecycle-event stream, and cleanup.

The default subscription path is pull-based (`next`/`iter`). Callback helpers,
if added, are adapter conveniences over the same owned subscription. No user
callback runs in the pure core.

### Semantics that must be fixed early

- `SUB` ids are allocated by `Client` and replayed with subscription intent.
- `max_payload` from `INFO` is enforced before emitting a publish.
- Header negotiation is enabled by default for the modern Core profile.
- `flush` is a `PING`/`PONG` server barrier, not local socket-write success.
- Core publishes are not silently replayed after reconnect.
- Requests complete exactly once as response, no responders, timeout,
  cancellation, disconnect, or structured server error.
- Connection drain and subscription drain are distinct operations.
- The default slow-consumer policy never silently drops messages.
- JetStream errors are defined in JetStream modules, not in the core error
  variant.

## Product waves and gates

| Wave | Scope | Stability gate |
| --- | --- | --- |
| A | Pure protocol core and Core NATS over Eio | G2: Core API stable |
| B | JetStream management, publishing, and consumers | G4: JetStream API stable |
| C | Key-Value and Object Store | G5: durable-feature APIs stable |
| D | Services over Core NATS | G6: Services API stable; may start after G2 |
| E | Operational polish, extra transports, and extra runtimes | Explicitly optional |

Each gate requires its acceptance criteria, a complete staged diff review, and
an outside design review before a stability claim or other major API decision.
No commit is implied by this plan.

## Phase 0 — Architecture freeze

### Goal

Turn the research design into a small, internally consistent Core NATS
contract before implementing protocol behavior.

### Work

- Keep the corrected layering in `docs/design.md`.
- Write a one-page module/data-flow diagram for
  `Bytesrw.Reader -> Packet -> Op -> Client -> output`.
- Define the first signatures for `Nats.Subject`, `Filter`, `Queue_group`,
  `Header`, `Message`, `Op`, `Packet`, `Codec`, `Client`, `Error`, and `Event`.
- Define the `Client.command`, `Client.delivery`, and `Client.transition`
  shapes; do not leave delivery routing implicit.
- Define the phase sum for `Client.t` and the ownership boundary between
  protocol state and Eio resources.
- Define Core errors separately from future JetStream errors.
- Decide the initial header/version profile, configured/discovered server
  representation, `max_payload` handling, and no-replay policy.
- Produce three to five signature-level caller examples: Core pub/sub,
  request/reply, queue worker drain, and pure transition driving.

### Exit criteria

- The module graph has no orphan `Protocol` concept.
- `Message`/`Op` responsibilities are unambiguous.
- `Client.incoming` has explicit wire/event/delivery outputs.
- Client-assigned sid allocation and reconnect intent replay are specified.
- The reader ownership and no-buffer-in-state rules are written next to the
  transition signature.
- The Core API can be reviewed without knowing the Eio implementation.

### Gate G0 — outside architecture review

Before protocol implementation begins, ask an independent reviewer to inspect
the signatures, module graph, invariants, and caller snippets. Resolve every
high-severity issue before moving to Phase 1.

## Phase 1 — Pure Core NATS vocabulary and wire codec

### Goal

Build a deterministic `nats` package that can parse and produce Core NATS wire
operations without a network or runtime dependency.

### Workstream 1A — Domain values

- Implement result-returning constructors and programmer-literal constructors
  for subjects, filters, and queue groups.
- Enforce ordinary-subject and wildcard-filter rules, including `>` only in the
  final position and no empty tokens.
- Implement immutable multi-valued headers with case-insensitive lookup,
  original spelling, insertion order, and absent/empty/repeated distinctions.
- Implement immutable application `Message` values with payload, subject,
  optional reply subject, and headers.
- Define structured Core `Error` and closed lifecycle `Event` variants.

### Workstream 1B — `Op`, `Packet`, and `Codec`

- Model all initial wire operations: `INFO`, `CONNECT`, `PUB`, `HPUB`, `SUB`,
  `UNSUB`, `MSG`, `HMSG`, `PING`, `PONG`, `+OK`, and `-ERR`.
- Parse CRLF control lines and payload lengths through a caller-owned
  `Bytesrw.Bytes.Reader.t`.
- Keep `NATS/1.0` framing separate from application header entries.
- Enforce maximum line, header, and payload lengths before allocation.
- Keep the codec phase-blind and fuzz `Packet` independently from `Client`.
- Return structured errors at the public boundary; do not leak parser
  exceptions.

### Workstream 1C — `Client` state machine

- Define connection phases as a closed sum, including connecting/established,
  draining, and closed states.
- Construct the initial client intent, parse typed server `INFO`, and emit
  `CONNECT` only through an explicit second transition.
- Allocate client sids, track subscription intent, auto-unsubscribe counts,
  negotiated headers, server limits, and discovered URLs.
- Implement command transitions for publish, subscribe, unsubscribe, ping and
  flush barriers, plus close/drain intent.
- Route `MSG`/`HMSG` operations to explicit sid-tagged deliveries, preserving
  HMSG status metadata for the Eio request layer.
- Accept asynchronous `INFO`, interleaved `PING`, `PONG`, `+OK`, and `-ERR`.
- Add `timer`/`next_timeout` only for protocol liveness; application request
  deadlines remain outside this state machine.

### Tests and evidence

- Golden vectors for every operation and exact wire output.
- Fragmented-reader tests that retain the same reader across calls.
- Async `INFO`, interleaved control messages, empty payloads, headers, and
  malformed length tests.
- `max_payload` rejection and subject/filter validation tests.
- In-memory transition tests that feed outputs back into a small peer driver.
- Fuzz tests for `Packet`/`Codec` crash safety and bounded allocations.

### Exit criteria

The pure package builds and tests without Eio, Lwt, Unix, TLS, or DNS. The
state-machine test suite proves protocol behavior without a socket. No
application API is stabilized yet.

### Gate G1 — pure-core review

Review the public `.mli`, merlint protocol invariants, dependency closure,
reader ownership, error boundaries, and fuzz results. An outside review is
required before the Eio facade starts depending on the shape.

## Phase 2 — Eio Core NATS facade

### Goal

Provide a usable Core NATS SDK with direct-style result APIs and structured
concurrency while keeping all protocol transitions inside `Nats.Client`.

### Work

- Current foundation: the `nats-eio` package provides the serialized owner,
  direct-style Core publish/subscribe/request/flush/drain/close operations,
  bounded delivery and event streams, structured adapter errors, and
  non-blocking fragmented input handling.
- Dial an endpoint seed list through Eio; the adapter now supports
  deterministic configured/discovered candidate selection, per-pass DNS
  resolution, address fallback, preferred-endpoint ordering, and failure
  rotation. `INFO.connect_urls` replaces the discovered set without removing
  configured seeds, and bare `host[:port]` advertisements are accepted.
  Initial handshake failures are bounded and fail over across remaining
  configured seeds. Explicit `tls://` candidates perform bounded TLS before
  the NATS handshake; peer identity and SNI remain caller-owned through
  `Tls.Config.client`, and server-required TLS still works through that
  configuration.
- The TLS upgrade path supports INFO-driven or explicitly forced TLS with a
  caller-owned `Tls.Config.client`,
  a replaceable reader, a bounded handshake, and a required post-TLS `INFO`
  before `CONNECT`. Callers must install the TLS RNG and configure peer
  identity in the TLS client configuration.
- Start one protocol-owner fiber per connection and serialize all commands
  through it.
- The current connection surface includes `connect`, `publish`, `publish_msg`,
  `subscribe`, `request`, `request_msg`, `flush`, `events`, `drain`, and
  `close`.
- The owned `Subscription` surface includes `next`, `iter`, `unsubscribe`,
  server-barrier `drain`, and auto-unsubscribe operations. Drain completion is
  independent of consumer scheduling; queued messages and the terminal marker
  remain available to the pull-based receive path.
- Implement queue groups, headers, structured no-responders, and the
  per-request inbox fallback. Shared inbox multiplexing remains a later
  optimization once reconnect semantics are defined.
- Generate inbox names in the adapter; make the prefix configurable.
- Current recovery foundation: after one unexpected transport loss, preserve
  live subscription queues and replay intent, fail transport-bound waiters,
  perform an immediate redial followed by configurable capped exponential
  retries, replay the handshake and subscriptions, emit non-terminal
  `Disconnected`/`Reconnected` events, and defer unsubscribe/auto-unsubscribe
  commands until reconnection completes.
- Exercise candidate selection and discovery against a cluster/failure
  injection harness once the single-server acceptance path is established. Do
  not add silent Core publish replay or pending-request replay.
- Pending requests and flush barriers now fail structurally and exactly once
  on disconnect, cancellation, timeout, and drain; they never silently replay.
- Enforce bounded subscription and event queues with an explicit overflow
  policy. Distinguish local slow consumers from remote/server disconnects.
- Keep the implemented `Nats.Auth` boundary free of private-key parsing and
  ensure signer capabilities remain outside `Client.t`; add crypto-backed
  credential helpers only when a concrete dependency boundary is justified.
- Map parent-switch cancellation to a defined immediate-close or
  best-effort-drain policy, and make that policy testable.

### Acceptance tests against `nats-server`

The opt-in `scripts/runtest-server.sh` harness currently covers single-server
publish/subscribe, headers, queue groups, request/reply, no-responders, flush,
close, and optional username/password authentication. It uses a private
executable and is not part of the default Dune test alias. The remaining
scenarios below require server configuration, a cluster, or failure-injection
control that a lone `nats-server` process cannot provide.

- Core publish/subscribe, queue-group load balancing, headers, and replies.
- Request success, timeout, no responders, cancellation, and disconnect race.
- `flush` confirms server processing rather than local write completion.
- Reconnect restores subscriptions and remaining auto-unsubscribe counts.
- Dynamic `INFO` updates replace the discovered candidate set while retaining
  configured seeds; server discovery and endpoint rotation are observable.
- No arbitrary Core publish is replayed after reconnect by default.
- Subscription drain delivers already queued messages before termination.
- Connection drain rejects new work, flushes, closes, and resolves all
  waiters.
- Slow-consumer behavior is observable and never silently drops by default.
- Parent switch cleanup closes flows and does not leak fibers.

### Gate G2 — Core API stabilization

Stabilize only the Core domain values and `Nats_eio.Connection`/
`Subscription` surface. Publish the high-level API only after the outside
reviewer has seen the black-box behavior, lifecycle/error semantics, and
ownership model. JetStream types remain experimental or absent from this gate.

## Phase 3 — Core interoperability hardening

### Goal

Make the stable Core surface trustworthy across supported server behavior and
operational conditions before adding JSON-heavy APIs.

### Work

- Build a fixture matrix for server versions and negotiated capabilities.
- Exercise reconnect, drain, lame-duck, slow-consumer, TLS, and authentication
  failure paths under repeated connect/disconnect cycles.
- Compare observable Core behavior with at least one official Tier 1 client
  for each of pub/sub, request/reply, reconnect, drain, and headers.
- Retain raw-frame and fuzz corpora as regression fixtures.
- Document the supported server-version and feature matrix.

### Gate G3 — Core completeness

The Core NATS feature matrix is signed off, the fuzz corpus is retained, and
the stable API has no known lifecycle or ownership holes. This gate is required
before describing the Core implementation as complete.

## Phase 4 — JetStream foundation

### Goal

Layer typed JetStream management and durable publishing over the existing Core
request/reply and subscription primitives.

### Workstream 4A — Typed API and management

- Use `Jsont` at the Eio/JetStream boundary with `bytesrw` string codecs;
  management replies are decoded as success/error envelopes and unknown server
  fields are skipped until update/list shapes require preservation.
- The completed foundation models stream configuration/info, API responses, and
  structured JetStream errors separately from `Nats.Error`; it implements stream
  create/bind/delete/info and durable publish acknowledgements with message-id
  options over application subjects.
- Implement stream update/list and consumer management.
- Extend publish acknowledgements with all server status fields and feature
  gates where supported.
- Gate features by server version and return structured unsupported-feature
  errors.

### Workstream 4B — Consumer delivery

- Completed: implement pull consumers first, with one-shot fetch and a
  long-lived `Consumer.Pull` session that exposes `next`, bounded
  `next_with_timeout`, `iter`, and idempotent `close`.
- Completed: expose delivery metadata as a typed `Msg.t` tied to the delivered
  message and implement `ack`, `nak`, `term`, and `in_progress`.
- Completed: validate batch/count/byte limits, account for partial batches,
  retry empty server batches without recursive growth, preserve an outstanding
  request across a local timeout, and release subscriptions on close or switch
  release.
- Remaining: add server idle heartbeats and consumer-failure detection, then
  add push consumers, ordered consumers, filtering, and flow-control behavior
  without introducing a second runtime or subscription abstraction.

### Acceptance tests

- The opt-in Docker harness covers stream create/info/delete, publish ack,
  duplicate message ids, message counts, and cleanup without hand-built
  `$JS.API.*` subjects; it runs in anonymous and username/password modes.
- Completed: add typed server-error assertions, consumer management, pull
  backpressure, local/server-expiry behavior, cancellation/cleanup, and
  acknowledgement metadata/redelivery coverage against a real JetStream
  server.
- Remaining: heartbeat/consumer failure handling and server-version gates.
- Push and ordered consumer behavior once their implementation lands.

### Gate G4 — JetStream API stabilization

Stabilize JetStream separately from Core. Require an outside review of the
consumer/ack model, metadata ownership, JSON error model, and cancellation
semantics before calling the feature complete.

## Phase 5 — Key-Value and Object Store

### Workstream 5A — Key-Value

- Add bucket create/open/status and typed entry/operation/revision values.
- Implement get, put, create/update compare-and-set, delete, purge, history,
  keys, TTL, and cancellable watches.
- Preserve watch ordering and expose bucket/key/value/revision/timestamp/
  operation without requiring callers to parse JetStream messages.

### Workstream 5B — Object Store

- Implement metadata, streaming put/get, list, watch, update, link, and seal.
- Transfer chunks incrementally; never require a whole object as one `string`.
- Define cancellation, digest/size verification, partial-failure, and cleanup
  behavior for interrupted transfers.

### Acceptance tests

- KV CAS success/failure, revisions, history, TTL, deletes/purges, and watches.
- Watch cancellation and ordering under reconnect.
- Large Object Store transfer, metadata, listing, linking, sealing, and
  interrupted-transfer cleanup.
- No direct dependence by these modules on a private socket or private
  connection lifecycle.

### Gate G5 — durable-feature stabilization

Stabilize KV and Object Store independently if their release cadence or
dependency surface diverges. Require a review of streaming/backpressure and
revision semantics before documenting them as stable.

## Phase 6 — Services over Core NATS

### Goal

Provide typed endpoint and monitoring helpers as a composition over Core
subscriptions, queue groups, and request/reply.

### Work

- Define service, endpoint, and group values with consistent metadata.
- Implement queue-backed endpoint workers and typed request handlers.
- Implement `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS` discovery/monitoring
  responses.
- Make service shutdown use the same subscription and connection drain
  semantics; do not introduce a second lifecycle manager.

### Acceptance tests and gate G6

Run discovery, request handling, monitoring, queue balancing, reconnect, and
drain tests against a real server. Services may start after G2 and do not block
JetStream, KV, or Object Store. Stabilize only after confirming that all
service behavior composes with the Core connection ownership model.

## Phase 7 — Operational polish and optional integrations

Only pursue these after the core feature waves are stable and a concrete user
needs them:

- typed payload codec helpers and documentation examples;
- adapter-level probes, metrics, and tracing hooks;
- a dedicated credential/NKey/JWT package if in-repo auth becomes too large;
- WebSocket transport;
- a second runtime adapter such as Lwt or Mirage.

Every integration must bridge the existing waists. It must not add a parallel
protocol state machine or force optional dependencies into `nats`.

## Verification workflow

### Test layers

1. Pure state-machine and codec tests with deterministic readers and clocks.
2. In-memory transition tests for wire round trips and reconnect intent.
3. Fuzz tests for framing and decoding crash safety.
4. Black-box Eio integration tests against `nats-server`.
5. Interoperability tests against maintained Tier 1 clients and supported
   server versions.

Host-level tests should exercise user-visible contracts rather than private
implementation details. Each phase should add raw protocol fixtures and
failure-path tests, not only happy-path examples.

### Local verification rules

- Build and test inside the project’s Nix shell.
- Use Dune directly; do not use opam for the workflow.
- On macOS, use `./scripts/runtest-linux.sh` for Linux-backed test runs when
  the project has reached that stage.
- Never use `dune clean`, `--force`, disabled Dune caching, or remove the Dune
  lock file.
- Do not hide warnings; warnings found during a phase are work for that phase.

## Decision and review checkpoints

Before landing any commit or freezing a major public decision:

1. Inspect the complete worktree and separate task-owned changes.
2. Review the relevant staged diff and its acceptance evidence.
3. Ask an outside agent to review the proposed boundary or stability claim.
4. Resolve high-severity feedback or record a deliberate rejection in the
   relevant design/plan section.

The next outside review should occur at G0 after the signature prototype. The
following mandatory reviews are G2 for Core API stabilization and G4 for the
JetStream acknowledgement/consumer model.

## Deferred decisions

Do not freeze these before their phase needs them:

- subscription and event queue sizes and exact overflow policy;
- single protocol-owner fiber versus a coordinated reader/writer topology;
- public timeout/duration type;
- NKey/JWT package boundary and private-key parsing;
- unknown JetStream JSON-field preservation policy;
- optional Core reconnect buffering;
- callback bridge shape;
- WebSocket and second-runtime package boundaries;
- metrics/probe naming and payload policy.

The following are not deferred: Core-first stabilization, client-assigned sids,
the `Op`/`Message` split, explicit deliveries, reader ownership, no silent
publish replay, structured errors, and distinct drain/close semantics.
