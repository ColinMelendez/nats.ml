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
handshake, rejects buffered plaintext, and reports structured
TLS/timeout/close failures. Transport recovery with
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
groups, request/reply, no-responders, flush, close, and optional token or
username/password authentication, server-required TLS, request timeout and
cancellation cleanup, auto-unsubscribe, subscription drain, connection drain,
slow-consumer handling, reconnect recovery, and three-node cluster
discovery/failover with subscription recovery. Authentication capabilities now
cover anonymous,
token, username/password, NKey, and JWT credentials; nonce signing is repeated
for every INFO. The auth acceptance harness now generates ephemeral NKey/JWT
credentials and mTLS certificates, exercises the existing signer and TLS
configuration seams, and checks both successful and rejected connections from
the OCaml client and the official Go peer. Private-key parsing remains an
optional caller-side concern rather than a public Core dependency. The
JetStream foundation now adds a typed, resource-free capability
over the connection, stream configuration/info including per-subject limits and
direct/rollup flags, stream create/bind/update/list/info/delete and direct
stored-message reads, publish acknowledgements with message ids, API error
envelopes, and an opt-in real-server acceptance path for management,
deduplication, and cleanup. The consumer slice now includes
consumer management and inventory, typed consumer updates, one-shot fetch,
typed message
acknowledgements, and a persistent single-owner
`Consumer.Pull` session with batch accounting, local timeout/resumption,
server-expiry retries, structured terminal statuses, and switch-owned cleanup.
Consumer status classification now detects terminal consumer failure consistently
across fetch and pull paths. The Eio layer also provides a switch-owned
single-owner `Consumer.Push` session with delivery-subject and queue-group
configuration, explicit acknowledgement, local timeout/resumption, and
fail-closed handling of unsupported status frames. Push sessions consume idle
heartbeats, answer flow-control requests, track heartbeat liveness, and preserve
absolute caller timeouts across control traffic. Ordered sessions now use
client-managed ephemeral pull consumers, force no-ack memory-backed
configuration, validate consumer sequence continuity, and recreate consumers
after gaps, liveness loss, deletion, or non-replayed disconnects while resuming
from the next stream sequence. Reconnection waits for the connection replay
barrier and retries transient JetStream availability failures without repeating
stale-consumer cleanup. Key-Value now provides typed bucket management,
compare-and-set mutations, finite scans, history, cancellable ordinary and
ordered watches, per-key/marker TTL, account-wide managers, policy projection,
and composed mirror/source/republish configuration. The opt-in single-server runner now
exercises these behaviors against a live JetStream bucket as well. Continuous
pull consumption is available through a bounded switch-owned
`Consumer.Consume` session with explicit stop, drain, and cleanup controls.
Priority consumers support multiple configured groups and preserve per-group
pin state.
Services now provide typed endpoint/group values, queue-backed workers,
request/service-error replies, `$SRV.*` monitoring and fan-out discovery,
statistics, statistics reset, stopped-state inspection, endpoint pending
message/byte limits, replayable subscriptions, and service-local draining.
Object Store now has a complete Eio data-plane slice with validated bucket
management, direct metadata reads, incremental Bytesrw transfers,
digest/size/chunk verification, deletion, replacement cleanup, bucket policy
projection and updates, account-wide managers, name/status listers, file
helpers, and structured timeout and cleanup errors. The opt-in single-server
runner now
exercises single-service monitoring, endpoint/group requests, service errors,
failure isolation, statistics, and service-local draining. It also exercises
two-instance queue-group routing and aggregate worker statistics, verifies
Service endpoint and monitoring recovery across a two-server reconnect, and
exercises chunked content, metadata, links, listing, deletion and tombstones,
watches, sealing, and cleanup. Its six-mode single-server authentication/TLS
matrix passes all 18 cases across `nats:2.10.22`, `nats:2.12.15`, and
`nats:2.14.5`. Push reconnect restoration is
implemented through replayable subscription
recovery, including durable confirmation and ephemeral recreation. Its
cross-SDK authenticated/TLS restart matrix now covers NKey, JWT,
NKey-over-TLS, JWT-over-TLS, and mTLS across the three pinned releases. The
cross-SDK Ordered reconnect harness now covers seed-node loss, elected
JetStream-leader loss, and durable seed restart under NKey, JWT, NKey-over-TLS,
JWT-over-TLS, and mTLS across the three pinned server releases; additional
cluster failure scenarios and the broader cross-SDK acceptance matrix remain
in the final acceptance phase. Current consumer confidence combines local mock
transport and pure-boundary tests with the passing single-server and
authenticated cluster acceptance slices. Priority-group pull consumers are
now modeled locally:
validated policy configuration with multiple groups, per-request thresholds
and priorities, INFO pin state, explicit unpin, pinned-client request echoing,
and 423 retry behavior are covered by the Eio mock transport. Real multi-group
priority interoperability remains acceptance work.
An additional opt-in `scripts/runtest-jetstream-cluster.sh` harness passes on
the pinned `nats:2.10.22` image: it forms a full three-node route mesh, creates
file-backed three-replica stream and durable explicit-ack Push consumer state,
acknowledges a baseline delivery, kills the seed, verifies reconnect to a
surviving node and replicated state, then publishes, delivers, acknowledges,
and cleans up after recovery. Its scope is the lower-level anonymous
connected-node-loss contract; the separate cross-SDK Ordered reconnect runner
covers leader-targeted failure and authenticated/TLS behavior.
The recovery bridge
preserves live subscription handles, queues, and replay intent; fails
transport-bound requests, flushes, and drains; redials through the stored
connection seam; replays INFO/TLS/CONNECT and subscriptions; emits
non-terminal `Disconnected`/`Reconnected` events; and defers unsubscribe
commands until the replacement session is ready. It does not replay
arbitrary publishes or pending requests.

### Go parity checkpoint

Against the pinned official Go `nats.go v1.52.0` surface, the material Eio
capabilities are now implemented: JetStream account and resource management,
stream persistence/message-counter and publish controls, pull/push/ordered and
continuous consumption, multiple priority groups, KV managers and composed
bucket policies, ordinary and ordered KV watches, and Object Store managers,
file helpers, and data-plane interop. The remaining differences are deliberate
runtime or product-scope choices: direct Eio iteration instead of Go
callbacks/channels, separate ordinary and ordered KV watch contracts,
adapter-specific diagnostics/dialer conveniences, broader failure matrices,
and alternative transports.

### Production-readiness acceptance program

The remaining work is now primarily acceptance and operational confidence,
not another broad feature pass. Existing opt-in server and Go interop runners
are the foundation; this program extends them in place and keeps the fast
local mock suite independent from Docker.

#### A. Make the acceptance harness reproducible

Keep all binary dependencies in the `integration` Nix shell. Docker or Colima
is the only host-level prerequisite. Each runner must use a pinned NATS image
by default, derive names from a unique run id, create only resources it owns,
wait for server readiness and cluster formation explicitly, and clean up those
resources on success, interruption, or failure. A failed run must preserve
OCaml, peer, server, and launcher diagnostics in a caller-selected artifact
directory; the default path may remain temporary. Cleanup must use returned
Docker ids or ownership labels, never a guessed name that could belong to a
different run. The first harness slice must also prove that two concurrent
runs do not collide, that interruption preserves the signal status, and that
every process and readiness phase has a deadline. The harness must not rely on
global Docker cleanup or a large VM, and the documented local Colima baseline
is a 10 GiB disk with room to increase only when evidence requires it.

Expose the layers as explicit commands rather than attaching Docker work to
the default `dune runtest` alias:

```text
dune runtest                         # deterministic local suite
./scripts/runtest-server.sh          # one-server black-box acceptance
./scripts/runtest-interop*.sh        # cross-SDK acceptance
./scripts/runtest-*-cluster*.sh      # fault-injection and cluster suites
```

Completed slice: once its integration shell is active, the single-node
JetStream interop runner now applies a bounded outer deadline, preserves
failure diagnostics through `NATS_TEST_ARTIFACT_DIR`, and records non-secret
run metadata before cleanup.
The shared Core and Services interop runner now has the same bounded outer
deadline, including the timeout in its failure metadata; this bounds the
legacy cross-SDK path without changing the deterministic local suite.
Its matrix wrapper also accepts the Object Store scenario explicitly. The
remaining harness work is to apply the same guarantees to the other legacy
interop runners, exercise concurrent-run and interruption behavior, and make
matrix-level failures retain their case context.

#### B. Close the single-server contract matrix

Run the Core, JetStream, Key-Value, Object Store, and Services user-facing
scenarios against the pinned server-version matrix. For every applicable
surface, cover anonymous, token, username/password, NKey/JWT, server-required
TLS, and mTLS in the smallest useful progression: first connection, then
reconnect and cleanup. Record unsupported combinations as explicit skips or
tracked gaps, never as an accidental absence of coverage.

#### C. Expand cluster failure injection

Reuse the existing three-node route and JetStream runners. Elected
stream-leader loss, seed-node loss, and durable seed restart now have a
45-case authenticated/TLS cross-SDK matrix over the three pinned releases,
with the restart mode retaining run-unique per-node data volumes, waiting for
both clients to fail over, and checking that all three stream replicas are
current after the seed returns. The remaining cases are each-node loss where
the replica count allows it, changed advertised client URLs, reconnect during
management and delivery operations, consumer recreation under more failure
modes, and multi-node loss. Keep the failure trigger synchronized with a
flushed, observable barrier so a test failure identifies the lost invariant
rather than a startup race.
The anonymous Object Store cluster runner separately covers replicated content
and metadata recovery after seed loss, elected-leader loss, and durable seed
restart; all nine cases pass across the three pinned releases. Authenticated,
multi-node, changed-advertisement, and broader management-operation failure
topologies remain in this workstream.
The authenticated KV companion wrappers now route the same five generated
NKey/JWT/TLS modes through the three failure modes and three pinned releases.
An NKey seed smoke and an isolated JWT restart case pass. The full 45-cell KV
sweep remains a release gate: a sequential run reached nats-server 2.12.15's
`JSInsufficientResourcesErr` after earlier cells, while the same JWT restart
case passed in isolation.

#### D. Make cross-SDK behavior the wire-level oracle

Use the maintained official Go SDK peer first, then add other Tier 1 SDKs only
when they clarify a protocol contract. Alternate publisher, subscriber,
requester, consumer, and management ownership between OCaml and the peer.
Assert headers, metadata, acknowledgement semantics, status/error envelopes,
consumer recovery, KV revisions, and Object Store chunk/digest behavior from
both directions. The baseline KV slice now alternates bucket creation,
revisioned updates, stale CAS, tombstones, watches, purge markers, and cleanup
with the official Go `nats.go` `jetstream.KeyValue` API across three pinned
server releases and six single-server authentication/TLS modes. Keep resource
names and cleanup ownership explicit so a failed case cannot contaminate the
next one.

#### E. Exercise authentication and TLS as a matrix

Completed single-server Core auth/TLS coverage now includes anonymous, token,
username/password, NKey, JWT, NKey-over-TLS, JWT-over-TLS, and mTLS. The
dedicated positive matrix passes all five non-anonymous modes across the three
pinned server releases; its companion negative matrix passes invalid NKey/JWT
signatures and missing mTLS client certificates across the same releases for
both SDKs. The cross-SDK Ordered reconnect matrix also passes those five modes
under seed failure, elected-leader failure, and durable seed restart on all
three releases. Authenticated
JetStream Push restart now passes the same five modes across all three
releases; authenticated multi-node loss and other feature-family combinations
remain later acceptance increments, with server-version and feature-gate
differences documented there.
The routed system-account acceptance matrix now covers seven privileged
credential/TLS modes—username/password, username/password over TLS, NKey, NKey
over TLS, JWT, JWT over TLS, and certificate-mapped mTLS—across the same three
releases, including monitoring, system events, reload, and ordered failover
recovery (21 cells). The Core auth API exposes `Nats.Auth.tls` separately from
anonymous authentication so a TLS client certificate can satisfy an
authentication-required server without putting transport credentials into the
NATS `CONNECT` payload. Simple token authentication remains outside this
privileged matrix because the token-only server mode does not select an
account-scoped system user; it is covered by the ordinary Core acceptance path.

#### F. Define release gates

The local suite must pass on every change. Single-server acceptance is the
normal production-readiness gate, while cluster, cross-SDK, and auth/TLS
matrices are explicit pre-release gates until their runtime is small and
stable enough for continuous execution. A release claim requires the pinned
server matrix, failure artifacts for negative cases, no known unclassified
wire-behavior differences against the Go peer, and an outside review of the
staged boundary and evidence.

Implementation order is deliberately incremental: (1) harness diagnostics and
ownership, (2) broader single-server and Go-peer feature-family acceptance,
(3) missing cluster failures and reconnect topologies, (4) newer server/SDK
feature gates, and (5) release automation and final evidence. The single-server
NKey/JWT/mTLS slice and the material pinned Go capability slices are complete.
Each remaining slice lands as a small semantic commit and is reviewed
independently.

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
- New Core publishes use the Go-compatible bounded reconnect buffer during
  transport recovery; already-submitted mutations are never blindly retried or
  reconciled by the client.
- Requests complete exactly once as response, no responders, timeout,
  cancellation, disconnect, or structured server error.
- Connection drain and subscription drain are distinct operations.
- Connection drain during reconnect closes and reports the reconnecting state,
  matching the Go client rather than starting a drain against a replacement
  transport.
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
  representation, `max_payload` handling, and reconnect-buffer/mutation policy.
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
  caller-owned `Tls.Config.client`, a replaceable reader, and a bounded
  handshake. A server-required upgrade sends `CONNECT` using the already-parsed
  plaintext `INFO`; an explicit `tls://` endpoint sends it after the first
  encrypted `INFO`. Callers must install the TLS RNG and configure peer
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
- The opt-in cluster runner starts a three-node route mesh, checks that the
  seed advertises the other client endpoints, kills the seed, verifies
  subscription recovery to a discovered peer, then kills that active peer and
  verifies a second recovery to the last node. Do not add silent Core publish
  replay or pending-request replay.
- The two-server reconnect runner now holds a request across the active-server
  failure and verifies that it fails as `Disconnected` before subscription
  recovery continues.
- Pending requests and flush barriers now fail structurally and exactly once
  on disconnect, cancellation, timeout, and drain; they never silently replay.
- Enforce bounded subscription and event queues with an explicit overflow
  policy. Distinguish local slow consumers from remote/server disconnects.
- Keep the implemented `Nats.Auth` boundary free of private-key parsing and
  ensure signer capabilities remain outside `Client.t`; add crypto-backed
  credential helpers only when a concrete dependency boundary is justified.
- Parent-switch cancellation is an immediate close rather than a best-effort
  drain; the live lifecycle harness verifies that a blocked receive terminates
  when its owning switch is failed.

### Acceptance tests against `nats-server`

The opt-in server harnesses currently cover single-server publish/subscribe,
headers, queue groups, request/reply, no-responders, flush, close, optional
token or username/password authentication, server-required TLS, two-server
subscription recovery and pending-request disconnect failure, request
timeout/cancellation cleanup, auto-unsubscribe, repeated slow-consumer and
drain handling, parent-switch cleanup, repeated lame-duck handling,
and repeated three-node cluster discovery/failover. These private executables
are not part of the
default Dune test alias. The
following matrix tracks the acceptance surface; the remaining scenarios
require additional server configuration or failure-injection control.

The server matrix runner repeats the server lifecycle, cluster discovery, and
lame-duck harnesses across `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`.
It is a bounded nine-cell version sweep of the existing live-server contracts;
authentication and JetStream variables apply only to the server lifecycle
cell, while cluster and lame-duck cells intentionally remain anonymous. It
does not replace broader failure-injection work. The anonymous
nine-cell sweep passed, and the server lifecycle cells also passed with token
and username/password authentication on all three images. Enabling
`NATS_TEST_JETSTREAM=1` completed the same nine-cell matrix; JetStream stream
and consumer acceptance ran only in the three server lifecycle cells, which
also passed with both authentication modes on all three images. Cluster and
lame-duck remained anonymous Core-only. Each lame-duck cell now performs two
fresh-container cycles; those repeats pass on all three images. The lifecycle
executable now performs two fresh slow-consumer cycles and two fresh
subscription/connection drain cycles per server cell; those repeats pass on all
three images, under both authentication modes. The anonymous repeats also pass
with JetStream enabled. A separate real-server consumer executable also passes
on all three images, under anonymous, token, and username/password
authentication, covering durable configured Push, owned Push, Ordered subject
filtering, and Ordered recovery after deleting an outstanding consumer.
The dedicated JetStream interop matrix runner now defaults to pull and Push
anonymous plaintext/TLS cells across the three pinned releases. Its explicit
six-mode sweep—anonymous, token, and username/password, each plaintext and
server-required TLS—passes for both pull and Push on all three releases.
Ordered is an opt-in matrix scenario; its six-mode sweep also passes on all
three pinned releases, checking two independent filtered sessions, interleaved
stream gaps, AckNone/memory-backed consumer configuration, exact stream and
consumer sequences, and coordinated cleanup.
The separate JetStream Push reconnect runner uses a file-backed stream and
durable consumers on one persistent server container, restarts that container,
and checks both the Go and OCaml Push legs after reconnect. Anonymous
plaintext plus NKey, JWT, NKey-over-TLS, JWT-over-TLS, and mTLS restart modes
pass on all three pinned releases. The base runner also covers token and
username/password compatibility, including server-required TLS. Its scope is
single-server restart; durable three-node seed restart is covered by the
separate Ordered reconnect cluster acceptance slice below, while broader
cluster restart combinations remain later work.

- Core publish/subscribe, queue-group load balancing, headers, and replies.
- Request success, timeout, no responders, cancellation, and a pending-request
  disconnect race are live-server covered; broader failure injection remains.
- `flush` confirms server processing rather than local write completion.
- Reconnect restores subscriptions and remaining auto-unsubscribe counts.
- A cluster seed advertises reachable peers, and reconnect fails over to a
  discovered peer while preserving subscription intent.
- A live server's lame-duck `INFO` sets the typed mode flag, emits the
  `Lame_duck_mode` event, and leaves the existing connection usable.
- Dynamic `INFO` updates replace the discovered candidate set while retaining
  configured seeds; server discovery and endpoint rotation are observable.
- New Core publishes accepted during reconnect are flushed from the bounded
  reconnect buffer after the replacement handshake; interrupted mutations are
  not reconciled by the client.
- Subscription drain delivers messages already queued or accepted by the server
  before the drain barrier, then terminates; an in-flight subscription drain
  survives transient reconnect and re-establishes that barrier after replay.
- Connection drain rejects new work, flushes, closes, and resolves all
  waiters.
- A full subscription reports structured slow-consumer failure and an event
  rather than blocking the connection or silently dropping the burst.
- Releasing a parent switch terminates a blocked subscription and closes the
  owned connection without leaking fibers.

### Gate G2 — Core API stabilization

Stabilize only the Core domain values and `Nats_eio.Connection`/
`Subscription` surface. Publish the high-level API only after the outside
reviewer has seen the black-box behavior, lifecycle/error semantics, and
ownership model. JetStream types remain experimental or absent from this gate.

The G2 review record is in [`docs/core-api-review.md`](docs/core-api-review.md).
It covers the pure transition API and the Eio request/subscription lifecycle,
records the resolved PONG and shared-jitter ownership findings, and documents
why drain deadlines remain an Eio policy rather than a pure-client timeout.

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

Completed initial slice: a Nix-built Go `nats.go` v1.52.0 peer and a pinned
`nats-server` 2.10.22 runner cover anonymous, token, and username/password
Core traffic. The black-box exchange checks bidirectional pub/sub, repeated
headers, request/reply in both directions, no-responders, and clean drain and
close. This proves the first cross-SDK seam; it does not satisfy the full
server-version, reconnect, or product-surface matrix for G3.

The interop matrix runner now defines a bounded Core gate across the
established `nats:2.10.22` compatibility floor, the `nats:2.12.15` older
release line, and the current `nats:2.14.5` release. Each image runs the Core
cross-SDK exchange, single-server TLS Core traffic, the repeated plaintext
failover exchange, and the repeated TLS failover exchange, for twelve
sequential cases by default. `NATS_SERVER_IMAGES` selects another
comma-separated image list and `NATS_INTEROP_MATRIX_SCENARIOS` selects a
subset of `core`, `reconnect`, `tls-core`, and `tls-reconnect`. The matrix
defaults to anonymous authentication; setting the token or username/password
variables repeats the selected matrix with that authentication mode. The
version selection is deliberately bounded rather than an assertion that every
historical patch release is covered.

Initial reconnect slice: a two-endpoint black-box runner starts independent
servers, asks the Go `nats.go` peer and OCaml client to establish subscriptions,
kills the first endpoint after a baseline exchange, and requires both clients
to recover to the second endpoint before exchanging messages again. Anonymous,
token, username/password, and the `nats:2.14.3` image have been exercised.

Completed repeated-failure slice: the reconnect runner now uses three
independent servers, arms each kill only after a flushed exchange, and requires
both clients to replay the subscription and complete a two-sided recovery
barrier after each of the first two endpoints fails. The same anonymous,
token, username/password, and `nats:2.14.3` modes have been exercised.

Completed TLS/reconnect slice: setting `NATS_TEST_TLS=1` makes the same runner
generate a short-lived CA and hostname-checked certificate, configures all
three independent servers for TLS, and supplies the CA to both SDKs. Anonymous,
token, and username/password authentication have been exercised on that TLS
reconnect scenario for all three matrix images.
Completed Core TLS slice: the single-server cross-SDK exchange now uses the
same generated CA and hostname policy, with anonymous, token, and
username/password authentication exercised for all three matrix images.
Completed authentication matrix: those three CONNECT authentication modes have
each been exercised across all twelve image/scenario cells, for 36 cross-SDK
acceptance combinations. Server authentication here is orthogonal to TLS
transport policy.
Completed NKey/JWT/mTLS slice: the dedicated auth matrix exercises NKey, JWT,
NKey-over-TLS, JWT-over-TLS, and mTLS against all three pinned server releases,
for fifteen positive cross-SDK cases. The negative matrix repeats those cells
with a mismatched NKey/JWT signing seed or no client certificate and requires
both the Go peer and OCaml client to fail authentication, for fifteen negative
cases. Credentials and certificates are ephemeral and never committed.
The bounded version/scenario matrix and repeated failure cases for the
documented Core contracts are now in place. Broader failure-injection campaigns
and JetStream, Key-Value, Object Store, and the remaining Services
interoperability matrix remain later product-surface acceptance work rather
than requirements of this Core gate.

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
  fields are preserved on stream update/list wire paths even when the public
  projection does not expose them.
- Completed: model stream configuration/info, API responses, and structured
  JetStream errors separately from `Nats.Error`; implement stream
  create/bind/update/list/delete/info and direct stored-message reads, consumer
  create/bind/info/list/delete/update with typed configuration combinators, and
  durable publish acknowledgements with message-id options over application
  subjects. Stream configuration retains per-subject limits and direct/rollup
  flags. Consumer configuration models metadata, sample frequency, push rate
  limit, replica inheritance, mutually exclusive singular/multi-subject
  filters, nanosecond redelivery backoff schedules, and typed UTC pause
  deadlines. Stream and consumer updates preserve unknown server
  configuration through an
  INFO/read-modify-write cycle, and list
  operations fail with a structured error rather than silently returning an
  incomplete page. Consumer updates use the named
  `CONSUMER.CREATE` endpoint with an explicit update action and retain durable
  identity when the new configuration omits it.
- Completed locally: add typed stream mirrors, source lists, source filters,
  sequence/time start points, subject transforms, cross-account external
  prefixes, republish rules, and mirror-direct reads. Mirror/source and
  source-filter/transform conflicts are rejected during construction. Nested
  source configuration preserves unknown JSON members through the
  INFO/read-modify-write path, and local tests cover the wire shapes and
  constructor invariants.
- Completed locally: model JetStream priority-group consumer policies and
  validated group names, including the pull-only and explicit-ack invariants;
  encode policy, multiple groups, and pinned-client timeouts through consumer
  create and update requests; project per-group pin state through
  `Consumer.Info`; and expose `Consumer.unpin`.
- Completed locally: model consumer pause state with `Ptime.t` deadlines,
  project paused/remaining state through INFO, and expose dedicated pause and
  resume operations through `CONSUMER.PAUSE`. Consumer updates preserve the
  current server deadline instead of pretending that the create/update API can
  change it.
- Completed locally: extend publish acknowledgements with stream, sequence,
  duplicate, domain, batch, and count fields. Add typed publish options for
  message IDs, optimistic-concurrency expectations, per-message TTL, scheduled
  messages, retry policy, and asynchronous stall limits. Synchronous publishing
  retries `No_responders` through the connection-owned monotonic clock; the
  switch-owned asynchronous publisher provides bounded pending state, futures,
  cancellation, retries, and completion waiting.
- Completed locally: add atomic and fast batch publishing over the server's
  `Nats-Batch-*` and `$FI` protocols. Atomic staging uses a final request-backed
  commit; fast batches consume flow acknowledgements, gap notices, and flow
  errors. Stream configuration now models the `allow_atomic`,
  `allow_msg_schedules`, and `allow_batched` feature gates.
- Remaining: replace per-future private request subscriptions with a shared
  wildcard acknowledgement subscription for higher async publish throughput.
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
- Completed: add opt-in server idle-heartbeat support to one-shot fetches and
  persistent pull sessions. Status-100 deliveries reset typed heartbeat
  deadlines, local timeouts preserve the outstanding request, queued control
  deliveries are drained before declaring a miss, and missing heartbeats fail
  the pull session with structured cleanup.
- Completed: centralize consumer-failure and status classification across fetch
  and pull, preserving consumer deletion and conflict errors while keeping
  request-expiry and batch-completion statuses internal.
- Completed: add `Consumer.Push` with server-configured delivery subjects and
  queue groups, switch-owned lifecycle, typed deliveries, explicit
  acknowledgement, timeout/resumption, and local failure/close contracts.
  Push sessions consume idle heartbeats, answer flow-control requests including
  stalled-heartbeat replies, and fail with structured missing-heartbeat errors
  after two configured intervals.
- Completed locally: restore replayable Push subscriptions after reconnect.
  Subscription-local recovery generations suspend heartbeat liveness during
  transport recovery; durable consumers are rechecked, ephemeral consumers
  are recreated when the server reports error code 10014, and absolute caller
  timeouts remain bounded across restoration.
- Completed locally: add `Consumer.Ordered` as a sibling pull session. It
  creates ephemeral no-ack memory consumers, checks consumer sequence
  continuity rather than stream sequence continuity, resumes at the next
  stream sequence after recovery, and preserves absolute caller deadlines
  across recovery attempts.
- Completed locally: add `Consumer.Consume` as a bounded, switch-owned
  continuous pull session. It keeps a background pull loop behind an Eio
  queue, exposes `next`/`iter`, supports message/byte bounds and stop-after,
  and distinguishes stop (discard buffered messages) from drain (preserve
  them).
- Completed locally: extend one-shot and persistent pull requests with
  priority groups, overflow thresholds, and prioritized levels. Consumer
  handles retain server-issued `Nats-Pin-Id` values per group, so both
  persistent sessions and subsequent one-shot fetches echo them privately;
  both paths clear stale identity on 423 pin mismatch and retry. Multiple
  configured groups are validated, encoded, and tracked independently.

### Acceptance tests

- The opt-in Docker harness covers stream create/update/info/list/delete,
  filtered stream and consumer inventory, publish ack, duplicate message ids,
  message counts, unknown-config preservation, and cleanup; it runs in
  anonymous, token, and username/password modes.
- Completed locally: add typed server-error assertions, consumer management,
  consumer create/update scalar configuration coverage, update action envelopes
  and unknown-field preservation, pull backpressure, server-expiry behavior,
  cancellation/cleanup,
  acknowledgement metadata, consumer-failure detection, and push lifecycle
  contracts through the Eio mock transport.
- Completed locally: cover consumer pause deadline encoding, INFO projection,
  dedicated pause/resume requests, and update/reconnect preservation through
  the Eio mock transport.
- Completed locally: cover priority policy/group validation, create/update wire
  fields, INFO pin state, unpin requests, priority fetch/pull fields,
  pin-header capture, explicit-unpin cache clearing, 423 stale-pin recovery,
  and subsequent requests without the stale id through the Eio mock transport.
- Completed: heartbeat liveness and local/server timeout interaction.
- Completed real-server slice: the Docker harness exercises durable configured
  Push delivery and acknowledgement, owned Push creation/deletion, Ordered
  subject filtering, and Ordered consumer recreation after a server-side
  deletion across the three pinned releases in anonymous, token, and
  username/password modes.
- Completed locally: cover ordered consumer creation, filtered stream-sequence
  gaps, consumer-sequence recovery, missing-heartbeat recovery, deletion
  recovery, timeout preservation, and switch/explicit cleanup through the Eio
  mock transport.
- Completed locally: cover Push durable restoration, ephemeral recreation,
  heartbeat suspension, timeout preservation, and independent lifecycle-event
  delivery across reconnect through the Eio mock transport.
- Completed cross-SDK slice: a separate Nix-built official Go `nats.go` peer
  creates a unique memory stream and durable pull consumers, while the OCaml
  client binds the consumers and exchanges messages in both directions. The
  exchange checks stream/consumer metadata, explicit acknowledgements, headers,
  publish acknowledgements, and duplicate message ids. The dedicated runner
  covers anonymous, token, username/password, and server-required TLS
  connections; all six modes have passed on the three pinned server releases.
- Completed cross-SDK Push slice: the same official Go peer creates two durable
  explicit-ack push consumers with delivery subjects outside the stream, and
  the OCaml and Go clients bind opposite delivery legs. The exchange validates
  consumer delivery/filter configuration, push metadata and sequences,
  synchronous acknowledgements, and cleanup ordering. Anonymous plaintext
  and TLS pass on all three pinned releases. The dedicated matrix runner
  defaults to anonymous plaintext/TLS pull and Push cells across all three
  pinned releases; its explicit token and username/password plaintext/TLS
  sweep also passes for Push on all three.
- Completed cross-SDK Ordered slice: the official Go `nats.go` peer and OCaml
  client each create independent ephemeral Ordered sessions over a shared
  memory stream. Interleaved matching and non-matching publications verify
  filtered stream-sequence gaps, consecutive consumer sequences, exact
  delivery metadata and headers, AckNone/memory-backed consumer configuration,
  publish acknowledgements, and coordinated unsubscribe/deletion cleanup. The
  dedicated matrix scenario passes in anonymous, token, and username/password
  plaintext/TLS modes on all three pinned releases.
- Completed cross-SDK Ordered reconnect slice: a separate Nix-built official Go
  `nats.go` peer and OCaml client share a file-backed, three-replica stream in a
  three-node cluster. Matching and non-matching baseline messages establish
  filtered stream and consumer sequences; after seed-node loss, elected
  stream-leader loss, or durable seed restart, both clients reconnect through
  surviving peer URLs and validate Ordered progress. Restart mode uses
  run-unique per-node data volumes, waits for both clients to report failover,
  and requires the returned seed to rejoin with all three replicas current
  before post-recovery delivery. A one-replica consumer may either retain its
  identity and continue at consumer sequence 3 or be recreated at sequence 1
  when its consumer leader was also lost. The authenticated matrix covers
  NKey, JWT, NKey-over-TLS, JWT-over-TLS, and mTLS under all three failure
  modes: all 45 cases pass on `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`.
  Multi-node-loss combinations remain separate work.
- Completed cross-SDK Push reconnect floor: a dedicated runner keeps the same
  Go and OCaml durable Push sessions across a persistent file-backed
  nats-server restart, checks recovery barriers and post-restart JetStream
  readiness, and validates new deliveries, stream/consumer sequences,
  acknowledgement floors, and two-phase cleanup. Anonymous plaintext passes
  on all three pinned releases. Its authenticated companion covers NKey, JWT,
  NKey-over-TLS, JWT-over-TLS, and mTLS across the same releases; all 15 cases
  pass. The base runner also accepts token and username/password credentials,
  including server-required TLS variants. Cluster reconnect remains a separate
  matrix.
- Completed real-server cluster slice: the dedicated Docker harness forms a
  full three-node JetStream route mesh, verifies file-backed three-replica
  stream and durable explicit-ack Push consumer state, survives one seed-node
  kill with reconnect to a surviving node, and checks post-failover publish,
  delivery, acknowledgement floors, replicated stream state, and cleanup on
  the pinned `nats:2.10.22` image. It intentionally does not claim
  JetStream-leader targeting, durable seed restart, or multi-node loss; those
  are covered by the cross-SDK Ordered reconnect runner only where its protocol
  assertions apply.
- Remaining: additional real cluster failure scenarios, authenticated/TLS
  multi-node-loss matrices, feature gates for remaining server-version
  differences, and broader cross-SDK JetStream cluster coverage.

### Gate G4 — JetStream API stabilization

Stabilize JetStream separately from Core. Require an outside review of the
consumer/ack model, metadata ownership, JSON error model, and cancellation
semantics before calling the feature complete.

#### Current review status

- Completed: the public JetStream API review is recorded in
  `docs/jetstream-api-review.md`. The review checked ownership, cancellation,
  read-modify-write configuration semantics, structured errors, unknown JSON
  preservation, and the direct-style consumer surfaces against the pinned
  server behavior, the official Go SDK, focused mock tests, OpenCode review,
  and a high-effort `gpt-5.6-sol` review.
- Resolved: daemon-owned `Consume` lifecycle, monotonic stream safety flags,
  local consumer-mode validation and its public contract, create-only consumer
  requests for owned Push paths, retryable owned Push cleanup including named
  ephemeral recovery, and consumer priority-group/policy replacement during
  update, including the atomic public combinator and clear-path coverage for
  coupled priority fields. Switch-release cleanup is documented as best effort;
  explicit `close` remains the confirming operation.
- Remaining before a broad compatibility claim: choose whether to expose a
  server capability/version projection or publish an explicit supported
  feature matrix; expand the live authenticated/TLS and cluster-failure
  acceptance work already listed above.

## Phase 5 — Key-Value and Object Store

### Workstream 5A — Key-Value

- Completed locally: add bucket create/open/status and typed
  entry/operation/revision values; implement get, put, create/update
  compare-and-set, delete, purge, history, finite scans, TTL, keys, and
  cancellable ordinary and ordered watches.
- Preserve watch ordering and expose bucket/key/value/revision/timestamp/
  operation without requiring callers to parse JetStream messages.
- Completed locally: add `Key_value.Ordered_watch` as a separate stronger
  contract over `Consumer.Ordered`. It emits the retained-snapshot marker,
  preserves filters, metadata-only delivery, heartbeat/replay settings,
  name-prefix/reset controls, and recreates at the next stream revision after
  consumer-sequence gaps, missing heartbeats, deletion, or non-replayed
  disconnects.
- Completed cross-SDK baseline: a Nix-built official Go `nats.go` peer and the
  OCaml client alternate bucket creation, revisioned updates, stale CAS,
  tombstones, watches, purge markers, and cleanup. The matrix passes across
  `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5` in anonymous, token,
  username/password, and corresponding server-required TLS modes.
- Completed live cluster acceptance: the dedicated
  `runtest-interop-key-value-cluster.sh` runner uses the official Go
  `jetstream.KeyValue` API and a three-node file-backed stream to verify the
  retained snapshot marker, revision continuity, and ordered-watch recovery
  after seed loss, elected-leader loss, and durable seed restart. Broader
  server/version combinations beyond the pinned auth/TLS matrix remain.
- Added the authenticated KV cluster wrappers, reusing the established
  JetStream cluster matrix for NKey, JWT, NKey-over-TLS, JWT-over-TLS, and mTLS
  across seed, leader, and restart failures. The first smoke and isolated
  restart evidence pass; the full matrix remains pending because a sequential
  run encountered nats-server 2.12.15's `JSInsufficientResourcesErr`.

### Workstream 5B — Object Store

- Completed locally: validated bucket management, direct metadata reads,
  incremental `Bytesrw.Bytes.Reader`/`Writer` transfers, SHA-256 and size/chunk
  verification, metadata updates and rename, object and bucket links,
  recursive link reads, deletion, replacement cleanup, snapshot/live watches,
  listing, sealing, typed bucket policy configuration and read-modify-write
  updates, and structured operation deadlines.
- Completed cross-SDK data-plane slice: the dedicated Go peer runner exchanges
  chunked content and metadata, applies updates, resolves object and bucket
  links, checks listing and tombstones, and validates sealing. Its six-mode
  single-server authentication/TLS matrix passes all 18 cases across
  `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`. Stream updates omit
  default values for version-gated fields absent from an older server's INFO,
  preserving Object Store sealing compatibility on the pinned 2.12 release.
- Completed anonymous live cluster acceptance: the dedicated
  `runtest-interop-object-store-cluster.sh` runner uses the official Go peer
  and a three-node file-backed stream to verify cross-SDK content and metadata
  visibility, post-failure writes and reads, cleanup, seed loss, elected-leader
  loss, and durable seed restart. All nine cases pass across the three pinned
  releases. Multi-node, changed-advertisement, and broader
  management-operation failure topologies remain acceptance work.
  Its companion matrix wrapper repeats the nine default cells and supports
  bounded image and failure-mode selection. Thin authenticated companion
  wrappers now select this Object Store scenario in the existing NKey/JWT/mTLS
  cluster matrix; the five credential/TLS modes across seed, leader, and
  restart failures passed all 45 cells across nats-server 2.10.22, 2.12.15,
  and 2.14.5.
- Keep transfer chunks incremental; never require a whole object as one
  `string`.
- Preserve the metadata rollup as the commit point and define cancellation,
  partial-failure, digest, and cleanup behavior before each later surface is
  stabilized.

### Acceptance tests

- KV CAS success/failure, revisions, history, TTL, deletes/purges, finite
  scans, and watches are covered locally through the Eio mock transport.
- Watch cancellation and ordering under reconnect are covered locally.
- Ordered-watch retained/live and empty snapshots, metadata-only delivery, and
  consumer-gap replay are covered locally; the dedicated live cluster wrapper
  also verifies retained snapshot markers, revision continuity, and recovery
  after seed loss, elected-leader loss, and durable seed restart.
- The opt-in single-server runner covers bucket status, direct reads, CAS
  failures, history, tombstones, filtered keys, and a live watch against
  nats-server.
- The dedicated cross-SDK runner covers Go/OCaml revision ownership, stale CAS,
  delete and purge tombstones, watch ordering, and cleanup across the pinned
  single-server authentication/TLS matrix.
- The Object Store interop runner covers chunked content, metadata, links,
  listing, deletion and tombstones, updates, sealing, and cleanup against the
  official Go peer. Its six-mode single-server authentication/TLS matrix
  passes all 18 cases across the three pinned releases.
- The dedicated Object Store cluster runner covers the same cross-SDK content
  and metadata contracts through seed loss, elected-leader loss, and durable
  seed restart; all nine anonymous cases pass across the three pinned releases.
- Large Object Store transfer, metadata, replacement/deletion ordering,
  interrupted-transfer cleanup, listing/watch boundaries, links, rename,
  sealing, and bucket configuration updates are covered locally; broader
  authenticated and multi-node cluster/failure coverage remains acceptance
  work.
- No direct dependence by these modules on a private socket or private
  connection lifecycle.

### Gate G5 — durable-feature stabilization

Stabilize KV and Object Store independently if their release cadence or
dependency surface diverges. Require a review of streaming/backpressure and
revision semantics before documenting them as stable.

#### Current review status

- Completed: the public Key-Value and Object Store API review is recorded in
  [`docs/durable-feature-api-review.md`](docs/durable-feature-api-review.md).
  It checks switch ownership, Bytesrw transfer boundaries, cancellation,
  retained-snapshot markers, revision/CAS semantics, ordered-watch recovery,
  metadata commit ordering, tombstone cleanup, and structured error outcomes.
- Resolved: Object Store empty-snapshot termination, repeated tombstone-delete
  cleanup, and replacement cleanup for prior tombstone NUIDs are covered by
  focused mock-transport regressions. File-transfer partial-result behavior is
  now explicit in the public documentation.
- Remaining before a release claim: full authenticated and multi-node KV/
  Object Store failure evidence, plus the final acceptance evidence described
  in the production-readiness program.

## Phase 6 — Services over Core NATS

### Goal

Provide typed endpoint and monitoring helpers as a composition over Core
subscriptions, queue groups, and request/reply.

### Work

- Completed locally: define service, endpoint, and group values with
  consistent metadata; implement queue-backed endpoint workers and typed
  result-returning request handlers; implement `$SRV.PING`, `$SRV.INFO`, and
  `$SRV.STATS` monitoring responses; and make service shutdown use the same
  subscription and connection drain semantics without a second lifecycle
  manager.
- Completed locally: preserve monitoring and endpoint subscriptions across
  reconnect, collect per-endpoint request/error/timing statistics, and keep
  individual handler or response failures from taking down the service.
- Completed locally: expose Go-compatible service statistics reset and stopped
  state inspection, and carry validated endpoint pending message/byte limits
  through the generic subscription queue. Limit violations terminate the
  affected subscription as structured slow-consumer failures rather than
  blocking the connection owner.
- Completed locally: query `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS` for all
  services, named services, or individual instances through bounded fan-out
  collection windows with typed JSON response decoding.
- Completed locally: expose custom endpoint statistics data plus service error
  and completion callbacks. Statistics callbacks receive immutable endpoint
  snapshots; error and done callbacks run in FIFO order on a service-owned Eio
  dispatcher, outside the service mutex, with normal stop waiting for done
  delivery.
- Completed: the dedicated official-Go Service interop runner now checks
  bidirectional custom endpoint statistics data, and its matrix repeats the
  contract across `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5` under
  anonymous, token, username/password, NKey, JWT, mTLS, and their supported
  TLS variants. The shared authentication helper is used by the Service
  acceptance executable, so the advanced matrix modes exercise the OCaml
  client as well as the Go peer.
- Completed: the cross-SDK Service reconnect runner reuses the three-server
  failover harness, gates each kill on a round barrier after both peers have
  validated their requests and counters, and checks endpoint replay plus
  named INFO/STATS monitoring after each failover. Anonymous and TLS runs
  pass through the same official Go `micro` peer.
- Completed: the single-server Service failure runner uses the official Go
  client as a controlled publisher to overflow a bounded OCaml endpoint queue,
  validates the structured slow-consumer failure and failed Service state, and
  proves the parent connection remains usable. Anonymous and TLS runs pass.
- Completed: the parent-close runner closes the OCaml parent connection while
  a Service is active, waits for an after-close marker, proves the Go peer can
  no longer reach a stale endpoint, and checks clean explicit Service
  stopping. Anonymous and TLS runs pass.
- Completed: the Service failure and parent-close scenarios now run through
  the existing three-version, eleven-mode matrix. All 66 lifecycle cases pass
  across anonymous, token, username/password, NKey, JWT, mTLS, and TLS modes.
- Remaining: broader feature-family matrices and future server/SDK versions.
  The callbacks themselves remain local API behavior because they are not
  represented on the NATS service wire.

### Acceptance tests and gate G6

Local mock-transport tests cover configuration, monitoring wire payloads,
fan-out discovery, malformed response rejection, queue policy,
request/service-error replies, statistics and reset, stopped-state transitions,
pending-limit backpressure, reconnect replay, service-local drain, and
parent-connection isolation, custom statistics data, and lifecycle callbacks.
The opt-in server runner now covers one
live service's monitoring, endpoint/group requests, service errors, failure
isolation, statistics, and drain behavior, plus two-instance queue-group
routing. The two-server reconnect runner also checks Service endpoint and
monitoring recovery after active-server loss. The dedicated Service interop
runner now cross-checks the OCaml implementation against the official Go
`nats.go` `micro` SDK for bidirectional endpoint requests, service-error
headers, named INFO/STATS discovery, queue and metadata declarations, exact
counters, custom endpoint statistics data in both directions, and a request/
reply completion barrier. The Service matrix repeats this exchange across the
three pinned server releases and eleven anonymous/token/username-password/
NKey/JWT/mTLS plaintext/TLS modes. Its selectable Service failure and
parent-close scenarios pass all 66 lifecycle cases across that same matrix;
future server versions and additional cluster-level failure topologies remain
follow-up work.
Services may start after G2 and do not block JetStream,
KV, or Object Store. Stabilize only after confirming that all service behavior
composes with the Core connection ownership model.

## Phase 7 — Privileged system-account administration

### Goal

Provide an explicitly optional package for operators who have access to the
NATS system account, without adding privileged subjects or JSON schemas to the
ordinary `nats-eio` package.

### Work

- Completed: add the `nats-eio-system` package over the existing Eio Core
  connection, with validated server/account targets and server-name, cluster,
  host, exact-match, tag, and JetStream-domain selectors.
- Completed: cover the current server monitor services and account `INFO`,
  `STATZ`, and connection-tracking endpoints through targeted or bounded
  fan-out request/reply. Typed endpoint options cover connection and
  subscription pagination/details, routing, gateway, leaf-node, account,
  JetStream, health, profile, IP-queue, and Raft filters. Preserve complete
  `Jsont.json` response bodies and structured API errors so newer server fields
  do not become decode failures.
- Completed: implement targeted configuration reload, client kick, and client
  lame-duck (`LDM`) controls. Keep control mutations separate from monitoring
  and require an explicit server target and non-negative client id.
- Completed: expose scoped `$SYS` event subscriptions for server lifecycle,
  server statistics/authentication, and account connection/leaf-node events,
  while retaining unknown event subjects and payloads for forward compatibility.
- Completed: make shared Eio subscription acquisition cancellation-safe, so a
  cancellation racing the wire `SUB` setup revokes the created subscription
  before the cancellation escapes; add a regression test for that ownership
  boundary.
- Completed: add local Eio mock coverage for target/selector validation,
  monitoring envelopes, fan-out collection, controls, and event classification,
  plus a real `nats-server` system-account runner covering targeted monitoring,
  fan-out, account monitoring, reload, and a live account-connect event.
- Completed: parameterize that runner over ordered client endpoints and add a
  reusable three-node routed system-account fixture. The pinned
  `nats:2.14.5` scenario now verifies exact system-monitor fan-out, primary
  loss and Core reconnect, reduced post-failure fan-out, and replay of the
  account event subscription on a recovered connection.
- Completed: add a cached-image system-account matrix for
  `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`. It uses the same small
  ephemeral fixture for each release and refuses to pull missing images.
- Completed: expand the routed system-account matrix to username/password,
  username/password over TLS, NKey, NKey over TLS, JWT, JWT over TLS, and
  certificate-mapped mTLS. The 21-cell release/authentication matrix covers
  monitoring, system events, reload, and failover recovery, and the Core API
  now models certificate authentication with `Nats.Auth.tls`.

### Remaining

- Consider operator JWT claims/user-management requests only as a separately
  reviewed authorization feature; they are not part of this first package.

## Phase 8 — Operational polish and optional integrations

Only pursue these after the core feature waves are stable and a concrete user
needs them:

- typed payload codec helpers and documentation examples;
- Completed: the dependency-free `Connection.stats` snapshot and lifecycle
  event observation boundary, plus the optional `nats-eio-opentelemetry`
  package for cumulative metrics and payload-free lifecycle spans. Message
  context propagation and redaction remain application-owned;
- a dedicated credential/NKey/JWT package if in-repo auth becomes too large;
- alternative transports such as WebSocket (currently out of scope; revisit
  only if a concrete requirement justifies a separate adapter);
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

- NKey/JWT package boundary and private-key parsing;
- reconnect-buffer occupancy metrics and probes;
- WebSocket and second-runtime package boundaries;
- metrics/probe naming and payload policy.

The following are already chosen and must not be reopened by harness work:
single protocol-owner fiber, bounded subscription and event queues with
explicit slow-consumer behavior, `Mtime.Span.t` as the public timeout type,
unknown JetStream JSON-field preservation, Core-first stabilization,
client-assigned sids,
the `Op`/`Message` split, explicit deliveries, reader ownership,
Go-compatible bounded reconnect buffering without mutation reconciliation,
structured errors, and distinct drain/close semantics.
