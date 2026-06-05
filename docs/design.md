# NATS client SDK for OCaml

## Status

This design began as a proposal based on the maintained NATS clients and the
NATS protocol documentation available on 2026-08-10. It remains a design
document rather than a promise that every planned surface is stable; concrete
APIs that have been implemented are called out explicitly and should continue
to pass the stability gates below.

The recommendation is to build a feature-complete SDK in waves around a small,
runtime-independent protocol core and an Eio integration. The core should own
the protocol state machine; the Eio layer should own sockets, fibers, queues,
timers, reconnect attempts, and the user-facing blocking/direct-style API.

The first API-stability gate should cover Core NATS and its Eio facade. The
longer-term SDK target is JetStream, Key-Value, Object Store, and Services, but
those surfaces should stabilize only after the Core NATS connection,
subscription, reconnect, and drain semantics have been exercised against a
real server. NATS Streaming is intentionally out of scope. It is a separate,
legacy protocol and compatibility with its APIs or data formats would weaken a
new library without helping the NATS design.

## 1. Research scope

The official NATS ecosystem classifies clients as Tier 1, which track server
releases, and Tier 2, which may lag in features. The Tier 1 set is Go,
JavaScript/TypeScript, Python, Java, Rust, .NET, and C; this document gives
those clients the most weight. The maintained Tier 2 set is Zig, Swift, Ruby,
and Elixir. The classification is maintained by NATS in the
[official ecosystem guide](https://docs.nats.io/concepts/ecosystem).

### Maintained SDK comparison

| SDK | General runtime and receive model | Feature shape | Design lesson for OCaml |
| --- | --- | --- | --- |
| [Go](https://github.com/nats-io/nats.go) | A mutable connection object; callback, synchronous, and channel subscriptions; request/reply and flush on the connection. | Core NATS, JetStream, KV, Object Store, and the `micro` Services API. The newer JetStream package separates stream, consumer, and message concerns and supports pull, push, ordered, fetch, and continuous consumption. | The reference client has excellent protocol coverage, but its several receive styles and historical APIs should not all become separate OCaml abstractions. |
| [JavaScript/TypeScript](https://github.com/nats-io/nats.js) | A runtime-independent base client with transport packages for Node, Deno, Bun, and browser WebSockets. Subscriptions are async iterators by default, with callbacks as an explicit alternative. | Core, JetStream, KV, Object Store, and Services are separate modules over a common connection interface. Orbit helpers are intentionally separated from direct protocol-parity APIs. | A strong model for separating protocol logic from transports and for making the common receive path compositional. |
| [Python](https://github.com/nats-io/nats.py) | Python 3 `asyncio`; subscriptions can use callbacks or async iteration. Connection options cover reconnect, TLS, authentication, and lifecycle callbacks. | Core and JetStream are prominent in the main client; the maintained documentation also exposes KV and Object Store APIs. | Async iteration is a useful usability reference, but an OCaml API should use direct style and typed results rather than Python-style callback conventions. |
| [Java](https://github.com/nats-io/nats.java) | A connection with synchronous subscriptions, `Future` requests, and dispatcher callbacks. Builders expose connection, TLS, authentication, reconnect, and executor choices. | Core, JetStream, KV, Object Store, and a Service Framework. The client has typed request failure reasons and a deliberate distinction between core and higher-level APIs. | Typed failure categories and an explicit low-level escape hatch are valuable; exposing executor ownership as a central concern is not. |
| [Rust](https://github.com/nats-io/nats.rs) (`async-nats`) | Tokio-based async client. A cloneable client handle produces `Subscriber` values implementing `Stream`; subscriber drain and unsubscribe are explicit. | Core, JetStream management and consumption, KV, Object Store, and Services, with typed consumer and acknowledgement models. | The typed resource/stream shape is the closest conceptual analogue to an OCaml module API, although the first OCaml adapter should not be tied to Tokio-like runtime assumptions. |
| [.NET](https://github.com/nats-io/nats.net) | Modern API is async-first. `NatsClient` is a high-level facade and `NatsConnection` is the lower-level connection; subscriptions use `IAsyncEnumerable`/channels. | Split packages cover Core, JetStream, KV, Object Store, Services, serializers, and dependency injection. | Progressive disclosure works well: make the normal path short while retaining a lower-level connection and typed payload escape hatch. |
| [C](https://github.com/nats-io/nats.c) | Opaque connection/subscription/message handles with explicit destruction; callback and synchronous subscriptions. The default implementation uses threads, with libevent/libuv integration points. | Core, JetStream, KV, Object Store, and `micro` Services, including reconnect, TLS, authentication, headers, pending limits, and slow-consumer reporting. | Explicit ownership and event-loop seams matter. The threading model belongs in an adapter, not in the protocol model. |

The current Tier 2 implementations are useful for edge cases but should not
drive the first architecture:

| SDK | Current shape and notable boundary |
| --- | --- |
| [Zig](https://github.com/nats-io/nats.zig) | Core, JetStream, KV, and Micro Services with an explicit allocator and an async/future-oriented standard-library design; Object Store and mTLS are not complete in the current README. |
| [Swift](https://github.com/nats-io/nats.swift) | Core NATS with Swift async/await and `AsyncSequence`; JetStream, KV, Object Store, and Services are presented as future work. |
| [Ruby](https://github.com/nats-io/nats-pure.rb) | Thread-safe callback and blocking request APIs, with Core, JetStream, and Services. |
| [Elixir](https://github.com/nats-io/nats.ex) | A GenServer/OTP process model with supervised reconnecting consumers and a Services implementation; its concurrency and ownership model is intentionally unlike an OCaml library. |

This survey intentionally does not treat old language-specific repositories or
NATS Streaming clients as architectural authorities. The official ecosystem
page is the source of truth for the maintained-client set.

## 2. What the protocol and product require

NATS is a line-oriented protocol over a byte stream. Control lines and payloads
are terminated by CRLF. The core vocabulary is `INFO`, `CONNECT`, `PUB`,
`HPUB`, `SUB`, `UNSUB`, `MSG`, `HMSG`, `PING`, `PONG`, `+OK`, and `-ERR`.
Headers add a second length pair around the header block and payload. `INFO`
may arrive asynchronously after the initial server information, and protocol
version 1 enables dynamic server discovery. These details are specified in the
[NATS protocol reference](https://docs.nats.io/reference/reference-protocols/nats-protocol).

The header block begins with the `NATS/1.0` version line and can carry repeated
fields such as `Status` and `Description`; that version line is framing, not an
ordinary application header. The Core connection should negotiate headers by
default because no-responders and several modern server features depend on
them. The client must also enforce the negotiated `max_payload` from `INFO`
before emitting a publish.

The feature set that a modern client must make coherent is:

| Area | Observed behavior in official clients | Required design consequence |
| --- | --- | --- |
| Subjects | Case-sensitive dot-separated tokens; `*` matches one token and `>` matches the remaining tail. Publish subjects cannot be subscription wildcards. | Validate ordinary subjects and subscription filters at construction. Give the two concepts distinct types so invalid publish calls are unrepresentable. |
| Messages and headers | Messages carry subject, optional reply subject, payload, and optional multi-valued headers. Header names are case-insensitive for lookup but their wire spelling is observable. | Keep payload opaque and immutable. Preserve header values and original spelling while providing case-insensitive lookup. |
| Subscription identity | The client chooses a numeric subscription id in `SUB`; `MSG`/`HMSG` frames echo it. Queue groups, explicit unsubscribe, and auto-unsubscribe are standard. | Let `Client` allocate and own server ids while preserving subscription intent across reconnect. Expose an owned `Subscription.t` with `unsubscribe`, `drain`, `next`, and auto-unsubscribe operations. |
| Request/reply | Clients create inbox subjects. Several official clients multiplex requests through one wildcard inbox; a per-request subscription remains useful when multiplexing is disabled. `503 No Responders` is a first-class failure when headers/no-responders are negotiated. | Provide one request operation with a structured timeout/no-responder result. Hide inbox subscriptions by default, but retain a low-level option for debugging and unusual routing. |
| Flush and readiness | `flush` is a protocol barrier implemented with `PING`/`PONG`; a successful local write is not proof that the server processed a subscription or publish. | Expose `flush` as an explicit server-confirmation operation and document the distinction from socket write completion. |
| Reconnect and discovery | Official clients reconnect by default, discover servers from `INFO`, restore subscriptions, and expose reconnect/lifecycle events. Reconnect buffers and retry limits are configurable. | Make reconnect policy part of configuration. Keep subscription intent separate from transient wire state so the protocol layer can replay `SUB` state safely. |
| Drain and close | Close is immediate. Drain stops new work, drains subscriptions, flushes remaining output, and then closes; subscription drain processes already-cached messages. | Model `close` and `drain` as distinct terminal transitions. Make drain observable and cancellation-safe. See the [NATS drain guide](https://docs.nats.io/using-nats/developer/receiving/drain). |
| Slow consumers | A server can disconnect a slow client; clients also impose pending limits and report local slow-consumer conditions. | Use bounded per-subscription queues. Never silently drop messages; report the condition and unsubscribe or fail according to an explicit policy. |
| Security | CONNECT supports token, username/password, NKey/JWT-related fields, no-echo, headers, and TLS negotiation. TLS certificate handling belongs to the transport. | Keep credentials and nonce signing behind an authentication interface. Do not put private-key storage or TLS implementation in the pure protocol package. |
| Cluster behavior | Multiple seed URLs, server-provided URLs, reconnect delay/backoff, max attempts, and lame-duck/connection lifecycle events are normal client features. | Represent configured and discovered servers separately, deduplicate them, and make the chosen server/event history inspectable without exposing mutable socket state. |
| JetStream | A request/reply API layered over Core NATS. Streams own retained messages; consumers own delivery/ack state. Pull, push, ordered, durable, ephemeral, filtering, batch fetch, heartbeats, and acknowledgement policies are all important. | Make JetStream a typed capability over the same connection and subscription primitives, not a second transport. Separate management handles from consumption handles. |
| Key-Value | A JetStream-backed bucket API with revisions, compare-and-set create/update, TTL, delete/purge, history, keys, and watches. | Expose revisions and operation kinds as typed values. Watch results must preserve ordering and cancellation. |
| Object Store | A JetStream-backed chunked blob API with metadata, streaming get/put, list/watch, links, and seal/update operations. | Stream data; do not require the whole object in memory. Keep object metadata and the underlying message sequence separate. |
| Services | A convention over Core NATS: endpoint/group definitions plus discovery and monitoring subjects such as `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS`. | Build Services from ordinary subscriptions, queue groups, and request/reply, adding typed endpoint and monitoring helpers rather than a parallel transport. |

The broader product surface—Core, Services, JetStream, KV, Object Store,
reconnect/drain, security, and WebSockets—is reflected in the official
[NATS concepts guide](https://docs.nats.io/learn/). WebSockets are a transport
feature, not a reason to make the protocol model browser- or socket-specific.

## 3. Design goals expressed as caller code

These sketches are the tests for the public shape. They are illustrative, not
compiled API declarations.

### Core publish and subscription

The common case should be direct style, with no callback registry or promise
plumbing in application code:

```ocaml
let run conn =
  let subject = Nats.Subject.literal "orders.created" in
  let filter = Nats.Subject.Filter.literal "orders.*" in
  let sub = Nats_eio.Connection.subscribe conn filter in
  match Nats_eio.Connection.publish conn ~subject "order-123" with
  | Error error -> report_error error
  | Ok () ->
      (match Nats_eio.Connection.flush conn ~timeout:(Mtime.Span.of_sec 1.) with
       | Error error -> report_error error
       | Ok () ->
           let rec consume () =
             match Nats_eio.Subscription.next sub with
             | Ok message ->
                 handle_order message;
                 consume ()
             | Error Nats.Error.Closed -> ()
             | Error error -> report_error error
           in
           consume ())
```

`literal` is reserved for programmer-written static names; configuration and
user input use result-returning constructors.

### Request/reply and failure categories

Timeout, no responders, a server `-ERR`, cancellation, and a closed
connection must not be distinguished by parsing a human-readable string:

```ocaml
match Nats_eio.Connection.request conn
        ~timeout:(Mtime.Span.of_sec 2.)
        (Nats.Subject.literal "orders.lookup")
        "order-123" with
| Ok reply -> use_reply reply
| Error Nats_eio.Error.No_responders -> handle_missing_service ()
| Error Nats_eio.Error.Timeout -> retry_or_fail ()
| Error (Nats_eio.Error.Protocol error) -> report_core_error error
| Error error -> report_error error
```

### A queue worker with graceful shutdown

Queue groups and drain should compose with ordinary structured concurrency:

```ocaml
let worker =
  Nats_eio.Connection.subscribe conn
    ~queue:(Nats.Queue_group.literal "order-workers")
    (Nats.Subject.Filter.literal "orders.created")
in

Eio.Switch.run @@ fun child_sw ->
  Eio.Fiber.fork ~sw:child_sw (fun () ->
    Nats_eio.Subscription.iter worker ~f:process_order);
  wait_until_shutdown_requested ();
  Nats_eio.Subscription.drain worker;
  Nats_eio.Connection.drain conn
```

The switch that creates a connection owns its termination. Releasing or
failing that switch cancels the connection fibers and performs an immediate
`Closed` finish rather than a best-effort drain; blocked receives therefore
end through cancellation or `Error Closed`.

### JetStream pull consumption

JetStream exposes a typed consumer rather than making users assemble API
subjects and decode acknowledgement metadata themselves. The current Eio
surface separates a local consumer handle from a persistent pull session:

```ocaml
let consume ~sw consumer =
  match Nats_eio.Jetstream.Consumer.Pull.v ~sw consumer with
  | Error error -> report_error error
  | Ok pull ->
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Jetstream.Consumer.Pull.close pull))
        (fun () ->
          match
            Nats_eio.Jetstream.Consumer.Pull.iter pull ~f:(fun message ->
                match process_order message with
                | Ok () -> ignore (Nats_eio.Jetstream.Msg.ack message)
                | Error _ -> ignore (Nats_eio.Jetstream.Msg.nak message))
          with
          | Ok () -> ()
          | Error error -> report_error error)
```

`Consumer.fetch` remains available for one-shot pulls. `Consumer.Pull` owns a
fresh reply inbox subscription, keeps at most one server pull outstanding, and
uses the requested batch/count and byte limits for the session. `next` and
`iter` do not acknowledge messages; acknowledgement is an explicit operation
on the typed `Msg.t`. A local timeout leaves the current server request
outstanding so a later call can receive its message, while a transport loss
returns a structured error and requires the caller to create a new session.
The session is single-owner and closes its subscription with its switch or
through `close`.

Consumer management also exposes a typed `Consumer.update`. It performs an
INFO/read-modify-write cycle and sends the update action through the named
`CONSUMER.CREATE` endpoint, matching the server's action-bearing management
request. The configuration is a full replacement of modeled fields; the
`Consumer.Config.with_*` combinators make it possible to derive a replacement
from `Info.config` without rebuilding every field. The handle name remains the
consumer identity: an omitted durable name retains an existing durable
identity, an explicit name must match, and configuration members not modeled by
the public `Config.t` are preserved from the preceding INFO response. The
modeled configuration includes consumer metadata, sample frequency, push rate
limit, replica inheritance, mutually exclusive singular/multi-subject filters,
and nanosecond redelivery backoff schedules, plus a typed UTC pause deadline.
Create omits unset optional values while update uses explicit server clear
sentinels for those fields: an empty
singular filter, empty multi-subject list, or empty backoff list. The server
uses the first backoff delay as the effective acknowledgement/redelivery wait;
callers should keep it consistent with `ack_wait` when both are set. Push
recovery carries these modeled delivery fields into ephemeral consumer
recreation. Pause/resume use the dedicated `CONSUMER.PAUSE` endpoint; consumer
updates preserve the current server pause deadline because the create/update
endpoint does not mutate it.
Concurrent updates intentionally use last-writer-wins semantics.

Priority-group consumers are pull-only and are modeled as an extension of the
same consumer configuration and pull session. A configuration names one
validated group and selects `overflow`, `pinned_client`, or
`prioritized` policy; overflow thresholds and prioritized levels belong to an
individual pull request, not to the consumer handle. The public request
surface follows the server's JSON fields (`group`, `min_pending`,
`min_ack_pending`, and `priority`) and validates group syntax, non-negative
thresholds, and the inclusive priority range 0--9 before publishing.

Pinned-client sessions keep the opaque `Nats-Pin-Id` returned in a delivery
header in the consumer handle's per-group pin table. Later `Pull` requests and
subsequent one-shot `fetch` calls on that handle echo it without exposing it as
application state. A 423 pin-mismatch status clears the local identifier and
causes the outstanding pull or fetch request to retry without it; the explicit
`Consumer.unpin` operation uses the server's `CONSUMER.UNPIN` endpoint. INFO
projections expose the group's configured name, pinned client id, and pin
timestamp. Priority policy and group identity are preserved across consumer
updates; changing that identity through the typed update API fails rather than
silently changing failover semantics. The initial implementation follows
ADR-42 and rejects multi-group configurations; future server-side multi-group
consumers and transparent priority-pull restoration after transport recovery
remain outside this slice.

The wire behavior is based on the [NATS priority groups
documentation](https://docs.nats.io/learn/jetstream/priority-groups) and
[ADR-42](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-42.md).

The high-level API still exposes raw request/reply and raw NATS messages for
advanced JetStream features that arrive before a convenience wrapper.

Both one-shot fetch and persistent pull sessions accept an opt-in
[`idle_heartbeat`](https://docs.nats.io/nats-concepts/jetstream/consumers)
interval. The request codec sends the interval in nanoseconds, status-100
heartbeat deliveries are consumed internally, and a missing heartbeat after
two intervals fails the operation with a structured error. Pull sessions use
heartbeat deadlines alongside caller timeouts and drain already queued control
deliveries before declaring the session unhealthy; a normal local timeout still
leaves the outstanding server request available for a later call.

### Ordered JetStream consumption

Ordered consumption is a sibling of `Consumer.Pull`, not a wrapper around the
push-session API. `Consumer.Ordered.v` creates a client-managed ephemeral pull
consumer and owns its reply subscription. The client fixes the consumer to
no-ack, one-replica memory-backed storage, and a bounded inactive threshold;
the caller chooses the initial delivery policy, filter, and pull limits.

The ordered state machine tracks two distinct cursors. Delivered consumer
sequence numbers must be consecutive, while stream sequence numbers are the
resume cursor and may skip when a filter excludes messages. A gap, missing
heartbeat, consumer-deleted status, or a disconnect that does not replay the
pull subscription tears down the current generation and creates a new one at
the next stream sequence. The initial delivery policy is used only for the
first generation. A normal caller timeout leaves the current generation open;
an absolute timeout covers both waiting and recovery work.

This behavior is covered through the local Eio mock transport and a live
cross-SDK acceptance runner. The runner covers both a seed-node transport
failure and an elected JetStream stream-leader failure. After either failure,
each client either continues the existing Ordered consumer at the next
consumer sequence or recreates it from the next stream sequence if the
one-replica consumer leader was also lost. Consumer identity is therefore not
an invariant of endpoint or stream-leader recovery. The anonymous plaintext
matrix passes both failures on `nats:2.10.22`, `nats:2.12.15`, and
`nats:2.14.5`; authentication, TLS, and broader failure matrices remain
final-acceptance work.

### The protocol core as a testable boundary

An adapter should be able to drive the protocol without a socket:

```ocaml
let state = Nats.Client.v config in
let state, output = Nats.Client.outgoing state command in
let state, transition =
  Nats.Client.incoming state ~now reader
in
(* transition.output is wire data; events and deliveries are separate. *)
```

The exact return record may evolve, but the invariant is fixed: bytes in and
bytes out are separate from events, and the core does not perform I/O, sleep,
DNS, TLS, randomness, or callbacks.

## 4. Architectural alternatives

Five credible designs were considered against the examples above.

| Alternative | What it makes easy | What it makes hard | Decision |
| --- | --- | --- | --- |
| Mutable Eio connection as the whole library | A short application API; natural blocking operations. | Deterministic protocol tests, alternate transports, parser fuzzing, and reuse from another runtime. Protocol state and resource ownership become entangled. | Reject as the library boundary; retain this shape as the `nats-eio` facade. |
| Pure protocol state machine only | Interop testing, formal reasoning, and transport portability. | Every user must implement connection ownership, reconnect, queues, and cancellation. It is a protocol toolkit, not an SDK. | Reject as the only public layer; make it the load-bearing core. |
| Functorized runtime/backend abstraction | A single source-level design for Eio, Lwt, Unix, and future runtimes. | Functor plumbing leaks into every caller and does not remove the need to specify ownership and backpressure. It over-generalizes before a second backend exists. | Reject initially; keep the core I/O-free so a later adapter can be added without redesigning it. |
| Callback/dispatcher-first API | Familiarity for Go, C, Java, and Python users; easy background delivery. | Inversion of control, exceptions/effect handling in callbacks, difficult backpressure, and awkward composition with Eio fibers. | Reject as the default; offer a small callback/iteration bridge over owned subscriptions. |
| Pure core plus Eio direct-style facade | Deterministic protocol implementation, one coherent runtime API, and room for future adapters. | Requires a deliberate ownership protocol and a little adapter machinery. | Recommend. This is the best balance of portability, modern OCaml ergonomics, and feature completeness. |

This also settles the package question. Do not publish a package per NATS
feature at the beginning. Keep the pure protocol package small and make one
runtime package expose Core, JetStream, KV, Object Store, and Services as they
are implemented, all over the same connection. Stabilize those modules in
separate waves; split optional features later only if dependency or release
pressure demonstrates a real need.

## 5. Recommended architecture

### Package boundary

The initial public packages should be:

```text
nats
├── Nats.Subject
├── Nats.Queue_group
├── Nats.Header
├── Nats.Message
├── Nats.Op
├── Nats.Error
├── Nats.Event
├── Nats.Codec
├── Nats.Packet
└── Nats.Client

nats-eio
├── Nats_eio.Connection
├── Nats_eio.Subscription
└── Nats_eio.Jetstream
```

`nats` must not depend on Eio, Lwt, Unix, TLS, DNS, or a particular socket
implementation. Its likely dependencies are limited to the byte-reader and
time/value libraries needed to express an incremental protocol. `nats-eio`
owns network dialing, TLS, DNS, timers, fibers, channels, cancellation, and
buffer limits.

The narrow waist is the pure transition

```text
bytes -> Packet -> Op -> Client -> (wire output, lifecycle events, deliveries)
```

Every runtime adapter and every higher-level feature uses that transition and
the shared application `Nats.Message.t`; no feature gets a private socket
path. `Op` is the protocol's wire AST. `Message` is the application message
carried by publish and delivery operations; keeping those concepts separate
prevents control lines and application data from becoming one overloaded type.

### Domain modules

#### Subjects and queues

`Nats.Subject.t` represents a valid publish or reply subject. A distinct
`Nats.Subject.Filter.t` represents a subscription filter and is the only type
that can contain wildcards. `Nats.Queue_group.t` represents a queue group name.

Each module should provide a result-returning parser, a programmer-literal
constructor for fixed source-level values, a formatter, and accessors only
where they improve composition. No caller should need to know the wire rules
for wildcard placement.

#### Headers and messages

`Nats.Header.t` is an immutable, multi-valued map. It must:

- preserve all values and their insertion order for wire encoding;
- perform case-insensitive lookup;
- preserve the first/original spelling when re-encoding;
- distinguish absent, present-with-empty-value, and repeated values.

`Nats.Message.t` contains an immutable payload (`string`), a subject, an
optional reply subject, and headers. Payloads remain opaque: JSON, CBOR,
binary codecs, and application-specific formats belong in small codec values
passed to higher-level operations. The default API should not require a PPX or
make JSON the protocol payload.

#### Errors and events

`Nats.Error.t`, introduced with `Nats.Client`, is a closed, structured core
variant covering:

- invalid subject/filter/header/configuration;
- protocol framing or decoding failure;
- malformed typed INFO data, closed/draining state, and subscription identity;
- negotiated header and payload-limit violations.

The Eio facade adds authentication/TLS/connection/reconnect failures, timeout,
cancellation, no-responders, and slow-consumer outcomes around this core. The
wire server error text remains a structured field rather than a value callers
must parse. JetStream API errors belong to the JetStream module and carry
status, code, description, and optional metadata.

Human-readable `message`/`pp` functions are for CLI/logging only. Callers and
tests must match structured constructors and fields.

`Nats.Event.t`, also introduced with `Nats.Client`, is separate from wire
output. The pure machine reports server INFO updates, connected, lame-duck
mode, server errors, protocol notices, flush completion, drain, and close. The
Eio facade adds reconnect, disconnection, and slow-consumer events. An
application message is delivered through a subscription, not hidden in a
generic lifecycle callback.

### The pure protocol layer

The pure package has four protocol responsibilities plus the application
message model:

1. `Nats.Message` and `Nats.Header` model application data.
2. `Nats.Op` is the closed, phase-blind wire AST for `INFO`, `CONNECT`, `PUB`,
   `MSG`, `PING`, and the other control operations.
3. `Nats.Codec` maps `Op` values to and from bytes; `Nats.Packet` handles CRLF
   framing, length checks, header-block bounds, and incremental reader
   progress.
4. `Nats.Client` is the asymmetric client state machine over `Op` values.

`Nats.Op` is intentionally separate from `Nats.Message`: a `MSG` operation
contains an application message, while `INFO`, `SUB`, and `PING` do not. The
codec is phase-blind; only `Client` decides whether an operation is valid in
the current connection phase. There is no public `Nats.Protocol` module: the
curated top-level interface and the `Op`/`Codec` pair are the protocol surface.

`INFO` and `CONNECT` JSON is intentionally opaque at this layer. `Codec`
checks that the operation carries a non-empty, line-safe JSON string but does
not interpret its fields. `Client` owns typed negotiation data such as
`max_payload`, header support, no-responders support, server URLs, nonces, and
lame-duck state. `Packet.default_limits` are defensive parser bounds; a client
must apply the negotiated `max_payload` separately when encoding publishes.

`Nats.Client.t` is abstract. It owns protocol facts such as connection phase,
server information, next client-assigned subscription id, active subscription
intent, pending protocol barriers, negotiated features, and discovered server
URLs. It does not own a socket, a fiber, a mutex, a clock, a buffer, a random
generator, or an application callback.

The state machine should expose canonical transitions along these lines:

```text
Client.v       : config -> Client.t
Client.outgoing: Client.t -> command -> (Client.t * wire_output, Error.t) result
Client.incoming: Client.t -> now -> byte_reader -> transition
Client.timer   : Client.t -> now -> transition
Client.next_timeout: Client.t -> Mtime.t option
```

`command` is a closed client-owned vocabulary for publish, subscribe,
unsubscribe, ping/flush barriers, and connection intent. Adapters do not
construct arbitrary `Op.SUB` values: `Client` allocates sids, tracks
auto-unsubscribe counts, and keeps the intent needed for reconnect replay.

The final signature may return records rather than tuples, but the separation
must remain:

- wire output is what the adapter writes;
- events are what the adapter observes;
- application deliveries are a separate result channel, each carrying the
  client-assigned sid and a `Message.t`;
- fatal errors are structured and may include required closing output;
- time is supplied by the caller. Core NATS inbox names and reconnect jitter
  are adapter concerns; any future core randomness is an explicit input.

The transition result should make the delivery channel explicit, for example:

```text
type delivery = { sid : int; message : Message.t; status : Op.status option }
type transition = {
  state : Client.t;
  output : string list;
  events : Event.t list;
  deliveries : delivery list;
}
```

`Event.t` contains only observations the pure machine can emit: typed INFO,
connected, protocol notices, flush completion, server errors, drain, and close.
Reconnect, disconnection, and slow-consumer events belong to the Eio facade. A
delivery is not an event: it has a different consumer, status metadata,
backpressure policy, and ownership path in the Eio layer.

The parser must accept asynchronous `INFO`, interleaved control messages, and
both payload-bearing and header-bearing message forms. One `incoming` call
consumes at most one complete operation; the adapter loops while the reader
has data, bounding deliveries and output per transition. Partial bytes remain
in that reader, never in `Client.t`. It must reject invalid lengths, malformed subjects,
incomplete control lines at EOF, and protocol violations without exceptions
escaping the boundary.

`Packet.read` returning `Need_more` means that the same reader is retained and
retried after the transport appends bytes. A successful packet is consumed
before `Codec.decode` runs; a subsequent codec error therefore poisons the
stream just like a framing error. The client must close or reconnect after
either fatal case, never skip bytes and continue.

### The Eio connection layer

`Nats_eio.Connection.t` is the owned runtime capability. Creating it starts a
single protocol owner that serializes application commands and socket input
through the pure `Nats.Client` state machine. This avoids races between a
reader, reconnect logic, and concurrent publishers without making protocol
state mutable or globally shared.

The connection owns:

- configured endpoint seeds, the current discovered endpoint set, and
  deterministic candidate selection/rotation;
- the current TCP/TLS flow and connection phase;
- reconnect backoff, attempt limits, jitter, and retry policy;
- bounded per-subscription delivery queues;
- request waiters and the shared inbox multiplexer;
- a bounded lifecycle event stream with an explicit overflow policy;
- cancellation and final resource cleanup.

The current Eio milestone implements a recovery bridge behind this ownership
boundary: one unexpected transport loss fails in-flight requests, flushes, and
drains; preserves live subscription handles, queues, and replay intent; closes
the old flow idempotently; and redials through the stored connection seam. The
connection accepts validated endpoint seeds, resolves each candidate again for
every dial pass, tries every returned stream address, prefers the successful
endpoint, and rotates failures. An `INFO` replaces the discovered candidate
set while configured seeds remain sticky; both full endpoint URLs and bare
`host[:port]` advertisements are accepted. Initial handshake failures fail
over across remaining configured seeds. The first redial is immediate, later
attempts use a configurable capped exponential backoff, and the bridge
re-enters the INFO/TLS/CONNECT handshake for each attempt. It emits
non-terminal `Disconnected` and `Reconnected` events, defers unsubscribe and
auto-unsubscribe commands until the replacement session is connected, and
leaves ordinary publishes and pending requests unreplayed. Explicit `tls://`
candidates perform bounded TLS before the NATS handshake, while peer identity
and SNI remain caller-owned through `Tls.Config.client`. A live two-server
acceptance test also holds a request across active-server failure and verifies
that it fails as `Disconnected` rather than being replayed. Delayed reconnects
support bounded configurable jitter while retaining a deterministic backoff
base; the opt-in real-server harness covers single-server Core NATS
publish/subscribe, headers, queue groups, request/reply, no-responders, token
and username/password authentication, server-required TLS, request timeout and
cancellation cleanup, auto-unsubscribe, subscription drain, connection drain,
bounded slow-consumer handling, parent-switch cleanup, reconnect recovery,
three-node cluster discovery/failover with subscription recovery, and lame-duck
INFO/event handling with continued use of the existing connection. Advanced
cluster failure scenarios and cross-SDK acceptance remain later work.

The normal user operations should be direct-style and result-returning:

```text
Connection.connect
Connection.publish
Connection.publish_msg
Connection.subscribe
Connection.request
Connection.flush
Connection.events
Connection.drain
Connection.close

Subscription.next
Subscription.iter
Subscription.unsubscribe
Subscription.drain
Subscription.auto_unsubscribe
```

`publish` accepts a subject, optional reply subject, optional headers, and an
immutable payload. `publish_msg` accepts a pre-built message for callers that
need exact control. Core publish acknowledges successful handoff to the local
connection machinery, not server processing; callers needing a server barrier
use `flush`, and callers needing durable acknowledgement use JetStream publish.

#### Receive and backpressure policy

The default subscription is pull-based: `next` waits for a message and
`iter` is a convenience loop. An adapter-level callback helper can be built on
the same primitive, but the protocol core never invokes user code.

Every subscription has a bounded queue. The default policy is correctness over
silent loss: when the queue is full, the subscription is failed or locally
unsubscribed and a structured slow-consumer event is emitted. Optional
explicit policies may drop with an event or use a larger/unbounded queue, but
there must be no implicit loss. The policy is local and independent from the
server's pending limits.

#### Requests

Requests use a private inbox prefix and a shared wildcard subscription by
default, as in the modern JavaScript and Zig clients. The implementation must
also support a no-multiplexing/per-request subscription mode for diagnostics
and compatibility with unusual servers. A request completes with exactly one
of:

- a response message;
- `No_responders` when negotiated and received;
- timeout;
- cancellation;
- connection close/drain;
- a structured server error.

Pending requests must not be silently replayed across reconnect. A future
option may make selected requests retryable, but it must be opt-in and tied to
an idempotency policy. Request timeout is an Eio waiter concern; the pure
`Client.timer` handles protocol liveness such as ping/stale detection and must
not become a global timer for application RPCs. Response, timeout, disconnect,
and cancellation races must complete each waiter exactly once.

#### Reconnect, drain, and close

Reconnect is enabled by default. The current adapter performs one immediate
redial followed by configurable retries after an unexpected transport loss.
The default allows three redial attempts; `None` permits unlimited attempts,
and `Some 0` makes the transport loss terminal. The connection:

1. preserve subscription intent and local limits;
2. redial through the configured connection seam;
3. send `CONNECT`, re-establish active subscriptions, and restore
   auto-unsubscribe state;
4. expose the transition through `Event.t`;
5. apply the explicit no-replay policy to buffered publishes.

The bridge fails pending requests and flush/drain barriers instead of silently
replaying them. Unsubscribe and auto-unsubscribe commands received during the
handshake are deferred until `Reconnected`; `close` wins over recovery. A
redial or handshake failure is retried when the configured policy permits it;
exhaustion terminates the event stream with a structured error without
emitting a second facade disconnect event. Any opt-in retryable request policy
remains a future extension.

Buffered publish replay is inherently at-least-once at the transport boundary:
a publish may have reached the server just before a disconnect and then be
sent again. Therefore the default should not silently replay arbitrary Core
publishes. If a reconnect buffer is enabled, the API must document the
duplicate-delivery risk and provide size/overflow/error visibility. JetStream
publish should use message ids when the caller needs deduplication.

`close` stops immediately and releases resources. `drain` rejects new
publishes/subscriptions, lets existing subscription queues and pending output
finish, flushes, then closes. A connection drain must also drain or cancel
request waiters deterministically. Subscription drain has the narrower meaning
of unsubscribe while delivering already-received messages. In the Eio facade,
it waits for the server's unsubscribe barrier, enqueues a terminal marker, and
leaves already queued messages available to `next`/`iter`; it does not wait for
a consumer fiber to observe those items. A timed-out drain retains its terminal
result for subsequent calls rather than reporting a later false success.

### Authentication and transports

The core now exposes a reusable `Nats.Auth.t` capability for the data required
to construct CONNECT and sign a server nonce. Its common constructors are:

```text
Auth.none
Auth.token
Auth.user_pass
Auth.nkey : nkey:string -> sign:signer -> t
Auth.jwt  : jwt:string -> nkey:string -> sign:signer -> t
```

`Auth.connect` derives a fresh low-level `Client.Connect.t` from each server
`INFO`; this makes reconnects use the new nonce rather than reusing a stale
signature. Signer failures, missing nonces, and an authentication-required
anonymous connection are structured local errors. The Eio adapter stores the
capability in its configuration and invokes it after the settled `INFO`, while
`Client.t` stores neither the capability nor a signer. Private key
parsing/storage belongs in an optional authentication module, and the signer
contract expects the caller to provide any required signature encoding.

The Eio adapter now owns the TCP-to-TLS transition: for a server-required
upgrade it reads the initial plaintext `INFO`, pauses and joins its reader,
wraps the flow with the configured TLS client, and sends `CONNECT` using that
already-parsed `INFO` with `tls_required=true`. For an explicit TLS-first
endpoint it performs TLS before reading the first encrypted `INFO`. The pure
core sees only the final protocol intent; it does not depend on TLS. The
adapter rejects bytes left over from the plaintext phase, bounds the TLS
handshake with the configured handshake deadline, and reports TLS failures
through structured adapter errors. The
caller supplies the TLS peer configuration and must install the TLS RNG; host
name/SNI policy is therefore part of that configuration. Multi-endpoint TCP
dialing policy, server discovery, and explicit endpoint TLS are implemented in
the Eio endpoint planner; peer identity/SNI selection remains caller-owned.
The opt-in real-server harness now exercises token and username/password authentication,
server-required TLS, reconnect recovery, pending-request disconnect failure,
bounded slow-consumer handling, parent-switch cleanup, and lame-duck INFO/event
handling alongside single-server Core NATS headers, queue groups, request/reply,
and no-responders. NKey/JWT server
acceptance remains a later Core milestone.

### JetStream, KV, Object Store, and Services

These modules should be layered over `Connection.request` and
`Connection.subscribe`:

- `Nats_eio.Jetstream` provides a connection capability, typed API request and
  response models, stream/consumer management (including typed update and
  inventory operations), publish acknowledgements, consumer handles, one-shot
  fetch, a persistent `Consumer.Pull` session, and a switch-owned
  `Consumer.Push` session, and a client-managed `Consumer.Ordered` session.
  Delivered `Msg.t` values carry the stream/consumer metadata needed for
  explicit `ack`, `nak`, `term`, and `in_progress` operations. Push sessions
  consume idle-heartbeat status frames, answer flow-control requests (including
  stalled-heartbeat replies), and fail with structured missing-heartbeat
  errors. Ordered sessions validate consumer sequence continuity and resume from
  the next stream sequence after recovery. Push sessions restore replayable
  delivery subscriptions after reconnect, recheck durable consumers, and
  recreate ephemeral consumers when the server reports consumer-not-found.
  Other advanced consumer behavior remains a planned extension.
- `Nats_eio.Object_store` provides the local streaming lifecycle over the same
  connection: validated bucket capabilities, direct metadata reads,
  incremental Bytesrw transfers, digest/size/chunk verification, metadata
  updates and links, snapshot/live watches, listing, deletion, replacement
  cleanup, sealing, typed bucket policy, read-modify-write updates, and
  structured operation deadlines. The opt-in live-server runner covers
  chunked content, metadata, links, listing, deletion and tombstones, watches,
  sealing, and cleanup.
- `Nats_eio.Key_value` provides revisioned values, compare-and-set mutations,
  finite scans, history, and cancellable watches over the same connection. The
  opt-in live-server runner covers bucket status, direct reads, CAS failures,
  history, tombstones, filtered keys, and a live watch.
- `Nats_eio.Service` provides typed service identity, endpoint/group values,
  queue-backed workers, request and service-error replies, `$SRV.PING`,
  `$SRV.INFO`, and `$SRV.STATS` monitoring, replayable subscriptions, service
  statistics, service-local draining, and resource-free fan-out discovery with
  typed response decoding. The opt-in live-server runner covers one service's
  endpoint and group registration, monitoring discovery, successful and
  service-error replies, handler failure isolation, statistics, service-local
  draining, and parent-connection usability. A companion live slice verifies
  two instances sharing a queue group, while the Core reconnect runner also
  verifies Service endpoint and monitoring recovery. The dedicated Service
  interop runner cross-checks the wire behavior with the official Go
  `nats.go` `micro` SDK, including bidirectional requests, service-error
  headers, named INFO/STATS discovery, queue/metadata declarations, exact
  counters, and an ordered completion barrier under anonymous, token,
  username/password, and server-required TLS modes; broader version,
  credential, cluster, and feature-family matrices remain stability work.

JetStream consumers deserve particular care. Pull consumption is the default
for new code because it makes demand and backpressure explicit; push consumers
remain fully supported. A consumer handle should expose both one-shot fetch
and a long-lived iterator/loop, with cancellation that releases server-side
resources. Acknowledgement methods must be tied to the delivered message's
metadata, not reconstructed from application payloads.

Management operations should use typed request/response codecs and preserve
unknown server fields where practical. They should not require callers to
construct `$JS.API.*` subjects or parse JSON error strings. The server-version
minimum for each feature should be checked and returned as a structured error,
not hidden behind a generic request failure. A full-replacement server update
must use a read-modify-write boundary when the public OCaml config is only a
projection of the wire config; modeled fields are explicit replacements and
unknown fields must not be silently reset.

The implemented JetStream slice follows this boundary in `nats-eio`: a
resource-free `Jetstream` capability uses `Jsont`/`bytesrw` at the Eio boundary,
decodes management success/error envelopes, and exposes typed stream
configuration (including per-subject limits and direct/rollup flags), stream
create/bind/update/list/info/delete and direct message reads through
`Stream.Message`, `get`, and `get_last`, consumer
create/bind/info/list/delete/update with typed configuration combinators,
durable publish acknowledgements, message acknowledgement verbs, one-shot
fetch, and persistent pull sessions. Stream and consumer updates preserve
unknown server configuration through an INFO/read-modify-write cycle; list
operations consume server pagination and fail explicitly on an incomplete
page. The management prefix is configurable for JetStream domains,
while application subjects remain ordinary Core NATS subjects. KV, Object
Store, and Services now compose over the same connection; their local
discovery, lifecycle, and data-path behavior is implemented, while their wider
real-server and cross-SDK matrices remain later stability work beyond the
baseline Service interop slice described above.

## 6. Testing and interoperability

The protocol boundary should be tested as a black-box state machine, not only
through individual helper functions:

- feed valid and invalid raw frames through the parser, including fragmented
  input, interleaved `INFO`, headers, empty payloads, and large lengths;
- assert exact wire output for CONNECT, PUB/HPUB, SUB/UNSUB, PING/PONG, and
  reconnect replay;
- exercise request multiplexing, no responders, auto-unsubscribe, slow
  consumers, drain, and server errors;
- exercise TLS-required negotiation failure, forced-TLS validation, buffered
  pre-TLS input rejection, and handshake timeout in the Eio adapter;
- fuzz framing and control-line decoding for crash safety and bounded memory;
- run in-memory client/server transition tests to verify the state machine
  without a network;
- run black-box integration tests against a real `nats-server` for reconnect,
  three-node cluster discovery/failover, TLS/authentication, queue groups, and
  lifecycle/error behavior, lame-duck INFO updates, and JetStream. The
  current opt-in Docker harness enables its JetStream slice with
  `NATS_TEST_JETSTREAM=1` and covers stream management, stream update/list,
  consumer inventory, unknown-config preservation, publish acknowledgements,
  duplicate message ids, one-shot and persistent pull delivery,
  idle-heartbeat behavior, timeout/expiry behavior, max-bytes errors, and
  cleanup. The same opt-in runner also covers Key-Value bucket status, direct
  reads, CAS failures, history, tombstones, filtered keys, and a live watch.
  The same runner covers Object Store chunked content, metadata, links,
  listing, deletion and tombstones, watches, sealing, and cleanup. The same
  runner covers single-service monitoring, endpoint/group requests, service
  errors, failure isolation, statistics, and drain behavior, plus two-instance
  queue-group routing, and Service endpoint/monitoring recovery through the
  live reconnect runner. The dedicated Service interop runner also checks the
  OCaml/official-Go wire contract for endpoint requests, service errors,
  discovery, metadata, queue groups, statistics, and coordinated draining;
  broader cross-SDK matrices remain later work.
- cross-check observable behavior with NATS by Example and at least one
  official client for each feature family.

The Eio adapter tests should verify ownership and cancellation: closing the
parent switch closes the socket, waiting subscriptions terminate, pending
requests receive a structured result, and a failed reconnect cannot leak a
fiber. Tests should make server confirmation explicit—local writes must not be
mistaken for `flush` success.

## 7. Implementation sequence

1. **Protocol vocabulary and invariants.** Define subjects, filters, queues,
   headers, messages, errors, packet framing, and the client state machine.
2. **Core NATS over Eio.** Implement TCP/TLS connection ownership, publish,
   subscribe, queue groups, request/reply, headers, no responders, flush,
   reconnect, discovery, drain, close, and bounded delivery queues.
3. **Interop hardening.** Add parser fuzzing, raw-frame fixtures, reconnect and
   drain tests, and a small compatibility matrix against the maintained Tier 1
   clients and supported server versions.
4. **JetStream foundation.** Add typed JSON API codecs, stream/consumer
   management, publish acknowledgements, pull/push consumers, metadata, and
   acknowledgements.
5. **Durable services.** Add KV, Object Store, and Services on the JetStream/
   Core primitives, with streaming and cancellation tests.
6. **Ergonomic and operational polish.** Add codec helpers, structured event
   observation, metrics hooks, WebSocket transport if justified, and optional
   NKey/JWT helpers.

The order intentionally gets Core NATS and the reconnect/drain invariants
right before adding the JSON-heavy APIs. Every higher-level feature should
consume the same connection and message abstractions rather than introduce a
second client runtime.

## 8. Non-goals and open decisions

### Non-goals

- NATS Streaming/STAN compatibility.
- A callback-only public API.
- A protocol core that depends on Eio, Lwt, Unix, TLS, DNS, or a socket.
- Silent publish replay across reconnect.
- Unbounded buffering as the default slow-consumer policy.
- An untyped “send an arbitrary JetStream JSON request” API as the primary UX.

### Decisions to validate in prototypes

- Whether the subscription queue should default to a fixed size or require an
  explicit limit in every connection configuration.
- The exact Eio ownership shape: one protocol-owner fiber with command/reply
  channels versus a small set of coordinated fibers.
- Whether `Mtime.Span.t` is the public timeout type or whether the Eio facade
  should provide a thin duration alias.
- How much of NKey/JWT signing belongs in this repository versus a separate
  credential package.
- Whether raw JetStream models should preserve unknown JSON fields in an
  extensible representation or reject unsupported server features early.
- When a WebSocket adapter and a second runtime justify a new package.

## References

The primary sources used for this proposal are:

- [NATS ecosystem and maintained-client tiers](https://docs.nats.io/concepts/ecosystem)
- [NATS wire protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol)
- [NATS client development guide](https://docs.nats.io/reference/reference-protocols/nats-protocol/nats-client-dev)
- [Reconnect behavior](https://docs.nats.io/using-nats/developer/connecting/reconnect)
- [Drain behavior](https://docs.nats.io/using-nats/developer/receiving/drain)
- [NATS concepts guide](https://docs.nats.io/learn/)
- [NATS by Example](https://natsbyexample.com/)
- [nats.go](https://github.com/nats-io/nats.go)
- [nats.js](https://github.com/nats-io/nats.js)
- [nats.py](https://github.com/nats-io/nats.py) and its [API documentation](https://nats-io.github.io/nats.py/)
- [nats.java](https://github.com/nats-io/nats.java)
- [async-nats](https://github.com/nats-io/nats.rs)
- [nats.net](https://github.com/nats-io/nats.net)
- [nats.c](https://github.com/nats-io/nats.c)
- [nats.zig](https://github.com/nats-io/nats.zig)
- [nats.swift](https://github.com/nats-io/nats.swift)
- [nats-pure.rb](https://github.com/nats-io/nats-pure.rb)
- [nats.ex](https://github.com/nats-io/nats.ex)
