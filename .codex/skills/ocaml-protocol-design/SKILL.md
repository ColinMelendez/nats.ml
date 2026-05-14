---
name: ocaml-protocol-design
description: "Module shape for OCaml network/wire protocol libraries (SSH, TLS, HTTP/2, QUIC, BGP, IMAP, AMQP, MQTT, NATS, WebSocket, OAuth, CFDP, COSE). Use when creating or restructuring a stateful protocol library, deciding where the state machine and IO adapters live, shaping the incoming/outgoing verbs, splitting protocol logic from Eio/Lwt/Unix, or reviewing a protocol's module shape. Sister skill to ocaml-encodings (pure codecs)."
license: ISC
metadata:
  copyright: "Copyright (c) 2026 Thomas Gazagnaire <thomas@gazagnaire.org>"
---

# OCaml Protocol Library Design

Applies to anything where "what message comes next?" depends on a history of
bytes already exchanged: SSH, TLS, HTTP/2, QUIC, BGP, IMAP, AMQP, MQTT, NATS,
WebSocket, OAuth flows, COSE, CFDP, file-transfer and key-exchange protocols.

The reference model is [ocaml-tls](https://github.com/mirleft/ocaml-tls) (a
purely functional `Engine` core with separate `_lwt` / `_mirage` / `_eio`
adapters) crossed with the I/O-free pattern: protocol logic processes bytes
synchronously and never touches a socket.

## No I/O is the load-bearing constraint

The core is a pure transformation:

```txt
bytes-in + state -> bytes-out + new-state + events
```

- **Zero dependencies on `eio` / `lwt` / `unix` / `mirage-net`.** Only `bytes` /
  `string` / `fmt` / a wire-codec library. If the core package's
  `(depends ...)` mentions any of those, the core has bled.
- **Time is an input.** Handlers needing a clock take `~now:Mtime.t`
  (monotonic) or `~now:Ptime.t` (calendar), passed by the caller.
- **Randomness is an input.** Either the caller threads a PRNG state, or the
  core takes `~rng:(int -> string)`. `nox-crypto-rng` is acceptable inside the
  core (seeded once at startup, API is purely `int -> string`);
  `Random.State.make_self_init ()` is not.
- **Concurrency lives outside.** The core never yields, schedules, or spawns.
  Waiting for "the next handshake message" is a *state*, and the caller decides
  how to obtain bytes.

The payoff: one state machine runs unchanged under Eio, Lwt, a Mirage
unikernel, the fuzz harness, and a deterministic `dune test`.

## The four layers

This is [`ocaml-encodings`](../ocaml-encodings/SKILL.md)' `Value` / `Codec`
layering, with the AST renamed and two protocol-only layers added.

```txt
Foo.Message   Message AST + IO. Foo.message = Foo.Message.t. The protocol's
              analogue of Foo.Value: constructors, pp, equal, of_string /
              of_reader / to_string / to_writer.
Foo.Codec     Typed bidirectional codec ('a Foo.codec). Maps Message values
              to/from bytes. Depends on Message, built on AST-free wire
              combinators. The combinator layer is NOT Foo.Codec.
Foo.Packet    Framing: record/packet framing, length prefixes, MAC and cipher
              state. Byte stream <-> sequence of payloads. (TLS record, SSH
              binary packet, WebSocket frame.)
Foo.State     The state machine: incoming / outgoing / events over an opaque
              state. The package's defining concern. For asymmetric protocols,
              a role pair of top-level modules instead of a single State.
Foo           Top-level re-exports + IO verbs. Adapters (foo.eio in lib/eio/)
              wire the state machine into an I/O model.
```

The dependency edge runs `Codec -> Message`, never the reverse; merlint
**E945** flags a `message.ml` referencing its sibling `Codec`. `Message` may
freely use ocaml-wire's `Wire.Codec`; that is the external combinator library,
not the sibling module, and E945 does not flag it.

There is no `Foo.Wire` module: `Wire` is ocaml-wire's namespace, so a protocol
using it would collide. A protocol's own lowest combinator layer is `Core` or
`Parser`. The real references name these `Tls.Core` / `Tls.Reader` /
`Tls.Writer` / `Tls.Packet`, and `Ssh.Message` / `Ssh.Codec` / `Ssh.Packet` /
`Ssh.Client` / `Ssh.Server`.

Documentation leads with the state-machine API; a user who only wants to encode
one message reaches into `Foo.Message`.

## The state-machine module: a closed role vocabulary

The state machine lives in a module named from a closed, documented
vocabulary, never an ad-hoc name. This is what makes a protocol's core
findable by a reviewer and a linter without guessing.

- **Symmetric** (peers interchangeable): a single `State` module. TLS,
  WebSocket, a gossip peer.
- **Asymmetric** (who initiates matters): a **role pair** of two top-level
  modules, each with its own `t` and verbs. The recognized pairs are
  `Client`/`Server`, `Sender`/`Receiver`, `Initiator`/`Responder`, and
  `Requester`/`Responder`. Pick the honest one: a file-delivery protocol has a
  `Sender` and a `Receiver`, not a client and a server.

To add a pair, extend this list *and* the merlint role set; do not invent a
one-off name in one package. Merlint **E946** flags a protocol package exposing
no state-machine module.

A protocol whose machines genuinely do not fit: a CFDP keeping Class-1 and
Class-2 senders and receivers as four deliberate machines: splits them one
module per machine and **declares** the names:

```toml
# ocaml-cfdp/merlint.toml
allowed_states = ["sender1", "sender2", "receiver1", "receiver2"]
```

`allowed_states` is a positive declaration, not an exclusion: the listed modules
get checked by E947/E948/E949 exactly like a `State` module, and E946 accepts
the package. Reach for it only when the machines are genuinely distinct; two
roles of one protocol are still `Client`/`Server`.

Four invariants on those modules are machine-checked:

| Rule | Check |
|------|-------|
| **E949** | One state machine per module. Roles are top-level modules, never nested `module Sender` / `module Receiver` in one file. |
| **E947** | Immutable state: the state `type t` and its component types carry no `mutable` field and no `bytes` / `ref` / `array`. |
| **E948** | Canonical verbs only (see Naming). |
| **E954** | The module exposes `incoming` and a constructor (`v` or a role constructor). |

## Public surface

`foo.mli` restates the curated public API. Never `include module type of State`
or re-export the wire-codec vocabulary wholesale; it makes implementation
helpers look endorsed.

```ocaml
(* foo.ml re-exports implementation modules. *)
module Message = Message
module Codec   = Codec
module Packet  = Packet
module State   = State   (* or a role pair for asymmetric protocols *)
module Event   = Event
```

```ocaml
(* foo.mli restates the public subset, in order. *)
type t = State.t
(** Opaque protocol state. *)

(** {1 Construction} *)
val client : Config.client -> t * string
val server : Config.server -> t

(** {1 I/O verbs} *)
type ret = (t * Event.t list * string list, Error.t) result
val incoming : t -> Bytesrw.Bytes.Reader.t -> ret
val outgoing : t -> ?id:int32 -> string -> (t * string list, Error.t) result
val close : t -> t * string option

(** {1 Inspection} *)
val handshake_in_progress : t -> bool
val pp : t Fmt.t

(** {1 Errors, events, submodules} *)
module Error : sig ... end
module Event : sig ... end
module Message : sig ... end
```

If the odoc tree shows `State.Handshake_state`, `Packet.Internal`, or
`Codec.cursor` on the first page, the surface is leaking: use `private_modules`
or a curated `mli`.

## Layer 1: `Message` and `Codec`

The codec follows [`ocaml-encodings`](../ocaml-encodings/SKILL.md) exactly:
GADT-backed combinators, `Loc.Meta.t` where locations matter, exceptions
internally with `result` at the boundary. Three protocol-specific deltas:

- Use the `wire` library for binary framing, or `nox-toml` / `nox-cbor` /
  `nox-json` for structured payloads, whichever the spec specifies. Don't
  invent a parallel format.
- **The codec must not know about the state machine.** A `Handshake` message
  received in error has the same byte layout as one received in handshake
  state; only the state machine cares whether it was expected. Keep the codec
  phase-blind: shaping it around handshake phases (separate `Message.handshake`
  and `Message.application_data` types) makes it bigger than necessary for no
  gain.
- For framed protocols, the **frame** (header + opaque payload) lives in
  `Foo.Packet` and the typed **message** in `Foo.Message`. The state machine
  consumes typed messages, but the framing layer stays separable so it can be
  fuzzed and dumped independently.

```ocaml
module Frame : sig
  type t = { kind : int; payload : string }

  val of_string : string -> (t * string, Error.t) result
  (** [of_string s] is [Ok (frame, rest)] where [rest] follows the first
      complete frame. [Error _] when [s] lacks enough bytes (the caller buffers
      and retries) or the frame is malformed. *)

  val to_string : t -> string
end
```

## Layer 2: the state machine

The state is **opaque**. Internally a record carrying handshake position,
negotiated parameters, sequence numbers, and timers; externally just `Foo.t`.

Its job is to answer three questions per message: is it valid in the current
state (if not, fail with a `fatal` error), what output does the transition
produce, and what user-visible events does it emit.

### State is a sum, not a mode bool

```ocaml
(* GOOD -- pattern-matching tells you what is safe to access *)
type t =
  | Pre_handshake of pre_state
  | Handshaking   of handshake_state
  | Established   of session
  | Closing       of closing_state
  | Closed        of close_reason
```

Not one record with a `mode : [`Pre | `Handshake | ... ]` field and per-field
options: a closing state must not be able to read handshake-only fields.

**`Established` gets its own named type.** Modules that only operate on
established connections (key rotation, application data, alerts) take
`session -> ...`, not `t -> ...`, so a signature saying `session ->` cannot be
called on a pre-handshake state.

### `incoming` / `outgoing` / `close`

```ocaml
module State : sig
  type ret =
    ( t
      * [ `Eof ] option
      * [ `Response of string option ]   (* bytes for the wire *)
      * [ `Data of string option ]       (* bytes for the application *)
      * Event.t list,
      Error.fatal * [ `Response of string ] )
    result

  val incoming : t -> string -> ret
  val outgoing : t -> string -> (t * string * Event.t list, Error.t) result
  val close    : t -> t * string option
end
```

This generalizes `Tls.Engine.handle_tls`'s return tuple: any I/O-free protocol
needs at minimum (new state, bytes for the wire, bytes for the app, events).
Both byte channels are `string option` because some frames produce no wire
response (a pure app-data record) and some produce no app data (a pure
handshake message).

The error side pairs `Error.t` with the bytes to send *before* tearing down
(a close-notify or alert frame). Never return a bare error that leaves the wire
in an indeterminate state.

### Events vs responses

Two parallel output channels; keep them separate.

- **`Response`** is bytes the caller writes back to the peer. The adapter's
  whole job is `Eio.Flow.copy_string response flow`.
- **`Event`** is structured information for application logic:
  `Handshake_done`, `Peer_authenticated of identity`, `Channel_data`,
  `Disconnected of reason`. The caller switches on the variant.

Don't pack events into the response, and don't make the application read events
out of the byte stream.

```ocaml
module Event : sig
  type t =
    | Handshake_done
    | Peer_authenticated of identity
    | Channel_data of int32 * string
    | Channel_eof of int32
    | Disconnected of string

  val pp : t Fmt.t
  val emit_probe : t -> unit
end
```

`Event.t` is a **closed** variant in the core; new event kinds are an API
change. Don't reach for `[>` polymorphic variants: the variance traps callers
into exhaustive matches that break silently on additions.

**Probes stay out of the pure core.** `incoming` / `timer` / `outgoing` return
`Event.t list` and never call `emit_probe`. I/O adapters and callback
boundaries call `Event.emit_probe` immediately before delivering the event to
the application, which gives consumers a runtime-events stream without turning
pure state transitions into ambient side effects. Declare probes next to the
event type with stable names (`ssh.server.event`, `tcp.connection.event`) and
flat scalar fields: event kind, stream id, byte count, status, peer identity.
Never emit nested payloads or raw application data: record lengths and stable
identifiers. Use `Probe.span` only for real extents owned by an adapter or
public operation (connection, session, request, transfer); point events beat
incorrectly nested spans synthesized from interleaved `Started`/`Done` pairs.

### Asymmetric roles

Each role is its own top-level module and each exposes `incoming` (and
`outgoing` for wire output):

```ocaml
val Client.incoming : t -> Mtime.t -> string -> ret
val Client.outgoing_data : t -> ?id:int32 -> Buf.t -> (t * Buf.t list, string) result
```

Splitting the roles lets `Client.incoming` and `Server.incoming` have different
types: only the client side carries an authenticator-request queue, only the
server side surfaces `Userauth` events.

## Determinism and time

Every time-dependent transition takes `~now` as a parameter (session-ticket
expiry, rekey thresholds, timeouts). The state may carry the last-rekey time and
compare against `now`, but it never reads `Mtime_clock.now ()`. Tests then pass
`now:0L` for the handshake and `now:threshold + 1L` to trigger a rekey, without
sleeping.

Time-driven transitions get their **own entry point**, separate from the
byte-driven `incoming`. `ocaml-tcp` is the in-repo model:

```ocaml
val incoming     : t -> now:Mtime.t -> input -> (t * out, _) result
val timer        : t -> now:Mtime.t -> (t * out, _) result  (* RTO, delayed ACK, TIME_WAIT *)
val next_timeout : t -> Mtime.t option  (* when the I/O layer must next call timer *)
```

The I/O layer asks `next_timeout` when to wake and calls `timer` at the
deadline; loss detection, retransmission and ACK-delay logic all live in that
*pure* transition, never in a real-clock callback. Don't fold time into
`incoming` as a tagged input: a distinct `timer` reads more honestly and matches
TCP's and QUIC's two independent input sources (a packet arrived; a deadline
fired).

## Mutable resources are passed in, never stored in `t`

Real protocols need mutable scratch for performance: an inbound receive ring
decrypted in place, a reused encode buffer, an HPACK/QPACK dynamic table. None
of these belong in `t`. A `mutable` field on the otherwise-immutable state is
the lie that erodes determinism, two callers holding the same `t` would see
each other's writes, and "return a new `t`" stops meaning what it says.

Transient state lives function-local, or, when it must persist across calls, in
the I/O adapter and is **borrowed** by the transition as a labeled argument:

```ocaml
(* the ring is the adapter's; the transition borrows it for the call *)
val drain : t -> rx:Rx.t -> now:Mtime.t -> (t * out, Error.t) result
val feed  : rx:Rx.t -> Slice.t -> unit
```

`~rx` reads exactly like `~now`: the caller owns the resource and lends it for
the duration of the call, so the signature *declares* the mutable dependency
instead of hiding it as state. Ownership is single-writer, the function mutates
in place, and the borrow ends on return. `t` stays a pure replayable value and
the zero-copy work is preserved.

What earns a `mutable`: a concrete performance reason (in-place decrypt, reused
scratch, a dynamic compression table). What does not: protocol state:
flow-control windows, sequence numbers, the state variant, the stream/session
table. If you cannot name the performance reason, it is an immutable field
returned anew. (`ocaml-matter` keeps its mutable session cell in `connection`,
not `Session.t`; `ocaml-ssh` passes its decrypt ring as `~rx`.)

E947 enforces this: `bytes`, a `ref` cell, or an `array` (`Bigarray` included)
in the state type is a mutable buffer hiding in the state. Keep payloads as
immutable `string`.

## The streaming boundary: `bytesrw` in, bytes out

The state machine consumes bytes through a `Bytesrw.Bytes.Reader.t`, not a raw
`string`. That single choice is what lets the core not care where bytes came
from.

```ocaml
val incoming : t -> Bytesrw.Bytes.Reader.t -> ret
```

- A test feeds `Bytes.Reader.of_string payload`; an adapter feeds a reader
  backed by `Eio.Flow.single_read`. The state machine is identical.
- Partial reads are the reader's concern. When the framing layer has consumed a
  whole packet and a fragment of the next remains, it hands the fragment back
  with `Bytes.Reader.push_back` and the caller keeps the *same* reader across
  calls. **A buffered-bytes field in the state record means the reader boundary
  has leaked.**
- Determinism holds: an in-memory reader never blocks and signals end-of-data
  deterministically; under Eio a socket-backed reader yields the fiber. The core
  does no syscalls either way; it pulls from an abstract byte source.

Output stays as returned bytes the caller writes (a `string list`, or into a
`Bytes.Writer.t` the caller supplies). The asymmetry is deliberate: input is
*pulled* from a reader, output is *returned* for the adapter to drain.

That asymmetry is also what gives back-pressure for free. The state machine
never blocks on output; the adapter decides when to drain. If the socket is
full the adapter pauses and the state machine doesn't care; if the application
produces 1 MiB, `outgoing` returns 1 MiB and the adapter chunk-writes. Don't
bake a window/credit system into the core unless the protocol itself has one
(SSH channel windows, HTTP/2 flow control), and when it does, the window
mechanics live in the state, not the adapter.

## Layer 4: top level and IO adapters

`foo.mli` exposes the opaque `Foo.t`, the primary verbs (`client`, `server`,
`incoming`, `outgoing`, `close`), the `Error` / `Event` / `Config` submodules,
and a `Message` re-export.

Adapters live in sub-libraries under `lib/` and have **zero protocol
knowledge**; they don't construct messages and don't know what a handshake is.
An adapter reads bytes into a buffer, calls `Foo.State.incoming` until the
machine consumes everything, writes the `Response` bytes back, and surfaces the
events.

```ocaml
(* foo/lib/eio/foo_eio.mli *)
val client :
  sw:Eio.Switch.t -> net:_ Eio.Net.t -> Foo.Config.client ->
  Eio.Net.Sockaddr.stream -> Foo.t * < Eio.Flow.two_way ; .. >
```

```txt
foo/
  dune-project           # declares foo, foo-eio
  lib/
    dune                 # library foo, no eio deps
    foo.ml(i)            # re-exports + curated public API
    config.ml(i)  error.ml(i)  event.ml(i)
    message.ml(i) codec.ml(i)  packet.ml(i)
    state.ml(i)          # the state machine
    eio/dune             # library foo_eio, public_name foo.eio
    eio/foo_eio.ml(i)
  test/                  # state-machine tests via State (no IO)
  fuzz/                  # crowbar against Packet and Message
```

Don't bury the eio adapter in the core library: that drags `eio` into every
consumer's transitive deps, and the whole point of I/O-free is that `foo` builds
without `eio` installed.

## Configuration

Records with named fields, built by smart constructors that validate
consistency (at least one cipher, authenticator required, ALPN protocols
non-empty). Don't let users construct an invalid config and discover it
mid-handshake.

```ocaml
module Config : sig
  type client = {
    ciphers : Cipher.t list;
    authenticator : Authenticator.t;
    alpn_protocols : string list;
    peer_name : string option;
    session_cache : Session.cache;
    timeout : Mtime.span;
  }

  val client :
    ?ciphers:Cipher.t list -> ?alpn_protocols:string list ->
    ?peer_name:string -> ?session_cache:Session.cache -> ?timeout:Mtime.span ->
    authenticator:Authenticator.t -> unit ->
    (client, [ `Msg of string ]) result
end
```

## Errors: fatal vs recoverable

```ocaml
module Error : sig
  type recoverable =
    | Authentication_failure of string
    | No_configured_cipher of Cipher.t list
    | No_matching_peer_version of Version.t list
  (** Failures the caller can retry after fixing config or peer. *)

  type fatal =
    | Protocol_violation of string
    | Bad_mac
    | Decode of string
    | Unexpected_message of string
    | Record_overflow of int
  (** Peer misbehaviour or spec violation; the connection is dead. *)

  type t = [ `Error of recoverable | `Fatal of fatal | `Alert of Message.Alert.t ]

  val pp : t Fmt.t
  val to_alert : t -> Message.Alert.t
end
```

One `Error` module. Don't expose `Message.Alert.t` separately as a public error:
the alert taxonomy is implementation detail, and the user only wants to know
"transient retry or terminal close?".

## Naming

- The state type is `t` in `state.mli`, re-exported as `Foo.t`.
- Canonical verbs: `v`, `client`, `server`, `incoming`, `outgoing`, `close`,
  plus `timer` / `next_timeout`. Merlint **E948** rejects the synonyms: bare
  `send` / `recv` / `receive` / `read` / `write` / `emit` / `step` / `make` /
  `create` / `init` / `shutdown` / `disconnect` / `handle`, and the `parse_*` /
  `process_*` / `eat_*` prefixes. Each maps to a canonical verb (`send` ->
  `outgoing`, `recv` / `parse_*` / `handle` -> `incoming`, `make` -> `v`,
  `shutdown` -> `close`).
- A *distinct concept* keeps its own descriptive name: `send_window` (a
  flow-control credit, not byte output), `feed ~rx`, `maybe_rekey`, and field
  accessors are not synonyms and are left alone.
- Verbs that emit wire bytes are named for what they do, not how the caller uses
  them: `State.outgoing`, not `State.send`.
- Event constructors are descriptive nouns, past-tense or noun-phrase:
  `Handshake_done`, `Peer_authenticated`, `Channel_eof`.

## Composition: sub-protocols

Some protocols layer (TLS over TCP, SSH userauth over SSH transport, HTTP/2
streams over the connection). Model the inner protocol as a sub-module with its
own state and verbs, and embed its state inside the outer state:

```ocaml
module Userauth : sig
  type t
  val incoming : t -> Message.t -> (t * Message.t list * Event.t list, Error.t) result
end

(* the outer state carries an embedded Userauth.t while that phase runs *)
type state = { ...; userauth : Userauth.t option; ... }
```

The outer engine delegates during the userauth phase and re-wraps the result.
Each sub-module follows the same four-layer pattern recursively.

## What not to expose

- The internal record fields of `State.t` (or `Client.t` / `Server.t`). Always
  opaque.
- The buffered-bytes state. If the caller can poke at it, the caller can break
  the invariant that `incoming` is the only entry point for inbound bytes.
- The cipher / handshake sub-machines as public types. Wrap them.
- The intermediate decode results. Internally a record decodes to a `Frame.t`,
  then a typed `Message.t`, then a `Transition.t` (message plus effects). Only
  `Message.t` is public, and even that sits behind `State.incoming`;
  `Transition.t` is the state machine's private contract with the state.
- `State.unsafe_*` helpers. Once you label something unsafe, users use it.

## Testing

1. **State-machine unit tests.** Drive `State.incoming` directly with
   hand-constructed byte sequences. No network, no `Eio_main.run`. Assert on
   `(state, response, events)`. Most of the suite lives here.
2. **End-to-end round-trips.** Pair a `client` and `server` machine and pipe
   their responses into each other in a loop. Full handshake in memory, still
   I/O-free. Catches protocol bugs without sockets.
3. **Adapter integration tests.** A real `foo_eio` server and client in one
   process. Slower: keep them in their own dune stanza so the unit suite stays
   fast.
4. **Fuzz.** Crowbar-feed random bytes into `Packet.read` and
   `Message.of_packet`. The bar is "no sample produces a stack trace", not
   round-trip: random bytes generally won't round-trip.
5. **Interop.** For protocols with a spec-mandated peer, a
   `test/interop/<vendor>/` driving in-tree code against it (OpenSSH and
   paramiko for SSH; OpenSSL `s_client` / `s_server` for TLS). See the
   `interop-testing` skill.

Tests describe the spec, not the code. Quote the RFC clause in a comment and
paste appendix vectors verbatim:

```ocaml
(* RFC 8446 5.1: TLS 1.3 records of type [Alert] are always one fragment. *)
let test_alert_record_is_one_fragment () =
  let frame = Packet.{ kind = alert_type; payload = "\002\050" } in
  let _s, _resp, events = State.incoming s (Packet.to_string frame) in
  Alcotest.(check int) "1 alert event" 1
    (List.length (List.filter (function Event.Alert _ -> true | _ -> false) events))
```

A test passing only because the vector matches an incidental implementation
detail is weak: anchor it to the spec text.

## Prior art

| Library | Core | IO adapters | State | Events |
|---------|------|-------------|-------|--------|
| `ocaml-tls` (mirleft) | `Tls.Engine` | `Tls_lwt`, `Tls_mirage`, `Tls_eio` | `state` (sum) | inline (`ret` tuple) |
| `ocaml-ssh` (this repo) | `Ssh.Client` / `Ssh.Server` | `Ssh_eio` | per-side `t` | `Ssh.Server.event` |
| `h2` (anmonteiro) | `H2.Server_connection` | `h2-eio`, `h2-lwt-unix` | lifecycle handlers | callback API |
| `h11` (Python) | `h11.Connection` | aiohttp, httpcore | `next_event` driven | `Event` sum |
| `quinn` (Rust QUIC) | `quinn-proto` | `quinn` (runtime) | rich state record | event sum |
| `s2n-quic` (AWS) | I/O-free core | bindings per-runtime | typed states | event trait |
| `bgpkit` | I/O-free BGP FSM | tokio bindings | typed FSM nodes | event sum |

- **ocaml-tls**: the direct OCaml reference; this skill's `incoming` return
  tuple is its `ret`.
- **h11 / h2**: the canonical I/O-free HTTP implementations. Their `next_event`
  API is a viable alternative to the response-tuple shape; pick whichever fits
  the protocol's flow.
- **quinn-proto**: the strongest example of an I/O-free complex protocol; worth
  reading for how it decouples congestion control from the state machine.
- **nqsb-tls paper**: Kaloper-Mersinjak, Mehnert, Madhavapeddy, Bünzli,
  [USENIX Security 2015](https://www.usenix.org/system/files/conference/usenixsecurity15/sec15-paper-kaloper-mersinjak.pdf).
  The original purely-functional-protocol-state-machine pitch for OCaml.

## Review checklist

`dune exec -- merlint <dir>` covers E945-E954. Then confirm what it cannot see:

- Core `(depends ...)` has no `eio` / `lwt` / `unix` / `mirage`; time and
  randomness are parameters.
- State is a closed sum over phases, not a record with a `mode` field;
  `Established` is its own type and post-handshake operations take it.
- `incoming` returns bytes-for-wire separately from app data and events;
  `Event.t` is a closed variant; `emit_probe` is called only by adapters.
- Time-driven transitions have `timer` + `next_timeout`, distinct from
  `incoming`.
- Mutable resources are borrowed labeled args (`~rx`), never fields of `t`;
  no buffered-bytes field (the reader owns partial reads).
- `Packet` and `Message` are separate and independently fuzzable; the codec is
  phase-blind.
- Adapters live in `foo.eio` under `lib/eio/`; the core builds without them.
- `Error.t` distinguishes `recoverable` / `fatal` / `alert`; no public
  `unsafe_*` verbs.
