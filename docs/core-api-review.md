# Core API stability review

This review covers the G2 surface: the pure `nats` protocol values and state
machine, plus the Core-facing parts of `nats-eio` (`Connection`,
`Subscription`, `Request`, and `Event_stream`). JetStream, Key-Value, Object
Store, Services, and the system-account package remain outside this gate.

The review checked the public `.mli` contracts against their implementations,
the `Packet -> Op -> Client` transition boundary, reader ownership, structured
errors, reconnect state, request and subscription ownership, cancellation, and
terminal event behavior. Independent outside reviews were used for the pure
state machine, Eio lifecycle, and PRNG ownership. The remediation was then
rechecked against the local Go SDK behavior and black-box regressions.

## Resolved findings

- PONG classification now follows wire order. Liveness, ordinary flush,
  connection-drain, and subscription-drain barriers each occupy an ordered
  slot, so a PONG for an earlier liveness probe cannot complete a later flush.
- A drain keeps its local subscription intent and delivery queue alive until the
  server's barrier PONG. Messages accepted by the server after `UNSUB` but
  before that PONG are delivered before the terminal marker.
- Negotiated `max_payload` applies to the complete HPUB body, including the
  encoded NATS header block. The structured client error reports that complete
  size.
- Request cancellation records the private reply SID before leaving the
  cancellation-protected setup region, so a cancellation/setup race can issue
  its cleanup `UNSUB` immediately. Request retry validation reports its own
  structured error variant.
- A reusable Eio configuration no longer gives every connection an identical
  reconnect-jitter stream. Caller-provided PRNG state is copied at
  configuration construction; connection creation splits independent child
  states under a short mutex-protected critical section.
- Public documentation now builds without unresolved `Jsont` or `Bytesrw`
  cross-reference warnings when the external package documentation is not
  installed.

## Deliberate boundary

The pure client's `Draining` phase has no `next_timeout` deadline. The pure
state machine does not own a clock policy for drain completion; the Eio facade
owns `Config.drain_timeout` and applies it to connection and subscription
drains. Keeping that split avoids silently adding adapter policy to the pure
protocol API and is covered by the existing drain contract test.

The Eio connection reports initial server authorization errors through its
event stream after writing `CONNECT`; `Connection.connect` therefore means that
the transport and initial protocol exchange are active, not that the server has
already accepted the credentials. During reconnect, Core events from the failed
transport are suppressed and the replacement `INFO`/`Connected` sequence is
emitted as the reconnect control sequence.

New Core publishes follow the Go client's bounded reconnect-buffer contract:
their encoded bytes are accepted while the replacement handshake is in flight
and flushed after `CONNECT` and subscription replay. A subscription created in
that interval records local intent immediately, while unsubscribe and
auto-unsubscribe update that intent without separate wire commands. Existing
request waiters are not retried as new publishes; they remain pending until a
reply, timeout, cancellation, or final connection close. This keeps transport
recovery stateful without making the client infer whether an interrupted
mutation reached the server.

## Evidence

The local G2 evidence gate passes inside the pinned Nix/OCaml 5.5 environment:

- `dune build @check`
- `dune runtest`
- `dune build @doc`
- pure Core client tests, including the overlap regressions;
- Eio mock lifecycle tests, including shared-config jitter separation; and
- the existing Core fuzz suites.

The review found no unresolved high-severity API, ownership, or lifecycle
issue. Later gates still own broader server-version, cluster-failure, and
higher-level feature acceptance work.
