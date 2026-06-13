# Core API stability review

This review covers the G2 surface: the pure `nats` protocol values and state
machine, plus the Core-facing parts of `nats-eio` (`Connection`,
`Subscription`, `Request`, and `Event_stream`). JetStream, Key-Value, Object
Store, Services, and the system-account package remain outside this gate.

The review checked the public `.mli` contracts against their implementations,
the `Packet -> Op -> Client` transition boundary, reader ownership, structured
errors, reconnect state, request and subscription ownership, cancellation, and
terminal event behavior. An independent outside review was used for the pure
state machine, Eio lifecycle, PRNG ownership, and the final overlap fix.

## Resolved findings

- A `PONG` completing a flush also proves connection liveness. The client now
  resets liveness close debt for every PONG while retaining a separate count of
  outstanding liveness replies, so additional legitimate PONGs are not
  reported as unsolicited protocol notices.
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
