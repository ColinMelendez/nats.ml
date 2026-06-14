# JetStream API stability review

This review covers the public JetStream surface in `nats-eio`: stream and
consumer configuration, management handles, delivery sessions, publishing,
structured errors, and the ownership and cancellation rules around them. The
Key-Value, Object Store, Services, and privileged system-account APIs compose
with this surface but have their own acceptance and documentation gates.

The review closes the API-shape portion of G4. It does not claim that the
remaining live-server and version-matrix work is complete.

## Method

The public signatures were read together with their implementations, callers,
mock transport tests, and the pinned NATS server behavior. The comparison
points were the official Go SDK and NATS server releases used by the repository
(`2.10.22`, `2.12.15`, and `2.14.5`). Two independent outside reviews were
also requested: OpenCode and a `gpt-5.6-sol` review at high reasoning effort.
Their findings were checked against the implementation and the pinned server
source rather than accepted as authority.

The important design questions were:

- Which values are ordinary handles and which values own subscriptions,
  workers, or server resources?
- What does cancellation do to a blocked operation and to its public handle?
- Which configuration errors can be reported locally, and which belong to the
  server because defaults and feature availability are server-owned?
- Can a read-modify-write operation preserve fields the OCaml model does not
  know about?
- Does a status frame have a stable wire identity, or must its code and
  description be interpreted together?

## Public ownership model

| Surface | Ownership | Contract |
| --- | --- | --- |
| `Jetstream.t`, `Stream.t`, `Consumer.t` | Borrowed connection handles | Direct operations use the connection; server resources are created, updated, and deleted explicitly. |
| `Consumer.Msg.t` | No resource ownership | Carries the delivery identity needed for acknowledgement operations. Messages are not acknowledged implicitly. |
| `Consumer.Pull.t` | Caller switch owns a subscription and request state | `next` is single-owner; `close` is idempotent; switch release closes the session. |
| `Consumer.Consume.t` | Caller switch owns a daemon worker and its `Pull` session | `stop` discards buffered messages, `drain` preserves them, and switch release makes subsequent reads return `Pull_closed`. |
| `Consumer.Push.t` | Caller switch owns the delivery subscription; `create` also owns its ephemeral consumer | `Push.v` owns only the subscription initially, but a recovery-created replacement for a missing ephemeral consumer retains its public name, becomes owned by the handle, and is cleaned up by retryable `close` calls. Switch-release cleanup is best effort; explicit `close` reports cleanup failure. |
| `Consumer.Ordered.t` | Caller switch owns generations, subscriptions, and ephemeral consumers | Gap, heartbeat, deletion, and reconnect recovery are internal to the ordered session. |
| `Publisher.t` | Caller switch owns pending futures and the acknowledgement machinery | Futures expose structured completion results; cancellation and bounded pending state are explicit. |

This keeps the narrow waist at the connection and JetStream request layer.
Higher-level sessions compose subscriptions and consumers rather than exposing
a second resource manager with a competing lifecycle.

## Representative callers

### Read-modify-write management

```ocaml
let stream = expect (Jetstream.Stream.lookup jetstream ~name:"ORDERS") in
let consumer = expect (Jetstream.Consumer.lookup stream ~name:"worker") in
let current = expect (Jetstream.Consumer.info consumer) in
let replacement =
  expect
    (Jetstream.Consumer.Config.with_description
       (Jetstream.Consumer.Info.config current) (Some "processed orders"))
in
expect (Jetstream.Consumer.update consumer replacement)
```

`Consumer.update` reads the current server configuration, replaces the fields
modeled by `Config.t`, preserves unmodeled fields and pause state, and leaves
last-writer-wins concurrency explicit. Immutable fields remain server errors.

### Pull delivery

```ocaml
Eio.Switch.run (fun sw ->
  let pull = expect (Jetstream.Consumer.Pull.v ~sw consumer ~batch:32) in
  match Jetstream.Consumer.Pull.next pull with
  | Ok message -> expect (Jetstream.Consumer.Msg.ack message)
  | Error Jetstream.Error.Pull_closed -> ()
  | Error error -> report error)
```

The session owns the request subscription and releases it on close or switch
release. A timeout does not silently close or recreate the session.

### Continuous consumption

```ocaml
Eio.Switch.run (fun sw ->
  let consume = expect (Jetstream.Consumer.Consume.v ~sw consumer) in
  Jetstream.Consumer.Consume.iter consume ~f:handle_message)
```

The worker is tied to `sw`. Releasing `sw` transitions the public handle to a
closed state, wakes a blocked reader, and prevents a stale `Open` handle from
blocking forever.

### Push and ordered sessions

```ocaml
Eio.Switch.run (fun sw ->
  let push = expect (Jetstream.Consumer.Push.create ~sw stream config) in
  Fun.protect
    (fun () -> Jetstream.Consumer.Push.iter push ~f:handle_message)
    ~finally:(fun () -> ignore (Jetstream.Consumer.Push.close push)))
```

`Push.create` sends the server's create-only action and owns its ephemeral
consumer. An explicit public name is a caller-unique precondition: the server
rejects a conflicting existing configuration, but may treat an identical
configuration as an idempotent create. `Push.v` attaches to an existing
consumer and owns only its subscription until recovery creates a replacement;
named replacements retain their public name and all replacements become owned
for cleanup. Switch-release cleanup is best effort, while explicit `close` can
be retried and inspected.
`Ordered.v` adds a stronger recovery contract over pull delivery;
its generation changes are not exposed as a second public consumer lifecycle.

Key-Value ordered watches, Object Store transfers, and Services endpoints use
the same switch-owned direct-style convention. They do not require callers to
parse JetStream subjects or status messages.

## Alternatives considered

| Question | Considered | Decision and reason |
| --- | --- | --- |
| Resource shape | A manager object that owns every stream and consumer vs small handles | Small handles. Ownership is local to the operation that actually owns a subscription, worker, or ephemeral server resource. |
| Delivery style | Callback-only consumers vs direct sessions | Direct sessions plus `iter`. Direct style makes cancellation and backpressure visible while retaining a convenient loop. |
| Consumer sessions | One universal session with mode flags vs separate Pull, Consume, Push, and Ordered modules | Separate modules. Each mode has different wire control, recovery, and ownership invariants. |
| Update semantics | A large patch type vs full replacement from an `Info.config` snapshot | Full replacement of modeled fields. It avoids silent partial updates and gives callers a clear read-modify-write path; the server remains authoritative for immutable fields. |
| Unknown JSON | Raw JSON everywhere vs typed fields with preserved unknown members | Typed known fields plus retained unknown object members. This permits round trips without making normal callers parse JSON. |
| Feature availability | Proactive version gates vs sending the request and returning the server error | The current boundary is server-authoritative. JetStream errors retain API metadata, and unsupported features are not silently ignored. A capability/version snapshot remains a release follow-up rather than an implicit compatibility promise. |

## Resolved findings

- Continuous `Consume` sessions now run their worker as a daemon child, install
  a cancellable hook on the caller's switch, and wake blocked readers during
  release. The hook stops the worker before the enclosing switch waits for
  children, so the public `closed` and `next` contracts agree after switch
  release.
- Stream `deny_delete`, `deny_purge`, and `sealed` are monotonic in update
  requests. The client cannot accidentally ask the server to clear a safety
  flag that the server treats as irreversible.
- `Consumer.Config.v` rejects locally knowable server-invalid combinations:
  `Last_per_subject` without a filter, invalid backoff spans or lengths,
  flow-control acknowledgement without a push subject, incompatible ack
  timing/retry fields, pull flow-control, push `max_waiting`, missing
  flow-control heartbeats, sub-minimum common heartbeat/expiry values, and
  positive `max_ack_pending` for `No_ack`. Rules whose normalization differs
  across the pinned server versions remain server-owned rather than being
  guessed locally.
- Owned Push cleanup is retryable. A failed delete leaves the owned consumer
  attached to the handle, while a later `close` retries it; already-missing
  consumers are treated as successfully cleaned up. If `Push.v` has to
  recreate a missing ephemeral consumer, the replacement retains the configured
  public name when present, is owned, and is cleaned up by that handle too.
- Consumer priority groups and policy are sent as replacement fields during
  update. `Config.with_priority` provides the atomic public transition needed
  to add, replace, or clear the coupled fields; the previous implementation
  rejected all identity changes even though the pinned server accepts them.

## Deliberate boundaries and follow-ups

The following are not hidden API defects, but they remain explicit release
work:

- There is no proactive server capability snapshot or minimum-version feature
  table in `Connection`. The package currently relies on structured
  server-side API errors. Before a broad compatibility promise is made, either
  add a capability projection from the server `INFO` exchange or document the
  supported feature/version matrix as part of release policy.
- Unknown object members survive decode and re-encode, but unknown enum values
  are rejected. This is a deliberate fail-closed choice for policy fields; a
  future compatibility design can add an explicit `Unknown` constructor if
  forward enum tolerance becomes necessary.
- Consumer status classification combines numeric status codes with normalized
  descriptions because the NATS status frames do not provide a dedicated
  subtype for every JetStream control condition. The pinned server strings and
  case variants are covered by mock fixtures; a table-driven status matrix is
  still useful before supporting new server families.
- Management updates are read-modify-write and last-writer-wins. There is no
  compare-and-set update token in the public API.
- Stream safety flags are monotonic relative to the preceding `INFO` snapshot.
  A concurrent external update can still make the snapshot stale; the server
  may reject that update and the structured API error is returned.
- `By_start_time` retains the server's RFC3339 string rather than introducing a
  second public time representation. `Mtime.Span.t` remains the timeout and
  duration type, while `Ptime.t` is used for UTC pause deadlines.

## Review result

The high-severity ownership, lifecycle, irreversible-flag, local-validation,
and priority-update findings are resolved and covered by focused tests. The
JetStream public shape is suitable for the next documentation and acceptance
phase. G4 is not a claim that every server/version or cluster-failure matrix
has passed; the capability/version policy and the broader live acceptance
work remain visible in `plan.md`.

## Evidence

- `nix develop .#integration -c dune runtest test/eio --build-dir
  _build-g4-review` — local Eio suite, including the JetStream lifecycle,
  stream-safety, validation, priority-update, and cleanup regressions plus the
  Key-Value and Object Store consumers of the validated configuration path.
- The pinned local JetStream and cross-SDK runners cover the server releases
  listed above; their remaining cluster and authentication scope is recorded
  in `plan.md`.
- Official behavior references: [nats-server consumer actions and validation](https://github.com/nats-io/nats-server/blob/v2.14.5/server/consumer.go#L140-L152),
  [nats-server existing-consumer action handling](https://github.com/nats-io/nats-server/blob/v2.14.5/server/consumer.go#L1017-L1047),
  [nats-server stream update validation](https://github.com/nats-io/nats-server/blob/v2.14.5/server/stream.go),
  and [nats.go JetStream consumers](https://github.com/nats-io/nats.go/tree/v1.52.0/jetstream).
