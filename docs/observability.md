# Observability

The Eio facade exposes connection observations without making the protocol
core depend on a logging, metrics, or tracing runtime. The current boundary has
two parts: a race-safe statistics snapshot and the existing lifecycle event
stream.

## Connection statistics

`Nats_eio.Connection.stats` returns an immutable snapshot that can be read from
application code or a monitoring fiber:

```ocaml
let snapshot = Nats_eio.Connection.stats connection in
let published = Nats_eio.Connection.Stats.out_messages snapshot in
let published_bytes = Nats_eio.Connection.Stats.out_bytes snapshot in
Format.eprintf "published=%Ld bytes=%Ld@." published published_bytes
```

The counters start at connection creation, remain cumulative for the lifetime
of the connection, and are non-negative `int64` values. They saturate at
`Int64.max_int`. The message counters cover messages accepted by publish and
request operations, and messages delivered to a request or subscription. The
byte counters cover the message payload plus the encoded NATS header block,
excluding the protocol command line and CRLF framing. Messages accepted into
the bounded reconnect buffer are counted when accepted. `reconnects` counts a
replacement connection after its CONNECT and subscription replay have been
written; it does not count failed attempts or the initial connection, and it
advances before reconnect-buffer flushing and lifecycle-event delivery.

The returned snapshot is coherent: all fields come from one atomic immutable
value. Taking a snapshot does not wait for the connection owner and does not
change protocol state.

## Lifecycle observations

`Nats_eio.Connection.events` returns the connection's lifecycle and Core event
stream. Read it from a dedicated fiber when an application needs connection
state transitions:

```ocaml
let rec observe events =
  match Nats_eio.Event_stream.next events with
  | Ok (Nats_eio.Event.Reconnected as event) ->
      Format.eprintf "event=%a@." Nats_eio.Event.pp event;
      observe events
  | Ok _event -> observe events
  | Error _ -> ()
```

The stream is single-consumer and bounded by the connection configuration;
slow consumers retain the same explicit `Slow_consumer` behavior as the rest
of the Eio API. It is an event observation surface, not a callback dispatcher,
and application deliveries continue to use `Subscription.t`.

## Optional integrations

The statistics and event surfaces intentionally carry no OpenTelemetry,
`tracing`, or Pyro dependency. An optional bridge can translate these stable
observations into application metrics or lifecycle spans without changing the
NATS protocol state machine. Message-level tracing requires an explicit
instrumented publish/request/subscription wrapper so propagation and subject
redaction remain application policy; it is not inferred from lifecycle events.
