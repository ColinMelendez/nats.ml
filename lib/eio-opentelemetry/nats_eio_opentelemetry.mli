(** Optional OpenTelemetry bridges for the Eio NATS client. *)

module Metrics : sig
  type t

  val start :
    sw:Eio.Switch.t ->
    clock:_ Eio.Time.Mono.t ->
    ?interval:Mtime.Span.t ->
    ?attributes:Opentelemetry.Key_value.t list ->
    meter:Opentelemetry.Meter.t ->
    Nats_eio.Connection.t ->
    (t, Nats_eio.Error.t) result
  (** [start ~sw ~clock ~meter connection] starts a switch-owned poller that
      emits cumulative connection counters as OpenTelemetry monotonic sums.

      The first snapshot is emitted immediately. Later snapshots are emitted
      every [interval], which defaults to five seconds. The poller stops when
      [sw] is released. It does not consume the connection event stream, so it
      can run alongside an application event consumer.

      Counter values are represented as OpenTelemetry floating-point values by
      the current OpenTelemetry OCaml API. The exact non-negative [int64]
      counters remain available through {!Nats_eio.Connection.stats}. [t]
      reports bridge-local submissions and exporter failures; it does not report
      collector receipt. *)

  val submitted : t -> int64
  (** [submitted bridge] is the number of metric batches submitted to the
      supplied meter. *)

  val exporter_errors : t -> int64
  (** [exporter_errors bridge] is the number of meter emissions that raised an
      exception. Such failures are isolated to the bridge and do not fail the
      connection's switch. *)
end

module Events : sig
  type t

  val start :
    sw:Eio.Switch.t ->
    ?capacity:int ->
    tracer:Opentelemetry.Tracer.t ->
    Nats_eio.Event_stream.t ->
    (t, Nats_eio.Error.t) result
  (** [start ~sw ~tracer events] transfers ownership of [events] to a
      switch-owned lifecycle exporter. The caller must not consume [events]
      after this call.

      The exporter emits one span per lifecycle or Core event. Spans contain
      only a fixed event-kind attribute; payloads, server information, error
      messages, subjects, headers, and subscription identifiers are not
      exported. The default [capacity] is 64. The event pump never waits for the
      tracer: when its bounded bridge queue is full, an event is dropped and
      [dropped] is incremented. The terminal event has one reserved queue slot
      so the bridge can finish without waiting for the exporter.

      The bridge is switch-owned and best-effort. Exporter exceptions are
      counted and do not fail the NATS connection. Message spans and
      trace-context propagation are deliberately outside this API because
      subject, header, and payload redaction are application policy. *)

  val submitted : t -> int64
  (** [submitted bridge] is the number of lifecycle spans submitted to the
      supplied tracer. *)

  val dropped : t -> int64
  (** [dropped bridge] is the number of non-terminal lifecycle events dropped
      because the bridge queue was full. *)

  val exporter_errors : t -> int64
  (** [exporter_errors bridge] is the number of tracer emissions that raised an
      exception. *)
end
