type item = Event of Nats_eio.Event.t | Terminal of Nats_eio.Error.t

type t = {
  dropped : int64 Atomic.t;
  submitted : int64 Atomic.t;
  exporter_errors : int64 Atomic.t;
}

let saturating_add value increment =
  if Int64.compare value (Int64.sub Int64.max_int increment) > 0 then
    Int64.max_int
  else Int64.add value increment

let increment counter =
  let current = Atomic.get counter in
  Atomic.set counter (saturating_add current 1L)

let dropped t = Atomic.get t.dropped
let submitted t = Atomic.get t.submitted
let exporter_errors t = Atomic.get t.exporter_errors

let event_name = function
  | Nats_eio.Event.Core (Nats.Event.Info _) -> "info"
  | Nats_eio.Event.Core Nats.Event.Connected -> "connected"
  | Nats_eio.Event.Core Nats.Event.Lame_duck_mode -> "lame_duck_mode"
  | Nats_eio.Event.Core (Nats.Event.Server_error _) -> "server_error"
  | Nats_eio.Event.Core (Nats.Event.Protocol_notice _) -> "protocol_notice"
  | Nats_eio.Event.Core Nats.Event.Flush_completed -> "flush_completed"
  | Nats_eio.Event.Core Nats.Event.Draining -> "draining"
  | Nats_eio.Event.Core Nats.Event.Closed -> "closed"
  | Nats_eio.Event.Disconnected -> "disconnected"
  | Nats_eio.Event.Reconnected -> "reconnected"
  | Nats_eio.Event.Slow_consumer Nats_eio.Error.Events -> "slow_consumer.events"
  | Nats_eio.Event.Slow_consumer (Nats_eio.Error.Subscription _) ->
      "slow_consumer.subscription"

let terminal_name = function
  | Nats_eio.Error.Closed -> "closed"
  | Nats_eio.Error.Disconnected -> "disconnected"
  | Nats_eio.Error.Draining -> "draining"
  | Nats_eio.Error.Slow_consumer _ -> "slow_consumer"
  | _ -> "error"

let emit_span ~tracer ~name ~attributes =
  Opentelemetry.Tracer.with_ ~tracer
    ~kind:Opentelemetry.Span_kind.Span_kind_internal ~attrs:attributes name
    (fun _span -> ())

let emit_item t ~tracer = function
  | Event event ->
      let name = event_name event in
      emit_span ~tracer
        ~name:("nats.connection." ^ name)
        ~attributes:[ ("nats.event.kind", `String name) ]
  | Terminal error ->
      let name = terminal_name error in
      emit_span ~tracer ~name:"nats.connection.terminal"
        ~attributes:
          [
            ("nats.event.kind", `String "terminal");
            ("nats.event.terminal", `String name);
          ]

let enqueue t ~capacity queue item =
  let is_terminal = match item with Event _ -> false | Terminal _ -> true in
  let limit = if is_terminal then capacity + 1 else capacity in
  if Eio.Stream.length queue >= limit then (
    if not is_terminal then increment t.dropped;
    false)
  else (
    Eio.Stream.add queue item;
    true)

let start ~sw ?(capacity = 64) ~tracer events =
  if capacity <= 0 || Int.equal capacity max_int then
    Error
      (Nats_eio.Error.Invalid_capacity
         { name = "observability event"; value = capacity })
  else
    let t =
      {
        dropped = Atomic.make 0L;
        submitted = Atomic.make 0L;
        exporter_errors = Atomic.make 0L;
      }
    in
    let queue = Eio.Stream.create (capacity + 1) in
    let rec pump () =
      match Nats_eio.Event_stream.next events with
      | Ok event ->
          ignore (enqueue t ~capacity queue (Event event));
          pump ()
      | Error error -> ignore (enqueue t ~capacity queue (Terminal error))
    in
    let rec export () =
      match Eio.Stream.take queue with
      | Event _ as item ->
          (try
             emit_item t ~tracer item;
             increment t.submitted
           with
          | Eio.Cancel.Cancelled _ as exn -> raise exn
          | exn ->
              ignore exn;
              increment t.exporter_errors);
          export ()
      | Terminal _ as item -> (
          try
            emit_item t ~tracer item;
            increment t.submitted
          with
          | Eio.Cancel.Cancelled _ as exn -> raise exn
          | exn ->
              ignore exn;
              increment t.exporter_errors)
    in
    ignore (Eio.Fiber.fork_promise ~sw pump);
    ignore (Eio.Fiber.fork_promise ~sw export);
    Ok t
