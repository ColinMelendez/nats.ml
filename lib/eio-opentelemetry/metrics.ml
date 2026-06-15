type t = { submitted : int64 Atomic.t; exporter_errors : int64 Atomic.t }

let saturating_add value increment =
  if Int64.compare value (Int64.sub Int64.max_int increment) > 0 then
    Int64.max_int
  else Int64.add value increment

let increment counter =
  let current = Atomic.get counter in
  Atomic.set counter (saturating_add current 1L)

let submitted t = Atomic.get t.submitted
let exporter_errors t = Atomic.get t.exporter_errors

let metric ~attributes ~name ~unit_ value =
  let point = Opentelemetry.Metrics.float ~attrs:attributes value in
  Opentelemetry.Metrics.sum ~name ~unit_ ~is_monotonic:true [ point ]

let emit ~meter ~attributes ~stats =
  let metric name unit_ value =
    metric ~attributes ~name ~unit_ (Int64.to_float value)
  in
  Opentelemetry.Meter.emit meter
    [
      metric "nats.connection.in.messages" "1"
        (Nats_eio.Connection.Stats.in_messages stats);
      metric "nats.connection.in.bytes" "By"
        (Nats_eio.Connection.Stats.in_bytes stats);
      metric "nats.connection.out.messages" "1"
        (Nats_eio.Connection.Stats.out_messages stats);
      metric "nats.connection.out.bytes" "By"
        (Nats_eio.Connection.Stats.out_bytes stats);
      metric "nats.connection.reconnects" "1"
        (Nats_eio.Connection.Stats.reconnects stats);
    ]

let start ~sw ~clock ?(interval = Mtime.Span.(5 * s)) ?(attributes = []) ~meter
    connection =
  if Mtime.Span.compare interval Mtime.Span.zero <= 0 then
    Error (Nats_eio.Error.Invalid_timeout "observability interval")
  else
    let t = { submitted = Atomic.make 0L; exporter_errors = Atomic.make 0L } in
    let seconds = Mtime.Span.to_float_ns interval /. 1e9 in
    let poll () =
      let stats = Nats_eio.Connection.stats connection in
      try
        emit ~meter ~attributes ~stats;
        increment t.submitted
      with
      | Eio.Cancel.Cancelled _ as exn -> raise exn
      | exn ->
          ignore exn;
          increment t.exporter_errors
    in
    let run () =
      poll ();
      while not (Eio.Fiber.is_cancelled ()) do
        Eio.Time.Mono.sleep clock seconds;
        if not (Eio.Fiber.is_cancelled ()) then poll ()
      done;
      `Stop_daemon
    in
    Eio.Fiber.fork_daemon ~sw (fun () ->
        try run () with Eio.Cancel.Cancelled _ -> `Stop_daemon);
    Ok t
