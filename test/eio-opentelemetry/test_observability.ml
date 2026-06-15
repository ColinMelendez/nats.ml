open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let address = `Tcp (Eio.Net.Ipaddr.V4.loopback, 4222)

let endpoint =
  match Nats.Endpoint.of_string "nats://127.0.0.1:4222" with
  | Ok endpoint -> endpoint
  | Error error -> fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let configure_net net =
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]))

let bump counter =
  let current = Atomic.get counter in
  Atomic.set counter (current + 1)

let exporter ?(raise_export = false) () =
  let spans = Atomic.make 0 in
  let metrics = Atomic.make 0 in
  let base = Opentelemetry.Exporter.dummy () in
  let export =
    if raise_export then fun _ -> failwith "test exporter failure"
    else function
      | Opentelemetry.Any_signal_l.Spans values ->
          List.iter (fun _ -> bump spans) values
      | Opentelemetry.Any_signal_l.Metrics values ->
          List.iter (fun _ -> bump metrics) values
      | Opentelemetry.Any_signal_l.Logs _ -> ()
  in
  ({ base with export }, spans, metrics)

let with_connection ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "nats-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "nats-network" in
  configure_net net;
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ])
  in
  f ~clock:env#mono_clock ~sw connection

let rec yield_n count =
  if count <= 0 then ()
  else (
    Eio.Fiber.yield ();
    yield_n (count - 1))

let () =
  run "nats-eio-opentelemetry"
    [
      test "metrics and lifecycle events are isolated and observable" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~clock ~sw connection ->
              let exporter, spans, metrics = exporter () in
              let tracer = Opentelemetry.Exporter.get_tracer exporter in
              let meter = Opentelemetry.Exporter.get_meter exporter in
              let events = Nats_eio.Connection.events connection in
              (match
                 Nats_eio_opentelemetry.Events.start ~sw ~capacity:0 ~tracer
                   events
               with
              | Error (Nats_eio.Error.Invalid_capacity { name; value }) ->
                  equal string "observability event" name;
                  equal int 0 value
              | Error error ->
                  fail
                    (Format.asprintf "unexpected capacity error: %a"
                       Nats_eio.Error.pp error)
              | Ok _ -> fail "zero event capacity was accepted");
              (match
                 Nats_eio_opentelemetry.Metrics.start ~sw ~clock
                   ~interval:Mtime.Span.zero ~meter connection
               with
              | Error (Nats_eio.Error.Invalid_timeout name) ->
                  equal string "observability interval" name
              | Error error ->
                  fail
                    (Format.asprintf "unexpected interval error: %a"
                       Nats_eio.Error.pp error)
              | Ok _ -> fail "zero metrics interval was accepted");
              let event_bridge =
                expect_ok
                  (Nats_eio_opentelemetry.Events.start ~sw ~tracer events)
              in
              let metrics_bridge =
                expect_ok
                  (Nats_eio_opentelemetry.Metrics.start ~sw ~clock ~meter
                     connection)
              in
              yield_n 12;
              if Atomic.get spans < 2 then
                fail
                  (Format.asprintf "expected initial lifecycle spans, got %d"
                     (Atomic.get spans));
              if Atomic.get metrics < 5 then
                fail
                  (Format.asprintf "expected connection metrics, got %d"
                     (Atomic.get metrics));
              if
                Int64.compare
                  (Nats_eio_opentelemetry.Events.submitted event_bridge)
                  2L
                < 0
              then fail "event bridge did not submit the initial events";
              if
                Int64.compare
                  (Nats_eio_opentelemetry.Metrics.submitted metrics_bridge)
                  1L
                < 0
              then fail "metrics bridge did not submit its initial snapshot";
              if
                not
                  (Int64.equal
                     (Nats_eio_opentelemetry.Events.dropped event_bridge)
                     0L)
              then fail "initial lifecycle events were unexpectedly dropped";
              Eio.Promise.resolve hold_u (Ok "");
              expect_ok (Nats_eio.Connection.close connection)));
      test "exporter failures remain local to the bridge" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~clock ~sw connection ->
              let exporter, spans, metrics = exporter ~raise_export:true () in
              ignore (spans, metrics);
              let tracer = Opentelemetry.Exporter.get_tracer exporter in
              let events = Nats_eio.Connection.events connection in
              let bridge =
                expect_ok
                  (Nats_eio_opentelemetry.Events.start ~sw ~tracer events)
              in
              yield_n 12;
              if
                Int64.compare
                  (Nats_eio_opentelemetry.Events.exporter_errors bridge)
                  1L
                < 0
              then fail "exporter failure was not isolated and counted";
              Eio.Time.Mono.sleep clock 0.;
              Eio.Promise.resolve hold_u (Ok "")));
    ]
