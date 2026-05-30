let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let endpoint () =
  let value =
    match Sys.getenv_opt "NATS_TEST_SERVER" with
    | Some value -> value
    | None -> failf "NATS_TEST_SERVER is required"
  in
  match Nats.Endpoint.of_string value with
  | Ok value -> value
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

let signal () =
  match Sys.getenv_opt "NATS_TEST_LAMEDUCK_SIGNAL" with
  | Some value -> value
  | None -> failf "NATS_TEST_LAMEDUCK_SIGNAL is required"

let touch path =
  let output = open_out path in
  close_out output

let next_event_with_timeout ~clock ~timeout events =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Event_stream.next events)
    (fun () ->
      Eio.Time.Mono.sleep clock seconds;
      Error Nats_eio.Error.Timeout)

let next_with_timeout ~clock ~timeout subscription =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Subscription.next subscription)
    (fun () ->
      Eio.Time.Mono.sleep clock seconds;
      Error Nats_eio.Error.Timeout)

let subject = Nats.Subject.literal "ocaml.integration.lameduck"
let filter = Nats.Subject.Filter.literal "ocaml.integration.lameduck"

let expect_lame_duck ~clock ~timeout events =
  let rec loop remaining info_seen event_seen =
    if Int.equal remaining 0 then
      failf "lame-duck INFO/event pair was not observed"
    else if info_seen && event_seen then ()
    else
      match next_event_with_timeout ~clock ~timeout events with
      | Error error -> failf "lame-duck event: %s" (error_message error)
      | Ok (Nats_eio.Event.Core (Nats.Event.Info info))
        when Nats.Info.lame_duck_mode info ->
          loop (remaining - 1) true event_seen
      | Ok (Nats_eio.Event.Core Nats.Event.Lame_duck_mode) ->
          loop (remaining - 1) info_seen true
      | Ok _ -> loop (remaining - 1) info_seen event_seen
  in
  loop 16 false false

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(5 * s) in
  let endpoint = endpoint () in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock [ endpoint ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let events = Nats_eio.Connection.events connection in
      let subscription =
        expect_ok "subscribe" (Nats_eio.Connection.subscribe connection filter)
      in
      expect_ok "subscribe flush" (Nats_eio.Connection.flush connection);
      touch (signal ());
      expect_lame_duck ~clock ~timeout events;
      expect_ok "post-lame-duck publish"
        (Nats_eio.Connection.publish connection subject "after");
      expect_ok "post-lame-duck flush" (Nats_eio.Connection.flush connection);
      let delivery =
        expect_ok "post-lame-duck delivery"
          (next_with_timeout ~clock ~timeout subscription)
      in
      if not (String.equal (Nats.Message.payload delivery.message) "after") then
        failf "post-lame-duck payload was %S"
          (Nats.Message.payload delivery.message);
      print_endline "lame_duck: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server lame-duck failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server lame-duck failed: " ^ Printexc.to_string error);
      exit 1
