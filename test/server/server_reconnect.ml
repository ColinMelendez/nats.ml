let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let endpoint value =
  match Nats.Endpoint.of_string value with
  | Ok value -> value
  | Error error ->
      failf "invalid endpoint %S: %a" value Nats.Endpoint.pp_error error

let endpoints () =
  match Sys.getenv_opt "NATS_TEST_SERVERS" with
  | None -> failf "NATS_TEST_SERVERS is required"
  | Some value -> (
      let values =
        List.filter
          (fun value -> not (String.equal value ""))
          (String.split_on_char ',' value)
      in
      match values with
      | [ first; second ] -> [ endpoint first; endpoint second ]
      | _ -> failf "NATS_TEST_SERVERS must contain exactly two endpoints")

let timeout = Mtime.Span.(5 * s)

let next_event ~clock events =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  match
    Eio.Fiber.first
      (fun () -> Nats_eio.Event_stream.next events)
      (fun () ->
        Eio.Time.Mono.sleep clock seconds;
        Error Nats_eio.Error.Timeout)
  with
  | Ok event -> event
  | Error error -> failf "lifecycle event: %s" (error_message error)

let rec wait_for_event ~clock ~label ~remaining predicate events =
  if Int.equal remaining 0 then failf "timed out waiting for %s" label
  else
    let event = next_event ~clock events in
    if predicate event then event
    else
      wait_for_event ~clock ~label ~remaining:(remaining - 1) predicate events

let expect_initial_connection ~clock events =
  ignore
    (wait_for_event ~clock ~label:"initial connection" ~remaining:8
       (function
         | Nats_eio.Event.Core Nats.Event.Connected -> true | _ -> false)
       events)

let expect_disconnected ~clock events =
  ignore
    (wait_for_event ~clock ~label:"disconnect" ~remaining:16
       (function Nats_eio.Event.Disconnected -> true | _ -> false)
       events)

let expect_reconnected ~clock events =
  ignore
    (wait_for_event ~clock ~label:"reconnect" ~remaining:16
       (function Nats_eio.Event.Reconnected -> true | _ -> false)
       events)

let touch path =
  let output = open_out path in
  close_out output

let expect_recovered ~clock subscription initial =
  let recovery =
    expect_ok "subscription recovery"
      (Nats_eio.Subscription.await_recovery ~timeout ~from:initial subscription)
  in
  match recovery with
  | Nats_eio.Subscription.Attached generation when Int.equal generation 1 -> ()
  | Nats_eio.Subscription.Detached generation -> (
      let recovered =
        expect_ok "subscription reattachment"
          (Nats_eio.Subscription.await_recovery ~timeout ~from:recovery
             subscription)
      in
      match recovered with
      | Nats_eio.Subscription.Attached generation when Int.equal generation 1 ->
          ()
      | Nats_eio.Subscription.Attached generation ->
          failf "subscription attached at unexpected generation %d" generation
      | Nats_eio.Subscription.Detached next_generation ->
          failf "subscription remained detached at generation %d"
            next_generation)
  | Nats_eio.Subscription.Attached generation ->
      failf "subscription attached at unexpected generation %d" generation

let run env =
  let endpoints = endpoints () in
  let signal =
    match Sys.getenv_opt "NATS_TEST_RECONNECT_SIGNAL" with
    | Some value -> value
    | None -> failf "NATS_TEST_RECONNECT_SIGNAL is required"
  in
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 20)
         ~reconnect_delay:Mtime.Span.(50 * ms)
         ~reconnect_max_delay:Mtime.Span.(100 * ms)
         ())
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config endpoints)
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let events = Nats_eio.Connection.events connection in
      expect_initial_connection ~clock events;
      let subject = Nats.Subject.literal "ocaml.integration.reconnect" in
      let filter = Nats.Subject.Filter.literal "ocaml.integration.reconnect" in
      let subscription =
        expect_ok "subscribe" (Nats_eio.Connection.subscribe connection filter)
      in
      expect_ok "subscribe flush" (Nats_eio.Connection.flush connection);
      let initial_recovery = Nats_eio.Subscription.recovery subscription in
      (match initial_recovery with
      | Nats_eio.Subscription.Attached 0 -> ()
      | Nats_eio.Subscription.Attached generation ->
          failf "initial subscription generation was %d" generation
      | Nats_eio.Subscription.Detached generation ->
          failf "initial subscription was detached at generation %d" generation);
      expect_ok "baseline publish"
        (Nats_eio.Connection.publish connection subject "before");
      expect_ok "baseline flush" (Nats_eio.Connection.flush connection);
      let baseline =
        expect_ok "baseline delivery"
          (Nats_eio.Subscription.next_with_timeout ~timeout subscription)
      in
      if not (String.equal (Nats.Message.payload baseline.message) "before")
      then
        failf "baseline payload was %S" (Nats.Message.payload baseline.message);
      touch signal;
      expect_disconnected ~clock events;
      expect_reconnected ~clock events;
      expect_recovered ~clock subscription initial_recovery;
      expect_ok "post-reconnect publish"
        (Nats_eio.Connection.publish connection subject "after");
      expect_ok "post-reconnect flush" (Nats_eio.Connection.flush connection);
      let recovered =
        expect_ok "post-reconnect delivery"
          (Nats_eio.Subscription.next_with_timeout ~timeout subscription)
      in
      if not (String.equal (Nats.Message.payload recovered.message) "after")
      then
        failf "post-reconnect payload was %S"
          (Nats.Message.payload recovered.message);
      print_endline "reconnect: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server reconnect failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server reconnect failed: " ^ Printexc.to_string error);
      exit 1
