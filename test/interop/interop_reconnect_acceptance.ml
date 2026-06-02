let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let endpoint value =
  match Nats.Endpoint.of_string (String.trim value) with
  | Ok value -> value
  | Error error ->
      failf "invalid endpoint %S: %a" value Nats.Endpoint.pp_error error

let endpoints () =
  match Sys.getenv_opt "NATS_TEST_SERVERS" with
  | None -> failf "NATS_TEST_SERVERS is required"
  | Some value -> (
      match
        List.filter
          (fun value -> not (String.equal (String.trim value) ""))
          (String.split_on_char ',' value)
      with
      | first :: second :: rest -> List.map endpoint (first :: second :: rest)
      | _ -> failf "NATS_TEST_SERVERS must contain at least two endpoints")

let auth () =
  match
    ( Sys.getenv_opt "NATS_TEST_USER",
      Sys.getenv_opt "NATS_TEST_PASS",
      Sys.getenv_opt "NATS_TEST_TOKEN" )
  with
  | None, None, None -> None
  | None, None, Some token -> Some (Nats.Auth.token token)
  | Some user, Some pass, None -> Some (Nats.Auth.user_pass ~user ~pass)
  | _ ->
      failf
        "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let read_file path = In_channel.with_open_bin path In_channel.input_all

let tls_config () =
  match Sys.getenv_opt "NATS_TEST_TLS_CA" with
  | None -> None
  | Some ca_file -> (
      let ca =
        match X509.Certificate.decode_pem (read_file ca_file) with
        | Ok value -> value
        | Error (`Msg message) ->
            failf "invalid test CA certificate: %s" message
      in
      let authenticator =
        X509.Authenticator.chain_of_trust
          ~time:(fun () -> Some (Ptime_clock.now ()))
          [ ca ]
      in
      let peer_name =
        Domain_name.host_exn (Domain_name.of_string_exn "localhost")
      in
      match Tls.Config.client ~authenticator ~peer_name () with
      | Ok value -> Some value
      | Error (`Msg message) -> failf "TLS client configuration: %s" message)

let next_event ~clock ~timeout events =
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

let rec wait_for_event ~clock ~timeout ~label ~remaining predicate events =
  if Int.equal remaining 0 then failf "timed out waiting for %s" label
  else
    let event = next_event ~clock ~timeout events in
    if predicate event then ()
    else
      wait_for_event ~clock ~timeout ~label ~remaining:(remaining - 1) predicate
        events

let expect_initial_connection ~clock ~timeout events =
  wait_for_event ~clock ~timeout ~label:"initial connection" ~remaining:8
    (function Nats_eio.Event.Core Nats.Event.Connected -> true | _ -> false)
    events

let expect_disconnected ~clock ~timeout events =
  wait_for_event ~clock ~timeout ~label:"disconnect" ~remaining:16
    (function Nats_eio.Event.Disconnected -> true | _ -> false)
    events

let expect_reconnected ~clock ~timeout events =
  wait_for_event ~clock ~timeout ~label:"reconnect" ~remaining:16
    (function Nats_eio.Event.Reconnected -> true | _ -> false)
    events

let next_message ~timeout label subscription =
  match Nats_eio.Subscription.next_with_timeout ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let expect_recovered ~timeout ~expected_generation subscription initial =
  let recovery = ref initial in
  let recovered = ref false in
  while not !recovered do
    let next =
      expect_ok "subscription recovery"
        (Nats_eio.Subscription.await_recovery ~timeout ~from:!recovery
           subscription)
    in
    match next with
    | Nats_eio.Subscription.Attached generation
      when Int.equal generation expected_generation ->
        recovered := true
    | Nats_eio.Subscription.Detached _ -> recovery := next
    | Nats_eio.Subscription.Attached generation ->
        failf "subscription attached at unexpected generation %d" generation
  done

let await_go_ready ~clock ~timeout ~cycle connection reconnect_ready =
  let request_payload = "ocaml-ready-" ^ string_of_int cycle in
  let response_payload = "go-ready-" ^ string_of_int cycle in
  let deadline =
    match Mtime.add_span (Nats_eio.Connection.now connection) timeout with
    | Some deadline -> deadline
    | None -> Mtime.max_stamp
  in
  let attempt_timeout = Mtime.Span.(500 * ms) in
  let ready = ref false in
  while not !ready do
    let now = Nats_eio.Connection.now connection in
    if Mtime.compare now deadline >= 0 then
      failf "timed out waiting for Go recovery barrier"
    else
      let remaining = Mtime.span now deadline in
      let request_timeout =
        if Mtime.Span.compare remaining attempt_timeout < 0 then remaining
        else attempt_timeout
      in
      match
        Nats_eio.Connection.request ~timeout:request_timeout connection
          reconnect_ready request_payload
      with
      | Ok response ->
          expect_payload "Go recovery barrier" response_payload response;
          ready := true
      | Error Nats_eio.Error.Timeout
      | Error Nats_eio.Error.No_responders
      | Error Nats_eio.Error.Disconnected ->
          Eio.Time.Mono.sleep clock 0.01
      | Error error -> failf "Go recovery barrier: %s" (error_message error)
  done

let subject prefix suffix = Nats.Subject.literal (prefix ^ "." ^ suffix)
let filter prefix suffix = Nats.Subject.Filter.literal (prefix ^ "." ^ suffix)
let round_payload round = "round-" ^ string_of_int round
let round_marker round = round_payload round ^ "-flushed"

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoints = endpoints () in
  let cycles = List.length endpoints - 1 in
  let prefix =
    match Sys.getenv_opt "NATS_TEST_INTEROP_PREFIX" with
    | Some value -> value
    | None -> failf "NATS_TEST_INTEROP_PREFIX is required"
  in
  let auth = auth () in
  let tls = tls_config () in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ?auth ?tls ~max_reconnect_attempts:(Some 20)
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
      let timeout = Mtime.Span.(10 * s) in
      let events = Nats_eio.Connection.events connection in
      expect_initial_connection ~clock ~timeout events;
      let from_go =
        expect_ok "subscribe from-go"
          (Nats_eio.Connection.subscribe connection (filter prefix "from-go"))
      in
      expect_ok "subscribe from-go flush" (Nats_eio.Connection.flush connection);
      let initial_recovery = Nats_eio.Subscription.recovery from_go in
      (match initial_recovery with
      | Nats_eio.Subscription.Attached 0 -> ()
      | Nats_eio.Subscription.Attached generation ->
          failf "initial subscription generation was %d" generation
      | Nats_eio.Subscription.Detached generation ->
          failf "initial subscription was detached at generation %d" generation);
      let to_go = subject prefix "to-go" in
      let start = subject prefix "start" in
      let start_response =
        expect_ok "start Go reconnect peer"
          (Nats_eio.Connection.request ~timeout connection start "start")
      in
      expect_payload "start response" "started" start_response;
      let recovery = ref initial_recovery in
      for round = 0 to cycles do
        let payload = round_payload round in
        let marker = round_marker round in
        let from_go_message = next_message ~timeout ("Go " ^ payload) from_go in
        expect_payload ("Go " ^ payload) payload from_go_message;
        expect_ok ("publish " ^ payload)
          (Nats_eio.Connection.publish connection to_go payload);
        expect_ok ("flush " ^ payload) (Nats_eio.Connection.flush connection);
        expect_ok ("publish " ^ marker)
          (Nats_eio.Connection.publish connection to_go marker);
        if round < cycles then (
          expect_disconnected ~clock ~timeout events;
          expect_reconnected ~clock ~timeout events;
          expect_recovered ~timeout ~expected_generation:(round + 1) from_go
            !recovery;
          recovery := Nats_eio.Subscription.Attached (round + 1);
          await_go_ready ~clock ~timeout ~cycle:(round + 1) connection
            (subject prefix ("reconnect-ready." ^ string_of_int (round + 1))))
      done;
      expect_ok "drain" (Nats_eio.Connection.drain connection);
      expect_ok "close after drain" (Nats_eio.Connection.close connection);
      print_endline "interop-reconnect: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("interop reconnect acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("interop reconnect acceptance failed: " ^ Printexc.to_string error);
      exit 1
