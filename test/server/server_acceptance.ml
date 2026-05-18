let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_subject_reply delivery =
  match Nats.Message.reply_to delivery.Nats_eio.Subscription.message with
  | Some subject -> subject
  | None -> failf "request responder received no reply subject"

let next_with_timeout ~clock ~timeout subscription =
  let timeout = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Subscription.next subscription)
    (fun () ->
      Eio.Time.Mono.sleep clock timeout;
      Error Nats_eio.Error.Timeout)

let endpoint () =
  let value =
    match Sys.getenv_opt "NATS_TEST_SERVER" with
    | Some value -> value
    | None -> "nats://127.0.0.1:4222"
  in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

let connect ~sw ~net ~clock endpoint =
  expect_ok "connect"
    (Nats_eio.Connection.connect ~sw ~net ~clock [ endpoint ])

let run env =
  Eio.Switch.run @@ fun sw ->
  let endpoint = endpoint () in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(2 * s) in
  let client = connect ~sw ~net ~clock endpoint in
  let responder = connect ~sw ~net ~clock endpoint in
  let events_subject = Nats.Subject.literal "ocaml.integration.events" in
  let events_filter = Nats.Subject.Filter.literal "ocaml.integration.events" in
  let subscription =
    expect_ok "subscribe" (Nats_eio.Connection.subscribe client events_filter)
  in
  expect_ok "subscribe flush" (Nats_eio.Connection.flush client);
  expect_ok "publish"
    (Nats_eio.Connection.publish responder events_subject "hello");
  expect_ok "publish flush" (Nats_eio.Connection.flush responder);
  let delivery =
    expect_ok "delivery"
      (next_with_timeout ~clock ~timeout subscription)
  in
  if not (String.equal (Nats.Message.payload delivery.message) "hello") then
    failf "delivery payload was %S" (Nats.Message.payload delivery.message);
  print_endline "pubsub: ok";
  let request_subject = Nats.Subject.literal "ocaml.integration.request" in
  let request_filter =
    Nats.Subject.Filter.literal "ocaml.integration.request"
  in
  let request_subscription =
    expect_ok "request subscribe"
      (Nats_eio.Connection.subscribe responder request_filter)
  in
  expect_ok "request subscribe flush" (Nats_eio.Connection.flush responder);
  let responder_done, responder_done_u = Eio.Promise.create () in
  Eio.Fiber.fork ~sw (fun () ->
      let result =
        match next_with_timeout ~clock ~timeout request_subscription with
        | Error error -> Error (error_message error)
        | Ok delivery -> (
            let reply_to = expect_subject_reply delivery in
            match Nats_eio.Connection.publish responder reply_to "pong" with
            | Ok () -> Ok ()
            | Error error -> Error (error_message error))
      in
      Eio.Promise.resolve responder_done_u result);
  let response =
    expect_ok "request"
      (Nats_eio.Connection.request ~timeout client request_subject "ping")
  in
  (match Eio.Promise.await responder_done with
  | Ok () -> ()
  | Error message -> failf "request responder: %s" message);
  if not (String.equal (Nats.Message.payload response) "pong") then
    failf "response payload was %S" (Nats.Message.payload response);
  print_endline "request: ok";
  expect_ok "flush" (Nats_eio.Connection.flush client);
  print_endline "flush: ok";
  expect_ok "close responder" (Nats_eio.Connection.close responder);
  expect_ok "close client" (Nats_eio.Connection.close client);
  print_endline "close: ok"

let () =
  try Eio_main.run run
  with Failure message ->
    prerr_endline ("server acceptance failed: " ^ message);
    exit 1
  | error ->
      prerr_endline
        ("server acceptance failed: " ^ Printexc.to_string error);
      exit 1
