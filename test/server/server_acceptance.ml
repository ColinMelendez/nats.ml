let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let expect_jetstream_config_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %a" label Nats_eio.Jetstream.Error.pp_config error

let expect_header_ok = function
  | Ok value -> value
  | Error error -> failf "invalid test header: %a" Nats.Header.pp_error error

let expect_subject_reply delivery =
  match Nats.Message.reply_to delivery.Nats_eio.Subscription.message with
  | Some subject -> subject
  | None -> failf "request responder received no reply subject"

let safe_credential value =
  String.length value > 0
  && String.for_all
       (fun character ->
         let code = Char.code character in
         (code >= Char.code 'A' && code <= Char.code 'Z')
         || (code >= Char.code 'a' && code <= Char.code 'z')
         || (code >= Char.code '0' && code <= Char.code '9')
         || code = Char.code '_'
         || code = Char.code '-')
       value

let auth () =
  match (Sys.getenv_opt "NATS_TEST_USER", Sys.getenv_opt "NATS_TEST_PASS") with
  | None, None -> None
  | Some user, Some pass when safe_credential user && safe_credential pass ->
      Some (Nats.Auth.user_pass ~user ~pass)
  | Some _, Some _ ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must be non-empty ASCII letters, \
         digits, underscores, or hyphens"
  | _ ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must both be non-empty or both unset"

let next_with_timeout ~clock ~timeout subscription =
  let timeout = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Subscription.next subscription)
    (fun () ->
      Eio.Time.Mono.sleep clock timeout;
      Error Nats_eio.Error.Timeout)

let collect_queue_payloads ~sw ~clock ~timeout ~expected worker_one worker_two =
  let payloads = Eio.Stream.create expected in
  let collect subscription =
    let finished = ref false in
    while not !finished do
      match Nats_eio.Subscription.next subscription with
      | Ok delivery ->
          Eio.Stream.add payloads (Nats.Message.payload delivery.message)
      | Error Nats_eio.Error.Closed -> finished := true
      | Error error -> failf "queue delivery: %s" (error_message error)
    done
  in
  Eio.Fiber.fork ~sw (fun () -> collect worker_one);
  Eio.Fiber.fork ~sw (fun () -> collect worker_two);
  let received = ref [] in
  Eio.Fiber.first
    (fun () ->
      for _ = 1 to expected do
        received := Eio.Stream.take payloads :: !received
      done)
    (fun () ->
      Eio.Time.Mono.sleep clock (Mtime.Span.to_float_ns timeout /. 1e9);
      failf "queue group timed out after receiving %d of %d deliveries"
        (List.length !received) expected);
  List.rev !received

let expect_headers delivery expected =
  let actual = Nats.Header.find_all "x-trace" (Nats.Message.headers delivery) in
  if not (List.equal String.equal actual expected) then
    failf "header values were [%s]" (String.concat ", " actual)

let safe_identifier value =
  String.length value > 0
  && String.for_all
       (fun character ->
         let code = Char.code character in
         (code >= Char.code 'A' && code <= Char.code 'Z')
         || (code >= Char.code 'a' && code <= Char.code 'z')
         || (code >= Char.code '0' && code <= Char.code '9')
         || code = Char.code '_'
         || code = Char.code '-')
       value

let run_jetstream ~client ~timeout =
  let jetstream =
    expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v client)
  in
  let run_id =
    match Sys.getenv_opt "NATS_TEST_JETSTREAM_RUN_ID" with
    | Some value when safe_identifier value -> value
    | _ -> "direct"
  in
  let stream_name = "OCAML_TEST_STREAM_" ^ run_id in
  let subject = Nats.Subject.literal "ocaml.integration.js.events" in
  let filter = Nats.Subject.Filter.literal "ocaml.integration.js.events" in
  let config =
    expect_jetstream_config_ok "jetstream config"
      (Nats_eio.Jetstream.Stream.Config.v ~name:stream_name ~subjects:[ filter ]
         ~storage:Nats_eio.Jetstream.Stream.Config.Memory ())
  in
  let stream =
    expect_jetstream_ok "jetstream stream create"
      (Nats_eio.Jetstream.Stream.create jetstream config)
  in
  let deleted = ref false in
  Fun.protect
    ~finally:(fun () ->
      if not !deleted then
        match Nats_eio.Jetstream.Stream.delete stream with
        | Ok () -> ()
        | Error error ->
            prerr_endline
              (Format.asprintf "JetStream cleanup failed: %a"
                 Nats_eio.Jetstream.Error.pp error))
    (fun () ->
      let initial_info =
        expect_jetstream_ok "jetstream initial stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Stream.Info.messages initial_info)
             0L)
      then failf "new JetStream stream was not empty";
      let first_ack =
        expect_jetstream_ok "jetstream publish"
          (Nats_eio.Jetstream.publish ~timeout ~msg_id:"integration-message-1"
             jetstream subject "hello")
      in
      if
        not
          (String.equal
             (Nats_eio.Jetstream.Publish_ack.stream first_ack)
             stream_name)
      then failf "JetStream publish ack named the wrong stream";
      if Nats_eio.Jetstream.Publish_ack.duplicate first_ack then
        failf "first JetStream publish was marked duplicate";
      let duplicate_ack =
        expect_jetstream_ok "jetstream duplicate publish"
          (Nats_eio.Jetstream.publish ~timeout ~msg_id:"integration-message-1"
             jetstream subject "hello")
      in
      if not (Nats_eio.Jetstream.Publish_ack.duplicate duplicate_ack) then
        failf "duplicate JetStream publish was not marked duplicate";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Publish_ack.sequence duplicate_ack)
             (Nats_eio.Jetstream.Publish_ack.sequence first_ack))
      then failf "duplicate JetStream publish changed its sequence";
      let final_info =
        expect_jetstream_ok "jetstream final stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      if
        not
          (Int64.equal (Nats_eio.Jetstream.Stream.Info.messages final_info) 1L)
      then failf "JetStream stream retained the wrong message count";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Publish_ack.sequence first_ack)
             (Nats_eio.Jetstream.Stream.Info.last_sequence final_info))
      then failf "JetStream stream info disagreed with the publish ack";
      let consumer_name = "OCAML_TEST_CONSUMER_" ^ run_id in
      let consumer_config =
        expect_jetstream_config_ok "jetstream consumer config"
          (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:consumer_name
             ~filter_subject:filter ())
      in
      let consumer =
        expect_jetstream_ok "jetstream consumer create"
          (Nats_eio.Jetstream.Consumer.create stream consumer_config)
      in
      let consumer_deleted = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !consumer_deleted then
            match Nats_eio.Jetstream.Consumer.delete consumer with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "JetStream consumer cleanup failed: %a"
                     Nats_eio.Jetstream.Error.pp error))
        (fun () ->
          let consumer_info =
            expect_jetstream_ok "jetstream consumer info"
              (Nats_eio.Jetstream.Consumer.info consumer)
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Consumer.Info.name consumer_info)
                 consumer_name)
          then failf "JetStream consumer info named the wrong consumer";
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Consumer.Info.stream_name consumer_info)
                 stream_name)
          then failf "JetStream consumer info named the wrong stream";
          let info_config =
            Nats_eio.Jetstream.Consumer.Info.config consumer_info
          in
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.filter_subject info_config
               with
              | Some value ->
                  String.equal
                    (Nats.Subject.Filter.to_string value)
                    (Nats.Subject.Filter.to_string filter)
              | None -> false)
          then failf "JetStream consumer info lost its filter subject";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.durable_name info_config
               with
              | Some value -> String.equal value consumer_name
              | None -> false)
          then failf "JetStream consumer info lost its durable name";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.ack_policy info_config
               with
              | Nats_eio.Jetstream.Consumer.Config.Explicit -> true
              | _ -> false)
          then failf "JetStream consumer info changed its ack policy";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.max_deliver info_config
               with
              | None -> true
              | Some _ -> false)
          then failf "JetStream consumer info exposed an unlimited max deliver";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.deliver_policy info_config
               with
              | Nats_eio.Jetstream.Consumer.Config.All -> true
              | _ -> false)
          then failf "JetStream consumer info changed its deliver policy";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Consumer.Info.num_pending consumer_info)
                 1L)
          then failf "JetStream consumer info reported the wrong pending count";
          let one_message label = function
            | [ message ] -> message
            | messages ->
                failf "%s returned %d messages"
                  label (List.length messages)
          in
          let first_message =
            one_message "JetStream fetch"
              (expect_jetstream_ok "jetstream fetch"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Msg.payload first_message)
                 "hello")
          then failf "JetStream fetch returned the wrong payload";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Msg.stream_sequence first_message)
                 1L)
          then failf "JetStream fetch returned the wrong stream sequence";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Msg.num_delivered first_message)
                 1L)
          then failf "JetStream fetch returned the wrong delivery count";
          if
            Int64.compare
              (Nats_eio.Jetstream.Msg.timestamp first_message)
              0L
            <= 0
          then failf "JetStream fetch returned an invalid timestamp";
          expect_jetstream_ok "JetStream ack"
            (Nats_eio.Jetstream.Msg.ack first_message);
          (match
             expect_jetstream_ok "empty JetStream fetch"
               (Nats_eio.Jetstream.Consumer.fetch
                  ~expires:Mtime.Span.(1 * ms)
                  consumer ~batch:1)
           with
          | [] -> ()
          | messages ->
              failf "empty JetStream fetch returned %d messages"
                (List.length messages));
          let second_ack =
            expect_jetstream_ok "second JetStream publish"
              (Nats_eio.Jetstream.publish ~timeout ~msg_id:"integration-message-2"
                 jetstream subject "world")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate second_ack then
            failf "second JetStream publish was marked duplicate";
          let second_message =
            one_message "JetStream NAK fetch"
              (expect_jetstream_ok "JetStream NAK fetch"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          expect_jetstream_ok "JetStream NAK"
            (Nats_eio.Jetstream.Msg.nak second_message);
          let redelivered_message =
            one_message "JetStream redelivery"
              (expect_jetstream_ok "JetStream redelivery"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Msg.num_delivered redelivered_message)
                 2L)
          then failf "JetStream NAK did not cause a redelivery";
          expect_jetstream_ok "JetStream redelivery ack"
            (Nats_eio.Jetstream.Msg.ack redelivered_message);
          let max_bytes_ack =
            expect_jetstream_ok "max-bytes JetStream publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-max-bytes" jetstream subject
                 "large")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate max_bytes_ack then
            failf "max-bytes JetStream publish was marked duplicate";
          (match
             expect_jetstream_ok "max-bytes JetStream fetch"
               (Nats_eio.Jetstream.Consumer.fetch
                  ~expires:Mtime.Span.(250 * ms)
                  ~max_bytes:1 consumer ~batch:1)
           with
          | [] -> ()
          | messages ->
              failf "max-bytes JetStream fetch returned %d messages"
                (List.length messages));
          let max_bytes_message =
            one_message "max-bytes JetStream redelivery"
              (expect_jetstream_ok "max-bytes JetStream redelivery"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Msg.payload max_bytes_message)
                 "large")
          then failf "max-bytes JetStream fetch lost the pending message";
          expect_jetstream_ok "max-bytes JetStream ack"
            (Nats_eio.Jetstream.Msg.ack max_bytes_message);
          let rebound =
            expect_jetstream_ok "jetstream consumer bind"
              (Nats_eio.Jetstream.Consumer.bind stream ~name:consumer_name)
          in
          let rebound_info =
            expect_jetstream_ok "jetstream rebound consumer info"
              (Nats_eio.Jetstream.Consumer.info rebound)
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Consumer.Info.name rebound_info)
                 consumer_name)
          then failf "bound JetStream consumer named the wrong consumer";
          expect_jetstream_ok "jetstream consumer delete"
            (Nats_eio.Jetstream.Consumer.delete consumer);
          consumer_deleted := true;
          (match Nats_eio.Jetstream.Consumer.info consumer with
          | Error (Nats_eio.Jetstream.Error.Api _) -> ()
          | Ok _ -> failf "deleted JetStream consumer still existed"
          | Error error ->
              failf "deleted JetStream consumer info: %s"
                (jetstream_error_message error));
          print_endline "jetstream_consumer: ok");
      expect_jetstream_ok "jetstream stream delete"
        (Nats_eio.Jetstream.Stream.delete stream);
      deleted := true;
      print_endline "jetstream: ok")

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

let connect ~sw ~net ~clock ?config endpoint =
  expect_ok "connect"
    (Nats_eio.Connection.connect ~sw ~net ~clock ?config [ endpoint ])

let expect_auth_required ~sw ~net ~clock endpoint =
  match Nats_eio.Connection.connect ~sw ~net ~clock [ endpoint ] with
  | Error (Nats_eio.Error.Auth Nats.Auth.Auth_required) -> ()
  | Ok connection ->
      expect_ok "close anonymous connection"
        (Nats_eio.Connection.close connection);
      failf "anonymous connection succeeded against an auth server"
  | Error error -> failf "anonymous connection: %s" (error_message error)

let run env =
  Eio.Switch.run @@ fun sw ->
  let endpoint = endpoint () in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(2 * s) in
  let auth = auth () in
  let config =
    match auth with
    | None -> None
    | Some auth ->
        Some (expect_ok "auth config" (Nats_eio.Connection.Config.v ~auth ()))
  in
  let client = connect ~sw ~net ~clock ?config endpoint in
  let responder = connect ~sw ~net ~clock ?config endpoint in
  let worker_one = connect ~sw ~net ~clock ?config endpoint in
  let worker_two = connect ~sw ~net ~clock ?config endpoint in
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
    expect_ok "delivery" (next_with_timeout ~clock ~timeout subscription)
  in
  if not (String.equal (Nats.Message.payload delivery.message) "hello") then
    failf "delivery payload was %S" (Nats.Message.payload delivery.message);
  print_endline "pubsub: ok";
  let headers =
    expect_header_ok
      (Nats.Header.of_list [ ("X-Trace", "one"); ("x-trace", "two") ])
  in
  let headers_subject = Nats.Subject.literal "ocaml.integration.headers" in
  let headers_filter =
    Nats.Subject.Filter.literal "ocaml.integration.headers"
  in
  let headers_subscription =
    expect_ok "headers subscribe"
      (Nats_eio.Connection.subscribe client headers_filter)
  in
  expect_ok "headers subscribe flush" (Nats_eio.Connection.flush client);
  expect_ok "headers publish"
    (Nats_eio.Connection.publish responder ~headers headers_subject "payload");
  expect_ok "headers publish flush" (Nats_eio.Connection.flush responder);
  let headers_delivery =
    expect_ok "headers delivery"
      (next_with_timeout ~clock ~timeout headers_subscription)
  in
  if
    not (String.equal (Nats.Message.payload headers_delivery.message) "payload")
  then
    failf "header delivery payload was %S"
      (Nats.Message.payload headers_delivery.message);
  expect_headers headers_delivery.message [ "one"; "two" ];
  print_endline "headers: ok";
  let queue_subject = Nats.Subject.literal "ocaml.integration.queue" in
  let queue_filter = Nats.Subject.Filter.literal "ocaml.integration.queue" in
  let queue_group = Nats.Queue_group.literal "ocaml.integration.workers" in
  let worker_one_subscription =
    expect_ok "worker one subscribe"
      (Nats_eio.Connection.subscribe worker_one ~queue_group queue_filter)
  in
  let worker_two_subscription =
    expect_ok "worker two subscribe"
      (Nats_eio.Connection.subscribe worker_two ~queue_group queue_filter)
  in
  expect_ok "worker one subscribe flush" (Nats_eio.Connection.flush worker_one);
  expect_ok "worker two subscribe flush" (Nats_eio.Connection.flush worker_two);
  let queue_payloads = [ "one"; "two"; "three"; "four" ] in
  List.iter
    (fun payload ->
      expect_ok "queue publish"
        (Nats_eio.Connection.publish responder queue_subject payload))
    queue_payloads;
  expect_ok "queue publish flush" (Nats_eio.Connection.flush responder);
  let delivered_payloads =
    List.sort String.compare
      (collect_queue_payloads ~sw ~clock ~timeout
         ~expected:(List.length queue_payloads)
         worker_one_subscription worker_two_subscription)
  in
  let expected_payloads = List.sort String.compare queue_payloads in
  if not (List.equal String.equal delivered_payloads expected_payloads) then
    failf "queue group delivered [%s]" (String.concat ", " delivered_payloads);
  print_endline "queue_group: ok";
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
  let no_responder_subject =
    Nats.Subject.literal "ocaml.integration.no_responder"
  in
  (match
     Nats_eio.Connection.request ~timeout client no_responder_subject "ping"
   with
  | Error Nats_eio.Error.No_responders -> print_endline "no_responders: ok"
  | Ok response ->
      failf "no-responder request returned %S" (Nats.Message.payload response)
  | Error error -> failf "no-responder request: %s" (error_message error));
  expect_ok "flush" (Nats_eio.Connection.flush client);
  print_endline "flush: ok";
  (match Sys.getenv_opt "NATS_TEST_JETSTREAM" with
  | Some "1" -> run_jetstream ~client ~timeout
  | _ -> ());
  expect_ok "close responder" (Nats_eio.Connection.close responder);
  expect_ok "close worker one" (Nats_eio.Connection.close worker_one);
  expect_ok "close worker two" (Nats_eio.Connection.close worker_two);
  expect_ok "close client" (Nats_eio.Connection.close client);
  print_endline "close: ok";
  match auth with
  | None -> ()
  | Some _ ->
      expect_auth_required ~sw ~net ~clock endpoint;
      print_endline "auth: user_pass"

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server acceptance failed: " ^ Printexc.to_string error);
      exit 1
