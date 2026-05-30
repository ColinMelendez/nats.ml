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

let authentication () =
  match
    ( Sys.getenv_opt "NATS_TEST_USER",
      Sys.getenv_opt "NATS_TEST_PASS",
      Sys.getenv_opt "NATS_TEST_TOKEN" )
  with
  | None, None, None -> None
  | None, None, Some token when safe_credential token ->
      Some (Nats.Auth.token token)
  | Some user, Some pass, None when safe_credential user && safe_credential pass
    -> Some (Nats.Auth.user_pass ~user ~pass)
  | Some _, Some _, None ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must be non-empty ASCII letters, \
         digits, underscores, or hyphens"
  | None, None, Some _ ->
      failf
        "NATS_TEST_TOKEN must be non-empty ASCII letters, digits, underscores, \
         or hyphens"
  | _ ->
      failf
        "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let connection_config ?subscription_capacity () =
  let auth = authentication () in
  match (auth, subscription_capacity) with
  | None, None -> None
  | _ ->
      Some
        (expect_ok "connection config"
           (Nats_eio.Connection.Config.v ?subscription_capacity ?auth ()))

let connect ~sw ~net ~clock ?config endpoint =
  expect_ok "connect"
    (Nats_eio.Connection.connect ~sw ~net ~clock ?config [ endpoint ])

let next_with_timeout ~clock ~timeout subscription =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Subscription.next subscription)
    (fun () ->
      Eio.Time.Mono.sleep clock seconds;
      Error Nats_eio.Error.Timeout)

let next_event_with_timeout ~clock ~timeout events =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Event_stream.next events)
    (fun () ->
      Eio.Time.Mono.sleep clock seconds;
      Error Nats_eio.Error.Timeout)

let expect_slow_consumer_event ~clock ~timeout ~sid events =
  let rec loop remaining =
    if Int.equal remaining 0 then
      failf "slow-consumer event was not observed for subscription %d" sid
    else
      match next_event_with_timeout ~clock ~timeout events with
      | Error error -> failf "slow-consumer event: %s" (error_message error)
      | Ok
          (Nats_eio.Event.Slow_consumer
             (Nats_eio.Error.Subscription { sid = event_sid }))
        when Int.equal event_sid sid ->
          ()
      | Ok (Nats_eio.Event.Core _) -> loop (remaining - 1)
      | Ok event ->
          failf "unexpected event while waiting for slow consumer: %a"
            Nats_eio.Event.pp event
  in
  loop 8

let expect_message_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S" label actual

let expect_payload label expected delivery =
  expect_message_payload label expected delivery.Nats_eio.Subscription.message

let expect_closed label = function
  | Error Nats_eio.Error.Closed -> ()
  | Ok _ -> failf "%s unexpectedly delivered a message" label
  | Error error -> failf "%s: %s" label (error_message error)

let subject name = Nats.Subject.literal ("ocaml.integration.lifecycle." ^ name)

let filter name =
  Nats.Subject.Filter.literal ("ocaml.integration.lifecycle." ^ name)

let expect_parent_switch_cleanup ~net ~clock ~endpoint ?config () =
  let stopped, stopped_u = Eio.Promise.create () in
  let switch_failed =
    try
      Eio.Switch.run @@ fun child_sw ->
      let child_client = connect ~sw:child_sw ~net ~clock ?config endpoint in
      let child_subscription =
        expect_ok "parent cleanup subscribe"
          (Nats_eio.Connection.subscribe child_client (filter "parent-cleanup"))
      in
      expect_ok "parent cleanup subscribe flush"
        (Nats_eio.Connection.flush child_client);
      Eio.Fiber.fork ~sw:child_sw (fun () ->
          let stopped =
            try
              match Nats_eio.Subscription.next child_subscription with
              | Ok _ -> false
              | Error Nats_eio.Error.Closed -> true
              | Error _ -> false
            with Eio.Cancel.Cancelled _ -> true
          in
          Eio.Promise.resolve stopped_u stopped);
      Eio.Fiber.fork ~sw:child_sw (fun () ->
          Eio.Time.Mono.sleep clock 0.1;
          Eio.Switch.fail child_sw (Failure "parent switch cleanup"));
      false
    with Failure message when String.equal message "parent switch cleanup" ->
      true
  in
  if not switch_failed then failf "parent switch did not fail as expected";
  if not (Eio.Promise.await stopped) then
    failf "subscription read survived parent switch cleanup"

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoint = endpoint () in
  let config = connection_config () in
  let timeout = Mtime.Span.(2 * s) in
  let client = connect ~sw ~net ~clock ?config endpoint in
  let responder = connect ~sw ~net ~clock ?config endpoint in
  let slow_config = connection_config ~subscription_capacity:1 () in
  let slow_client = connect ~sw ~net ~clock ?config:slow_config endpoint in
  Fun.protect
    ~finally:(fun () ->
      ignore (Nats_eio.Connection.close slow_client);
      ignore (Nats_eio.Connection.close responder);
      ignore (Nats_eio.Connection.close client))
    (fun () ->
      let timeout_subject = subject "timeout" in
      let timeout_subscription =
        expect_ok "timeout subscribe"
          (Nats_eio.Connection.subscribe responder (filter "timeout"))
      in
      expect_ok "timeout subscribe flush" (Nats_eio.Connection.flush responder);
      let timeout_seen, timeout_seen_u = Eio.Promise.create () in
      Eio.Fiber.fork ~sw (fun () ->
          match next_with_timeout ~clock ~timeout timeout_subscription with
          | Ok delivery -> Eio.Promise.resolve timeout_seen_u (Some delivery)
          | Error _ -> Eio.Promise.resolve timeout_seen_u None);
      (match
         Nats_eio.Connection.request
           ~timeout:Mtime.Span.(100 * ms)
           client timeout_subject "timeout"
       with
      | Error Nats_eio.Error.Timeout -> ()
      | Ok _ -> failf "silent request unexpectedly returned a response"
      | Error error -> failf "silent request: %s" (error_message error));
      (match Eio.Promise.await timeout_seen with
      | Some _ -> ()
      | None -> failf "silent responder did not receive the request");
      expect_ok "timeout unsubscribe"
        (Nats_eio.Subscription.unsubscribe timeout_subscription);
      print_endline "request_timeout: ok";
      let cancellation_subject = subject "cancel" in
      let cancellation_subscription =
        expect_ok "cancellation subscribe"
          (Nats_eio.Connection.subscribe responder (filter "cancel"))
      in
      expect_ok "cancellation subscribe flush"
        (Nats_eio.Connection.flush responder);
      let cancellation_seen, cancellation_seen_u = Eio.Promise.create () in
      Eio.Fiber.fork ~sw (fun () ->
          match next_with_timeout ~clock ~timeout cancellation_subscription with
          | Ok _ -> Eio.Promise.resolve cancellation_seen_u ()
          | Error error ->
              failf "cancellation responder: %s" (error_message error));
      let cancel, cancel_u = Eio.Promise.create () in
      let cancellation_result, cancellation_result_u = Eio.Promise.create () in
      Eio.Fiber.fork ~sw (fun () ->
          Eio.Cancel.sub (fun cancellation ->
              Eio.Promise.resolve cancel_u cancellation;
              try
                ignore
                  (Nats_eio.Connection.request ~timeout client
                     cancellation_subject "cancel");
                Eio.Promise.resolve cancellation_result_u `Completed
              with Eio.Cancel.Cancelled _ ->
                Eio.Promise.resolve cancellation_result_u `Cancelled));
      Eio.Promise.await cancellation_seen;
      Eio.Cancel.cancel (Eio.Promise.await cancel) (Failure "cancel request");
      (match Eio.Promise.await cancellation_result with
      | `Cancelled -> ()
      | `Completed -> failf "cancelled request unexpectedly completed");
      expect_ok "cancellation unsubscribe"
        (Nats_eio.Subscription.unsubscribe cancellation_subscription);
      print_endline "request_cancellation: ok";
      let reply_subject = subject "reply" in
      let reply_subscription =
        expect_ok "reply subscribe"
          (Nats_eio.Connection.subscribe responder (filter "reply"))
      in
      expect_ok "reply subscribe flush" (Nats_eio.Connection.flush responder);
      let reply_done, reply_done_u = Eio.Promise.create () in
      Eio.Fiber.fork ~sw (fun () ->
          match next_with_timeout ~clock ~timeout reply_subscription with
          | Error error -> failf "reply responder: %s" (error_message error)
          | Ok delivery -> (
              match Nats.Message.reply_to delivery.message with
              | None -> failf "reply request had no reply subject"
              | Some reply_to ->
                  expect_ok "reply publish"
                    (Nats_eio.Connection.publish responder reply_to "pong");
                  Eio.Promise.resolve reply_done_u ()));
      let response =
        expect_ok "post-cancellation request"
          (Nats_eio.Connection.request ~timeout client reply_subject "ping")
      in
      expect_message_payload "post-cancellation response" "pong" response;
      Eio.Promise.await reply_done;
      expect_ok "reply unsubscribe"
        (Nats_eio.Subscription.unsubscribe reply_subscription);
      print_endline "request_cleanup: ok";
      let auto_subscription =
        expect_ok "auto-unsubscribe subscribe"
          (Nats_eio.Connection.subscribe client (filter "auto"))
      in
      expect_ok "auto-unsubscribe flush" (Nats_eio.Connection.flush client);
      expect_ok "auto-unsubscribe"
        (Nats_eio.Subscription.auto_unsubscribe auto_subscription
           ~max_messages:2);
      expect_ok "auto-unsubscribe barrier" (Nats_eio.Connection.flush client);
      let auto_subject = subject "auto" in
      List.iter
        (fun payload ->
          expect_ok "auto-unsubscribe publish"
            (Nats_eio.Connection.publish responder auto_subject payload))
        [ "one"; "two"; "three" ];
      expect_ok "auto-unsubscribe publish flush"
        (Nats_eio.Connection.flush responder);
      expect_payload "auto-unsubscribe first" "one"
        (expect_ok "auto-unsubscribe first delivery"
           (Nats_eio.Subscription.next_with_timeout ~timeout auto_subscription));
      expect_payload "auto-unsubscribe second" "two"
        (expect_ok "auto-unsubscribe second delivery"
           (Nats_eio.Subscription.next_with_timeout ~timeout auto_subscription));
      expect_closed "auto-unsubscribe terminal"
        (Nats_eio.Subscription.next_with_timeout
           ~timeout:Mtime.Span.(500 * ms)
           auto_subscription);
      print_endline "auto_unsubscribe: ok";
      let slow_events = Nats_eio.Connection.events slow_client in
      let slow_subscription =
        expect_ok "slow-consumer subscribe"
          (Nats_eio.Connection.subscribe slow_client (filter "slow"))
      in
      let slow_ready_subscription =
        expect_ok "slow-consumer readiness subscribe"
          (Nats_eio.Connection.subscribe slow_client (filter "slow-ready"))
      in
      expect_ok "slow-consumer subscribe flush"
        (Nats_eio.Connection.flush slow_client);
      let slow_subject = subject "slow" in
      List.iter
        (fun payload ->
          expect_ok "slow-consumer publish"
            (Nats_eio.Connection.publish responder slow_subject payload))
        [ "first"; "second" ];
      expect_ok "slow-consumer readiness publish"
        (Nats_eio.Connection.publish responder (subject "slow-ready") "ready");
      expect_ok "slow-consumer publish flush"
        (Nats_eio.Connection.flush responder);
      expect_payload "slow-consumer readiness" "ready"
        (expect_ok "slow-consumer readiness delivery"
           (Nats_eio.Subscription.next_with_timeout ~timeout
              slow_ready_subscription));
      expect_ok "slow-consumer readiness unsubscribe"
        (Nats_eio.Subscription.unsubscribe slow_ready_subscription);
      expect_payload "slow-consumer first" "first"
        (expect_ok "slow-consumer first delivery"
           (Nats_eio.Subscription.next_with_timeout ~timeout slow_subscription));
      let slow_sid = Nats_eio.Subscription.sid slow_subscription in
      (match
         Nats_eio.Subscription.next_with_timeout ~timeout slow_subscription
       with
      | Error
          (Nats_eio.Error.Slow_consumer (Nats_eio.Error.Subscription { sid }))
        when Int.equal sid slow_sid ->
          ()
      | Ok delivery ->
          failf "slow-consumer delivered %S after its queue filled"
            (Nats.Message.payload delivery.message)
      | Error error ->
          failf "slow-consumer subscription: %s" (error_message error));
      expect_slow_consumer_event ~clock ~timeout ~sid:slow_sid slow_events;
      expect_ok "slow-consumer close" (Nats_eio.Connection.close slow_client);
      print_endline "slow_consumer: ok";
      let drain_subscription =
        expect_ok "subscription drain subscribe"
          (Nats_eio.Connection.subscribe client (filter "drain"))
      in
      let drain_ready_subscription =
        expect_ok "subscription drain readiness subscribe"
          (Nats_eio.Connection.subscribe client (filter "drain-ready"))
      in
      expect_ok "subscription drain flush" (Nats_eio.Connection.flush client);
      let drain_subject = subject "drain" in
      List.iter
        (fun payload ->
          expect_ok "subscription drain publish"
            (Nats_eio.Connection.publish responder drain_subject payload))
        [ "first"; "second" ];
      expect_ok "subscription drain readiness publish"
        (Nats_eio.Connection.publish responder (subject "drain-ready") "ready");
      expect_ok "subscription drain publish flush"
        (Nats_eio.Connection.flush responder);
      expect_payload "subscription drain readiness" "ready"
        (expect_ok "subscription drain readiness delivery"
           (Nats_eio.Subscription.next_with_timeout ~timeout
              drain_ready_subscription));
      expect_ok "subscription drain readiness unsubscribe"
        (Nats_eio.Subscription.unsubscribe drain_ready_subscription);
      expect_ok "subscription drain"
        (Nats_eio.Subscription.drain drain_subscription);
      expect_payload "subscription drain first" "first"
        (expect_ok "subscription drain first delivery"
           (Nats_eio.Subscription.next drain_subscription));
      expect_payload "subscription drain second" "second"
        (expect_ok "subscription drain second delivery"
           (Nats_eio.Subscription.next drain_subscription));
      expect_closed "subscription drain terminal"
        (Nats_eio.Subscription.next drain_subscription);
      print_endline "subscription_drain: ok";
      expect_ok "connection drain" (Nats_eio.Connection.drain client);
      (match Nats_eio.Connection.publish client (subject "closed") "closed" with
      | Error Nats_eio.Error.Closed -> ()
      | Error Nats_eio.Error.Draining -> ()
      | Ok () -> failf "publish succeeded after connection drain"
      | Error error ->
          failf "publish after connection drain: %s" (error_message error));
      print_endline "connection_drain: ok";
      expect_parent_switch_cleanup ~net ~clock ~endpoint ?config ();
      print_endline "parent_switch_cleanup: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server lifecycle failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server lifecycle failed: " ^ Printexc.to_string error);
      exit 1
