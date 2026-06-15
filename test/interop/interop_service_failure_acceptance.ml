let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let service_error_message error =
  Format.asprintf "%a" Nats_eio.Service.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_service_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (service_error_message error)

let endpoint () =
  match Sys.getenv_opt "NATS_TEST_SERVER" with
  | None -> failf "NATS_TEST_SERVER is required"
  | Some value -> (
      match Nats.Endpoint.of_string value with
      | Ok endpoint -> endpoint
      | Error error ->
          failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error
            error)

let prefix () =
  match Sys.getenv_opt "NATS_TEST_INTEROP_PREFIX" with
  | Some value -> value
  | None -> failf "NATS_TEST_INTEROP_PREFIX is required"

let subject prefix suffix = Nats.Subject.literal (prefix ^ "." ^ suffix)
let filter prefix suffix = Nats.Subject.Filter.literal (prefix ^ "." ^ suffix)

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let reply_subject message =
  match Nats.Message.reply_to message with
  | Some subject -> subject
  | None -> failf "request message had no reply subject"

let next_message ~timeout label subscription =
  match Nats_eio.Subscription.next_with_timeout ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let await_promise ~clock ~timeout label promise =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Eio.Promise.await promise)
    (fun () ->
      Eio.Time.Mono.sleep clock seconds;
      failf "timed out waiting for %s" label)

let expect_initial_connection events =
  let connected = ref false in
  while not !connected do
    match Nats_eio.Event_stream.next events with
    | Ok (Nats_eio.Event.Core Nats.Event.Connected) -> connected := true
    | Ok (Nats_eio.Event.Core _) -> ()
    | Ok event ->
        failf "unexpected initial lifecycle event: %a" Nats_eio.Event.pp event
    | Error error -> failf "initial connection: %s" (error_message error)
  done

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoint = endpoint () in
  let prefix = prefix () in
  let auth = Interop_auth.auth () in
  let tls = Interop_auth.tls_config () in
  let config =
    match (auth, tls) with
    | None, None -> None
    | _ ->
        Some
          (expect_ok "connection config"
             (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 0)
                ?auth ?tls ()))
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ?config [ endpoint ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let timeout = Mtime.Span.(10 * s) in
      let events = Nats_eio.Connection.events connection in
      expect_initial_connection events;
      Eio.Switch.run @@ fun service_sw ->
      let first_started, first_started_u = Eio.Promise.create () in
      let release_first, release_first_u = Eio.Promise.create () in
      let failure, failure_u = Eio.Promise.create () in
      let calls = ref 0 in
      let done_calls = ref 0 in
      let service_config =
        expect_service_ok "Service failure config"
          (Nats_eio.Service.Config.v ~name:"ocaml-failure-service"
             ~version:"1.2.3"
             ~error_handler:(fun error ->
               if Option.is_none (Eio.Promise.peek failure) then
                 Eio.Promise.resolve failure_u error)
             ~done_handler:(fun () -> incr done_calls)
             ())
      in
      let service =
        expect_service_ok "Service failure start"
          (Nats_eio.Service.v ~sw:service_sw ~clock:(Eio.Stdenv.clock env)
             connection service_config)
      in
      Fun.protect
        ~finally:(fun () ->
          if Option.is_none (Eio.Promise.peek release_first) then
            Eio.Promise.resolve release_first_u ();
          ignore (Nats_eio.Service.stop service))
        (fun () ->
          let pending_limits =
            expect_service_ok "Service failure pending limits"
              (Nats_eio.Service.Endpoint.Pending_limits.v ~messages:1
                 ~bytes:(-1))
          in
          let endpoint_handler request =
            incr calls;
            if Int.equal !calls 1 then (
              Eio.Promise.resolve first_started_u ();
              Eio.Promise.await release_first;
              Ok ())
            else Ok ()
          in
          let endpoint =
            expect_service_ok "Service failure endpoint"
              (Nats_eio.Service.Endpoint.v ~name:"limited"
                 ~subject:(filter prefix "ocaml.limited")
                 ~pending_limits endpoint_handler)
          in
          expect_service_ok "add Service failure endpoint"
            (Nats_eio.Service.add_endpoint service endpoint);
          let release_subscription =
            expect_ok "subscribe Service failure release"
              (Nats_eio.Connection.subscribe connection
                 (filter prefix "release"))
          in
          let parent_subscription =
            expect_ok "subscribe parent-connection check"
              (Nats_eio.Connection.subscribe connection
                 (filter prefix "parent-check"))
          in
          let done_subscription =
            expect_ok "subscribe Service failure completion"
              (Nats_eio.Connection.subscribe connection (filter prefix "done"))
          in
          expect_ok "flush Service failure setup"
            (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Go Service failure peer"
              (Nats_eio.Connection.request ~timeout connection
                 (subject prefix "start") "start")
          in
          expect_payload "Service failure start response" "started"
            start_response;
          await_promise ~clock ~timeout "first Service failure handler"
            first_started;
          let first_started_response =
            expect_ok "acknowledge first Service failure handler"
              (Nats_eio.Connection.request ~timeout connection
                 (subject prefix "first-started")
                 "started")
          in
          expect_payload "first-handler barrier response" "first-started"
            first_started_response;
          let release_message =
            next_message ~timeout "Service failure release" release_subscription
          in
          expect_payload "Service failure release" "release" release_message;
          Eio.Promise.resolve release_first_u ();
          expect_ok "reply Service failure release"
            (Nats_eio.Connection.publish connection
               (reply_subject release_message)
               "released");
          expect_ok "flush Service failure release"
            (Nats_eio.Connection.flush connection);
          let failure =
            await_promise ~clock ~timeout "Service subscription failure" failure
          in
          (match failure with
          | Nats_eio.Service.Error.Connection
              (Nats_eio.Error.Slow_consumer (Nats_eio.Error.Subscription _)) ->
              ()
          | error ->
              failf "unexpected Service subscription failure: %s"
                (service_error_message error));
          if Nats_eio.Service.stopped service then
            failf "Service became stopped instead of failed";
          (match Nats_eio.Service.add_group service ~name:"after-failure" with
          | Error
              (Nats_eio.Service.Error.Connection
                 (Nats_eio.Error.Slow_consumer (Nats_eio.Error.Subscription _)))
            ->
              ()
          | Error error ->
              failf "Service accepted an unexpected post-failure error: %s"
                (service_error_message error)
          | Ok _ -> failf "Service accepted a group after subscription failure");
          let parent_message =
            next_message ~timeout "parent-connection check" parent_subscription
          in
          expect_payload "parent-connection check" "usable" parent_message;
          expect_ok "reply parent-connection check"
            (Nats_eio.Connection.publish connection
               (reply_subject parent_message)
               "parent-usable");
          expect_ok "flush parent-connection check"
            (Nats_eio.Connection.flush connection);
          let done_message =
            next_message ~timeout "Service failure completion" done_subscription
          in
          expect_payload "Service failure completion" "go-finished" done_message;
          expect_ok "reply Service failure completion"
            (Nats_eio.Connection.publish connection
               (reply_subject done_message)
               "ocaml-validated");
          expect_ok "flush Service failure completion"
            (Nats_eio.Connection.flush connection);
          if not (Int.equal !done_calls 0) then
            failf "Service completion callback ran after failure";
          expect_ok "close parent connection"
            (Nats_eio.Connection.close connection);
          print_endline "interop-service-failure: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("interop Service failure acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("interop Service failure acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
