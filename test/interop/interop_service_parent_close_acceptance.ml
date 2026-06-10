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

let parent_close_file () =
  match Sys.getenv_opt "NATS_TEST_INTEROP_PARENT_CLOSE_FILE" with
  | Some value when not (String.equal value "") -> value
  | _ -> failf "NATS_TEST_INTEROP_PARENT_CLOSE_FILE is required"

let write_closed_marker path =
  let output = open_out path in
  output_string output "closed\n";
  close_out output

let next_message ~timeout label subscription =
  match Nats_eio.Subscription.next_with_timeout ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

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
  let parent_close_file = parent_close_file () in
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
      let service_config =
        expect_service_ok "Service parent-close config"
          (Nats_eio.Service.Config.v ~name:"ocaml-parent-close-service"
             ~version:"1.2.3" ~description:"OCaml Service parent-close interop"
             ~metadata:[ ("language", "ocaml"); ("suite", "interop") ]
             ())
      in
      let service =
        expect_service_ok "Service parent-close start"
          (Nats_eio.Service.v ~sw:service_sw ~clock:(Eio.Stdenv.clock env)
             connection service_config)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Service.stop service))
        (fun () ->
          let endpoint_handler request =
            Nats_eio.Service.Request.respond request
              (Nats_eio.Service.Request.payload request ^ "-response")
          in
          let endpoint =
            expect_service_ok "Service parent-close endpoint"
              (Nats_eio.Service.Endpoint.v ~name:"echo"
                 ~subject:(filter prefix "ocaml.echo")
                 endpoint_handler)
          in
          expect_service_ok "add Service parent-close endpoint"
            (Nats_eio.Service.add_endpoint service endpoint);
          let close_subscription =
            expect_ok "subscribe parent-close control"
              (Nats_eio.Connection.subscribe connection (filter prefix "close"))
          in
          expect_ok "flush Service parent-close setup"
            (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Go Service parent-close peer"
              (Nats_eio.Connection.request ~timeout connection
                 (subject prefix "start") "start")
          in
          expect_payload "Service parent-close start response" "started"
            start_response;
          let close_message =
            next_message ~timeout "parent-close control" close_subscription
          in
          expect_payload "parent-close control" "close" close_message;
          expect_ok "reply parent-close control"
            (Nats_eio.Connection.publish connection
               (reply_subject close_message)
               "closing");
          expect_ok "flush parent-close control"
            (Nats_eio.Connection.flush connection);
          expect_ok "close parent connection"
            (Nats_eio.Connection.close connection);
          write_closed_marker parent_close_file;
          if Nats_eio.Service.stopped service then
            failf "Service became stopped before explicit service stop";
          expect_service_ok "stop Service after parent close"
            (Nats_eio.Service.stop service);
          if not (Nats_eio.Service.stopped service) then
            failf "Service remained open after explicit stop";
          print_endline "interop-service-parent-close: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline
        ("interop Service parent-close acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("interop Service parent-close acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
