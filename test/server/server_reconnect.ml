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
  let clock = Eio.Stdenv.clock env in
  let mono_clock = Eio.Stdenv.mono_clock env in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 20)
         ~reconnect_delay:Mtime.Span.(50 * ms)
         ~reconnect_max_delay:Mtime.Span.(100 * ms)
         ())
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock:mono_clock ~config endpoints)
  in
  let responder =
    expect_ok "responder connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock:mono_clock ~config endpoints)
  in
  Fun.protect
    ~finally:(fun () ->
      ignore (Nats_eio.Connection.close responder);
      ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let events = Nats_eio.Connection.events connection in
      let responder_events = Nats_eio.Connection.events responder in
      expect_initial_connection ~clock:mono_clock events;
      expect_initial_connection ~clock:mono_clock responder_events;
      Eio.Switch.run @@ fun service_sw ->
      let service_name = "ocaml-reconnect-service" in
      let service_endpoint_name = "echo" in
      let service_config =
        expect_service_ok "service config"
          (Nats_eio.Service.Config.v ~name:service_name ~version:"1.0.0" ())
      in
      let service =
        expect_service_ok "service start"
          (Nats_eio.Service.v ~sw:service_sw ~clock
             ~random:(Random.State.make [| 7; 8; 9 |])
             connection service_config)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Service.stop service))
        (fun () ->
          let service_endpoint =
            expect_service_ok "service endpoint"
              (Nats_eio.Service.Endpoint.v ~name:service_endpoint_name
                 (fun request ->
                   Nats_eio.Service.Request.respond request
                     ("service:" ^ Nats_eio.Service.Request.payload request)))
          in
          expect_service_ok "service endpoint registration"
            (Nats_eio.Service.add_endpoint service service_endpoint);
          expect_ok "service subscription flush"
            (Nats_eio.Connection.flush connection);
          let service_subject = Nats.Subject.literal service_endpoint_name in
          let expect_service_request label payload expected =
            let response =
              expect_ok label
                (Nats_eio.Connection.request ~timeout responder service_subject
                   payload)
            in
            let actual = Nats.Message.payload response in
            if not (String.equal actual expected) then
              failf "%s returned %S, expected %S" label actual expected
          in
          let discover_service label =
            let target =
              Nats_eio.Service.Discovery.Instance
                { service = service_name; id = Nats_eio.Service.id service }
            in
            let values =
              expect_service_ok label
                (Nats_eio.Service.Discovery.info ~timeout ~target responder)
            in
            match values with
            | [ value ] ->
                if
                  not
                    (String.equal
                       (Nats_eio.Service.Info.name value)
                       service_name)
                then failf "%s returned the wrong service name" label;
                if
                  not
                    (String.equal
                       (Nats_eio.Service.Info.id value)
                       (Nats_eio.Service.id service))
                then failf "%s returned the wrong service id" label;
                if
                  not
                    (String.equal (Nats_eio.Service.Info.version value) "1.0.0")
                then failf "%s returned the wrong service version" label;
                let endpoints = Nats_eio.Service.Info.endpoints value in
                (match endpoints with
                | [ endpoint ] -> (
                    if
                      not
                        (String.equal
                           (Nats_eio.Service.Info.endpoint_name endpoint)
                           service_endpoint_name)
                    then failf "%s returned the wrong endpoint name" label;
                    if
                      not
                        (String.equal
                           (Nats.Subject.Filter.to_string
                              (Nats_eio.Service.Info.endpoint_subject endpoint))
                           service_endpoint_name)
                    then failf "%s returned the wrong endpoint subject" label;
                    match Nats_eio.Service.Info.endpoint_queue endpoint with
                    | Some queue
                      when String.equal (Nats.Queue_group.to_string queue) "q"
                      ->
                        ()
                    | Some _ ->
                        failf "%s returned the wrong endpoint queue" label
                    | None -> failf "%s omitted the endpoint queue" label)
                | _ -> failf "%s returned an unexpected endpoint count" label);
                Nats_eio.Service.Info.id value
            | _ -> failf "%s returned an unexpected service count" label
          in
          expect_service_request "baseline service request" "before"
            "service:before";
          let initial_service_id = discover_service "baseline service info" in
          let subject = Nats.Subject.literal "ocaml.integration.reconnect" in
          let filter =
            Nats.Subject.Filter.literal "ocaml.integration.reconnect"
          in
          let subscription =
            expect_ok "subscribe"
              (Nats_eio.Connection.subscribe connection filter)
          in
          expect_ok "subscribe flush" (Nats_eio.Connection.flush connection);
          let initial_recovery = Nats_eio.Subscription.recovery subscription in
          (match initial_recovery with
          | Nats_eio.Subscription.Attached 0 -> ()
          | Nats_eio.Subscription.Attached generation ->
              failf "initial subscription generation was %d" generation
          | Nats_eio.Subscription.Detached generation ->
              failf "initial subscription was detached at generation %d"
                generation);
          expect_ok "baseline publish"
            (Nats_eio.Connection.publish connection subject "before");
          expect_ok "baseline flush" (Nats_eio.Connection.flush connection);
          let baseline =
            expect_ok "baseline delivery"
              (Nats_eio.Subscription.next_with_timeout ~timeout subscription)
          in
          if not (String.equal (Nats.Message.payload baseline.message) "before")
          then
            failf "baseline payload was %S"
              (Nats.Message.payload baseline.message);
          let request_subject =
            Nats.Subject.literal "ocaml.integration.reconnect.request"
          in
          let request_filter =
            Nats.Subject.Filter.literal "ocaml.integration.reconnect.request"
          in
          let request_subscription =
            expect_ok "pending request subscribe"
              (Nats_eio.Connection.subscribe responder request_filter)
          in
          expect_ok "pending request subscribe flush"
            (Nats_eio.Connection.flush responder);
          let request_result, request_result_u = Eio.Promise.create () in
          Eio.Fiber.fork ~sw (fun () ->
              Eio.Promise.resolve request_result_u
                (Nats_eio.Connection.request ~timeout connection request_subject
                   "pending"));
          let pending_request =
            expect_ok "pending request delivery"
              (Nats_eio.Subscription.next_with_timeout ~timeout
                 request_subscription)
          in
          (match Nats.Message.reply_to pending_request.message with
          | Some _ -> ()
          | None -> failf "pending request had no reply subject");
          touch signal;
          expect_disconnected ~clock:mono_clock events;
          expect_disconnected ~clock:mono_clock responder_events;
          (match Eio.Promise.await request_result with
          | Error Nats_eio.Error.Disconnected -> ()
          | Ok _ -> failf "pending request unexpectedly completed"
          | Error error ->
              failf "pending request returned %s instead of disconnected"
                (error_message error));
          expect_reconnected ~clock:mono_clock events;
          expect_reconnected ~clock:mono_clock responder_events;
          expect_recovered ~clock:mono_clock subscription initial_recovery;
          expect_ok "service reconnect flush"
            (Nats_eio.Connection.flush connection);
          expect_service_request "post-reconnect service request" "after"
            "service:after";
          let recovered_service_id =
            discover_service "post-reconnect service info"
          in
          if not (String.equal recovered_service_id initial_service_id) then
            failf "service id changed across reconnect";
          expect_ok "post-reconnect publish"
            (Nats_eio.Connection.publish connection subject "after");
          expect_ok "post-reconnect flush"
            (Nats_eio.Connection.flush connection);
          let recovered =
            expect_ok "post-reconnect delivery"
              (Nats_eio.Subscription.next_with_timeout ~timeout subscription)
          in
          if not (String.equal (Nats.Message.payload recovered.message) "after")
          then
            failf "post-reconnect payload was %S"
              (Nats.Message.payload recovered.message);
          expect_service_ok "service stop" (Nats_eio.Service.stop service);
          print_endline "reconnect: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server reconnect failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server reconnect failed: " ^ Printexc.to_string error);
      exit 1
