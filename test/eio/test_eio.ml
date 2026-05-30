open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let discovered_info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[\"discovered.example:4223\"]}"
  ^ "\r\n"

let tls_required_info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"tls_required\":true,\"connect_urls\":[]}" ^ "\r\n"

let auth_info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"nonce\":\"nonce\",\"connect_urls\":[]}" ^ "\r\n"

let auth_required_info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"auth_required\":true,\"connect_urls\":[]}" ^ "\r\n"

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let expect_core_event = function
  | Ok (Nats_eio.Event.Core event) -> event
  | Ok event ->
      fail
        (Format.asprintf "expected a core event, got %a" Nats_eio.Event.pp event)
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let address = `Tcp (Eio.Net.Ipaddr.V4.loopback, 4222)

let endpoint_of_string value =
  match Nats.Endpoint.of_string value with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)

let endpoint = endpoint_of_string "nats://127.0.0.1:4222"

let configure_net net =
  Eio_mock.Net.on_getaddrinfo net (List.init 64 (fun _ -> `Return [ address ]))

let make_net label =
  let net = Eio_mock.Net.make label in
  configure_net net;
  net

let with_connection ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "nats-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = make_net "nats-network" in
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw connection

let with_reconnecting_connection ?config ~first_reads ~second_reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let first = Eio_mock.Flow.make "nats-server-first" in
  Eio_mock.Flow.on_read first first_reads;
  let second = Eio_mock.Flow.make "nats-server-second" in
  Eio_mock.Flow.on_read second second_reads;
  let net = make_net "nats-reconnect-network" in
  Eio_mock.Net.on_connect net [ `Return first; `Return second ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw connection

let with_reconnecting_connection_traced ?config ~first_reads ~second_reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let first = Eio_mock.Flow.make "nats-server-first" in
  Eio_mock.Flow.on_read first first_reads;
  let second = Eio_mock.Flow.make "nats-server-second" in
  Eio_mock.Flow.on_read second second_reads;
  let net = make_net "nats-reconnect-network" in
  Eio_mock.Net.on_connect net [ `Return first; `Return second ];
  let trace = Buffer.create 4096 in
  let debug = Eio.Stdenv.debug env in
  let tracer =
    {
      Eio.Debug.traceln =
        (fun ?__POS__:_ fmt ->
          Format.kasprintf
            (fun message ->
              Buffer.add_string trace message;
              Buffer.add_char trace '\n')
            fmt);
    }
  in
  Eio.Fiber.with_binding debug#traceln tracer @@ fun () ->
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw ~trace connection

let contains_substring ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let found = ref false in
  while (not !found) && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      found := true;
    incr index
  done;
  !found

let filter =
  match Nats.Subject.Filter.of_string "orders.*" with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Subject.pp_error error)

let subject = Nats.Subject.literal "orders.created"

let tls_config () =
  let authenticator =
    match X509.Authenticator.of_string "none" with
    | Error (`Msg message) -> fail message
    | Ok make -> make (fun () -> None)
  in
  match Tls.Config.client ~authenticator () with
  | Error (`Msg message) -> fail message
  | Ok config -> config

let rec yield_n count =
  if count <= 0 then ()
  else (
    Eio.Fiber.yield ();
    yield_n (count - 1))

let () =
  run "nats-eio"
    [
      test "handshake, delivery, publish, and flush" (fun () ->
          let deliver, deliver_u = Eio.Promise.create () in
          let pong, pong_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let message = "MSG orders.created 1 5\r\nhello\r\n" in
          with_connection
            ~reads:
              [ `Return info_wire; `Await deliver; `Await pong; `Await hold ]
            (fun ~sw connection ->
              let events = Nats_eio.Connection.events connection in
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected INFO, got %a" Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected CONNECTED, got %a" Nats.Event.pp
                       event));
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve deliver_u (Ok message);
              let delivery =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "hello" (Nats.Message.payload delivery.message);
              equal string "orders.created"
                (Nats.Subject.to_string (Nats.Message.subject delivery.message));
              expect_ok
                (Nats_eio.Connection.publish connection subject "outgoing");
              let flush_result, flush_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve flush_result_u
                    (Nats_eio.Connection.flush connection));
              yield_n 4;
              Eio.Promise.resolve pong_u (Ok "PONG\r\n");
              expect_ok (Eio.Promise.await flush_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "reads queued subscription deliveries without blocking" (fun () ->
          let deliver, deliver_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let message = "MSG orders.created 1 5\r\nhello\r\n" in
          with_connection
            ~reads:[ `Return info_wire; `Await deliver; `Await hold ]
            (fun ~sw:_ connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              (match Nats_eio.Subscription.next_nonblocking subscription with
              | None -> ()
              | Some (Ok _) ->
                  fail "empty subscription returned a delivery"
              | Some (Error error) ->
                  fail
                    (Format.asprintf "empty subscription returned %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve deliver_u (Ok message);
              yield_n 5;
              (match Nats_eio.Subscription.next_nonblocking subscription with
              | Some (Ok delivery) ->
                  equal string "hello" (Nats.Message.payload delivery.message)
              | None -> fail "queued subscription delivery was not available"
              | Some (Error error) ->
                  fail
                    (Format.asprintf "queued subscription returned %a"
                       Nats_eio.Error.pp error));
              (match Nats_eio.Subscription.next_nonblocking subscription with
              | None -> ()
              | Some (Ok _) ->
                  fail "subscription returned an unexpected second delivery"
              | Some (Error error) ->
                  fail
                    (Format.asprintf "drained subscription returned %a"
                       Nats_eio.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "derives nonce credentials before the Eio handshake completes"
        (fun () ->
          let signed, signed_u = Eio.Promise.create () in
          let auth =
            Nats.Auth.nkey ~nkey:"PUB"
              ~sign:(fun ~nonce ->
                Eio.Promise.resolve signed_u nonce;
                Ok "signature")
          in
          let config =
            expect_ok (Nats_eio.Connection.Config.v ~auth ())
          in
          let hold, hold_u = Eio.Promise.create () in
          with_connection ~config
            ~reads:[ `Return auth_info_wire; `Await hold ]
            (fun ~sw:_ connection ->
              equal string "nonce" (Eio.Promise.await signed);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "reports required authentication before sending CONNECT" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let flow = Eio_mock.Flow.make "auth-required-nats-server" in
          Eio_mock.Flow.on_read flow [ `Return auth_required_info_wire ];
          let net = make_net "auth-required-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          Eio.Switch.run @@ fun sw ->
          match
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
              [ endpoint ]
          with
          | Error (Nats_eio.Error.Auth Nats.Auth.Auth_required) -> ()
          | Ok connection ->
              expect_ok (Nats_eio.Connection.close connection);
              fail "anonymous auth unexpectedly connected"
          | Error error ->
              fail
                (Format.asprintf "unexpected auth error: %a"
                   Nats_eio.Error.pp error));
      test "keeps the owner available after fragmented INFO" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          let split = String.length info_wire / 2 in
          with_connection
            ~reads:
              [
                `Return (String.sub info_wire 0 split);
                `Return
                  (String.sub info_wire split (String.length info_wire - split));
                `Await hold;
              ]
            (fun ~sw:_ connection ->
              expect_ok (Nats_eio.Connection.publish connection subject "ok");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "keeps commands running while a message is fragmented" (fun () ->
          let partial, partial_u = Eio.Promise.create () in
          let rest, rest_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [ `Return info_wire; `Await partial; `Await rest; `Await hold ]
            (fun ~sw:_ connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve partial_u (Ok "MSG orders.created 1 5\r\nhe");
              yield_n 3;
              expect_ok (Nats_eio.Connection.publish connection subject "ok");
              Eio.Promise.resolve rest_u (Ok "llo\r\n");
              let delivery =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "hello" (Nats.Message.payload delivery.message);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "reports server errors without closing the connection" (fun () ->
          let server_error, server_error_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await server_error;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw:_ connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              Eio.Promise.resolve server_error_u
                (Ok "-ERR 'permissions violation'\r\n");
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Server_error { message = "permissions violation" } ->
                  ()
              | event ->
                  fail
                    (Format.asprintf "expected server error event, got %a"
                       Nats.Event.pp event));
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve delivery_u
                (Ok "MSG orders.created 1 5\r\nhello\r\n");
              let received =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "hello" (Nats.Message.payload received.message);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "request handles replies, no responders, and timeout" (fun () ->
          let response_one, response_one_u = Eio.Promise.create () in
          let response_two, response_two_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~inbox_prefix:"_INBOX.test" ())
          in
          with_connection ~config
            ~reads:
              [
                `Return info_wire;
                `Await response_one;
                `Await response_two;
                `Await hold;
              ]
            (fun ~sw connection ->
              let result_one, result_one_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_one_u
                    (Nats_eio.Connection.request connection subject "lookup"));
              yield_n 5;
              Eio.Promise.resolve response_one_u
                (Ok "MSG _INBOX.test.0.0 1 5\r\nreply\r\n");
              (match Eio.Promise.await result_one with
              | Ok message ->
                  equal string "reply" (Nats.Message.payload message)
              | Error error ->
                  fail
                    (Format.asprintf "expected request reply, got %a"
                       Nats_eio.Error.pp error));
              let result_two, result_two_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_two_u
                    (Nats_eio.Connection.request connection subject "missing"));
              yield_n 5;
              Eio.Promise.resolve response_two_u
                (Ok
                   "HMSG _INBOX.test.0.1 2 30 30\r\n\
                    NATS/1.0 503 No Responders\r\n\
                    \r\n\
                    \r\n");
              (match Eio.Promise.await result_two with
              | Error Nats_eio.Error.No_responders -> ()
              | Ok _ -> fail "expected no responders"
              | Error error ->
                  fail
                    (Format.asprintf "expected no responders, got %a"
                       Nats_eio.Error.pp error));
              (match
                 Nats_eio.Connection.request
                   ~timeout:Mtime.Span.(1 * ms)
                   connection subject "slow"
               with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok _ -> fail "expected request timeout"
              | Error error ->
                  fail
                    (Format.asprintf "expected request timeout, got %a"
                       Nats_eio.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "cancelling a request leaves cleanup non-blocking" (fun () ->
          let cancellation, cancellation_u = Eio.Promise.create () in
          let result, result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw connection ->
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Cancel.sub (fun cancel ->
                      Eio.Promise.resolve cancellation_u cancel;
                      try
                        ignore
                          (Nats_eio.Connection.request connection subject
                             "cancel-me");
                        Eio.Promise.resolve result_u `Completed
                      with Eio.Cancel.Cancelled _ ->
                        Eio.Promise.resolve result_u `Cancelled));
              let cancel = Eio.Promise.await cancellation in
              yield_n 5;
              Eio.Cancel.cancel cancel (Failure "cancel request");
              (match Eio.Promise.await result with
              | `Cancelled -> ()
              | `Completed -> fail "request unexpectedly completed");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "drain fails pending requests before waiting for its barrier"
        (fun () ->
          let pong, pong_u = Eio.Promise.create () in
          let request_result, request_result_u = Eio.Promise.create () in
          let drain_result, drain_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await pong; `Await hold ]
            (fun ~sw connection ->
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve request_result_u
                    (Nats_eio.Connection.request connection subject "drain-me"));
              yield_n 5;
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve drain_result_u
                    (Nats_eio.Connection.drain connection));
              yield_n 5;
              (match Eio.Promise.await request_result with
              | Error Nats_eio.Error.Draining -> ()
              | Ok _ -> fail "request unexpectedly completed during drain"
              | Error error ->
                  fail
                    (Format.asprintf "expected draining request error, got %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve pong_u (Ok "PONG\r\n");
              expect_ok (Eio.Promise.await drain_result);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "auto-unsubscribe bounds deliveries and iter stops cleanly"
        (fun () ->
          let messages_one, messages_one_u = Eio.Promise.create () in
          let messages_two, messages_two_u = Eio.Promise.create () in
          let iter_payload, iter_payload_u = Eio.Promise.create () in
          let iter_result, iter_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await messages_one;
                `Await messages_two;
                `Await hold;
              ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              expect_ok
                (Nats_eio.Subscription.auto_unsubscribe subscription
                   ~max_messages:2);
              Eio.Promise.resolve messages_one_u
                (Ok
                   ("MSG orders.created 1 1\r\na\r\n"
                  ^ "MSG orders.created 1 1\r\nb\r\n"
                  ^ "MSG orders.created 1 1\r\nc\r\n"));
              let first = expect_ok (Nats_eio.Subscription.next subscription) in
              let second =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "a" (Nats.Message.payload first.message);
              equal string "b" (Nats.Message.payload second.message);
              (match Nats_eio.Subscription.next subscription with
              | Error Nats_eio.Error.Closed -> ()
              | Ok _ ->
                  fail "expected the auto-unsubscribed subscription to end"
              | Error error ->
                  fail
                    (Format.asprintf "expected subscription close, got %a"
                       Nats_eio.Error.pp error));
              let iter_subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              expect_ok
                (Nats_eio.Subscription.auto_unsubscribe iter_subscription
                   ~max_messages:1);
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve iter_result_u
                    (Nats_eio.Subscription.iter iter_subscription
                       ~f:(fun delivery ->
                         Eio.Promise.resolve iter_payload_u
                           (Nats.Message.payload delivery.message))));
              Eio.Promise.resolve messages_two_u
                (Ok "MSG orders.created 2 1\r\nz\r\n");
              equal string "z" (Eio.Promise.await iter_payload);
              expect_ok (Eio.Promise.await iter_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "subscription drain preserves queued messages until the barrier"
        (fun () ->
          let messages, messages_u = Eio.Promise.create () in
          let pong, pong_u = Eio.Promise.create () in
          let first, first_u = Eio.Promise.create () in
          let second, second_u = Eio.Promise.create () in
          let iter_result, iter_result_u = Eio.Promise.create () in
          let drain_result, drain_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [ `Return info_wire; `Await messages; `Await pong; `Await hold ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve messages_u
                (Ok
                   ("MSG orders.created 1 1\r\na\r\n"
                  ^ "MSG orders.created 1 1\r\nb\r\n"));
              yield_n 5;
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve drain_result_u
                    (Nats_eio.Subscription.drain subscription));
              yield_n 5;
              let count = ref 0 in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve iter_result_u
                    (Nats_eio.Subscription.iter subscription ~f:(fun delivery ->
                         (match !count with
                         | 0 ->
                             Eio.Promise.resolve first_u
                               (Nats.Message.payload delivery.message)
                         | 1 ->
                             Eio.Promise.resolve second_u
                               (Nats.Message.payload delivery.message)
                         | _ -> ());
                         count := !count + 1)));
              equal string "a" (Eio.Promise.await first);
              equal string "b" (Eio.Promise.await second);
              Eio.Promise.resolve pong_u (Ok "PONG\r\n");
              expect_ok (Eio.Promise.await drain_result);
              expect_ok (Eio.Promise.await iter_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "subscription drain timeout leaves its terminal item queued"
        (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw:_ connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              (match
                 Nats_eio.Subscription.drain
                   ~timeout:Mtime.Span.(1 * ms)
                   subscription
               with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "expected subscription drain timeout"
              | Error error ->
                  fail
                    (Format.asprintf "expected drain timeout, got %a"
                       Nats_eio.Error.pp error));
              (match Nats_eio.Subscription.next subscription with
              | Error Nats_eio.Error.Closed -> ()
              | Ok _ -> fail "expected the terminal item after drain timeout"
              | Error error ->
                  fail
                    (Format.asprintf "expected closed subscription, got %a"
                       Nats_eio.Error.pp error));
              (match Nats_eio.Subscription.drain subscription with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "a timed-out drain was reported as successful"
              | Error error ->
                  fail
                    (Format.asprintf "expected the prior drain timeout, got %a"
                       Nats_eio.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "concurrent subscription drains share one barrier" (fun () ->
          let pong, pong_u = Eio.Promise.create () in
          let first_result, first_result_u = Eio.Promise.create () in
          let second_result, second_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~drain_timeout:Mtime.Span.(1 * min)
                 ())
          in
          with_connection ~config
            ~reads:[ `Return info_wire; `Await pong; `Await hold ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Subscription.drain subscription));
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Subscription.drain subscription));
              yield_n 6;
              match Eio.Promise.peek second_result with
              | Some _ -> fail "the second drain completed before the barrier"
              | None ->
                  Eio.Promise.resolve pong_u (Ok "PONG\r\n");
                  expect_ok (Eio.Promise.await first_result);
                  expect_ok (Eio.Promise.await second_result);
                  (match Nats_eio.Subscription.next subscription with
                  | Error Nats_eio.Error.Closed -> ()
                  | Ok _ -> fail "expected the queued terminal marker"
                  | Error error ->
                      fail
                        (Format.asprintf
                           "expected a closed subscription, got %a"
                           Nats_eio.Error.pp error));
                  expect_ok (Nats_eio.Connection.close connection);
                  Eio.Promise.resolve hold_u (Error End_of_file)));
      test "cancelling a subscription drain leaves the owner non-blocking"
        (fun () ->
          let cancellation, cancellation_u = Eio.Promise.create () in
          let result, result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~drain_timeout:Mtime.Span.(1 * min)
                 ())
          in
          with_connection ~config
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Cancel.sub (fun cancel ->
                      Eio.Promise.resolve cancellation_u cancel;
                      try
                        ignore (Nats_eio.Subscription.drain subscription);
                        Eio.Promise.resolve result_u `Completed
                      with Eio.Cancel.Cancelled _ ->
                        Eio.Promise.resolve result_u `Cancelled));
              let cancel = Eio.Promise.await cancellation in
              yield_n 5;
              Eio.Cancel.cancel cancel (Failure "cancel subscription drain");
              (match Eio.Promise.await result with
              | `Cancelled -> ()
              | `Completed -> fail "subscription drain unexpectedly completed");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "subscription drain timeout preserves barrier ordering" (fun () ->
          let first_pong, first_pong_u = Eio.Promise.create () in
          let second_pong, second_pong_u = Eio.Promise.create () in
          let drain_result, drain_result_u = Eio.Promise.create () in
          let flush_result, flush_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~flush_timeout:Mtime.Span.(1 * s)
                 ())
          in
          with_connection ~config
            ~reads:
              [
                `Return info_wire;
                `Await first_pong;
                `Await second_pong;
                `Await hold;
              ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve drain_result_u
                    (Nats_eio.Subscription.drain
                       ~timeout:Mtime.Span.(1 * ms)
                       subscription));
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve flush_result_u
                    (Nats_eio.Connection.flush connection));
              yield_n 8;
              (match Eio.Promise.await drain_result with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "expected the subscription drain to time out"
              | Error error ->
                  fail
                    (Format.asprintf "expected drain timeout, got %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve first_pong_u (Ok "PONG\r\n");
              yield_n 3;
              (match Eio.Promise.peek flush_result with
              | None -> ()
              | Some _ -> fail "the later flush consumed the drain PONG");
              Eio.Promise.resolve second_pong_u (Ok "PONG\r\n");
              expect_ok (Eio.Promise.await flush_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "a delivery on another subscription does not abort a drain"
        (fun () ->
          let message, message_u = Eio.Promise.create () in
          let pong, pong_u = Eio.Promise.create () in
          let drain_result, drain_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~drain_timeout:Mtime.Span.(1 * min)
                 ())
          in
          with_connection ~config
            ~reads:
              [ `Return info_wire; `Await message; `Await pong; `Await hold ]
            (fun ~sw connection ->
              let draining =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              let other =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve drain_result_u
                    (Nats_eio.Subscription.drain draining));
              yield_n 5;
              Eio.Promise.resolve message_u
                (Ok "MSG orders.created 2 1\r\nb\r\n");
              yield_n 3;
              Eio.Promise.resolve pong_u (Ok "PONG\r\n");
              expect_ok (Eio.Promise.await drain_result);
              let delivery = expect_ok (Nats_eio.Subscription.next other) in
              equal string "b" (Nats.Message.payload delivery.message);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "a later flush still times out behind a retired drain barrier"
        (fun () ->
          let first_pong, first_pong_u = Eio.Promise.create () in
          let drain_result, drain_result_u = Eio.Promise.create () in
          let flush_result, flush_result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~drain_timeout:Mtime.Span.(1 * ms)
                 ~flush_timeout:Mtime.Span.(2 * ms)
                 ())
          in
          with_connection ~config
            ~reads:[ `Return info_wire; `Await first_pong; `Await hold ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve drain_result_u
                    (Nats_eio.Subscription.drain subscription));
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve flush_result_u
                    (Nats_eio.Connection.flush connection));
              (match Eio.Promise.await drain_result with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "expected the subscription drain to time out"
              | Error error ->
                  fail
                    (Format.asprintf "expected drain timeout, got %a"
                       Nats_eio.Error.pp error));
              (match Eio.Promise.await flush_result with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "expected the later flush to time out"
              | Error error ->
                  fail
                    (Format.asprintf "expected flush timeout, got %a"
                       Nats_eio.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve first_pong_u (Error End_of_file);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "a timed-out flush keeps its PONG slot behind a drain" (fun () ->
          let first_pong, first_pong_u = Eio.Promise.create () in
          let second_pong, second_pong_u = Eio.Promise.create () in
          let third_pong, third_pong_u = Eio.Promise.create () in
          let drain_result, drain_result_u = Eio.Promise.create () in
          let first_flush, first_flush_u = Eio.Promise.create () in
          let second_flush, second_flush_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~drain_timeout:Mtime.Span.(1 * ms)
                 ~flush_timeout:Mtime.Span.(1 * min)
                 ())
          in
          with_connection ~config
            ~reads:
              [
                `Return info_wire;
                `Await first_pong;
                `Await second_pong;
                `Await third_pong;
                `Await hold;
              ]
            (fun ~sw connection ->
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve drain_result_u
                    (Nats_eio.Subscription.drain
                       ~timeout:Mtime.Span.(1 * ms)
                       subscription));
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_flush_u
                    (Nats_eio.Connection.flush
                       ~timeout:Mtime.Span.(2 * ms)
                       connection));
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_flush_u
                    (Nats_eio.Connection.flush connection));
              (match Eio.Promise.await drain_result with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "expected the subscription drain to time out"
              | Error error ->
                  fail
                    (Format.asprintf "expected drain timeout, got %a"
                       Nats_eio.Error.pp error));
              (match Eio.Promise.await first_flush with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok () -> fail "expected the first flush to time out"
              | Error error ->
                  fail
                    (Format.asprintf "expected first flush timeout, got %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve first_pong_u (Ok "PONG\r\n");
              yield_n 3;
              Eio.Promise.resolve second_pong_u (Ok "PONG\r\n");
              yield_n 3;
              (match Eio.Promise.peek second_flush with
              | None -> ()
              | Some _ -> fail "the second flush consumed the first flush PONG");
              Eio.Promise.resolve third_pong_u (Ok "PONG\r\n");
              expect_ok (Eio.Promise.await second_flush);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "a full subscription reports a slow consumer without blocking"
        (fun () ->
          let messages, messages_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let two_messages =
            "MSG orders.created 1 1\r\na\r\nMSG orders.created 1 1\r\n"
            ^ "b\r\nMSG orders.created 1 1\r\nc\r\n"
          in
          let config =
            expect_ok (Nats_eio.Connection.Config.v ~subscription_capacity:1 ())
          in
          with_connection ~config
            ~reads:[ `Return info_wire; `Await messages; `Await hold ]
            (fun ~sw connection ->
              Eio.Switch.check sw;
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve messages_u (Ok two_messages);
              let first = expect_ok (Nats_eio.Subscription.next subscription) in
              equal string "a" (Nats.Message.payload first.message);
              let second =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "b" (Nats.Message.payload second.message);
              match Nats_eio.Subscription.next subscription with
              | Error
                  (Nats_eio.Error.Slow_consumer
                     (Nats_eio.Error.Subscription { sid = 1 })) ->
                  expect_ok (Nats_eio.Connection.close connection);
                  Eio.Promise.resolve hold_u (Error End_of_file)
              | Ok delivery ->
                  fail
                    (Format.asprintf "expected the slow-consumer error, got %S"
                       (Nats.Message.payload delivery.message))
              | Error error ->
                  fail
                    (Format.asprintf "expected slow consumer, got %a"
                       Nats_eio.Error.pp error)));
      test "reports EOF after processing complete input" (fun () ->
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 0) ())
          in
          with_connection ~config
            ~reads:[ `Return info_wire ]
            (fun ~sw:_ connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Closed -> ()
              | event ->
                  fail
                    (Format.asprintf "expected closed event, got %a"
                       Nats.Event.pp event));
              match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Disconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected disconnect, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected disconnect event, got %a"
                       Nats_eio.Error.pp error)));
      test "tries configured endpoints during initial connect" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "second-seed" in
          Eio_mock.Flow.on_read flow [ `Return info_wire; `Await hold ];
          let net = Eio_mock.Net.make "seed-network" in
          Eio_mock.Net.on_getaddrinfo net
            [ `Raise (Failure "first seed DNS"); `Return [ address; address ] ];
          Eio_mock.Net.on_connect net [ `Raise End_of_file; `Return flow ];
          let first = endpoint_of_string "nats://first-seed.example" in
          let second = endpoint_of_string "nats://second-seed.example" in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 [ first; second ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve hold_u (Error End_of_file));
      test "fails over after an initial handshake timeout" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let silent_hold, silent_hold_u = Eio.Promise.create () in
          let good_hold, good_hold_u = Eio.Promise.create () in
          let silent = Eio_mock.Flow.make "initial-silent" in
          Eio_mock.Flow.on_read silent [ `Await silent_hold ];
          let good = Eio_mock.Flow.make "initial-good" in
          Eio_mock.Flow.on_read good [ `Return info_wire; `Await good_hold ];
          let net = Eio_mock.Net.make "initial-handshake-network" in
          Eio_mock.Net.on_getaddrinfo net
            [ `Return [ address ]; `Return [ address ] ];
          Eio_mock.Net.on_connect net [ `Return silent; `Return good ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~handshake_timeout:Mtime.Span.(1 * ms)
                 ())
          in
          let first = endpoint_of_string "nats://initial-silent" in
          let second = endpoint_of_string "nats://initial-good" in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config [ first; second ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve silent_hold_u (Error End_of_file);
          Eio.Promise.resolve good_hold_u (Error End_of_file));
      test "does not let a TLS configuration error mask transport failure"
        (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let net = Eio_mock.Net.make "mixed-seed-network" in
          Eio_mock.Net.on_getaddrinfo net [ `Raise (Failure "seed transport") ];
          let nats_endpoint = endpoint_of_string "nats://transport.example" in
          let tls_endpoint = endpoint_of_string "tls://unsupported.example" in
          Eio.Switch.run @@ fun sw ->
          match
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
              [ nats_endpoint; tls_endpoint ]
          with
          | Error (Nats_eio.Error.Io _) -> ()
          | Error error ->
              fail
                (Format.asprintf "transport error was masked by %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "mixed seeds unexpectedly connected");
      test "requires TLS configuration for an explicit TLS seed" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let net = Eio_mock.Net.make "explicit-tls-config-network" in
          let tls_endpoint = endpoint_of_string "tls://tls.example" in
          Eio.Switch.run @@ fun sw ->
          match
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
              [ tls_endpoint ]
          with
          | Error Nats_eio.Error.Tls_required -> ()
          | Error error ->
              fail
                (Format.asprintf "expected TLS configuration error, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "explicit TLS seed connected without TLS config");
      test "uses bare INFO connect URLs for the next dial pass" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "discovery-first" in
          Eio_mock.Flow.on_read first
            [ `Return discovered_info_wire; `Await disconnect ];
          let second = Eio_mock.Flow.make "discovery-second" in
          Eio_mock.Flow.on_read second [ `Return info_wire; `Await hold ];
          let net = Eio_mock.Net.make "discovery-network" in
          Eio_mock.Net.on_getaddrinfo net
            [
              `Return [ address ];
              `Raise (Failure "discovered candidate");
              `Return [ address ];
            ];
          Eio_mock.Net.on_connect net [ `Return first; `Return second ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 1) ())
          in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config [ endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected discovery disconnect, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected discovery lifecycle, got %a"
                   Nats_eio.Error.pp error));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Reconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected discovered reconnection, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected discovered success, got %a"
                   Nats_eio.Error.pp error));
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve hold_u (Error End_of_file));
      test "tries every endpoint before consuming a reconnect attempt"
        (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "pass-first" in
          Eio_mock.Flow.on_read first [ `Return info_wire; `Await disconnect ];
          let second = Eio_mock.Flow.make "pass-second" in
          Eio_mock.Flow.on_read second [ `Return info_wire; `Await hold ];
          let net = Eio_mock.Net.make "pass-network" in
          Eio_mock.Net.on_getaddrinfo net
            [ `Return [ address ]; `Return [ address ]; `Return [ address ] ];
          Eio_mock.Net.on_connect net
            [ `Return first; `Raise End_of_file; `Return second ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 1)
                 ~reconnect_delay:Mtime.Span.(1 * ns)
                 ~reconnect_max_delay:Mtime.Span.(1 * ns)
                 ())
          in
          let first_endpoint = endpoint_of_string "nats://pass-first.example" in
          let second_endpoint =
            endpoint_of_string "nats://pass-second.example"
          in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config
                 [ first_endpoint; second_endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected pass disconnect, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected pass lifecycle, got %a"
                   Nats_eio.Error.pp error));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Reconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected pass reconnection, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected pass success, got %a"
                   Nats_eio.Error.pp error));
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve hold_u (Error End_of_file));
      test "rotates an endpoint whose handshake times out" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let silent_hold, silent_hold_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "handshake-first" in
          Eio_mock.Flow.on_read first [ `Return info_wire; `Await disconnect ];
          let silent = Eio_mock.Flow.make "handshake-silent" in
          Eio_mock.Flow.on_read silent [ `Await silent_hold ];
          let recovered = Eio_mock.Flow.make "handshake-recovered" in
          Eio_mock.Flow.on_read recovered [ `Return info_wire; `Await hold ];
          let net = Eio_mock.Net.make "handshake-rotation-network" in
          Eio_mock.Net.on_getaddrinfo net
            [ `Return [ address ]; `Return [ address ]; `Return [ address ] ];
          Eio_mock.Net.on_connect net
            [ `Return first; `Return silent; `Return recovered ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 2)
                 ~handshake_timeout:Mtime.Span.(1 * ms)
                 ~reconnect_delay:Mtime.Span.(1 * ns)
                 ~reconnect_max_delay:Mtime.Span.(1 * ns)
                 ())
          in
          Eio.Switch.run @@ fun sw ->
          let first_endpoint = endpoint_of_string "nats://handshake-first" in
          let second_endpoint = endpoint_of_string "nats://handshake-second" in
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config
                 [ first_endpoint; second_endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected handshake disconnect, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected handshake lifecycle, got %a"
                   Nats_eio.Error.pp error));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Reconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected rotated reconnection, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected rotated success, got %a"
                   Nats_eio.Error.pp error));
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve silent_hold_u (Error End_of_file);
          Eio.Promise.resolve hold_u (Error End_of_file));
      test "reconnects a live subscription without losing queued delivery"
        (fun () ->
          let queued, queued_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let later, later_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok (Nats_eio.Connection.Config.v ~event_capacity:3 ())
          in
          with_reconnecting_connection ~config
            ~first_reads:[ `Return info_wire; `Await queued; `Await disconnect ]
            ~second_reads:
              [ `Await reconnect_info; `Await later; `Await hold ]
            (fun ~sw connection ->
              let events = Nats_eio.Connection.events connection in
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              let initial_recovery =
                Nats_eio.Subscription.recovery subscription
              in
              (match initial_recovery with
              | Nats_eio.Subscription.Attached 0 -> ()
              | Nats_eio.Subscription.Attached generation ->
                  fail
                    (Format.asprintf
                       "new subscription has recovery generation %d" generation)
              | Nats_eio.Subscription.Detached generation ->
                  fail
                    (Format.asprintf
                       "new subscription is detached at generation %d"
                       generation));
              let detached_result, detached_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve detached_result_u
                    (Nats_eio.Subscription.await_recovery
                       ~from:initial_recovery subscription));
              Eio.Promise.resolve queued_u
                (Ok "MSG orders.created 1 6\r\nbefore\r\n");
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              let detached = Eio.Promise.await detached_result in
              (match detached with
              | Ok (Nats_eio.Subscription.Detached 0) -> ()
              | Ok recovery ->
                  fail
                    (Format.asprintf
                       "expected detached generation 0, got %s"
                       (match recovery with
                       | Nats_eio.Subscription.Detached generation ->
                           Format.asprintf "detached %d" generation
                       | Nats_eio.Subscription.Attached generation ->
                           Format.asprintf "attached %d" generation))
              | Error error ->
                  fail
                    (Format.asprintf "recovery detached with %a"
                       Nats_eio.Error.pp error));
              let attached_result, attached_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve attached_result_u
                    (Nats_eio.Subscription.await_recovery
                       ~from:(match detached with Ok value -> value | Error _ ->
                         initial_recovery)
                       subscription));
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected initial INFO, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected initial CONNECTED, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Closed -> ()
              | event ->
                  fail
                    (Format.asprintf "expected disconnect CLOSED, got %a"
                       Nats.Event.pp event));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Disconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected disconnect event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnect lifecycle, got %a"
                       Nats_eio.Error.pp error));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected reconnect INFO, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected reconnect CONNECTED, got %a"
                       Nats.Event.pp event));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Reconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected reconnected event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnected lifecycle, got %a"
                       Nats_eio.Error.pp error));
              (match Eio.Promise.await attached_result with
              | Ok (Nats_eio.Subscription.Attached 1) -> ()
              | Ok recovery ->
                  fail
                    (Format.asprintf
                       "expected attached generation 1, got %s"
                       (match recovery with
                       | Nats_eio.Subscription.Detached generation ->
                           Format.asprintf "detached %d" generation
                       | Nats_eio.Subscription.Attached generation ->
                           Format.asprintf "attached %d" generation))
              | Error error ->
                  fail
                    (Format.asprintf "recovery attached with %a"
                       Nats_eio.Error.pp error));
              let queued_delivery =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "before"
                (Nats.Message.payload queued_delivery.message);
              Eio.Promise.resolve later_u
                (Ok "MSG orders.created 1 5\r\nafter\r\n");
              let later_delivery =
                expect_ok (Nats_eio.Subscription.next subscription)
              in
              equal string "after" (Nats.Message.payload later_delivery.message);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "does not replay subscriptions that opt out of reconnect" (fun () ->
          let queued, queued_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let later, later_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:[ `Return info_wire; `Await queued; `Await disconnect ]
            ~second_reads:[ `Return info_wire; `Await later; `Await hold ]
            (fun ~sw ~trace connection ->
              let events = Nats_eio.Connection.events connection in
              let ordinary =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              let ephemeral =
                expect_ok
                  (Nats_eio.Connection.subscribe ~replay_on_reconnect:false
                     connection filter)
              in
              Eio.Promise.resolve queued_u
                (Ok "MSG orders.created 2 6\r\nbefore\r\n");
              yield_n 5;
              let queued_delivery =
                expect_ok (Nats_eio.Subscription.next ephemeral)
              in
              equal string "before"
                (Nats.Message.payload queued_delivery.message);
              let ephemeral_result, ephemeral_result_u =
                Eio.Promise.create ()
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ephemeral_result_u
                    (Nats_eio.Subscription.next ephemeral));
              yield_n 5;
              let trace_before_disconnect = Buffer.length trace in
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              (match Eio.Promise.await ephemeral_result with
              | Error Nats_eio.Error.Disconnected -> ()
              | Ok _ -> fail "non-replaying subscription received a delivery"
              | Error error ->
                  fail
                    (Format.asprintf
                       "expected non-replaying subscription disconnect, got %a"
                       Nats_eio.Error.pp error));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected initial INFO, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected initial CONNECTED, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Closed -> ()
              | event ->
                  fail
                    (Format.asprintf "expected disconnect CLOSED, got %a"
                       Nats.Event.pp event));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Disconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected disconnect event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnect lifecycle, got %a"
                       Nats_eio.Error.pp error));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected reconnect INFO, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected reconnect CONNECTED, got %a"
                       Nats.Event.pp event));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Reconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf
                       "expected opt-out reconnected event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected opt-out reconnect, got %a"
                       Nats_eio.Error.pp error));
              let reconnect_trace =
                let trace = Buffer.contents trace in
                String.sub trace trace_before_disconnect
                  (String.length trace - trace_before_disconnect)
              in
              if
                contains_substring
                  ~needle:"wrote \"SUB orders.* 2\\r\\n\""
                  reconnect_trace
              then fail "non-replaying subscription was restored on the wire";
              if
                not
                  (contains_substring
                     ~needle:"wrote \"SUB orders.* 1\\r\\n\""
                     reconnect_trace)
              then fail "ordinary subscription was not restored on the wire";
              Eio.Promise.resolve later_u
                (Ok "MSG orders.created 1 5\r\nafter\r\n");
              let ordinary_delivery =
                expect_ok (Nats_eio.Subscription.next ordinary)
              in
              equal string "after"
                (Nats.Message.payload ordinary_delivery.message);
              (match Nats_eio.Subscription.next ephemeral with
              | Error Nats_eio.Error.Disconnected -> ()
              | Ok _ -> fail "closed non-replaying subscription was replayed"
              | Error error ->
                  fail
                    (Format.asprintf
                       "expected terminal non-replaying subscription, got %a"
                       Nats_eio.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "retries a failed redial before restoring subscriptions" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let deliver, deliver_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "retry-first" in
          Eio_mock.Flow.on_read first [ `Return info_wire; `Await disconnect ];
          let third = Eio_mock.Flow.make "retry-third" in
          Eio_mock.Flow.on_read third
            [ `Return info_wire; `Await deliver; `Await hold ];
          let net = make_net "retry-network" in
          Eio_mock.Net.on_connect net
            [ `Return first; `Raise End_of_file; `Return third ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 3)
                 ~reconnect_delay:Mtime.Span.(1 * ns)
                 ~reconnect_max_delay:Mtime.Span.(1 * ns)
                 ~reconnect_jitter:Mtime.Span.(1 * ns)
                 ~random:(Random.State.make [| 7 |])
                 ())
          in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config [ endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          let subscription =
            expect_ok (Nats_eio.Connection.subscribe connection filter)
          in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          (match expect_core_event (Nats_eio.Event_stream.next events) with
          | Nats.Event.Closed -> ()
          | event ->
              fail
                (Format.asprintf "expected retry CLOSED, got %a" Nats.Event.pp
                   event));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected one retry disconnect, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected retry lifecycle, got %a"
                   Nats_eio.Error.pp error));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Reconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected reconnected event, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected retry success, got %a"
                   Nats_eio.Error.pp error));
          Eio.Promise.resolve deliver_u
            (Ok "MSG orders.created 1 5\r\nafter\r\n");
          let delivery = expect_ok (Nats_eio.Subscription.next subscription) in
          equal string "after" (Nats.Message.payload delivery.message);
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve hold_u (Error End_of_file));
      test "defers unsubscribe until the reconnect handshake completes"
        (fun () ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection
            ~first_reads:[ `Return info_wire; `Await disconnect ]
            ~second_reads:[ `Await reconnect_info; `Await hold ]
            (fun ~sw connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              yield_n 5;
              let unsubscribe_result, unsubscribe_result_u =
                Eio.Promise.create ()
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve unsubscribe_result_u
                    (Nats_eio.Subscription.unsubscribe subscription));
              yield_n 5;
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              expect_ok (Eio.Promise.await unsubscribe_result);
              (match Nats_eio.Subscription.next subscription with
              | Error Nats_eio.Error.Closed -> ()
              | Ok _ -> fail "unsubscribed handle received a delivery"
              | Error error ->
                  fail
                    (Format.asprintf "expected closed subscription, got %a"
                       Nats_eio.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "rejects ordinary publishes during reconnect" (fun () ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:[ `Return info_wire; `Await disconnect ]
            ~second_reads:[ `Await reconnect_info; `Await hold ]
            (fun ~sw:_ ~trace connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              let trace_before_disconnect = Buffer.length trace in
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              yield_n 5;
              (match
                 Nats_eio.Connection.publish connection subject "during-reconnect"
               with
              | Error Nats_eio.Error.Disconnected -> ()
              | Ok () -> fail "ordinary publish unexpectedly survived reconnect"
              | Error error ->
                  fail
                    (Format.asprintf
                       "expected ordinary publish to fail as disconnected, got %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Disconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected reconnect disconnect, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnect lifecycle, got %a"
                       Nats_eio.Error.pp error));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Reconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected reconnected event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnection, got %a"
                       Nats_eio.Error.pp error));
              let reconnect_trace =
                let trace = Buffer.contents trace in
                String.sub trace trace_before_disconnect
                  (String.length trace - trace_before_disconnect)
              in
              if
                contains_substring ~needle:"wrote \"PUB orders.created"
                  reconnect_trace
              then fail "ordinary publish was written during reconnect";
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "rejects requests created during reconnect" (fun () ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:[ `Return info_wire; `Await disconnect ]
            ~second_reads:[ `Await reconnect_info; `Await hold ]
            (fun ~sw:_ ~trace connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              let trace_before_disconnect = Buffer.length trace in
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              yield_n 5;
              (match
                 Nats_eio.Connection.request connection subject "during-reconnect"
               with
              | Error Nats_eio.Error.Disconnected -> ()
              | Ok _ ->
                  fail "request created during reconnect unexpectedly survived"
              | Error error ->
                  fail
                    (Format.asprintf
                       "expected request to fail as disconnected, got %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Disconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected reconnect disconnect, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnect lifecycle, got %a"
                       Nats_eio.Error.pp error));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Reconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected reconnected event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnection, got %a"
                       Nats_eio.Error.pp error));
              let reconnect_trace =
                let trace = Buffer.contents trace in
                String.sub trace trace_before_disconnect
                  (String.length trace - trace_before_disconnect)
              in
              if
                contains_substring ~needle:"wrote \"SUB " reconnect_trace
              then
                fail "request subscription was written during reconnect";
              if
                contains_substring ~needle:"wrote \"PUB " reconnect_trace
              then
                fail "request publish was written during reconnect";
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "fails a pending flush during reconnect" (fun () ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection
            ~first_reads:[ `Return info_wire; `Await disconnect ]
            ~second_reads:[ `Return info_wire; `Await hold ]
            (fun ~sw connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              let flush_result, flush_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve flush_result_u
                    (Nats_eio.Connection.flush connection));
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              (match Eio.Promise.await flush_result with
              | Error Nats_eio.Error.Disconnected -> ()
              | Ok () -> fail "flush unexpectedly survived disconnect"
              | Error error ->
                  fail
                    (Format.asprintf "expected flush disconnect, got %a"
                       Nats_eio.Error.pp error));
              yield_n 10;
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "a failed redial terminates without duplicating disconnect"
        (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "failed-redial-first" in
          Eio_mock.Flow.on_read first [ `Return info_wire; `Await disconnect ];
          let net = make_net "failed-redial-network" in
          Eio_mock.Net.on_connect net [ `Return first; `Raise End_of_file ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 1) ())
          in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config [ endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          yield_n 10;
          (match expect_core_event (Nats_eio.Event_stream.next events) with
          | Nats.Event.Closed -> ()
          | event ->
              fail
                (Format.asprintf "expected failed-redial CLOSED, got %a"
                   Nats.Event.pp event));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected one disconnect event, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf
                   "expected disconnect before terminal error, got %a"
                   Nats_eio.Error.pp error));
          match Nats_eio.Event_stream.next events with
          | Error Nats_eio.Error.Disconnected -> ()
          | Ok Nats_eio.Event.Disconnected ->
              fail "redial emitted a duplicate disconnect event"
          | Ok event ->
              fail
                (Format.asprintf "expected terminal disconnect, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected terminal disconnect, got %a"
                   Nats_eio.Error.pp error));
      test "a non-Io redial exception terminates the event stream" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "exceptional-redial-first" in
          Eio_mock.Flow.on_read first [ `Return info_wire; `Await disconnect ];
          let net = make_net "exceptional-redial-network" in
          Eio_mock.Net.on_connect net
            [ `Return first; `Raise (Failure "redial failed") ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 1) ())
          in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config [ endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          yield_n 10;
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected disconnect event, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf
                   "expected disconnect before terminal error, got %a"
                   Nats_eio.Error.pp error));
          match Nats_eio.Event_stream.next events with
          | Error (Nats_eio.Error.Io (Failure message)) ->
              equal string "redial failed" message
          | Error error ->
              fail
                (Format.asprintf "expected redial I/O error, got %a"
                   Nats_eio.Error.pp error)
          | Ok event ->
              fail
                (Format.asprintf "expected terminal redial error, got %a"
                   Nats_eio.Event.pp event));
      test "validates reconnect policy" (fun () ->
          (match
             Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some (-1)) ()
           with
          | Error (Nats_eio.Error.Invalid_reconnect_attempts -1) -> ()
          | Error error ->
              fail
                (Format.asprintf "expected attempt validation, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "negative reconnect attempts were accepted");
          (match
             Nats_eio.Connection.Config.v ~max_reconnect_attempts:None ()
           with
          | Ok _ -> ()
          | Error error ->
              fail
                (Format.asprintf "unlimited reconnect attempts failed: %a"
                   Nats_eio.Error.pp error));
          (match
             Nats_eio.Connection.Config.v
               ~reconnect_delay:Mtime.Span.(2 * s)
               ~reconnect_max_delay:Mtime.Span.(1 * s)
               ()
           with
          | Error (Nats_eio.Error.Invalid_reconnect_delay _) -> ()
          | Error error ->
              fail
                (Format.asprintf "expected delay validation, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "a short maximum reconnect delay was accepted");
          match
            Nats_eio.Connection.Config.v ~reconnect_delay:Mtime.Span.zero ()
          with
          | Error (Nats_eio.Error.Invalid_timeout "reconnect delay") -> ()
          | Error error ->
              fail
                (Format.asprintf "expected timeout validation, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "a zero reconnect delay was accepted");
      test "rejects a TLS-required server without TLS configuration" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "tls-nats-server" in
          Eio_mock.Flow.on_read flow
            [ `Return tls_required_info_wire; `Await hold ];
          let net = make_net "tls-nats-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
              [ endpoint ]
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Tls_required -> ()
          | Error error ->
              fail
                (Format.asprintf "expected TLS-required error, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected TLS negotiation to be required");
      test "forced TLS requires a client configuration" (fun () ->
          match Nats_eio.Connection.Config.v ~tls_required:true () with
          | Error Nats_eio.Error.Tls_required -> ()
          | Error error ->
              fail
                (Format.asprintf "expected TLS configuration error, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected forced TLS configuration to be rejected");
      test "rejects buffered plaintext before TLS" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "buffered-tls-nats-server" in
          Eio_mock.Flow.on_read flow
            [ `Return (tls_required_info_wire ^ "PING\r\n"); `Await hold ];
          let net = make_net "buffered-tls-nats-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          let config =
            expect_ok (Nats_eio.Connection.Config.v ~tls:(tls_config ()) ())
          in
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ~config
              [ endpoint ]
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Tls_unexpected_input -> ()
          | Error error ->
              fail
                (Format.asprintf "expected buffered-input error, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected buffered plaintext to be rejected");
      test "times out the TLS handshake" (fun () ->
          Mirage_crypto_rng_unix.use_default ();
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "slow-tls-nats-server" in
          Eio_mock.Flow.on_read flow
            [ `Return tls_required_info_wire; `Await hold; `Await hold ];
          let net = make_net "slow-tls-nats-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~tls:(tls_config ())
                 ~handshake_timeout:Mtime.Span.(1 * ms)
                 ())
          in
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ~config
              [ endpoint ]
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Timeout -> ()
          | Error error ->
              fail
                (Format.asprintf "expected TLS timeout, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected TLS handshake to time out");
      test "times out an explicit TLS endpoint handshake" (fun () ->
          Mirage_crypto_rng_unix.use_default ();
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "explicit-slow-tls-server" in
          Eio_mock.Flow.on_read flow [ `Await hold ];
          let net = make_net "explicit-slow-tls-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~tls:(tls_config ())
                 ~handshake_timeout:Mtime.Span.(1 * ms)
                 ())
          in
          let tls_endpoint = endpoint_of_string "tls://explicit-slow-tls" in
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ~config
              [ tls_endpoint ]
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Timeout -> ()
          | Error error ->
              fail
                (Format.asprintf "expected explicit TLS timeout, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected explicit TLS endpoint to time out");
      test "times out a silent handshake" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "silent-nats-server" in
          Eio_mock.Flow.on_read flow [ `Await hold ];
          let net = make_net "silent-nats-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v
                 ~handshake_timeout:Mtime.Span.(1 * ms)
                 ())
          in
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ~config
              [ endpoint ]
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Timeout -> ()
          | Error error ->
              fail
                (Format.asprintf "expected handshake timeout, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected the silent handshake to time out");
      test "close bypasses a full reconnect command budget" (fun () ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~command_capacity:1
                 ~max_reconnect_attempts:(Some 1) ())
          in
          with_reconnecting_connection ~config
            ~first_reads:[ `Return info_wire; `Await disconnect ]
            ~second_reads:[ `Await reconnect_info; `Await hold ]
            (fun ~sw connection ->
              let events = Nats_eio.Connection.events connection in
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              ignore (expect_core_event (Nats_eio.Event_stream.next events));
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              yield_n 5;
              let unsubscribe_result, unsubscribe_result_u =
                Eio.Promise.create ()
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve unsubscribe_result_u
                    (Nats_eio.Subscription.unsubscribe subscription));
              yield_n 5;
              expect_ok (Nats_eio.Connection.close connection);
              (match Eio.Promise.await unsubscribe_result with
              | Error Nats_eio.Error.Closed -> ()
              | Ok () -> fail "unsubscribe unexpectedly succeeded after close"
              | Error error ->
                  fail
                    (Format.asprintf "expected closed unsubscribe, got %a"
                       Nats_eio.Error.pp error));
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "retries a timed-out reconnect handshake" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let silent_hold, silent_hold_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let first = Eio_mock.Flow.make "timeout-first" in
          Eio_mock.Flow.on_read first [ `Return info_wire; `Await disconnect ];
          let silent = Eio_mock.Flow.make "timeout-silent" in
          Eio_mock.Flow.on_read silent [ `Await silent_hold ];
          let third = Eio_mock.Flow.make "timeout-third" in
          Eio_mock.Flow.on_read third [ `Return info_wire; `Await hold ];
          let net = make_net "timeout-retry-network" in
          Eio_mock.Net.on_connect net
            [ `Return first; `Return silent; `Return third ];
          let config =
            expect_ok
              (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 3)
                 ~reconnect_delay:Mtime.Span.(1 * ns)
                 ~reconnect_max_delay:Mtime.Span.(1 * ns)
                 ~handshake_timeout:Mtime.Span.(1 * ms)
                 ())
          in
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 ~config [ endpoint ])
          in
          let events = Nats_eio.Connection.events connection in
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          Eio.Promise.resolve disconnect_u (Error End_of_file);
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Disconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected timeout disconnect, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected timeout retry, got %a"
                   Nats_eio.Error.pp error));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          ignore (expect_core_event (Nats_eio.Event_stream.next events));
          (match Nats_eio.Event_stream.next events with
          | Ok Nats_eio.Event.Reconnected -> ()
          | Ok event ->
              fail
                (Format.asprintf "expected timeout reconnection, got %a"
                   Nats_eio.Event.pp event)
          | Error error ->
              fail
                (Format.asprintf "expected timeout retry success, got %a"
                   Nats_eio.Error.pp error));
          expect_ok (Nats_eio.Connection.close connection);
          Eio.Promise.resolve silent_hold_u (Error End_of_file);
          Eio.Promise.resolve hold_u (Error End_of_file));
    ]
