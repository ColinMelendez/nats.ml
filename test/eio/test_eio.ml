open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let tls_required_info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"tls_required\":true,\"connect_urls\":[]}" ^ "\r\n"

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

let with_connection ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "nats-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "nats-network" in
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         address)
  in
  f ~sw connection

let with_reconnecting_connection ?config ~first_reads ~second_reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let first = Eio_mock.Flow.make "nats-server-first" in
  Eio_mock.Flow.on_read first first_reads;
  let second = Eio_mock.Flow.make "nats-server-second" in
  Eio_mock.Flow.on_read second second_reads;
  let net = Eio_mock.Net.make "nats-reconnect-network" in
  Eio_mock.Net.on_connect net [ `Return first; `Return second ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         address)
  in
  f ~sw connection

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
          with_connection
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
      test "reconnects a live subscription without losing queued delivery"
        (fun () ->
          let queued, queued_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let later, later_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            expect_ok (Nats_eio.Connection.Config.v ~event_capacity:3 ())
          in
          with_reconnecting_connection ~config
            ~first_reads:[ `Return info_wire; `Await queued; `Await disconnect ]
            ~second_reads:[ `Return info_wire; `Await later; `Await hold ]
            (fun ~sw:_ connection ->
              let events = Nats_eio.Connection.events connection in
              let subscription =
                expect_ok (Nats_eio.Connection.subscribe connection filter)
              in
              Eio.Promise.resolve queued_u
                (Ok "MSG orders.created 1 6\r\nbefore\r\n");
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              yield_n 10;
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
          let net = Eio_mock.Net.make "failed-redial-network" in
          Eio_mock.Net.on_connect net [ `Return first; `Raise End_of_file ];
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 address)
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
          let net = Eio_mock.Net.make "exceptional-redial-network" in
          Eio_mock.Net.on_connect net
            [ `Return first; `Raise (Failure "redial failed") ];
          Eio.Switch.run @@ fun sw ->
          let connection =
            expect_ok
              (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock
                 address)
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
      test "rejects a TLS-required server without TLS configuration" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "tls-nats-server" in
          Eio_mock.Flow.on_read flow
            [ `Return tls_required_info_wire; `Await hold ];
          let net = Eio_mock.Net.make "tls-nats-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock address
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
          let net = Eio_mock.Net.make "buffered-tls-nats-network" in
          Eio_mock.Net.on_connect net [ `Return flow ];
          let config =
            expect_ok (Nats_eio.Connection.Config.v ~tls:(tls_config ()) ())
          in
          Eio.Switch.run @@ fun sw ->
          let result =
            Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ~config
              address
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
          let net = Eio_mock.Net.make "slow-tls-nats-network" in
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
              address
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Timeout -> ()
          | Error error ->
              fail
                (Format.asprintf "expected TLS timeout, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected TLS handshake to time out");
      test "times out a silent handshake" (fun () ->
          Eio_mock.Backend.run_full @@ fun env ->
          let hold, hold_u = Eio.Promise.create () in
          let flow = Eio_mock.Flow.make "silent-nats-server" in
          Eio_mock.Flow.on_read flow [ `Await hold ];
          let net = Eio_mock.Net.make "silent-nats-network" in
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
              address
          in
          Eio.Promise.resolve hold_u (Error End_of_file);
          match result with
          | Error Nats_eio.Error.Timeout -> ()
          | Error error ->
              fail
                (Format.asprintf "expected handshake timeout, got %a"
                   Nats_eio.Error.pp error)
          | Ok _ -> fail "expected the silent handshake to time out");
    ]
