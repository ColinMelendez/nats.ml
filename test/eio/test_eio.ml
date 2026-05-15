open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

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

let filter =
  match Nats.Subject.Filter.of_string "orders.*" with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Subject.pp_error error)

let subject = Nats.Subject.literal "orders.created"

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
