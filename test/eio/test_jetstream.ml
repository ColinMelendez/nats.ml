open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let endpoint =
  match Nats.Endpoint.of_string "nats://127.0.0.1:4222" with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)

let address = `Tcp (Eio.Net.Ipaddr.V4.loopback, 4222)

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let expect_jetstream_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error)

let expect_jetstream_error result predicate =
  match result with
  | Ok _ -> fail "expected a JetStream error"
  | Error error -> equal bool true (predicate error)

let operation_wire operation =
  match Nats.Codec.encode operation with
  | Ok wire -> wire
  | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error)

let status_wire ~code ~description =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ""
  in
  operation_wire
    (Nats.Op.Hmsg { sid = 1; message; status = Some { code; description } })

let delivery_wire payload =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "orders.created")
      ~reply_to:(Nats.Subject.literal "$JS.ACK.ORDERS.worker.1.1.1.0.0")
      payload
  in
  operation_wire (Nats.Op.Hmsg { sid = 1; message; status = None })

let with_connection ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "jetstream-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "jetstream-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw connection

let consumer connection =
  let jetstream = expect_jetstream_ok (Nats_eio.Jetstream.v connection) in
  let stream =
    expect_jetstream_ok
      (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
  in
  expect_jetstream_ok (Nats_eio.Jetstream.Consumer.bind stream ~name:"worker")

let rec yield_n count =
  if count <= 0 then ()
  else (
    Eio.Fiber.yield ();
    yield_n (count - 1))

let () =
  run "nats-eio-jetstream"
    [
      test "pull fails permanently when its consumer is deleted" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:409 ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Pull.next pull) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "fetch shares consumer deletion classification and cleans up"
        (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.fetch (consumer connection)
                       ~batch:1));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:409 ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "queued heartbeat control is processed before deletion" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let config = Mtime.Span.(1 * ms) in
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(10 * ms)
                     ~idle_heartbeat:config (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (status_wire ~code:100 ~description:"Idle Heartbeat"
                   ^ status_wire ~code:409 ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "non-heartbeat status is not treated as idle heartbeat" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(10 * ms)
                     ~idle_heartbeat:Mtime.Span.(1 * ms)
                     (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:100 ~description:"Flow Control Request"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Unexpected_status
                    { code; description } ->
                    Int.equal code 100
                    && String.equal description "Flow Control Request"
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull retries an expired batch" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await response;
                `Return (delivery_wire "payload");
                `Await hold;
              ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:408 ~description:"Request Timeout"));
              (match Eio.Promise.await result with
              | Ok message ->
                  equal string "payload"
                    (Nats_eio.Jetstream.Msg.payload message)
              | Error error ->
                  fail
                    (Format.asprintf "expired pull did not retry: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull reports max-bytes conflicts" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (status_wire ~code:409
                      ~description:"message size exceeds maxbytes"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Conflict { code; description } ->
                    Int.equal code 409
                    && String.equal description "message size exceeds maxbytes"
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull timeout leaves its outstanding request available" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(1 * ms)
                   pull
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "pull timeout unexpectedly returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected pull timeout: %a"
                       Nats_eio.Jetstream.Error.pp error));
              Eio.Promise.resolve response_u (Ok (delivery_wire "resumed"));
              (match Nats_eio.Jetstream.Consumer.Pull.next pull with
              | Ok message ->
                  equal string "resumed"
                    (Nats_eio.Jetstream.Msg.payload message)
              | Error error ->
                  fail
                    (Format.asprintf "timed-out pull did not resume: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "idle heartbeat silence fails a pull distinctly" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(10 * ms)
                     ~idle_heartbeat:Mtime.Span.(1 * ms)
                     (consumer connection))
              in
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(10 * ms)
                   pull
               with
              | Error Nats_eio.Jetstream.Error.Missing_heartbeat -> ()
              | Ok _ -> fail "silent heartbeat pull returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected heartbeat failure: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve response_u (Error End_of_file);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
