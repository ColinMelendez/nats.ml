open Windtrap

let fail_with pp_error error = fail (Format.asprintf "%a" pp_error error)

let expect_config result =
  match result with
  | Ok value -> value
  | Error error -> fail_with Nats.Config.pp_error error

let expect_client result =
  match result with
  | Ok value -> value
  | Error error -> fail_with Nats.Error.pp error

let expect_error result predicate =
  match result with
  | Ok _ -> fail "expected a client error"
  | Error error -> equal bool true (predicate error)

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":100,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let client_at_info () =
  let client = Nats.Client.v Nats.Config.default in
  let reader = Bytesrw.Bytes.Reader.of_string info_wire in
  expect_client
    (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader)

let connected_client () =
  let info_transition = client_at_info () in
  expect_client
    (Nats.Client.outgoing info_transition.state
       (Nats.Client.Connect (Nats.Client.Connect.v ())))

let incoming state wire =
  let reader = Bytesrw.Bytes.Reader.of_string wire in
  expect_client
    (Nats.Client.incoming ~eod:true state ~now:Mtime.min_stamp reader)

let () =
  run "nats-client"
    [
      test "INFO is typed and CONNECT is an explicit second transition"
        (fun () ->
          let info = client_at_info () in
          equal bool true
            (match Nats.Client.phase info.state with
            | Nats.Client.Awaiting_connect -> true
            | _ -> false);
          (match info.events with
          | [ Nats.Event.Info value ] ->
              equal int 100 (Nats.Info.max_payload value);
              equal bool true (Nats.Info.headers value);
              equal bool true (Nats.Info.no_responders value)
          | _ -> fail "expected one INFO event");
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect (Nats.Client.Connect.v ())))
          in
          equal bool true
            (match Nats.Client.phase connected.state with
            | Nats.Client.Connected -> true
            | _ -> false);
          (match connected.events with
          | [ Nats.Event.Connected ] -> ()
          | _ -> fail "expected a connected event");
          match connected.output with
          | [ wire ] -> (
              match
                Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire)
              with
              | Ok (Nats.Op.Connect json) ->
                  equal bool true (String.length json > 0)
              | Ok _ -> fail "expected CONNECT"
              | Error error -> fail_with Nats.Codec.pp_error error)
          | _ -> fail "expected one CONNECT output");
      test "incomplete input is an idle transition and leaves the reader"
        (fun () ->
          let client = Nats.Client.v Nats.Config.default in
          let reader =
            Bytesrw.Bytes.Reader.of_string "INFO {\"max_payload\":100"
          in
          let transition =
            expect_client
              (Nats.Client.incoming client ~now:Mtime.min_stamp reader)
          in
          equal int 0 (Bytesrw.Bytes.Reader.pos reader);
          equal int 0 (List.length transition.output);
          match
            Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader
          with
          | Error (Nats.Error.Packet Nats.Packet.Unexpected_end) -> ()
          | Ok _ -> fail "expected an unexpected-end error"
          | Error error -> fail_with Nats.Error.pp error);
      test "subscriptions allocate ids and preserve HMSG status" (fun () ->
          let connected = connected_client () in
          let filter =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let subscribed =
            expect_client
              (Nats.Client.outgoing connected.state
                 (Nats.Client.Subscribe { subject = filter; queue_group = None }))
          in
          equal int 1
            (match subscribed.subscription_id with
            | Some sid -> sid
            | None -> fail "expected a subscription id");
          let message =
            Nats.Message.v ~subject:(Nats.Subject.literal "orders.created") "x"
          in
          let wire =
            match
              Nats.Codec.encode
                (Nats.Op.Hmsg
                   {
                     sid = 1;
                     message;
                     status = Some { code = 503; description = "No Responders" };
                   })
            with
            | Ok value -> value
            | Error error -> fail_with Nats.Codec.pp_error error
          in
          let auto =
            expect_client
              (Nats.Client.outgoing subscribed.state
                 (Nats.Client.Auto_unsubscribe { sid = 1; max_messages = 1 }))
          in
          let delivered = incoming auto.state wire in
          (match delivered.deliveries with
          | [
           {
             sid = 1;
             status = Some { code = 503; description = "No Responders" };
             _;
           };
          ] ->
              ()
          | _ -> fail "expected one status-bearing delivery");
          let ignored = incoming delivered.state wire in
          equal int 0 (List.length ignored.deliveries));
      test "publish uses negotiated max_payload" (fun () ->
          let connected = connected_client () in
          let message =
            Nats.Message.v
              ~subject:(Nats.Subject.literal "orders.created")
              (String.make 101 'x')
          in
          expect_error
            (Nats.Client.outgoing connected.state (Nats.Client.Publish message))
            (function
            | Nats.Error.Max_payload_exceeded { size = 101; limit = 100 } ->
                true
            | _ -> false));
      test "headers are rejected when the server did not negotiate them"
        (fun () ->
          let client = Nats.Client.v (expect_config (Nats.Config.v ())) in
          let reader =
            Bytesrw.Bytes.Reader.of_string
              "INFO {\"max_payload\":100,\"headers\":false}\r\n"
          in
          let received =
            expect_client
              (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader)
          in
          let connected =
            expect_client
              (Nats.Client.outgoing received.state
                 (Nats.Client.Connect (Nats.Client.Connect.v ())))
          in
          let headers =
            match Nats.Header.of_list [ ("Status", "200") ] with
            | Ok value -> value
            | Error error -> fail_with Nats.Header.pp_error error
          in
          let message =
            Nats.Message.v
              ~subject:(Nats.Subject.literal "orders.created")
              ~headers "payload"
          in
          expect_error
            (Nats.Client.outgoing connected.state (Nats.Client.Publish message))
            (function
            | Nats.Error.Headers_not_negotiated -> true
            | _ -> false));
      test "flush waits for PONG and inbound PING gets PONG" (fun () ->
          let connected = connected_client () in
          let flushed =
            expect_client
              (Nats.Client.outgoing connected.state Nats.Client.Flush)
          in
          equal string "PING\r\n"
            (match flushed.output with
            | [ output ] -> output
            | _ -> fail "expected PING");
          let completed = incoming flushed.state "PONG\r\n" in
          (match completed.events with
          | [ Nats.Event.Flush_completed ] -> ()
          | _ -> fail "expected flush completion");
          let pinged = incoming completed.state "PING\r\n" in
          equal string "PONG\r\n"
            (match pinged.output with
            | [ output ] -> output
            | _ -> fail "expected PONG"));
      test "auto-unsubscribe removes the intent after its last delivery"
        (fun () ->
          let connected = connected_client () in
          let filter =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let subscribed =
            expect_client
              (Nats.Client.outgoing connected.state
                 (Nats.Client.Subscribe { subject = filter; queue_group = None }))
          in
          let limited =
            expect_client
              (Nats.Client.outgoing subscribed.state
                 (Nats.Client.Auto_unsubscribe { sid = 1; max_messages = 1 }))
          in
          let message =
            Nats.Message.v ~subject:(Nats.Subject.literal "orders.created") "x"
          in
          let wire =
            match Nats.Codec.encode (Nats.Op.Msg { sid = 1; message }) with
            | Ok value -> value
            | Error error -> fail_with Nats.Codec.pp_error error
          in
          let delivered = incoming limited.state wire in
          equal int 0 (List.length (Nats.Client.subscriptions delivered.state)));
      test "timer emits bounded liveness PINGs and then closes" (fun () ->
          let config =
            expect_config
              (Nats.Config.v ~ping_interval:(Some Mtime.Span.s)
                 ~max_pings_without_pong:1 ())
          in
          let client = Nats.Client.v config in
          let info =
            expect_client
              (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp
                 (Bytesrw.Bytes.Reader.of_string
                    "INFO {\"max_payload\":100}\r\n"))
          in
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect (Nats.Client.Connect.v ())))
          in
          let deadline =
            match Nats.Client.next_timeout connected.state with
            | Some value -> value
            | None -> fail "expected a liveness deadline"
          in
          let pinged = Nats.Client.timer connected.state ~now:deadline in
          equal string "PING\r\n"
            (match pinged.output with
            | [ output ] -> output
            | _ -> fail "expected a liveness PING");
          let next_deadline =
            match Nats.Client.next_timeout pinged.state with
            | Some value -> value
            | None -> fail "expected the next liveness deadline"
          in
          let closed = Nats.Client.timer pinged.state ~now:next_deadline in
          match (Nats.Client.phase closed.state, closed.events) with
          | Nats.Client.Closed, [ Nats.Event.Closed ] -> ()
          | _ -> fail "expected liveness close");
      test "drain unsubscribes and leaves final close to the owner" (fun () ->
          let connected = connected_client () in
          let filter =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let subscribed =
            expect_client
              (Nats.Client.outgoing connected.state
                 (Nats.Client.Subscribe { subject = filter; queue_group = None }))
          in
          let draining =
            expect_client
              (Nats.Client.outgoing subscribed.state Nats.Client.Drain)
          in
          equal bool true
            (match Nats.Client.phase draining.state with
            | Nats.Client.Draining -> true
            | _ -> false);
          equal int 2 (List.length draining.output);
          match draining.events with
          | [ Nats.Event.Draining ] -> ()
          | _ -> fail "expected draining event");
    ]
