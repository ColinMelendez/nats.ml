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

let headers_only_info_wire =
  "INFO {\"max_payload\":100,\"headers\":true,\"connect_urls\":[]}" ^ "\r\n"

let no_headers_info_wire =
  "INFO {\"max_payload\":100,\"headers\":false,\"connect_urls\":[]}" ^ "\r\n"

let client_at_info () =
  let client = Nats.Client.v Nats.Config.default in
  let reader = Bytesrw.Bytes.Reader.of_string info_wire in
  expect_client
    (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader)

let connected_client () =
  let info_transition = client_at_info () in
  expect_client
    (Nats.Client.outgoing info_transition.state
       (Nats.Client.Connect
          { credentials = Nats.Client.Connect.v (); tls_required = false }))

let incoming state wire =
  let reader = Bytesrw.Bytes.Reader.of_string wire in
  expect_client
    (Nats.Client.incoming ~eod:true state ~now:Mtime.min_stamp reader)

let operation wire =
  match Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire) with
  | Ok value -> value
  | Error error -> fail_with Nats.Codec.pp_error error

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
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
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
      test "CONNECT advertises explicit TLS intent" (fun () ->
          let info = client_at_info () in
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = true;
                    }))
          in
          match connected.output with
          | [ wire ] -> (
              match
                Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire)
              with
              | Ok (Nats.Op.Connect json) ->
                  equal string
                    "{\"verbose\":false,\"pedantic\":false,\"tls_required\":true,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true}"
                    json
              | Ok _ -> fail "expected CONNECT"
              | Error error -> fail_with Nats.Codec.pp_error error)
          | _ -> fail "expected one CONNECT output");
      test "CONNECT opts into no responders with header negotiation alone"
        (fun () ->
          let client = Nats.Client.v Nats.Config.default in
          let reader = Bytesrw.Bytes.Reader.of_string headers_only_info_wire in
          let received =
            expect_client
              (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader)
          in
          let connected =
            expect_client
              (Nats.Client.outgoing received.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          match connected.output with
          | [ wire ] -> (
              match
                Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire)
              with
              | Ok (Nats.Op.Connect json) ->
                  equal string
                    "{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true}"
                    json
              | Ok _ -> fail "expected CONNECT"
              | Error error -> fail_with Nats.Codec.pp_error error)
          | _ -> fail "expected one CONNECT output");
      test "CONNECT disables no responders without header negotiation"
        (fun () ->
          let client = Nats.Client.v Nats.Config.default in
          let reader = Bytesrw.Bytes.Reader.of_string no_headers_info_wire in
          let received =
            expect_client
              (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader)
          in
          let connected =
            expect_client
              (Nats.Client.outgoing received.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          match connected.output with
          | [ wire ] -> (
              match
                Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire)
              with
              | Ok (Nats.Op.Connect json) ->
                  equal string
                    "{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":false}"
                    json
              | Ok _ -> fail "expected CONNECT"
              | Error error -> fail_with Nats.Codec.pp_error error)
          | _ -> fail "expected one CONNECT output");
      test "gates commands by phase and preserves queue subscription identity"
        (fun () ->
          let subject = Nats.Subject.literal "orders.created" in
          let message = Nats.Message.v ~subject "payload" in
          let filter =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let queue_group =
            match Nats.Queue_group.of_string "workers" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let client = Nats.Client.v Nats.Config.default in
          expect_error
            (Nats.Client.outgoing client (Nats.Client.Publish message))
            (function
            | Nats.Error.Not_connected -> true
            | _ -> false);
          expect_error
            (Nats.Client.outgoing client
               (Nats.Client.Subscribe { subject = filter; queue_group = None }))
            (function Nats.Error.Not_connected -> true | _ -> false);
          expect_error (Nats.Client.outgoing client Nats.Client.Flush) (function
            | Nats.Error.Not_connected -> true
            | _ -> false);
          expect_error
            (Nats.Client.outgoing client
               (Nats.Client.Connect
                  {
                    credentials = Nats.Client.Connect.v ();
                    tls_required = false;
                  }))
            (function Nats.Error.Info_not_received -> true | _ -> false);
          let info = client_at_info () in
          expect_error
            (Nats.Client.outgoing info.state (Nats.Client.Publish message))
            (function
            | Nats.Error.Not_connected -> true
            | _ -> false);
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          expect_error
            (Nats.Client.outgoing connected.state
               (Nats.Client.Connect
                  {
                    credentials = Nats.Client.Connect.v ();
                    tls_required = false;
                  }))
            (function Nats.Error.Already_connected -> true | _ -> false);
          let subscribed =
            expect_client
              (Nats.Client.outgoing connected.state
                 (Nats.Client.Subscribe
                    { subject = filter; queue_group = Some queue_group }))
          in
          (match subscribed.output with
          | [ wire ] -> (
              match operation wire with
              | Nats.Op.Sub { subject; queue_group = Some group; sid = 1 } ->
                  equal string "orders.*"
                    (Nats.Subject.Filter.to_string subject);
                  equal string "workers" (Nats.Queue_group.to_string group)
              | _ -> fail "expected a queue-group SUB")
          | _ -> fail "expected one queue-group SUB");
          match Nats.Client.subscriptions subscribed.state with
          | [ { sid = 1; queue_group = Some group; _ } ] ->
              equal string "workers" (Nats.Queue_group.to_string group)
          | _ -> fail "expected queue-group replay metadata");
      test "reports control events and ignores unknown subscription ids"
        (fun () ->
          let connected = connected_client () in
          let acknowledged = incoming connected.state "+OK\r\n" in
          (match acknowledged.events with
          | [ Nats.Event.Protocol_notice Nats.Event.Ok ] -> ()
          | _ -> fail "expected an OK protocol notice");
          let failed =
            incoming acknowledged.state "-ERR 'permissions violation'\r\n"
          in
          (match failed.events with
          | [ Nats.Event.Server_error { message = "permissions violation" } ] ->
              ()
          | _ -> fail "expected a normalized server error");
          let pinged = incoming failed.state "PING\r\n" in
          equal string "PONG\r\n"
            (match pinged.output with
            | [ output ] -> output
            | _ -> fail "expected a PONG response");
          let updated = incoming pinged.state info_wire in
          (match updated.events with
          | [ Nats.Event.Info _ ] -> ()
          | _ -> fail "expected an asynchronous INFO event");
          let unknown = incoming updated.state "MSG unknown 41 1\r\nx\r\n" in
          equal int 0 (List.length unknown.deliveries);
          equal int
            (List.length (Nats.Client.subscriptions updated.state))
            (List.length (Nats.Client.subscriptions unknown.state)));
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
      test "HPUB max_payload includes the encoded headers" (fun () ->
          let connected = connected_client () in
          let headers =
            match Nats.Header.of_list [ ("X-Test", "header") ] with
            | Ok value -> value
            | Error error -> fail_with Nats.Header.pp_error error
          in
          let message =
            Nats.Message.v
              ~subject:(Nats.Subject.literal "orders.created")
              ~headers (String.make 90 'x')
          in
          expect_error
            (Nats.Client.outgoing connected.state (Nats.Client.Publish message))
            (function
            | Nats.Error.Max_payload_exceeded { size; limit = 100 } ->
                size > 100
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
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
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
      test "reconnect preserves subscription ids and replay limits" (fun () ->
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
                 (Nats.Client.Auto_unsubscribe { sid = 1; max_messages = 2 }))
          in
          let reconnecting = Nats.Client.prepare_reconnect limited.state in
          let info = incoming reconnecting info_wire in
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          (match Nats.Client.phase connected.state with
          | Nats.Client.Connected -> ()
          | _ -> fail "expected a connected replay state");
          (match connected.output with
          | [ connect; subscribe; unsubscribe ] -> (
              (match operation connect with
              | Nats.Op.Connect _ -> ()
              | _ -> fail "expected replay CONNECT");
              (match operation subscribe with
              | Nats.Op.Sub { subject; queue_group = None; sid } ->
                  equal int 1 sid;
                  equal string "orders.*"
                    (Nats.Subject.Filter.to_string subject)
              | _ -> fail "expected replay SUB");
              match operation unsubscribe with
              | Nats.Op.Unsub { sid; max_messages = Some 2 } -> equal int 1 sid
              | _ -> fail "expected replay AUTO_UNSUB")
          | _ -> fail "expected CONNECT, SUB, and UNSUB replay");
          match Nats.Client.subscriptions connected.state with
          | [ { sid = 1; remaining = Some 2; _ } ] -> ()
          | _ -> fail "expected the original subscription intent");
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
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
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
      test "a flush PONG also proves liveness" (fun () ->
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
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          let deadline =
            match Nats.Client.next_timeout connected.state with
            | Some value -> value
            | None -> fail "expected a liveness deadline"
          in
          let pinged = Nats.Client.timer connected.state ~now:deadline in
          let flushed =
            expect_client (Nats.Client.outgoing pinged.state Nats.Client.Flush)
          in
          let liveness_pong =
            expect_client
              (Nats.Client.incoming ~eod:true flushed.state ~now:deadline
                 (Bytesrw.Bytes.Reader.of_string "PONG\r\n"))
          in
          (match liveness_pong.events with
          | [] -> ()
          | _ -> fail "expected the liveness PONG to remain silent");
          let completed =
            expect_client
              (Nats.Client.incoming ~eod:true liveness_pong.state ~now:deadline
                 (Bytesrw.Bytes.Reader.of_string "PONG\r\n"))
          in
          (match completed.events with
          | [ Nats.Event.Flush_completed ] -> ()
          | _ -> fail "expected flush completion");
          let next_deadline =
            match Nats.Client.next_timeout completed.state with
            | Some value -> value
            | None -> fail "expected the next liveness deadline"
          in
          let next_ping =
            Nats.Client.timer completed.state ~now:next_deadline
          in
          match (Nats.Client.phase next_ping.state, next_ping.output) with
          | Nats.Client.Connected, [ output ] -> equal string "PING\r\n" output
          | Nats.Client.Closed, _ ->
              fail "closed although the flush PONG proved liveness"
          | _ -> fail "expected the next liveness PING");
      test "multiple liveness PONGs remain classified as liveness" (fun () ->
          let config =
            expect_config
              (Nats.Config.v ~ping_interval:(Some Mtime.Span.s)
                 ~max_pings_without_pong:3 ())
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
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          let first_deadline =
            match Nats.Client.next_timeout connected.state with
            | Some value -> value
            | None -> fail "expected the first liveness deadline"
          in
          let first_ping =
            Nats.Client.timer connected.state ~now:first_deadline
          in
          let second_deadline =
            match Nats.Client.next_timeout first_ping.state with
            | Some value -> value
            | None -> fail "expected the second liveness deadline"
          in
          let second_ping =
            Nats.Client.timer first_ping.state ~now:second_deadline
          in
          let first_pong = incoming second_ping.state "PONG\r\n" in
          (match first_pong.events with
          | [] -> ()
          | _ -> fail "expected the first liveness PONG to be silent");
          let second_pong = incoming first_pong.state "PONG\r\n" in
          match second_pong.events with
          | [] -> ()
          | _ -> fail "expected the second liveness PONG to be silent");
      test "drain orders UNSUBs, permits flush, and leaves final close to owner"
        (fun () ->
          let config =
            expect_config (Nats.Config.v ~ping_interval:(Some Mtime.Span.s) ())
          in
          let client = Nats.Client.v config in
          let info =
            expect_client
              (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp
                 (Bytesrw.Bytes.Reader.of_string info_wire))
          in
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          let first_filter =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let second_filter =
            match Nats.Subject.Filter.of_string "orders.created" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let queue_group =
            match Nats.Queue_group.of_string "workers" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let first =
            expect_client
              (Nats.Client.outgoing connected.state
                 (Nats.Client.Subscribe
                    { subject = first_filter; queue_group = None }))
          in
          let second =
            expect_client
              (Nats.Client.outgoing first.state
                 (Nats.Client.Subscribe
                    { subject = second_filter; queue_group = Some queue_group }))
          in
          let draining =
            expect_client (Nats.Client.outgoing second.state Nats.Client.Drain)
          in
          (match Nats.Client.phase draining.state with
          | Nats.Client.Draining -> ()
          | _ -> fail "expected the draining phase");
          (match draining.output with
          | [ first_unsub; second_unsub; ping ] ->
              equal string "UNSUB 1\r\n" first_unsub;
              equal string "UNSUB 2\r\n" second_unsub;
              equal string "PING\r\n" ping
          | _ -> fail "expected ordered UNSUBs followed by PING");
          (match draining.events with
          | [ Nats.Event.Draining ] -> ()
          | _ -> fail "expected draining event");
          equal int 2 (List.length (Nats.Client.subscriptions draining.state));
          (match Nats.Client.next_timeout draining.state with
          | None -> ()
          | Some _ -> fail "draining state retained a liveness deadline");
          let message =
            Nats.Message.v ~subject:(Nats.Subject.literal "orders.created") "x"
          in
          expect_error
            (Nats.Client.outgoing draining.state (Nats.Client.Publish message))
            (function
            | Nats.Error.Draining -> true
            | _ -> false);
          expect_error
            (Nats.Client.outgoing draining.state
               (Nats.Client.Subscribe
                  { subject = first_filter; queue_group = None }))
            (function Nats.Error.Draining -> true | _ -> false);
          let idle = Nats.Client.timer draining.state ~now:Mtime.min_stamp in
          equal int 0 (List.length idle.output);
          let drain_ack = incoming draining.state "PONG\r\n" in
          (match drain_ack.events with
          | [ Nats.Event.Flush_completed ] -> ()
          | _ -> fail "expected the drain flush completion");
          equal int 0 (List.length (Nats.Client.subscriptions drain_ack.state));
          let flushed =
            expect_client
              (Nats.Client.outgoing drain_ack.state Nats.Client.Flush)
          in
          equal string "PING\r\n"
            (match flushed.output with
            | [ output ] -> output
            | _ -> fail "expected a flush PING while draining");
          let flush_ack = incoming flushed.state "PONG\r\n" in
          (match flush_ack.events with
          | [ Nats.Event.Flush_completed ] -> ()
          | _ -> fail "expected the draining flush completion");
          let closed =
            expect_client
              (Nats.Client.outgoing flush_ack.state Nats.Client.Close)
          in
          match (Nats.Client.phase closed.state, closed.events) with
          | Nats.Client.Closed, [ Nats.Event.Closed ] -> ()
          | _ -> fail "expected the owner-triggered close");
      test "forgets ephemeral subscriptions before reconnect replay" (fun () ->
          let config =
            expect_config (Nats.Config.v ~ping_interval:(Some Mtime.Span.s) ())
          in
          let client = Nats.Client.v config in
          let info =
            expect_client
              (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp
                 (Bytesrw.Bytes.Reader.of_string info_wire))
          in
          let connected =
            expect_client
              (Nats.Client.outgoing info.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          let queue_filter =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let inbox_filter =
            match Nats.Subject.Filter.of_string "_INBOX.reply" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let queue_group =
            match Nats.Queue_group.of_string "workers" with
            | Ok value -> value
            | Error error -> fail_with Nats.Subject.pp_error error
          in
          let queue_subscription =
            expect_client
              (Nats.Client.outgoing connected.state
                 (Nats.Client.Subscribe
                    { subject = queue_filter; queue_group = Some queue_group }))
          in
          let inbox_subscription =
            expect_client
              (Nats.Client.outgoing queue_subscription.state
                 (Nats.Client.Subscribe
                    { subject = inbox_filter; queue_group = None }))
          in
          let limited =
            expect_client
              (Nats.Client.outgoing inbox_subscription.state
                 (Nats.Client.Auto_unsubscribe { sid = 1; max_messages = 3 }))
          in
          let forgotten = Nats.Client.forget_subscription limited.state 2 in
          equal int 1 (List.length (Nats.Client.subscriptions forgotten));
          let reconnecting = Nats.Client.prepare_reconnect forgotten in
          (match Nats.Client.phase reconnecting with
          | Nats.Client.Awaiting_info -> ()
          | _ -> fail "expected reconnect negotiation");
          (match Nats.Client.next_timeout reconnecting with
          | None -> ()
          | Some _ -> fail "reconnect retained the old liveness deadline");
          (match Nats.Client.subscriptions reconnecting with
          | [ { sid = 1; queue_group = Some group; remaining = Some 3; _ } ] ->
              equal string "workers" (Nats.Queue_group.to_string group)
          | _ -> fail "unexpected replay metadata after forgetting inbox");
          let received = incoming reconnecting info_wire in
          let reconnected =
            expect_client
              (Nats.Client.outgoing received.state
                 (Nats.Client.Connect
                    {
                      credentials = Nats.Client.Connect.v ();
                      tls_required = false;
                    }))
          in
          (match reconnected.output with
          | [ connect; subscribe; unsubscribe ] -> (
              (match operation connect with
              | Nats.Op.Connect _ -> ()
              | _ -> fail "expected replay CONNECT");
              (match operation subscribe with
              | Nats.Op.Sub { sid = 1; queue_group = Some group; _ } ->
                  equal string "workers" (Nats.Queue_group.to_string group)
              | _ -> fail "expected queue-group replay");
              match operation unsubscribe with
              | Nats.Op.Unsub { sid = 1; max_messages = Some 3 } -> ()
              | _ -> fail "expected replay auto-unsubscribe")
          | _ -> fail "forgotten subscription was replayed");
          match Nats.Client.subscriptions reconnected.state with
          | [ { sid = 1; remaining = Some 3; _ } ] -> ()
          | _ -> fail "replay changed the remaining delivery intent");
    ]
