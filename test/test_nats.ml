open Windtrap

let expect_ok result =
  match result with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Subject.pp_error error)

let expect_header_ok result =
  match result with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)

let expect_codec_ok result =
  match result with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error)

let read_operation wire =
  let reader = Bytesrw.Bytes.Reader.of_string wire in
  match Nats.Codec.read ~eod:true reader with
  | Ok value -> value
  | Error error ->
      fail
        (Format.asprintf "%a while reading %S" Nats.Codec.pp_error error wire)

let expect_error result predicate =
  match result with
  | Ok _ -> fail "expected a validation error"
  | Error error -> equal bool true (predicate error)

let () =
  run "nats"
    [
      test "validates ordinary subjects and filters separately" (fun () ->
          let subject = expect_ok (Nats.Subject.of_string "orders.created") in
          equal string "orders.created" (Nats.Subject.to_string subject);
          expect_error (Nats.Subject.of_string "orders.*") (function
            | Nats.Subject.Wildcard_not_allowed _ -> true
            | _ -> false);
          let filter = expect_ok (Nats.Subject.Filter.of_string "orders.*") in
          equal string "orders.*" (Nats.Subject.Filter.to_string filter);
          let tail_filter =
            expect_ok (Nats.Subject.Filter.of_string "orders.>")
          in
          equal string "orders.>" (Nats.Subject.Filter.to_string tail_filter);
          expect_error (Nats.Subject.Filter.of_string "orders.>.created")
            (function
            | Nats.Subject.Wildcard_not_terminal _ -> true
            | _ -> false));
      test "rejects empty subject tokens" (fun () ->
          expect_error (Nats.Subject.of_string "orders..created") (function
            | Nats.Subject.Empty_token _ -> true
            | _ -> false);
          expect_error (Nats.Subject.Filter.of_string ".orders") (function
            | Nats.Subject.Empty_token _ -> true
            | _ -> false));
      test "queue groups reuse ordinary subject validation without wildcards"
        (fun () ->
          let group = expect_ok (Nats.Queue_group.of_string "workers") in
          equal string "workers" (Nats.Queue_group.to_string group);
          expect_error (Nats.Queue_group.of_string "workers.*") (function
            | Nats.Subject.Wildcard_not_allowed _ -> true
            | _ -> false));
      test "headers preserve order, duplicates, and original spelling"
        (fun () ->
          let headers =
            expect_header_ok
              (Nats.Header.of_list
                 [ ("Trace-ID", "one"); ("trace-id", "two"); ("Status", "200") ])
          in
          equal bool true (Nats.Header.mem "TRACE-id" headers);
          equal string "one"
            (match Nats.Header.find "trace-id" headers with
            | Some value -> value
            | None -> fail "expected a header value");
          (match Nats.Header.find_all "TRACE-ID" headers with
          | [ "one"; "two" ] -> ()
          | _ -> fail "header values were not returned in wire order");
          match Nats.Header.to_list headers with
          | [ ("Trace-ID", "one"); ("trace-id", "two"); ("Status", "200") ] ->
              ()
          | _ -> fail "header spelling or order was not preserved");
      test "rejects header injection characters" (fun () ->
          equal bool false
            (Nats.Header.is_empty
               (expect_header_ok (Nats.Header.of_list [ ("X-Test", "ok") ])));
          expect_error
            (Nats.Header.of_list [ ("X\nTest", "ok") ])
            (function
              | Nats.Header.Invalid_name_character _ -> true | _ -> false);
          expect_error
            (Nats.Header.of_list [ ("X-Test", "bad\r\nvalue") ])
            (function
              | Nats.Header.Invalid_value_character _ -> true | _ -> false));
      test "messages retain validated routing and immutable payload values"
        (fun () ->
          let subject = Nats.Subject.literal "orders.reply" in
          let reply_to = Nats.Subject.literal "_INBOX.reply" in
          let headers =
            expect_header_ok (Nats.Header.of_list [ ("Status", "200") ])
          in
          let message = Nats.Message.v ~subject ~reply_to ~headers "payload" in
          equal string "orders.reply"
            (Nats.Subject.to_string (Nats.Message.subject message));
          equal string "payload" (Nats.Message.payload message);
          equal bool true
            (Nats.Header.equal headers (Nats.Message.headers message));
          (match Nats.Message.reply_to message with
          | Some actual ->
              equal string "_INBOX.reply" (Nats.Subject.to_string actual)
          | None -> fail "expected a reply subject");
          equal string "changed"
            (Nats.Message.payload (Nats.Message.with_payload "changed" message)));
      test "encodes and decodes a publish with a reply subject" (fun () ->
          let subject = Nats.Subject.literal "orders.created" in
          let reply_to = Nats.Subject.literal "_INBOX.reply" in
          let message = Nats.Message.v ~subject ~reply_to "hello" in
          let operation = Nats.Op.Pub message in
          let encoded = expect_codec_ok (Nats.Codec.encode operation) in
          equal string "PUB orders.created _INBOX.reply 5\r\nhello\r\n" encoded;
          match read_operation encoded with
          | Nats.Op.Pub decoded ->
              equal bool true (Nats.Message.equal message decoded)
          | _ -> fail "expected a publish operation");
      test "places the sid before the reply subject in MSG" (fun () ->
          let subject = Nats.Subject.literal "orders.created" in
          let reply_to = Nats.Subject.literal "_INBOX.reply" in
          let message = Nats.Message.v ~subject ~reply_to "hello" in
          let operation = Nats.Op.Msg { sid = 9; message } in
          equal string "MSG orders.created 9 _INBOX.reply 5\r\nhello\r\n"
            (expect_codec_ok (Nats.Codec.encode operation));
          match
            read_operation "MSG orders.created 9 _INBOX.reply 5\r\nhello\r\n"
          with
          | Nats.Op.Msg { sid = 9; message = decoded } ->
              equal bool true
                (Nats.Message.equal
                   (Nats.Message.with_headers Nats.Header.empty message)
                   decoded)
          | _ -> fail "expected a message operation");
      test "encodes subscription control operations" (fun () ->
          let filter = expect_ok (Nats.Subject.Filter.of_string "orders.*") in
          let operation =
            Nats.Op.Sub
              {
                subject = filter;
                queue_group =
                  Some (expect_ok (Nats.Queue_group.of_string "workers"));
                sid = 7;
              }
          in
          equal string "SUB orders.* workers 7\r\n"
            (expect_codec_ok (Nats.Codec.encode operation));
          match read_operation "UNSUB 7 3\r\n" with
          | Nats.Op.Unsub { sid = 7; max_messages = Some 3 } -> ()
          | _ -> fail "expected an auto-unsubscribe operation");
      test "encodes and decodes repeated headers and status lines" (fun () ->
          let subject = Nats.Subject.literal "_INBOX.reply" in
          let headers =
            expect_header_ok
              (Nats.Header.of_list [ ("Trace-ID", "one"); ("trace-id", "two") ])
          in
          let message = Nats.Message.v ~subject ~headers "payload" in
          let operation = Nats.Op.Hmsg { sid = 4; message; status = None } in
          let encoded = expect_codec_ok (Nats.Codec.encode operation) in
          equal string
            ("HMSG _INBOX.reply 4 42 49\r\n"
           ^ "NATS/1.0\r\nTrace-ID: one\r\ntrace-id: two\r\n\r\n"
           ^ "payload\r\n")
            encoded;
          let () =
            match read_operation encoded with
            | Nats.Op.Hmsg { sid = 4; message = decoded; status = None } ->
                equal bool true (Nats.Message.equal message decoded)
            | _ -> fail "expected a header-bearing message"
          in
          let () =
            match
              read_operation
                "HMSG _INBOX.reply 4 30 30\r\n\
                 NATS/1.0 503 No Responders\r\n\
                 \r\n\
                 \r\n"
            with
            | Nats.Op.Hmsg
                {
                  status = Some { code = 503; description = "No Responders" };
                  _;
                } ->
                ()
            | _ -> fail "expected a no-responders status"
          in
          let bare_status =
            Nats.Op.Hmsg
              {
                sid = 4;
                message;
                status = Some { code = 503; description = "" };
              }
          in
          let bare_encoded = expect_codec_ok (Nats.Codec.encode bare_status) in
          equal string
            ("HMSG _INBOX.reply 4 46 53\r\n"
           ^ "NATS/1.0 503\r\nTrace-ID: one\r\ntrace-id: two\r\n\r\n"
           ^ "payload\r\n")
            bare_encoded;
          match read_operation bare_encoded with
          | Nats.Op.Hmsg { status = Some { code = 503; description = "" }; _ }
            ->
              ()
          | _ -> fail "expected a status without a description");
      test "reads multiple complete operations from one reader" (fun () ->
          let reader = Bytesrw.Bytes.Reader.of_string "PING\r\nPONG\r\n" in
          (match expect_codec_ok (Nats.Codec.read reader) with
          | Nats.Op.Ping -> ()
          | _ -> fail "expected PING");
          match expect_codec_ok (Nats.Codec.read ~eod:true reader) with
          | Nats.Op.Pong -> ()
          | _ -> fail "expected PONG");
      test "rejects payloads above the configured limit" (fun () ->
          let subject = Nats.Subject.literal "orders.created" in
          let operation = Nats.Op.Pub (Nats.Message.v ~subject "hello") in
          let limits =
            { Nats.Packet.default_limits with max_payload_bytes = 2 }
          in
          match Nats.Codec.encode ~limits operation with
          | Error (Nats.Codec.Packet (Nats.Packet.Payload_too_large _)) -> ()
          | _ -> fail "expected a payload limit error");
      test "reports malformed packet lengths structurally" (fun () ->
          match
            Nats.Codec.read ~eod:true
              (Bytesrw.Bytes.Reader.of_string "PUB orders.created nope\r\n")
          with
          | Error
              (Nats.Codec.Packet
                 (Nats.Packet.Invalid_length { keyword = "PUB"; _ })) ->
              ()
          | _ -> fail "expected an invalid length error");
      test "leaves an incomplete packet in the reader" (fun () ->
          let reader =
            Bytesrw.Bytes.Reader.of_string "PUB orders.created 5\r\nhe"
          in
          (match Nats.Codec.read reader with
          | Error (Nats.Codec.Packet Nats.Packet.Need_more) ->
              equal int 0 (Bytesrw.Bytes.Reader.pos reader)
          | _ -> fail "expected an incomplete packet");
          match Nats.Codec.read ~eod:true reader with
          | Error (Nats.Codec.Packet Nats.Packet.Unexpected_end) -> ()
          | _ -> fail "expected unexpected end of input");
    ]
