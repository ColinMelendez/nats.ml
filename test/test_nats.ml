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

let reader_of_slices strings =
  let slices = List.map Bytesrw.Bytes.Slice.of_string strings in
  let remaining = ref slices in
  Bytesrw.Bytes.Reader.make (fun () ->
      match !remaining with
      | [] -> Bytesrw.Bytes.Slice.eod
      | slice :: rest ->
          remaining := rest;
          slice)

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
      test "parses and canonicalizes NATS endpoint URLs" (fun () ->
          let endpoint =
            match Nats.Endpoint.of_string "NATS://Example.COM" with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          equal bool true
            (match Nats.Endpoint.scheme endpoint with
            | Nats.Endpoint.Nats -> true
            | Nats.Endpoint.Tls -> false);
          equal string "example.com" (Nats.Endpoint.host endpoint);
          equal int 4222 (Nats.Endpoint.port endpoint);
          equal string "nats://example.com:4222"
            (Nats.Endpoint.to_string endpoint));
      test "distinguishes anonymous and TLS certificate authentication"
        (fun () ->
          let info =
            match
              Nats.Info.of_string
                {|{"max_payload":1048576,"auth_required":true}|}
            with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Info.pp_error error)
          in
          (match Nats.Auth.connect Nats.Auth.none info with
          | Error Nats.Auth.Auth_required -> ()
          | Ok _ -> fail "anonymous authentication unexpectedly succeeded"
          | Error error -> fail (Format.asprintf "%a" Nats.Auth.pp_error error));
          match Nats.Auth.connect Nats.Auth.tls info with
          | Ok _ -> ()
          | Error error ->
              fail
                (Format.asprintf "TLS certificate authentication: %a"
                   Nats.Auth.pp_error error));
      test "parses TLS and bracketed IPv6 endpoints" (fun () ->
          let endpoint =
            match Nats.Endpoint.of_string "tls://[2001:DB8::1]:4443" with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          equal bool true
            (match Nats.Endpoint.scheme endpoint with
            | Nats.Endpoint.Tls -> true
            | Nats.Endpoint.Nats -> false);
          equal string "2001:db8::1" (Nats.Endpoint.host endpoint);
          equal int 4443 (Nats.Endpoint.port endpoint);
          equal string "tls://[2001:db8::1]:4443"
            (Nats.Endpoint.to_string endpoint));
      test "parses bare server advertisements as NATS endpoints" (fun () ->
          let endpoint =
            match Nats.Endpoint.of_connect_url "Discovered.EXAMPLE:4223" with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          equal bool true
            (match Nats.Endpoint.scheme endpoint with
            | Nats.Endpoint.Nats -> true
            | Nats.Endpoint.Tls -> false);
          equal string "discovered.example" (Nats.Endpoint.host endpoint);
          equal int 4223 (Nats.Endpoint.port endpoint));
      test "inherits TLS for bare server advertisements when requested"
        (fun () ->
          let endpoint =
            match
              Nats.Endpoint.of_connect_url ~default_scheme:Nats.Endpoint.Tls
                "discovered.example:4223"
            with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          equal bool true
            (match Nats.Endpoint.scheme endpoint with
            | Nats.Endpoint.Nats -> false
            | Nats.Endpoint.Tls -> true));
      test
        "rejects endpoint credentials, unsupported schemes, and malformed ports"
        (fun () ->
          expect_error (Nats.Endpoint.of_string "nats://user:pass@example.com")
            (function
            | Nats.Endpoint.Userinfo_not_supported -> true
            | _ -> false);
          expect_error (Nats.Endpoint.of_string "ws://example.com:80") (function
            | Nats.Endpoint.Unsupported_scheme "ws" -> true
            | _ -> false);
          expect_error (Nats.Endpoint.of_string "nats://2001:db8::1") (function
            | Nats.Endpoint.Unbracketed_ipv6 -> true
            | _ -> false);
          expect_error (Nats.Endpoint.of_string "nats://example.com:65536")
            (function
            | Nats.Endpoint.Port_out_of_range "65536" -> true
            | _ -> false);
          expect_error (Nats.Endpoint.of_string "nats://example.com/path")
            (function
            | Nats.Endpoint.Invalid_suffix -> true
            | _ -> false));
      test "keeps configured seeds and replaces discovered endpoints" (fun () ->
          let endpoint value =
            match Nats.Endpoint.of_string value with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          let seed_a = endpoint "nats://seed-a.example" in
          let seed_b = endpoint "nats://seed-b.example:4223" in
          let discovered_a = endpoint "nats://cluster-a.example" in
          let discovered_b = endpoint "nats://cluster-b.example" in
          let pool =
            Nats.Endpoint.Pool.v [ seed_a; seed_b; seed_a ] |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool
              [ discovered_a; seed_b; discovered_b; discovered_a ]
          in
          match Nats.Endpoint.Pool.candidates pool with
          | [ a; b; c; d ] -> (
              equal bool true (Nats.Endpoint.equal seed_a a);
              equal bool true (Nats.Endpoint.equal seed_b b);
              equal bool true (Nats.Endpoint.equal discovered_a c);
              equal bool true (Nats.Endpoint.equal discovered_b d);
              let replacement = endpoint "nats://cluster-new.example" in
              let pool =
                Nats.Endpoint.Pool.update_discovered pool [ replacement ]
              in
              match Nats.Endpoint.Pool.candidates pool with
              | [ first; second; third ] ->
                  equal bool true (Nats.Endpoint.equal seed_a first);
                  equal bool true (Nats.Endpoint.equal seed_b second);
                  equal bool true (Nats.Endpoint.equal replacement third)
              | _ -> fail "discovered endpoint replacement changed seed order")
          | _ -> fail "pool did not deduplicate endpoint sources");
      test "ignores empty advertisements and excludes configured seeds"
        (fun () ->
          let endpoint value =
            match Nats.Endpoint.of_string value with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          let seed = endpoint "nats://seed.example" in
          let discovered = endpoint "nats://cluster.example" in
          let pool =
            Nats.Endpoint.Pool.v [ seed ] |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ seed; discovered ]
          in
          equal int 1 (List.length (Nats.Endpoint.Pool.discovered pool));
          equal bool true
            (match Nats.Endpoint.Pool.discovered pool with
            | [ value ] -> Nats.Endpoint.equal value discovered
            | _ -> false);
          let pool = Nats.Endpoint.Pool.update_discovered pool [] in
          equal bool true
            (match Nats.Endpoint.Pool.discovered pool with
            | [ value ] -> Nats.Endpoint.equal value discovered
            | _ -> false));
      test "retains the current discovered endpoint until replacement"
        (fun () ->
          let endpoint value =
            match Nats.Endpoint.of_string value with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          let seed = endpoint "nats://seed.example" in
          let current = endpoint "nats://current.example" in
          let replacement = endpoint "nats://replacement.example" in
          let pool =
            Nats.Endpoint.Pool.v [ seed ] |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ current ] |> fun pool ->
            Nats.Endpoint.Pool.connected pool current |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ replacement ]
          in
          equal bool true
            (match Nats.Endpoint.Pool.discovered pool with
            | [ first; second ] ->
                Nats.Endpoint.equal first current
                && Nats.Endpoint.equal second replacement
            | _ -> false);
          let pool =
            Nats.Endpoint.Pool.connected pool replacement |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ replacement ]
          in
          equal bool true
            (match Nats.Endpoint.Pool.discovered pool with
            | [ value ] -> Nats.Endpoint.equal value replacement
            | _ -> false));
      test "rotates failed candidates and prefers the last success" (fun () ->
          let endpoint value =
            match Nats.Endpoint.of_string value with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          let first = endpoint "nats://first.example" in
          let second = endpoint "nats://second.example" in
          let third = endpoint "nats://third.example" in
          let pool =
            Nats.Endpoint.Pool.v [ first ] |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ second; third ]
            |> fun pool -> Nats.Endpoint.Pool.connected pool second
          in
          equal bool true
            (match Nats.Endpoint.Pool.candidates pool with
            | head :: _ -> Nats.Endpoint.equal head second
            | [] -> false);
          let pool = Nats.Endpoint.Pool.failed pool second in
          equal bool true
            (match Nats.Endpoint.Pool.candidates pool with
            | [ a; b; c ] ->
                Nats.Endpoint.equal a first
                && Nats.Endpoint.equal b third
                && Nats.Endpoint.equal c second
            | _ -> false);
          let pool = Nats.Endpoint.Pool.update_discovered pool [ third ] in
          match Nats.Endpoint.Pool.candidates pool with
          | [ a; b ] ->
              equal bool true (Nats.Endpoint.equal a first);
              equal bool true (Nats.Endpoint.equal b third)
          | _ -> fail "removed discovered endpoint remained in the pool");
      test "drops a removed preferred endpoint after a later success" (fun () ->
          let endpoint value =
            match Nats.Endpoint.of_string value with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)
          in
          let seed = endpoint "nats://seed.example" in
          let old = endpoint "nats://old.example" in
          let replacement = endpoint "nats://replacement.example" in
          let pool =
            Nats.Endpoint.Pool.v [ seed ] |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ old ] |> fun pool ->
            Nats.Endpoint.Pool.connected pool old |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ replacement ]
            |> fun pool ->
            Nats.Endpoint.Pool.connected pool seed |> fun pool ->
            Nats.Endpoint.Pool.update_discovered pool [ replacement ]
          in
          match Nats.Endpoint.Pool.candidates pool with
          | [ first; second ] ->
              equal bool true (Nats.Endpoint.equal first seed);
              equal bool true (Nats.Endpoint.equal second replacement)
          | _ -> fail "removed preferred endpoint remained after a new success");
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
      test "accepts empty reply fields in MSG and HMSG" (fun () ->
          (match read_operation "MSG inbox 1  3\r\nfoo\r\n" with
          | Nats.Op.Msg { sid = 1; message } -> (
              match Nats.Message.reply_to message with
              | None -> ()
              | Some _ -> fail "expected no reply subject")
          | _ -> fail "expected a message operation");
          match
            read_operation "HMSG inbox 1  12 12\r\nNATS/1.0\r\n\r\n\r\n"
          with
          | Nats.Op.Hmsg { sid = 1; message; status = None } -> (
              match Nats.Message.reply_to message with
              | None -> ()
              | Some _ -> fail "expected no reply subject")
          | _ -> fail "expected a header-bearing message");
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
      test "encodes and decodes HPUB empty and status-bearing messages"
        (fun () ->
          let subject = Nats.Subject.literal "orders.created" in
          let reply_to = Nats.Subject.literal "_INBOX.reply" in
          let empty = Nats.Message.v ~subject ~reply_to "" in
          let empty_operation =
            Nats.Op.Hpub { message = empty; status = None }
          in
          equal string
            ("HPUB orders.created _INBOX.reply 12 12\r\n"
           ^ "NATS/1.0\r\n\r\n\r\n")
            (expect_codec_ok (Nats.Codec.encode empty_operation));
          (match
             read_operation
               "HPUB orders.created _INBOX.reply 12 12\r\nNATS/1.0\r\n\r\n\r\n"
           with
          | Nats.Op.Hpub { message; status = None } ->
              equal bool true (Nats.Message.equal empty message)
          | _ -> fail "expected an empty HPUB operation");
          let status = { Nats.Op.code = 202; description = "Accepted" } in
          let status_operation =
            Nats.Op.Hpub { message = empty; status = Some status }
          in
          equal string
            ("HPUB orders.created _INBOX.reply 25 25\r\n"
           ^ "NATS/1.0 202 Accepted\r\n\r\n\r\n")
            (expect_codec_ok (Nats.Codec.encode status_operation));
          match
            read_operation
              "HPUB orders.created _INBOX.reply 25 25\r\n\
               NATS/1.0 202 Accepted\r\n\
               \r\n\
               \r\n"
          with
          | Nats.Op.Hpub
              { status = Some { code = 202; description = "Accepted" }; _ } ->
              ()
          | _ -> fail "expected the HPUB status line");
      test "keeps HMSG status errors and following operations separate"
        (fun () ->
          let reader =
            Bytesrw.Bytes.Reader.of_string
              ("HMSG inbox 7 12 12\r\nNATS/1.0\r\n\r\n\r\n" ^ "PING\r\n")
          in
          (match Nats.Codec.read reader with
          | Ok (Nats.Op.Hmsg { sid = 7; status = None; message }) ->
              equal string "" (Nats.Message.payload message)
          | _ -> fail "expected the HMSG before PING");
          (match Nats.Codec.read ~eod:true reader with
          | Ok Nats.Op.Ping -> ()
          | _ -> fail "expected PING to remain after HMSG");
          let subject = Nats.Subject.literal "inbox" in
          let message = Nats.Message.v ~subject "" in
          (match
             Nats.Codec.encode
               (Nats.Op.Hpub
                  {
                    message;
                    status = Some { code = 99; description = "invalid" };
                  })
           with
          | Error Nats.Codec.Invalid_status -> ()
          | _ -> fail "expected invalid outgoing status");
          match
            Nats.Codec.read ~eod:true
              (Bytesrw.Bytes.Reader.of_string
                 "HMSG inbox 1 16 16\r\nNATS/1.0 099\r\n\r\n\r\n")
          with
          | Error Nats.Codec.Invalid_status -> ()
          | _ -> fail "expected invalid incoming status");
      test "reads multiple complete operations from one reader" (fun () ->
          let reader = Bytesrw.Bytes.Reader.of_string "PING\r\nPONG\r\n" in
          (match expect_codec_ok (Nats.Codec.read reader) with
          | Nats.Op.Ping -> ()
          | _ -> fail "expected PING");
          match expect_codec_ok (Nats.Codec.read ~eod:true reader) with
          | Nats.Op.Pong -> ()
          | _ -> fail "expected PONG");
      test "accepts framing split across reader slices" (fun () ->
          let reader = reader_of_slices [ "PING\r"; "\nPONG\r"; "\n" ] in
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
      test "restores the reader after a framing error" (fun () ->
          let reader = Bytesrw.Bytes.Reader.of_string "PING\n" in
          (match Nats.Codec.read ~eod:true reader with
          | Error (Nats.Codec.Packet Nats.Packet.Malformed_line) -> ()
          | _ -> fail "expected a malformed-line error");
          equal int 0 (Bytesrw.Bytes.Reader.pos reader));
      test "distinguishes incomplete lines, EOF, and incomplete payloads"
        (fun () ->
          let incomplete_line = Bytesrw.Bytes.Reader.of_string "PING" in
          (match Nats.Packet.read incomplete_line with
          | Error Nats.Packet.Need_more -> ()
          | _ -> fail "expected more input for an incomplete line");
          equal int 0 (Bytesrw.Bytes.Reader.pos incomplete_line);
          (match
             Nats.Packet.read ~eod:true (Bytesrw.Bytes.Reader.of_string "")
           with
          | Error Nats.Packet.End_of_input -> ()
          | _ -> fail "expected end of input for an empty reader");
          let incomplete_payload =
            Bytesrw.Bytes.Reader.of_string "PUB orders.created 5\r\nhello"
          in
          match Nats.Packet.read incomplete_payload with
          | Error Nats.Packet.Need_more ->
              equal int 0 (Bytesrw.Bytes.Reader.pos incomplete_payload)
          | _ -> fail "expected more input for an incomplete payload");
      test "checks packet limits before copying a body" (fun () ->
          let line_limits =
            { Nats.Packet.default_limits with max_line_bytes = 3 }
          in
          (match
             Nats.Packet.read ~limits:line_limits
               (Bytesrw.Bytes.Reader.of_string "PING\r\n")
           with
          | Error (Nats.Packet.Line_too_long { limit = 3 }) -> ()
          | _ -> fail "expected a control-line limit error");
          let header_limits =
            { Nats.Packet.default_limits with max_header_bytes = 9 }
          in
          (match
             Nats.Packet.read ~limits:header_limits
               (Bytesrw.Bytes.Reader.of_string "HMSG reply 1 10 10\r\n")
           with
          | Error (Nats.Packet.Headers_too_large { size = 10; limit = 9 }) -> ()
          | _ -> fail "expected a header limit error");
          let packet_limits =
            { Nats.Packet.default_limits with max_packet_bytes = 10 }
          in
          match
            Nats.Packet.read ~limits:packet_limits
              (Bytesrw.Bytes.Reader.of_string "PUB foo 100\r\n")
          with
          | Error (Nats.Packet.Packet_too_large { limit = 10; _ }) -> ()
          | _ -> fail "expected a packet limit error");
      test "rejects invalid length pairs and payload terminators" (fun () ->
          (match
             Nats.Packet.read ~eod:true
               (Bytesrw.Bytes.Reader.of_string "HMSG reply 1 11 10\r\n")
           with
          | Error (Nats.Packet.Invalid_lengths { keyword = "HMSG" }) -> ()
          | _ -> fail "expected an invalid HMSG length pair");
          let reader = Bytesrw.Bytes.Reader.of_string "PUB foo 3\r\nabcXX" in
          (match Nats.Packet.read ~eod:true reader with
          | Error Nats.Packet.Invalid_terminator -> ()
          | _ -> fail "expected a repeated invalid terminator error");
          equal int 0 (Bytesrw.Bytes.Reader.pos reader));
    ]
