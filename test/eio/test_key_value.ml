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

let operation_wire operation =
  match Nats.Codec.encode operation with
  | Ok wire -> wire
  | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error)

let response_wire ~sid payload =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") payload
  in
  operation_wire (Nats.Op.Msg { sid; message })

let consumer_response_wire ~sid ~policy ~num_pending =
  response_wire ~sid
    (Format.asprintf
       "{\"stream_name\":\"KV_users\",\"name\":\"watch\",\"config\":{\"deliver_subject\":\"_INBOX.watch\",\"deliver_policy\":\"%s\",\"ack_policy\":\"none\",\"replay_policy\":\"instant\"},\"num_pending\":%Ld}"
       policy num_pending)

let watch_delivery_wire ~sid ~stream_sequence ~consumer_sequence ~num_pending
    ?operation payload =
  let reply_to =
    Format.asprintf "$JS.ACK.KV_users.watch.1.%Ld.%Ld.1710000000000000000.%Ld"
      stream_sequence consumer_sequence num_pending
  in
  let subject = Nats.Subject.literal "$KV.users.name" in
  let message =
    match operation with
    | None ->
        Nats.Message.v ~subject
          ~reply_to:(Nats.Subject.literal reply_to)
          payload
    | Some operation ->
        let headers =
          match Nats.Header.of_list [ ("KV-Operation", operation) ] with
          | Ok headers -> headers
          | Error error ->
              fail (Format.asprintf "%a" Nats.Header.pp_error error)
        in
        Nats.Message.v ~subject
          ~reply_to:(Nats.Subject.literal reply_to)
          ~headers payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let one_shot_consumer_response_wire ~sid ~name ~policy ~num_pending
    ?filter_subject ?headers_only () =
  let filter_subject =
    match filter_subject with
    | None -> ""
    | Some value -> Format.asprintf ",\"filter_subject\":\"%s\"" value
  in
  let headers_only =
    match headers_only with
    | None -> ""
    | Some value -> Format.asprintf ",\"headers_only\":%b" value
  in
  response_wire ~sid
    (Format.asprintf
       "{\"stream_name\":\"KV_users\",\"name\":\"%s\",\"config\":{\"deliver_policy\":\"%s\",\"ack_policy\":\"none\",\"replay_policy\":\"instant\"%s%s},\"num_pending\":%Ld}"
       name policy filter_subject headers_only num_pending)

let key_delivery_wire ~sid ~consumer ~stream_sequence ~consumer_sequence
    ~num_pending ~subject ?operation payload =
  let reply_to =
    Format.asprintf "$JS.ACK.KV_users.%s.1.%Ld.%Ld.1710000000000000000.%Ld"
      consumer stream_sequence consumer_sequence num_pending
  in
  let message =
    match operation with
    | None ->
        Nats.Message.v
          ~subject:(Nats.Subject.literal subject)
          ~reply_to:(Nats.Subject.literal reply_to)
          payload
    | Some operation ->
        let headers =
          match Nats.Header.of_list [ ("KV-Operation", operation) ] with
          | Ok headers -> headers
          | Error error ->
              fail (Format.asprintf "%a" Nats.Header.pp_error error)
        in
        Nats.Message.v
          ~subject:(Nats.Subject.literal subject)
          ~reply_to:(Nats.Subject.literal reply_to)
          ~headers payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let status_wire_with_sid ~sid ~code ~description =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ""
  in
  operation_wire
    (Nats.Op.Hmsg { sid; message; status = Some { code; description } })

let api_error_wire ~sid ~err_code =
  response_wire ~sid
    (Format.asprintf
       "{\"error\":{\"code\":400,\"err_code\":%d,\"description\":\"wrong last \
        sequence\"}}"
       err_code)

let direct_message_wire ~sid ~stream ~subject ~sequence ~timestamp ?operation
    payload =
  let fields =
    [
      ("JSStream", stream);
      ("JSSequence", Int64.to_string sequence);
      ("JSTimeStamp", timestamp);
      ("JSSubject", subject);
    ]
  in
  let fields =
    match operation with
    | None -> fields
    | Some operation -> ("KV-Operation", operation) :: fields
  in
  let headers =
    match Nats.Header.of_list fields with
    | Ok headers -> headers
    | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "_INBOX.reply")
      ~headers payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let with_connection ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "key-value-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "key-value-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    match
      Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ]
    with
    | Ok value -> value
    | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)
  in
  f ~sw connection

let with_connection_traced ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "key-value-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "key-value-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return flow ];
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
    match
      Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ]
    with
    | Ok value -> value
    | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)
  in
  f ~sw ~trace connection

let index_substring ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let found = ref None in
  while Option.is_none !found && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      found := Some !index;
    incr index
  done;
  !found

let contains_substring ~needle value =
  Option.is_some (index_substring ~needle value)

let rec yield_n count =
  if count <= 0 then ()
  else (
    Eio.Fiber.yield ();
    yield_n (count - 1))

let expect_jetstream = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error)

let expect_key_value = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Key_value.Error.pp error)

let expect_config = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Key_value.Error.pp_config error)

let expect_key = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Key_value.Error.pp_key error)

let () =
  run "nats-eio-key-value"
    [
      test "config validates bucket limits and preserves values" (fun () ->
          let config =
            expect_config
              (Nats_eio.Key_value.Config.v ~bucket:"users" ~history:8
                 ~ttl:Mtime.Span.(5 * s)
                 ~max_bytes:1024L ~max_value_size:256L
                 ~storage:Nats_eio.Key_value.Config.Memory ())
          in
          equal string "users" (Nats_eio.Key_value.Config.bucket config);
          equal int 8 (Nats_eio.Key_value.Config.history config);
          equal int64 5_000_000_000L
            (Option.get
               (Option.map Mtime.Span.to_uint64_ns
                  (Nats_eio.Key_value.Config.ttl config)));
          equal int64 1024L
            (Option.get (Nats_eio.Key_value.Config.max_bytes config));
          equal int64 256L
            (Option.get (Nats_eio.Key_value.Config.max_value_size config));
          match Nats_eio.Key_value.Config.storage config with
          | Nats_eio.Key_value.Config.Memory -> ()
          | Nats_eio.Key_value.Config.File -> fail "config changed storage");
      test "config rejects invalid bucket and history" (fun () ->
          (match Nats_eio.Key_value.Config.v ~bucket:"bad.bucket" () with
          | Ok _ -> fail "bucket with a dot was accepted"
          | Error
              (Nats_eio.Key_value.Error.Invalid_bucket_character
                 { position = 3; character = '.' }) ->
              ()
          | Error error ->
              fail
                (Format.asprintf "unexpected bucket error: %a"
                   Nats_eio.Key_value.Error.pp_config error));
          match Nats_eio.Key_value.Config.v ~bucket:"users" ~history:65 () with
          | Ok _ -> fail "history above the server limit was accepted"
          | Error (Nats_eio.Key_value.Error.Invalid_history 65) -> ()
          | Error error ->
              fail
                (Format.asprintf "unexpected history error: %a"
                   Nats_eio.Key_value.Error.pp_config error));
      test "key validation accepts NATS KV key characters" (fun () ->
          let key =
            expect_key (Nats_eio.Key_value.Key.of_string "team/a.b_c-1=2")
          in
          equal string "team/a.b_c-1=2" (Nats_eio.Key_value.Key.to_string key));
      test "key validation rejects wildcard and empty-dot forms" (fun () ->
          (match Nats_eio.Key_value.Key.of_string "team.*" with
          | Ok _ -> fail "wildcard key was accepted"
          | Error
              (Nats_eio.Key_value.Error.Invalid_key_character
                 { position = 5; character = '*' }) ->
              ()
          | Error error ->
              fail
                (Format.asprintf "unexpected wildcard error: %a"
                   Nats_eio.Key_value.Error.pp_key error));
          match Nats_eio.Key_value.Key.of_string "team..member" with
          | Ok _ -> fail "empty key token was accepted"
          | Error Nats_eio.Key_value.Error.Invalid_key_dots -> ()
          | Error error ->
              fail
                (Format.asprintf "unexpected dot error: %a"
                   Nats_eio.Key_value.Error.pp_key error));
      test "put and get preserve the KV revision contract" (fun () ->
          let put_response, put_response_u = Eio.Promise.create () in
          let get_response, get_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await put_response;
                `Await get_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let put_result, put_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve put_result_u
                    (Nats_eio.Key_value.put bucket ~key:"name" "alice"));
              yield_n 5;
              Eio.Promise.resolve put_response_u
                (Ok (response_wire ~sid:1 {|{"stream":"KV_users","seq":7}|}));
              equal int64 7L (expect_key_value (Eio.Promise.await put_result));
              let get_result, get_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve get_result_u
                    (Nats_eio.Key_value.get bucket ~key:"name"));
              yield_n 5;
              Eio.Promise.resolve get_response_u
                (Ok
                   (direct_message_wire ~sid:2 ~stream:"KV_users"
                      ~subject:"$KV.users.name" ~sequence:7L
                      ~timestamp:"2026-08-11T12:00:00.000000000Z" "alice"));
              let entry = expect_key_value (Eio.Promise.await get_result) in
              equal string "users" (Nats_eio.Key_value.Entry.bucket entry);
              equal string "name" (Nats_eio.Key_value.Entry.key entry);
              equal string "alice" (Nats_eio.Key_value.Entry.value entry);
              equal int64 7L (Nats_eio.Key_value.Entry.revision entry);
              (match Nats_eio.Key_value.Entry.operation entry with
              | Nats_eio.Key_value.Entry.Put -> ()
              | Nats_eio.Key_value.Entry.Delete | Nats_eio.Key_value.Entry.Purge
                ->
                  fail "put was decoded as a tombstone");
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "CAS failures are structured" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Key_value.update bucket ~key:"name" ~revision:6L
                       "alice"));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (api_error_wire ~sid:1 ~err_code:10071));
              (match Eio.Promise.await result with
              | Error
                  (Nats_eio.Key_value.Error.Revision_mismatch { expected = 6L })
                ->
                  ()
              | Ok _ -> fail "CAS update unexpectedly succeeded"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected CAS error: %a"
                       Nats_eio.Key_value.Error.pp error));
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "get reports a tombstone with its operation and revision" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Key_value.get bucket ~key:"name"));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (direct_message_wire ~sid:1 ~stream:"KV_users"
                      ~subject:"$KV.users.name" ~sequence:8L
                      ~timestamp:"2026-08-11T12:00:01.000000000Z"
                      ~operation:"DEL" ""));
              (match Eio.Promise.await result with
              | Error (Nats_eio.Key_value.Error.Key_deleted entry) -> (
                  equal int64 8L (Nats_eio.Key_value.Entry.revision entry);
                  match Nats_eio.Key_value.Entry.operation entry with
                  | Nats_eio.Key_value.Entry.Delete -> ()
                  | Nats_eio.Key_value.Entry.Put
                  | Nats_eio.Key_value.Entry.Purge ->
                      fail "delete marker lost its operation")
              | Ok _ -> fail "tombstone was returned as a visible value"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected tombstone result: %a"
                       Nats_eio.Key_value.Error.pp error));
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "watch orders the initial marker before live entries" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let initial_delivery, initial_delivery_u = Eio.Promise.create () in
          let live_delivery, live_delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await initial_delivery;
                `Await live_delivery;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let watch_result, watch_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve watch_result_u
                    (Nats_eio.Key_value.Watch.v ~sw bucket));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (consumer_response_wire ~sid:2 ~policy:"last_per_subject"
                      ~num_pending:1L));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_response_wire ~sid:3 ~policy:"last_per_subject"
                      ~num_pending:1L));
              let watch = expect_key_value (Eio.Promise.await watch_result) in
              let trace_output = Buffer.contents trace in
              (match
                 ( index_substring ~needle:"SUB _INBOX" trace_output,
                   index_substring
                     ~needle:"PUB $JS.API.CONSUMER.CREATE.KV_users" trace_output
                 )
               with
              | Some subscribe, Some create
                when Int.compare subscribe create < 0 ->
                  ()
              | _ -> fail "watch created its consumer before subscribing");
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Key_value.Watch.next watch));
              yield_n 5;
              Eio.Promise.resolve initial_delivery_u
                (Ok
                   (watch_delivery_wire ~sid:1 ~stream_sequence:7L
                      ~consumer_sequence:1L ~num_pending:0L "alice"));
              (match Eio.Promise.await first_result with
              | Ok (Nats_eio.Key_value.Watch.Entry entry) ->
                  equal string "alice" (Nats_eio.Key_value.Entry.value entry);
                  equal int64 7L (Nats_eio.Key_value.Entry.revision entry);
                  equal string "2024-03-09T16:00:00.000000000Z"
                    (Nats_eio.Key_value.Entry.timestamp entry)
              | Ok Nats_eio.Key_value.Watch.Initial_done ->
                  fail "watch emitted Initial_done before the initial entry"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected initial watch error: %a"
                       Nats_eio.Key_value.Error.pp error));
              (match Nats_eio.Key_value.Watch.next watch with
              | Ok Nats_eio.Key_value.Watch.Initial_done -> ()
              | Ok (Nats_eio.Key_value.Watch.Entry _) ->
                  fail "watch omitted Initial_done"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected marker error: %a"
                       Nats_eio.Key_value.Error.pp error));
              let live_result, live_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve live_result_u
                    (Nats_eio.Key_value.Watch.next watch));
              yield_n 5;
              Eio.Promise.resolve live_delivery_u
                (Ok
                   (watch_delivery_wire ~sid:1 ~stream_sequence:8L
                      ~consumer_sequence:2L ~num_pending:0L "bob"));
              (match Eio.Promise.await live_result with
              | Ok (Nats_eio.Key_value.Watch.Entry entry) ->
                  equal string "bob" (Nats_eio.Key_value.Entry.value entry)
              | Ok Nats_eio.Key_value.Watch.Initial_done ->
                  fail "watch emitted Initial_done more than once"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected live watch error: %a"
                       Nats_eio.Key_value.Error.pp error));
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Key_value.Watch.close watch));
              yield_n 5;
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire ~sid:4 "{}"));
              (match Eio.Promise.await close_result with
              | Ok () -> ()
              | Error error ->
                  fail
                    (Format.asprintf "watch close failed: %a\n%s"
                       Nats_eio.Key_value.Error.pp error (Buffer.contents trace)));
              if
                not
                  (contains_substring
                     ~needle:"PUB $JS.API.CONSUMER.DELETE.KV_users.watch"
                     (Buffer.contents trace))
              then fail "watch close did not delete its owned consumer";
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "new watch emits Initial_done before its first update" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await delivery;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let watch_result, watch_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve watch_result_u
                    (Nats_eio.Key_value.Watch.v ~sw
                       ~delivery:Nats_eio.Key_value.Watch.New bucket));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (consumer_response_wire ~sid:2 ~policy:"new" ~num_pending:0L));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_response_wire ~sid:3 ~policy:"new" ~num_pending:0L));
              let watch = expect_key_value (Eio.Promise.await watch_result) in
              (match Nats_eio.Key_value.Watch.next watch with
              | Ok Nats_eio.Key_value.Watch.Initial_done -> ()
              | Ok (Nats_eio.Key_value.Watch.Entry _) ->
                  fail "new watch returned an entry before Initial_done"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected new-watch marker error: %a"
                       Nats_eio.Key_value.Error.pp error));
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Key_value.Watch.next watch));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok
                   (watch_delivery_wire ~sid:1 ~stream_sequence:9L
                      ~consumer_sequence:1L ~num_pending:0L "carol"));
              (match Eio.Promise.await result with
              | Ok (Nats_eio.Key_value.Watch.Entry entry) ->
                  equal string "carol" (Nats_eio.Key_value.Entry.value entry)
              | Ok Nats_eio.Key_value.Watch.Initial_done ->
                  fail "new watch emitted the marker twice"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected new-watch error: %a"
                       Nats_eio.Key_value.Error.pp error));
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Key_value.Watch.close watch));
              yield_n 5;
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire ~sid:4 "{}"));
              (match Eio.Promise.await close_result with
              | Ok () -> ()
              | Error error ->
                  fail
                    (Format.asprintf "new watch close failed: %a\n%s"
                       Nats_eio.Key_value.Error.pp error (Buffer.contents trace)));
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "keys returns live keys and cleans up its consumer" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let third_delivery, third_delivery_u = Eio.Promise.create () in
          let fetch_done, fetch_done_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await first_delivery;
                `Await second_delivery;
                `Await third_delivery;
                `Await fetch_done;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Key_value.keys ~filter:"team.>" bucket));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (one_shot_consumer_response_wire ~sid:1 ~name:"reader"
                      ~policy:"last_per_subject" ~num_pending:0L
                      ~filter_subject:"$KV.users.team.>" ~headers_only:true ()));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (one_shot_consumer_response_wire ~sid:2 ~name:"reader"
                      ~policy:"last_per_subject" ~num_pending:3L
                      ~filter_subject:"$KV.users.team.>" ~headers_only:true ()));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (key_delivery_wire ~sid:3 ~consumer:"reader"
                      ~stream_sequence:7L ~consumer_sequence:1L ~num_pending:2L
                      ~subject:"$KV.users.team.alice" ""));
              yield_n 5;
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (key_delivery_wire ~sid:3 ~consumer:"reader"
                      ~stream_sequence:8L ~consumer_sequence:2L ~num_pending:1L
                      ~subject:"$KV.users.team.bob" ~operation:"DEL" ""));
              yield_n 5;
              Eio.Promise.resolve third_delivery_u
                (Ok
                   (key_delivery_wire ~sid:3 ~consumer:"reader"
                      ~stream_sequence:9L ~consumer_sequence:3L ~num_pending:0L
                      ~subject:"$KV.users.team.carol" ""));
              yield_n 5;
              Eio.Promise.resolve fetch_done_u
                (Ok
                   (status_wire_with_sid ~sid:3 ~code:408
                      ~description:"Request Timeout"));
              let keys = expect_key_value (Eio.Promise.await result) in
              (match keys with
              | [ first; second ] ->
                  equal string "team.alice" first;
                  equal string "team.carol" second
              | _ -> fail "keys returned the wrong live-key set");
              let trace_output = Buffer.contents trace in
              if
                not
                  (contains_substring
                     ~needle:"deliver_policy\\\":\\\"last_per_subject"
                     trace_output)
              then fail "keys did not use Last_per_subject";
              if
                not
                  (contains_substring ~needle:"headers_only\\\":true"
                     trace_output)
              then fail "keys did not request metadata-only delivery";
              if
                not
                  (contains_substring
                     ~needle:"filter_subject\\\":\\\"$KV.users.team.>"
                     trace_output)
              then fail "keys did not apply its bucket-relative filter";
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire ~sid:4 "{}"));
              yield_n 5;
              if
                not
                  (contains_substring
                     ~needle:"PUB $JS.API.CONSUMER.DELETE.KV_users.reader"
                     (Buffer.contents trace))
              then fail "keys did not delete its ephemeral consumer";
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "history retries empty fetches and preserves tombstones" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let first_fetch_done, first_fetch_done_u = Eio.Promise.create () in
          let retry_info, retry_info_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let third_delivery, third_delivery_u = Eio.Promise.create () in
          let second_fetch_done, second_fetch_done_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await first_fetch_done;
                `Await retry_info;
                `Await first_delivery;
                `Await second_delivery;
                `Await third_delivery;
                `Await second_fetch_done;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Key_value.history bucket ~key:"name"));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (one_shot_consumer_response_wire ~sid:1 ~name:"reader"
                      ~policy:"all" ~num_pending:0L
                      ~filter_subject:"$KV.users.name" ~headers_only:false ()));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (one_shot_consumer_response_wire ~sid:2 ~name:"reader"
                      ~policy:"all" ~num_pending:3L
                      ~filter_subject:"$KV.users.name" ~headers_only:false ()));
              yield_n 5;
              Eio.Promise.resolve first_fetch_done_u
                (Ok
                   (status_wire_with_sid ~sid:3 ~code:408
                      ~description:"Request Timeout"));
              yield_n 5;
              Eio.Promise.resolve retry_info_u
                (Ok
                   (one_shot_consumer_response_wire ~sid:4 ~name:"reader"
                      ~policy:"all" ~num_pending:3L
                      ~filter_subject:"$KV.users.name" ~headers_only:false ()));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (key_delivery_wire ~sid:5 ~consumer:"reader"
                      ~stream_sequence:10L ~consumer_sequence:1L ~num_pending:2L
                      ~subject:"$KV.users.name" "alice"));
              yield_n 5;
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (key_delivery_wire ~sid:5 ~consumer:"reader"
                      ~stream_sequence:11L ~consumer_sequence:2L ~num_pending:1L
                      ~subject:"$KV.users.name" ~operation:"DEL" ""));
              yield_n 5;
              Eio.Promise.resolve third_delivery_u
                (Ok
                   (key_delivery_wire ~sid:5 ~consumer:"reader"
                      ~stream_sequence:12L ~consumer_sequence:3L ~num_pending:0L
                      ~subject:"$KV.users.name" ~operation:"PURGE" ""));
              yield_n 5;
              Eio.Promise.resolve second_fetch_done_u
                (Ok
                   (status_wire_with_sid ~sid:5 ~code:408
                      ~description:"Request Timeout"));
              let entries = expect_key_value (Eio.Promise.await result) in
              (match entries with
              | [ put; delete; purge ] -> (
                  equal string "alice" (Nats_eio.Key_value.Entry.value put);
                  equal int64 10L (Nats_eio.Key_value.Entry.revision put);
                  (match Nats_eio.Key_value.Entry.operation put with
                  | Nats_eio.Key_value.Entry.Put -> ()
                  | Nats_eio.Key_value.Entry.Delete
                  | Nats_eio.Key_value.Entry.Purge ->
                      fail "history changed the put operation");
                  (match Nats_eio.Key_value.Entry.operation delete with
                  | Nats_eio.Key_value.Entry.Delete -> ()
                  | Nats_eio.Key_value.Entry.Put
                  | Nats_eio.Key_value.Entry.Purge ->
                      fail "history lost the delete operation");
                  match Nats_eio.Key_value.Entry.operation purge with
                  | Nats_eio.Key_value.Entry.Purge -> ()
                  | Nats_eio.Key_value.Entry.Put
                  | Nats_eio.Key_value.Entry.Delete ->
                      fail "history lost the purge operation")
              | _ -> fail "history returned the wrong number of entries");
              let trace_output = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"deliver_policy\\\":\\\"all"
                     trace_output)
              then fail "history did not request all retained entries";
              if
                not
                  (contains_substring
                     ~needle:"filter_subject\\\":\\\"$KV.users.name"
                     trace_output)
              then fail "history did not filter the requested key";
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire ~sid:6 "{}"));
              yield_n 5;
              if
                not
                  (contains_substring
                     ~needle:"PUB $JS.API.CONSUMER.DELETE.KV_users.reader"
                     (Buffer.contents trace))
              then fail "history did not delete its ephemeral consumer";
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "keys rejects invalid filters before creating a consumer" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw:_ ~trace connection ->
              let jetstream =
                expect_jetstream (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_key_value
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              (match Nats_eio.Key_value.keys ~filter:"bad..key" bucket with
              | Error (Nats_eio.Key_value.Error.Invalid_filter _) -> ()
              | Ok _ -> fail "invalid filter was accepted"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected filter error: %a"
                       Nats_eio.Key_value.Error.pp error));
              if
                contains_substring ~needle:"CONSUMER.CREATE"
                  (Buffer.contents trace)
              then fail "invalid filter created a consumer";
              (match Nats_eio.Connection.close connection with
              | Ok () -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
