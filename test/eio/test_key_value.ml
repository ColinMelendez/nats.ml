open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}\r\n"

let endpoint =
  match Nats.Endpoint.of_string "nats://127.0.0.1:4222" with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)

let address = `Tcp (Eio.Net.Ipaddr.V4.loopback, 4222)

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let expect_kv_ok = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Key_value.Error.pp error)

let expect_jetstream_ok = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error)

let expect_kv_error result predicate =
  match result with
  | Ok _ -> fail "expected a key-value error"
  | Error error ->
      if not (predicate error) then
        fail
          (Format.asprintf "unexpected key-value error: %a"
             Nats_eio.Key_value.Error.pp error)

let operation_wire operation =
  match Nats.Codec.encode operation with
  | Ok wire -> wire
  | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error)

let response_wire_with_sid ~sid ?(headers = Nats.Header.empty) payload =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ~headers
      payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let stream_config_response ~sid ~bucket ~history =
  let payload =
    Format.asprintf
      {|{"config":{"name":"KV_%s","subjects":["$KV.%s.>"],"storage":"file","retention":"limits","discard":"new","max_msgs":-1,"max_msgs_per_subject":%d,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":true,"allow_direct":true,"deny_delete":true}}|}
      bucket bucket history
  in
  response_wire_with_sid ~sid payload

let stream_info_response ~sid ~bucket ~history =
  let payload =
    Format.asprintf
      {|{"config":{"name":"KV_%s","subjects":["$KV.%s.>"],"storage":"file","retention":"limits","discard":"new","max_msgs":-1,"max_msgs_per_subject":%d,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":true,"allow_direct":true,"deny_delete":true},"state":{"messages":3,"bytes":42,"first_seq":1,"last_seq":7,"consumer_count":0}}|}
      bucket bucket history
  in
  response_wire_with_sid ~sid payload

let publish_ack_response ~sid ~stream ~sequence =
  let payload =
    Format.asprintf {|{"stream":"%s","seq":%Ld}|} stream sequence
  in
  response_wire_with_sid ~sid payload

let publish_error_response ~sid ~code ~err_code ~description =
  let payload =
    Format.asprintf
      {|{"error":{"code":%d,"err_code":%d,"description":"%s"}}|}
      code err_code description
  in
  response_wire_with_sid ~sid payload

let api_ok_wire ~sid = response_wire_with_sid ~sid "{}"

let consumer_response ~sid ~policy ~headers_only ~pending =
  let headers_only =
    if headers_only then ",\"headers_only\":true" else ""
  in
  let pending =
    match pending with None -> "" | Some value -> Format.asprintf ",\"num_pending\":%Ld" value
  in
  let payload =
    Format.asprintf
      {|{"stream_name":"KV_users","name":"scan","config":{"deliver_policy":"%s","ack_policy":"none","replay_policy":"instant"%s}%s}|}
      policy headers_only pending
  in
  response_wire_with_sid ~sid payload

let consumer_delivery_wire ~sid ~consumer ~key ~stream_sequence
    ~consumer_sequence ~pending ?operation payload =
  let reply_to =
    Format.asprintf "$JS.ACK.KV_users.%s.1.%Ld.%Ld.0.%Ld" consumer
      stream_sequence consumer_sequence pending
  in
  let headers =
    match operation with
    | None -> Nats.Header.empty
    | Some operation -> (
        match Nats.Header.of_list [ ("KV-Operation", operation) ] with
        | Ok headers -> headers
        | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error))
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal ("$KV.users." ^ key))
      ~reply_to:(Nats.Subject.literal reply_to) ~headers
      payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let fetch_end_wire ~sid =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ""
  in
  operation_wire
    (Nats.Op.Hmsg
       {
         sid;
         message;
         status = Some { code = 408; description = "Request Timeout" };
       })

let direct_response ~sid ~bucket ~key ~sequence ~operation payload =
  let headers =
    let values =
      [
        ("JSStream", "KV_" ^ bucket);
        ("JSSequence", Int64.to_string sequence);
        ("JSTimeStamp", "2026-08-12T12:00:00.000000000Z");
        ("JSSubject", "$KV." ^ bucket ^ "." ^ key);
      ]
    in
    let values =
      match operation with
      | None -> values
      | Some operation -> ("KV-Operation", operation) :: values
    in
    match Nats.Header.of_list values with
    | Ok headers -> headers
    | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)
  in
  response_wire_with_sid ~sid ~headers payload

let with_connection ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "key-value-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "key-value-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ])
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
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ])
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

let require_trace ~trace ~needle =
  if not (contains_substring ~needle (Buffer.contents trace)) then
    fail
      (Format.asprintf "trace did not contain %S; trace:\n%s" needle
         (Buffer.contents trace))

let wait_for_trace ~trace ~needle =
  let attempts = ref 0 in
  while
    not (contains_substring ~needle (Buffer.contents trace))
    && Int.compare !attempts 100 < 0
  do
    Eio.Fiber.yield ();
    incr attempts
  done;
  require_trace ~trace ~needle

let key value =
  match Nats_eio.Key_value.Key.of_string value with
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Key_value.Error.pp_key error)

let config ~bucket ~history =
  match Nats_eio.Key_value.Config.v ~bucket ~history () with
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Key_value.Error.pp_config error)

let rec yield_n count =
  if count <= 0 then ()
  else (
    Eio.Fiber.yield ();
    yield_n (count - 1))

let () =
  run "nats-eio-key-value"
    [
      test "configuration and keys reject invalid external names" (fun () ->
          (match Nats_eio.Key_value.Config.v ~bucket:"users" ~history:5 () with
          | Ok config ->
              equal string "users"
                (Nats_eio.Key_value.Config.bucket config);
              equal int 5 (Nats_eio.Key_value.Config.history config)
          | Error error ->
              fail
                (Format.asprintf "%a"
                   Nats_eio.Key_value.Error.pp_config error));
          (match Nats_eio.Key_value.Config.v ~bucket:"bad.bucket" () with
          | Ok _ -> fail "bucket validation accepted a dot"
          | Error Nats_eio.Key_value.Config.Invalid_bucket_character _ -> ()
          | Error error ->
              fail
                (Format.asprintf "unexpected config error: %a"
                   Nats_eio.Key_value.Error.pp_config error));
          (match Nats_eio.Key_value.Key.of_string "a..b" with
          | Ok _ -> fail "key validation accepted consecutive dots"
          | Error Nats_eio.Key_value.Key.Invalid_key_dots -> ()
          | Error error ->
              fail
                (Format.asprintf "unexpected key error: %a"
                   Nats_eio.Key_value.Error.pp_key error));
          (match
             Nats_eio.Key_value.Config.v ~bucket:"limits" ~max_bytes:(-1L)
               ~max_value_size:(-1L) ()
           with
          | Ok config ->
              equal (option int64) None
                (Nats_eio.Key_value.Config.max_bytes config);
              equal (option int64) None
                (Nats_eio.Key_value.Config.max_value_size config)
          | Error error ->
              fail
                (Format.asprintf "unexpected normalized config error: %a"
                   Nats_eio.Key_value.Error.pp_config error)));
      test "create and status project the KV stream contract" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let status_response, status_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await status_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let create_result, create_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve create_result_u
                    (Nats_eio.Key_value.create jetstream
                       (config ~bucket:"users" ~history:5)));
              yield_n 5;
              require_trace ~trace ~needle:"STREAM.CREATE.KV_users";
              require_trace ~trace ~needle:"$KV.users.>";
              require_trace ~trace ~needle:"discard\\\":\\\"new";
              require_trace ~trace ~needle:"max_msgs_per_subject\\\":5";
              require_trace ~trace ~needle:"allow_rollup_hdrs\\\":true";
              require_trace ~trace ~needle:"allow_direct\\\":true";
              require_trace ~trace ~needle:"deny_delete\\\":true";
              Eio.Promise.resolve create_response_u
                (Ok
                   (stream_config_response ~sid:1 ~bucket:"users"
                      ~history:5));
              let value = expect_kv_ok (Eio.Promise.await create_result) in
              equal string "users" (Nats_eio.Key_value.bucket value);
              let status_result, status_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve status_result_u
                    (Nats_eio.Key_value.status value));
              yield_n 5;
              Eio.Promise.resolve status_response_u
                (Ok
                   (stream_info_response ~sid:2 ~bucket:"users"
                      ~history:5));
              let status = expect_kv_ok (Eio.Promise.await status_result) in
              equal int64 3L (Nats_eio.Key_value.Status.values status);
              equal int64 42L (Nats_eio.Key_value.Status.bytes status);
              equal int64 5L
                (Option.get (Nats_eio.Key_value.Status.history status));
              equal int64 1L
                (Nats_eio.Key_value.Status.first_revision status);
              equal int64 7L
                (Nats_eio.Key_value.Status.last_revision status);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "direct reads expose values and tombstones" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await second_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let value =
                expect_kv_ok
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let alice = key "alice" in
              expect_kv_error
                (Nats_eio.Key_value.update value alice ~revision:0L "invalid")
                (function
                | Nats_eio.Key_value.Error.Invalid_revision 0L -> true
                | _ -> false);
              expect_kv_error
                (Nats_eio.Key_value.get_revision value alice ~revision:0L)
                (function
                | Nats_eio.Key_value.Error.Invalid_revision 0L -> true
                | _ -> false);
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Key_value.get value alice));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok
                   (direct_response ~sid:1 ~bucket:"users" ~key:"alice"
                      ~sequence:4L ~operation:None "value"));
              let entry = expect_kv_ok (Eio.Promise.await first_result) in
              equal string "value" (Nats_eio.Key_value.Entry.value entry);
              equal int64 4L (Nats_eio.Key_value.Entry.revision entry);
              equal string "2026-08-12T12:00:00.000000000Z"
                (Nats_eio.Key_value.Entry.timestamp entry);
              (match Nats_eio.Key_value.Entry.operation entry with
              | Nats_eio.Key_value.Entry.Put -> ()
              | _ -> fail "value entry was not a put");
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Key_value.get value alice));
              yield_n 5;
              Eio.Promise.resolve second_response_u
                (Ok
                   (direct_response ~sid:2 ~bucket:"users" ~key:"alice"
                      ~sequence:5L ~operation:(Some "DEL") ""));
              expect_kv_error (Eio.Promise.await second_result) (function
                | Nats_eio.Key_value.Error.Key_deleted tombstone ->
                    equal int64 5L
                      (Nats_eio.Key_value.Entry.revision tombstone);
                    (match Nats_eio.Key_value.Entry.operation tombstone with
                    | Nats_eio.Key_value.Entry.Delete -> true
                    | _ -> false)
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "mutations encode CAS and tombstone headers" (fun () ->
          let put_response, put_response_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let purge_response, purge_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await put_response;
                `Await delete_response;
                `Await purge_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let value =
                expect_kv_ok
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let alice = key "alice" in
              let put_result, put_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve put_result_u
                    (Nats_eio.Key_value.put value alice "one"));
              yield_n 5;
              require_trace ~trace ~needle:"PUB $KV.users.alice";
              Eio.Promise.resolve put_response_u
                (Ok
                   (publish_ack_response ~sid:1 ~stream:"KV_users"
                      ~sequence:8L));
              equal int64 8L (expect_kv_ok (Eio.Promise.await put_result));
              let delete_result, delete_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve delete_result_u
                    (Nats_eio.Key_value.delete ~expected_revision:8L value alice));
              yield_n 5;
              require_trace ~trace
                ~needle:"Nats-Expected-Last-Subject-Sequence: 8";
              require_trace ~trace ~needle:"KV-Operation: DEL";
              Eio.Promise.resolve delete_response_u
                (Ok
                   (publish_ack_response ~sid:2 ~stream:"KV_users"
                      ~sequence:9L));
              equal int64 9L (expect_kv_ok (Eio.Promise.await delete_result));
              let purge_result, purge_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve purge_result_u
                    (Nats_eio.Key_value.purge ~expected_revision:9L value alice));
              yield_n 5;
              require_trace ~trace
                ~needle:"Nats-Expected-Last-Subject-Sequence: 9";
              require_trace ~trace ~needle:"KV-Operation: PURGE";
              require_trace ~trace ~needle:"Nats-Rollup: sub";
              Eio.Promise.resolve purge_response_u
                (Ok
                   (publish_ack_response ~sid:3 ~stream:"KV_users"
                      ~sequence:10L));
              equal int64 10L (expect_kv_ok (Eio.Promise.await purge_result));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "invalid scan filters fail before consumer creation" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw:_ connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let value =
                expect_kv_ok
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              expect_kv_error (Nats_eio.Key_value.keys ~filter:"a..>" value)
                (function
                | Nats_eio.Key_value.Error.Invalid_filter _ -> true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "keys uses a temporary pull consumer and omits tombstones" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let fetch_response, fetch_response_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await fetch_response;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let value =
                expect_kv_ok
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u (Nats_eio.Key_value.keys value));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (consumer_response ~sid:1 ~policy:"last_per_subject"
                      ~headers_only:true ~pending:None));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_response ~sid:2 ~policy:"last_per_subject"
                      ~headers_only:true ~pending:(Some 3L)));
              yield_n 5;
              require_trace ~trace ~needle:"CONSUMER.CREATE.KV_users";
              require_trace ~trace ~needle:"headers_only\\\":true";
              Eio.Promise.resolve fetch_response_u
                (Ok
                   (consumer_delivery_wire ~sid:3 ~consumer:"scan"
                      ~key:"alice" ~stream_sequence:1L ~consumer_sequence:1L
                      ~pending:2L ""
                   ^ consumer_delivery_wire ~sid:3 ~consumer:"scan"
                       ~key:"bob" ~stream_sequence:2L ~consumer_sequence:2L
                       ~pending:1L ""
                   ^ consumer_delivery_wire ~sid:3 ~consumer:"scan"
                       ~key:"charlie" ~stream_sequence:3L
                       ~consumer_sequence:3L ~pending:0L ~operation:"DEL"
                       ""
                   ^ fetch_end_wire ~sid:3));
              yield_n 5;
              require_trace ~trace ~needle:"CONSUMER.MSG.NEXT.KV_users.scan";
              wait_for_trace ~trace
                ~needle:"PUB $JS.API.CONSUMER.DELETE.KV_users.scan";
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:4));
              let keys =
                match Eio.Promise.await result with
                | Ok keys -> keys
                | Error error ->
                    fail
                      (Format.asprintf "keys scan failed: %a"
                         Nats_eio.Key_value.Error.pp error)
              in
              equal (list string)
                [ "alice"; "bob" ]
                (List.map Nats_eio.Key_value.Key.to_string keys);
              require_trace ~trace ~needle:"CONSUMER.DELETE.KV_users.scan";
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "history retains tombstones in server order" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let fetch_response, fetch_response_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await fetch_response;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let value =
                expect_kv_ok
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Key_value.history value (key "alice")));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (consumer_response ~sid:1 ~policy:"all" ~headers_only:false
                      ~pending:None));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_response ~sid:2 ~policy:"all" ~headers_only:false
                      ~pending:(Some 3L)));
              yield_n 5;
              Eio.Promise.resolve fetch_response_u
                (Ok
                   (consumer_delivery_wire ~sid:3 ~consumer:"scan"
                      ~key:"alice" ~stream_sequence:1L ~consumer_sequence:1L
                      ~pending:2L "one"
                   ^ consumer_delivery_wire ~sid:3 ~consumer:"scan"
                       ~key:"alice" ~stream_sequence:2L ~consumer_sequence:2L
                       ~pending:1L ~operation:"DEL" ""
                   ^ consumer_delivery_wire ~sid:3 ~consumer:"scan"
                       ~key:"alice" ~stream_sequence:3L ~consumer_sequence:3L
                       ~pending:0L ~operation:"PURGE" ""
                   ^ fetch_end_wire ~sid:3));
              yield_n 5;
              wait_for_trace ~trace
                ~needle:"PUB $JS.API.CONSUMER.DELETE.KV_users.scan";
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:4));
              let entries =
                match Eio.Promise.await result with
                | Ok entries -> entries
                | Error error ->
                    fail
                      (Format.asprintf "history scan failed: %a"
                         Nats_eio.Key_value.Error.pp error)
              in
              equal int 3 (List.length entries);
              let revisions =
                List.map Nats_eio.Key_value.Entry.revision entries
              in
              equal (list int64) [ 1L; 2L; 3L ] revisions;
              equal string "1970-01-01T00:00:00.000000000Z"
                (Nats_eio.Key_value.Entry.timestamp (List.hd entries));
              (match
                 List.map Nats_eio.Key_value.Entry.operation entries
               with
              | [
                  Nats_eio.Key_value.Entry.Put;
                  Nats_eio.Key_value.Entry.Delete;
                  Nats_eio.Key_value.Entry.Purge;
                ] -> ()
              | _ -> fail "history did not retain operation order");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "CAS errors are mapped and create resurrects tombstones" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let third_response, third_response_u = Eio.Promise.create () in
          let fourth_response, fourth_response_u = Eio.Promise.create () in
          let fifth_response, fifth_response_u = Eio.Promise.create () in
          let sixth_response, sixth_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await second_response;
                `Await third_response;
                `Await fourth_response;
                `Await fifth_response;
                `Await sixth_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let value =
                expect_kv_ok
                  (Nats_eio.Key_value.bind jetstream ~bucket:"users")
              in
              let alice = key "alice" in
              let update_result, update_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve update_result_u
                    (Nats_eio.Key_value.update value alice ~revision:3L "new"));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok
                   (publish_error_response ~sid:1 ~code:400 ~err_code:10071
                      ~description:"wrong last sequence"));
              require_trace ~trace
                ~needle:"Nats-Expected-Last-Subject-Sequence: 3";
              let update_result = Eio.Promise.await update_result in
              (match update_result with
              | Error (Nats_eio.Key_value.Error.Revision_mismatch { expected = 3L })
                -> ()
              | Ok _ -> fail "update unexpectedly succeeded"
              | Error error ->
                  fail
                    (Format.asprintf "update stage failed: %a"
                       Nats_eio.Key_value.Error.pp error));
              let create_result, create_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve create_result_u
                    (Nats_eio.Key_value.create_key value alice "new"));
              yield_n 5;
              Eio.Promise.resolve second_response_u
                (Ok
                   (publish_error_response ~sid:2 ~code:400 ~err_code:10071
                      ~description:"wrong last sequence"));
              yield_n 5;
              Eio.Promise.resolve third_response_u
                (Ok
                   (direct_response ~sid:3 ~bucket:"users" ~key:"alice"
                      ~sequence:7L ~operation:(Some "DEL") ""));
              yield_n 5;
              require_trace ~trace
                ~needle:"Nats-Expected-Last-Subject-Sequence: 7";
              Eio.Promise.resolve fourth_response_u
                (Ok
                   (publish_ack_response ~sid:4 ~stream:"KV_users"
                      ~sequence:8L));
              let revision =
                match Eio.Promise.await create_result with
                | Ok revision -> revision
                | Error error ->
                    fail
                      (Format.asprintf "create resurrection failed: %a"
                         Nats_eio.Key_value.Error.pp error)
              in
              equal int64 8L revision;
              let existing_result, existing_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve existing_result_u
                    (Nats_eio.Key_value.create_key value alice "again"));
              yield_n 5;
              Eio.Promise.resolve fifth_response_u
                (Ok
                   (publish_error_response ~sid:5 ~code:400 ~err_code:10071
                      ~description:"wrong last sequence"));
              yield_n 5;
              Eio.Promise.resolve sixth_response_u
                (Ok
                   (direct_response ~sid:6 ~bucket:"users" ~key:"alice"
                      ~sequence:8L ~operation:None "new"));
              (match Eio.Promise.await existing_result with
              | Error Nats_eio.Key_value.Error.Key_exists -> ()
              | Ok _ -> fail "second create unexpectedly succeeded"
              | Error error ->
                  fail
                    (Format.asprintf "second create failed: %a"
                       Nats_eio.Key_value.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
