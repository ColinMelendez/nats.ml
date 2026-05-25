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

let expect_jetstream_ok = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error)

let expect_object_ok = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error)

let operation_wire operation =
  match Nats.Codec.encode operation with
  | Ok wire -> wire
  | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error)

let response_wire_with_sid ~sid payload =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") payload
  in
  operation_wire (Nats.Op.Msg { sid; message })

let direct_response_wire ~sid ~bucket ~name payload =
  let encoded_name =
    Base64.encode_string ~alphabet:Base64.uri_safe_alphabet name
  in
  let headers =
    match
      Nats.Header.of_list
        [
          ("JSStream", "OBJ_" ^ bucket);
          ("JSSequence", "12");
          ("JSTimeStamp", "2026-08-12T12:00:00.000000000Z");
          ("JSSubject", "$O." ^ bucket ^ ".M." ^ encoded_name);
        ]
    with
    | Ok headers -> headers
    | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)
  in
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ~headers
      payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let stream_response ~sid ~bucket =
  let payload =
    Format.asprintf
      {|{"config":{"name":"OBJ_%s","description":"media","subjects":["$O.%s.C.>","$O.%s.M.>"],"storage":"memory","retention":"limits","discard":"new","max_bytes":4096,"max_age":1000000000,"allow_rollup_hdrs":true,"allow_direct":true}}|}
      bucket bucket bucket
  in
  response_wire_with_sid ~sid payload

let stream_info_response ~sid ~bucket =
  let payload =
    Format.asprintf
      {|{"config":{"name":"OBJ_%s","description":"media","subjects":["$O.%s.C.>","$O.%s.M.>"],"storage":"memory","retention":"limits","discard":"new","max_bytes":4096,"max_age":1000000000,"allow_rollup_hdrs":true,"allow_direct":true},"state":{"messages":4,"bytes":321,"first_seq":3,"last_seq":9,"consumer_count":0}}|}
      bucket bucket bucket
  in
  response_wire_with_sid ~sid payload

let with_connection_traced ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "object-store-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "object-store-network" in
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

let yield_n count =
  for _ = 1 to count do
    Eio.Fiber.yield ()
  done

let contains_substring ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let found = ref false in
  while not !found && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      found := true
    else incr index
  done;
  !found

let require_trace ~trace ~needle =
  if not (contains_substring ~needle (Buffer.contents trace)) then
    fail
      (Format.asprintf "trace did not contain %S; trace:\n%s" needle
         (Buffer.contents trace))

let count_substring ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let count = ref 0 in
  while !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then (
      incr count;
      index := !index + needle_length)
    else incr index
  done;
  !count

let wait_for_trace_count ~trace ~needle ~count =
  let attempts = ref 0 in
  while
    Int.compare (count_substring ~needle (Buffer.contents trace)) count < 0
    && Int.compare !attempts 100 < 0
  do
    Eio.Fiber.yield ();
    incr attempts
  done;
  if
    Int.compare (count_substring ~needle (Buffer.contents trace)) count < 0
  then
    fail
      (Format.asprintf "trace did not contain %d copies of %S; trace:\n%s"
         count needle (Buffer.contents trace))

let config () =
  match
    Nats_eio.Object_store.Config.v ~bucket:"assets" ~description:"media"
      ~ttl:Mtime.Span.(1 * s) ~max_bytes:4096L
      ~storage:Nats_eio.Object_store.Config.Memory ()
  with
  | Ok value -> value
  | Error error ->
      fail
        (Format.asprintf "%a" Nats_eio.Object_store.Error.pp_config error)

let object_name value =
  match Nats_eio.Object_store.Name.of_string value with
  | Ok value -> value
  | Error error ->
      fail
        (Format.asprintf "%a" Nats_eio.Object_store.Error.pp_name error)

let object_meta ~name ~chunk_size =
  match
    Nats_eio.Object_store.Meta.v ~name:(object_name name) ~chunk_size ()
  with
  | Ok value -> value
  | Error error ->
      fail
        (Format.asprintf "%a" Nats_eio.Object_store.Error.pp_meta error)

let uploaded_info_wire ~sid ~name =
  let payload =
    Format.asprintf
      {|{"name":"%s","bucket":"assets","nuid":"test-nuid","size":5,"chunks":3,"digest":"SHA-256=NrvlDtloQdEEQ7y2cNZVTwo0t2G-Z-ycSorSwMRMpCw=","options":{"max_chunk_size":2}}|}
      name
  in
  direct_response_wire ~sid ~bucket:"assets" ~name payload

let object_info_wire ~sid ~name ~nuid ~size ~chunks ~digest ~chunk_size =
  let payload =
    Format.asprintf
      {|{"name":"%s","bucket":"assets","nuid":"%s","size":%Ld,"chunks":%Ld,"digest":"%s","options":{"max_chunk_size":%d}}|}
      name nuid size chunks digest chunk_size
  in
  direct_response_wire ~sid ~bucket:"assets" ~name payload

let object_ordered_create_wire ~sid =
  response_wire_with_sid ~sid
    {|{"stream_name":"OBJ_assets","name":"ordered-1","config":{"deliver_policy":"all","ack_policy":"none","replay_policy":"instant"}}|}

let object_ordered_delivery_wire ~sid ~stream_sequence ~consumer_sequence
    ~pending payload =
  let reply_to =
    Format.asprintf "$JS.ACK.OBJ_assets.ordered-1.1.%Ld.%Ld.0.%Ld"
      stream_sequence consumer_sequence pending
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "$O.assets.C.test-nuid")
      ~reply_to:(Nats.Subject.literal reply_to) payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let () =
  run "nats-eio-object-store"
    [
      test "configuration and metadata validate external values" (fun () ->
          (match Nats_eio.Object_store.Config.v ~bucket:"assets" () with
          | Ok value ->
              equal string "assets"
                (Nats_eio.Object_store.Config.bucket value);
              equal (option string) None
                (Nats_eio.Object_store.Config.description value)
          | Error error ->
              fail
                (Format.asprintf "%a"
                   Nats_eio.Object_store.Error.pp_config error));
          (match Nats_eio.Object_store.Config.v ~bucket:"bad.bucket" () with
          | Ok _ -> fail "bucket validation accepted a dot"
          | Error Nats_eio.Object_store.Config.Invalid_bucket_character _ -> ()
          | Error error ->
              fail
                (Format.asprintf "unexpected config error: %a"
                   Nats_eio.Object_store.Error.pp_config error));
          (match Nats_eio.Object_store.Name.of_string "" with
          | Ok _ -> fail "object-name validation accepted an empty name"
          | Error Nats_eio.Object_store.Name.Empty_name -> ());
          let name =
            match Nats_eio.Object_store.Name.of_string "images/cat.png" with
            | Ok value -> value
            | Error error ->
                fail
                  (Format.asprintf "%a" Nats_eio.Object_store.Error.pp_name
                     error)
          in
          (match
             Nats_eio.Object_store.Meta.v ~name ~chunk_size:0 ()
           with
          | Ok _ -> fail "metadata validation accepted a zero chunk size"
          | Error (Nats_eio.Object_store.Meta.Invalid_chunk_size 0) -> ()
          | Error error ->
              fail
                (Format.asprintf "unexpected metadata error: %a"
                   Nats_eio.Object_store.Error.pp_meta error)));
      test "create and status project the object-store stream contract" (fun () ->
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
                    (Nats_eio.Object_store.create jetstream (config ())));
              yield_n 5;
              require_trace ~trace ~needle:"STREAM.CREATE.OBJ_assets";
              require_trace ~trace ~needle:"$O.assets.C.>";
              require_trace ~trace ~needle:"$O.assets.M.>";
              require_trace ~trace ~needle:"storage\\\":\\\"memory";
              require_trace ~trace ~needle:"max_bytes\\\":4096";
              require_trace ~trace ~needle:"max_age\\\":1000000000";
              require_trace ~trace ~needle:"allow_rollup_hdrs\\\":true";
              require_trace ~trace ~needle:"allow_direct\\\":true";
              Eio.Promise.resolve create_response_u
                (Ok (stream_response ~sid:1 ~bucket:"assets"));
              let value =
                match Eio.Promise.await create_result with
                | Ok value -> value
                | Error error ->
                    fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error)
              in
              let status_result, status_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve status_result_u
                    (Nats_eio.Object_store.status value));
              yield_n 5;
              Eio.Promise.resolve status_response_u
                (Ok (stream_info_response ~sid:2 ~bucket:"assets"));
              let status =
                match Eio.Promise.await status_result with
                | Ok value -> value
                | Error error ->
                    fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error)
              in
              equal string "assets"
                (Nats_eio.Object_store.Status.bucket status);
              equal (option string) (Some "media")
                (Nats_eio.Object_store.Status.description status);
              equal int64 4L (Nats_eio.Object_store.Status.messages status);
              equal int64 321L (Nats_eio.Object_store.Status.bytes status);
              equal int64 3L
                (Nats_eio.Object_store.Status.first_sequence status);
              equal int64 9L
                (Nats_eio.Object_store.Status.last_sequence status);
              equal int64 4096L
                (Option.get (Nats_eio.Object_store.Status.max_bytes status));
              Eio.Promise.resolve hold_u (Ok "done")))
      ;
      test "put preserves reader slices and commits interoperable metadata" (fun () ->
          let initial_info, initial_info_u = Eio.Promise.create () in
          let chunk_one, chunk_one_u = Eio.Promise.create () in
          let chunk_two, chunk_two_u = Eio.Promise.create () in
          let chunk_three, chunk_three_u = Eio.Promise.create () in
          let metadata, metadata_u = Eio.Promise.create () in
          let final_info, final_info_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await initial_info;
                `Await chunk_one;
                `Await chunk_two;
                `Await chunk_three;
                `Await metadata;
                `Await final_info;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                match Nats_eio.Object_store.bind jetstream ~bucket:"assets" with
                | Ok value -> value
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              let name = object_name "images/cat.png" in
              let meta =
                object_meta
                  ~name:(Nats_eio.Object_store.Name.to_string name)
                  ~chunk_size:2
              in
              let result =
                let result_p, result_u = Eio.Promise.create () in
                Eio.Fiber.fork ~sw (fun () ->
                    Eio.Promise.resolve result_u
                      (Nats_eio.Object_store.put
                         ~timeout:Mtime.Span.(1 * s) bucket meta
                         (Bytesrw.Bytes.Reader.of_string ~slice_length:3
                            "abcde")));
                wait_for_trace_count ~trace
                  ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
                Eio.Promise.resolve initial_info_u
                  (Ok (response_wire_with_sid ~sid:1 ""));
                wait_for_trace_count ~trace ~needle:"PUB $O.assets.C." ~count:1;
                Eio.Promise.resolve chunk_one_u
                  (Ok
                     (response_wire_with_sid ~sid:2
                        {|{"stream":"OBJ_assets","seq":1}|}));
                wait_for_trace_count ~trace ~needle:"PUB $O.assets.C." ~count:2;
                Eio.Promise.resolve chunk_two_u
                  (Ok
                     (response_wire_with_sid ~sid:3
                        {|{"stream":"OBJ_assets","seq":2}|}));
                wait_for_trace_count ~trace ~needle:"PUB $O.assets.C." ~count:3;
                Eio.Promise.resolve chunk_three_u
                  (Ok
                     (response_wire_with_sid ~sid:4
                        {|{"stream":"OBJ_assets","seq":3}|}));
                wait_for_trace_count ~trace ~needle:"PUB $O.assets.M." ~count:1;
                Eio.Promise.resolve metadata_u
                  (Ok
                     (response_wire_with_sid ~sid:5
                        {|{"stream":"OBJ_assets","seq":4}|}));
                wait_for_trace_count ~trace
                  ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:2;
                Eio.Promise.resolve final_info_u
                  (Ok (uploaded_info_wire ~sid:6 ~name:"images/cat.png"));
                Eio.Promise.await result_p
              in
              let info =
                match result with
                | Ok value -> value
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              equal string "images/cat.png"
                (Nats_eio.Object_store.Name.to_string
                   (Nats_eio.Object_store.Info.name info));
              equal int64 5L (Nats_eio.Object_store.Info.size info);
              equal int64 3L (Nats_eio.Object_store.Info.chunks info);
              equal string
                "SHA-256=NrvlDtloQdEEQ7y2cNZVTwo0t2G-Z-ycSorSwMRMpCw="
                (Nats_eio.Object_store.Info.digest info);
              require_trace ~trace ~needle:"PUB $O.assets.C.";
              require_trace ~trace ~needle:"PUB $O.assets.M.";
              require_trace ~trace
                ~needle:"PUB $O.assets.M.aW1hZ2VzL2NhdC5wbmc=";
              require_trace ~trace ~needle:"size\\\":5";
              require_trace ~trace ~needle:"chunks\\\":3";
              require_trace ~trace ~needle:"digest\\\":\\\"SHA-256=Nrvl";
              require_trace ~trace ~needle:"Nats-Rollup"))
      ;
      test "get streams ordered chunks and verifies the digest" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let create_response, create_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let third_delivery, third_delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await create_response;
                `Await first_delivery;
                `Await second_delivery;
                `Await third_delivery;
                `Await delete_response;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                match Nats_eio.Object_store.bind jetstream ~bucket:"assets" with
                | Ok value -> value
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              let buffer = Buffer.create 0 in
              let writer = Bytesrw.Bytes.Writer.of_buffer buffer in
              let result_p, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.get
                       ~timeout:Mtime.Span.(1 * s) bucket
                       (object_name "images/cat.png") writer));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve info_response_u
                (Ok
                   (direct_response_wire ~sid:1 ~bucket:"assets"
                      ~name:"images/cat.png"
                      {|{"name":"images/cat.png","bucket":"assets","nuid":"test-nuid","size":5,"chunks":3,"digest":"SHA-256=NrvlDtloQdEEQ7y2cNZVTwo0t2G-Z-ycSorSwMRMpCw=","options":{"max_chunk_size":2}}|}));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.CREATE.OBJ_assets" ~count:1;
              Eio.Promise.resolve create_response_u
                (Ok (object_ordered_create_wire ~sid:2));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (object_ordered_delivery_wire ~sid:3 ~stream_sequence:1L
                      ~consumer_sequence:1L ~pending:2L "ab"));
              yield_n 5;
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (object_ordered_delivery_wire ~sid:3 ~stream_sequence:2L
                      ~consumer_sequence:2L ~pending:1L "cd"));
              yield_n 5;
              Eio.Promise.resolve third_delivery_u
                (Ok
                   (object_ordered_delivery_wire ~sid:3 ~stream_sequence:3L
                      ~consumer_sequence:3L ~pending:0L "e"));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.DELETE.OBJ_assets" ~count:1;
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire_with_sid ~sid:4 "{}"));
              let info =
                match Eio.Promise.await result_p with
                | Ok value -> value
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              equal string "abcde" (Buffer.contents buffer);
              equal int64 5L (Nats_eio.Object_store.Info.size info);
              equal int64 3L (Nats_eio.Object_store.Info.chunks info);
              require_trace ~trace ~needle:"$O.assets.C.test-nuid";
              require_trace ~trace
                ~needle:"filter_subject\\\":\\\"$O.assets.C.test-nuid\\\""))
      ;
      test "replacement commits new metadata before purging old chunks" (fun () ->
          let initial_info, initial_info_u = Eio.Promise.create () in
          let chunk, chunk_u = Eio.Promise.create () in
          let metadata, metadata_u = Eio.Promise.create () in
          let final_info, final_info_u = Eio.Promise.create () in
          let purge, purge_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await initial_info;
                `Await chunk;
                `Await metadata;
                `Await final_info;
                `Await purge;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let meta = object_meta ~name:"images/cat.png" ~chunk_size:2 in
              let result_p, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.put ~timeout:Mtime.Span.(1 * s)
                       bucket meta
                       (Bytesrw.Bytes.Reader.of_string ~slice_length:2 "xy")));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve initial_info_u
                (Ok
                   (object_info_wire ~sid:1 ~name:"images/cat.png"
                      ~nuid:"old-nuid" ~size:3L ~chunks:1L
                      ~digest:"SHA-256=old" ~chunk_size:2));
              wait_for_trace_count ~trace ~needle:"PUB $O.assets.C." ~count:1;
              Eio.Promise.resolve chunk_u
                (Ok
                   (response_wire_with_sid ~sid:2
                      {|{"stream":"OBJ_assets","seq":10}|}));
              wait_for_trace_count ~trace ~needle:"PUB $O.assets.M." ~count:1;
              Eio.Promise.resolve metadata_u
                (Ok
                   (response_wire_with_sid ~sid:3
                      {|{"stream":"OBJ_assets","seq":11}|}));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:2;
              Eio.Promise.resolve final_info_u
                (Ok
                   (object_info_wire ~sid:4 ~name:"images/cat.png"
                      ~nuid:"new-nuid" ~size:2L ~chunks:1L
                      ~digest:"SHA-256=dppObQADGJx-lsXZt-gQoNEcOhKDJSfslLD4bSd_Uco="
                      ~chunk_size:2));
              wait_for_trace_count ~trace
                ~needle:"STREAM.PURGE.OBJ_assets" ~count:1;
              require_trace ~trace ~needle:"$O.assets.C.old-nuid";
              Eio.Promise.resolve purge_u
                (Ok (response_wire_with_sid ~sid:5 {|{"purged":1}|}));
              let info =
                match Eio.Promise.await result_p with
                | Ok value -> value
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              equal string "new-nuid"
                (Nats_eio.Object_store.Info.nuid info);
              require_trace ~trace ~needle:"Nats-Rollup"))
      ;
      test "delete publishes a tombstone before purging object chunks" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let metadata, metadata_u = Eio.Promise.create () in
          let purge, purge_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await metadata;
                `Await purge;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let result_p, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.delete ~timeout:Mtime.Span.(1 * s)
                       bucket (object_name "images/cat.png")));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve info_response_u
                (Ok
                   (object_info_wire ~sid:1 ~name:"images/cat.png"
                      ~nuid:"old-nuid" ~size:5L ~chunks:3L
                      ~digest:"SHA-256=NrvlDtloQdEEQ7y2cNZVTwo0t2G-Z-ycSorSwMRMpCw="
                      ~chunk_size:2));
              wait_for_trace_count ~trace ~needle:"PUB $O.assets.M." ~count:1;
              require_trace ~trace ~needle:"deleted\\\":true";
              Eio.Promise.resolve metadata_u
                (Ok
                   (response_wire_with_sid ~sid:2
                      {|{"stream":"OBJ_assets","seq":12}|}));
              wait_for_trace_count ~trace
                ~needle:"STREAM.PURGE.OBJ_assets" ~count:1;
              require_trace ~trace ~needle:"$O.assets.C.old-nuid";
              Eio.Promise.resolve purge_u
                (Ok (response_wire_with_sid ~sid:3 {|{"purged":3}|}));
              match Eio.Promise.await result_p with
              | Ok () -> ()
              | Error error ->
                  fail
                    (Format.asprintf "%a\ntrace:\n%s"
                       Nats_eio.Object_store.Error.pp error
                       (Buffer.contents trace))))
    ]
