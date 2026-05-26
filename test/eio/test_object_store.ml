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

let stream_info_response ~sid ~bucket ?(sealed = false) () =
  let sealed_field = if sealed then ",\"sealed\":true" else "" in
  let payload =
    Format.asprintf
      {|{"config":{"name":"OBJ_%s","description":"media","subjects":["$O.%s.C.>","$O.%s.M.>"],"storage":"memory","retention":"limits","discard":"new","max_bytes":4096,"max_age":1000000000,"allow_rollup_hdrs":true,"allow_direct":true%s},"state":{"messages":4,"bytes":321,"first_seq":3,"last_seq":9,"consumer_count":0}}|}
      bucket bucket bucket sealed_field
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

let trace_json_string ~trace ~field =
  let value = Buffer.contents trace in
  let marker = field ^ "\\\":\\\"" in
  let marker_length = String.length marker in
  let limit = String.length value - marker_length in
  let position = ref 0 in
  let found = ref None in
  while Option.is_none !found && !position <= limit do
    if String.equal (String.sub value !position marker_length) marker then
      found := Some (!position + marker_length)
    else incr position
  done;
  match !found with
  | None ->
      fail
        (Format.asprintf "trace did not contain JSON field %S; trace:\n%s" field
           value)
  | Some start ->
      let finish = ref None in
      let cursor = ref start in
      while Option.is_none !finish && !cursor < String.length value - 1 do
        if
          Char.equal (String.get value !cursor) '\\'
          && Char.equal (String.get value (!cursor + 1)) '"'
        then finish := Some !cursor
        else incr cursor
      done;
      (match !finish with
      | None ->
          fail
            (Format.asprintf "trace field %S was not terminated; trace:\n%s"
               field value)
      | Some finish -> String.sub value start (finish - start))

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

let object_meta_description ~description ~name ~chunk_size =
  match
    Nats_eio.Object_store.Meta.v ~name:(object_name name) ~description ~chunk_size
      ()
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

let object_link_info_wire ~sid ~name ~target =
  let payload =
    Format.asprintf
      {|{"name":"%s","bucket":"assets","nuid":"link-nuid","size":0,"chunks":0,"options":{"link":{"bucket":"assets","name":"%s"}}}|}
      name target
  in
  direct_response_wire ~sid ~bucket:"assets" ~name payload

let object_ordered_create_wire ~sid =
  response_wire_with_sid ~sid
    {|{"stream_name":"OBJ_assets","name":"ordered-1","config":{"deliver_policy":"all","ack_policy":"none","replay_policy":"instant"}}|}

let object_ordered_delivery_wire ~sid ~stream_sequence ~consumer_sequence
    ~pending ?(nuid = "test-nuid") payload =
  let reply_to =
    Format.asprintf "$JS.ACK.OBJ_assets.ordered-1.1.%Ld.%Ld.0.%Ld"
      stream_sequence consumer_sequence pending
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal ("$O.assets.C." ^ nuid))
      ~reply_to:(Nats.Subject.literal reply_to) payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let object_push_create_wire ~sid ~subject ~pending =
  let payload =
    Format.asprintf
      {|{"stream_name":"OBJ_assets","name":"watch-1","config":{"deliver_subject":"%s","deliver_policy":"last_per_subject","ack_policy":"none","replay_policy":"instant","idle_heartbeat":5000000000,"flow_control":true},"num_pending":%Ld}|}
      subject pending
  in
  response_wire_with_sid ~sid payload

let object_watch_delivery_wire ~sid ~stream_sequence ~consumer_sequence
    ~pending ~name ~nuid ~size ~chunks ~digest ?(deleted = false) () =
  let encoded_name =
    Base64.encode_string ~alphabet:Base64.uri_safe_alphabet name
  in
  let reply_to =
    Format.asprintf "$JS.ACK.OBJ_assets.watch-1.1.%Ld.%Ld.0.%Ld"
      stream_sequence consumer_sequence pending
  in
  let payload =
    Format.asprintf
      {|{"name":"%s","bucket":"assets","nuid":"%s","size":%Ld,"mtime":"2026-08-12T12:00:00.000000000Z","chunks":%Ld,"digest":"%s","deleted":%s,"options":{"max_chunk_size":2}}|}
      name nuid size chunks digest (if deleted then "true" else "false")
  in
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal ("$O.assets.M." ^ encoded_name))
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
                (Ok (stream_info_response ~sid:2 ~bucket:"assets" ()));
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
      test "metadata update preserves content identity without uploading chunks" (fun () ->
          let initial_info, initial_info_u = Eio.Promise.create () in
          let metadata, metadata_u = Eio.Promise.create () in
          let final_info, final_info_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await initial_info;
                `Await metadata;
                `Await final_info;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let meta =
                object_meta_description ~description:"updated"
                  ~name:"images/cat.png" ~chunk_size:99
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.update bucket meta));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve initial_info_u
                (Ok
                   (object_info_wire ~sid:1 ~name:"images/cat.png"
                      ~nuid:"old-nuid" ~size:5L ~chunks:3L ~digest:"SHA-256=old"
                      ~chunk_size:2));
              wait_for_trace_count ~trace ~needle:"PUB $O.assets.M." ~count:1;
              require_trace ~trace ~needle:"description\\\":\\\"updated";
              require_trace ~trace ~needle:"nuid\\\":\\\"old-nuid";
              require_trace ~trace ~needle:"size\\\":5";
              require_trace ~trace ~needle:"chunks\\\":3";
              require_trace ~trace ~needle:"max_chunk_size\\\":2";
              Eio.Promise.resolve metadata_u
                (Ok (response_wire_with_sid ~sid:2 {|{"stream":"OBJ_assets","seq":20}|}));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:2;
              Eio.Promise.resolve final_info_u
                (Ok
                   (object_info_wire ~sid:3 ~name:"images/cat.png"
                      ~nuid:"old-nuid" ~size:5L ~chunks:3L ~digest:"SHA-256=old"
                      ~chunk_size:2));
              let info =
                match Eio.Promise.await result with
                | Ok info -> info
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              equal string "old-nuid" (Nats_eio.Object_store.Info.nuid info);
              if contains_substring ~needle:"PUB $O.assets.C." (Buffer.contents trace)
              then fail "metadata update uploaded a chunk";
              if contains_substring ~needle:"STREAM.PURGE.OBJ_assets"
                   (Buffer.contents trace)
              then fail "metadata update purged content"))
      ;
      test "metadata update can rename before purging the old subject" (fun () ->
          let old_info, old_info_u = Eio.Promise.create () in
          let new_info, new_info_u = Eio.Promise.create () in
          let metadata, metadata_u = Eio.Promise.create () in
          let final_info, final_info_u = Eio.Promise.create () in
          let purge, purge_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await old_info;
                `Await new_info;
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
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.update
                       ~name:(object_name "images/cat.png") bucket
                       (object_meta ~name:"images/new.png" ~chunk_size:99)));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve old_info_u
                (Ok
                   (object_info_wire ~sid:1 ~name:"images/cat.png"
                      ~nuid:"old-nuid" ~size:5L ~chunks:3L ~digest:"SHA-256=old"
                      ~chunk_size:2));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:2;
              Eio.Promise.resolve new_info_u
                (Ok (response_wire_with_sid ~sid:2 ""));
              wait_for_trace_count ~trace ~needle:"PUB $O.assets.M." ~count:1;
              require_trace ~trace
                ~needle:"PUB $O.assets.M.aW1hZ2VzL25ldy5wbmc=";
              Eio.Promise.resolve metadata_u
                (Ok (response_wire_with_sid ~sid:3 {|{"stream":"OBJ_assets","seq":30}|}));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:3;
              Eio.Promise.resolve final_info_u
                (Ok
                   (object_info_wire ~sid:4 ~name:"images/new.png"
                      ~nuid:"old-nuid" ~size:5L ~chunks:3L ~digest:"SHA-256=old"
                      ~chunk_size:2));
              wait_for_trace_count ~trace
                ~needle:"STREAM.PURGE.OBJ_assets" ~count:1;
              require_trace ~trace
                ~needle:"$O.assets.M.aW1hZ2VzL2NhdC5wbmc=";
              Eio.Promise.resolve purge_u
                (Ok (response_wire_with_sid ~sid:5 {|{"purged":1}|}));
              match Eio.Promise.await result with
              | Ok info ->
                  equal string "images/new.png"
                    (Nats_eio.Object_store.Name.to_string
                       (Nats_eio.Object_store.Info.name info))
              | Error error ->
                  fail
                    (Format.asprintf "%a\ntrace:\n%s"
                       Nats_eio.Object_store.Error.pp error
                       (Buffer.contents trace))))
      ;
      test "put_link commits a metadata-only object link" (fun () ->
          let target_info, target_info_u = Eio.Promise.create () in
          let missing_info, missing_info_u = Eio.Promise.create () in
          let metadata, metadata_u = Eio.Promise.create () in
          let final_info, final_info_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await missing_info;
                `Await target_info;
                `Await metadata;
                `Await final_info;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let link =
                match
                  Nats_eio.Object_store.Link.v ~bucket:"assets"
                    ~name:(object_name "images/base.png") ()
                with
                | Ok link -> link
                | Error error ->
                    fail
                      (Format.asprintf "%a"
                         Nats_eio.Object_store.Error.pp_config error)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.put_link bucket
                       (object_meta ~name:"images/cat.png" ~chunk_size:2)
                       link));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve missing_info_u
                (Ok (response_wire_with_sid ~sid:1 ""));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:2;
              Eio.Promise.resolve target_info_u
                (Ok
                   (object_info_wire ~sid:2 ~name:"images/base.png"
                      ~nuid:"base-nuid" ~size:2L ~chunks:1L ~digest:"SHA-256=base"
                      ~chunk_size:2));
              wait_for_trace_count ~trace ~needle:"PUB $O.assets.M." ~count:1;
              require_trace ~trace
                ~needle:"link\\\":{\\\"bucket\\\":\\\"assets\\\",\\\"name\\\":\\\"images/base.png\\\"";
              require_trace ~trace ~needle:"chunks\\\":0";
              if contains_substring ~needle:"max_chunk_size\\\":131072"
                   (Buffer.contents trace)
              then fail "put_link invented a chunk-size option";
              Eio.Promise.resolve metadata_u
                (Ok (response_wire_with_sid ~sid:3 {|{"stream":"OBJ_assets","seq":21}|}));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:3;
              Eio.Promise.resolve final_info_u
                (Ok (object_link_info_wire ~sid:4 ~name:"images/cat.png"
                       ~target:"images/base.png"));
              let info =
                match Eio.Promise.await result with
                | Ok info -> info
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              match Nats_eio.Object_store.Info.link info with
              | None -> fail "link metadata was lost"
              | Some link ->
                  equal string "assets"
                    (Nats_eio.Object_store.Link.bucket link);
                  equal (option string) (Some "images/base.png")
                    (Option.map Nats_eio.Object_store.Name.to_string
                       (Nats_eio.Object_store.Link.name link));
                  if contains_substring ~needle:"PUB $O.assets.C."
                       (Buffer.contents trace)
                  then fail "put_link uploaded content"))
      ;
      test "put_link refuses to replace an ordinary object" (fun () ->
          let existing_info, existing_info_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await existing_info; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let link =
                match
                  Nats_eio.Object_store.Link.v ~bucket:"assets"
                    ~name:(object_name "images/base.png") ()
                with
                | Ok link -> link
                | Error error ->
                    fail
                      (Format.asprintf "%a"
                         Nats_eio.Object_store.Error.pp_config error)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.put_link bucket
                       (object_meta ~name:"images/cat.png" ~chunk_size:2)
                       link));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve existing_info_u
                (Ok
                   (object_info_wire ~sid:1 ~name:"images/cat.png"
                      ~nuid:"content-nuid" ~size:2L ~chunks:1L
                      ~digest:"SHA-256=content" ~chunk_size:2));
              (match Eio.Promise.await result with
              | Error (Nats_eio.Object_store.Error.Object_already_exists info) ->
                  equal string "content-nuid"
                    (Nats_eio.Object_store.Info.nuid info)
              | Error error ->
                  fail
                    (Format.asprintf "unexpected error: %a"
                       Nats_eio.Object_store.Error.pp error)
              | Ok _ -> fail "put_link replaced an ordinary object");
              if contains_substring ~needle:"PUB $O.assets.M."
                   (Buffer.contents trace)
              then fail "put_link published after rejecting an existing object";
              Eio.Promise.resolve hold_u (Error End_of_file)))
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
      test "get follows object links before streaming target chunks" (fun () ->
          let alias_info, alias_info_u = Eio.Promise.create () in
          let target_info, target_info_u = Eio.Promise.create () in
          let create_response, create_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await alias_info;
                `Await target_info;
                `Await create_response;
                `Await delivery;
                `Await delete_response;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let buffer = Buffer.create 0 in
              let writer = Bytesrw.Bytes.Writer.of_buffer buffer in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.get bucket
                       (object_name "images/cat.png") writer));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:1;
              Eio.Promise.resolve alias_info_u
                (Ok
                   (object_link_info_wire ~sid:1 ~name:"images/cat.png"
                      ~target:"images/base.png"));
              wait_for_trace_count ~trace
                ~needle:"PUB $JS.API.DIRECT.GET.OBJ_assets" ~count:2;
              Eio.Promise.resolve target_info_u
                (Ok
                   (object_info_wire ~sid:2 ~name:"images/base.png"
                      ~nuid:"target-nuid" ~size:2L ~chunks:1L
                      ~digest:"SHA-256=dppObQADGJx-lsXZt-gQoNEcOhKDJSfslLD4bSd_Uco="
                      ~chunk_size:2));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.CREATE.OBJ_assets" ~count:1;
              Eio.Promise.resolve create_response_u
                (Ok (object_ordered_create_wire ~sid:3));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok
                   (object_ordered_delivery_wire ~sid:4 ~nuid:"target-nuid"
                      ~stream_sequence:1L ~consumer_sequence:1L ~pending:0L
                      "xy"));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.DELETE.OBJ_assets" ~count:1;
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire_with_sid ~sid:5 "{}"));
              let info =
                match Eio.Promise.await result with
                | Ok info -> info
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              equal string "xy" (Buffer.contents buffer);
              equal string "target-nuid"
                (Nats_eio.Object_store.Info.nuid info);
              require_trace ~trace ~needle:"$O.assets.C.target-nuid"))
      ;
      test "watch drains retained metadata and emits an initial marker" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
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
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let watch_result, watch_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve watch_result_u
                    (Nats_eio.Object_store.Watch.v ~sw ~ignore_deletes:true
                       bucket));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.CREATE.OBJ_assets" ~count:1;
              let subject =
                trace_json_string ~trace ~field:"deliver_subject"
              in
              Eio.Promise.resolve create_response_u
                (Ok (object_push_create_wire ~sid:2 ~subject ~pending:2L));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.INFO.OBJ_assets.watch-1" ~count:1;
              Eio.Promise.resolve info_response_u
                (Ok (object_push_create_wire ~sid:3 ~subject ~pending:2L));
              let watch =
                match Eio.Promise.await watch_result with
                | Ok watch -> watch
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Object_store.Watch.next watch));
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (object_watch_delivery_wire ~sid:1 ~stream_sequence:1L
                      ~consumer_sequence:1L ~pending:1L ~name:"images/cat.png"
                      ~nuid:"cat-nuid" ~size:1L ~chunks:1L ~digest:"SHA-256=cat"
                      ()));
              let first =
                match Eio.Promise.await first_result with
                | Ok event -> event
                | Error error ->
                    fail
                      (Format.asprintf "%a"
                         Nats_eio.Object_store.Error.pp error)
              in
              (match first with
              | Nats_eio.Object_store.Watch.Initial_done ->
                  fail "watch emitted its marker before retained metadata"
              | Nats_eio.Object_store.Watch.Info info ->
                  equal string "images/cat.png"
                    (Nats_eio.Object_store.Name.to_string
                       (Nats_eio.Object_store.Info.name info)));
              let marker_result, marker_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve marker_result_u
                    (Nats_eio.Object_store.Watch.next watch));
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (object_watch_delivery_wire ~sid:1 ~stream_sequence:2L
                      ~consumer_sequence:2L ~pending:0L ~name:"images/old.png"
                      ~nuid:"old-nuid" ~size:0L ~chunks:0L ~digest:""
                      ~deleted:true ()));
              (match Eio.Promise.await marker_result with
              | Ok Nats_eio.Object_store.Watch.Initial_done -> ()
              | Ok _ -> fail "watch did not return its initial marker"
              | Error error ->
                  fail
                    (Format.asprintf "%a"
                       Nats_eio.Object_store.Error.pp error));
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Object_store.Watch.close watch));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.DELETE.OBJ_assets.watch-1" ~count:1;
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire_with_sid ~sid:4 "{}"));
              (match Eio.Promise.await close_result with
              | Ok () -> ()
              | Error error ->
                  fail
                    (Format.asprintf "%a"
                       Nats_eio.Object_store.Error.pp error));
              Eio.Promise.resolve hold_u (Error End_of_file)))
      ;
      test "list uses the watch snapshot boundary" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await delivery;
                `Await second_delivery;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.list bucket));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.CREATE.OBJ_assets" ~count:1;
              let subject =
                trace_json_string ~trace ~field:"deliver_subject"
              in
              Eio.Promise.resolve create_response_u
                (Ok (object_push_create_wire ~sid:2 ~subject ~pending:2L));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.INFO.OBJ_assets.watch-1" ~count:1;
              Eio.Promise.resolve info_response_u
                (Ok (object_push_create_wire ~sid:3 ~subject ~pending:2L));
              Eio.Promise.resolve delivery_u
                (Ok
                   (object_watch_delivery_wire ~sid:1 ~stream_sequence:1L
                      ~consumer_sequence:1L ~pending:1L ~name:"images/cat.png"
                      ~nuid:"cat-nuid" ~size:1L ~chunks:1L ~digest:"SHA-256=cat"
                      ()));
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (object_watch_delivery_wire ~sid:1 ~stream_sequence:2L
                      ~consumer_sequence:2L ~pending:0L ~name:"images/dog.png"
                      ~nuid:"dog-nuid" ~size:1L ~chunks:1L ~digest:"SHA-256=dog"
                      ()));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.DELETE.OBJ_assets.watch-1" ~count:1;
              Eio.Promise.resolve delete_response_u
                (Ok (response_wire_with_sid ~sid:4 "{}"));
              let infos =
                match Eio.Promise.await result with
                | Ok infos -> infos
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              (match infos with
              | [ first; second ] ->
                  equal string "images/cat.png"
                    (Nats_eio.Object_store.Name.to_string
                       (Nats_eio.Object_store.Info.name first));
                  equal string "images/dog.png"
                    (Nats_eio.Object_store.Name.to_string
                       (Nats_eio.Object_store.Info.name second))
              | _ -> fail "list returned the wrong snapshot");
              Eio.Promise.resolve hold_u (Error End_of_file)))
      ;
      test "seal updates the backing stream and reports sealed status" (fun () ->
          let first_info, first_info_u = Eio.Promise.create () in
          let second_info, second_info_u = Eio.Promise.create () in
          let update_response, update_response_u = Eio.Promise.create () in
          let third_info, third_info_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_info;
                `Await second_info;
                `Await update_response;
                `Await third_info;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let bucket =
                expect_object_ok
                  (Nats_eio.Object_store.bind jetstream ~bucket:"assets")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.seal bucket));
              wait_for_trace_count ~trace
                ~needle:"STREAM.INFO.OBJ_assets" ~count:1;
              Eio.Promise.resolve first_info_u
                (Ok (stream_info_response ~sid:1 ~bucket:"assets" ()));
              wait_for_trace_count ~trace
                ~needle:"STREAM.INFO.OBJ_assets" ~count:2;
              Eio.Promise.resolve second_info_u
                (Ok (stream_info_response ~sid:2 ~bucket:"assets" ()));
              wait_for_trace_count ~trace
                ~needle:"STREAM.UPDATE.OBJ_assets" ~count:1;
              require_trace ~trace ~needle:"sealed\\\":true";
              Eio.Promise.resolve update_response_u
                (Ok (stream_info_response ~sid:3 ~bucket:"assets" ~sealed:true ()));
              wait_for_trace_count ~trace
                ~needle:"STREAM.INFO.OBJ_assets" ~count:3;
              Eio.Promise.resolve third_info_u
                (Ok (stream_info_response ~sid:4 ~bucket:"assets" ~sealed:true ()));
              let status =
                match Eio.Promise.await result with
                | Ok status -> status
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              equal bool true (Nats_eio.Object_store.Status.sealed status)))
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
