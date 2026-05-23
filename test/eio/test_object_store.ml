open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\"," ^
  "\"proto\":1,\"max_payload\":1048576,\"headers\":true," ^
  "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

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

let direct_message_wire ~sid ~stream ~subject ~sequence ~timestamp payload =
  let headers =
    match
      Nats.Header.of_list
        [
          ("JSStream", stream);
          ("JSSequence", Int64.to_string sequence);
          ("JSTimeStamp", timestamp);
          ("JSSubject", subject);
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

let object_consumer_info_wire ~sid ~name ~num_pending =
  let payload =
    Format.asprintf
      "{\"stream_name\":\"OBJ_docs\",\"name\":\"%s\",\"config\":{\"deliver_policy\":\"all\",\"ack_policy\":\"none\",\"replay_policy\":\"instant\"},\"num_pending\":%Ld}"
      name num_pending
  in
  response_wire ~sid payload

let object_delivery_wire ~sid ~consumer ~nuid payload =
  let reply_to =
    Format.asprintf "$JS.ACK.OBJ_docs.%s.1.1.1.0.0" consumer
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal ("$O.docs.C." ^ nuid))
      ~reply_to:(Nats.Subject.literal reply_to) payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let object_metadata_delivery_wire ~sid ~consumer ~name ~num_pending payload =
  let reply_to =
    Format.asprintf "$JS.ACK.OBJ_docs.%s.1.1.1.1710000000000000000.%Ld"
      consumer num_pending
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal ("$O.docs.M." ^
        (if String.equal name "hello" then "aGVsbG8=" else name)))
      ~reply_to:(Nats.Subject.literal reply_to) payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let with_connection ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "object-store-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "object-store-network" in
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
    match
      Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ]
    with
    | Ok value -> value
    | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)
  in
  f ~sw ~trace connection

let yield_n count =
  let count = ref count in
  while !count > 0 do
    Eio.Fiber.yield ();
    decr count
  done

let expect_config = function
  | Ok value -> value
  | Error error ->
      fail
        (Format.asprintf "%a" Nats_eio.Object_store.Error.pp_config error)

let expect_meta = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp_meta error)

let expect_store = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error)

let expect_jetstream = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error)

let object_info_json ?(digest = "") ?(deleted = false) ?(headers = "{}")
    ~name ~bucket ~nuid ~size ~chunks () =
  Format.asprintf
    "{\"name\":%S,\"description\":\"\",\"headers\":%s,\"metadata\":{},\"options\":{\"max_chunk_size\":131072},\"bucket\":%S,\"nuid\":%S,\"size\":%Ld,\"mtime\":\"\",\"chunks\":%Ld,\"digest\":%S,\"deleted\":%b}"
    name headers bucket nuid size chunks digest deleted

let () =
  run "nats-eio-object-store"
    [
      test "config and metadata retain caller fields" (fun () ->
          let config =
            expect_config
              (Nats_eio.Object_store.Config.v ~bucket:"docs"
                 ~description:"uploaded documents" ~ttl:Mtime.Span.(5 * s)
                 ~max_bytes:10_000L
                 ~storage:Nats_eio.Object_store.Config.Memory ())
          in
          equal string "docs" (Nats_eio.Object_store.Config.bucket config);
          equal (option string) (Some "uploaded documents")
            (Nats_eio.Object_store.Config.description config);
          equal int64 5_000_000_000L
            (Option.get
               (Option.map Mtime.Span.to_uint64_ns
                  (Nats_eio.Object_store.Config.ttl config)));
          equal int64 10_000L
            (Option.get (Nats_eio.Object_store.Config.max_bytes config));
          (match Nats_eio.Object_store.Config.storage config with
          | Nats_eio.Object_store.Config.Memory -> ()
          | Nats_eio.Object_store.Config.File -> fail "storage changed");
          let headers =
            match Nats.Header.of_list [ ("X-Tag", "a"); ("X-Tag", "b") ] with
            | Ok value -> value
            | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)
          in
          let meta =
            expect_meta
              (Nats_eio.Object_store.Meta.v ~description:"report" ~headers
                 ~attributes:[ ("team", "infra") ] ~chunk_size:4096 ())
          in
          equal string "report"
            (Nats_eio.Object_store.Meta.description meta);
          equal int 2
               (List.length
               (Nats.Header.to_list (Nats_eio.Object_store.Meta.headers meta)));
          equal int 4096
            (Option.get (Nats_eio.Object_store.Meta.chunk_size meta)));
      test "get_info decodes padded metadata subjects and headers" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let subject = "$O.docs.M.Z3JlZXRpbmc=" in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.get_info store ~name:"greeting"));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (direct_message_wire ~sid:1 ~stream:"OBJ_docs" ~subject
                      ~sequence:4L ~timestamp:"2026-08-11T12:00:00.000000000Z"
                      (object_info_json ~name:"greeting" ~bucket:"docs"
                         ~nuid:"0123456789012345678901" ~size:5L ~chunks:1L
                         ~headers:"{\"X-Tag\":[\"a\",\"b\"]}"
                         ~digest:"SHA-256=LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" ())));
              let info = expect_store (Eio.Promise.await result) in
              equal string "greeting"
                (Nats_eio.Object_store.Info.name info);
              equal string "docs" (Nats_eio.Object_store.Info.bucket info);
              equal int64 5L (Nats_eio.Object_store.Info.size info);
              equal int64 1L (Nats_eio.Object_store.Info.chunks info);
              equal string "2026-08-11T12:00:00.000000000Z"
                (Nats_eio.Object_store.Info.modified info);
              let headers =
                Nats_eio.Object_store.Info.meta info
                |> Nats_eio.Object_store.Meta.headers
                |> Nats.Header.to_list
              in
              equal int 2 (List.length headers);
              match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error)));
      test "empty put publishes metadata only after source EOF" (fun () ->
          let missing, missing_u = Eio.Promise.create () in
          let publish, publish_u = Eio.Promise.create () in
          let direct, direct_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await missing;
                `Await publish;
                `Await direct;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.put_string store ~name:"empty" ""));
              yield_n 5;
              Eio.Promise.resolve missing_u (Ok (response_wire ~sid:1 ""));
              yield_n 5;
              Eio.Promise.resolve publish_u
                (Ok (response_wire ~sid:2 {|{"stream":"OBJ_docs","seq":1}|}));
              yield_n 5;
              Eio.Promise.resolve direct_u
                (Ok
                   (direct_message_wire ~sid:3 ~stream:"OBJ_docs"
                      ~subject:"$O.docs.M.ZW1wdHk=" ~sequence:1L
                      ~timestamp:"2026-08-11T12:00:00.000000000Z"
                      (object_info_json ~name:"empty" ~bucket:"docs"
                         ~nuid:"0123456789012345678901" ~size:0L ~chunks:0L ())));
              let info = expect_store (Eio.Promise.await result) in
              equal string "empty" (Nats_eio.Object_store.Info.name info);
              equal int64 0L (Nats_eio.Object_store.Info.size info);
              equal int64 0L (Nats_eio.Object_store.Info.chunks info);
              (match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error))));
      test "put acknowledges chunks before publishing metadata" (fun () ->
          let missing, missing_u = Eio.Promise.create () in
          let chunk, chunk_u = Eio.Promise.create () in
          let publish, publish_u = Eio.Promise.create () in
          let direct, direct_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await missing;
                `Await chunk;
                `Await publish;
                `Await direct;
                `Await hold;
              ]
            (fun ~sw connection ->
              let source = Eio_mock.Flow.make "upload-source" in
              Eio_mock.Flow.on_read source
                [ `Return "hello"; `Raise End_of_file ];
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.put store ~name:"hello" ~source
                       ()));
              yield_n 5;
              Eio.Promise.resolve missing_u (Ok (response_wire ~sid:1 ""));
              yield_n 5;
              Eio.Promise.resolve chunk_u
                (Ok (response_wire ~sid:2 {|{"stream":"OBJ_docs","seq":1}|}));
              yield_n 5;
              Eio.Promise.resolve publish_u
                (Ok (response_wire ~sid:3 {|{"stream":"OBJ_docs","seq":2}|}));
              yield_n 5;
              Eio.Promise.resolve direct_u
                (Ok
                   (direct_message_wire ~sid:4 ~stream:"OBJ_docs"
                      ~subject:"$O.docs.M.aGVsbG8=" ~sequence:2L
                      ~timestamp:"2026-08-11T12:00:00.000000000Z"
                      (object_info_json ~name:"hello" ~bucket:"docs"
                         ~nuid:"0123456789012345678901" ~size:5L ~chunks:1L
                         ~digest:"SHA-256=LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" ())));
              let info = expect_store (Eio.Promise.await result) in
              equal string "hello" (Nats_eio.Object_store.Info.name info);
              equal int64 5L (Nats_eio.Object_store.Info.size info);
              equal string
                "SHA-256=LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="
                (Nats_eio.Object_store.Info.digest info);
              (match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error))));
      test "get streams the advertised chunks and verifies the digest" (fun () ->
          let direct, direct_u = Eio.Promise.create () in
          let create, create_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let delete, delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await direct;
                `Await create;
                `Await delivery;
                `Await delete;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let buffer = Buffer.create 16 in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.get ~sw store ~name:"hello"
                       ~sink:(Eio.Flow.buffer_sink buffer)));
              yield_n 5;
              Eio.Promise.resolve direct_u
                (Ok
                   (direct_message_wire ~sid:1 ~stream:"OBJ_docs"
                      ~subject:"$O.docs.M.aGVsbG8=" ~sequence:2L
                      ~timestamp:"2026-08-11T12:00:00.000000000Z"
                      (object_info_json ~name:"hello" ~bucket:"docs"
                         ~nuid:"0123456789012345678901" ~size:5L ~chunks:1L
                         ~digest:"SHA-256=LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" ())));
              yield_n 5;
              Eio.Promise.resolve create_u
                (Ok
                   (object_consumer_info_wire ~sid:2 ~name:"ordered-1"
                      ~num_pending:0L));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok
                   (object_delivery_wire ~sid:3 ~consumer:"ordered-1"
                      ~nuid:"0123456789012345678901" "hello"));
              yield_n 5;
              Eio.Promise.resolve delete_u (Ok (response_wire ~sid:4 "{}"));
              let info = expect_store (Eio.Promise.await result) in
              equal string "hello" (Buffer.contents buffer);
              equal string "hello" (Nats_eio.Object_store.Info.name info);
              (match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error))));
      test "delete publishes a tombstone and purges old chunks" (fun () ->
          let direct, direct_u = Eio.Promise.create () in
          let publish, publish_u = Eio.Promise.create () in
          let purge, purge_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await direct;
                `Await publish;
                `Await purge;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.delete store ~name:"hello"));
              (* The operation is synchronous; the enclosing switch is kept
                 alive by the mock connection while the response promises are
                 resolved below. *)
              yield_n 5;
              Eio.Promise.resolve direct_u
                (Ok
                   (direct_message_wire ~sid:1 ~stream:"OBJ_docs"
                      ~subject:"$O.docs.M.aGVsbG8=" ~sequence:2L
                      ~timestamp:"2026-08-11T12:00:00.000000000Z"
                      (object_info_json ~name:"hello" ~bucket:"docs"
                         ~nuid:"0123456789012345678901" ~size:5L ~chunks:1L
                         ~digest:"SHA-256=LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" ())));
              yield_n 5;
              Eio.Promise.resolve publish_u
                (Ok (response_wire ~sid:2 {|{"stream":"OBJ_docs","seq":3}|}));
              yield_n 5;
              Eio.Promise.resolve purge_u (Ok (response_wire ~sid:3 "{}"));
              let () =
                match Eio.Promise.await result with
                | Ok () -> ()
                | Error error ->
                    fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error)
              in
              match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error)));
      test "list returns the latest metadata for each object" (fun () ->
          let create, create_u = Eio.Promise.create () in
          let info, info_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let cleanup, cleanup_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await create;
                `Await info;
                `Await delivery;
                `Await cleanup;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Object_store.list store));
              yield_n 5;
              Eio.Promise.resolve create_u
                (Ok
                   (object_consumer_info_wire ~sid:1 ~name:"list-1"
                      ~num_pending:1L));
              yield_n 5;
              Eio.Promise.resolve info_u
                (Ok
                   (object_consumer_info_wire ~sid:2 ~name:"list-1"
                      ~num_pending:1L));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok
                   (object_metadata_delivery_wire ~sid:3 ~consumer:"list-1"
                      ~name:"hello" ~num_pending:0L
                      (object_info_json ~name:"hello" ~bucket:"docs"
                         ~nuid:"0123456789012345678901" ~size:5L ~chunks:1L
                         ~digest:"SHA-256=LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" ())));
              yield_n 5;
              Eio.Promise.resolve cleanup_u (Ok (response_wire ~sid:4 "{}"));
              let infos =
                match Eio.Promise.await result with
                | Ok infos -> infos
                | Error error ->
                    fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error)
              in
              equal int 1 (List.length infos);
              equal string "hello"
                (Nats_eio.Object_store.Info.name (List.hd infos));
              match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error)));
      test "new watches emit the initial marker before close" (fun () ->
          let create, create_u = Eio.Promise.create () in
          let info, info_u = Eio.Promise.create () in
          let cleanup, cleanup_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create;
                `Await info;
                `Await cleanup;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream = expect_jetstream (Nats_eio.Jetstream.v connection) in
              let store =
                expect_store
                  (Nats_eio.Object_store.bind jetstream ~bucket:"docs")
              in
              let watch_result, watch_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve watch_result_u
                    (Nats_eio.Object_store.Watch.v ~sw
                       ~delivery:Nats_eio.Object_store.Watch.New store));
              yield_n 5;
              Eio.Promise.resolve create_u
                (Ok
                   (object_consumer_info_wire ~sid:1 ~name:"watch-1"
                      ~num_pending:0L));
              yield_n 5;
              Eio.Promise.resolve info_u
                (Ok
                   (object_consumer_info_wire ~sid:3 ~name:"watch-1"
                      ~num_pending:0L));
              let watch =
                match Eio.Promise.await watch_result with
                | Ok value -> value
                | Error error ->
                    fail
                      (Format.asprintf "%a\ntrace:\n%s"
                         Nats_eio.Object_store.Error.pp error
                         (Buffer.contents trace))
              in
              (match Nats_eio.Object_store.Watch.next watch with
              | Ok Nats_eio.Object_store.Watch.Initial_done -> ()
              | Ok _ -> fail "watch emitted an object before its marker"
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Object_store.Error.pp error));
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Object_store.Watch.close watch));
              yield_n 5;
              Eio.Promise.resolve cleanup_u (Ok (response_wire ~sid:4 "{}"));
              ignore (expect_store (Eio.Promise.await close_result));
              match Nats_eio.Connection.close connection with
              | Ok () -> Eio.Promise.resolve hold_u (Error End_of_file)
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Error.pp error)));
    ]
