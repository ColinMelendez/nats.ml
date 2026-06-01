let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let required name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> value
  | Some _ | None -> failf "%s is required" name

let endpoint () =
  let value = required "NATS_TEST_SERVER" in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

let next_message ~timeout label subscription =
  match Nats_eio.Subscription.next_with_timeout ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let expect_header label expected headers name =
  match Nats.Header.find name headers with
  | Some actual when String.equal actual expected -> ()
  | Some actual -> failf "%s header was %S, expected %S" label actual expected
  | None -> failf "%s header was missing" label

let expect_publish_ack label ~stream ~duplicate ~sequence ack =
  let actual_stream = Nats_eio.Jetstream.Publish_ack.stream ack in
  if not (String.equal actual_stream stream) then
    failf "%s named stream %S, expected %S" label actual_stream stream;
  let actual_duplicate = Nats_eio.Jetstream.Publish_ack.duplicate ack in
  if Bool.compare actual_duplicate duplicate <> 0 then
    failf "%s duplicate=%b, expected %b" label actual_duplicate duplicate;
  let actual_sequence = Nats_eio.Jetstream.Publish_ack.sequence ack in
  if not (Int64.equal actual_sequence sequence) then
    failf "%s sequence=%Ld, expected %Ld" label actual_sequence sequence

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(5 * s) in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let stream_name = required "NATS_TEST_INTEROP_STREAM" in
  let endpoint = endpoint () in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock [ endpoint ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let jetstream =
        expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v connection)
      in
      let stream =
        expect_jetstream_ok "bind stream"
          (Nats_eio.Jetstream.Stream.bind jetstream ~name:stream_name)
      in
      let ocaml_consumer =
        expect_jetstream_ok "bind OCaml consumer"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:"OCAML_PULL")
      in
      let go_consumer =
        expect_jetstream_ok "bind Go consumer"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:"GO_PULL")
      in
      let stream_info =
        expect_jetstream_ok "stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      let actual_stream_name =
        Nats_eio.Jetstream.Stream.Config.name
          (Nats_eio.Jetstream.Stream.Info.config stream_info)
      in
      if not (String.equal actual_stream_name stream_name) then
        failf "stream info named %S, expected %S" actual_stream_name stream_name;
      let ocaml_info =
        expect_jetstream_ok "OCaml consumer info"
          (Nats_eio.Jetstream.Consumer.info ocaml_consumer)
      in
      if
        not
          (String.equal
             (Nats_eio.Jetstream.Consumer.Info.name ocaml_info)
             "OCAML_PULL")
      then failf "OCaml consumer info returned the wrong name";
      let go_info =
        expect_jetstream_ok "Go consumer info"
          (Nats_eio.Jetstream.Consumer.info go_consumer)
      in
      if
        not
          (String.equal
             (Nats_eio.Jetstream.Consumer.Info.name go_info)
             "GO_PULL")
      then failf "Go consumer info returned the wrong name";
      let done_subscription =
        expect_ok "subscribe completion"
          (Nats_eio.Connection.subscribe connection
             (Nats.Subject.Filter.literal (prefix ^ ".done")))
      in
      expect_ok "completion subscription flush"
        (Nats_eio.Connection.flush connection);
      let start = Nats.Subject.literal (prefix ^ ".start") in
      let start_response =
        expect_ok "start Go peer"
          (Nats_eio.Connection.request ~timeout connection start "start")
      in
      expect_payload "start response" "started" start_response;
      let messages =
        expect_jetstream_ok "fetch Go message"
          (Nats_eio.Jetstream.Consumer.fetch ~expires:timeout ocaml_consumer
             ~batch:1)
      in
      let go_message =
        match messages with
        | [ message ] -> message
        | messages -> failf "Go fetch returned %d messages" (List.length messages)
      in
      expect_payload "Go JetStream message" "from-go-jetstream"
        (Nats_eio.Jetstream.Msg.message go_message);
      expect_header "Go JetStream X-Interop" "go-jetstream"
        (Nats_eio.Jetstream.Msg.headers go_message) "X-Interop";
      expect_header "Go JetStream X-Trace" "go"
        (Nats_eio.Jetstream.Msg.headers go_message) "X-Trace";
      if
        not
          (String.equal
             (Nats_eio.Jetstream.Msg.stream go_message)
             stream_name)
      then failf "Go message named the wrong stream";
      if
        not
          (String.equal
             (Nats_eio.Jetstream.Msg.consumer go_message)
             "OCAML_PULL")
      then failf "Go message named the wrong consumer";
      if
        not (Int64.equal (Nats_eio.Jetstream.Msg.stream_sequence go_message) 1L)
      then failf "Go message had the wrong stream sequence";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Msg.consumer_sequence go_message)
             1L)
      then failf "Go message had the wrong consumer sequence";
      if not (Int64.equal (Nats_eio.Jetstream.Msg.num_pending go_message) 0L) then
        failf "Go message had %Ld pending messages"
          (Nats_eio.Jetstream.Msg.num_pending go_message);
      expect_jetstream_ok "ack Go message"
        (Nats_eio.Jetstream.Msg.ack go_message);
      let go_acknowledged = Nats.Subject.literal (prefix ^ ".go-acked") in
      let acknowledgement =
        expect_ok "acknowledge Go message to peer"
          (Nats_eio.Connection.request ~timeout connection go_acknowledged
             "go-message-acked")
      in
      expect_payload "Go acknowledgement" "acknowledged" acknowledgement;
      let ocaml_subject = Nats.Subject.literal (prefix ^ ".ocaml") in
      let headers =
        match Nats.Header.of_list [ ("X-Interop", "ocaml-jetstream"); ("X-Trace", "ocaml") ] with
        | Ok headers -> headers
        | Error error -> failf "interop headers: %a" Nats.Header.pp_error error
      in
      let ocaml_ack =
        expect_jetstream_ok "publish OCaml JetStream message"
          (Nats_eio.Jetstream.publish ~timeout ~headers
             ~msg_id:"ocaml-jetstream-message" jetstream ocaml_subject
             "from-ocaml-jetstream")
      in
      expect_publish_ack "OCaml publish" ~stream:stream_name ~duplicate:false
        ~sequence:2L
        ocaml_ack;
      let ocaml_duplicate_ack =
        expect_jetstream_ok "duplicate OCaml JetStream message"
          (Nats_eio.Jetstream.publish ~timeout ~headers
             ~msg_id:"ocaml-jetstream-message" jetstream ocaml_subject
             "from-ocaml-jetstream")
      in
      expect_publish_ack "duplicate OCaml publish" ~stream:stream_name
        ~duplicate:true ~sequence:2L ocaml_duplicate_ack;
      let done_message = next_message ~timeout "completion" done_subscription in
      expect_payload "completion" "go-message-acked" done_message;
      expect_jetstream_ok "delete OCaml consumer"
        (Nats_eio.Jetstream.Consumer.delete ocaml_consumer);
      expect_jetstream_ok "delete Go consumer"
        (Nats_eio.Jetstream.Consumer.delete go_consumer);
      expect_jetstream_ok "delete interop stream"
        (Nats_eio.Jetstream.Stream.delete stream);
      print_endline "interop-jetstream: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("JetStream interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("JetStream interop acceptance failed: " ^ Printexc.to_string error);
      exit 1
