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

let auth () =
  match
    ( Sys.getenv_opt "NATS_TEST_USER",
      Sys.getenv_opt "NATS_TEST_PASS",
      Sys.getenv_opt "NATS_TEST_TOKEN" )
  with
  | None, None, None -> None
  | None, None, Some token -> Some (Nats.Auth.token token)
  | Some user, Some pass, None -> Some (Nats.Auth.user_pass ~user ~pass)
  | _ ->
      failf "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let read_file path = In_channel.with_open_bin path In_channel.input_all

let tls_config () =
  match Sys.getenv_opt "NATS_TEST_TLS_CA" with
  | None -> None
  | Some ca_file ->
      let ca =
        match X509.Certificate.decode_pem (read_file ca_file) with
        | Ok value -> value
        | Error (`Msg message) ->
            failf "invalid test CA certificate: %s" message
      in
      let authenticator =
        X509.Authenticator.chain_of_trust
          ~time:(fun () -> Some (Ptime_clock.now ())) [ ca ]
      in
      let peer_name =
        Domain_name.host_exn (Domain_name.of_string_exn "localhost")
      in
      match Tls.Config.client ~authenticator ~peer_name () with
      | Ok value -> Some value
      | Error (`Msg message) -> failf "TLS client configuration: %s" message

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

let expect_publish_ack label ~stream ~sequence ack =
  let actual_stream = Nats_eio.Jetstream.Publish_ack.stream ack in
  if not (String.equal actual_stream stream) then
    failf "%s named stream %S, expected %S" label actual_stream stream;
  let actual_sequence = Nats_eio.Jetstream.Publish_ack.sequence ack in
  if not (Int64.equal actual_sequence sequence) then
    failf "%s sequence=%Ld, expected %Ld" label actual_sequence sequence

let ordered_headers ~interop ~trace =
  match Nats.Header.of_list [ ("X-Interop", interop); ("X-Trace", trace) ] with
  | Ok headers -> headers
  | Error error -> failf "ordered interop headers: %a" Nats.Header.pp_error error

let expect_ordered_consumer_configs stream filter =
  let infos =
    expect_jetstream_ok "list ordered consumers"
      (Nats_eio.Jetstream.Consumer.list stream)
  in
  let matching_infos =
    List.filter
      (fun info ->
        match
          Nats_eio.Jetstream.Consumer.Config.filter_subject
            (Nats_eio.Jetstream.Consumer.Info.config info)
        with
        | Some value -> String.equal (Nats.Subject.Filter.to_string value) filter
        | None -> false)
      infos
  in
  if List.length matching_infos <> 2 then
    failf "expected two ordered consumers, found %d" (List.length matching_infos);
  List.iter
    (fun info ->
      let config = Nats_eio.Jetstream.Consumer.Info.config info in
      (match Nats_eio.Jetstream.Consumer.Config.ack_policy config with
      | Nats_eio.Jetstream.Consumer.Config.No_ack -> ()
      | _ -> failf "ordered consumer did not use no acknowledgements");
      match Nats_eio.Jetstream.Consumer.Config.mem_storage config with
      | Some true -> ()
      | Some false | None -> failf "ordered consumer did not use memory storage")
    matching_infos;
  List.map Nats_eio.Jetstream.Consumer.Info.name matching_infos

let expect_ordered_delivery label ~stream ~subject ~consumer ~payload ~interop
    ~trace ~stream_sequence ~consumer_sequence message =
  expect_payload label payload (Nats_eio.Jetstream.Msg.message message);
  let actual_subject =
    Nats.Subject.to_string (Nats_eio.Jetstream.Msg.subject message)
  in
  if not (String.equal actual_subject subject) then
    failf "%s subject was %S, expected %S" label actual_subject subject;
  expect_header (label ^ " X-Interop") interop
    (Nats_eio.Jetstream.Msg.headers message) "X-Interop";
  expect_header (label ^ " X-Trace") trace
    (Nats_eio.Jetstream.Msg.headers message) "X-Trace";
  if not (String.equal (Nats_eio.Jetstream.Msg.stream message) stream) then
    failf "%s named the wrong stream" label;
  let actual_consumer = Nats_eio.Jetstream.Msg.consumer message in
  if not (String.equal actual_consumer consumer) then
    failf "%s named consumer %S, expected %S" label actual_consumer consumer;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Msg.stream_sequence message)
         stream_sequence)
  then failf "%s had the wrong stream sequence" label;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Msg.consumer_sequence message)
         consumer_sequence)
  then failf "%s had the wrong consumer sequence" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.num_delivered message) 1L) then
    failf "%s had the wrong delivery count" label

let publish_ordered ~timeout jetstream ~stream ~subject ~payload ~interop ~trace
    ~sequence =
  let headers = ordered_headers ~interop ~trace in
  let ack =
    expect_jetstream_ok ("publish " ^ payload)
      (Nats_eio.Jetstream.publish ~timeout ~headers jetstream
         (Nats.Subject.literal subject) payload)
  in
  expect_publish_ack ("publish " ^ payload) ~stream ~sequence ack

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(30 * s) in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let stream_name = required "NATS_TEST_INTEROP_STREAM" in
  let match_subject = prefix ^ ".match" in
  let gap_subject = prefix ^ ".gap" in
  let endpoint = endpoint () in
  let auth = auth () in
  let tls = tls_config () in
  let config =
    match (auth, tls) with
    | None, None -> None
    | _ ->
        Some
          (expect_ok "connection config"
             (Nats_eio.Connection.Config.v ?auth ?tls ()))
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ?config [ endpoint ])
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
      let done_subscription =
        expect_ok "subscribe completion"
          (Nats_eio.Connection.subscribe connection
             (Nats.Subject.Filter.literal (prefix ^ ".go-done")))
      in
      let ordered =
        expect_jetstream_ok "open OCaml ordered session"
          (Nats_eio.Jetstream.Consumer.Ordered.v ~sw
             ~filter_subject:(Nats.Subject.Filter.literal match_subject) stream)
      in
      let ordered_closed = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !ordered_closed then
            match Nats_eio.Jetstream.Consumer.Ordered.close ordered with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "ordered cleanup failed: %a"
                     Nats_eio.Jetstream.Error.pp error))
        (fun () ->
          let consumer_names =
            expect_ordered_consumer_configs stream match_subject
          in
          expect_ok "ordered setup flush"
            (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Go ordered peer"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".start")) "start")
          in
          expect_payload "start response" "started" start_response;
          let first =
            expect_jetstream_ok "receive first ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          let ocaml_consumer_name = Nats_eio.Jetstream.Msg.consumer first in
          if not (List.exists (String.equal ocaml_consumer_name) consumer_names) then
            failf "OCaml ordered delivery named an unexpected consumer %S"
              ocaml_consumer_name;
          let second =
            expect_jetstream_ok "receive second ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          let third =
            expect_jetstream_ok "receive third ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          expect_ordered_delivery "OCaml ordered first" ~stream:stream_name
            ~subject:match_subject ~consumer:ocaml_consumer_name ~payload:"go-one"
            ~interop:"go-ordered" ~trace:"go-one" ~stream_sequence:1L
            ~consumer_sequence:1L first;
          expect_ordered_delivery "OCaml ordered second" ~stream:stream_name
            ~subject:match_subject ~consumer:ocaml_consumer_name ~payload:"go-three"
            ~interop:"go-ordered" ~trace:"go-three" ~stream_sequence:3L
            ~consumer_sequence:2L second;
          expect_ordered_delivery "OCaml ordered third" ~stream:stream_name
            ~subject:match_subject ~consumer:ocaml_consumer_name ~payload:"go-five"
            ~interop:"go-ordered" ~trace:"go-five" ~stream_sequence:5L
            ~consumer_sequence:3L third;
          let batch_two_response =
            expect_ok "request second ordered batch"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".batch2")) "batch2")
          in
          expect_payload "second batch response" "go-batch2-ready"
            batch_two_response;
          publish_ordered ~timeout jetstream ~stream:stream_name
            ~subject:match_subject ~payload:"ocaml-six" ~interop:"ocaml-ordered"
            ~trace:"ocaml-six" ~sequence:6L;
          publish_ordered ~timeout jetstream ~stream:stream_name
            ~subject:gap_subject ~payload:"ocaml-gap" ~interop:"ocaml-ordered"
            ~trace:"ocaml-gap" ~sequence:7L;
          publish_ordered ~timeout jetstream ~stream:stream_name
            ~subject:match_subject ~payload:"ocaml-eight"
            ~interop:"ocaml-ordered" ~trace:"ocaml-eight" ~sequence:8L;
          let fourth =
            expect_jetstream_ok "receive fourth ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          let fifth =
            expect_jetstream_ok "receive fifth ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          expect_ordered_delivery "OCaml ordered fourth" ~stream:stream_name
            ~subject:match_subject ~consumer:ocaml_consumer_name ~payload:"ocaml-six"
            ~interop:"ocaml-ordered" ~trace:"ocaml-six" ~stream_sequence:6L
            ~consumer_sequence:4L fourth;
          expect_ordered_delivery "OCaml ordered fifth" ~stream:stream_name
            ~subject:match_subject ~consumer:ocaml_consumer_name ~payload:"ocaml-eight"
            ~interop:"ocaml-ordered" ~trace:"ocaml-eight" ~stream_sequence:8L
            ~consumer_sequence:5L fifth;
          expect_jetstream_ok "close OCaml ordered session"
            (Nats_eio.Jetstream.Consumer.Ordered.close ordered);
          ordered_closed := true;
          expect_ok "flush after ordered close"
            (Nats_eio.Connection.flush connection);
          let close_response =
            expect_ok "notify Go ordered close"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".ordered-closed"))
                 "ocaml-ordered-closed")
          in
          expect_payload "Go ordered unsubscribe response" "go-unsubscribe"
            close_response;
          let done_message =
            next_message ~timeout "Go ordered completion" done_subscription
          in
          expect_payload "Go ordered completion" "go-ordered-unsubscribed"
            done_message;
          expect_jetstream_ok "delete ordered interop stream"
            (Nats_eio.Jetstream.Stream.delete stream);
          let cleanup_response =
            expect_ok "request ordered cleanup"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".cleanup")) "cleanup")
          in
          expect_payload "ordered cleanup response" "cleaned" cleanup_response;
          print_endline "interop-jetstream-ordered: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("JetStream ordered interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("JetStream ordered interop acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
