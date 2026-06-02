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
      failf
        "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let read_file path = In_channel.with_open_bin path In_channel.input_all

let tls_config () =
  match Sys.getenv_opt "NATS_TEST_TLS_CA" with
  | None -> None
  | Some ca_file -> (
      let ca =
        match X509.Certificate.decode_pem (read_file ca_file) with
        | Ok value -> value
        | Error (`Msg message) ->
            failf "invalid test CA certificate: %s" message
      in
      let authenticator =
        X509.Authenticator.chain_of_trust
          ~time:(fun () -> Some (Ptime_clock.now ()))
          [ ca ]
      in
      let peer_name =
        Domain_name.host_exn (Domain_name.of_string_exn "localhost")
      in
      match Tls.Config.client ~authenticator ~peer_name () with
      | Ok value -> Some value
      | Error (`Msg message) -> failf "TLS client configuration: %s" message)

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

let expect_push_consumer_config label info ~name ~delivery ~filter =
  let actual_name = Nats_eio.Jetstream.Consumer.Info.name info in
  if not (String.equal actual_name name) then
    failf "%s named consumer %S, expected %S" label actual_name name;
  let config = Nats_eio.Jetstream.Consumer.Info.config info in
  (match Nats_eio.Jetstream.Consumer.Config.deliver_subject config with
  | Some subject when String.equal (Nats.Subject.to_string subject) delivery ->
      ()
  | Some subject ->
      failf "%s delivered to %S, expected %S" label
        (Nats.Subject.to_string subject)
        delivery
  | None -> failf "%s had no delivery subject" label);
  (match Nats_eio.Jetstream.Consumer.Config.filter_subject config with
  | Some subject
    when String.equal (Nats.Subject.Filter.to_string subject) filter ->
      ()
  | Some subject ->
      failf "%s filtered %S, expected %S" label
        (Nats.Subject.Filter.to_string subject)
        filter
  | None -> failf "%s had no filter subject" label);
  match Nats_eio.Jetstream.Consumer.Config.ack_policy config with
  | Nats_eio.Jetstream.Consumer.Config.Explicit -> ()
  | _ -> failf "%s did not use explicit acknowledgements" label

let expect_push_delivery label ~stream ~consumer ~payload ~interop ~trace
    message =
  expect_payload label payload (Nats_eio.Jetstream.Msg.message message);
  expect_header (label ^ " X-Interop") interop
    (Nats_eio.Jetstream.Msg.headers message)
    "X-Interop";
  expect_header (label ^ " X-Trace") trace
    (Nats_eio.Jetstream.Msg.headers message)
    "X-Trace";
  if not (String.equal (Nats_eio.Jetstream.Msg.stream message) stream) then
    failf "%s named the wrong stream" label;
  if not (String.equal (Nats_eio.Jetstream.Msg.consumer message) consumer) then
    failf "%s named the wrong consumer" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.stream_sequence message) 1L) then
    failf "%s had the wrong stream sequence" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.consumer_sequence message) 1L)
  then failf "%s had the wrong consumer sequence" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.num_delivered message) 1L) then
    failf "%s had the wrong delivery count" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.num_pending message) 0L) then
    failf "%s had %Ld pending messages" label
      (Nats_eio.Jetstream.Msg.num_pending message)

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(5 * s) in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let stream_name = required "NATS_TEST_INTEROP_STREAM" in
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
      let ocaml_consumer =
        expect_jetstream_ok "bind OCaml push consumer"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:"OCAML_PUSH")
      in
      let go_consumer =
        expect_jetstream_ok "bind Go push consumer"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:"GO_PUSH")
      in
      let ocaml_info =
        expect_jetstream_ok "OCaml push consumer info"
          (Nats_eio.Jetstream.Consumer.info ocaml_consumer)
      in
      expect_push_consumer_config "OCaml push consumer" ocaml_info
        ~name:"OCAML_PUSH"
        ~delivery:(prefix ^ ".deliver.ocaml")
        ~filter:(prefix ^ ".go");
      let go_info =
        expect_jetstream_ok "Go push consumer info"
          (Nats_eio.Jetstream.Consumer.info go_consumer)
      in
      expect_push_consumer_config "Go push consumer" go_info ~name:"GO_PUSH"
        ~delivery:(prefix ^ ".deliver.go") ~filter:(prefix ^ ".ocaml");
      let push =
        expect_jetstream_ok "open OCaml push session"
          (Nats_eio.Jetstream.Consumer.Push.v ~sw ocaml_consumer)
      in
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Consumer.Push.initial_pending push)
             0L)
      then failf "OCaml push session started with pending messages";
      let done_subscription =
        expect_ok "subscribe completion"
          (Nats_eio.Connection.subscribe connection
             (Nats.Subject.Filter.literal (prefix ^ ".done")))
      in
      expect_ok "push setup flush" (Nats_eio.Connection.flush connection);
      let start_response =
        expect_ok "start Go push peer"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".start"))
             "start")
      in
      expect_payload "start response" "started" start_response;
      let go_message =
        expect_jetstream_ok "receive Go JetStream push message"
          (Nats_eio.Jetstream.Consumer.Push.next_with_timeout ~timeout push)
      in
      expect_push_delivery "Go JetStream push message" ~stream:stream_name
        ~consumer:"OCAML_PUSH" ~payload:"from-go-jetstream-push"
        ~interop:"go-jetstream-push" ~trace:"go-push" go_message;
      expect_jetstream_ok "acknowledge Go push message"
        (Nats_eio.Jetstream.Msg.ack_sync ~timeout go_message);
      let acknowledgement =
        expect_ok "acknowledge Go push message to peer"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".go-push-acked"))
             "go-push-message-acked")
      in
      expect_payload "Go push acknowledgement" "acknowledged" acknowledgement;
      let ocaml_subject = Nats.Subject.literal (prefix ^ ".ocaml") in
      let headers =
        match
          Nats.Header.of_list
            [ ("X-Interop", "ocaml-jetstream-push"); ("X-Trace", "ocaml-push") ]
        with
        | Ok headers -> headers
        | Error error ->
            failf "push interop headers: %a" Nats.Header.pp_error error
      in
      let ocaml_ack =
        expect_jetstream_ok "publish OCaml JetStream push message"
          (Nats_eio.Jetstream.publish ~timeout ~headers jetstream ocaml_subject
             "from-ocaml-jetstream-push")
      in
      expect_publish_ack "OCaml push publish" ~stream:stream_name ~sequence:2L
        ocaml_ack;
      let done_message =
        next_message ~timeout "push completion" done_subscription
      in
      expect_payload "push completion" "go-push-message-acked" done_message;
      expect_jetstream_ok "close OCaml push session"
        (Nats_eio.Jetstream.Consumer.Push.close push);
      expect_ok "flush after push close" (Nats_eio.Connection.flush connection);
      expect_jetstream_ok "delete OCaml push consumer"
        (Nats_eio.Jetstream.Consumer.delete ocaml_consumer);
      expect_jetstream_ok "delete Go push consumer"
        (Nats_eio.Jetstream.Consumer.delete go_consumer);
      expect_jetstream_ok "delete push interop stream"
        (Nats_eio.Jetstream.Stream.delete stream);
      print_endline "interop-jetstream-push: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("JetStream push interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("JetStream push interop acceptance failed: " ^ Printexc.to_string error);
      exit 1
