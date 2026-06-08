let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let config_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp_config error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let expect_config_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (config_error_message error)

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

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let expect_contains label expected values =
  if not (List.exists (String.equal expected) values) then
    failf "%s did not contain %S" label expected

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
      let start_response =
        expect_ok "start Go administration peer"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".start"))
             "start")
      in
      expect_payload "start response" "started" start_response;
      let account =
        expect_jetstream_ok "account info"
          (Nats_eio.Jetstream.account_info jetstream)
      in
      let tier = Nats_eio.Jetstream.Account.tier account in
      if Nats_eio.Jetstream.Account.Tier.streams tier < 1 then
        failf "account info reported no streams";
      if Nats_eio.Jetstream.Account.Tier.consumers tier < 1 then
        failf "account info reported no consumers";
      let stream =
        expect_jetstream_ok "stream lookup"
          (Nats_eio.Jetstream.Stream.lookup jetstream ~name:stream_name)
      in
      let stream_names =
        expect_jetstream_ok "stream names"
          (Nats_eio.Jetstream.Stream.names jetstream)
      in
      expect_contains "stream names" stream_name stream_names;
      let stream_by_subject =
        expect_jetstream_ok "stream name by subject"
          (Nats_eio.Jetstream.Stream.name_by_subject jetstream
             ~subject:(Nats.Subject.literal (prefix ^ ".ocaml")))
      in
      if not (String.equal stream_by_subject stream_name) then
        failf "stream name by subject returned %S, expected %S"
          stream_by_subject stream_name;
      let stream_info =
        expect_jetstream_ok "stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      let stream_config =
        expect_config_ok "stream description config"
          (Nats_eio.Jetstream.Stream.Config.with_description
             (Nats_eio.Jetstream.Stream.Info.config stream_info)
             (Some "updated-by-ocaml"))
      in
      let stream =
        expect_jetstream_ok "stream create or update"
          (Nats_eio.Jetstream.Stream.create_or_update jetstream stream_config)
      in
      let consumer =
        expect_jetstream_ok "consumer lookup"
          (Nats_eio.Jetstream.Consumer.lookup stream ~name:"ADMIN_GO")
      in
      let consumer_names =
        expect_jetstream_ok "consumer names"
          (Nats_eio.Jetstream.Consumer.names stream)
      in
      expect_contains "consumer names" "ADMIN_GO" consumer_names;
      let consumer_info =
        expect_jetstream_ok "consumer info"
          (Nats_eio.Jetstream.Consumer.info consumer)
      in
      let consumer_config =
        expect_config_ok "consumer max deliver config"
          (Nats_eio.Jetstream.Consumer.Config.with_max_deliver
             (Nats_eio.Jetstream.Consumer.Info.config consumer_info)
             (Some 5))
      in
      let consumer =
        expect_jetstream_ok "consumer create or update"
          (Nats_eio.Jetstream.Consumer.create_or_update stream consumer_config)
      in
      if
        not
          (String.equal (Nats_eio.Jetstream.Consumer.name consumer) "ADMIN_GO")
      then
        failf "consumer handle named %S, expected %S"
          (Nats_eio.Jetstream.Consumer.name consumer)
          "ADMIN_GO";
      let reset =
        expect_jetstream_ok "consumer reset to sequence"
          (Nats_eio.Jetstream.Consumer.reset_to_sequence consumer ~sequence:7L)
      in
      if not (Int64.equal (Nats_eio.Jetstream.Consumer.Reset.sequence reset) 7L)
      then
        failf "consumer reset selected sequence %Ld, expected 7"
          (Nats_eio.Jetstream.Consumer.Reset.sequence reset);
      let reset_info = Nats_eio.Jetstream.Consumer.Reset.info reset in
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Consumer.Info.num_pending reset_info)
             4L)
      then
        failf "consumer reset reported %Ld pending messages, expected 4"
          (Nats_eio.Jetstream.Consumer.Info.num_pending reset_info);
      let done_response =
        expect_ok "complete administration peer"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".admin-done"))
             "done")
      in
      expect_payload "administration completion response" "verified"
        done_response)

let () = Eio_main.run run
