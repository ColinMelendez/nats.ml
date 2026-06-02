let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let endpoint () =
  match Sys.getenv_opt "NATS_TEST_TLS_SERVER" with
  | Some value -> (
      match Nats.Endpoint.of_string value with
      | Ok value -> value
      | Error error ->
          failf "invalid TLS endpoint %S: %a" value Nats.Endpoint.pp_error error
      )
  | None -> failf "NATS_TEST_TLS_SERVER is required"

let read_file path = In_channel.with_open_bin path In_channel.input_all

let tls_config () =
  let ca_file =
    match Sys.getenv_opt "NATS_TEST_TLS_CA" with
    | Some value -> value
    | None -> failf "NATS_TEST_TLS_CA is required"
  in
  let ca =
    match X509.Certificate.decode_pem (read_file ca_file) with
    | Ok value -> value
    | Error (`Msg message) -> failf "invalid test CA certificate: %s" message
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
  | Ok value -> value
  | Error (`Msg message) -> failf "TLS client configuration: %s" message

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ~tls:(tls_config ()) ())
  in
  let connection =
    expect_ok "TLS connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config [ endpoint () ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let subject = Nats.Subject.literal "ocaml.integration.tls" in
      let filter = Nats.Subject.Filter.literal "ocaml.integration.tls" in
      let subscription =
        expect_ok "TLS subscribe"
          (Nats_eio.Connection.subscribe connection filter)
      in
      expect_ok "TLS subscribe flush" (Nats_eio.Connection.flush connection);
      expect_ok "TLS publish"
        (Nats_eio.Connection.publish connection subject "secure");
      expect_ok "TLS publish flush" (Nats_eio.Connection.flush connection);
      let delivery =
        expect_ok "TLS delivery"
          (Nats_eio.Subscription.next_with_timeout
             ~timeout:Mtime.Span.(5 * s)
             subscription)
      in
      if not (String.equal (Nats.Message.payload delivery.message) "secure")
      then failf "TLS payload was %S" (Nats.Message.payload delivery.message);
      print_endline "tls: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server TLS failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server TLS failed: " ^ Printexc.to_string error);
      exit 1
