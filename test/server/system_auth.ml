type mode = User_pass | User_pass_tls | Nkey | Nkey_tls | Jwt | Jwt_tls | Mtls

let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let required name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> value
  | _ -> failf "%s is required" name

let mode () =
  match
    Option.value ~default:"user-pass"
      (Sys.getenv_opt "NATS_TEST_SYSTEM_AUTH_MODE")
  with
  | "user-pass" -> User_pass
  | "user-pass-tls" -> User_pass_tls
  | "nkey" -> Nkey
  | "nkey-tls" -> Nkey_tls
  | "jwt" -> Jwt
  | "jwt-tls" -> Jwt_tls
  | "mtls" -> Mtls
  | value -> failf "unknown NATS_TEST_SYSTEM_AUTH_MODE %S" value

let read_file path = In_channel.with_open_bin path In_channel.input_all

let base32_value = function
  | 'A' .. 'Z' as value -> Char.code value - Char.code 'A'
  | '2' .. '7' as value -> 26 + Char.code value - Char.code '2'
  | value -> failf "invalid NKey seed base32 character %C" value

let decode_nkey_seed value =
  let value = String.trim value in
  let length = String.length value in
  let raw = Bytes.create (length * 5 / 8) in
  let buffer = ref 0 in
  let bits = ref 0 in
  let output = ref 0 in
  for index = 0 to length - 1 do
    buffer := (!buffer lsl 5) lor base32_value value.[index];
    bits := !bits + 5;
    while !bits >= 8 do
      bits := !bits - 8;
      if !output >= Bytes.length raw then
        failf "NKey seed decoded beyond its allocated length";
      Bytes.set raw !output (Char.chr ((!buffer lsr !bits) land 0xff));
      output := !output + 1
    done
  done;
  if not (Int.equal !output 36) then
    failf "NKey seed decoded to %d bytes, expected 36" !output;
  let first = Char.code (Bytes.get raw 0) in
  let second = Char.code (Bytes.get raw 1) in
  let seed_prefix = first land 0xf8 in
  let public_prefix =
    ((first land 0x07) lsl 5) lor ((second land 0xf8) lsr 3)
  in
  if not (Int.equal seed_prefix 0x90 && Int.equal public_prefix 0xa0) then
    failf "NKey seed did not contain a user seed prefix";
  Bytes.sub_string raw 2 32

let signer seed_file error_label =
  let seed = decode_nkey_seed (read_file seed_file) in
  let key =
    match Mirage_crypto_ec.Ed25519.priv_of_octets seed with
    | Ok key -> key
    | Error _ -> failf "invalid %s NKey seed private key" error_label
  in
  fun ~nonce ->
    Ok
      (Base64.encode_string ~pad:false ~alphabet:Base64.uri_safe_alphabet
         (Mirage_crypto_ec.Ed25519.sign ~key nonce))

let auth () =
  match mode () with
  | User_pass | User_pass_tls ->
      Some
        (Nats.Auth.user_pass
           ~user:(required "NATS_TEST_SYSTEM_USER")
           ~pass:(required "NATS_TEST_SYSTEM_PASS"))
  | Nkey | Nkey_tls ->
      Some
        (Nats.Auth.nkey
           ~nkey:(required "NATS_TEST_SYSTEM_NKEY_PUBLIC")
           ~sign:(signer (required "NATS_TEST_SYSTEM_NKEY_SEED_FILE") "system"))
  | Jwt | Jwt_tls ->
      let jwt =
        String.trim (read_file (required "NATS_TEST_SYSTEM_USER_JWT_FILE"))
      in
      Some
        (Nats.Auth.jwt ~jwt
           ~nkey:(required "NATS_TEST_SYSTEM_NKEY_PUBLIC")
           ~sign:
             (signer
                (required "NATS_TEST_SYSTEM_USER_SEED_FILE")
                "system JWT user"))
  | Mtls -> Some Nats.Auth.tls

let tls_certificates () =
  match
    (Sys.getenv_opt "NATS_TEST_TLS_CERT", Sys.getenv_opt "NATS_TEST_TLS_KEY")
  with
  | Some certificate_file, Some key_file ->
      let certificates =
        match
          X509.Certificate.decode_pem_multiple (read_file certificate_file)
        with
        | Ok value -> value
        | Error (`Msg message) -> failf "invalid client certificate: %s" message
      in
      let key =
        match X509.Private_key.decode_pem (read_file key_file) with
        | Ok value -> value
        | Error (`Msg message) -> failf "invalid client key: %s" message
      in
      Some (`Single (certificates, key))
  | None, None -> None
  | _ ->
      failf "NATS_TEST_TLS_CERT and NATS_TEST_TLS_KEY must be supplied together"

let tls () =
  match mode () with
  | User_pass | Nkey | Jwt -> None
  | User_pass_tls | Nkey_tls | Jwt_tls | Mtls -> (
      let ca_file = required "NATS_TEST_TLS_CA" in
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
      let certificates =
        match mode () with Mtls -> tls_certificates () | _ -> None
      in
      match Tls.Config.client ~authenticator ~peer_name ?certificates () with
      | Ok value -> Some value
      | Error (`Msg message) -> failf "TLS client configuration: %s" message)

let initialize () = Mirage_crypto_rng_unix.use_default ()
