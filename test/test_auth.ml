open Windtrap

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Auth.pp_error error)

let expect_client = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Error.pp error)

let expect_info wire =
  let client = Nats.Client.v Nats.Config.default in
  expect_client
    (Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp
       (Bytesrw.Bytes.Reader.of_string wire))

let info_wire ?(auth_required = false) ?nonce () =
  let nonce =
    match nonce with None -> "" | Some value -> ",\"nonce\":\"" ^ value ^ "\""
  in
  "INFO {\"max_payload\":100,\"headers\":true,"
  ^ "\"no_responders\":true,\"auth_required\":"
  ^ string_of_bool auth_required
  ^ nonce ^ "}\r\n"

let connect_json auth wire =
  let received = expect_info wire in
  let info =
    match Nats.Client.info received.state with
    | Some value -> value
    | None -> fail "expected typed server INFO"
  in
  let credentials = expect_ok (Nats.Auth.connect auth info) in
  let connected =
    expect_client
      (Nats.Client.outgoing received.state
         (Nats.Client.Connect { credentials; tls_required = false }))
  in
  match connected.output with
  | [ wire ] -> (
      match Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire) with
      | Ok (Nats.Op.Connect json) -> json
      | Ok _ -> fail "expected CONNECT output"
      | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error))
  | _ -> fail "expected one CONNECT output"

let () =
  run "nats-auth"
    [
      test "anonymous auth is rejected when the server requires it" (fun () ->
          let received = expect_info (info_wire ~auth_required:true ()) in
          let info =
            match Nats.Client.info received.state with
            | Some value -> value
            | None -> fail "expected typed server INFO"
          in
          match Nats.Auth.connect Nats.Auth.none info with
          | Error Nats.Auth.Auth_required -> ()
          | Ok _ -> fail "anonymous auth unexpectedly succeeded"
          | Error error ->
              fail
                (Format.asprintf "unexpected auth error: %a" Nats.Auth.pp_error
                   error));
      test "token and username-password credentials enter CONNECT" (fun () ->
          equal string
            "{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"auth_token\":\"token\"}"
            (connect_json (Nats.Auth.token "token") (info_wire ()));
          equal string
            "{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"user\":\"alice\",\"pass\":\"secret\"}"
            (connect_json
               (Nats.Auth.user_pass ~user:"alice" ~pass:"secret")
               (info_wire ())));
      test "NKey and JWT auth sign each INFO nonce" (fun () ->
          let sign ~nonce =
            if String.equal nonce "nonce" then Ok "signature"
            else Error "unexpected nonce"
          in
          equal string
            "{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"nkey\":\"PUB\",\"sig\":\"signature\"}"
            (connect_json
               (Nats.Auth.nkey ~nkey:"PUB" ~sign)
               (info_wire ~nonce:"nonce" ()));
          equal string
            "{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"lang\":\"ocaml\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"jwt\":\"jwt\",\"nkey\":\"PUB\",\"sig\":\"signature\"}"
            (connect_json
               (Nats.Auth.jwt ~jwt:"jwt" ~nkey:"PUB" ~sign)
               (info_wire ~nonce:"nonce" ())));
      test "nonce auth reports missing nonces and signer failures" (fun () ->
          let received = expect_info (info_wire ()) in
          let info =
            match Nats.Client.info received.state with
            | Some value -> value
            | None -> fail "expected typed server INFO"
          in
          (match
             Nats.Auth.connect
               (Nats.Auth.nkey ~nkey:"PUB" ~sign:(fun ~nonce:_ -> Ok "sig"))
               info
           with
          | Error Nats.Auth.Missing_nonce -> ()
          | Ok _ -> fail "missing nonce was accepted"
          | Error error ->
              fail
                (Format.asprintf "unexpected nonce error: %a" Nats.Auth.pp_error
                   error));
          let received = expect_info (info_wire ~nonce:"nonce" ()) in
          let info =
            match Nats.Client.info received.state with
            | Some value -> value
            | None -> fail "expected typed server INFO"
          in
          match
            Nats.Auth.connect
              (Nats.Auth.nkey ~nkey:"PUB" ~sign:(fun ~nonce:_ ->
                   Error "key unavailable"))
              info
          with
          | Error (Nats.Auth.Signing message) ->
              equal string "key unavailable" message
          | Ok _ -> fail "signer failure was accepted"
          | Error error ->
              fail
                (Format.asprintf "unexpected signer error: %a"
                   Nats.Auth.pp_error error));
    ]
