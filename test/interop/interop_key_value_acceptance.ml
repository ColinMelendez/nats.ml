let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let key_value_error_message error =
  Format.asprintf "%a" Nats_eio.Key_value.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_key_value_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (key_value_error_message error)

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

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let expect_revision label expected actual =
  if not (Int64.equal actual expected) then
    failf "%s revision was %Ld, expected %Ld" label actual expected

let expect_operation label expected actual =
  match (expected, actual) with
  | Nats_eio.Key_value.Entry.Put, Nats_eio.Key_value.Entry.Put
  | Nats_eio.Key_value.Entry.Delete, Nats_eio.Key_value.Entry.Delete
  | Nats_eio.Key_value.Entry.Purge, Nats_eio.Key_value.Entry.Purge ->
      ()
  | _ ->
      failf "%s operation was %a, expected %a" label
        Nats_eio.Key_value.Entry.pp_operation actual
        Nats_eio.Key_value.Entry.pp_operation expected

let expect_entry label ~key ~value ~revision ~operation entry =
  let actual_key =
    Nats_eio.Key_value.Key.to_string (Nats_eio.Key_value.Entry.key entry)
  in
  if not (String.equal actual_key key) then
    failf "%s key was %S, expected %S" label actual_key key;
  if not (String.equal (Nats_eio.Key_value.Entry.value entry) value) then
    failf "%s value was %S, expected %S" label
      (Nats_eio.Key_value.Entry.value entry)
      value;
  expect_revision label revision (Nats_eio.Key_value.Entry.revision entry);
  expect_operation label operation (Nats_eio.Key_value.Entry.operation entry)

let expect_status status bucket values =
  if not (String.equal (Nats_eio.Key_value.Status.bucket status) bucket) then
    failf "Key-Value status bucket was %S, expected %S"
      (Nats_eio.Key_value.Status.bucket status)
      bucket;
  if not (Int64.equal (Nats_eio.Key_value.Status.values status) values) then
    failf "Key-Value status values were %Ld, expected %Ld"
      (Nats_eio.Key_value.Status.values status)
      values;
  (match Nats_eio.Key_value.Status.history status with
  | Some history when Int64.equal history 5L -> ()
  | Some history -> failf "Key-Value status history was %Ld, expected 5" history
  | None -> failf "Key-Value status omitted history");
  (match Nats_eio.Key_value.Status.ttl status with
  | None -> ()
  | Some _ -> failf "Key-Value status unexpectedly supplied a TTL");
  match Nats_eio.Key_value.Status.storage status with
  | Nats_eio.Key_value.Config.Memory -> ()
  | Nats_eio.Key_value.Config.File ->
      failf "Key-Value status used file storage, expected memory"

let expect_history label expected entries =
  if not (Int.equal (List.length entries) (List.length expected)) then
    failf "%s had %d entries, expected %d" label (List.length entries)
      (List.length expected);
  List.iter2
    (fun (key, value, revision, operation) entry ->
      expect_entry label ~key ~value ~revision ~operation entry)
    expected entries

let request_control ~timeout connection prefix suffix payload =
  expect_ok ("request " ^ suffix)
    (Nats_eio.Connection.request ~timeout connection
       (Nats.Subject.literal (prefix ^ suffix))
       payload)

let expect_watch_marker ~timeout label watch =
  match Nats_eio.Key_value.Watch.next_with_timeout ~timeout watch with
  | Ok Nats_eio.Key_value.Watch.Initial_done -> ()
  | Ok (Nats_eio.Key_value.Watch.Entry entry) ->
      failf "%s emitted %S before its initial marker" label
        (Nats_eio.Key_value.Key.to_string (Nats_eio.Key_value.Entry.key entry))
  | Error error -> failf "%s: %s" label (key_value_error_message error)

let expect_watch_entry ~timeout label watch ~key ~value ~revision =
  match Nats_eio.Key_value.Watch.next_with_timeout ~timeout watch with
  | Ok (Nats_eio.Key_value.Watch.Entry entry) ->
      expect_entry label ~key ~value ~revision
        ~operation:Nats_eio.Key_value.Entry.Put entry
  | Ok Nats_eio.Key_value.Watch.Initial_done ->
      failf "%s repeated its initial marker" label
  | Error error -> failf "%s: %s" label (key_value_error_message error)

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(10 * s) in
  let bucket_name = required "NATS_TEST_INTEROP_BUCKET" in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
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
        expect_jetstream_ok "JetStream" (Nats_eio.Jetstream.v connection)
      in
      let bucket =
        expect_key_value_ok "open Key-Value bucket"
          (Nats_eio.Key_value.open_ jetstream ~bucket:bucket_name)
      in
      let status =
        expect_key_value_ok "initial Key-Value status"
          (Nats_eio.Key_value.status bucket)
      in
      expect_status status bucket_name 0L;
      expect_ok "flush Key-Value setup" (Nats_eio.Connection.flush connection);
      let start_response =
        request_control ~timeout connection prefix ".start" "start"
      in
      expect_payload "Go Key-Value start" "started" start_response;
      let go_key =
        match Nats_eio.Key_value.Key.of_string "go.key" with
        | Ok key -> key
        | Error error ->
            failf "invalid Go Key-Value key: %a" Nats_eio.Key_value.Error.pp_key
              error
      in
      let go_entry =
        expect_key_value_ok "get Go Key-Value entry"
          (Nats_eio.Key_value.get bucket go_key)
      in
      expect_entry "Go Key-Value entry" ~key:"go.key" ~value:"from-go"
        ~revision:1L ~operation:Nats_eio.Key_value.Entry.Put go_entry;
      let ocaml_revision =
        expect_key_value_ok "update Go Key-Value entry"
          (Nats_eio.Key_value.update bucket go_key ~revision:1L "from-ocaml")
      in
      expect_revision "OCaml Key-Value update" 2L ocaml_revision;
      let update_response =
        request_control ~timeout connection prefix ".ocaml-updated" "updated"
      in
      expect_payload "Go Key-Value update validation" "go-validated"
        update_response;
      let deleted_revision =
        expect_key_value_ok "delete Go Key-Value entry"
          (Nats_eio.Key_value.delete ~expected_revision:ocaml_revision bucket
             go_key)
      in
      expect_revision "OCaml Key-Value delete" 3L deleted_revision;
      (match Nats_eio.Key_value.get bucket go_key with
      | Error (Nats_eio.Key_value.Error.Key_deleted entry) ->
          expect_entry "OCaml Key-Value tombstone" ~key:"go.key" ~value:""
            ~revision:3L ~operation:Nats_eio.Key_value.Entry.Delete entry
      | Ok entry ->
          failf "deleted Go Key-Value entry returned %S"
            (Nats_eio.Key_value.Entry.value entry)
      | Error error ->
          failf "deleted Go Key-Value entry returned %s"
            (key_value_error_message error));
      let delete_response =
        request_control ~timeout connection prefix ".ocaml-deleted" "deleted"
      in
      expect_payload "Go Key-Value delete validation" "delete-validated"
        delete_response;
      let watch_ocaml =
        expect_key_value_ok "watch OCaml Key-Value entry"
          (Nats_eio.Key_value.Watch.v ~sw ~key:"watch.ocaml"
             ~delivery:Nats_eio.Key_value.Watch.New bucket)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Key_value.Watch.close watch_ocaml))
        (fun () ->
          expect_watch_marker ~timeout "OCaml watch" watch_ocaml;
          let watch_ready_response =
            request_control ~timeout connection prefix ".go-watch-ready" "ready"
          in
          expect_payload "Go Key-Value watch readiness" "ready"
            watch_ready_response;
          let watch_ocaml_key =
            match Nats_eio.Key_value.Key.of_string "watch.ocaml" with
            | Ok key -> key
            | Error error ->
                failf "invalid OCaml watch key: %a"
                  Nats_eio.Key_value.Error.pp_key error
          in
          let watch_revision =
            expect_key_value_ok "put OCaml watch Key-Value entry"
              (Nats_eio.Key_value.put bucket watch_ocaml_key "from-ocaml-watch")
          in
          expect_revision "OCaml watch put" 4L watch_revision;
          let watch_response =
            request_control ~timeout connection prefix ".ocaml-watch-written"
              "written"
          in
          expect_payload "Go Key-Value watch validation" "watch-seen"
            watch_response);
      let watch_go =
        expect_key_value_ok "watch Go Key-Value entry"
          (Nats_eio.Key_value.Watch.v ~sw ~key:"watch.go"
             ~delivery:Nats_eio.Key_value.Watch.New bucket)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Key_value.Watch.close watch_go))
        (fun () ->
          expect_watch_marker ~timeout "Go watch" watch_go;
          let go_write_response =
            request_control ~timeout connection prefix ".go-write" "write"
          in
          expect_payload "Go Key-Value watch write" "written" go_write_response;
          expect_watch_entry ~timeout "OCaml Go watch" watch_go ~key:"watch.go"
            ~value:"from-go-watch" ~revision:5L);
      let purge_key =
        match Nats_eio.Key_value.Key.of_string "purge.key" with
        | Ok key -> key
        | Error error ->
            failf "invalid purge key: %a" Nats_eio.Key_value.Error.pp_key error
      in
      let purge_revision =
        expect_key_value_ok "put purge Key-Value entry"
          (Nats_eio.Key_value.put bucket purge_key "before-purge")
      in
      expect_revision "purge setup put" 6L purge_revision;
      let purge_update_revision =
        expect_key_value_ok "update purge Key-Value entry"
          (Nats_eio.Key_value.update bucket purge_key ~revision:purge_revision
             "after-purge")
      in
      expect_revision "purge setup update" 7L purge_update_revision;
      let purge_response =
        request_control ~timeout connection prefix ".ocaml-purge-ready" "purge"
      in
      expect_payload "Go Key-Value purge" "purged" purge_response;
      (match Nats_eio.Key_value.get bucket purge_key with
      | Error (Nats_eio.Key_value.Error.Key_deleted entry) ->
          expect_entry "Go Key-Value purge tombstone" ~key:"purge.key" ~value:""
            ~revision:8L ~operation:Nats_eio.Key_value.Entry.Purge entry
      | Ok entry ->
          failf "purged Key-Value entry returned %S"
            (Nats_eio.Key_value.Entry.value entry)
      | Error error ->
          failf "purged Key-Value entry returned %s"
            (key_value_error_message error));
      let purge_history =
        expect_key_value_ok "read purged Key-Value history"
          (Nats_eio.Key_value.history bucket purge_key)
      in
      expect_history "purged Key-Value history"
        [ ("purge.key", "", 8L, Nats_eio.Key_value.Entry.Purge) ]
        purge_history;
      let done_response =
        request_control ~timeout connection prefix ".done" "done"
      in
      expect_payload "Key-Value completion" "go-validated" done_response;
      print_endline "interop-key-value: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("interop Key-Value acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("interop Key-Value acceptance failed: " ^ Printexc.to_string error);
      exit 1
