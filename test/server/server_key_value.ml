let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let key_value_error_message error =
  Format.asprintf "%a" Nats_eio.Key_value.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_key_value_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (key_value_error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let expect_config_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %a" label Nats_eio.Key_value.Error.pp_config error

let safe_credential value =
  String.length value > 0
  && String.for_all
       (fun character ->
         let code = Char.code character in
         (code >= Char.code 'A' && code <= Char.code 'Z')
         || (code >= Char.code 'a' && code <= Char.code 'z')
         || (code >= Char.code '0' && code <= Char.code '9')
         || code = Char.code '_'
         || code = Char.code '-')
       value

let auth () =
  match
    ( Sys.getenv_opt "NATS_TEST_USER",
      Sys.getenv_opt "NATS_TEST_PASS",
      Sys.getenv_opt "NATS_TEST_TOKEN" )
  with
  | None, None, None -> None
  | None, None, Some token when safe_credential token ->
      Some (Nats.Auth.token token)
  | Some user, Some pass, None when safe_credential user && safe_credential pass
    ->
      Some (Nats.Auth.user_pass ~user ~pass)
  | Some _, Some _, None ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must be non-empty ASCII letters, \
         digits, underscores, or hyphens"
  | None, None, Some _ ->
      failf
        "NATS_TEST_TOKEN must be non-empty ASCII letters, digits, underscores, \
         or hyphens"
  | _ ->
      failf
        "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let endpoint () =
  let value =
    match Sys.getenv_opt "NATS_TEST_SERVER" with
    | Some value -> value
    | None -> "nats://127.0.0.1:4222"
  in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

let run_id () =
  match Sys.getenv_opt "NATS_TEST_JETSTREAM_RUN_ID" with
  | Some value when safe_credential value -> value
  | _ -> "direct"

let expect_revision label expected actual =
  if not (Int64.equal actual expected) then
    failf "%s revision was %Ld, expected %Ld" label actual expected

let expect_entry label ~key ~value ~revision entry =
  if
    not
      (String.equal
         (Nats_eio.Key_value.Key.to_string (Nats_eio.Key_value.Entry.key entry))
         key)
  then failf "%s returned the wrong key" label;
  if not (String.equal (Nats_eio.Key_value.Entry.value entry) value) then
    failf "%s value was %S, expected %S" label
      (Nats_eio.Key_value.Entry.value entry)
      value;
  expect_revision label revision (Nats_eio.Key_value.Entry.revision entry)

let expect_put_entry label entry =
  match Nats_eio.Key_value.Entry.operation entry with
  | Nats_eio.Key_value.Entry.Put -> ()
  | Nats_eio.Key_value.Entry.Delete | Nats_eio.Key_value.Entry.Purge ->
      failf "%s was not a put entry" label

let expect_key value =
  match Nats_eio.Key_value.Key.of_string value with
  | Ok key -> key
  | Error error ->
      failf "invalid test key %S: %a" value Nats_eio.Key_value.Error.pp_key
        error

let expect_watch_marker label watch =
  match
    Nats_eio.Key_value.Watch.next_with_timeout ~timeout:Mtime.Span.(5 * s) watch
  with
  | Ok Nats_eio.Key_value.Watch.Initial_done -> ()
  | Ok (Nats_eio.Key_value.Watch.Entry entry) ->
      failf "%s emitted entry %S before its marker" label
        (Nats_eio.Key_value.Key.to_string (Nats_eio.Key_value.Entry.key entry))
  | Error error -> failf "%s: %s" label (key_value_error_message error)

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoint = endpoint () in
  let config =
    match auth () with
    | None -> None
    | Some auth ->
        Some (expect_ok "auth config" (Nats_eio.Connection.Config.v ~auth ()))
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
      let bucket_name = "OCAML_TEST_KV_" ^ run_id () in
      let bucket_config =
        expect_config_ok "key-value config"
          (Nats_eio.Key_value.Config.v ~bucket:bucket_name ~history:3
             ~storage:Nats_eio.Key_value.Config.Memory ())
      in
      let bucket =
        expect_key_value_ok "key-value create"
          (Nats_eio.Key_value.create jetstream bucket_config)
      in
      let deleted = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !deleted then
            match Nats_eio.Key_value.delete_bucket bucket with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "key-value cleanup failed: %a"
                     Nats_eio.Key_value.Error.pp error))
        (fun () ->
          let status =
            expect_key_value_ok "key-value status"
              (Nats_eio.Key_value.status bucket)
          in
          if
            not
              (String.equal
                 (Nats_eio.Key_value.Status.bucket status)
                 bucket_name)
          then failf "status named the wrong bucket";
          if not (Int64.equal (Nats_eio.Key_value.Status.values status) 0L) then
            failf "new key-value bucket was not empty";
          if
            not
              (Int64.equal
                 (Option.value ~default:0L
                    (Nats_eio.Key_value.Status.history status))
                 3L)
          then failf "key-value history was not three";
          (match Nats_eio.Key_value.Status.storage status with
          | Nats_eio.Key_value.Config.Memory -> ()
          | Nats_eio.Key_value.Config.File ->
              failf "key-value bucket used file storage");
          let alice = expect_key "alice" in
          let alice_revision =
            expect_key_value_ok "key-value put alice"
              (Nats_eio.Key_value.put bucket alice "one")
          in
          expect_revision "first put" 1L alice_revision;
          let alice_entry =
            expect_key_value_ok "key-value get alice"
              (Nats_eio.Key_value.get bucket alice)
          in
          expect_entry "first get" ~key:"alice" ~value:"one" ~revision:1L
            alice_entry;
          let updated_revision =
            expect_key_value_ok "key-value update alice"
              (Nats_eio.Key_value.update bucket alice ~revision:alice_revision
                 "two")
          in
          expect_revision "update" 2L updated_revision;
          (match
             Nats_eio.Key_value.update bucket alice ~revision:alice_revision
               "stale"
           with
          | Error (Nats_eio.Key_value.Error.Revision_mismatch { expected }) ->
              expect_revision "stale update" alice_revision expected
          | Ok revision ->
              failf "stale update unexpectedly succeeded at %Ld" revision
          | Error error ->
              failf "stale update returned %s" (key_value_error_message error));
          (match Nats_eio.Key_value.create_key bucket alice "duplicate" with
          | Error Nats_eio.Key_value.Error.Key_exists -> ()
          | Ok revision ->
              failf "duplicate create unexpectedly succeeded at %Ld" revision
          | Error error ->
              failf "duplicate create returned %s"
                (key_value_error_message error));
          let history =
            expect_key_value_ok "key-value history alice"
              (Nats_eio.Key_value.history bucket alice)
          in
          (match history with
          | [ first; second ] ->
              expect_entry "alice history first" ~key:"alice" ~value:"one"
                ~revision:1L first;
              expect_put_entry "alice history first" first;
              expect_entry "alice history second" ~key:"alice" ~value:"two"
                ~revision:2L second;
              expect_put_entry "alice history second" second
          | _ -> failf "alice history did not contain exactly two entries");
          let deleted_revision =
            expect_key_value_ok "key-value delete alice"
              (Nats_eio.Key_value.delete ~expected_revision:updated_revision
                 bucket alice)
          in
          expect_revision "delete" 3L deleted_revision;
          (match Nats_eio.Key_value.get bucket alice with
          | Error (Nats_eio.Key_value.Error.Key_deleted entry) -> (
              expect_entry "alice tombstone" ~key:"alice" ~value:""
                ~revision:deleted_revision entry;
              match Nats_eio.Key_value.Entry.operation entry with
              | Nats_eio.Key_value.Entry.Delete -> ()
              | Nats_eio.Key_value.Entry.Put | Nats_eio.Key_value.Entry.Purge ->
                  failf "alice tombstone had the wrong operation")
          | Ok entry ->
              failf "deleted alice returned value %S"
                (Nats_eio.Key_value.Entry.value entry)
          | Error error ->
              failf "deleted alice returned %s" (key_value_error_message error));
          let resurrected_revision =
            expect_key_value_ok "key-value resurrect alice"
              (Nats_eio.Key_value.create_key bucket alice "three")
          in
          expect_revision "resurrection" 4L resurrected_revision;
          let bob = expect_key "team.bob" in
          let bob_revision =
            expect_key_value_ok "key-value put bob"
              (Nats_eio.Key_value.put bucket bob "bob")
          in
          expect_revision "bob put" 5L bob_revision;
          let bob_second_revision =
            expect_key_value_ok "key-value update bob"
              (Nats_eio.Key_value.update bucket bob ~revision:bob_revision
                 "bob-two")
          in
          expect_revision "bob update" 6L bob_second_revision;
          let keys_before_purge =
            expect_key_value_ok "key-value filtered keys before purge"
              (Nats_eio.Key_value.keys ~filter:"team.>" bucket)
          in
          (match keys_before_purge with
          | [ key ]
            when String.equal (Nats_eio.Key_value.Key.to_string key) "team.bob"
            ->
              ()
          | _ -> failf "filtered keys did not return team.bob");
          let purged_revision =
            expect_key_value_ok "key-value purge bob"
              (Nats_eio.Key_value.purge ~expected_revision:bob_second_revision
                 bucket bob)
          in
          expect_revision "bob purge" 7L purged_revision;
          let keys =
            expect_key_value_ok "key-value filtered keys"
              (Nats_eio.Key_value.keys ~filter:"team.>" bucket)
          in
          if not (List.is_empty keys) then
            failf "purged bob remained in filtered keys";
          let watch =
            expect_key_value_ok "key-value watch"
              (Nats_eio.Key_value.Watch.v ~sw ~delivery:New bucket)
          in
          let watch_closed = ref false in
          Fun.protect
            ~finally:(fun () ->
              if not !watch_closed then
                ignore (Nats_eio.Key_value.Watch.close watch))
            (fun () ->
              expect_watch_marker "key-value watch" watch;
              let watched = expect_key "watched" in
              let put_result, put_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve put_result_u
                    (Nats_eio.Key_value.put bucket watched "live"));
              let event =
                match
                  Nats_eio.Key_value.Watch.next_with_timeout
                    ~timeout:Mtime.Span.(5 * s)
                    watch
                with
                | Ok (Nats_eio.Key_value.Watch.Entry entry) -> entry
                | Ok Nats_eio.Key_value.Watch.Initial_done ->
                    failf "key-value watch repeated its marker"
                | Error error ->
                    failf "key-value live watch: %s"
                      (key_value_error_message error)
              in
              expect_entry "key-value live watch" ~key:"watched" ~value:"live"
                ~revision:8L event;
              expect_revision "watched put" 8L
                (expect_key_value_ok "watched put result"
                   (Eio.Promise.await put_result));
              expect_key_value_ok "key-value watch close"
                (Nats_eio.Key_value.Watch.close watch);
              watch_closed := true);
          expect_key_value_ok "key-value delete bucket"
            (Nats_eio.Key_value.delete_bucket bucket);
          deleted := true;
          print_endline "key_value: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server key-value acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("server key-value acceptance failed: " ^ Printexc.to_string error);
      exit 1
