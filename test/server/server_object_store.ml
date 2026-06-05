let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let object_store_error_message error =
  Format.asprintf "%a" Nats_eio.Object_store.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_object_store_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (object_store_error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let expect_config_ok label = function
  | Ok value -> value
  | Error error ->
      failf "%s: %a" label Nats_eio.Object_store.Error.pp_config error

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

let object_name value =
  match Nats_eio.Object_store.Name.of_string value with
  | Ok value -> value
  | Error error ->
      failf "invalid object name %S: %a" value
        Nats_eio.Object_store.Error.pp_name error

let object_meta ?description ?(headers = Nats.Header.empty) ?(metadata = [])
    ~name ~chunk_size () =
  match
    Nats_eio.Object_store.Meta.v ~name:(object_name name) ?description ~headers
      ~metadata ~chunk_size ()
  with
  | Ok value -> value
  | Error error ->
      failf "object metadata: %a" Nats_eio.Object_store.Error.pp_meta error

let expect_name label expected info =
  let actual =
    Nats_eio.Object_store.Name.to_string (Nats_eio.Object_store.Info.name info)
  in
  if not (String.equal actual expected) then
    failf "%s name was %S, expected %S" label actual expected

let expect_string label expected actual =
  if not (String.equal actual expected) then
    failf "%s was %S, expected %S" label actual expected

let expect_int64 label expected actual =
  if not (Int64.equal actual expected) then
    failf "%s was %Ld, expected %Ld" label actual expected

let expect_header label name expected headers =
  match Nats.Header.find name headers with
  | Some actual when String.equal actual expected -> ()
  | Some actual -> failf "%s was %S, expected %S" label actual expected
  | None -> failf "%s was absent, expected %S" label expected

let expect_info_content label ~name ~payload ~chunks info =
  expect_name label name info;
  expect_int64 (label ^ " size")
    (Int64.of_int (String.length payload))
    (Nats_eio.Object_store.Info.size info);
  expect_int64 (label ^ " chunks") chunks
    (Nats_eio.Object_store.Info.chunks info);
  if Nats_eio.Object_store.Info.deleted info then
    failf "%s was marked deleted" label;
  if String.length (Nats_eio.Object_store.Info.digest info) = 0 then
    failf "%s had no digest" label

let expect_watch_marker label watch =
  match
    Nats_eio.Object_store.Watch.next_with_timeout
      ~timeout:Mtime.Span.(5 * s)
      watch
  with
  | Ok Nats_eio.Object_store.Watch.Initial_done -> ()
  | Ok (Nats_eio.Object_store.Watch.Info info) ->
      failf "%s emitted %S before its marker" label
        (Nats_eio.Object_store.Name.to_string
           (Nats_eio.Object_store.Info.name info))
  | Error error -> failf "%s: %s" label (object_store_error_message error)

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
      let bucket_name = "OCAML_TEST_OBJ_" ^ run_id () in
      let bucket_config =
        expect_config_ok "object-store config"
          (Nats_eio.Object_store.Config.v ~bucket:bucket_name
             ~description:"media" ~max_bytes:1_000_000L
             ~storage:Nats_eio.Object_store.Config.Memory ())
      in
      let bucket =
        expect_object_store_ok "object-store create"
          (Nats_eio.Object_store.create jetstream bucket_config)
      in
      let deleted = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !deleted then
            match Nats_eio.Object_store.delete_bucket bucket with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "object-store cleanup failed: %a"
                     Nats_eio.Object_store.Error.pp error))
        (fun () ->
          let status =
            expect_object_store_ok "object-store status"
              (Nats_eio.Object_store.status bucket)
          in
          expect_string "status bucket" bucket_name
            (Nats_eio.Object_store.Status.bucket status);
          (match Nats_eio.Object_store.Status.storage status with
          | Nats_eio.Object_store.Config.Memory -> ()
          | Nats_eio.Object_store.Config.File ->
              failf "object-store bucket used file storage");
          expect_string "status description" "media"
            (Option.value ~default:""
               (Nats_eio.Object_store.Status.description status));
          expect_int64 "status max bytes" 1_000_000L
            (Option.value ~default:(-1L)
               (Nats_eio.Object_store.Status.max_bytes status));
          if Nats_eio.Object_store.Status.sealed status then
            failf "new object-store bucket was sealed";
          let headers =
            match Nats.Header.of_list [ ("X-Test", "object") ] with
            | Ok headers -> headers
            | Error error ->
                failf "object headers: %a" Nats.Header.pp_error error
          in
          let base_payload = "abcdefghij" in
          let base_meta =
            object_meta ~description:"base object" ~headers
              ~metadata:[ ("kind", "fixture") ]
              ~name:"base.txt" ~chunk_size:3 ()
          in
          let base_info =
            expect_object_store_ok "object-store put base"
              (Nats_eio.Object_store.put bucket base_meta
                 (Bytesrw.Bytes.Reader.of_string ~slice_length:2 base_payload))
          in
          expect_info_content "base object" ~name:"base.txt"
            ~payload:base_payload ~chunks:4L base_info;
          expect_int64 "base chunk size" 3L
            (Int64.of_int (Nats_eio.Object_store.Info.chunk_size base_info));
          expect_string "base description" "base object"
            (Option.value ~default:""
               (Nats_eio.Object_store.Info.description base_info));
          expect_header "base object X-Test" "X-Test" "object"
            (Nats_eio.Object_store.Info.headers base_info);
          (match Nats_eio.Object_store.Info.metadata base_info with
          | [ ("kind", "fixture") ] -> ()
          | _ -> failf "base object lost its metadata");
          let base_nuid = Nats_eio.Object_store.Info.nuid base_info in
          let base_digest = Nats_eio.Object_store.Info.digest base_info in
          let base_buffer = Buffer.create 16 in
          let base_read =
            expect_object_store_ok "object-store get base"
              (Nats_eio.Object_store.get bucket (object_name "base.txt")
                 (Bytesrw.Bytes.Writer.of_buffer base_buffer))
          in
          expect_info_content "base read" ~name:"base.txt" ~payload:base_payload
            ~chunks:4L base_read;
          expect_string "base content" base_payload
            (Buffer.contents base_buffer);
          let updated_meta =
            object_meta ~description:"updated object" ~name:"base.txt"
              ~chunk_size:99 ()
          in
          let updated_info =
            expect_object_store_ok "object-store metadata update"
              (Nats_eio.Object_store.update bucket updated_meta)
          in
          expect_string "updated description" "updated object"
            (Option.value ~default:""
               (Nats_eio.Object_store.Info.description updated_info));
          expect_string "updated nuid" base_nuid
            (Nats_eio.Object_store.Info.nuid updated_info);
          expect_string "updated digest" base_digest
            (Nats_eio.Object_store.Info.digest updated_info);
          expect_int64 "updated size" 10L
            (Nats_eio.Object_store.Info.size updated_info);
          expect_int64 "updated chunks" 4L
            (Nats_eio.Object_store.Info.chunks updated_info);
          expect_int64 "updated chunk size" 3L
            (Int64.of_int (Nats_eio.Object_store.Info.chunk_size updated_info));
          let link =
            match
              Nats_eio.Object_store.Link.v ~bucket:bucket_name
                ~name:(object_name "base.txt") ()
            with
            | Ok value -> value
            | Error error ->
                failf "object-store link: %a"
                  Nats_eio.Object_store.Error.pp_config error
          in
          let alias_info =
            expect_object_store_ok "object-store put link"
              (Nats_eio.Object_store.put_link bucket
                 (object_meta ~name:"alias.txt" ~chunk_size:3 ())
                 link)
          in
          expect_name "link" "alias.txt" alias_info;
          (match Nats_eio.Object_store.Info.link alias_info with
          | Some link
            when String.equal
                   (Nats_eio.Object_store.Link.bucket link)
                   bucket_name
                 &&
                 match Nats_eio.Object_store.Link.name link with
                 | Some name ->
                     String.equal
                       (Nats_eio.Object_store.Name.to_string name)
                       "base.txt"
                 | None -> false ->
              ()
          | Some _ -> failf "link pointed at the wrong object"
          | None -> failf "link metadata did not retain its target");
          let alias_content =
            expect_object_store_ok "object-store get link"
              (Nats_eio.Object_store.get_string bucket (object_name "alias.txt"))
          in
          expect_string "link content" base_payload alias_content;
          let listed =
            expect_object_store_ok "object-store list"
              (Nats_eio.Object_store.list bucket)
          in
          let listed_names =
            List.map
              (fun info ->
                Nats_eio.Object_store.Name.to_string
                  (Nats_eio.Object_store.Info.name info))
              listed
          in
          if not (List.exists (String.equal "base.txt") listed_names) then
            failf "object-store list omitted base.txt; names=[%s]"
              (String.concat "," listed_names);
          if not (List.exists (String.equal "alias.txt") listed_names) then
            failf "object-store list omitted alias.txt; names=[%s]"
              (String.concat "," listed_names);
          expect_object_store_ok "object-store delete target"
            (Nats_eio.Object_store.delete bucket (object_name "base.txt"));
          (match Nats_eio.Object_store.info bucket (object_name "base.txt") with
          | Error Nats_eio.Object_store.Error.Not_found -> ()
          | Ok _ -> failf "deleted object remained visible"
          | Error error ->
              failf "deleted object info: %s" (object_store_error_message error));
          (match
             Nats_eio.Object_store.info ~include_deleted:true bucket
               (object_name "base.txt")
           with
          | Ok info when Nats_eio.Object_store.Info.deleted info -> ()
          | Ok _ -> failf "include_deleted did not return a tombstone"
          | Error error ->
              failf "deleted object tombstone: %s"
                (object_store_error_message error));
          (match
             Nats_eio.Object_store.get_string bucket (object_name "alias.txt")
           with
          | Error (Nats_eio.Object_store.Error.Link_to_deleted _) -> ()
          | Ok _ -> failf "link to deleted object unexpectedly returned content"
          | Error error ->
              failf "link to deleted object: %s"
                (object_store_error_message error));
          expect_object_store_ok "object-store delete alias"
            (Nats_eio.Object_store.delete bucket (object_name "alias.txt"));
          let watch =
            expect_object_store_ok "object-store watch"
              (Nats_eio.Object_store.Watch.v ~sw ~delivery:New bucket)
          in
          let watch_closed = ref false in
          Fun.protect
            ~finally:(fun () ->
              if not !watch_closed then
                ignore (Nats_eio.Object_store.Watch.close watch))
            (fun () ->
              expect_watch_marker "object-store watch" watch;
              let live_payload = "live-object" in
              let live_result, live_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve live_result_u
                    (Nats_eio.Object_store.put_string bucket
                       (object_meta ~name:"live.txt" ~chunk_size:4 ())
                       live_payload));
              let live_info =
                match
                  Nats_eio.Object_store.Watch.next_with_timeout
                    ~timeout:Mtime.Span.(5 * s)
                    watch
                with
                | Ok (Nats_eio.Object_store.Watch.Info info) -> info
                | Ok Nats_eio.Object_store.Watch.Initial_done ->
                    failf "object-store watch repeated its marker"
                | Error error ->
                    failf "object-store live watch: %s"
                      (object_store_error_message error)
              in
              expect_info_content "live watch" ~name:"live.txt"
                ~payload:live_payload ~chunks:3L live_info;
              ignore
                (expect_object_store_ok "live object put"
                   (Eio.Promise.await live_result));
              expect_object_store_ok "object-store watch close"
                (Nats_eio.Object_store.Watch.close watch);
              watch_closed := true);
          expect_object_store_ok "object-store delete live"
            (Nats_eio.Object_store.delete bucket (object_name "live.txt"));
          let sealed =
            expect_object_store_ok "object-store seal"
              (Nats_eio.Object_store.seal bucket)
          in
          if not (Nats_eio.Object_store.Status.sealed sealed) then
            failf "object-store seal did not mark the bucket sealed";
          expect_object_store_ok "object-store delete bucket"
            (Nats_eio.Object_store.delete_bucket bucket);
          (match Nats_eio.Object_store.open_ jetstream ~bucket:bucket_name with
          | Error
              (Nats_eio.Object_store.Error.Jetstream
                 (Nats_eio.Jetstream.Error.Api
                    { code = 404; err_code = Some 10059; _ })) ->
              ()
          | Error error ->
              failf "deleted object-store bucket open: %s"
                (object_store_error_message error)
          | Ok _ -> failf "deleted object-store bucket could be reopened");
          deleted := true;
          print_endline "object_store: ok"))

let () =
  Printexc.record_backtrace true;
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server object-store acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("server object-store acceptance failed: " ^ Printexc.to_string error);
      let backtrace = Printexc.get_backtrace () in
      if String.length backtrace > 0 then prerr_endline backtrace;
      exit 1
