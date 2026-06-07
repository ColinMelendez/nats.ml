let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let object_error_message error =
  Format.asprintf "%a" Nats_eio.Object_store.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_object_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (object_error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let required name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> value
  | Some _ | None -> failf "%s is required" name

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let request_control ~timeout connection prefix suffix payload =
  expect_ok ("request " ^ suffix)
    (Nats_eio.Connection.request ~timeout connection
       (Nats.Subject.literal (prefix ^ suffix))
       payload)

let expect_status status bucket =
  if not (String.equal (Nats_eio.Object_store.Status.bucket status) bucket) then
    failf "Object Store status bucket was %S, expected %S"
      (Nats_eio.Object_store.Status.bucket status)
      bucket;
  (match Nats_eio.Object_store.Status.description status with
  | Some description when String.equal description "interop" -> ()
  | Some description ->
      failf "Object Store status description was %S, expected %S" description
        "interop"
  | None -> failf "Object Store status omitted its description");
  (match Nats_eio.Object_store.Status.storage status with
  | Nats_eio.Object_store.Config.Memory -> ()
  | Nats_eio.Object_store.Config.File ->
      failf "Object Store status used file storage, expected memory");
  (match Nats_eio.Object_store.Status.metadata status with
  | [ (key, value) ]
    when String.equal key "owner" && String.equal value "interop" ->
      ()
  | metadata ->
      failf
        "Object Store status metadata had %d entries, expected owner=interop"
        (List.length metadata));
  if Nats_eio.Object_store.Status.sealed status then
    failf "Object Store status was unexpectedly sealed"

let expect_info label ~bucket ~name ~size ~chunks info =
  let actual_bucket = Nats_eio.Object_store.Info.bucket info in
  if not (String.equal actual_bucket bucket) then
    failf "%s bucket was %S, expected %S" label actual_bucket bucket;
  let actual_name =
    Nats_eio.Object_store.Name.to_string (Nats_eio.Object_store.Info.name info)
  in
  if not (String.equal actual_name name) then
    failf "%s name was %S, expected %S" label actual_name name;
  let actual_size = Nats_eio.Object_store.Info.size info in
  if not (Int64.equal actual_size size) then
    failf "%s size was %Ld, expected %Ld" label actual_size size;
  let actual_chunks = Nats_eio.Object_store.Info.chunks info in
  if not (Int64.equal actual_chunks chunks) then
    failf "%s chunks were %Ld, expected %Ld" label actual_chunks chunks

let expect_header label expected headers name =
  match Nats.Header.find name headers with
  | Some actual when String.equal actual expected -> ()
  | Some actual -> failf "%s header was %S, expected %S" label actual expected
  | None -> failf "%s header was missing" label

let find_metadata key metadata =
  List.find_map
    (fun (actual_key, value) ->
      if String.equal actual_key key then Some value else None)
    metadata

let expect_metadata label expected metadata key =
  match find_metadata key metadata with
  | Some actual when String.equal actual expected -> ()
  | Some actual ->
      failf "%s metadata %S was %S, expected %S" label key actual expected
  | None -> failf "%s metadata %S was missing" label key

let expect_link label info ~bucket ~name =
  match Nats_eio.Object_store.Info.link info with
  | None -> failf "%s did not contain a link" label
  | Some link -> (
      let actual_bucket = Nats_eio.Object_store.Link.bucket link in
      if not (String.equal actual_bucket bucket) then
        failf "%s target bucket was %S, expected %S" label actual_bucket bucket;
      match Nats_eio.Object_store.Link.name link with
      | Some actual_name
        when String.equal
               (Nats_eio.Object_store.Name.to_string actual_name)
               name ->
          ()
      | Some actual_name ->
          failf "%s target name was %S, expected %S" label
            (Nats_eio.Object_store.Name.to_string actual_name)
            name
      | None -> failf "%s omitted its target object name" label)

let expect_initial_snapshot ~timeout label watch ~bucket =
  let seen_go = ref false in
  let seen_ocaml = ref false in
  let complete = ref false in
  while not !complete do
    match Nats_eio.Object_store.Watch.next_with_timeout ~timeout watch with
    | Ok Nats_eio.Object_store.Watch.Initial_done -> complete := true
    | Ok (Nats_eio.Object_store.Watch.Info info) ->
        let name =
          Nats_eio.Object_store.Name.to_string
            (Nats_eio.Object_store.Info.name info)
        in
        if String.equal name "go.txt" && not !seen_go then (
          seen_go := true;
          expect_info "initial Go object" ~bucket ~name ~size:7L ~chunks:1L info)
        else if String.equal name "ocaml.txt" && not !seen_ocaml then (
          seen_ocaml := true;
          expect_info "initial OCaml object" ~bucket ~name ~size:10L ~chunks:5L
            info)
        else failf "%s emitted unexpected initial object %S" label name
    | Error error -> failf "%s: %s" label (object_error_message error)
  done;
  if (not !seen_go) || not !seen_ocaml then
    failf "%s omitted an initial object (go=%b, ocaml=%b)" label !seen_go
      !seen_ocaml

let expect_watch_info ~timeout label watch =
  match Nats_eio.Object_store.Watch.next_with_timeout ~timeout watch with
  | Ok (Nats_eio.Object_store.Watch.Info info) -> info
  | Ok Nats_eio.Object_store.Watch.Initial_done ->
      failf "%s repeated its initial marker" label
  | Error error -> failf "%s: %s" label (object_error_message error)

let expect_name label expected infos =
  if
    not
      (List.exists
         (fun info ->
           String.equal
             (Nats_eio.Object_store.Name.to_string
                (Nats_eio.Object_store.Info.name info))
             expected)
         infos)
  then failf "%s did not include %S" label expected

let object_name label value =
  match Nats_eio.Object_store.Name.of_string value with
  | Ok name -> name
  | Error error ->
      failf "%s: %a" label Nats_eio.Object_store.Error.pp_name error

let object_meta label ~name ?description ?headers ?(metadata = [])
    ?(chunk_size = 128 * 1024) () =
  match
    Nats_eio.Object_store.Meta.v ~name ?description ?headers ~metadata
      ~chunk_size ()
  with
  | Ok meta -> meta
  | Error error ->
      failf "%s: %a" label Nats_eio.Object_store.Error.pp_meta error

let link_target label ~bucket ~name =
  match
    Nats_eio.Object_store.Link.v ~bucket
      ~name:(object_name (label ^ " target") name)
      ()
  with
  | Ok link -> link
  | Error error ->
      failf "%s: %a" label Nats_eio.Object_store.Error.pp_config error

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(10 * s) in
  let bucket_name = required "NATS_TEST_INTEROP_BUCKET" in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let endpoint_value = required "NATS_TEST_SERVER" in
  let endpoint =
    match Nats.Endpoint.of_string endpoint_value with
    | Ok endpoint -> endpoint
    | Error error ->
        failf "invalid NATS_TEST_SERVER %S: %a" endpoint_value
          Nats.Endpoint.pp_error error
  in
  let auth = Interop_auth.auth () in
  let tls = Interop_auth.tls_config () in
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
        expect_object_ok "open Object Store"
          (Nats_eio.Object_store.open_ jetstream ~bucket:bucket_name)
      in
      let status =
        expect_object_ok "initial Object Store status"
          (Nats_eio.Object_store.status bucket)
      in
      expect_status status bucket_name;
      expect_ok "flush Object Store setup"
        (Nats_eio.Connection.flush connection);
      let start_response =
        request_control ~timeout connection prefix ".start" "start"
      in
      expect_payload "Go Object Store start" "started" start_response;

      let go_name = object_name "Go object" "go.txt" in
      let go_payload =
        expect_object_ok "get Go object"
          (Nats_eio.Object_store.get_string bucket go_name)
      in
      if not (String.equal go_payload "from-go") then
        failf "Go object payload was %S, expected %S" go_payload "from-go";
      let go_info =
        expect_object_ok "get Go object info"
          (Nats_eio.Object_store.info bucket go_name)
      in
      expect_info "Go object" ~bucket:bucket_name ~name:"go.txt" ~size:7L
        ~chunks:1L go_info;

      let ocaml_name = object_name "OCaml object" "ocaml.txt" in
      let headers =
        match Nats.Header.of_list [ ("X-Object", "ocaml") ] with
        | Ok headers -> headers
        | Error error ->
            failf "Object Store headers: %a" Nats.Header.pp_error error
      in
      let ocaml_meta =
        object_meta "OCaml object metadata" ~name:ocaml_name
          ~description:"from-ocaml" ~headers
          ~metadata:[ ("origin", "ocaml") ]
          ~chunk_size:2 ()
      in
      let ocaml_info =
        expect_object_ok "put OCaml object"
          (Nats_eio.Object_store.put_string bucket ocaml_meta "from-ocaml")
      in
      expect_info "OCaml object" ~bucket:bucket_name ~name:"ocaml.txt" ~size:10L
        ~chunks:5L ocaml_info;
      (match Nats_eio.Object_store.Info.description ocaml_info with
      | Some description when String.equal description "from-ocaml" -> ()
      | Some description ->
          failf "OCaml object description was %S, expected %S" description
            "from-ocaml"
      | None -> failf "OCaml object omitted its description");
      expect_header "OCaml object" "ocaml"
        (Nats_eio.Object_store.Info.headers ocaml_info)
        "X-Object";
      expect_metadata "OCaml object" "ocaml"
        (Nats_eio.Object_store.Info.metadata ocaml_info)
        "origin";
      if not (Int.equal (Nats_eio.Object_store.Info.chunk_size ocaml_info) 2)
      then
        failf "OCaml object chunk size was %d, expected 2"
          (Nats_eio.Object_store.Info.chunk_size ocaml_info);
      let written_response =
        request_control ~timeout connection prefix ".ocaml-written" "written"
      in
      expect_payload "Go Object Store write validation" "go-validated"
        written_response;

      let watch =
        expect_object_ok "watch Object Store"
          (Nats_eio.Object_store.Watch.v ~sw
             ~delivery:Nats_eio.Object_store.Watch.Last_per_subject bucket)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Object_store.Watch.close watch))
        (fun () ->
          expect_initial_snapshot ~timeout "Object Store watch" watch
            ~bucket:bucket_name;
          let go_watch_response =
            request_control ~timeout connection prefix ".go-watch" "write"
          in
          expect_payload "Go Object Store watch write" "written"
            go_watch_response;
          let go_watch_info =
            expect_watch_info ~timeout "OCaml Go Object Store watch" watch
          in
          expect_info "OCaml Go Object Store watch" ~bucket:bucket_name
            ~name:"watch.go" ~size:13L ~chunks:1L go_watch_info;
          let ocaml_watch_name =
            object_name "OCaml watch object" "watch.ocaml"
          in
          let ocaml_watch_meta =
            object_meta "OCaml watch metadata" ~name:ocaml_watch_name ()
          in
          let ocaml_watch_info =
            expect_object_ok "put OCaml watch object"
              (Nats_eio.Object_store.put_string bucket ocaml_watch_meta
                 "from-ocaml-watch")
          in
          expect_info "OCaml watch object" ~bucket:bucket_name
            ~name:"watch.ocaml" ~size:16L ~chunks:1L ocaml_watch_info;
          let watch_response =
            request_control ~timeout connection prefix ".ocaml-watch" "written"
          in
          expect_payload "Go Object Store watch validation" "watch-seen"
            watch_response);

      let go_link_response =
        request_control ~timeout connection prefix ".go-link" "link"
      in
      expect_payload "Go Object Store link" "linked" go_link_response;
      let go_link_name = object_name "Go link" "go-link" in
      let go_link_info =
        expect_object_ok "get Go object link"
          (Nats_eio.Object_store.info bucket go_link_name)
      in
      expect_info "Go object link" ~bucket:bucket_name ~name:"go-link" ~size:0L
        ~chunks:0L go_link_info;
      expect_link "Go object link" go_link_info ~bucket:bucket_name
        ~name:"go.txt";
      let go_link_payload =
        expect_object_ok "get Go object link payload"
          (Nats_eio.Object_store.get_string bucket go_link_name)
      in
      if not (String.equal go_link_payload "from-go") then
        failf "Go object link payload was %S, expected %S" go_link_payload
          "from-go";

      let ocaml_link_name = object_name "OCaml link" "ocaml-link" in
      let ocaml_link_meta =
        object_meta "OCaml link metadata" ~name:ocaml_link_name ()
      in
      let ocaml_link_target =
        link_target "OCaml link" ~bucket:bucket_name ~name:"ocaml.txt"
      in
      let ocaml_link_info =
        expect_object_ok "put OCaml object link"
          (Nats_eio.Object_store.put_link bucket ocaml_link_meta
             ocaml_link_target)
      in
      expect_info "OCaml object link" ~bucket:bucket_name ~name:"ocaml-link"
        ~size:0L ~chunks:0L ocaml_link_info;
      expect_link "OCaml object link" ocaml_link_info ~bucket:bucket_name
        ~name:"ocaml.txt";
      let ocaml_link_response =
        request_control ~timeout connection prefix ".ocaml-link" "linked"
      in
      expect_payload "Go Object Store link validation" "go-validated"
        ocaml_link_response;
      let listed =
        expect_object_ok "list Object Store" (Nats_eio.Object_store.list bucket)
      in
      expect_name "Object Store list" "go.txt" listed;
      expect_name "Object Store list" "ocaml.txt" listed;
      expect_name "Object Store list" "watch.go" listed;
      expect_name "Object Store list" "watch.ocaml" listed;
      expect_name "Object Store list" "go-link" listed;
      expect_name "Object Store list" "ocaml-link" listed;

      expect_object_ok "delete Go object"
        (Nats_eio.Object_store.delete bucket go_name);
      (match Nats_eio.Object_store.info bucket go_name with
      | Error Nats_eio.Object_store.Error.Not_found -> ()
      | Ok _ -> failf "deleted Go object remained visible"
      | Error error ->
          failf "deleted Go object: %s" (object_error_message error));
      let deleted_info =
        expect_object_ok "get deleted Go object info"
          (Nats_eio.Object_store.info ~include_deleted:true bucket go_name)
      in
      if not (Nats_eio.Object_store.Info.deleted deleted_info) then
        failf "deleted Go object info was not marked deleted";
      let delete_response =
        request_control ~timeout connection prefix ".ocaml-delete" "deleted"
      in
      expect_payload "Go Object Store delete validation" "go-validated"
        delete_response;

      let sealed =
        expect_object_ok "seal Object Store" (Nats_eio.Object_store.seal bucket)
      in
      if not (Nats_eio.Object_store.Status.sealed sealed) then
        failf "Object Store seal did not report sealed status";
      let sealed_response =
        request_control ~timeout connection prefix ".ocaml-sealed" "sealed"
      in
      expect_payload "Go Object Store seal validation" "go-validated"
        sealed_response;
      let done_response =
        request_control ~timeout connection prefix ".done" "done"
      in
      expect_payload "Object Store completion" "go-validated" done_response)

let () = Eio_main.run run
