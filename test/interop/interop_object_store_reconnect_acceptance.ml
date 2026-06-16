let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let object_error_message error =
  Format.asprintf "%a" Nats_eio.Object_store.Error.pp error

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

let endpoint () =
  let value = required "NATS_TEST_SERVER" in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

let leader_failover () =
  match Sys.getenv_opt "NATS_TEST_JS_CLUSTER_FAILURE_MODE" with
  | None
  | Some "seed"
  | Some "node-a"
  | Some "node-b"
  | Some "node-c"
  | Some "restart" ->
      false
  | Some "leader" -> true
  | Some value ->
      failf
        "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, node-a, node-b, \
         node-c, restart, or leader, got %S"
        value

let restart () =
  match Sys.getenv_opt "NATS_TEST_JS_CLUSTER_FAILURE_MODE" with
  | Some "restart" -> true
  | None
  | Some "seed"
  | Some "node-a"
  | Some "node-b"
  | Some "node-c"
  | Some "leader" ->
      false
  | Some value ->
      failf
        "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, node-a, node-b, \
         node-c, restart, or leader, got %S"
        value

let string_list name =
  required name |> String.split_on_char ','
  |> List.filter (fun value -> not (String.equal value ""))

let touch path =
  let output = open_out path in
  close_out output

let next_event ~clock ~timeout ~failure_file events =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  match
    Eio.Fiber.first
      (fun () ->
        Eio.Fiber.first
          (fun () -> Nats_eio.Event_stream.next events)
          (fun () ->
            while not (Sys.file_exists failure_file) do
              Eio.Time.Mono.sleep clock 0.05
            done;
            failf "cluster watcher failed (see %s)" failure_file))
      (fun () ->
        Eio.Time.Mono.sleep clock seconds;
        Error Nats_eio.Error.Timeout)
  with
  | Ok event -> event
  | Error error -> failf "lifecycle event: %s" (error_message error)

let wait_for_event ~clock ~timeout ~failure_file ~label ~remaining predicate
    events =
  let remaining = ref remaining in
  let result = ref None in
  while !remaining > 0 && Option.is_none !result do
    let event = next_event ~clock ~timeout ~failure_file events in
    if predicate event then result := Some event else decr remaining
  done;
  match !result with
  | Some event -> event
  | None -> failf "timed out waiting for %s" label

let expect_server_info ~clock ~timeout ~failure_file ~names events =
  let event =
    wait_for_event ~clock ~timeout ~failure_file
      ~label:({|INFO from |} ^ String.concat "/" names)
      ~remaining:24
      (function
        | Nats_eio.Event.Core (Nats.Event.Info info) ->
            Option.fold ~none:false
              ~some:(fun name -> List.exists (String.equal name) names)
              (Nats.Info.server_name info)
        | _ -> false)
      events
  in
  match event with
  | Nats_eio.Event.Core (Nats.Event.Info info) -> info
  | _ -> assert false

let expect_disconnected ~clock ~timeout ~failure_file events =
  ignore
    (wait_for_event ~clock ~timeout ~failure_file ~label:"disconnect"
       ~remaining:24
       (function Nats_eio.Event.Disconnected -> true | _ -> false)
       events)

let expect_reconnected ~clock ~timeout ~failure_file events =
  ignore
    (wait_for_event ~clock ~timeout ~failure_file ~label:"reconnect"
       ~remaining:24
       (function Nats_eio.Event.Reconnected -> true | _ -> false)
       events)

let wait_for_file ~clock ~timeout ~failure_file ~label path =
  let deadline =
    match Mtime.add_span (Eio.Time.Mono.now clock) timeout with
    | Some value -> value
    | None -> Mtime.max_stamp
  in
  let found = ref false in
  while not !found do
    if Sys.file_exists failure_file then
      failf "cluster watcher failed (see %s)" failure_file
    else if Sys.file_exists path then found := true
    else if Mtime.compare (Eio.Time.Mono.now clock) deadline >= 0 then
      failf "timed out waiting for %s" label
    else Eio.Time.Mono.sleep clock 0.05
  done

let request_until_response ~clock ~timeout ~failure_file ~label connection
    subject payload =
  let deadline =
    match Mtime.add_span (Nats_eio.Connection.now connection) timeout with
    | Some value -> value
    | None -> Mtime.max_stamp
  in
  let last_error = ref None in
  let result = ref None in
  while Option.is_none !result do
    if Sys.file_exists failure_file then
      failf "cluster watcher failed (see %s)" failure_file
    else
      let now = Nats_eio.Connection.now connection in
      if Mtime.compare now deadline >= 0 then
        result := Some (Error Nats_eio.Error.Timeout)
      else
        let remaining = Mtime.span now deadline in
        match
          Nats_eio.Connection.request ~timeout:remaining connection subject
            payload
        with
        | Ok value -> result := Some (Ok value)
        | Error
            (( Nats_eio.Error.No_responders | Nats_eio.Error.Timeout
             | Nats_eio.Error.Disconnected ) as error) ->
            last_error := Some error;
            Eio.Time.Mono.sleep clock 0.2
        | Error error -> result := Some (Error error)
  done;
  match !result with
  | Some (Ok value) -> value
  | Some (Error Nats_eio.Error.Timeout) -> (
      match !last_error with
      | Some error -> failf "%s: %s" label (error_message error)
      | None -> failf "%s: operation timed out" label)
  | Some (Error error) -> failf "%s: %s" label (error_message error)
  | None -> failf "%s returned no result" label

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let object_name label value =
  match Nats_eio.Object_store.Name.of_string value with
  | Ok name -> name
  | Error error ->
      failf "%s: %a" label Nats_eio.Object_store.Error.pp_name error

let object_meta label ~name ~chunk_size =
  match Nats_eio.Object_store.Meta.v ~name ~chunk_size () with
  | Ok meta -> meta
  | Error error ->
      failf "%s: %a" label Nats_eio.Object_store.Error.pp_meta error

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

let expect_status status bucket =
  if not (String.equal (Nats_eio.Object_store.Status.bucket status) bucket) then
    failf "Object Store status named the wrong bucket";
  (match Nats_eio.Object_store.Status.description status with
  | Some description when String.equal description "interop-cluster" -> ()
  | Some description ->
      failf "Object Store description was %S, expected %S" description
        "interop-cluster"
  | None -> failf "Object Store status omitted its description");
  (match Nats_eio.Object_store.Status.storage status with
  | Nats_eio.Object_store.Config.File -> ()
  | Nats_eio.Object_store.Config.Memory ->
      failf "Object Store status used memory storage, expected file storage");
  if not (Int.equal (Nats_eio.Object_store.Status.replicas status) 3) then
    failf "Object Store status did not report three replicas";
  if
    not
      (List.exists
         (fun (key, value) ->
           String.equal key "owner" && String.equal value "interop-cluster")
         (Nats_eio.Object_store.Status.metadata status))
  then failf "Object Store status omitted owner=interop-cluster metadata";
  if Nats_eio.Object_store.Status.sealed status then
    failf "Object Store status was unexpectedly sealed"

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(90 * s) in
  let reconnect_timeout = Mtime.Span.(90 * s) in
  let recovery_request_timeout = Mtime.Span.(3 * reconnect_timeout) in
  let bucket_name = required "NATS_TEST_INTEROP_BUCKET" in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let signal = required "NATS_TEST_INTEROP_SIGNAL" in
  let leader_failover = leader_failover () in
  let restart = restart () in
  let failure_file = signal ^ ".failed" in
  let initial_name = required "NATS_TEST_JS_CLUSTER_INITIAL_NAME" in
  let recovered_names = string_list "NATS_TEST_JS_CLUSTER_RECOVERED_NAMES" in
  let discovered = string_list "NATS_TEST_JS_CLUSTER_DISCOVERED" in
  if List.length recovered_names <> 2 then
    failf "NATS_TEST_JS_CLUSTER_RECOVERED_NAMES must contain two names";
  if List.length discovered <> 2 then
    failf "NATS_TEST_JS_CLUSTER_DISCOVERED must contain two endpoints";
  let auth = Interop_auth.auth () in
  let tls = Interop_auth.tls_config () in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 100)
         ~reconnect_delay:Mtime.Span.(100 * ms)
         ~reconnect_max_delay:Mtime.Span.(500 * ms)
         ?auth ?tls ())
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config [ endpoint () ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let events = Nats_eio.Connection.events connection in
      let initial_info =
        expect_server_info ~clock ~timeout ~failure_file ~names:[ initial_name ]
          events
      in
      let advertised = Nats.Info.connect_urls initial_info in
      List.iter
        (fun expected ->
          if not (List.exists (String.equal expected) advertised) then
            failf "initial INFO did not advertise discovered server %S" expected)
        discovered;
      ignore
        (wait_for_event ~clock ~timeout ~failure_file
           ~label:"initial connection" ~remaining:24
           (function
             | Nats_eio.Event.Core Nats.Event.Connected -> true | _ -> false)
           events);
      let jetstream =
        expect_jetstream_ok "JetStream" (Nats_eio.Jetstream.v connection)
      in
      let bucket =
        expect_object_ok "open replicated Object Store"
          (Nats_eio.Object_store.open_ jetstream ~bucket:bucket_name)
      in
      let initial_status =
        expect_object_ok "initial Object Store status"
          (Nats_eio.Object_store.status bucket)
      in
      expect_status initial_status bucket_name;
      let start_response =
        expect_ok "start Object Store reconnect peer"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".start"))
             "start")
      in
      expect_payload "Object Store start response" "started" start_response;
      let go_before = object_name "pre-failover Go object" "go-before" in
      let go_payload =
        expect_object_ok "get pre-failover Go object"
          (Nats_eio.Object_store.get_string bucket go_before)
      in
      if not (String.equal go_payload "from-go-before") then
        failf "pre-failover Go object was %S, expected %S" go_payload
          "from-go-before";
      let ocaml_before =
        object_name "pre-failover OCaml object" "ocaml-before"
      in
      let ocaml_before_meta =
        object_meta "pre-failover OCaml metadata" ~name:ocaml_before
          ~chunk_size:4
      in
      let ocaml_before_info =
        expect_object_ok "put pre-failover OCaml object"
          (Nats_eio.Object_store.put_string bucket ocaml_before_meta
             "from-ocaml-before")
      in
      expect_info "pre-failover OCaml object" ~bucket:bucket_name
        ~name:"ocaml-before" ~size:17L ~chunks:5L ocaml_before_info;
      let baseline_response =
        expect_ok "confirm Object Store baseline"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".baseline"))
             "baseline-complete")
      in
      expect_payload "Object Store baseline response" "go-baseline-ready"
        baseline_response;
      touch (signal ^ ".1");
      if leader_failover then
        wait_for_file ~clock ~timeout:reconnect_timeout ~failure_file
          ~label:"leader kill" (signal ^ ".killed")
      else (
        expect_disconnected ~clock ~timeout:reconnect_timeout ~failure_file
          events;
        let recovered_info =
          expect_server_info ~clock ~timeout:reconnect_timeout ~failure_file
            ~names:recovered_names events
        in
        expect_reconnected ~clock ~timeout:reconnect_timeout ~failure_file
          events;
        (match Nats.Info.server_name recovered_info with
        | Some value when not (String.equal value initial_name) -> ()
        | Some value -> failf "reconnected to killed server %S" value
        | None -> failf "reconnect INFO had no server name");
        if restart then touch (signal ^ ".ocaml-reconnected"));
      let recovery_response =
        request_until_response ~clock ~timeout:recovery_request_timeout
          ~failure_file ~label:"confirm Object Store recovery" connection
          (Nats.Subject.literal (prefix ^ ".recovery-ready"))
          (if leader_failover then "ocaml-leader-failover"
           else "ocaml-reconnected")
      in
      expect_payload "Object Store recovery response" "go-recovery-ready"
        recovery_response;
      let go_after = object_name "post-failover Go object" "go-after" in
      let go_after_payload =
        expect_object_ok "get post-failover Go object"
          (Nats_eio.Object_store.get_string ~timeout:reconnect_timeout bucket
             go_after)
      in
      if not (String.equal go_after_payload "from-go-after") then
        failf "post-failover Go object was %S, expected %S" go_after_payload
          "from-go-after";
      let ocaml_after =
        object_name "post-failover OCaml object" "ocaml-after"
      in
      let ocaml_after_meta =
        object_meta "post-failover OCaml metadata" ~name:ocaml_after
          ~chunk_size:4
      in
      let ocaml_after_info =
        expect_object_ok "put post-failover OCaml object"
          (Nats_eio.Object_store.put_string ~timeout:reconnect_timeout bucket
             ocaml_after_meta "from-ocaml-after")
      in
      expect_info "post-failover OCaml object" ~bucket:bucket_name
        ~name:"ocaml-after" ~size:16L ~chunks:4L ocaml_after_info;
      let after_response =
        expect_ok "confirm post-failover Object Store object"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".after-seen"))
             "ocaml-after-seen")
      in
      expect_payload "post-failover Object Store response" "go-after-validated"
        after_response;
      let cleanup_response =
        expect_ok "request Object Store cleanup"
          (Nats_eio.Connection.request ~timeout connection
             (Nats.Subject.literal (prefix ^ ".cleanup"))
             "cleanup")
      in
      expect_payload "Object Store cleanup response" "cleaned" cleanup_response;
      print_endline "interop-object-store-reconnect: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline
        ("Object Store reconnect interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("Object Store reconnect interop acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
