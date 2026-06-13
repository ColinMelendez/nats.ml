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
  | None | Some "seed" | Some "restart" -> false
  | Some "leader" -> true
  | Some value ->
      failf
        "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, leader, or restart, \
         got %S"
        value

let restart () =
  match Sys.getenv_opt "NATS_TEST_JS_CLUSTER_FAILURE_MODE" with
  | Some "restart" -> true
  | None | Some "seed" | Some "leader" -> false
  | Some value ->
      failf
        "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, leader, or restart, \
         got %S"
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
      ~label:({|INFO from |} ^ String.concat "/" names) ~remaining:24
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

let expect_discovered info expected =
  let urls = Nats.Info.connect_urls info in
  List.iter
    (fun value ->
      if not (List.exists (String.equal value) urls) then
        failf "initial INFO did not advertise discovered server %S" value)
    expected

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

let expect_entry label ~key ~value ~revision ~operation entry =
  let actual_key =
    Nats_eio.Key_value.Key.to_string (Nats_eio.Key_value.Entry.key entry)
  in
  if not (String.equal actual_key key) then
    failf "%s key was %S, expected %S" label actual_key key;
  let actual_value = Nats_eio.Key_value.Entry.value entry in
  if not (String.equal actual_value value) then
    failf "%s value was %S, expected %S" label actual_value value;
  let actual_revision = Nats_eio.Key_value.Entry.revision entry in
  if not (Int64.equal actual_revision revision) then
    failf "%s revision was %Ld, expected %Ld" label actual_revision revision;
  match (operation, Nats_eio.Key_value.Entry.operation entry) with
  | Nats_eio.Key_value.Entry.Put, Nats_eio.Key_value.Entry.Put -> ()
  | Nats_eio.Key_value.Entry.Delete, Nats_eio.Key_value.Entry.Delete -> ()
  | Nats_eio.Key_value.Entry.Purge, Nats_eio.Key_value.Entry.Purge -> ()
  | expected, actual ->
      failf "%s operation was %a, expected %a" label
        Nats_eio.Key_value.Entry.pp_operation actual
        Nats_eio.Key_value.Entry.pp_operation expected

let expect_status status bucket =
  if not (String.equal (Nats_eio.Key_value.Status.bucket status) bucket) then
    failf "Key-Value status bucket was %S, expected %S"
      (Nats_eio.Key_value.Status.bucket status)
      bucket;
  if not (Int.equal (Nats_eio.Key_value.Status.replicas status) 3) then
    failf "Key-Value status replicas were %d, expected 3"
      (Nats_eio.Key_value.Status.replicas status);
  (match Nats_eio.Key_value.Status.history status with
  | Some 1L -> ()
  | Some history -> failf "Key-Value status history was %Ld, expected 1" history
  | None -> failf "Key-Value status omitted history");
  match Nats_eio.Key_value.Status.storage status with
  | Nats_eio.Key_value.Config.File -> ()
  | Nats_eio.Key_value.Config.Memory ->
      failf "Key-Value status used memory storage, expected file storage"

let expect_watch_entry ~timeout label watch ~key ~value ~revision =
  match
    Nats_eio.Key_value.Ordered_watch.next_with_timeout ~timeout watch
  with
  | Ok (Nats_eio.Key_value.Ordered_watch.Entry entry) ->
      expect_entry label ~key ~value ~revision
        ~operation:Nats_eio.Key_value.Entry.Put entry
  | Ok Nats_eio.Key_value.Ordered_watch.Initial_done ->
      failf "%s emitted Initial_done before its entry" label
  | Error error -> failf "%s: %s" label (key_value_error_message error)

let expect_initial_done ~timeout label watch =
  match
    Nats_eio.Key_value.Ordered_watch.next_with_timeout ~timeout watch
  with
  | Ok Nats_eio.Key_value.Ordered_watch.Initial_done -> ()
  | Ok (Nats_eio.Key_value.Ordered_watch.Entry entry) ->
      failf "%s emitted %S instead of Initial_done" label
        (Nats_eio.Key_value.Key.to_string
           (Nats_eio.Key_value.Entry.key entry))
  | Error error -> failf "%s: %s" label (key_value_error_message error)

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
      expect_discovered initial_info discovered;
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
        expect_key_value_ok "open replicated Key-Value bucket"
          (Nats_eio.Key_value.open_ jetstream ~bucket:bucket_name)
      in
      let status =
        expect_key_value_ok "initial replicated Key-Value status"
          (Nats_eio.Key_value.status bucket)
      in
      expect_status status bucket_name;
      let watch =
        expect_key_value_ok "open ordered Key-Value watch"
          (Nats_eio.Key_value.Ordered_watch.v ~sw ~key:"watch"
             ~delivery:Nats_eio.Key_value.Ordered_watch.Last_per_subject
             ~name_prefix:"kv-ordered-reconnect" bucket)
      in
      let watch_closed = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !watch_closed then
            match Nats_eio.Key_value.Ordered_watch.close watch with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "ordered Key-Value cleanup failed: %a"
                     Nats_eio.Key_value.Error.pp error))
        (fun () ->
          expect_watch_entry ~timeout "retained ordered Key-Value entry" watch
            ~key:"watch" ~value:"before-failover" ~revision:1L;
          expect_initial_done ~timeout "ordered Key-Value initial marker" watch;
          expect_ok "flush ordered Key-Value setup"
            (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Key-Value ordered reconnect peer"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".start")) "start")
          in
          expect_payload "Key-Value start response" "started" start_response;
          let baseline_response =
            expect_ok "confirm Key-Value baseline"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".baseline"))
                 "baseline-complete")
          in
          expect_payload "Key-Value baseline response" "go-baseline-ready"
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
          let recovery_result, recovery_result_u = Eio.Promise.create () in
          Eio.Fiber.fork ~sw (fun () ->
              Eio.Promise.resolve recovery_result_u
                (Nats_eio.Key_value.Ordered_watch.next_with_timeout
                   ~timeout:reconnect_timeout watch));
          let recovery_response =
            request_until_response ~clock ~timeout:recovery_request_timeout
              ~failure_file ~label:"confirm Key-Value ordered recovery"
              connection
              (Nats.Subject.literal (prefix ^ ".recovery-ready"))
              (if leader_failover then "ocaml-leader-failover"
               else "ocaml-reconnected")
          in
          expect_payload "Key-Value recovery response" "go-recovery-ready"
            recovery_response;
          let recovery_event =
            expect_key_value_ok "receive post-failover Key-Value entry"
              (Eio.Promise.await recovery_result)
          in
          (match recovery_event with
          | Nats_eio.Key_value.Ordered_watch.Entry entry ->
              expect_entry "post-failover ordered Key-Value entry" ~key:"watch"
                ~value:"after-failover" ~revision:2L
                ~operation:Nats_eio.Key_value.Entry.Put entry
          | Nats_eio.Key_value.Ordered_watch.Initial_done ->
              failf "post-failover ordered watch repeated Initial_done");
          let entry_response =
            expect_ok "acknowledge post-failover Key-Value entry"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".entry-seen"))
                 "ocaml-entry-seen")
          in
          expect_payload "Key-Value entry acknowledgement" "go-entry-validated"
            entry_response;
          let close_response =
            expect_ok "close Go Key-Value ordered session"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".close")) "ocaml-close")
          in
          expect_payload "Go Key-Value close response" "go-closed" close_response;
          expect_key_value_ok "close ordered Key-Value watch"
            (Nats_eio.Key_value.Ordered_watch.close watch);
          watch_closed := true;
          expect_ok "flush after ordered Key-Value close"
            (Nats_eio.Connection.flush connection);
          let cleanup_response =
            expect_ok "request Key-Value cleanup"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".cleanup")) "cleanup")
          in
          expect_payload "Key-Value cleanup response" "cleaned" cleanup_response;
          print_endline "interop-key-value-ordered-reconnect: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline
        ("Key-Value ordered reconnect interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("Key-Value ordered reconnect interop acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
