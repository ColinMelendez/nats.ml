let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let required name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> value
  | Some _ | None -> failf "%s is required" name

let leader_failover () =
  match Sys.getenv_opt "NATS_TEST_JS_CLUSTER_FAILURE_MODE" with
  | None
  | Some "seed"
  | Some "node-a"
  | Some "node-b"
  | Some "node-c"
  | Some "restart"
  | Some "multi-node" -> false
  | Some "leader" -> true
  | Some value ->
      failf
        "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, node-a, node-b, \
         node-c, leader, restart, or multi-node, got %S"
        value

let multi_node_loss () =
  match Sys.getenv_opt "NATS_TEST_JS_CLUSTER_FAILURE_MODE" with
  | Some "multi-node" -> true
  | None
  | Some "seed"
  | Some "node-a"
  | Some "node-b"
  | Some "node-c"
  | Some "leader"
  | Some "restart" -> false
  | Some value ->
      failf
        "NATS_TEST_JS_CLUSTER_FAILURE_MODE must be seed, node-a, node-b, \
         node-c, leader, restart, or multi-node, got %S"
        value

let endpoint () =
  let value = required "NATS_TEST_SERVER" in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

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
      ~label:("INFO from " ^ String.concat "/" names)
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

let expect_header label expected headers name =
  match Nats.Header.find name headers with
  | Some actual when String.equal actual expected -> ()
  | Some actual -> failf "%s header was %S, expected %S" label actual expected
  | None -> failf "%s header was missing" label

let expect_ordered_delivery label ~stream ~subject ~consumer ~payload ~interop
    ~trace ~stream_sequence ~consumer_sequence message =
  expect_payload label payload (Nats_eio.Jetstream.Msg.message message);
  let actual_subject =
    Nats.Subject.to_string (Nats_eio.Jetstream.Msg.subject message)
  in
  if not (String.equal actual_subject subject) then
    failf "%s subject was %S, expected %S" label actual_subject subject;
  expect_header (label ^ " X-Interop") interop
    (Nats_eio.Jetstream.Msg.headers message)
    "X-Interop";
  expect_header (label ^ " X-Trace") trace
    (Nats_eio.Jetstream.Msg.headers message)
    "X-Trace";
  if not (String.equal (Nats_eio.Jetstream.Msg.stream message) stream) then
    failf "%s named the wrong stream" label;
  let actual_consumer = Nats_eio.Jetstream.Msg.consumer message in
  if not (String.equal actual_consumer consumer) then
    failf "%s named consumer %S, expected %S" label actual_consumer consumer;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Msg.stream_sequence message)
         stream_sequence)
  then failf "%s had the wrong stream sequence" label;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Msg.consumer_sequence message)
         consumer_sequence)
  then failf "%s had the wrong consumer sequence" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.num_delivered message) 1L) then
    failf "%s had the wrong delivery count" label

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(30 * s) in
  let reconnect_timeout = Mtime.Span.(90 * s) in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let stream_name = required "NATS_TEST_INTEROP_STREAM" in
  let signal = required "NATS_TEST_INTEROP_SIGNAL" in
  let leader_failover = leader_failover () in
  let multi_node_loss = multi_node_loss () in
  let restart =
    match Sys.getenv_opt "NATS_TEST_JS_CLUSTER_FAILURE_MODE" with
    | Some "restart" -> true
    | None | Some _ -> false
  in
  let failure_file = signal ^ ".failed" in
  let initial_name = required "NATS_TEST_JS_CLUSTER_INITIAL_NAME" in
  let recovered_names =
    required "NATS_TEST_JS_CLUSTER_RECOVERED_NAMES"
    |> String.split_on_char ','
    |> List.filter (fun value -> not (String.equal value ""))
  in
  let discovered = string_list "NATS_TEST_JS_CLUSTER_DISCOVERED" in
  if List.length recovered_names <> 2 then
    failf "NATS_TEST_JS_CLUSTER_RECOVERED_NAMES must contain two names";
  if List.length discovered <> 2 then
    failf "NATS_TEST_JS_CLUSTER_DISCOVERED must contain two endpoints";
  let endpoint = endpoint () in
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
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config [ endpoint ])
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
        expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v connection)
      in
      let stream =
        expect_jetstream_ok "bind stream"
          (Nats_eio.Jetstream.Stream.bind jetstream ~name:stream_name)
      in
      let match_subject = prefix ^ ".match" in
      let ordered =
        expect_jetstream_ok "open OCaml ordered session"
          (Nats_eio.Jetstream.Consumer.Ordered.v ~sw
             ~filter_subject:(Nats.Subject.Filter.literal match_subject)
             stream)
      in
      let ordered_closed = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !ordered_closed then
            match Nats_eio.Jetstream.Consumer.Ordered.close ordered with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "ordered cleanup failed: %a"
                     Nats_eio.Jetstream.Error.pp error))
        (fun () ->
          expect_ok "ordered setup flush" (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start ordered reconnect peer"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".start"))
                 "start")
          in
          expect_payload "start response" "started" start_response;
          let first =
            expect_jetstream_ok "receive first ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          let consumer_name = Nats_eio.Jetstream.Msg.consumer first in
          let second =
            expect_jetstream_ok "receive second ordered message"
              (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                 ordered)
          in
          expect_ordered_delivery "OCaml ordered first" ~stream:stream_name
            ~subject:match_subject ~consumer:consumer_name
            ~payload:"go-before-one" ~interop:"go-ordered-reconnect"
            ~trace:"go-before-one" ~stream_sequence:1L ~consumer_sequence:1L
            first;
          expect_ordered_delivery "OCaml ordered second" ~stream:stream_name
            ~subject:match_subject ~consumer:consumer_name
            ~payload:"go-before-three" ~interop:"go-ordered-reconnect"
            ~trace:"go-before-three" ~stream_sequence:3L ~consumer_sequence:2L
            second;
          let baseline_response =
            expect_ok "confirm ordered baseline"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".baseline"))
                 "baseline-complete")
          in
          expect_payload "baseline response" "go-baseline-ready"
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
            if restart || multi_node_loss then (
              touch (signal ^ ".ocaml-reconnected");
              if multi_node_loss then
                wait_for_file ~clock ~timeout:reconnect_timeout ~failure_file
                  ~label:"multi-node recovery" (signal ^ ".multi-node-recovered")));
          let after_result, after_result_u = Eio.Promise.create () in
          Eio.Fiber.fork ~sw (fun () ->
              Eio.Promise.resolve after_result_u
                (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout
                   ~timeout:reconnect_timeout ordered));
          let recovery_response =
            request_until_response ~clock ~timeout:reconnect_timeout
              ~failure_file ~label:"confirm OCaml reconnect" connection
              (Nats.Subject.literal (prefix ^ ".recovery-ready"))
              (if leader_failover then "ocaml-leader-failover"
               else "ocaml-reconnected")
          in
          expect_payload "recovery response" "go-recovery-ready"
            recovery_response;
          let after =
            expect_jetstream_ok "receive post-failover ordered message"
              (Eio.Promise.await after_result)
          in
          let after_consumer = Nats_eio.Jetstream.Msg.consumer after in
          let after_consumer_sequence =
            if String.equal after_consumer consumer_name then 3L else 1L
          in
          expect_ordered_delivery "OCaml ordered post-failover"
            ~stream:stream_name ~subject:match_subject ~consumer:after_consumer
            ~payload:"go-after-four" ~interop:"go-ordered-reconnect"
            ~trace:"go-after-four" ~stream_sequence:4L
            ~consumer_sequence:after_consumer_sequence after;
          let close_response =
            expect_ok "close Go ordered session"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".close"))
                 "ocaml-close")
          in
          expect_payload "Go close response" "go-closed" close_response;
          expect_jetstream_ok "close OCaml ordered session"
            (Nats_eio.Jetstream.Consumer.Ordered.close ordered);
          ordered_closed := true;
          expect_ok "flush after ordered close"
            (Nats_eio.Connection.flush connection);
          expect_jetstream_ok "delete ordered reconnect stream"
            (Nats_eio.Jetstream.Stream.delete stream);
          let cleanup_response =
            expect_ok "request ordered reconnect cleanup"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".cleanup"))
                 "cleanup")
          in
          expect_payload "ordered reconnect cleanup response" "cleaned"
            cleanup_response;
          print_endline "interop-jetstream-ordered-reconnect: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline
        ("JetStream ordered reconnect interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("JetStream ordered reconnect interop acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
