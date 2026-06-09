let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let service_error_message error =
  Format.asprintf "%a" Nats_eio.Service.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_service_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (service_error_message error)

let endpoint value =
  match Nats.Endpoint.of_string (String.trim value) with
  | Ok value -> value
  | Error error ->
      failf "invalid endpoint %S: %a" value Nats.Endpoint.pp_error error

let endpoints () =
  match Sys.getenv_opt "NATS_TEST_SERVERS" with
  | None -> failf "NATS_TEST_SERVERS is required"
  | Some value -> (
      match
        List.filter
          (fun value -> not (String.equal (String.trim value) ""))
          (String.split_on_char ',' value)
      with
      | first :: second :: rest -> List.map endpoint (first :: second :: rest)
      | _ -> failf "NATS_TEST_SERVERS must contain at least two endpoints")

let prefix () =
  match Sys.getenv_opt "NATS_TEST_INTEROP_PREFIX" with
  | Some value -> value
  | None -> failf "NATS_TEST_INTEROP_PREFIX is required"

let subject prefix suffix = Nats.Subject.literal (prefix ^ "." ^ suffix)
let filter prefix suffix = Nats.Subject.Filter.literal (prefix ^ "." ^ suffix)

let headers entries =
  match Nats.Header.of_list entries with
  | Ok value -> value
  | Error error ->
      failf "invalid interop headers: %a" Nats.Header.pp_error error

let expect_string label expected actual =
  if not (String.equal actual expected) then
    failf "%s was %S, expected %S" label actual expected

let expect_header label expected message =
  match Nats.Header.find "x-interop" (Nats.Message.headers message) with
  | Some actual -> expect_string label expected actual
  | None -> failf "%s was absent, expected %S" label expected

let next_message ~timeout label subscription =
  match Nats_eio.Subscription.next_with_timeout ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let expect_payload label expected message =
  expect_string label expected (Nats.Message.payload message)

let expect_initial_connection ~clock ~timeout events =
  let connected = ref false in
  while not !connected do
    let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
    match
      Eio.Fiber.first
        (fun () -> Nats_eio.Event_stream.next events)
        (fun () ->
          Eio.Time.Mono.sleep clock seconds;
          Error Nats_eio.Error.Timeout)
    with
    | Ok (Nats_eio.Event.Core Nats.Event.Connected) -> connected := true
    | Ok (Nats_eio.Event.Core _) -> ()
    | Ok event ->
        failf "unexpected initial lifecycle event: %a" Nats_eio.Event.pp event
    | Error error -> failf "initial connection: %s" (error_message error)
  done

let expect_event ~clock ~timeout ~label predicate events =
  let matched = ref false in
  while not !matched do
    let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
    match
      Eio.Fiber.first
        (fun () -> Nats_eio.Event_stream.next events)
        (fun () ->
          Eio.Time.Mono.sleep clock seconds;
          Error Nats_eio.Error.Timeout)
    with
    | Ok event -> if predicate event then matched := true
    | Error error -> failf "%s: %s" label (error_message error)
  done

let expect_disconnected ~clock ~timeout events =
  expect_event ~clock ~timeout ~label:"disconnect"
    (function Nats_eio.Event.Disconnected -> true | _ -> false)
    events

let expect_reconnected ~clock ~timeout events =
  expect_event ~clock ~timeout ~label:"reconnect"
    (function Nats_eio.Event.Reconnected -> true | _ -> false)
    events

let await_go_ready ~clock ~timeout ~cycle connection reconnect_ready =
  let request_payload = "ocaml-ready-" ^ string_of_int cycle in
  let response_payload = "go-ready-" ^ string_of_int cycle in
  let deadline =
    match Mtime.add_span (Nats_eio.Connection.now connection) timeout with
    | Some deadline -> deadline
    | None -> Mtime.max_stamp
  in
  let attempt_timeout = Mtime.Span.(500 * ms) in
  let ready = ref false in
  while not !ready do
    let now = Nats_eio.Connection.now connection in
    if Mtime.compare now deadline >= 0 then
      failf "timed out waiting for Go Service recovery barrier"
    else
      let remaining = Mtime.span now deadline in
      let request_timeout =
        if Mtime.Span.compare remaining attempt_timeout < 0 then remaining
        else attempt_timeout
      in
      match
        Nats_eio.Connection.request ~timeout:request_timeout connection
          reconnect_ready request_payload
      with
      | Ok response ->
          expect_payload "Go Service recovery barrier" response_payload response;
          ready := true
      | Error Nats_eio.Error.Timeout
      | Error Nats_eio.Error.No_responders
      | Error Nats_eio.Error.Disconnected ->
          Eio.Time.Mono.sleep clock 0.01
      | Error error ->
          failf "Go Service recovery barrier: %s" (error_message error)
  done

let expect_metadata label expected actual =
  if not (Int.equal (List.length expected) (List.length actual)) then
    failf "%s had an unexpected metadata count" label;
  List.iter
    (fun (name, expected_value) ->
      match
        List.find_opt
          (fun (actual_name, _) -> String.equal name actual_name)
          actual
      with
      | Some (_, actual_value) ->
          expect_string (label ^ " " ^ name) expected_value actual_value
      | None -> failf "%s omitted metadata %S" label name)
    expected

let expect_info ~label ~name ~description ~language ~prefix ~subject_suffix info
    =
  expect_string (label ^ " name") name (Nats_eio.Service.Info.name info);
  expect_string (label ^ " version") "1.2.3"
    (Nats_eio.Service.Info.version info);
  (match Nats_eio.Service.Info.description info with
  | Some actual -> expect_string (label ^ " description") description actual
  | None -> failf "%s omitted its description" label);
  expect_metadata (label ^ " metadata")
    [ ("language", language); ("suite", "interop") ]
    (Nats_eio.Service.Info.metadata info);
  match Nats_eio.Service.Info.endpoints info with
  | [ endpoint ] -> (
      expect_string (label ^ " endpoint name") "echo"
        (Nats_eio.Service.Info.endpoint_name endpoint);
      expect_string
        (label ^ " endpoint subject")
        (prefix ^ "." ^ subject_suffix)
        (Nats.Subject.Filter.to_string
           (Nats_eio.Service.Info.endpoint_subject endpoint));
      (match Nats_eio.Service.Info.endpoint_queue endpoint with
      | Some queue ->
          expect_string
            (label ^ " endpoint queue")
            "q"
            (Nats.Queue_group.to_string queue)
      | None -> failf "%s endpoint omitted its queue" label);
      match Nats_eio.Service.Info.endpoint_metadata endpoint with
      | Some metadata ->
          expect_metadata
            (label ^ " endpoint metadata")
            [ ("role", "interop") ]
            metadata
      | None -> failf "%s endpoint omitted its metadata" label)
  | endpoints ->
      failf "%s advertised %d endpoints, expected 1" label
        (List.length endpoints)

let stats_data ~generator =
  Jsont.Json.object'
    [
      Jsont.Json.mem (Jsont.Json.name "generator") (Jsont.Json.string generator);
      Jsont.Json.mem (Jsont.Json.name "endpoint") (Jsont.Json.string "echo");
      Jsont.Json.mem (Jsont.Json.name "status") (Jsont.Json.string "ready");
    ]

let expect_stats_data label ~generator = function
  | Some actual ->
      let expected = stats_data ~generator in
      if not (Jsont.Json.equal actual expected) then
        failf "%s was %a, expected %a" label Jsont.Json.pp actual Jsont.Json.pp
          expected
  | None -> failf "%s omitted custom endpoint data" label

let stats_ready ~name ~id ~prefix ~subject_suffix ~requests stats =
  String.equal (Nats_eio.Service.Stats.name stats) name
  && String.equal (Nats_eio.Service.Stats.id stats) id
  && String.equal (Nats_eio.Service.Stats.version stats) "1.2.3"
  &&
  match Nats_eio.Service.Stats.endpoints stats with
  | [ endpoint ] ->
      String.equal (Nats_eio.Service.Stats.endpoint_name endpoint) "echo"
      && String.equal
           (Nats.Subject.Filter.to_string
              (Nats_eio.Service.Stats.endpoint_subject endpoint))
           (prefix ^ "." ^ subject_suffix)
      && Int64.equal (Nats_eio.Service.Stats.num_requests endpoint) requests
      && Int64.equal (Nats_eio.Service.Stats.num_errors endpoint) 0L
  | _ -> false

let expect_stats ~label ~name ~id ~prefix ~subject_suffix ~requests ~generator
    ~endpoint_metadata stats =
  expect_string (label ^ " name") name (Nats_eio.Service.Stats.name stats);
  expect_string (label ^ " id") id (Nats_eio.Service.Stats.id stats);
  expect_string (label ^ " version") "1.2.3"
    (Nats_eio.Service.Stats.version stats);
  expect_metadata (label ^ " metadata")
    [ ("language", generator); ("suite", "interop") ]
    (Nats_eio.Service.Stats.metadata stats);
  match Nats_eio.Service.Stats.endpoints stats with
  | [ endpoint ] ->
      expect_string (label ^ " endpoint name") "echo"
        (Nats_eio.Service.Stats.endpoint_name endpoint);
      expect_string
        (label ^ " endpoint subject")
        (prefix ^ "." ^ subject_suffix)
        (Nats.Subject.Filter.to_string
           (Nats_eio.Service.Stats.endpoint_subject endpoint));
      (match Nats_eio.Service.Stats.endpoint_queue endpoint with
      | Some queue ->
          expect_string
            (label ^ " endpoint queue")
            "q"
            (Nats.Queue_group.to_string queue)
      | None -> failf "%s endpoint omitted its queue" label);
      (match Nats_eio.Service.Stats.endpoint_metadata endpoint with
      | Some metadata when endpoint_metadata ->
          expect_metadata
            (label ^ " endpoint metadata")
            [ ("role", "interop") ]
            metadata
      | None when not endpoint_metadata -> ()
      | None -> failf "%s omitted endpoint metadata" label
      | Some _ -> failf "%s unexpectedly supplied endpoint metadata" label);
      if
        not
          (Int64.equal (Nats_eio.Service.Stats.num_requests endpoint) requests)
      then
        failf "%s request count was %Ld, expected %Ld" label
          (Nats_eio.Service.Stats.num_requests endpoint)
          requests;
      if not (Int64.equal (Nats_eio.Service.Stats.num_errors endpoint) 0L) then
        failf "%s error count was %Ld, expected 0" label
          (Nats_eio.Service.Stats.num_errors endpoint);
      expect_stats_data (label ^ " custom data") ~generator
        (Nats_eio.Service.Stats.data endpoint)
  | endpoints ->
      failf "%s advertised %d endpoints, expected 1" label
        (List.length endpoints)

let discover_info ~timeout ~target connection label =
  match
    expect_service_ok label
      (Nats_eio.Service.Discovery.info ~timeout ~target connection)
  with
  | [ info ] -> info
  | values ->
      failf "%s returned %d instances, expected 1" label (List.length values)

let wait_for_stats ~clock ~timeout ~label ~target ~connection ~ready =
  let deadline =
    match Mtime.add_span (Nats_eio.Connection.now connection) timeout with
    | Some deadline -> deadline
    | None -> Mtime.max_stamp
  in
  let value = ref None in
  while Option.is_none !value do
    let now = Nats_eio.Connection.now connection in
    if Mtime.compare now deadline >= 0 then
      failf "timed out waiting for %s" label
    else
      let remaining = Mtime.span now deadline in
      let query_timeout =
        let maximum = Mtime.Span.(200 * ms) in
        if Mtime.Span.compare remaining maximum < 0 then remaining else maximum
      in
      match
        Nats_eio.Service.Discovery.stats ~timeout:query_timeout ~target
          connection
      with
      | Ok [] -> Eio.Time.Mono.sleep clock 0.01
      | Ok [ stats ] ->
          if ready stats then value := Some stats
          else Eio.Time.Mono.sleep clock 0.01
      | Ok values ->
          failf "%s returned %d instances, expected 1" label
            (List.length values)
      | Error
          (Nats_eio.Service.Error.Connection
            (Nats_eio.Error.Timeout | Nats_eio.Error.Disconnected)) ->
          Eio.Time.Mono.sleep clock 0.01
      | Error error -> failf "%s: %s" label (service_error_message error)
  done;
  match !value with Some stats -> stats | None -> assert false

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoints = endpoints () in
  let cycles = List.length endpoints - 1 in
  let prefix = prefix () in
  let auth = Interop_auth.auth () in
  let tls = Interop_auth.tls_config () in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ?auth ?tls ~max_reconnect_attempts:(Some 20)
         ~reconnect_delay:Mtime.Span.(50 * ms)
         ~reconnect_max_delay:Mtime.Span.(100 * ms)
         ())
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config endpoints)
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let timeout = Mtime.Span.(10 * s) in
      let discovery_timeout = Mtime.Span.(500 * ms) in
      let events = Nats_eio.Connection.events connection in
      expect_initial_connection ~clock ~timeout events;
      Eio.Switch.run @@ fun service_sw ->
      let service_config =
        expect_service_ok "OCaml Service reconnect config"
          (Nats_eio.Service.Config.v ~name:"ocaml-interop-service"
             ~version:"1.2.3" ~description:"OCaml Service reconnect interop"
             ~metadata:[ ("language", "ocaml"); ("suite", "interop") ]
             ~stats_handler:(fun _ -> Some (stats_data ~generator:"ocaml"))
             ())
      in
      let service =
        expect_service_ok "OCaml Service reconnect start"
          (Nats_eio.Service.v ~sw:service_sw ~clock:(Eio.Stdenv.clock env)
             connection service_config)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Service.stop service))
        (fun () ->
          let endpoint_handler request =
            let payload = Nats_eio.Service.Request.payload request in
            let valid_payload =
              String.starts_with ~prefix:"go-to-ocaml-round-" payload
            in
            let valid_header =
              match
                Nats.Header.find "x-interop"
                  (Nats_eio.Service.Request.headers request)
              with
              | Some value -> String.equal value "go-service-reconnect"
              | None -> false
            in
            if (not valid_payload) || not valid_header then
              Error Nats_eio.Service.Error.No_response
            else
              Nats_eio.Service.Request.respond
                ~headers:
                  (headers
                     [ ("X-Interop", "ocaml-service-reconnect-response") ])
                request
                ("ocaml-response-for-" ^ payload)
          in
          let endpoint =
            expect_service_ok "OCaml Service reconnect endpoint"
              (Nats_eio.Service.Endpoint.v ~name:"echo"
                 ~subject:(filter prefix "ocaml.echo")
                 ~metadata:[ ("role", "interop") ]
                 endpoint_handler)
          in
          expect_service_ok "add OCaml Service reconnect endpoint"
            (Nats_eio.Service.add_endpoint service endpoint);
          let done_subscription =
            expect_ok "subscribe Service reconnect completion"
              (Nats_eio.Connection.subscribe connection (filter prefix "done"))
          in
          let round_ready = subject prefix "round-ready" in
          expect_ok "flush OCaml Service reconnect"
            (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Go Service reconnect peer"
              (Nats_eio.Connection.request ~timeout connection
                 (subject prefix "start") "start")
          in
          expect_payload "Go Service reconnect start response" "started"
            start_response;
          let ocaml_info = Nats_eio.Service.info service in
          let ocaml_id = Nats_eio.Service.Info.id ocaml_info in
          expect_info ~label:"OCaml Service" ~name:"ocaml-interop-service"
            ~description:"OCaml Service reconnect interop" ~language:"ocaml"
            ~prefix ~subject_suffix:"ocaml.echo" ocaml_info;
          let go_info =
            discover_info ~timeout:discovery_timeout
              ~target:(Nats_eio.Service.Discovery.Named "go-interop-service")
              connection "discover Go Service reconnect info"
          in
          let go_id = Nats_eio.Service.Info.id go_info in
          expect_info ~label:"Go Service" ~name:"go-interop-service"
            ~description:"Go Service reconnect interop" ~language:"go" ~prefix
            ~subject_suffix:"go.echo" go_info;
          for round = 0 to cycles do
            let payload = "ocaml-to-go-round-" ^ string_of_int round in
            let response =
              expect_ok
                ("request Go Service round " ^ string_of_int round)
                (Nats_eio.Connection.request
                   ~headers:
                     (headers [ ("X-Interop", "ocaml-service-reconnect") ])
                   ~timeout connection (subject prefix "go.echo") payload)
            in
            expect_payload "Go Service round response"
              ("go-response-for-" ^ payload)
              response;
            expect_header "Go Service round response header"
              "go-service-reconnect-response" response;
            let expected_requests = Int64.of_int (round + 1) in
            let go_stats =
              wait_for_stats ~clock ~timeout ~label:"Go Service reconnect stats"
                ~target:
                  (Nats_eio.Service.Discovery.Instance
                     { service = "go-interop-service"; id = go_id })
                ~connection
                ~ready:
                  (stats_ready ~name:"go-interop-service" ~id:go_id ~prefix
                     ~subject_suffix:"go.echo" ~requests:expected_requests)
            in
            expect_stats ~label:"Go Service reconnect stats"
              ~name:"go-interop-service" ~id:go_id ~prefix
              ~subject_suffix:"go.echo" ~requests:expected_requests
              ~generator:"go" ~endpoint_metadata:false go_stats;
            let ocaml_stats =
              wait_for_stats ~clock ~timeout
                ~label:"OCaml Service reconnect stats"
                ~target:
                  (Nats_eio.Service.Discovery.Instance
                     { service = "ocaml-interop-service"; id = ocaml_id })
                ~connection
                ~ready:
                  (stats_ready ~name:"ocaml-interop-service" ~id:ocaml_id
                     ~prefix ~subject_suffix:"ocaml.echo"
                     ~requests:expected_requests)
            in
            expect_stats ~label:"OCaml Service reconnect stats"
              ~name:"ocaml-interop-service" ~id:ocaml_id ~prefix
              ~subject_suffix:"ocaml.echo" ~requests:expected_requests
              ~generator:"ocaml" ~endpoint_metadata:true ocaml_stats;
            let barrier_response =
              expect_ok "Service reconnect round barrier"
                (Nats_eio.Connection.request ~timeout connection round_ready
                   ("round-" ^ string_of_int round))
            in
            expect_payload "Service reconnect round barrier response" "accepted"
              barrier_response;
            if round < cycles then (
              expect_disconnected ~clock ~timeout events;
              expect_reconnected ~clock ~timeout events;
              await_go_ready ~clock ~timeout ~cycle:(round + 1) connection
                (subject prefix
                   ("reconnect-ready." ^ string_of_int (round + 1)));
              let after_go_info =
                discover_info ~timeout:discovery_timeout
                  ~target:
                    (Nats_eio.Service.Discovery.Instance
                       { service = "go-interop-service"; id = go_id })
                  connection "discover Go Service after reconnect"
              in
              expect_info ~label:"Go Service after reconnect"
                ~name:"go-interop-service"
                ~description:"Go Service reconnect interop" ~language:"go"
                ~prefix ~subject_suffix:"go.echo" after_go_info;
              let after_ocaml_info =
                discover_info ~timeout:discovery_timeout
                  ~target:
                    (Nats_eio.Service.Discovery.Instance
                       { service = "ocaml-interop-service"; id = ocaml_id })
                  connection "discover OCaml Service after reconnect"
              in
              expect_info ~label:"OCaml Service after reconnect"
                ~name:"ocaml-interop-service"
                ~description:"OCaml Service reconnect interop" ~language:"ocaml"
                ~prefix ~subject_suffix:"ocaml.echo" after_ocaml_info)
          done;
          let done_message =
            next_message ~timeout "Go Service reconnect completion"
              done_subscription
          in
          expect_payload "Go Service reconnect completion" "go-finished"
            done_message;
          expect_ok "reply Service reconnect completion"
            (Nats_eio.Connection.publish connection
               (match Nats.Message.reply_to done_message with
               | Some reply -> reply
               | None ->
                   failf "Service reconnect completion had no reply subject")
               "ocaml-validated");
          expect_ok "flush Service reconnect completion"
            (Nats_eio.Connection.flush connection);
          expect_service_ok "stop OCaml Service reconnect"
            (Nats_eio.Service.stop service);
          expect_ok "drain Service reconnect"
            (Nats_eio.Connection.drain connection);
          expect_ok "close Service reconnect after drain"
            (Nats_eio.Connection.close connection);
          print_endline "interop-service-reconnect: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("interop Service reconnect acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("interop Service reconnect acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
