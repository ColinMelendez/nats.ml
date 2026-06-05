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

let expect_string label expected actual =
  if not (String.equal actual expected) then
    failf "%s was %S, expected %S" label actual expected

let endpoint () =
  match Sys.getenv_opt "NATS_TEST_SERVER" with
  | None -> failf "NATS_TEST_SERVER is required"
  | Some value -> (
      match Nats.Endpoint.of_string value with
      | Ok endpoint -> endpoint
      | Error error ->
          failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error
            error)

let prefix () =
  match Sys.getenv_opt "NATS_TEST_INTEROP_PREFIX" with
  | None -> failf "NATS_TEST_INTEROP_PREFIX is required"
  | Some value -> value

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

let subject prefix suffix = Nats.Subject.literal (prefix ^ "." ^ suffix)
let filter prefix suffix = Nats.Subject.Filter.literal (prefix ^ "." ^ suffix)

let headers entries =
  match Nats.Header.of_list entries with
  | Ok value -> value
  | Error error ->
      failf "invalid interop headers: %a" Nats.Header.pp_error error

let expect_header label expected message =
  match Nats.Header.find "x-interop" (Nats.Message.headers message) with
  | Some actual -> expect_string label expected actual
  | None -> failf "%s was absent, expected %S" label expected

let expect_core_event events expected =
  match Nats_eio.Event_stream.next events with
  | Ok (Nats_eio.Event.Core event) -> (
      match (event, expected) with
      | Nats.Event.Info _, `Info -> ()
      | Nats.Event.Connected, `Connected -> ()
      | _ -> failf "unexpected core event: %a" Nats.Event.pp event)
  | Ok event -> failf "unexpected lifecycle event: %a" Nats_eio.Event.pp event
  | Error error -> failf "connection event stream: %s" (error_message error)

let next_message ~timeout label subscription =
  match Nats_eio.Subscription.next_with_timeout ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let reply_subject message =
  match Nats.Message.reply_to message with
  | Some subject -> subject
  | None -> failf "request message had no reply subject"

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

let expect_endpoint_metadata label = function
  | Some metadata -> expect_metadata label [ ("role", "interop") ] metadata
  | None -> failf "%s omitted endpoint metadata" label

let expect_info_endpoint ~prefix ~label ~name ~subject_suffix endpoint =
  expect_string (label ^ " endpoint name") name
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
  | None -> failf "%s omitted endpoint queue" label);
  expect_endpoint_metadata
    (label ^ " endpoint metadata")
    (Nats_eio.Service.Info.endpoint_metadata endpoint)

let expect_stats_endpoint ~prefix ~label ~name ~subject_suffix ~requests ~errors
    endpoint =
  expect_string (label ^ " endpoint name") name
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
  | None -> failf "%s omitted endpoint queue" label);
  (match Nats_eio.Service.Stats.endpoint_metadata endpoint with
  | None -> ()
  | Some _ -> failf "%s unexpectedly supplied endpoint metadata" label);
  if not (Int64.equal (Nats_eio.Service.Stats.num_requests endpoint) requests)
  then
    failf "%s request count was %Ld, expected %Ld" label
      (Nats_eio.Service.Stats.num_requests endpoint)
      requests;
  if not (Int64.equal (Nats_eio.Service.Stats.num_errors endpoint) errors) then
    failf "%s error count was %Ld, expected %Ld" label
      (Nats_eio.Service.Stats.num_errors endpoint)
      errors

let find_info_endpoint label name endpoints =
  match
    List.find_opt
      (fun endpoint ->
        String.equal (Nats_eio.Service.Info.endpoint_name endpoint) name)
      endpoints
  with
  | Some endpoint -> endpoint
  | None -> failf "%s omitted endpoint %S" label name

let find_stats_endpoint label name endpoints =
  match
    List.find_opt
      (fun endpoint ->
        String.equal (Nats_eio.Service.Stats.endpoint_name endpoint) name)
      endpoints
  with
  | Some endpoint -> endpoint
  | None -> failf "%s omitted endpoint %S" label name

let expect_go_info ~prefix info =
  expect_string "Go service name" "go-interop-service"
    (Nats_eio.Service.Info.name info);
  expect_string "Go service version" "1.2.3"
    (Nats_eio.Service.Info.version info);
  (match Nats_eio.Service.Info.description info with
  | Some description ->
      expect_string "Go service description" "Go Service interop" description
  | None -> failf "Go service omitted its description");
  expect_metadata "Go service metadata"
    [ ("language", "go"); ("suite", "interop") ]
    (Nats_eio.Service.Info.metadata info);
  let endpoints = Nats_eio.Service.Info.endpoints info in
  if not (Int.equal (List.length endpoints) 2) then
    failf "Go service advertised %d endpoints, expected 2"
      (List.length endpoints);
  expect_info_endpoint ~prefix ~label:"Go echo" ~name:"echo"
    ~subject_suffix:"go.echo"
    (find_info_endpoint "Go service info" "echo" endpoints);
  expect_info_endpoint ~prefix ~label:"Go error" ~name:"error"
    ~subject_suffix:"go.error"
    (find_info_endpoint "Go service info" "error" endpoints)

let expect_go_stats ~prefix ~id stats =
  expect_string "Go stats service name" "go-interop-service"
    (Nats_eio.Service.Stats.name stats);
  expect_string "Go stats service id" id (Nats_eio.Service.Stats.id stats);
  expect_string "Go stats service version" "1.2.3"
    (Nats_eio.Service.Stats.version stats);
  expect_metadata "Go stats service metadata"
    [ ("language", "go"); ("suite", "interop") ]
    (Nats_eio.Service.Stats.metadata stats);
  let endpoints = Nats_eio.Service.Stats.endpoints stats in
  if not (Int.equal (List.length endpoints) 2) then
    failf "Go stats advertised %d endpoints, expected 2" (List.length endpoints);
  let echo = find_stats_endpoint "Go service stats" "echo" endpoints in
  let error = find_stats_endpoint "Go service stats" "error" endpoints in
  expect_stats_endpoint ~prefix ~label:"Go echo stats" ~name:"echo"
    ~subject_suffix:"go.echo" ~requests:1L ~errors:0L echo;
  expect_stats_endpoint ~prefix ~label:"Go error stats" ~name:"error"
    ~subject_suffix:"go.error" ~requests:1L ~errors:1L error;
  if Int.equal (String.length (Nats_eio.Service.Stats.last_error error)) 0 then
    failf "Go error stats omitted its last error"

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoint = endpoint () in
  let prefix = prefix () in
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
      let events = Nats_eio.Connection.events connection in
      expect_core_event events `Info;
      expect_core_event events `Connected;
      let timeout = Mtime.Span.(10 * s) in
      let discovery_timeout = Mtime.Span.(500 * ms) in
      Eio.Switch.run @@ fun service_sw ->
      let service_config =
        expect_service_ok "OCaml service config"
          (Nats_eio.Service.Config.v ~name:"ocaml-interop-service"
             ~version:"1.2.3" ~description:"OCaml Service interop"
             ~metadata:[ ("language", "ocaml"); ("suite", "interop") ]
             ())
      in
      let service =
        expect_service_ok "OCaml service start"
          (Nats_eio.Service.v ~sw:service_sw ~clock:(Eio.Stdenv.clock env)
             connection service_config)
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Service.stop service))
        (fun () ->
          let handler_result, handler_result_u = Eio.Promise.create () in
          let handler_completed = ref false in
          let complete_handler result =
            if not !handler_completed then (
              handler_completed := true;
              Eio.Promise.resolve handler_result_u result)
          in
          let endpoint_handler request =
            let payload_ok =
              String.equal (Nats_eio.Service.Request.payload request) "from-go"
            in
            let header_ok =
              match
                Nats.Header.find "x-interop"
                  (Nats_eio.Service.Request.headers request)
              with
              | Some value -> String.equal value "go-service-request"
              | None -> false
            in
            if (not payload_ok) || not header_ok then (
              let message =
                Format.asprintf
                  "OCaml handler received payload %S and header %B"
                  (Nats_eio.Service.Request.payload request)
                  header_ok
              in
              complete_handler (Error message);
              Error Nats_eio.Service.Error.No_response)
            else
              match
                Nats_eio.Service.Request.respond
                  ~headers:(headers [ ("X-Interop", "ocaml-service-response") ])
                  request "ocaml-service-response"
              with
              | Ok () ->
                  complete_handler (Ok ());
                  Ok ()
              | Error error ->
                  complete_handler (Error (service_error_message error));
                  Error error
          in
          let endpoint =
            expect_service_ok "OCaml service endpoint"
              (Nats_eio.Service.Endpoint.v ~name:"echo"
                 ~subject:(filter prefix "ocaml.echo")
                 ~metadata:[ ("role", "interop") ]
                 endpoint_handler)
          in
          expect_service_ok "add OCaml service endpoint"
            (Nats_eio.Service.add_endpoint service endpoint);
          let done_subscription =
            expect_ok "subscribe final barrier"
              (Nats_eio.Connection.subscribe connection (filter prefix "done"))
          in
          expect_ok "flush OCaml service" (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Go service peer"
              (Nats_eio.Connection.request ~timeout connection
                 (subject prefix "start") "start")
          in
          expect_string "Go service start response" "started"
            (Nats.Message.payload start_response);
          let go_response =
            expect_ok "request Go echo endpoint"
              (Nats_eio.Connection.request
                 ~headers:(headers [ ("X-Interop", "ocaml-service") ])
                 ~timeout connection (subject prefix "go.echo") "from-ocaml")
          in
          expect_string "Go echo response" "go-service-response"
            (Nats.Message.payload go_response);
          expect_header "Go echo response header" "go-service-response"
            go_response;
          let go_error_response =
            expect_ok "request Go error endpoint"
              (Nats_eio.Connection.request ~timeout connection
                 (subject prefix "go.error")
                 "error-request")
          in
          expect_string "Go error response payload" "go-service-error-payload"
            (Nats.Message.payload go_error_response);
          (match
             Nats.Header.find "nats-service-error"
               (Nats.Message.headers go_error_response)
           with
          | Some value ->
              expect_string "Go service error description" "go-service-error"
                value
          | None -> failf "Go error response omitted Nats-Service-Error");
          (match
             Nats.Header.find "nats-service-error-code"
               (Nats.Message.headers go_error_response)
           with
          | Some value -> expect_string "Go service error code" "422" value
          | None -> failf "Go error response omitted Nats-Service-Error-Code");
          let go_info =
            match
              expect_service_ok "discover Go service info"
                (Nats_eio.Service.Discovery.info ~timeout:discovery_timeout
                   ~target:
                     (Nats_eio.Service.Discovery.Named "go-interop-service")
                   connection)
            with
            | [ info ] -> info
            | values ->
                failf "Go service info returned %d instances, expected 1"
                  (List.length values)
          in
          expect_go_info ~prefix go_info;
          let done_message =
            next_message ~timeout "Go service completion" done_subscription
          in
          expect_string "Go service completion payload" "go-finished"
            (Nats.Message.payload done_message);
          let go_stats =
            match
              expect_service_ok "discover Go service stats"
                (Nats_eio.Service.Discovery.stats ~timeout:discovery_timeout
                   ~target:
                     (Nats_eio.Service.Discovery.Instance
                        {
                          service = "go-interop-service";
                          id = Nats_eio.Service.Info.id go_info;
                        })
                   connection)
            with
            | [ stats ] -> stats
            | values ->
                failf "Go service stats returned %d instances, expected 1"
                  (List.length values)
          in
          expect_go_stats ~prefix
            ~id:(Nats_eio.Service.Info.id go_info)
            go_stats;
          expect_ok "reply final Service barrier"
            (Nats_eio.Connection.publish connection
               (reply_subject done_message)
               "ocaml-validated");
          expect_ok "flush final Service barrier"
            (Nats_eio.Connection.flush connection);
          (match Eio.Promise.await handler_result with
          | Ok () -> ()
          | Error message -> failf "OCaml service handler: %s" message);
          expect_service_ok "stop OCaml service" (Nats_eio.Service.stop service);
          expect_ok "drain" (Nats_eio.Connection.drain connection);
          expect_ok "close after drain" (Nats_eio.Connection.close connection);
          print_endline "interop-service: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("interop Service acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("interop Service acceptance failed: " ^ Printexc.to_string error);
      exit 1
