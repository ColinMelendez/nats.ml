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

let subject value = Nats.Subject.literal value

let expect_string label expected actual =
  if not (String.equal actual expected) then
    failf "%s was %S, expected %S" label actual expected

let expect_metadata label expected actual =
  let equal_entry (expected_name, expected_value) (actual_name, actual_value) =
    String.equal expected_name actual_name
    && String.equal expected_value actual_value
  in
  if not (List.equal equal_entry expected actual) then
    failf "%s did not match expected metadata" label

let expect_payload label expected message =
  expect_string label expected (Nats.Message.payload message)

let expect_header label name expected message =
  match Nats.Header.find name (Nats.Message.headers message) with
  | Some actual -> expect_string label expected actual
  | None -> failf "%s was absent, expected %S" label expected

let expect_one label = function
  | [ value ] -> value
  | values -> failf "%s returned %d values" label (List.length values)

let find_info_endpoint label name endpoints =
  match
    List.find_opt
      (fun endpoint ->
        String.equal (Nats_eio.Service.Info.endpoint_name endpoint) name)
      endpoints
  with
  | Some endpoint -> endpoint
  | None -> failf "%s endpoint %S was not discovered" label name

let find_stats_endpoint label name endpoints =
  match
    List.find_opt
      (fun endpoint ->
        String.equal (Nats_eio.Service.Stats.endpoint_name endpoint) name)
      endpoints
  with
  | Some endpoint -> endpoint
  | None -> failf "%s endpoint %S was not reported" label name

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let mono_clock = Eio.Stdenv.mono_clock env in
  let endpoint = endpoint () in
  let config =
    match auth () with
    | None -> None
    | Some auth ->
        Some (expect_ok "auth config" (Nats_eio.Connection.Config.v ~auth ()))
  in
  let connect label =
    expect_ok label
      (Nats_eio.Connection.connect ~sw ~net ~clock:mono_clock ?config
         [ endpoint ])
  in
  let service_connection = connect "service connect" in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close service_connection))
    (fun () ->
      let suffix = run_id () in
      let service_name = "ocaml-service-" ^ suffix in
      let echo_name = "echo-" ^ suffix in
      let status_name = "status-" ^ suffix in
      let service_config =
        expect_service_ok "service config"
          (Nats_eio.Service.Config.v ~name:service_name ~version:"1.2.3"
             ~description:"service acceptance"
             ~metadata:[ ("team", "infra") ]
             ())
      in
      let response_headers =
        match Nats.Header.of_list [ ("X-Service", "ocaml") ] with
        | Ok headers -> headers
        | Error error -> failf "response headers: %a" Nats.Header.pp_error error
      in
      let block_started, block_started_u = Eio.Promise.create () in
      let release_block, release_block_u = Eio.Promise.create () in
      let echo_endpoint =
        expect_service_ok "echo endpoint"
          (Nats_eio.Service.Endpoint.v ~name:echo_name
             ~metadata:[ ("kind", "echo") ]
             (fun request ->
               match Nats_eio.Service.Request.payload request with
               | "error" ->
                   Nats_eio.Service.Request.respond_error ~code:"400"
                     ~description:"bad request" ~payload:"rejected" request
               | "missing" -> Error Nats_eio.Service.Error.No_response
               | "raise" -> raise (Failure "service acceptance handler")
               | "block" ->
                   Eio.Promise.resolve block_started_u ();
                   Eio.Promise.await release_block;
                   Nats_eio.Service.Request.respond request "blocked"
               | payload ->
                   Nats_eio.Service.Request.respond ~headers:response_headers
                     request ("echo:" ^ payload)))
      in
      let service =
        expect_service_ok "service start"
          (Nats_eio.Service.v ~sw ~clock
             ~random:(Random.State.make [| 4; 2; 0 |])
             service_connection service_config)
      in
      let service_stopped = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !service_stopped then ignore (Nats_eio.Service.stop service))
        (fun () ->
          expect_service_ok "echo endpoint registration"
            (Nats_eio.Service.add_endpoint service echo_endpoint);
          let group =
            expect_service_ok "admin group"
              (Nats_eio.Service.add_group
                 ~queue:Nats_eio.Service.Config.Disabled service ~name:"admin")
          in
          let status_endpoint =
            expect_service_ok "status endpoint"
              (Nats_eio.Service.Endpoint.v ~name:status_name
                 ~metadata:[ ("kind", "status") ]
                 (fun request ->
                   Nats_eio.Service.Request.respond request "status"))
          in
          expect_service_ok "status endpoint registration"
            (Nats_eio.Service.Group.add_endpoint group status_endpoint);
          expect_ok "service subscription flush"
            (Nats_eio.Connection.flush service_connection);
          let requester = connect "requester connect" in
          Fun.protect
            ~finally:(fun () -> ignore (Nats_eio.Connection.close requester))
            (fun () ->
              let discovery_timeout = Mtime.Span.(2 * s) in
              let ping =
                expect_service_ok "service ping"
                  (Nats_eio.Service.Discovery.ping ~timeout:discovery_timeout
                     ~target:(Nats_eio.Service.Discovery.Named service_name)
                     requester)
                |> expect_one "service ping"
              in
              expect_string "ping name" service_name
                (Nats_eio.Service.Discovery.Ping.name ping);
              expect_string "ping version" "1.2.3"
                (Nats_eio.Service.Discovery.Ping.version ping);
              expect_metadata "ping metadata"
                [ ("team", "infra") ]
                (Nats_eio.Service.Discovery.Ping.metadata ping);
              let service_id = Nats_eio.Service.Discovery.Ping.id ping in
              let target =
                Nats_eio.Service.Discovery.Instance
                  { service = service_name; id = service_id }
              in
              let info =
                expect_service_ok "service info"
                  (Nats_eio.Service.Discovery.info ~timeout:discovery_timeout
                     ~target requester)
                |> expect_one "service info"
              in
              expect_string "info name" service_name
                (Nats_eio.Service.Info.name info);
              expect_string "info description" "service acceptance"
                (Option.value ~default:""
                   (Nats_eio.Service.Info.description info));
              expect_metadata "info metadata"
                [ ("team", "infra") ]
                (Nats_eio.Service.Info.metadata info);
              let echo_info =
                find_info_endpoint "service info" echo_name
                  (Nats_eio.Service.Info.endpoints info)
              in
              expect_string "echo subject" echo_name
                (Nats.Subject.Filter.to_string
                   (Nats_eio.Service.Info.endpoint_subject echo_info));
              (match Nats_eio.Service.Info.endpoint_queue echo_info with
              | Some queue ->
                  expect_string "echo queue" "q"
                    (Nats.Queue_group.to_string queue)
              | None -> failf "echo endpoint did not inherit its queue policy");
              expect_metadata "echo endpoint metadata"
                [ ("kind", "echo") ]
                (Option.value ~default:[]
                   (Nats_eio.Service.Info.endpoint_metadata echo_info));
              let status_info =
                find_info_endpoint "service info" status_name
                  (Nats_eio.Service.Info.endpoints info)
              in
              expect_string "status subject" ("admin." ^ status_name)
                (Nats.Subject.Filter.to_string
                   (Nats_eio.Service.Info.endpoint_subject status_info));
              (match Nats_eio.Service.Info.endpoint_queue status_info with
              | None -> ()
              | Some _ -> failf "disabled group endpoint acquired a queue");
              let request subject_name payload =
                expect_ok ("request " ^ payload)
                  (Nats_eio.Connection.request
                     ~timeout:Mtime.Span.(2 * s)
                     requester (subject subject_name) payload)
              in
              let response = request echo_name "hello" in
              expect_payload "echo response" "echo:hello" response;
              expect_header "echo response header" "X-Service" "ocaml" response;
              let error_response = request echo_name "error" in
              expect_payload "service error response" "rejected" error_response;
              expect_header "service error code" "Nats-Service-Error-Code" "400"
                error_response;
              expect_header "service error description" "Nats-Service-Error"
                "bad request" error_response;
              let status_response = request ("admin." ^ status_name) "status" in
              expect_payload "status response" "status" status_response;
              (match
                 Nats_eio.Connection.request
                   ~timeout:Mtime.Span.(200 * ms)
                   requester (subject echo_name) "missing"
               with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok _ -> failf "no-response handler unexpectedly replied"
              | Error error ->
                  failf "no-response request: %s" (error_message error));
              (match
                 Nats_eio.Connection.request
                   ~timeout:Mtime.Span.(200 * ms)
                   requester (subject echo_name) "raise"
               with
              | Error Nats_eio.Error.Timeout -> ()
              | Ok _ -> failf "raising handler unexpectedly replied"
              | Error error -> failf "raising request: %s" (error_message error));
              let recovered_response = request echo_name "recovered" in
              expect_payload "recovered echo response" "echo:recovered"
                recovered_response;
              expect_header "recovered echo response header" "X-Service" "ocaml"
                recovered_response;
              let stats =
                expect_service_ok "service stats"
                  (Nats_eio.Service.Discovery.stats ~timeout:discovery_timeout
                     ~target requester)
                |> expect_one "service stats"
              in
              expect_string "stats name" service_name
                (Nats_eio.Service.Stats.name stats);
              expect_metadata "stats metadata"
                [ ("team", "infra") ]
                (Nats_eio.Service.Stats.metadata stats);
              let echo_stats =
                find_stats_endpoint "service stats" echo_name
                  (Nats_eio.Service.Stats.endpoints stats)
              in
              if
                not
                  (Int64.equal
                     (Nats_eio.Service.Stats.num_requests echo_stats)
                     5L)
              then
                failf "echo stats counted %Ld requests, expected 5"
                  (Nats_eio.Service.Stats.num_requests echo_stats);
              if
                not
                  (Int64.equal
                     (Nats_eio.Service.Stats.num_errors echo_stats)
                     3L)
              then
                failf "echo stats counted %Ld errors, expected 3"
                  (Nats_eio.Service.Stats.num_errors echo_stats);
              if
                Int64.compare
                  (Nats_eio.Service.Stats.average_processing_time echo_stats)
                  (Nats_eio.Service.Stats.processing_time echo_stats)
                > 0
              then failf "echo stats average exceeded cumulative time";
              let status_stats =
                find_stats_endpoint "service stats" status_name
                  (Nats_eio.Service.Stats.endpoints stats)
              in
              if
                not
                  (Int64.equal
                     (Nats_eio.Service.Stats.num_requests status_stats)
                     1L)
              then failf "status stats did not count its request";
              let blocked_request, blocked_request_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve blocked_request_u
                    (Nats_eio.Connection.request
                       ~timeout:Mtime.Span.(2 * s)
                       requester (subject echo_name) "block"));
              Eio.Promise.await block_started;
              let stop_result, stop_result_u = Eio.Promise.create () in
              let stop_started, stop_started_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve stop_started_u ();
                  Eio.Promise.resolve stop_result_u
                    (Nats_eio.Service.stop service));
              Eio.Promise.await stop_started;
              Eio.Time.Mono.sleep mono_clock 0.01;
              (match Eio.Promise.peek stop_result with
              | None -> ()
              | Some _ ->
                  failf "service stop completed before the handler drained");
              Eio.Promise.resolve release_block_u ();
              let blocked_response =
                expect_ok "blocked request" (Eio.Promise.await blocked_request)
              in
              expect_payload "blocked response" "blocked" blocked_response;
              expect_service_ok "service drain" (Eio.Promise.await stop_result);
              service_stopped := true;
              expect_service_ok "idempotent service stop"
                (Nats_eio.Service.stop service);
              (match
                 Nats_eio.Connection.request
                   ~timeout:Mtime.Span.(500 * ms)
                   requester (subject echo_name) "after-stop"
               with
              | Error Nats_eio.Error.No_responders -> ()
              | Ok _ -> failf "stopped service still answered requests"
              | Error error ->
                  failf "request after service stop: %s" (error_message error));
              expect_ok "parent connection after service stop"
                (Nats_eio.Connection.publish service_connection
                   (subject ("outside-" ^ suffix))
                   "still-open");
              expect_ok "parent connection flush after service stop"
                (Nats_eio.Connection.flush service_connection);
              print_endline "service: ok")))

let () =
  Printexc.record_backtrace true;
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server service acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("server service acceptance failed: " ^ Printexc.to_string error);
      let backtrace = Printexc.get_backtrace () in
      if String.length backtrace > 0 then prerr_endline backtrace;
      exit 1
