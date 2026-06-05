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

let expect_header label expected message =
  match Nats.Header.find "X-Worker" (Nats.Message.headers message) with
  | Some actual -> expect_string label expected actual
  | None -> failf "%s was absent, expected %S" label expected

let expect_info_endpoint service_name endpoint_name probe_name info =
  expect_string "service info name" service_name
    (Nats_eio.Service.Info.name info);
  let endpoints = Nats_eio.Service.Info.endpoints info in
  let endpoint =
    match
      List.find_opt
        (fun value ->
          String.equal (Nats_eio.Service.Info.endpoint_name value) endpoint_name)
        endpoints
    with
    | Some value -> value
    | None -> failf "service did not advertise its queue endpoint"
  in
  expect_string "service endpoint subject" endpoint_name
    (Nats.Subject.Filter.to_string
       (Nats_eio.Service.Info.endpoint_subject endpoint));
  (match Nats_eio.Service.Info.endpoint_queue endpoint with
  | Some queue ->
      expect_string "service endpoint queue" "q"
        (Nats.Queue_group.to_string queue)
  | None -> failf "service endpoint did not advertise its queue group");
  if
    not
      (List.exists
         (fun value ->
           String.equal (Nats_eio.Service.Info.endpoint_name value) probe_name)
         endpoints)
  then failf "service did not advertise its instance probe endpoint"

let make_service ~sw ~clock ~connection ~service_name ~endpoint_name ~probe_name
    ~worker ~random_seed =
  let config =
    expect_service_ok "service config"
      (Nats_eio.Service.Config.v ~name:service_name ~version:"1.0.0" ())
  in
  let headers =
    match Nats.Header.of_list [ ("X-Worker", worker) ] with
    | Ok headers -> headers
    | Error error -> failf "worker headers: %a" Nats.Header.pp_error error
  in
  let endpoint =
    expect_service_ok "service endpoint"
      (Nats_eio.Service.Endpoint.v ~name:endpoint_name (fun request ->
           Nats_eio.Service.Request.respond ~headers request worker))
  in
  let probe_endpoint =
    expect_service_ok "service probe endpoint"
      (Nats_eio.Service.Endpoint.v ~name:probe_name
         ~queue:Nats_eio.Service.Config.Disabled (fun request ->
           Nats_eio.Service.Request.respond ~headers request worker))
  in
  let service =
    expect_service_ok "service start"
      (Nats_eio.Service.v ~sw ~clock
         ~random:(Random.State.make [| random_seed |])
         connection config)
  in
  expect_service_ok "service endpoint registration"
    (Nats_eio.Service.add_endpoint service endpoint);
  expect_service_ok "service probe endpoint registration"
    (Nats_eio.Service.add_endpoint service probe_endpoint);
  expect_ok "service subscription flush" (Nats_eio.Connection.flush connection);
  service

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
  let suffix = run_id () in
  let service_name = "ocaml-queue-" ^ suffix in
  let endpoint_name = "work-" ^ suffix in
  let probe_one_name = "probe-one-" ^ suffix in
  let probe_two_name = "probe-two-" ^ suffix in
  let service_one_connection = connect "first service connect" in
  Fun.protect
    ~finally:(fun () ->
      ignore (Nats_eio.Connection.close service_one_connection))
    (fun () ->
      let service_one =
        make_service ~sw ~clock ~connection:service_one_connection ~service_name
          ~endpoint_name ~probe_name:probe_one_name ~worker:"worker-one"
          ~random_seed:1
      in
      Fun.protect
        ~finally:(fun () -> ignore (Nats_eio.Service.stop service_one))
        (fun () ->
          let service_two_connection = connect "second service connect" in
          Fun.protect
            ~finally:(fun () ->
              ignore (Nats_eio.Connection.close service_two_connection))
            (fun () ->
              let service_two =
                make_service ~sw ~clock ~connection:service_two_connection
                  ~service_name ~endpoint_name ~probe_name:probe_two_name
                  ~worker:"worker-two" ~random_seed:2
              in
              Fun.protect
                ~finally:(fun () -> ignore (Nats_eio.Service.stop service_two))
                (fun () ->
                  let requester = connect "queue requester connect" in
                  Fun.protect
                    ~finally:(fun () ->
                      ignore (Nats_eio.Connection.close requester))
                    (fun () ->
                      let target =
                        Nats_eio.Service.Discovery.Named service_name
                      in
                      let info =
                        expect_service_ok "queue service info"
                          (Nats_eio.Service.Discovery.info
                             ~timeout:Mtime.Span.(2 * s)
                             ~target requester)
                      in
                      if not (Int.equal (List.length info) 2) then
                        failf "queue discovery returned %d services, expected 2"
                          (List.length info);
                      List.iter
                        (fun service_info ->
                          let probe_name =
                            if
                              String.equal
                                (Nats_eio.Service.Info.id service_info)
                                (Nats_eio.Service.id service_one)
                            then probe_one_name
                            else if
                              String.equal
                                (Nats_eio.Service.Info.id service_info)
                                (Nats_eio.Service.id service_two)
                            then probe_two_name
                            else failf "queue discovery returned an unknown id"
                          in
                          expect_info_endpoint service_name endpoint_name
                            probe_name service_info)
                        info;
                      let request_count = 8 in
                      let worker_one_count = ref 0 in
                      let worker_two_count = ref 0 in
                      for index = 1 to request_count do
                        let response =
                          expect_ok "queue request"
                            (Nats_eio.Connection.request
                               ~timeout:Mtime.Span.(2 * s)
                               requester (subject endpoint_name)
                               (string_of_int index))
                        in
                        match Nats.Message.payload response with
                        | "worker-one" ->
                            incr worker_one_count;
                            expect_header "worker-one response" "worker-one"
                              response
                        | "worker-two" ->
                            incr worker_two_count;
                            expect_header "worker-two response" "worker-two"
                              response
                        | payload ->
                            failf "queue response payload was %S" payload
                      done;
                      let probe_response_one =
                        expect_ok "first worker probe"
                          (Nats_eio.Connection.request
                             ~timeout:Mtime.Span.(2 * s)
                             requester (subject probe_one_name) "probe")
                      in
                      expect_string "first worker probe payload" "worker-one"
                        (Nats.Message.payload probe_response_one);
                      expect_header "first worker probe header" "worker-one"
                        probe_response_one;
                      let probe_response_two =
                        expect_ok "second worker probe"
                          (Nats_eio.Connection.request
                             ~timeout:Mtime.Span.(2 * s)
                             requester (subject probe_two_name) "probe")
                      in
                      expect_string "second worker probe payload" "worker-two"
                        (Nats.Message.payload probe_response_two);
                      expect_header "second worker probe header" "worker-two"
                        probe_response_two;
                      let stats =
                        expect_service_ok "queue service stats"
                          (Nats_eio.Service.Discovery.stats
                             ~timeout:Mtime.Span.(2 * s)
                             ~target requester)
                      in
                      if not (Int.equal (List.length stats) 2) then
                        failf "queue stats returned %d services, expected 2"
                          (List.length stats);
                      let total_requests = ref 0L in
                      List.iter
                        (fun service_stats ->
                          expect_string "queue stats name" service_name
                            (Nats_eio.Service.Stats.name service_stats);
                          let worker_count, probe_name, expected_id =
                            if
                              String.equal
                                (Nats_eio.Service.Stats.id service_stats)
                                (Nats_eio.Service.id service_one)
                            then (!worker_one_count, probe_one_name, "first")
                            else if
                              String.equal
                                (Nats_eio.Service.Stats.id service_stats)
                                (Nats_eio.Service.id service_two)
                            then (!worker_two_count, probe_two_name, "second")
                            else failf "queue stats returned an unknown service"
                          in
                          let endpoints =
                            Nats_eio.Service.Stats.endpoints service_stats
                          in
                          let endpoint =
                            match
                              List.find_opt
                                (fun value ->
                                  String.equal
                                    (Nats_eio.Service.Stats.endpoint_name value)
                                    endpoint_name)
                                endpoints
                            with
                            | Some value -> value
                            | None ->
                                failf "queue stats omitted the shared endpoint"
                          in
                          let probe =
                            match
                              List.find_opt
                                (fun value ->
                                  String.equal
                                    (Nats_eio.Service.Stats.endpoint_name value)
                                    probe_name)
                                endpoints
                            with
                            | Some value -> value
                            | None ->
                                failf
                                  "queue stats omitted the %s probe endpoint"
                                  expected_id
                          in
                          let requests =
                            Nats_eio.Service.Stats.num_requests endpoint
                          in
                          if
                            not
                              (Int64.equal requests (Int64.of_int worker_count))
                          then
                            failf
                              "queue stats disagreed with %s worker: %Ld vs %d"
                              expected_id requests worker_count;
                          if
                            not
                              (Int64.equal
                                 (Nats_eio.Service.Stats.num_errors endpoint)
                                 0L)
                          then failf "queue worker recorded an error";
                          if
                            not
                              (Int64.equal
                                 (Nats_eio.Service.Stats.num_requests probe)
                                 1L)
                          then failf "queue probe stats did not count its probe";
                          total_requests := Int64.add !total_requests requests)
                        stats;
                      if
                        not
                          (Int64.equal !total_requests
                             (Int64.of_int request_count))
                      then
                        failf "queue workers recorded %Ld requests, expected %d"
                          !total_requests request_count;
                      expect_service_ok "stop first queue service"
                        (Nats_eio.Service.stop service_one);
                      let remaining_response =
                        expect_ok "remaining queue worker request"
                          (Nats_eio.Connection.request
                             ~timeout:Mtime.Span.(2 * s)
                             requester (subject endpoint_name)
                             "after-first-stop")
                      in
                      expect_string "remaining queue worker payload"
                        "worker-two"
                        (Nats.Message.payload remaining_response);
                      expect_header "remaining queue worker header" "worker-two"
                        remaining_response;
                      expect_service_ok "stop second queue service"
                        (Nats_eio.Service.stop service_two);
                      (match
                         Nats_eio.Connection.request
                           ~timeout:Mtime.Span.(500 * ms)
                           requester (subject endpoint_name) "after-stop"
                       with
                      | Error Nats_eio.Error.No_responders -> ()
                      | Ok _ -> failf "stopped queue services still answered"
                      | Error error ->
                          failf "queue request after stop: %s"
                            (error_message error));
                      print_endline "service_queue: ok")))))

let () =
  Printexc.record_backtrace true;
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server service-queue acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("server service-queue acceptance failed: " ^ Printexc.to_string error);
      let backtrace = Printexc.get_backtrace () in
      if String.length backtrace > 0 then prerr_endline backtrace;
      exit 1
