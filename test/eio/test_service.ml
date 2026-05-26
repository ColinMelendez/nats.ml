open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.10.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let endpoint =
  match Nats.Endpoint.of_string "nats://127.0.0.1:4222" with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)

let address = `Tcp (Eio.Net.Ipaddr.V4.loopback, 4222)

let expect_config = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)

let expect_endpoint = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)

let expect_service = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)

let expect_connection_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let yield_n count =
  let count = ref count in
  while !count > 0 do
    Eio.Fiber.yield ();
    decr count
  done

let request_wire ~sid ~subject ~reply payload =
  Format.asprintf "MSG %s %d %s %d\r\n%s\r\n" subject sid reply
    (String.length payload) payload

let with_connection_traced ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "service-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "service-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 32 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return flow ];
  let trace = Buffer.create 8192 in
  let debug = Eio.Stdenv.debug env in
  let tracer =
    {
      Eio.Debug.traceln =
        (fun ?__POS__:_ fmt ->
          Format.kasprintf
            (fun message ->
              Buffer.add_string trace message;
              Buffer.add_char trace '\n')
            fmt);
    }
  in
  Eio.Fiber.with_binding debug#traceln tracer @@ fun () ->
  Eio.Switch.run @@ fun sw ->
  let connection =
    match
      Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock [ endpoint ]
    with
    | Ok value -> value
    | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)
  in
  f ~sw ~clock:env#clock ~trace connection

let with_reconnecting_connection_traced ~config ~first_reads ~second_reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let first = Eio_mock.Flow.make "service-server-first" in
  Eio_mock.Flow.on_read first first_reads;
  let second = Eio_mock.Flow.make "service-server-second" in
  Eio_mock.Flow.on_read second second_reads;
  let net = Eio_mock.Net.make "service-reconnect-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 64 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return first; `Return second ];
  let trace = Buffer.create 8192 in
  let debug = Eio.Stdenv.debug env in
  let tracer =
    {
      Eio.Debug.traceln =
        (fun ?__POS__:_ fmt ->
          Format.kasprintf
            (fun message ->
              Buffer.add_string trace message;
              Buffer.add_char trace '\n')
            fmt);
    }
  in
  Eio.Fiber.with_binding debug#traceln tracer @@ fun () ->
  Eio.Switch.run @@ fun sw ->
  let connection =
    match
      Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ~config
        [ endpoint ]
    with
    | Ok value -> value
    | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)
  in
  f ~sw ~clock:env#clock ~mono_clock:env#mono_clock ~trace connection

let contains ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let found = ref false in
  while (not !found) && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      found := true;
    incr index
  done;
  !found

let escaped_json_field name value =
  name ^ String.make 1 (Char.chr 92) ^ "\"" ^ ":" ^ value

let count ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let found = ref 0 in
  while !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      incr found;
    incr index
  done;
  !found

let wait_for_trace ~mono_clock ~trace ~needle ~expected =
  let seen = ref 0 in
  let attempts = ref 0 in
  while !seen < expected && !attempts < 100 do
    Eio.Time.Mono.sleep mono_clock 0.001;
    seen := count ~needle (Buffer.contents trace);
    incr attempts
  done;
  if !seen < expected then
    fail
      (Format.asprintf "trace did not contain %d occurrences of %S (saw %d)"
         expected needle !seen)

let wait_for_trace_yields ~trace ~needle ~expected =
  let seen = ref 0 in
  let attempts = ref 0 in
  while !seen < expected && !attempts < 1000 do
    Eio.Fiber.yield ();
    seen := count ~needle (Buffer.contents trace);
    incr attempts
  done;
  if !seen < expected then
    fail
      (Format.asprintf "trace did not contain %d occurrences of %S (saw %d)"
         expected needle !seen)

let () =
  run "nats-eio-service"
    [
      test "configuration and endpoint values validate their contracts"
        (fun () ->
          let config =
            expect_config
              (Nats_eio.Service.Config.v ~name:"orders-api" ~version:"1.2.3"
                 ~description:"orders"
                 ~metadata:[ ("team", "infra") ]
                 ~queue:Nats_eio.Service.Config.Disabled ())
          in
          equal string "orders-api" (Nats_eio.Service.Config.name config);
          equal string "1.2.3" (Nats_eio.Service.Config.version config);
          equal (option string) (Some "orders")
            (Nats_eio.Service.Config.description config);
          equal int 1 (List.length (Nats_eio.Service.Config.metadata config));
          (match Nats_eio.Service.Config.queue config with
          | Nats_eio.Service.Config.Disabled -> ()
          | Nats_eio.Service.Config.Default | Nats_eio.Service.Config.Queue _ ->
              fail "queue policy changed");
          (match Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2" () with
          | Error
              (Nats_eio.Service.Error.Invalid_config
                 (Nats_eio.Service.Error.Invalid_version "1.2")) ->
              ()
          | Error error ->
              fail
                (Format.asprintf "unexpected config error: %a"
                   Nats_eio.Service.Error.pp error)
          | Ok _ -> fail "invalid semantic version was accepted");
          (match
             Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2.3"
               ~metadata:[ ("team", "infra"); ("team", "platform") ]
               ()
           with
          | Error
              (Nats_eio.Service.Error.Invalid_config
                 (Nats_eio.Service.Error.Duplicate_metadata "team")) ->
              ()
          | Error error ->
              fail
                (Format.asprintf "unexpected metadata error: %a"
                   Nats_eio.Service.Error.pp error)
          | Ok _ -> fail "duplicate metadata was accepted");
          let subject =
            match Nats.Subject.Filter.of_string "orders.*" with
            | Ok value -> value
            | Error error ->
                fail (Format.asprintf "%a" Nats.Subject.pp_error error)
          in
          let endpoint =
            expect_endpoint
              (Nats_eio.Service.Endpoint.v ~name:"created" ~subject ~metadata:[]
                 (fun request ->
                   ignore (Nats_eio.Service.Request.payload request);
                   Ok ()))
          in
          equal string "created" (Nats_eio.Service.Endpoint.name endpoint);
          equal string "orders.*"
            (Nats.Subject.Filter.to_string
               (Nats_eio.Service.Endpoint.subject endpoint));
          equal
            (option (list (pair string string)))
            (Some [])
            (Nats_eio.Service.Endpoint.metadata endpoint));
      test "monitoring, queue policy, request replies, and stats" (fun () ->
          let ping, ping_u = Eio.Promise.create () in
          let info_request, info_request_u = Eio.Promise.create () in
          let first, first_u = Eio.Promise.create () in
          let second, second_u = Eio.Promise.create () in
          let third, third_u = Eio.Promise.create () in
          let fourth, fourth_u = Eio.Promise.create () in
          let stats_request, stats_request_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await ping;
                `Await info_request;
                `Await first;
                `Await second;
                `Await third;
                `Await fourth;
                `Await stats_request;
                `Await hold;
              ]
            (fun ~sw ~clock ~trace connection ->
              let config =
                expect_config
                  (Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2.3"
                     ~metadata:[ ("team", "infra") ]
                     ())
              in
              let service =
                expect_service
                  (Nats_eio.Service.v ~sw ~clock
                     ~random:(Random.State.make [| 1; 2; 3 |])
                     connection config)
              in
              let handled, handled_u = Eio.Promise.create () in
              let calls = ref 0 in
              let endpoint =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"created" (fun request ->
                       let result =
                         if
                           String.equal
                             (Nats_eio.Service.Request.payload request)
                             "bad"
                         then
                           Nats_eio.Service.Request.respond_error ~code:"400"
                             ~description:"bad request" ~payload:"rejected"
                             request
                         else if
                           String.equal
                             (Nats_eio.Service.Request.payload request)
                             "error"
                         then Error Nats_eio.Service.Error.No_response
                         else if
                           String.equal
                             (Nats_eio.Service.Request.payload request)
                             "race"
                         then
                           let first_result, first_result_u =
                             Eio.Promise.create ()
                           in
                           Eio.Fiber.fork ~sw (fun () ->
                               Eio.Promise.resolve first_result_u
                                 (Nats_eio.Service.Request.respond request
                                    "first"));
                           Eio.Fiber.yield ();
                           let second_result =
                             Nats_eio.Service.Request.respond request "second"
                           in
                           (match Eio.Promise.await first_result, second_result with
                           | Ok (),
                             Error Nats_eio.Service.Error.Already_responded
                           | Error Nats_eio.Service.Error.Already_responded, Ok () ->
                               Ok ()
                           | Error _, Error _ ->
                               Error Nats_eio.Service.Error.No_response
                           | Ok (), Ok () ->
                               Error Nats_eio.Service.Error.No_response
                           | Ok (), Error _ | Error _, Ok () ->
                               Error Nats_eio.Service.Error.No_response)
                         else
                           Nats_eio.Service.Request.respond request "accepted"
                       in
                       incr calls;
                       if Int.equal !calls 4 then
                         Eio.Promise.resolve handled_u ();
                       result))
              in
              expect_ok (Nats_eio.Service.add_endpoint service endpoint);
              let group =
                expect_service
                  (Nats_eio.Service.add_group
                     ~queue:Nats_eio.Service.Config.Disabled service
                     ~name:"admin")
              in
              equal string "admin" (Nats_eio.Service.Group.name group);
              equal string "admin"
                (Nats.Subject.to_string (Nats_eio.Service.Group.subject group));
              let nested =
                expect_service
                  (Nats_eio.Service.Group.add_group group ~name:"ops")
              in
              equal string "ops" (Nats_eio.Service.Group.name nested);
              equal string "admin.ops"
                (Nats.Subject.to_string (Nats_eio.Service.Group.subject nested));
              let grouped =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"status" (fun request ->
                       ignore (Nats_eio.Service.Request.payload request);
                       Ok ()))
              in
              expect_ok (Nats_eio.Service.Group.add_endpoint group grouped);
              let output = Buffer.contents trace in
              if not (contains ~needle:"wrote \"SUB $SRV.PING 1\\r\\n\"" output)
              then fail "general PING monitoring subscription is missing";
              if
                not
                  (contains ~needle:"wrote \"SUB $SRV.PING.orders 2\\r\\n\""
                     output)
              then fail "service-name PING monitoring subscription is missing";
              if not (contains ~needle:"wrote \"SUB $SRV.PING.orders." output)
              then fail "instance PING monitoring subscription is missing";
              if
                not (contains ~needle:"wrote \"SUB created q 10\\r\\n\"" output)
              then fail "default endpoint queue was not q";
              if
                not
                  (contains ~needle:"wrote \"SUB admin.status 11\\r\\n\"" output)
              then fail "disabled group queue was not omitted";
              Eio.Promise.resolve ping_u
                (Ok
                   (request_wire ~sid:1 ~subject:"$SRV.PING"
                      ~reply:"_INBOX.ping" ""));
              yield_n 6;
              Eio.Promise.resolve info_request_u
                (Ok
                   (request_wire ~sid:4 ~subject:"$SRV.INFO"
                      ~reply:"_INBOX.info" ""));
              yield_n 6;
              Eio.Promise.resolve first_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created" ~reply:"_INBOX.ok"
                      "ok"));
              yield_n 6;
              Eio.Promise.resolve second_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created" ~reply:"_INBOX.bad"
                      "bad"));
              yield_n 6;
              Eio.Promise.resolve third_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created"
                      ~reply:"_INBOX.error" "error"));
              yield_n 6;
              Eio.Promise.resolve fourth_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created"
                      ~reply:"_INBOX.race" "race"));
              Eio.Promise.await handled;
              Eio.Promise.resolve stats_request_u
                (Ok
                   (request_wire ~sid:7 ~subject:"$SRV.STATS"
                      ~reply:"_INBOX.stats" ""));
              yield_n 8;
              let output = Buffer.contents trace in
              if not (contains ~needle:"io.nats.micro.v1.ping_response" output)
              then fail "PING response was not encoded";
              if not (contains ~needle:"io.nats.micro.v1.info_response" output)
              then fail "INFO response was not encoded";
              if
                not
                  (contains ~needle:{|description":""|} output
                  || contains ~needle:{|description\":\"\"|} output)
              then
                fail
                  (Format.asprintf "INFO response omitted its description:\n%s"
                     output);
              if
                not
                  (contains ~needle:{|queue_group":""|} output
                  || contains ~needle:{|queue_group\":\"\"|} output)
              then
                fail
                  (Format.asprintf
                     "INFO response omitted a disabled queue group:\n%s" output);
              if not (contains ~needle:"Nats-Service-Error" output) then
                fail "service-error response did not carry its headers";
              if
                not
                  (contains
                     ~needle:(escaped_json_field "num_requests" "4")
                     output)
              then fail "stats did not count endpoint requests";
              if
                not
                  (contains
                     ~needle:(escaped_json_field "num_errors" "2")
                     output)
              then fail "stats did not count service errors";
              if
                not
                  (Int.equal
                     (count ~needle:{|wrote "PUB _INBOX.race|} output)
                     1)
              then fail "concurrent request replies were published more than once";
              let info = Nats_eio.Service.info service in
              equal string "orders" (Nats_eio.Service.Info.name info);
              equal int 2 (List.length (Nats_eio.Service.Info.endpoints info));
              equal (list string) [ "created"; "status" ]
                (List.map Nats_eio.Service.Info.endpoint_name
                   (Nats_eio.Service.Info.endpoints info));
              equal int 22 (String.length (Nats_eio.Service.id service));
              let service_stats = Nats_eio.Service.stats service in
              equal int 30
                (String.length (Nats_eio.Service.Stats.started service_stats));
              expect_connection_ok (Nats_eio.Connection.close connection);
              expect_ok (Nats_eio.Service.stop service);
              expect_ok (Nats_eio.Service.stop service);
              (match
                 Nats_eio.Service.Group.add_group group ~name:"after-stop"
               with
              | Error Nats_eio.Service.Error.Stopped -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)
              | Ok _ -> fail "nested group was added after service stop");
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "replayable service subscriptions survive reconnect" (fun () ->
          let disconnect, disconnect_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let config =
            match
              Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 1)
                ~reconnect_delay:Mtime.Span.(1 * ns)
                ~reconnect_max_delay:Mtime.Span.(1 * ns)
                ()
            with
            | Ok value -> value
            | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)
          in
          with_reconnecting_connection_traced ~config
            ~first_reads:[ `Return info_wire; `Await disconnect ]
            ~second_reads:[ `Return info_wire; `Await delivery; `Await hold ]
            (fun ~sw ~clock ~mono_clock ~trace connection ->
              let service_config =
                expect_config
                  (Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2.3" ())
              in
              let service =
                expect_service
                  (Nats_eio.Service.v ~sw ~clock
                     ~random:(Random.State.make [| 7; 8; 9 |])
                     connection service_config)
              in
              let handled, handled_u = Eio.Promise.create () in
              let endpoint =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"created" (fun request ->
                       let result =
                         Nats_eio.Service.Request.respond request "after"
                       in
                       Eio.Promise.resolve handled_u ();
                       result))
              in
              expect_ok (Nats_eio.Service.add_endpoint service endpoint);
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              wait_for_trace ~mono_clock ~trace
                ~needle:"wrote \"SUB created q 10\\r\\n\"" ~expected:2;
              Eio.Promise.resolve delivery_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created"
                      ~reply:"_INBOX.after" "request"));
              Eio.Promise.await handled;
              let output = Buffer.contents trace in
              List.iter
                (fun subject ->
                  let needle = Format.asprintf "wrote \"SUB %s" subject in
                  if count ~needle output < 2 then
                    fail
                      (Format.asprintf
                         "monitoring subscription %s was not replayed" subject))
                [ "$SRV.PING"; "$SRV.INFO"; "$SRV.STATS" ];
              expect_connection_ok (Nats_eio.Connection.close connection);
              expect_ok (Nats_eio.Service.stop service);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "service stop drains only owned subscriptions" (fun () ->
          let pong1, pong1_u = Eio.Promise.create () in
          let pong2, pong2_u = Eio.Promise.create () in
          let pong3, pong3_u = Eio.Promise.create () in
          let pong4, pong4_u = Eio.Promise.create () in
          let pong5, pong5_u = Eio.Promise.create () in
          let pong6, pong6_u = Eio.Promise.create () in
          let pong7, pong7_u = Eio.Promise.create () in
          let pong8, pong8_u = Eio.Promise.create () in
          let pong9, pong9_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await pong1;
                `Await pong2;
                `Await pong3;
                `Await pong4;
                `Await pong5;
                `Await pong6;
                `Await pong7;
                `Await pong8;
                `Await pong9;
                `Await hold;
              ]
            (fun ~sw ~clock ~trace connection ->
              let config =
                expect_config
                  (Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2.3" ())
              in
              let service =
                expect_service (Nats_eio.Service.v ~sw ~clock connection config)
              in
              let pongs =
                [
                  pong1_u;
                  pong2_u;
                  pong3_u;
                  pong4_u;
                  pong5_u;
                  pong6_u;
                  pong7_u;
                  pong8_u;
                  pong9_u;
                ]
              in
              Eio.Fiber.fork ~sw (fun () ->
                  List.iteri
                    (fun index resolver ->
                      wait_for_trace_yields ~trace
                        ~needle:"wrote \"PING\\r\\n\"" ~expected:(index + 1);
                      Eio.Promise.resolve resolver (Ok "PONG\r\n"))
                    pongs);
              expect_ok (Nats_eio.Service.stop service);
              let subject =
                match Nats.Subject.of_string "outside.service" with
                | Ok value -> value
                | Error error ->
                    fail (Format.asprintf "%a" Nats.Subject.pp_error error)
              in
              expect_connection_ok
                (Nats_eio.Connection.publish connection subject "still-open");
              expect_connection_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
