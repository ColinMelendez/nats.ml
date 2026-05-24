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
                 (fun _ -> ()))
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
          let first, first_u = Eio.Promise.create () in
          let second, second_u = Eio.Promise.create () in
          let stats_request, stats_request_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await ping;
                `Await first;
                `Await second;
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
                       if
                         String.equal
                           (Nats_eio.Service.Request.payload request)
                           "bad"
                       then
                         expect_ok
                           (Nats_eio.Service.Request.respond_error ~code:"400"
                              ~description:"bad request" ~payload:"rejected"
                              request)
                       else
                         expect_ok
                           (Nats_eio.Service.Request.respond request "accepted");
                       incr calls;
                       if Int.equal !calls 2 then
                         Eio.Promise.resolve handled_u ()))
              in
              expect_ok (Nats_eio.Service.add_endpoint service endpoint);
              let group =
                expect_service
                  (Nats_eio.Service.add_group
                     ~queue:Nats_eio.Service.Config.Disabled service
                     ~name:"admin")
              in
              let grouped =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"status" (fun _ -> ()))
              in
              expect_ok (Nats_eio.Service.Group.add_endpoint group grouped);
              let output = Buffer.contents trace in
              if not (contains ~needle:"wrote \"SUB $SRV.PING 1\\r\\n\"" output)
              then fail "general PING monitoring subscription is missing";
              if
                not
                  (contains ~needle:"wrote \"SUB $SRV.PING.orders 2\\r\\n\""
                     output)
              then
                fail
                  (Format.asprintf
                     "service-name PING monitoring subscription is missing\\n%s"
                     output);
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
              Eio.Promise.resolve first_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created" ~reply:"_INBOX.ok"
                      "ok"));
              yield_n 6;
              Eio.Promise.resolve second_u
                (Ok
                   (request_wire ~sid:10 ~subject:"created" ~reply:"_INBOX.bad"
                      "bad"));
              Eio.Promise.await handled;
              Eio.Promise.resolve stats_request_u
                (Ok
                   (request_wire ~sid:7 ~subject:"$SRV.STATS"
                      ~reply:"_INBOX.stats" ""));
              yield_n 8;
              let output = Buffer.contents trace in
              if not (contains ~needle:"io.nats.micro.v1.ping_response" output)
              then fail "PING response was not encoded";
              if not (contains ~needle:"Nats-Service-Error" output) then
                fail "service-error response did not carry its headers";
              if not (contains ~needle:{|num_requests\":2|} output) then
                fail "stats did not count endpoint requests";
              if not (contains ~needle:{|num_errors\":1|} output) then
                fail "stats did not count service errors";
              let info = Nats_eio.Service.info service in
              equal string "orders" (Nats_eio.Service.Info.name info);
              equal int 2 (List.length (Nats_eio.Service.Info.endpoints info));
              equal int 22 (String.length (Nats_eio.Service.id service));
              let service_stats = Nats_eio.Service.stats service in
              equal int 30
                (String.length (Nats_eio.Service.Stats.started service_stats));
              expect_connection_ok (Nats_eio.Connection.close connection);
              expect_ok (Nats_eio.Service.stop service);
              expect_ok (Nats_eio.Service.stop service);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
