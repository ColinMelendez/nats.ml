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

let expect_discovery = function
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

let delivery_wire ~sid ~subject payload =
  Format.asprintf "MSG %s %d %d\r\n%s\r\n" subject sid (String.length payload)
    payload

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

let find_from ~needle ~start value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref start in
  let found = ref None in
  while Option.is_none !found && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      found := Some !index
    else incr index
  done;
  !found

let subscribed_inbox ~trace ~occurrence =
  let output = Buffer.contents trace in
  let marker = "wrote \"SUB _INBOX.ocaml." in
  let start = ref 0 in
  let marker_start = ref None in
  let count = ref 0 in
  while Option.is_none !marker_start && !count < occurrence do
    match find_from ~needle:marker ~start:!start output with
    | None -> count := occurrence
    | Some index ->
        incr count;
        start := index + String.length marker;
        if Int.equal !count occurrence then marker_start := Some !start
  done;
  match !marker_start with
  | None -> fail "discovery inbox subscription was not traced"
  | Some start -> (
      match find_from ~needle:" " ~start output with
      | None -> fail "discovery inbox subscription has no sid"
      | Some finish -> "_INBOX.ocaml." ^ String.sub output start (finish - start)
      )

let subscribed_sid ~trace ~occurrence =
  let output = Buffer.contents trace in
  let marker = "wrote \"SUB _INBOX.ocaml." in
  let start = ref 0 in
  let marker_start = ref None in
  let count = ref 0 in
  while Option.is_none !marker_start && !count < occurrence do
    match find_from ~needle:marker ~start:!start output with
    | None -> count := occurrence
    | Some index ->
        incr count;
        start := index + String.length marker;
        if Int.equal !count occurrence then marker_start := Some !start
  done;
  match !marker_start with
  | None -> fail "discovery inbox subscription was not traced"
  | Some start -> (
      match find_from ~needle:"\\r\\n" ~start output with
      | None -> fail "discovery inbox subscription has no line ending"
      | Some finish -> (
          let line = String.sub output start (finish - start) in
          match find_from ~needle:" " ~start:0 line with
          | None -> fail "discovery inbox subscription has no sid"
          | Some sid_start ->
              int_of_string
                (String.sub line (sid_start + 1)
                   (String.length line - sid_start - 1))))

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
          let pending_limits =
            expect_ok
              (Nats_eio.Service.Endpoint.Pending_limits.v ~messages:4 ~bytes:128)
          in
          equal int 4
            (Nats_eio.Service.Endpoint.Pending_limits.messages pending_limits);
          equal int 128
            (Nats_eio.Service.Endpoint.Pending_limits.bytes pending_limits);
          (match
             Nats_eio.Service.Endpoint.Pending_limits.v ~messages:0 ~bytes:0
           with
          | Error
              (Nats_eio.Service.Error.Invalid_endpoint
                 Nats_eio.Service.Error.Invalid_pending_limits) ->
              ()
          | Error error ->
              fail
                (Format.asprintf "unexpected pending-limit error: %a"
                   Nats_eio.Service.Error.pp error)
          | Ok _ -> fail "zero pending limits were accepted");
          (match
             Nats_eio.Service.Endpoint.Pending_limits.v ~messages:(-2) ~bytes:1
           with
          | Error
              (Nats_eio.Service.Error.Invalid_endpoint
                 (Nats_eio.Service.Error.Invalid_pending_limit
                    { field = "messages"; value = -2 })) ->
              ()
          | Error error ->
              fail
                (Format.asprintf "unexpected pending-limit error: %a"
                   Nats_eio.Service.Error.pp error)
          | Ok _ -> fail "negative pending limit was accepted");
          let endpoint =
            expect_endpoint
              (Nats_eio.Service.Endpoint.v ~name:"created" ~subject ~metadata:[]
                 ~pending_limits
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
            (Nats_eio.Service.Endpoint.metadata endpoint);
          equal
            (option (pair int int))
            (Some (4, 128))
            (Option.map
               (fun limits ->
                 ( Nats_eio.Service.Endpoint.Pending_limits.messages limits,
                   Nats_eio.Service.Endpoint.Pending_limits.bytes limits ))
               (Nats_eio.Service.Endpoint.pending_limits endpoint)));
      test "discovery collects monitoring replies by target" (fun () ->
          let ping_read, ping_read_u = Eio.Promise.create () in
          let info_read, info_read_u = Eio.Promise.create () in
          let stats_read, stats_read_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let ping_one =
            {|{"type":"io.nats.micro.v1.ping_response","name":"orders","id":"a","version":"1.2.3","metadata":{"team":"infra"}}|}
          in
          let ping_two =
            {|{"type":"io.nats.micro.v1.ping_response","name":"orders","id":"b","version":"1.2.4","metadata":{}}|}
          in
          let info_payload =
            {|{"type":"io.nats.micro.v1.info_response","name":"orders","id":"a","version":"1.2.3","description":"orders","metadata":{"team":"infra"},"endpoints":[{"name":"created","subject":"orders.created","queue_group":"q","metadata":null}]}|}
          in
          let stats_payload =
            {|{"type":"io.nats.micro.v1.stats_response","name":"orders","id":"a","version":"1.2.3","metadata":{"team":"infra"},"started":"2026-08-12T00:00:00.000000000Z","endpoints":[{"name":"created","subject":"orders.created","queue_group":"q","metadata":null,"num_requests":4,"num_errors":1,"last_error":"bad request","processing_time":120,"average_processing_time":30,"data":"ready"}]}|}
          in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await ping_read;
                `Await info_read;
                `Await stats_read;
                `Await hold;
              ]
            (fun ~sw ~clock:_ ~trace connection ->
              let ping_result, ping_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ping_result_u
                    (Nats_eio.Service.Discovery.ping
                       ~timeout:Mtime.Span.(1 * ms)
                       ~target:(Nats_eio.Service.Discovery.Named "orders")
                       connection));
              wait_for_trace_yields ~trace ~needle:"wrote \"SUB _INBOX.ocaml."
                ~expected:1;
              let ping_inbox = subscribed_inbox ~trace ~occurrence:1 in
              let ping_sid = subscribed_sid ~trace ~occurrence:1 in
              Eio.Promise.resolve ping_read_u
                (Ok
                   (delivery_wire ~sid:ping_sid ~subject:ping_inbox ping_one
                   ^ delivery_wire ~sid:ping_sid ~subject:ping_inbox ping_two));
              let pings = expect_discovery (Eio.Promise.await ping_result) in
              equal int 2 (List.length pings);
              equal string "orders"
                (Nats_eio.Service.Discovery.Ping.name (List.hd pings));
              equal string "a"
                (Nats_eio.Service.Discovery.Ping.id (List.hd pings));
              let info_result, info_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve info_result_u
                    (Nats_eio.Service.Discovery.info
                       ~timeout:Mtime.Span.(1 * ms)
                       ~target:
                         (Nats_eio.Service.Discovery.Instance
                            { service = "orders"; id = "a" })
                       connection));
              wait_for_trace_yields ~trace ~needle:"wrote \"SUB _INBOX.ocaml."
                ~expected:2;
              let info_inbox = subscribed_inbox ~trace ~occurrence:2 in
              let info_sid = subscribed_sid ~trace ~occurrence:2 in
              Eio.Promise.resolve info_read_u
                (Ok
                   (delivery_wire ~sid:info_sid ~subject:info_inbox info_payload));
              let infos = expect_discovery (Eio.Promise.await info_result) in
              equal int 1 (List.length infos);
              let info = List.hd infos in
              equal string "orders" (Nats_eio.Service.Info.name info);
              equal string "orders.created"
                (Nats.Subject.Filter.to_string
                   (Nats_eio.Service.Info.endpoint_subject
                      (List.hd (Nats_eio.Service.Info.endpoints info))));
              let stats_result, stats_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve stats_result_u
                    (Nats_eio.Service.Discovery.stats
                       ~timeout:Mtime.Span.(1 * ms)
                       connection));
              wait_for_trace_yields ~trace ~needle:"wrote \"SUB _INBOX.ocaml."
                ~expected:3;
              let stats_inbox = subscribed_inbox ~trace ~occurrence:3 in
              let stats_sid = subscribed_sid ~trace ~occurrence:3 in
              Eio.Promise.resolve stats_read_u
                (Ok
                   (delivery_wire ~sid:stats_sid ~subject:stats_inbox
                      stats_payload));
              let stats = expect_discovery (Eio.Promise.await stats_result) in
              equal int 1 (List.length stats);
              let stats = List.hd stats in
              equal int64 4L
                (Nats_eio.Service.Stats.num_requests
                   (List.hd (Nats_eio.Service.Stats.endpoints stats)));
              (match
                 Nats_eio.Service.Stats.data
                   (List.hd (Nats_eio.Service.Stats.endpoints stats))
               with
              | Some (Jsont.String (value, _)) -> equal string "ready" value
              | Some _ -> fail "discovered custom stats data had the wrong type"
              | None -> fail "discovered custom stats data was omitted");
              (match
                 Nats_eio.Service.Discovery.ping
                   ~target:(Nats_eio.Service.Discovery.Named "") connection
               with
              | Error
                  (Nats_eio.Service.Error.Invalid_selector
                     Nats_eio.Service.Error.Empty_name) ->
                  ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)
              | Ok _ -> fail "empty discovery service name was accepted");
              if
                not
                  (contains ~needle:"wrote \"PUB $SRV.PING.orders"
                     (Buffer.contents trace))
              then fail "targeted PING subject was not published";
              if
                not
                  (contains ~needle:"wrote \"PUB $SRV.INFO.orders.a"
                     (Buffer.contents trace))
              then fail "targeted INFO subject was not published";
              if
                not
                  (contains ~needle:"wrote \"PUB $SRV.STATS"
                     (Buffer.contents trace))
              then fail "all-service STATS subject was not published";
              expect_connection_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "discovery rejects malformed monitoring payloads" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~clock:_ ~trace connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Service.Discovery.info
                       ~timeout:Mtime.Span.(1 * ms)
                       connection));
              wait_for_trace_yields ~trace ~needle:"wrote \"SUB _INBOX.ocaml."
                ~expected:1;
              let inbox = subscribed_inbox ~trace ~occurrence:1 in
              let sid = subscribed_sid ~trace ~occurrence:1 in
              Eio.Promise.resolve response_u
                (Ok
                   (delivery_wire ~sid ~subject:inbox
                      {|{"type":"io.nats.micro.v1.ping_response","name":"orders","id":"a","version":"1.2.3","description":"","metadata":{},"endpoints":[]}|}));
              (match Eio.Promise.await result with
              | Error
                  (Nats_eio.Service.Error.Unexpected_response_type
                     {
                       expected = "io.nats.micro.v1.info_response";
                       actual = "io.nats.micro.v1.ping_response";
                     }) ->
                  ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)
              | Ok _ -> fail "malformed discovery response was accepted");
              expect_connection_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "discovery accepts stats without endpoint metadata" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~clock:_ ~trace connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Service.Discovery.stats
                       ~timeout:Mtime.Span.(1 * ms)
                       connection));
              wait_for_trace_yields ~trace ~needle:"wrote \"SUB _INBOX.ocaml."
                ~expected:1;
              let inbox = subscribed_inbox ~trace ~occurrence:1 in
              let sid = subscribed_sid ~trace ~occurrence:1 in
              Eio.Promise.resolve response_u
                (Ok
                   (delivery_wire ~sid ~subject:inbox
                      {|{"type":"io.nats.micro.v1.stats_response","name":"orders","id":"a","version":"1.2.3","metadata":{},"started":"2026-01-01T00:00:00.000000000Z","endpoints":[{"name":"created","subject":"created","queue_group":"q","num_requests":1,"num_errors":0,"last_error":"","processing_time":1,"average_processing_time":1}]}|}));
              (match Eio.Promise.await result with
              | Ok [ stats ] -> (
                  equal string "orders" (Nats_eio.Service.Stats.name stats);
                  match Nats_eio.Service.Stats.endpoints stats with
                  | [ endpoint ] -> (
                      match
                        Nats_eio.Service.Stats.endpoint_metadata endpoint
                      with
                      | None -> ()
                      | Some _ ->
                          fail
                            "absent endpoint metadata was not preserved as None"
                      )
                  | _ ->
                      fail
                        "stats response returned an unexpected endpoint count")
              | Ok _ ->
                  fail "stats response returned an unexpected service count"
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error));
              expect_connection_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
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
                         then (
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
                           match
                             (Eio.Promise.await first_result, second_result)
                           with
                           | ( Ok (),
                               Error Nats_eio.Service.Error.Already_responded )
                           | ( Error Nats_eio.Service.Error.Already_responded,
                               Ok () ) ->
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
                   (request_wire ~sid:10 ~subject:"created" ~reply:"_INBOX.race"
                      "race"));
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
              then
                fail "concurrent request replies were published more than once";
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
              equal bool false (Nats_eio.Service.stopped service);
              Nats_eio.Service.reset service;
              let reset_stats = Nats_eio.Service.stats service in
              equal int 30
                (String.length (Nats_eio.Service.Stats.started reset_stats));
              (match Nats_eio.Service.Stats.endpoints reset_stats with
              | endpoint :: _ ->
                  equal int64 0L
                    (Nats_eio.Service.Stats.num_requests endpoint);
                  equal int64 0L
                    (Nats_eio.Service.Stats.num_errors endpoint);
                  equal string "" (Nats_eio.Service.Stats.last_error endpoint);
                  equal int64 0L
                    (Nats_eio.Service.Stats.processing_time endpoint)
              | [] -> fail "reset removed service endpoints");
              expect_connection_ok (Nats_eio.Connection.close connection);
              expect_ok (Nats_eio.Service.stop service);
              equal bool true (Nats_eio.Service.stopped service);
              expect_ok (Nats_eio.Service.stop service);
              (match
                 Nats_eio.Service.Group.add_group group ~name:"after-stop"
               with
              | Error Nats_eio.Service.Error.Stopped -> ()
              | Error error ->
                  fail (Format.asprintf "%a" Nats_eio.Service.Error.pp error)
              | Ok _ -> fail "nested group was added after service stop");
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "service callbacks expose stats data and stop ownership" (fun () ->
          let stats_request, stats_request_u = Eio.Promise.create () in
          let pong1, pong1_u = Eio.Promise.create () in
          let pong2, pong2_u = Eio.Promise.create () in
          let pong3, pong3_u = Eio.Promise.create () in
          let pong4, pong4_u = Eio.Promise.create () in
          let pong5, pong5_u = Eio.Promise.create () in
          let pong6, pong6_u = Eio.Promise.create () in
          let pong7, pong7_u = Eio.Promise.create () in
          let pong8, pong8_u = Eio.Promise.create () in
          let pong9, pong9_u = Eio.Promise.create () in
          let pong10, pong10_u = Eio.Promise.create () in
          let pong11, pong11_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await stats_request;
                `Await pong1;
                `Await pong2;
                `Await pong3;
                `Await pong4;
                `Await pong5;
                `Await pong6;
                `Await pong7;
                `Await pong8;
                `Await pong9;
                `Await pong10;
                `Await pong11;
                `Await hold;
              ]
            (fun ~sw ~clock ~trace connection ->
              let stats_calls = ref 0 in
              let error_calls = ref 0 in
              let done_calls = ref 0 in
              let config =
                expect_config
                  (Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2.3"
                     ~stats_handler:(fun endpoint ->
                       incr stats_calls;
                       if
                         String.equal
                           (Nats_eio.Service.Stats.endpoint_name endpoint)
                           "created"
                       then Some (Jsont.Json.string "ready")
                       else None)
                     ~error_handler:(fun _ -> incr error_calls)
                     ~done_handler:(fun () -> incr done_calls) ())
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
                  pong10_u;
                  pong11_u;
                ]
              in
              Eio.Fiber.fork ~sw (fun () ->
                  List.iteri
                    (fun index resolver ->
                      wait_for_trace_yields ~trace ~needle:"wrote \"PING\\r\\n\""
                        ~expected:(index + 1);
                      Eio.Promise.resolve resolver (Ok "PONG\r\n"))
                    pongs);
              let created =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"created" (fun request ->
                       ignore (Nats_eio.Service.Request.payload request);
                       Ok ()))
              in
              let status =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"status" (fun request ->
                       ignore (Nats_eio.Service.Request.payload request);
                       Ok ()))
              in
              expect_ok (Nats_eio.Service.add_endpoint service created);
              expect_ok (Nats_eio.Service.add_endpoint service status);
              Eio.Promise.resolve stats_request_u
                (Ok
                   (request_wire ~sid:7 ~subject:"$SRV.STATS"
                      ~reply:"_INBOX.stats" ""));
              yield_n 8;
              if
                not
                  (contains
                     ~needle:(escaped_json_field "data" "\\\"ready\\\"")
                     (Buffer.contents trace))
              then fail "stats response omitted custom endpoint data";
              let stats = Nats_eio.Service.stats service in
              equal int 4 !stats_calls;
              let find_endpoint name =
                match
                  List.find_opt
                    (fun endpoint ->
                      String.equal
                        (Nats_eio.Service.Stats.endpoint_name endpoint)
                        name)
                    (Nats_eio.Service.Stats.endpoints stats)
                with
                | Some endpoint -> endpoint
                | None -> fail ("stats endpoint " ^ name ^ " was missing")
              in
              (match
                 Nats_eio.Service.Stats.data (find_endpoint "created")
               with
              | Some (Jsont.String (value, _)) -> equal string "ready" value
              | Some _ -> fail "stats callback returned the wrong JSON value"
              | None -> fail "stats callback data was omitted");
              (match Nats_eio.Service.Stats.data (find_endpoint "status") with
              | None -> ()
              | Some _ -> fail "stats callback data leaked to another endpoint");
              expect_ok (Nats_eio.Service.stop service);
              expect_ok (Nats_eio.Service.stop service);
              equal int 1 !done_calls;
              equal int 0 !error_calls;
              equal bool true (Nats_eio.Service.stopped service);
              expect_connection_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "endpoint pending limits terminate slow consumers" (fun () ->
          let first, first_u = Eio.Promise.create () in
          let second, second_u = Eio.Promise.create () in
          let third, third_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          let failure_callback, failure_callback_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first;
                `Await second;
                `Await third;
                `Await hold;
              ]
            (fun ~sw ~clock ~trace connection ->
              let done_calls = ref 0 in
              let config =
                expect_config
                  (Nats_eio.Service.Config.v ~name:"orders" ~version:"1.2.3"
                     ~error_handler:(fun error ->
                       Eio.Promise.resolve failure_callback_u error)
                     ~done_handler:(fun () -> incr done_calls)
                     ())
              in
              let service =
                expect_service (Nats_eio.Service.v ~sw ~clock connection config)
              in
              let limits =
                expect_ok
                  (Nats_eio.Service.Endpoint.Pending_limits.v ~messages:1
                     ~bytes:(-1))
              in
              let first_started, first_started_u = Eio.Promise.create () in
              let second_handled, second_handled_u = Eio.Promise.create () in
              let release_first, release_first_u = Eio.Promise.create () in
              let calls = ref 0 in
              let endpoint =
                expect_endpoint
                  (Nats_eio.Service.Endpoint.v ~name:"limited"
                     ~pending_limits:limits (fun request ->
                       ignore
                         (Nats_eio.Service.Request.payload request);
                       incr calls;
                       if Int.equal !calls 1 then (
                         Eio.Promise.resolve first_started_u ();
                         Eio.Promise.await release_first;
                         Ok ())
                       else (
                         Eio.Promise.resolve second_handled_u ();
                         Ok ())))
              in
              expect_ok (Nats_eio.Service.add_endpoint service endpoint);
              Eio.Promise.resolve first_u
                (Ok
                   (request_wire ~sid:10 ~subject:"limited"
                      ~reply:"_INBOX.first" "one"));
              Eio.Promise.await first_started;
              Eio.Promise.resolve second_u
                (Ok
                   (request_wire ~sid:10 ~subject:"limited"
                      ~reply:"_INBOX.second" "two"));
              Eio.Promise.resolve third_u
                (Ok
                   (request_wire ~sid:10 ~subject:"limited"
                      ~reply:"_INBOX.third" "three"));
              yield_n 4;
              Eio.Promise.resolve release_first_u ();
              Eio.Promise.await second_handled;
              let failure = ref None in
              for attempt = 0 to 32 do
                match !failure with
                | Some _ -> ()
                | None -> (
                    match
                      Nats_eio.Service.add_group service
                        ~name:("probe" ^ string_of_int attempt)
                    with
                    | Ok _ -> Eio.Fiber.yield ()
                    | Error error -> failure := Some error)
              done;
              (match !failure with
              | Some
                  (Nats_eio.Service.Error.Connection
                     (Nats_eio.Error.Slow_consumer
                        (Nats_eio.Error.Subscription _))) ->
                  ()
              | Some error ->
                  fail
                    (Format.asprintf
                       "unexpected endpoint failure after pending overflow: %a\n%s"
                       Nats_eio.Service.Error.pp error (Buffer.contents trace))
              | None -> fail "endpoint pending overflow did not fail service");
              (match Eio.Promise.await failure_callback with
              | Nats_eio.Service.Error.Connection
                  (Nats_eio.Error.Slow_consumer
                     (Nats_eio.Error.Subscription _)) ->
                  ()
              | error ->
                  fail
                    (Format.asprintf
                       "error callback received an unexpected failure: %a"
                       Nats_eio.Service.Error.pp error));
              equal int 0 !done_calls;
              equal bool false (Nats_eio.Service.stopped service);
              expect_connection_ok (Nats_eio.Connection.close connection);
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
