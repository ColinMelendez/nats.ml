open Windtrap

let info_wire =
  "INFO {\"server_id\":\"srv\",\"version\":\"2.14.0\","
  ^ "\"proto\":1,\"max_payload\":1048576,\"headers\":true,"
  ^ "\"no_responders\":true,\"connect_urls\":[]}" ^ "\r\n"

let delivery_wire ~sid ~subject payload =
  Format.asprintf "MSG %s %d %d\r\n%s\r\n" subject sid (String.length payload)
    payload

let expect_core_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let expect_system_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio_system.Error.pp error)

let address = `Tcp (Eio.Net.Ipaddr.V4.loopback, 4222)

let endpoint =
  match Nats.Endpoint.of_string "nats://127.0.0.1:4222" with
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats.Endpoint.pp_error error)

let configure_net net =
  Eio_mock.Net.on_getaddrinfo net (List.init 32 (fun _ -> `Return [ address ]))

let make_net label =
  let net = Eio_mock.Net.make label in
  configure_net net;
  net

let with_connection ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "system-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = make_net "system-network" in
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_core_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw connection

let with_traced_connection ?config ?(configure_flow = fun _ -> ()) ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "traced-system-server" in
  configure_flow flow;
  Eio_mock.Flow.on_read flow reads;
  let net = make_net "traced-system-network" in
  Eio_mock.Net.on_connect net [ `Return flow ];
  let trace = Buffer.create 4096 in
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
    expect_core_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw connection ~trace

let contains ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let position = ref 0 in
  let found = ref false in
  while (not !found) && !position <= limit do
    if String.equal (String.sub value !position needle_length) needle then
      found := true;
    incr position
  done;
  !found

let has_json_field name = function
  | Jsont.Object (members, _) ->
      Option.is_some (Jsont.Json.find_mem name members)
  | _ -> false

let yield_n count =
  let remaining = ref count in
  while !remaining > 0 do
    decr remaining;
    Eio.Fiber.yield ()
  done

let config () =
  expect_core_ok (Nats_eio.Connection.Config.v ~inbox_prefix:"_INBOX.system" ())

let () =
  run "nats-eio-system"
    [
      test "validates targets, selectors, and client ids" (fun () ->
          (match Nats_eio_system.Target.server "srv.node" with
          | Error (Nats_eio_system.Error.Invalid_identifier _) -> ()
          | Ok _ -> fail "accepted a dotted server identifier"
          | Error error ->
              fail (Format.asprintf "%a" Nats_eio_system.Error.pp error));
          (match
             Nats_eio_system.Selector.v ~server_name:"node" ~cluster:"c1"
               ~host:"127.0.0.1" ~exact_match:true ~tags:[ "edge" ] ~domain:"js"
               ()
           with
          | Ok selector ->
              equal (option string) (Some "node")
                (Nats_eio_system.Selector.server_name selector);
              equal bool true (Nats_eio_system.Selector.exact_match selector);
              equal (list string) [ "edge" ]
                (Nats_eio_system.Selector.tags selector)
          | Error error ->
              fail (Format.asprintf "%a" Nats_eio_system.Error.pp error));
          (match Nats_eio_system.Selector.v ~tags:[ "" ] () with
          | Error (Nats_eio_system.Error.Invalid_selector_tag "") -> ()
          | Ok _ -> fail "accepted an empty selector tag"
          | Error error ->
              fail (Format.asprintf "%a" Nats_eio_system.Error.pp error));
          (match Nats_eio_system.Monitor.Options.connz ~offset:(-1) () with
          | Error (Nats_eio_system.Error.Invalid_option { name = "offset"; _ })
            ->
              ()
          | Ok _ -> fail "accepted a negative monitor offset"
          | Error error ->
              fail (Format.asprintf "%a" Nats_eio_system.Error.pp error));
          match Nats_eio_system.Target.account "account.name" with
          | Error (Nats_eio_system.Error.Invalid_identifier _) -> ()
          | Ok _ -> fail "accepted a dotted account identifier"
          | Error error ->
              fail (Format.asprintf "%a" Nats_eio_system.Error.pp error));
      test "decodes version-tolerant monitoring responses" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_traced_connection ~config:(config ())
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ~trace ->
              let system = Nats_eio_system.v connection in
              let selector =
                expect_system_ok
                  (Nats_eio_system.Selector.v ~server_name:"node"
                     ~exact_match:true ())
              in
              let options =
                expect_system_ok
                  (Nats_eio_system.Monitor.Options.connz ~auth:true
                     ~subscriptions:true ~offset:2 ~limit:10 ~cid:42L
                     ~state:Nats_eio_system.Monitor.Options.All ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio_system.Monitor.request
                       ~timeout:Mtime.Span.(1 * s)
                       ~selector ~options system
                       ~target:
                         (expect_system_ok
                            (Nats_eio_system.Target.server "srv"))
                       Nats_eio_system.Monitor.Endpoint.Connz));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (delivery_wire ~sid:1 ~subject:"_INBOX.system.0.0"
                      {|{"server":{"id":"srv"},"data":{"version":"2.14.0"},"new_field":true}|}));
              let responses = expect_system_ok (Eio.Promise.await result) in
              equal int 1 (List.length responses);
              (match responses with
              | [ response ] ->
                  (match Nats_eio_system.Monitor.endpoint response with
                  | Nats_eio_system.Monitor.Endpoint.Connz -> ()
                  | _ -> fail "monitor response lost its endpoint");
                  (match Nats_eio_system.Monitor.error response with
                  | None -> ()
                  | Some _ -> fail "successful monitor response has an error");
                  (match Nats_eio_system.Monitor.data response with
                  | Some _ -> ()
                  | None -> fail "monitor response lost its data");
                  if
                    not
                      (has_json_field "new_field"
                         (Nats_eio_system.Monitor.payload response))
                  then fail "monitor response lost its unknown field"
              | _ -> fail "monitor request returned the wrong response count");
              if
                not
                  (contains ~needle:"$SYS.REQ.SERVER.srv.CONNZ"
                     (Buffer.contents trace))
              then fail "monitor request used the wrong system subject";
              if
                not
                  (contains ~needle:"\\\"auth\\\":true" (Buffer.contents trace))
              then fail "monitor request omitted auth options";
              if
                not
                  (contains ~needle:"\\\"offset\\\":2" (Buffer.contents trace))
              then fail "monitor request omitted pagination options";
              if not (contains ~needle:"\\\"cid\\\":42" (Buffer.contents trace))
              then fail "monitor request omitted the connection filter";
              expect_core_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "collects fanout monitoring replies until the deadline" (fun () ->
          let replies, replies_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_traced_connection ~config:(config ())
            ~reads:[ `Return info_wire; `Await replies; `Await hold ]
            (fun ~sw connection ~trace ->
              let system = Nats_eio_system.v connection in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio_system.Monitor.request
                       ~timeout:Mtime.Span.(1 * ms)
                       system ~target:Nats_eio_system.Target.all
                       Nats_eio_system.Monitor.Endpoint.Statz));
              yield_n 5;
              let first =
                delivery_wire ~sid:1 ~subject:"_INBOX.system.0.0"
                  {|{"server":{"id":"a"},"statsz":{}}|}
              in
              let second =
                delivery_wire ~sid:1 ~subject:"_INBOX.system.0.1"
                  {|{"server":{"id":"b"},"statsz":{}}|}
              in
              Eio.Promise.resolve replies_u (Ok (first ^ second));
              let responses = expect_system_ok (Eio.Promise.await result) in
              equal int 2 (List.length responses);
              if
                not
                  (contains ~needle:"$SYS.REQ.SERVER.PING.STATSZ"
                     (Buffer.contents trace))
              then fail "fanout monitor used the wrong system subject";
              if not (contains ~needle:"UNSUB 1" (Buffer.contents trace)) then
                fail "fanout monitor did not clean up its inbox subscription";
              expect_core_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "cancelling subscription setup cleans up the wire subscription"
        (fun () ->
          let cancel_ref = ref None in
          let result, result_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_traced_connection
            ~configure_flow:(fun flow ->
              Eio_mock.Flow.on_copy_bytes flow
                [
                  `Return 4096;
                  `Run
                    (fun () ->
                      Option.iter
                        (fun cancel ->
                          Eio.Cancel.cancel cancel
                            (Failure "cancel subscription setup"))
                        !cancel_ref;
                      4096);
                  `Run
                    (fun () ->
                      Option.iter
                        (fun cancel ->
                          Eio.Cancel.cancel cancel
                            (Failure "cancel subscription setup"))
                        !cancel_ref;
                      4096);
                  `Run
                    (fun () ->
                      Option.iter
                        (fun cancel ->
                          Eio.Cancel.cancel cancel
                            (Failure "cancel subscription setup"))
                        !cancel_ref;
                      4096);
                ])
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw connection ~trace ->
              let filter = Nats.Subject.Filter.literal "system.events" in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Cancel.sub (fun cancel ->
                      cancel_ref := Some cancel;
                      try
                        ignore (Nats_eio.Connection.subscribe connection filter);
                        Eio.Promise.resolve result_u `Completed
                      with Eio.Cancel.Cancelled _ ->
                        Eio.Promise.resolve result_u `Cancelled));
              (match Eio.Promise.await result with
              | `Cancelled -> ()
              | `Completed -> fail "subscription setup unexpectedly completed");
              if
                not
                  (contains ~needle:"SUB system.events" (Buffer.contents trace))
              then fail "cancelled subscription did not reach the wire";
              if not (contains ~needle:"UNSUB 1" (Buffer.contents trace)) then
                fail "cancelled subscription was not cleaned up";
              expect_core_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "sends reload, kick, and client LDM controls" (fun () ->
          let reload, reload_u = Eio.Promise.create () in
          let kick, kick_u = Eio.Promise.create () in
          let ldm, ldm_u = Eio.Promise.create () in
          let api_error, api_error_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_traced_connection ~config:(config ())
            ~reads:
              [
                `Return info_wire;
                `Await reload;
                `Await kick;
                `Await ldm;
                `Await api_error;
                `Await hold;
              ]
            (fun ~sw connection ~trace ->
              let system = Nats_eio_system.v connection in
              (match
                 Nats_eio_system.Control.kick system ~server:"srv"
                   ~client_id:(-1L)
               with
              | Error (Nats_eio_system.Error.Invalid_client_id -1L) -> ()
              | _ -> fail "negative client id was not rejected");
              let call f gate response =
                let result, result_u = Eio.Promise.create () in
                Eio.Fiber.fork ~sw (fun () ->
                    Eio.Promise.resolve result_u (f ()));
                yield_n 5;
                Eio.Promise.resolve gate
                  (Ok
                     (delivery_wire ~sid:response ~subject:"_INBOX.system"
                        {|{"server":{"id":"srv"}}|}));
                expect_system_ok (Eio.Promise.await result)
              in
              call
                (fun () -> Nats_eio_system.Control.reload system ~server:"srv")
                reload_u 1;
              call
                (fun () ->
                  Nats_eio_system.Control.kick system ~server:"srv"
                    ~client_id:42L)
                kick_u 2;
              call
                (fun () ->
                  Nats_eio_system.Control.client_lame_duck system ~server:"srv"
                    ~client_id:43L)
                ldm_u 3;
              if
                not
                  (contains ~needle:"$SYS.REQ.SERVER.srv.RELOAD"
                     (Buffer.contents trace))
              then fail "reload used the wrong system subject";
              if
                not
                  (contains ~needle:"$SYS.REQ.SERVER.srv.KICK"
                     (Buffer.contents trace))
              then fail "kick used the wrong system subject";
              if not (contains ~needle:"\\\"cid\\\":42" (Buffer.contents trace))
              then fail "kick omitted the client id";
              if not (contains ~needle:"\\\"cid\\\":43" (Buffer.contents trace))
              then fail "client LDM omitted the client id";
              if
                not
                  (contains ~needle:"$SYS.REQ.SERVER.srv.LDM"
                     (Buffer.contents trace))
              then fail "client LDM used the wrong system subject";
              let error_result, error_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve error_result_u
                    (Nats_eio_system.Control.reload system ~server:"srv"));
              yield_n 5;
              Eio.Promise.resolve api_error_u
                (Ok
                   (delivery_wire ~sid:4 ~subject:"_INBOX.system"
                      {|{"error":{"code":400,"err_code":10001,"description":"reload denied"}}|}));
              (match Eio.Promise.await error_result with
              | Error (Nats_eio_system.Error.Server { code = 400; _ }) -> ()
              | Ok () -> fail "server API error was reported as success"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected control error: %a"
                       Nats_eio_system.Error.pp error));
              expect_core_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "classifies system event subjects and preserves JSON" (fun () ->
          let events, events_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await events; `Await hold ]
            (fun ~sw:_ connection ->
              let system = Nats_eio_system.v connection in
              let subscription =
                expect_system_ok
                  (Nats_eio_system.Events.subscribe
                     ~scope:Nats_eio_system.Events.All system)
              in
              Eio.Promise.resolve events_u
                (Ok
                   (delivery_wire ~sid:1 ~subject:"$SYS.SERVER.srv.STATSZ"
                      {|{"seq":1}|}
                   ^ delivery_wire ~sid:2 ~subject:"$SYS.ACCOUNT.SYS.NEW_EVENT"
                       {|{"future":true}|}));
              let saw_stats = ref false in
              let saw_unknown = ref false in
              let observe = function
                | Nats_eio_system.Events.Server_stats { server_id; _ } ->
                    equal string "srv" server_id;
                    saw_stats := true
                | Nats_eio_system.Events.Unknown { subject; payload } ->
                    if not (has_json_field "future" payload) then
                      fail "unknown event payload was not preserved";
                    equal string "$SYS.ACCOUNT.SYS.NEW_EVENT" subject;
                    saw_unknown := true
                | _ -> fail "system event was not classified"
              in
              observe
                (expect_system_ok (Nats_eio_system.Events.next subscription));
              observe
                (expect_system_ok (Nats_eio_system.Events.next subscription));
              if not !saw_stats then fail "server STATSZ was not classified";
              if not !saw_unknown then
                fail "unknown system event was not preserved";
              expect_system_ok (Nats_eio_system.Events.close subscription);
              expect_core_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
