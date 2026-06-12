let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let core_error error = Format.asprintf "%a" Nats_eio.Error.pp error
let system_error error = Format.asprintf "%a" Nats_eio_system.Error.pp error

let expect_core label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (core_error error)

let expect_system label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (system_error error)

let endpoint value =
  match Nats.Endpoint.of_string value with
  | Ok value -> value
  | Error error ->
      failf "invalid NATS server endpoint %S: %a" value Nats.Endpoint.pp_error
        error

let endpoints () =
  let values =
    match Sys.getenv_opt "NATS_TEST_SERVERS" with
    | Some value when not (String.equal value "") ->
        String.split_on_char ',' value
        |> List.filter (fun value -> not (String.equal value ""))
    | _ -> (
        match Sys.getenv_opt "NATS_TEST_SERVER" with
        | Some value when not (String.equal value "") -> [ value ]
        | _ -> failf "NATS_TEST_SERVER or NATS_TEST_SERVERS is required")
  in
  match values with
  | [] -> failf "NATS_TEST_SERVERS must contain at least one endpoint"
  | values -> List.map endpoint values

let optional_endpoint name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Some (endpoint value)
  | _ -> None

let expected_server_count () =
  match Sys.getenv_opt "NATS_TEST_EXPECTED_SERVERS" with
  | None | Some "" -> 1
  | Some value -> (
      match int_of_string_opt value with
      | Some count when count > 0 -> count
      | _ -> failf "NATS_TEST_EXPECTED_SERVERS must be a positive integer")

let auth () =
  let user =
    Option.value ~default:"sys" (Sys.getenv_opt "NATS_TEST_SYSTEM_USER")
  in
  let pass =
    Option.value ~default:"sys" (Sys.getenv_opt "NATS_TEST_SYSTEM_PASS")
  in
  Nats.Auth.user_pass ~user ~pass

let touch path =
  let output = open_out path in
  close_out output

let next_core_event ~clock ~label events =
  match
    Eio.Fiber.first
      (fun () -> Nats_eio.Event_stream.next events)
      (fun () ->
        Eio.Time.Mono.sleep clock 5.;
        Error Nats_eio.Error.Timeout)
  with
  | Ok event -> event
  | Error error -> failf "%s: %s" label (core_error error)

let wait_for_core_event ~clock ~label predicate events =
  let remaining = ref 16 in
  let found = ref false in
  while !remaining > 0 && not !found do
    decr remaining;
    if predicate (next_core_event ~clock ~label events) then found := true
  done;
  if not !found then failf "timed out waiting for %s" label

let server_id ~events =
  let remaining = ref 16 in
  let result = ref None in
  while !remaining > 0 && Option.is_none !result do
    decr remaining;
    match Nats_eio.Event_stream.next events with
    | Ok (Nats_eio.Event.Core (Nats.Event.Info info)) ->
        result := Nats.Info.server_id info
    | Ok _ -> ()
    | Error error ->
        failf "reading initial server events: %s" (core_error error)
  done;
  match !result with
  | Some value -> value
  | None -> failf "the server did not advertise a server id"

let assert_success label responses =
  match responses with
  | [ response ] -> (
      match Nats_eio_system.Monitor.error response with
      | None -> response
      | Some error ->
          failf "%s returned a server error: %s" label
            (system_error (Nats_eio_system.Error.Server error)))
  | [] -> failf "%s returned no responses" label
  | _ -> failf "%s returned more than one targeted response" label

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoints = endpoints () in
  let expected_servers = expected_server_count () in
  let auth = auth () in
  let config =
    expect_core "auth config" (Nats_eio.Connection.Config.v ~auth ())
  in
  let connection =
    expect_core "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config endpoints)
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let events = Nats_eio.Connection.events connection in
      let server_id = server_id ~events in
      let system = Nats_eio_system.v connection in
      let target =
        expect_system "server target" (Nats_eio_system.Target.server server_id)
      in
      let varz =
        expect_system "server VARZ"
          (Nats_eio_system.Monitor.request
             ~timeout:Mtime.Span.(5 * s)
             system ~target Nats_eio_system.Monitor.Endpoint.Varz)
        |> assert_success "server VARZ"
      in
      (match Nats_eio_system.Monitor.data varz with
      | Some _ -> ()
      | None -> failf "server VARZ response had no data");
      let all_stats =
        expect_system "all-server STATSZ"
          (Nats_eio_system.Monitor.request
             ~timeout:Mtime.Span.(1 * s)
             system ~target:Nats_eio_system.Target.all
             Nats_eio_system.Monitor.Endpoint.Statz)
      in
      let stats_count = List.length all_stats in
      if Int.equal stats_count 0 then
        failf "all-server STATSZ returned no responses"
      else if not (Int.equal stats_count expected_servers) then
        failf "all-server STATSZ returned %d responses, expected %d" stats_count
          expected_servers;
      let account =
        expect_system "system-account INFO"
          (Nats_eio_system.Target.account "SYS")
      in
      let account_info =
        expect_system "account INFO"
          (Nats_eio_system.Monitor.request
             ~timeout:Mtime.Span.(5 * s)
             system ~target:account
             Nats_eio_system.Monitor.Endpoint.Account_info)
      in
      if Int.equal (List.length account_info) 0 then
        failf "account INFO returned no responses";
      expect_system "reload"
        (Nats_eio_system.Control.reload
           ~timeout:Mtime.Span.(5 * s)
           system ~server:server_id);
      let account_events =
        expect_system "account event subscription"
          (Nats_eio_system.Events.subscribe
             ~scope:(Nats_eio_system.Events.Account "SYS") system)
      in
      expect_core "event subscription flush"
        (Nats_eio.Connection.flush connection);
      let secondary_endpoint =
        match optional_endpoint "NATS_TEST_SYSTEM_SECONDARY_SERVER" with
        | Some value -> value
        | None -> (
            match endpoints with
            | _ :: value :: _ -> value
            | value :: _ -> value
            | [] -> failf "no system connection endpoint is configured")
      in
      let secondary =
        expect_core "secondary system connection"
          (Nats_eio.Connection.connect ~sw ~net ~clock ~config
             [ secondary_endpoint ])
      in
      let connected =
        let remaining = ref 16 in
        let result = ref false in
        while !remaining > 0 && not !result do
          decr remaining;
          match
            Nats_eio_system.Events.next_with_timeout
              ~timeout:Mtime.Span.(1 * s)
              account_events
          with
          | Ok (Nats_eio_system.Events.Account_connect { account_id; _ })
            when String.equal account_id "SYS" ->
              result := true
          | Ok _ -> ()
          | Error (Nats_eio_system.Error.Connection Nats_eio.Error.Timeout) ->
              ()
          | Error error -> failf "system account event: %s" (system_error error)
        done;
        !result
      in
      expect_core "secondary system connection close"
        (Nats_eio.Connection.close secondary);
      if not connected then
        failf "system account CONNECT event was not observed";
      (match Sys.getenv_opt "NATS_TEST_SYSTEM_SIGNAL" with
      | None | Some "" -> ()
      | Some signal ->
          touch signal;
          wait_for_core_event ~clock ~label:"disconnect"
            (function Nats_eio.Event.Disconnected -> true | _ -> false)
            events;
          wait_for_core_event ~clock ~label:"reconnect"
            (function Nats_eio.Event.Reconnected -> true | _ -> false)
            events;
          let recovered_stats =
            expect_system "recovered all-server STATSZ"
              (Nats_eio_system.Monitor.request
                 ~timeout:Mtime.Span.(5 * s)
                 system ~target:Nats_eio_system.Target.all
                 Nats_eio_system.Monitor.Endpoint.Statz)
          in
          let recovered_count = List.length recovered_stats in
          if expected_servers <= 1 then
            failf "system reconnect requires more than one expected server"
          else if not (Int.equal recovered_count (expected_servers - 1)) then
            failf
              "recovered all-server STATSZ returned %d responses, expected %d"
              recovered_count (expected_servers - 1);
          let recovered_endpoint =
            match optional_endpoint "NATS_TEST_SYSTEM_RECOVERED_SERVER" with
            | Some value -> value
            | None -> secondary_endpoint
          in
          let recovered =
            expect_core "recovered system connection"
              (Nats_eio.Connection.connect ~sw ~net ~clock ~config
                 [ recovered_endpoint ])
          in
          let recovered_connected =
            let remaining = ref 16 in
            let result = ref false in
            while !remaining > 0 && not !result do
              decr remaining;
              match
                Nats_eio_system.Events.next_with_timeout
                  ~timeout:Mtime.Span.(1 * s)
                  account_events
              with
              | Ok (Nats_eio_system.Events.Account_connect { account_id; _ })
                when String.equal account_id "SYS" ->
                  result := true
              | Ok _ -> ()
              | Error (Nats_eio_system.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Error error ->
                  failf "recovered system account event: %s"
                    (system_error error)
            done;
            !result
          in
          expect_core "recovered system connection close"
            (Nats_eio.Connection.close recovered);
          if not recovered_connected then
            failf "recovered system account CONNECT event was not observed");
      expect_system "event close" (Nats_eio_system.Events.close account_events);
      print_endline "system administration: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server system administration failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("server system administration failed: " ^ Printexc.to_string error);
      exit 1
