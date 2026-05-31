let failf format = Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

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
      failf "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let subject prefix suffix = Nats.Subject.literal (prefix ^ "." ^ suffix)

let filter prefix suffix =
  Nats.Subject.Filter.literal (prefix ^ "." ^ suffix)

let headers entries =
  match Nats.Header.of_list entries with
  | Ok value -> value
  | Error error -> failf "invalid interop headers: %a" Nats.Header.pp_error error

let expect_header_values message expected =
  let actual = Nats.Header.find_all "x-trace" (Nats.Message.headers message) in
  if not (List.equal String.equal actual expected) then
    failf "header values were [%s], expected [%s]" (String.concat ", " actual)
      (String.concat ", " expected)

let expect_core_event events expected =
  match Nats_eio.Event_stream.next events with
  | Ok (Nats_eio.Event.Core event) -> (
      match (event, expected) with
      | Nats.Event.Info _, `Info -> ()
      | Nats.Event.Connected, `Connected -> ()
      | _ -> failf "unexpected core event: %a" Nats.Event.pp event)
  | Ok event -> failf "unexpected lifecycle event: %a" Nats_eio.Event.pp event
  | Error error -> failf "connection event stream: %s" (error_message error)

let next_with_timeout ~clock ~timeout subscription =
  Eio.Fiber.first
    (fun () -> Nats_eio.Subscription.next subscription)
    (fun () ->
      Eio.Time.Mono.sleep clock (Mtime.Span.to_float_ns timeout /. 1e9);
      Error Nats_eio.Error.Timeout)

let expect_message ~clock ~timeout label subscription =
  match next_with_timeout ~clock ~timeout subscription with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let reply_subject message =
  match Nats.Message.reply_to message with
  | Some subject -> subject
  | None -> failf "request message had no reply subject"

let start_responder ~sw ~clock ~timeout ~connection subscription =
  let result, result_u = Eio.Promise.create () in
  Eio.Fiber.fork ~sw (fun () ->
      let message =
        expect_message ~clock ~timeout "OCaml responder" subscription
      in
      if not (String.equal (Nats.Message.payload message) "request-from-go") then
        failf "Go request payload was %S" (Nats.Message.payload message);
      if
        not
          (String.equal
             (Option.value
                (Nats.Header.find "x-interop" (Nats.Message.headers message))
                ~default:"")
             "go-request")
      then failf "Go request X-Interop header was missing or incorrect";
      let response =
        Nats.Message.v ~subject:(reply_subject message)
          ~headers:(headers [ ("X-Interop", "ocaml-response") ])
          "response-from-ocaml"
      in
      Eio.Promise.resolve result_u
        (Nats_eio.Connection.publish_msg connection response));
  result

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let endpoint = endpoint () in
  let prefix = prefix () in
  let config =
    match auth () with
    | None -> None
    | Some auth ->
        Some
          (expect_ok "connection config"
             (Nats_eio.Connection.Config.v ~auth ()))
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
      let from_go =
        expect_ok "subscribe from-go"
          (Nats_eio.Connection.subscribe connection (filter prefix "from-go"))
      in
      let ocaml_request =
        expect_ok "subscribe ocaml-request"
          (Nats_eio.Connection.subscribe connection
             (filter prefix "ocaml-request"))
      in
      expect_ok "flush subscriptions" (Nats_eio.Connection.flush connection);
      let timeout = Mtime.Span.(10 * s) in
      let ocaml_responder =
        start_responder ~sw ~clock ~timeout ~connection ocaml_request
      in
      let start_response =
        expect_ok "start Go peer"
          (Nats_eio.Connection.request ~timeout connection
             (subject prefix "start") "start")
      in
      if not (String.equal (Nats.Message.payload start_response) "started") then
        failf "Go start response was %S" (Nats.Message.payload start_response);
      let from_go_message =
        expect_message ~clock ~timeout "Go publication" from_go
      in
      if not (String.equal (Nats.Message.payload from_go_message) "from-go") then
        failf "Go payload was %S" (Nats.Message.payload from_go_message);
      if
        not
          (String.equal
             (Option.value
                (Nats.Header.find "x-interop"
                   (Nats.Message.headers from_go_message))
                ~default:"")
             "go")
      then failf "Go X-Interop header was missing or incorrect";
      expect_header_values from_go_message [ "one"; "two" ];
      let to_go_headers =
        headers
          [ ("X-Interop", "ocaml"); ("X-Trace", "one");
            ("X-Trace", "two") ]
      in
      expect_ok "publish to Go"
        (Nats_eio.Connection.publish connection ~headers:to_go_headers
           (subject prefix "to-go") "to-go");
      expect_ok "flush to Go" (Nats_eio.Connection.flush connection);
      let go_response =
        expect_ok "Go request"
          (Nats_eio.Connection.request
             ~headers:(headers [ ("X-Interop", "ocaml-request") ]) ~timeout
             connection (subject prefix "go-request") "request-from-ocaml")
      in
      if not (String.equal (Nats.Message.payload go_response) "response-from-go")
      then failf "Go response was %S" (Nats.Message.payload go_response);
      if
        not
          (String.equal
             (Option.value
                (Nats.Header.find "x-interop"
                   (Nats.Message.headers go_response))
                ~default:"")
             "go-response")
      then failf "Go response X-Interop header was missing or incorrect";
      expect_ok "OCaml responder" (Eio.Promise.await ocaml_responder);
      (match
         Nats_eio.Connection.request ~timeout connection
           (subject prefix "no-responder") "missing"
       with
      | Error Nats_eio.Error.No_responders -> ()
      | Ok _ -> failf "no-responder request unexpectedly succeeded"
      | Error error -> failf "no-responder request: %s" (error_message error));
      expect_ok "drain" (Nats_eio.Connection.drain connection);
      expect_ok "close after drain" (Nats_eio.Connection.close connection);
      print_endline "interop: ok")

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("interop acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("interop acceptance failed: " ^ Printexc.to_string error);
      exit 1
