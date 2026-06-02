let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let required name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> value
  | Some _ | None -> failf "%s is required" name

let endpoint () =
  let value = required "NATS_TEST_SERVER" in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

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

let transient_jetstream_api { Nats_eio.Jetstream.Error.code; _ } =
  Int.equal code 408 || Int.equal code 500 || Int.equal code 502
  || Int.equal code 503 || Int.equal code 504

let transient_jetstream_error = function
  | Nats_eio.Jetstream.Error.Api api -> transient_jetstream_api api
  | Nats_eio.Jetstream.Error.Connection
      ( Nats_eio.Error.Timeout | Nats_eio.Error.No_responders
      | Nats_eio.Error.Disconnected ) ->
      true
  | _ -> false

let retry_jetstream ~clock ~connection ~deadline ~failure_file ~label operation
    =
  let last_error = ref None in
  let result = ref None in
  while Option.is_none !result do
    if Sys.file_exists failure_file then
      failf "restart watcher failed (see %s)" failure_file
    else
      match operation () with
      | Ok value -> result := Some (Ok value)
      | Error error when transient_jetstream_error error ->
          last_error := Some error;
          if Mtime.compare (Nats_eio.Connection.now connection) deadline >= 0
          then result := Some (Error error)
          else Eio.Time.Mono.sleep clock 0.2
      | Error error -> result := Some (Error error)
  done;
  match !result with
  | Some (Ok value) -> Ok value
  | Some (Error error) -> Error error
  | None -> (
      match !last_error with
      | Some error -> Error error
      | None -> failf "%s returned no result" label)

let next_event ~clock ~timeout ~failure_file events =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  match
    Eio.Fiber.first
      (fun () ->
        Eio.Fiber.first
          (fun () -> Nats_eio.Event_stream.next events)
          (fun () ->
            Eio.Time.Mono.sleep clock seconds;
            Error Nats_eio.Error.Timeout))
      (fun () ->
        while not (Sys.file_exists failure_file) do
          Eio.Time.Mono.sleep clock 0.05
        done;
        failf "restart watcher failed (see %s)" failure_file)
  with
  | Ok event -> event
  | Error error -> failf "lifecycle event: %s" (error_message error)

let rec wait_for_event ~clock ~timeout ~failure_file ~label ~remaining predicate
    events =
  if Int.equal remaining 0 then failf "timed out waiting for %s" label
  else
    let event = next_event ~clock ~timeout ~failure_file events in
    if predicate event then ()
    else
      wait_for_event ~clock ~timeout ~failure_file ~label
        ~remaining:(remaining - 1) predicate events

let expect_disconnected ~clock ~timeout ~failure_file events =
  wait_for_event ~clock ~timeout ~failure_file ~label:"disconnect" ~remaining:24
    (function Nats_eio.Event.Disconnected -> true | _ -> false)
    events

let expect_reconnected ~clock ~timeout ~failure_file events =
  wait_for_event ~clock ~timeout ~failure_file ~label:"reconnect" ~remaining:24
    (function Nats_eio.Event.Reconnected -> true | _ -> false)
    events

let next_message ~clock ~failure_file ~timeout label subscription =
  match
    Eio.Fiber.first
      (fun () -> Nats_eio.Subscription.next_with_timeout ~timeout subscription)
      (fun () ->
        while not (Sys.file_exists failure_file) do
          Eio.Time.Mono.sleep clock 0.05
        done;
        failf "restart watcher failed (see %s)" failure_file)
  with
  | Ok delivery -> delivery.Nats_eio.Subscription.message
  | Error error -> failf "%s: %s" label (error_message error)

let next_push_message ~clock ~failure_file ~timeout label push =
  match
    Eio.Fiber.first
      (fun () ->
        Nats_eio.Jetstream.Consumer.Push.next_with_timeout ~timeout push)
      (fun () ->
        while not (Sys.file_exists failure_file) do
          Eio.Time.Mono.sleep clock 0.05
        done;
        failf "restart watcher failed (see %s)" failure_file)
  with
  | Ok delivery -> delivery
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let expect_payload label expected message =
  let actual = Nats.Message.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let expect_header label expected headers name =
  match Nats.Header.find name headers with
  | Some actual when String.equal actual expected -> ()
  | Some actual -> failf "%s header was %S, expected %S" label actual expected
  | None -> failf "%s header was missing" label

let expect_publish_ack label ~stream ~sequence ack =
  let actual_stream = Nats_eio.Jetstream.Publish_ack.stream ack in
  if not (String.equal actual_stream stream) then
    failf "%s named stream %S, expected %S" label actual_stream stream;
  let actual_sequence = Nats_eio.Jetstream.Publish_ack.sequence ack in
  if not (Int64.equal actual_sequence sequence) then
    failf "%s sequence=%Ld, expected %Ld" label actual_sequence sequence

let expect_push_consumer_config label info ~name ~delivery ~filter =
  let actual_name = Nats_eio.Jetstream.Consumer.Info.name info in
  if not (String.equal actual_name name) then
    failf "%s named consumer %S, expected %S" label actual_name name;
  let config = Nats_eio.Jetstream.Consumer.Info.config info in
  (match Nats_eio.Jetstream.Consumer.Config.deliver_subject config with
  | Some subject when String.equal (Nats.Subject.to_string subject) delivery ->
      ()
  | Some subject ->
      failf "%s delivered to %S, expected %S" label
        (Nats.Subject.to_string subject)
        delivery
  | None -> failf "%s had no delivery subject" label);
  (match Nats_eio.Jetstream.Consumer.Config.filter_subject config with
  | Some subject
    when String.equal (Nats.Subject.Filter.to_string subject) filter ->
      ()
  | Some subject ->
      failf "%s filtered %S, expected %S" label
        (Nats.Subject.Filter.to_string subject)
        filter
  | None -> failf "%s had no filter subject" label);
  match Nats_eio.Jetstream.Consumer.Config.ack_policy config with
  | Nats_eio.Jetstream.Consumer.Config.Explicit -> ()
  | _ -> failf "%s did not use explicit acknowledgements" label

let expect_consumer_state label info ~stream_sequence ~consumer_sequence =
  (match Nats_eio.Jetstream.Consumer.Info.ack_floor_stream_sequence info with
  | Some actual when Int64.equal actual stream_sequence -> ()
  | Some actual ->
      failf "%s stream ack floor=%Ld, expected %Ld" label actual stream_sequence
  | None -> failf "%s had no stream ack floor" label);
  (match Nats_eio.Jetstream.Consumer.Info.ack_floor_consumer_sequence info with
  | Some actual when Int64.equal actual consumer_sequence -> ()
  | Some actual ->
      failf "%s consumer ack floor=%Ld, expected %Ld" label actual
        consumer_sequence
  | None -> failf "%s had no consumer ack floor" label);
  if not (Int.equal (Nats_eio.Jetstream.Consumer.Info.num_ack_pending info) 0)
  then
    failf "%s retained %d pending acknowledgements" label
      (Nats_eio.Jetstream.Consumer.Info.num_ack_pending info)

let expect_stream_state label info ~messages ~last_sequence =
  let config = Nats_eio.Jetstream.Stream.Info.config info in
  (match Nats_eio.Jetstream.Stream.Config.storage config with
  | Nats_eio.Jetstream.Stream.Config.File -> ()
  | Nats_eio.Jetstream.Stream.Config.Memory ->
      failf "%s was not file-backed" label);
  if not (Int64.equal (Nats_eio.Jetstream.Stream.Info.messages info) messages)
  then
    failf "%s messages=%Ld, expected %Ld" label
      (Nats_eio.Jetstream.Stream.Info.messages info)
      messages;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Stream.Info.last_sequence info)
         last_sequence)
  then
    failf "%s last sequence=%Ld, expected %Ld" label
      (Nats_eio.Jetstream.Stream.Info.last_sequence info)
      last_sequence

let expect_push_delivery label ~stream ~consumer ~payload ~interop ~trace
    ~stream_sequence ~consumer_sequence message =
  expect_payload label payload (Nats_eio.Jetstream.Msg.message message);
  expect_header (label ^ " X-Interop") interop
    (Nats_eio.Jetstream.Msg.headers message)
    "X-Interop";
  expect_header (label ^ " X-Trace") trace
    (Nats_eio.Jetstream.Msg.headers message)
    "X-Trace";
  if not (String.equal (Nats_eio.Jetstream.Msg.stream message) stream) then
    failf "%s named the wrong stream" label;
  if not (String.equal (Nats_eio.Jetstream.Msg.consumer message) consumer) then
    failf "%s named the wrong consumer" label;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Msg.stream_sequence message)
         stream_sequence)
  then failf "%s had the wrong stream sequence" label;
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Msg.consumer_sequence message)
         consumer_sequence)
  then failf "%s had the wrong consumer sequence" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.num_delivered message) 1L) then
    failf "%s had the wrong delivery count" label;
  if not (Int64.equal (Nats_eio.Jetstream.Msg.num_pending message) 0L) then
    failf "%s had %Ld pending messages" label
      (Nats_eio.Jetstream.Msg.num_pending message)

let await_go_ready ~clock ~timeout ~failure_file connection prefix =
  let subject = Nats.Subject.literal (prefix ^ ".reconnect-ready.1") in
  let request_payload = "ocaml-ready-1" in
  let response_payload = "go-ready-1" in
  let deadline =
    match Mtime.add_span (Nats_eio.Connection.now connection) timeout with
    | Some deadline -> deadline
    | None -> Mtime.max_stamp
  in
  let attempt_timeout = Mtime.Span.(500 * ms) in
  let ready = ref false in
  while not !ready do
    if Sys.file_exists failure_file then
      failf "restart watcher failed (see %s)" failure_file
    else
      let now = Nats_eio.Connection.now connection in
      if Mtime.compare now deadline >= 0 then
        failf "timed out waiting for Go JetStream reconnect barrier"
      else
        let remaining = Mtime.span now deadline in
        let request_timeout =
          if Mtime.Span.compare remaining attempt_timeout < 0 then remaining
          else attempt_timeout
        in
        match
          Nats_eio.Connection.request ~timeout:request_timeout connection
            subject request_payload
        with
        | Ok response ->
            expect_payload "Go JetStream reconnect barrier" response_payload
              response;
            ready := true
        | Error Nats_eio.Error.Timeout
        | Error Nats_eio.Error.No_responders
        | Error Nats_eio.Error.Disconnected ->
            Eio.Time.Mono.sleep clock 0.01
        | Error error ->
            failf "Go JetStream reconnect barrier: %s" (error_message error)
  done

let touch path =
  let output = open_out path in
  close_out output

let run env =
  Mirage_crypto_rng_unix.use_default ();
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(30 * s) in
  let reconnect_timeout = Mtime.Span.(90 * s) in
  let prefix = required "NATS_TEST_INTEROP_PREFIX" in
  let stream_name = required "NATS_TEST_INTEROP_STREAM" in
  let signal = required "NATS_TEST_INTEROP_SIGNAL" in
  let auth = auth () in
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ?auth ~max_reconnect_attempts:None
         ~reconnect_delay:Mtime.Span.(500 * ms)
         ~reconnect_max_delay:Mtime.Span.(2 * s)
         ())
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config [ endpoint () ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let failure_file = signal ^ ".failed" in
      let jetstream =
        expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v connection)
      in
      let stream =
        expect_jetstream_ok "bind stream"
          (Nats_eio.Jetstream.Stream.bind jetstream ~name:stream_name)
      in
      let stream_info =
        expect_jetstream_ok "initial stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      expect_stream_state "initial stream" stream_info ~messages:0L
        ~last_sequence:0L;
      let ocaml_consumer =
        expect_jetstream_ok "bind OCaml push consumer"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:"OCAML_PUSH")
      in
      let go_consumer =
        expect_jetstream_ok "bind Go push consumer"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:"GO_PUSH")
      in
      let ocaml_info =
        expect_jetstream_ok "initial OCaml push consumer info"
          (Nats_eio.Jetstream.Consumer.info ocaml_consumer)
      in
      expect_push_consumer_config "initial OCaml push consumer" ocaml_info
        ~name:"OCAML_PUSH"
        ~delivery:(prefix ^ ".deliver.ocaml")
        ~filter:(prefix ^ ".go");
      let go_info =
        expect_jetstream_ok "initial Go push consumer info"
          (Nats_eio.Jetstream.Consumer.info go_consumer)
      in
      expect_push_consumer_config "initial Go push consumer" go_info
        ~name:"GO_PUSH" ~delivery:(prefix ^ ".deliver.go")
        ~filter:(prefix ^ ".ocaml");
      let push =
        expect_jetstream_ok "open OCaml push session"
          (Nats_eio.Jetstream.Consumer.Push.v ~sw ocaml_consumer)
      in
      Fun.protect
        ~finally:(fun () ->
          ignore (Nats_eio.Jetstream.Consumer.Push.close push))
        (fun () ->
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Consumer.Push.initial_pending push)
                 0L)
          then failf "OCaml push session started with pending messages";
          let before_done =
            expect_ok "subscribe baseline completion"
              (Nats_eio.Connection.subscribe connection
                 (Nats.Subject.Filter.literal (prefix ^ ".before-done")))
          in
          let go_done =
            expect_ok "subscribe reconnect completion"
              (Nats_eio.Connection.subscribe connection
                 (Nats.Subject.Filter.literal (prefix ^ ".go-done")))
          in
          expect_ok "push setup flush" (Nats_eio.Connection.flush connection);
          let start_response =
            expect_ok "start Go push reconnect peer"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".start"))
                 "start")
          in
          expect_payload "start response" "started" start_response;
          let before_go =
            next_push_message ~clock ~failure_file ~timeout
              "receive baseline Go message" push
          in
          expect_push_delivery "baseline Go message" ~stream:stream_name
            ~consumer:"OCAML_PUSH" ~payload:"before-go"
            ~interop:"go-jetstream-push-reconnect" ~trace:"go-before"
            ~stream_sequence:1L ~consumer_sequence:1L before_go;
          expect_jetstream_ok "acknowledge baseline Go message"
            (Nats_eio.Jetstream.Msg.ack_sync ~timeout before_go);
          let baseline_ack =
            expect_ok "acknowledge baseline Go message to peer"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".go-acked"))
                 "before-go-acked")
          in
          expect_payload "baseline Go acknowledgement" "acknowledged"
            baseline_ack;
          let ocaml_subject = Nats.Subject.literal (prefix ^ ".ocaml") in
          let headers =
            match
              Nats.Header.of_list
                [
                  ("X-Interop", "ocaml-jetstream-push-reconnect");
                  ("X-Trace", "ocaml-before");
                ]
            with
            | Ok headers -> headers
            | Error error ->
                failf "push reconnect headers: %a" Nats.Header.pp_error error
          in
          let before_ocaml =
            expect_jetstream_ok "publish baseline OCaml message"
              (Nats_eio.Jetstream.publish ~timeout ~headers jetstream
                 ocaml_subject "before-ocaml")
          in
          expect_publish_ack "baseline OCaml publish" ~stream:stream_name
            ~sequence:2L before_ocaml;
          let completion =
            next_message ~clock ~failure_file ~timeout "baseline completion"
              before_done
          in
          expect_payload "baseline completion" "baseline-complete" completion;
          touch (signal ^ ".1");
          let events = Nats_eio.Connection.events connection in
          expect_disconnected ~clock ~timeout:reconnect_timeout ~failure_file
            events;
          expect_reconnected ~clock ~timeout:reconnect_timeout ~failure_file
            events;
          await_go_ready ~clock ~timeout:reconnect_timeout ~failure_file
            connection prefix;
          let recovery_deadline =
            match
              Mtime.add_span
                (Nats_eio.Connection.now connection)
                reconnect_timeout
            with
            | Some value -> value
            | None -> Mtime.max_stamp
          in
          let stream_info =
            expect_jetstream_ok "stream after reconnect"
              (retry_jetstream ~clock ~connection ~deadline:recovery_deadline
                 ~failure_file ~label:"stream after reconnect" (fun () ->
                   Nats_eio.Jetstream.Stream.info stream))
          in
          expect_stream_state "stream after reconnect" stream_info ~messages:2L
            ~last_sequence:2L;
          let ocaml_info =
            expect_jetstream_ok "OCaml consumer after reconnect"
              (retry_jetstream ~clock ~connection ~deadline:recovery_deadline
                 ~failure_file ~label:"OCaml consumer after reconnect"
                 (fun () -> Nats_eio.Jetstream.Consumer.info ocaml_consumer))
          in
          expect_push_consumer_config "OCaml consumer after reconnect"
            ocaml_info ~name:"OCAML_PUSH"
            ~delivery:(prefix ^ ".deliver.ocaml")
            ~filter:(prefix ^ ".go");
          expect_consumer_state "OCaml consumer after reconnect" ocaml_info
            ~stream_sequence:1L ~consumer_sequence:1L;
          let go_info =
            expect_jetstream_ok "Go consumer after reconnect"
              (retry_jetstream ~clock ~connection ~deadline:recovery_deadline
                 ~failure_file ~label:"Go consumer after reconnect" (fun () ->
                   Nats_eio.Jetstream.Consumer.info go_consumer))
          in
          expect_push_consumer_config "Go consumer after reconnect" go_info
            ~name:"GO_PUSH" ~delivery:(prefix ^ ".deliver.go")
            ~filter:(prefix ^ ".ocaml");
          expect_consumer_state "Go consumer after reconnect" go_info
            ~stream_sequence:2L ~consumer_sequence:1L;
          let recovery_verified =
            expect_ok "confirm JetStream recovery"
              (Nats_eio.Connection.request ~timeout:reconnect_timeout connection
                 (Nats.Subject.literal (prefix ^ ".recovery-verified"))
                 "ocaml-recovery-verified")
          in
          expect_payload "JetStream recovery confirmation"
            "go-recovery-verified" recovery_verified;
          let after_go =
            next_push_message ~clock ~failure_file ~timeout
              "receive post-reconnect Go message" push
          in
          expect_push_delivery "post-reconnect Go message" ~stream:stream_name
            ~consumer:"OCAML_PUSH" ~payload:"after-go"
            ~interop:"go-jetstream-push-reconnect" ~trace:"go-after"
            ~stream_sequence:3L ~consumer_sequence:2L after_go;
          expect_jetstream_ok "acknowledge post-reconnect Go message"
            (Nats_eio.Jetstream.Msg.ack_sync ~timeout after_go);
          let after_ack =
            expect_ok "acknowledge post-reconnect Go message to peer"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".go-acked"))
                 "after-go-acked")
          in
          expect_payload "post-reconnect Go acknowledgement" "acknowledged"
            after_ack;
          let headers =
            match
              Nats.Header.of_list
                [
                  ("X-Interop", "ocaml-jetstream-push-reconnect");
                  ("X-Trace", "ocaml-after");
                ]
            with
            | Ok headers -> headers
            | Error error ->
                failf "post-reconnect headers: %a" Nats.Header.pp_error error
          in
          let after_ocaml =
            expect_jetstream_ok "publish post-reconnect OCaml message"
              (Nats_eio.Jetstream.publish ~timeout ~headers jetstream
                 ocaml_subject "after-ocaml")
          in
          expect_publish_ack "post-reconnect OCaml publish" ~stream:stream_name
            ~sequence:4L after_ocaml;
          let completion =
            next_message ~clock ~failure_file ~timeout
              "post-reconnect completion" go_done
          in
          expect_payload "post-reconnect completion" "post-reconnect-complete"
            completion;
          let final_ocaml_info =
            expect_jetstream_ok "final OCaml consumer info"
              (Nats_eio.Jetstream.Consumer.info ocaml_consumer)
          in
          expect_consumer_state "final OCaml consumer" final_ocaml_info
            ~stream_sequence:3L ~consumer_sequence:2L;
          expect_jetstream_ok "close OCaml push session"
            (Nats_eio.Jetstream.Consumer.Push.close push);
          expect_ok "flush before cleanup"
            (Nats_eio.Connection.flush connection);
          expect_jetstream_ok "delete OCaml push consumer"
            (Nats_eio.Jetstream.Consumer.delete ocaml_consumer);
          expect_jetstream_ok "delete Go push consumer"
            (Nats_eio.Jetstream.Consumer.delete go_consumer);
          expect_jetstream_ok "delete reconnect stream"
            (Nats_eio.Jetstream.Stream.delete stream);
          let cleanup =
            expect_ok "request peer cleanup"
              (Nats_eio.Connection.request ~timeout connection
                 (Nats.Subject.literal (prefix ^ ".cleanup"))
                 "cleanup")
          in
          expect_payload "peer cleanup" "cleaned" cleanup;
          print_endline "interop-jetstream-push-reconnect: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("JetStream push reconnect acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("JetStream push reconnect acceptance failed: "
       ^ Printexc.to_string error);
      exit 1
