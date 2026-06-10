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

let expect_jetstream_config_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %a" label Nats_eio.Jetstream.Error.pp_config error

let required name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> value
  | Some _ | None -> failf "%s is required" name

let endpoint value =
  match Nats.Endpoint.of_string value with
  | Ok value -> value
  | Error error ->
      failf "invalid endpoint %S: %a" value Nats.Endpoint.pp_error error

let touch path =
  let output = open_out path in
  close_out output

let timeout = Mtime.Span.(10 * s)
let jetstream_timeout = Mtime.Span.(30 * s)

let next_event ~clock ~label events =
  let seconds = Mtime.Span.to_float_ns timeout /. 1e9 in
  match
    Eio.Fiber.first
      (fun () -> Nats_eio.Event_stream.next events)
      (fun () ->
        Eio.Time.Mono.sleep clock seconds;
        Error Nats_eio.Error.Timeout)
  with
  | Ok event -> event
  | Error error -> failf "%s: %s" label (error_message error)

let rec wait_for_event ~clock ~label ~remaining predicate events =
  if Int.equal remaining 0 then failf "timed out waiting for %s" label
  else
    let event = next_event ~clock ~label events in
    if predicate event then event
    else
      wait_for_event ~clock ~label ~remaining:(remaining - 1) predicate events

let expect_server_info ~clock ~names events =
  let label = String.concat "/" names in
  let event =
    wait_for_event ~clock ~label:("INFO from " ^ label) ~remaining:24
      (function
        | Nats_eio.Event.Core (Nats.Event.Info info) ->
            Option.fold ~none:false
              ~some:(fun name -> List.exists (String.equal name) names)
              (Nats.Info.server_name info)
        | _ -> false)
      events
  in
  match event with
  | Nats_eio.Event.Core (Nats.Event.Info info) -> info
  | _ -> assert false

let expect_disconnected ~clock events =
  ignore
    (wait_for_event ~clock ~label:"disconnect" ~remaining:24
       (function Nats_eio.Event.Disconnected -> true | _ -> false)
       events)

let expect_reconnected ~clock events =
  ignore
    (wait_for_event ~clock ~label:"reconnect" ~remaining:24
       (function Nats_eio.Event.Reconnected -> true | _ -> false)
       events)

let expect_connected ~clock events =
  ignore
    (wait_for_event ~clock ~label:"initial connection" ~remaining:24
       (function
         | Nats_eio.Event.Core Nats.Event.Connected -> true | _ -> false)
       events)

let expect_discovered info expected =
  let urls = Nats.Info.connect_urls info in
  List.iter
    (fun value ->
      if not (List.exists (String.equal value) urls) then
        failf "initial INFO did not advertise discovered server %S" value)
    expected

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

let retry_jetstream ~clock ~connection ~timeout:retry_timeout ~label operation =
  let deadline =
    match Mtime.add_span (Nats_eio.Connection.now connection) retry_timeout with
    | Some value -> value
    | None -> Mtime.max_stamp
  in
  let last_error = ref None in
  let result = ref None in
  while Option.is_none !result do
    match operation () with
    | Ok value -> result := Some (Ok value)
    | Error error when transient_jetstream_error error ->
        last_error := Some error;
        if Mtime.compare (Nats_eio.Connection.now connection) deadline >= 0 then
          result := Some (Error error)
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

let expect_stream_config info ~name =
  let config = Nats_eio.Jetstream.Stream.Info.config info in
  if not (String.equal (Nats_eio.Jetstream.Stream.Config.name config) name) then
    failf "stream info named %S, expected %S"
      (Nats_eio.Jetstream.Stream.Config.name config)
      name;
  (match Nats_eio.Jetstream.Stream.Config.storage config with
  | Nats_eio.Jetstream.Stream.Config.File -> ()
  | Nats_eio.Jetstream.Stream.Config.Memory ->
      failf "cluster stream is not file-backed");
  if not (Int.equal (Nats_eio.Jetstream.Stream.Config.replicas config) 3) then
    failf "cluster stream does not have three replicas"

let expect_stream_state info ~messages ~last_sequence =
  if not (Int64.equal (Nats_eio.Jetstream.Stream.Info.messages info) messages)
  then failf "cluster stream had the wrong message count";
  if
    not
      (Int64.equal
         (Nats_eio.Jetstream.Stream.Info.last_sequence info)
         last_sequence)
  then failf "cluster stream had the wrong last sequence"

let expect_consumer_config info ~name ~delivery ~filter =
  if not (String.equal (Nats_eio.Jetstream.Consumer.Info.name info) name) then
    failf "consumer info named the wrong consumer";
  let config = Nats_eio.Jetstream.Consumer.Info.config info in
  (match Nats_eio.Jetstream.Consumer.Config.ack_policy config with
  | Nats_eio.Jetstream.Consumer.Config.Explicit -> ()
  | Nats_eio.Jetstream.Consumer.Config.No_ack ->
      failf "cluster consumer did not use explicit acknowledgements"
  | Nats_eio.Jetstream.Consumer.Config.All ->
      failf "cluster consumer did not use explicit acknowledgements"
  | Nats_eio.Jetstream.Consumer.Config.Flow_control ->
      failf "cluster consumer did not use explicit acknowledgements");
  (match Nats_eio.Jetstream.Consumer.Config.replicas config with
  | Some 3 -> ()
  | Some replicas -> failf "consumer has %d replicas, expected three" replicas
  | None -> failf "consumer did not report a replica count");
  (match Nats_eio.Jetstream.Consumer.Config.deliver_subject config with
  | Some subject when String.equal (Nats.Subject.to_string subject) delivery ->
      ()
  | Some subject ->
      failf "consumer delivered to %S, expected %S"
        (Nats.Subject.to_string subject)
        delivery
  | None -> failf "consumer has no delivery subject");
  match Nats_eio.Jetstream.Consumer.Config.filter_subject config with
  | Some subject
    when String.equal (Nats.Subject.Filter.to_string subject) filter ->
      ()
  | Some subject ->
      failf "consumer filtered %S, expected %S"
        (Nats.Subject.Filter.to_string subject)
        filter
  | None -> failf "consumer has no filter subject"

let expect_ack_floor label expected info =
  match Nats_eio.Jetstream.Consumer.Info.ack_floor_stream_sequence info with
  | Some actual when Int64.equal actual expected -> ()
  | Some actual ->
      failf "%s ack floor was %Ld, expected %Ld" label actual expected
  | None -> failf "%s had no acknowledgement floor" label

let expect_pending_zero label info =
  if not (Int.equal (Nats_eio.Jetstream.Consumer.Info.num_ack_pending info) 0)
  then
    failf "%s retained %d pending acknowledgements" label
      (Nats_eio.Jetstream.Consumer.Info.num_ack_pending info)

let publish ~timeout jetstream ~msg_id subject payload =
  expect_jetstream_ok ("publish " ^ msg_id)
    (Nats_eio.Jetstream.publish ~timeout ~msg_id jetstream subject payload)

let expect_delivery label ~stream ~consumer ~payload ~stream_sequence
    ~consumer_sequence message =
  let actual_payload = Nats_eio.Jetstream.Msg.payload message in
  if not (String.equal actual_payload payload) then
    failf "%s payload was %S, expected %S" label actual_payload payload;
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

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let server = required "NATS_TEST_JS_CLUSTER_SERVER" in
  let signal = required "NATS_TEST_JS_CLUSTER_SIGNAL" in
  let initial_name = required "NATS_TEST_JS_CLUSTER_INITIAL_NAME" in
  let recovered_names =
    required "NATS_TEST_JS_CLUSTER_RECOVERED_NAMES"
    |> String.split_on_char ','
    |> List.filter (fun value -> not (String.equal value ""))
  in
  let discovered =
    required "NATS_TEST_JS_CLUSTER_DISCOVERED"
    |> String.split_on_char ','
    |> List.filter (fun value -> not (String.equal value ""))
  in
  if List.length recovered_names <> 2 then
    failf "NATS_TEST_JS_CLUSTER_RECOVERED_NAMES must contain two names";
  if List.length discovered <> 2 then
    failf "NATS_TEST_JS_CLUSTER_DISCOVERED must contain two endpoints";
  let config =
    expect_ok "connection config"
      (Nats_eio.Connection.Config.v ~max_reconnect_attempts:(Some 20)
         ~reconnect_delay:Mtime.Span.(100 * ms)
         ~reconnect_max_delay:Mtime.Span.(500 * ms)
         ())
  in
  let connection =
    expect_ok "connect"
      (Nats_eio.Connection.connect ~sw ~net ~clock ~config [ endpoint server ])
  in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let events = Nats_eio.Connection.events connection in
      let initial_info =
        expect_server_info ~clock ~names:[ initial_name ] events
      in
      expect_discovered initial_info discovered;
      expect_connected ~clock events;
      let jetstream =
        expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v connection)
      in
      let stream_name = "OCAML_CLUSTER_JS_" ^ string_of_int (Unix.getpid ()) in
      let subject = Nats.Subject.literal "ocaml.integration.cluster.js" in
      let filter = Nats.Subject.Filter.literal "ocaml.integration.cluster.js" in
      let stream_config =
        expect_jetstream_config_ok "stream config"
          (Nats_eio.Jetstream.Stream.Config.v ~name:stream_name
             ~subjects:[ filter ] ~storage:Nats_eio.Jetstream.Stream.Config.File
             ~replicas:3 ())
      in
      let stream =
        expect_jetstream_ok "create replicated stream"
          (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
             ~label:"create replicated stream" (fun () ->
               Nats_eio.Jetstream.Stream.create jetstream stream_config))
      in
      let stream_info =
        expect_jetstream_ok "replicated stream info"
          (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
             ~label:"replicated stream info" (fun () ->
               Nats_eio.Jetstream.Stream.info stream))
      in
      expect_stream_config stream_info ~name:stream_name;
      let delivery =
        Nats.Subject.literal
          ("ocaml.integration.cluster.js.delivery."
          ^ string_of_int (Unix.getpid ()))
      in
      let consumer_config =
        expect_jetstream_config_ok "consumer config"
          (Nats_eio.Jetstream.Consumer.Config.v
             ~durable_name:"OCAML_CLUSTER_PUSH" ~deliver_subject:delivery
             ~filter_subject:filter
             ~ack_policy:Nats_eio.Jetstream.Consumer.Config.Explicit ~replicas:3
             ())
      in
      let consumer =
        expect_jetstream_ok "create replicated consumer"
          (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
             ~label:"create replicated consumer" (fun () ->
               Nats_eio.Jetstream.Consumer.create stream consumer_config))
      in
      let consumer_info =
        expect_jetstream_ok "replicated consumer info"
          (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
             ~label:"replicated consumer info" (fun () ->
               Nats_eio.Jetstream.Consumer.info consumer))
      in
      expect_consumer_config consumer_info ~name:"OCAML_CLUSTER_PUSH"
        ~delivery:(Nats.Subject.to_string delivery)
        ~filter:(Nats.Subject.Filter.to_string filter);
      let push =
        expect_jetstream_ok "open replicated push"
          (Nats_eio.Jetstream.Consumer.Push.v ~sw consumer)
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
          then failf "replicated push was not initially empty";
          let first_ack =
            publish ~timeout jetstream ~msg_id:"cluster-before" subject
              "cluster-before"
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate first_ack then
            failf "baseline cluster publish was marked duplicate";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Publish_ack.sequence first_ack)
                 1L)
          then failf "baseline cluster publish did not receive sequence one";
          let first =
            expect_jetstream_ok "baseline cluster delivery"
              (Nats_eio.Jetstream.Consumer.Push.next_with_timeout ~timeout push)
          in
          expect_delivery "baseline cluster delivery" ~stream:stream_name
            ~consumer:"OCAML_CLUSTER_PUSH" ~payload:"cluster-before"
            ~stream_sequence:1L ~consumer_sequence:1L first;
          expect_jetstream_ok "baseline cluster acknowledgement"
            (Nats_eio.Jetstream.Msg.ack_sync ~timeout first);
          let baseline_info =
            expect_jetstream_ok "baseline acknowledgement info"
              (Nats_eio.Jetstream.Consumer.info consumer)
          in
          expect_pending_zero "baseline consumer" baseline_info;
          expect_ack_floor "baseline consumer" 1L baseline_info;
          let baseline_stream_info =
            expect_jetstream_ok "baseline stream state"
              (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
                 ~label:"baseline stream state" (fun () ->
                   Nats_eio.Jetstream.Stream.info stream))
          in
          expect_stream_config baseline_stream_info ~name:stream_name;
          expect_stream_state baseline_stream_info ~messages:1L
            ~last_sequence:1L;
          touch (signal ^ ".1");
          expect_disconnected ~clock events;
          let recovered_info =
            expect_server_info ~clock ~names:recovered_names events
          in
          expect_reconnected ~clock events;
          (match Nats.Info.server_name recovered_info with
          | Some value when not (String.equal value initial_name) -> ()
          | Some value -> failf "reconnected to killed server %S" value
          | None -> failf "reconnect INFO had no server name");
          let post_failover_stream_info =
            expect_jetstream_ok "post-failover stream readiness"
              (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
                 ~label:"post-failover stream readiness" (fun () ->
                   Nats_eio.Jetstream.Stream.info stream))
          in
          expect_stream_config post_failover_stream_info ~name:stream_name;
          expect_stream_state post_failover_stream_info ~messages:1L
            ~last_sequence:1L;
          let post_failover_consumer_info =
            expect_jetstream_ok "post-failover consumer readiness"
              (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
                 ~label:"post-failover consumer readiness" (fun () ->
                   Nats_eio.Jetstream.Consumer.info consumer))
          in
          expect_consumer_config post_failover_consumer_info
            ~name:"OCAML_CLUSTER_PUSH"
            ~delivery:(Nats.Subject.to_string delivery)
            ~filter:(Nats.Subject.Filter.to_string filter);
          expect_pending_zero "post-failover consumer"
            post_failover_consumer_info;
          expect_ack_floor "post-failover consumer" 1L
            post_failover_consumer_info;
          let second_ack =
            expect_jetstream_ok "post-failover cluster publish"
              (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
                 ~label:"post-failover cluster publish" (fun () ->
                   Nats_eio.Jetstream.publish ~timeout ~msg_id:"cluster-after"
                     jetstream subject "cluster-after"))
          in
          (* The first request may have committed before a reconnect hid its
             acknowledgement; a duplicate ack is valid when the message id
             still identifies the expected sequence. *)
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Publish_ack.sequence second_ack)
                 2L)
          then
            failf "post-failover cluster publish did not receive sequence two";
          let second =
            expect_jetstream_ok "post-failover cluster delivery"
              (Nats_eio.Jetstream.Consumer.Push.next_with_timeout
                 ~timeout:jetstream_timeout push)
          in
          expect_delivery "post-failover cluster delivery" ~stream:stream_name
            ~consumer:"OCAML_CLUSTER_PUSH" ~payload:"cluster-after"
            ~stream_sequence:2L ~consumer_sequence:2L second;
          expect_jetstream_ok "post-failover cluster acknowledgement"
            (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
               ~label:"post-failover cluster acknowledgement" (fun () ->
                 Nats_eio.Jetstream.Msg.ack_sync ~timeout second));
          let final_info =
            expect_jetstream_ok "post-failover acknowledgement info"
              (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
                 ~label:"post-failover acknowledgement info" (fun () ->
                   Nats_eio.Jetstream.Consumer.info consumer))
          in
          expect_pending_zero "final consumer" final_info;
          expect_ack_floor "final consumer" 2L final_info;
          let final_stream_info =
            expect_jetstream_ok "final stream state"
              (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
                 ~label:"final stream state" (fun () ->
                   Nats_eio.Jetstream.Stream.info stream))
          in
          expect_stream_config final_stream_info ~name:stream_name;
          expect_stream_state final_stream_info ~messages:2L ~last_sequence:2L;
          expect_jetstream_ok "close replicated push"
            (Nats_eio.Jetstream.Consumer.Push.close push);
          expect_jetstream_ok "delete replicated consumer"
            (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
               ~label:"delete replicated consumer" (fun () ->
                 Nats_eio.Jetstream.Consumer.delete consumer));
          expect_jetstream_ok "delete replicated stream"
            (retry_jetstream ~clock ~connection ~timeout:jetstream_timeout
               ~label:"delete replicated stream" (fun () ->
                 Nats_eio.Jetstream.Stream.delete stream));
          print_endline "jetstream-cluster: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("JetStream cluster acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("JetStream cluster acceptance failed: " ^ Printexc.to_string error);
      exit 1
