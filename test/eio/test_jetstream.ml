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

let expect_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let expect_core_event = function
  | Ok (Nats_eio.Event.Core event) -> event
  | Ok event ->
      fail
        (Format.asprintf "expected a core event, got %a" Nats_eio.Event.pp event)
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Error.pp error)

let expect_jetstream_ok = function
  | Ok value -> value
  | Error error -> fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error)

let expect_jetstream_config_ok = function
  | Ok value -> value
  | Error error ->
      fail (Format.asprintf "%a" Nats_eio.Jetstream.Error.pp_config error)

let expect_jetstream_error result predicate =
  match result with
  | Ok _ -> fail "expected a JetStream error"
  | Error error ->
      if not (predicate error) then
        fail
          (Format.asprintf "unexpected JetStream error: %a"
             Nats_eio.Jetstream.Error.pp error)

let ptime_of_rfc3339 value =
  match Ptime.of_rfc3339 value with
  | Ok (value, _, _) -> value
  | Error _ -> fail (Format.asprintf "invalid test timestamp %S" value)

let operation_wire operation =
  match Nats.Codec.encode operation with
  | Ok wire -> wire
  | Error error -> fail (Format.asprintf "%a" Nats.Codec.pp_error error)

let consumer_info_wire_with_sid ~sid payload =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") payload
  in
  operation_wire (Nats.Op.Msg { sid; message })

let consumer_info_wire payload = consumer_info_wire_with_sid ~sid:1 payload
let api_ok_wire ~sid = consumer_info_wire_with_sid ~sid "{}"

let push_consumer_info_wire =
  consumer_info_wire
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let push_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let owned_push_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"none","replay_policy":"instant"}}|}

let push_consumer_info_wire_with_sid_and_subject ~sid ~subject =
  let payload =
    Format.asprintf
      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"%s","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}
      subject
  in
  consumer_info_wire_with_sid ~sid payload

let push_heartbeat_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","idle_heartbeat":1000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let ephemeral_push_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let ephemeral_push_create_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let consumer_not_found_wire ~sid =
  let payload =
    {|{"error":{"code":404,"err_code":10014,"description":"consumer not found"}}|}
  in
  consumer_info_wire_with_sid ~sid payload

let pull_consumer_info_wire =
  consumer_info_wire
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let push_heartbeat_consumer_info_wire =
  consumer_info_wire
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","idle_heartbeat":1000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let push_flow_control_consumer_info_wire =
  consumer_info_wire
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","flow_control":true,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let ordered_create_wire ~sid ~name ~deliver_policy ?opt_start_seq () =
  let start_sequence =
    match opt_start_seq with
    | None -> ""
    | Some sequence -> Format.asprintf ",\"opt_start_seq\":%Ld" sequence
  in
  let payload =
    Format.asprintf
      "{\"stream_name\":\"ORDERS\",\"name\":\"%s\",\"config\":{\"deliver_policy\":\"%s\"%s,\"ack_policy\":\"none\",\"replay_policy\":\"instant\"}}"
      name deliver_policy start_sequence
  in
  consumer_info_wire_with_sid ~sid payload

let status_wire_with_sid ~sid ~code ~description =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ""
  in
  operation_wire
    (Nats.Op.Hmsg { sid; message; status = Some { code; description } })

let status_wire ~code ~description =
  status_wire_with_sid ~sid:1 ~code ~description

let control_status_wire ~sid ~reply_to ~description =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "orders.push")
      ~reply_to:(Nats.Subject.literal reply_to)
      ""
  in
  operation_wire
    (Nats.Op.Hmsg { sid; message; status = Some { code = 100; description } })

let delivery_wire_with_sid ~sid payload =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "orders.created")
      ~reply_to:(Nats.Subject.literal "$JS.ACK.ORDERS.worker.1.1.1.0.0")
      payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let delivery_wire_with_headers ~sid ~headers payload =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "orders.created")
      ~reply_to:(Nats.Subject.literal "$JS.ACK.ORDERS.worker.1.1.1.0.0")
      ~headers payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let ordered_delivery_wire ~sid ~consumer ~stream_sequence ~consumer_sequence
    payload =
  let reply_to =
    Format.asprintf "$JS.ACK.ORDERS.%s.1.%Ld.%Ld.0.0" consumer stream_sequence
      consumer_sequence
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "orders.created")
      ~reply_to:(Nats.Subject.literal reply_to)
      payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let delivery_wire payload = delivery_wire_with_sid ~sid:1 payload

let delivery_without_ack_wire payload =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "orders.created") payload
  in
  operation_wire (Nats.Op.Hmsg { sid = 1; message; status = None })

let direct_message_wire_with_sequence_header ~sid ~stream ~subject
    ~sequence_value ~timestamp payload =
  let headers =
    match
      Nats.Header.of_list
        [
          ("JSStream", stream);
          ("JSSequence", sequence_value);
          ("JSTimeStamp", timestamp);
          ("JSSubject", subject);
        ]
    with
    | Ok headers -> headers
    | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)
  in
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "_INBOX.reply")
      ~headers payload
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let direct_message_wire ~sid ~stream ~subject ~sequence ~timestamp payload =
  direct_message_wire_with_sequence_header ~sid ~stream ~subject
    ~sequence_value:(Int64.to_string sequence) ~timestamp payload

let ack_response_wire ~sid =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") "+ACK"
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let with_connection ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "jetstream-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "jetstream-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net [ `Return flow ];
  Eio.Switch.run @@ fun sw ->
  let connection =
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw connection

let with_connection_traced ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "jetstream-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "jetstream-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
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
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw ~trace connection

let with_connection_traced_clock ?config ~reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let flow = Eio_mock.Flow.make "jetstream-server" in
  Eio_mock.Flow.on_read flow reads;
  let net = Eio_mock.Net.make "jetstream-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
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
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw ~trace ~clock:env#mono_clock connection

let with_reconnecting_connection_traced ?config ~first_reads ~second_reads
    ?third_reads f =
  Eio_mock.Backend.run_full @@ fun env ->
  let first = Eio_mock.Flow.make "jetstream-server-first" in
  Eio_mock.Flow.on_read first first_reads;
  let second = Eio_mock.Flow.make "jetstream-server-second" in
  Eio_mock.Flow.on_read second second_reads;
  let connects =
    match third_reads with
    | None -> [ `Return first; `Return second ]
    | Some reads ->
        let third = Eio_mock.Flow.make "jetstream-server-third" in
        Eio_mock.Flow.on_read third reads;
        [ `Return first; `Return second; `Return third ]
  in
  let net = Eio_mock.Net.make "jetstream-reconnect-network" in
  Eio_mock.Net.on_getaddrinfo net (List.init 16 (fun _ -> `Return [ address ]));
  Eio_mock.Net.on_connect net connects;
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
    expect_ok
      (Nats_eio.Connection.connect ~sw ~net ~clock:env#mono_clock ?config
         [ endpoint ])
  in
  f ~sw ~trace ~clock:env#mono_clock connection

let contains_substring ~needle value =
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

let substring_position ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let result = ref None in
  while Option.is_none !result && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then
      result := Some !index
    else incr index
  done;
  !result

let nth_substring_position ~needle ~occurrence value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let seen = ref 0 in
  let result = ref None in
  while
    needle_length > 0 && occurrence > 0 && Option.is_none !result
    && !index <= limit
  do
    if String.equal (String.sub value !index needle_length) needle then (
      incr seen;
      if Int.equal !seen occurrence then result := Some !index
      else index := !index + needle_length)
    else incr index
  done;
  !result

let count_substring ~needle value =
  let needle_length = String.length needle in
  let limit = String.length value - needle_length in
  let index = ref 0 in
  let count = ref 0 in
  while needle_length > 0 && !index <= limit do
    if String.equal (String.sub value !index needle_length) needle then (
      incr count;
      index := !index + needle_length)
    else incr index
  done;
  !count

let wait_for_trace ~clock ~trace ~needle ~count =
  let seen = ref 0 in
  let attempts = ref 0 in
  while !seen < count && !attempts < 100 do
    Eio.Time.Mono.sleep clock 0.001;
    seen := count_substring ~needle (Buffer.contents trace);
    incr attempts
  done;
  if !seen < count then
    fail
      (Format.asprintf
         "trace did not contain %d occurrences of %S (saw %d); trace:\n%s" count
         needle !seen (Buffer.contents trace))

let consumer connection =
  let jetstream = expect_jetstream_ok (Nats_eio.Jetstream.v connection) in
  let stream =
    expect_jetstream_ok
      (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
  in
  expect_jetstream_ok (Nats_eio.Jetstream.Consumer.bind stream ~name:"worker")

let rec yield_n count =
  if count <= 0 then ()
  else (
    Eio.Fiber.yield ();
    yield_n (count - 1))

let () =
  run "nats-eio-jetstream"
    [
      test "stream config retains direct and per-subject limits" (fun () ->
          let subject = Nats.Subject.Filter.literal "$KV.users.>" in
          let placement =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.Placement.v ~cluster:"eu-west"
                 ~tags:[ "ssd" ] ())
          in
          let config =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.v ~name:"KV_users"
                 ~subjects:[ subject ] ~description:"user values"
                 ~max_msgs_per_subject:5L ~allow_rollup:true ~allow_direct:true
                 ~deny_delete:true ~replicas:3 ~placement
                 ~compression:Nats_eio.Jetstream.Stream.Config.S2
                 ~metadata:[ ("owner", "users") ]
                 ())
          in
          equal (option string) (Some "user values")
            (Nats_eio.Jetstream.Stream.Config.description config);
          let described =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_description config
                 (Some "updated values"))
          in
          equal (option string) (Some "updated values")
            (Nats_eio.Jetstream.Stream.Config.description described);
          equal (option int64) (Some 5L)
            (Nats_eio.Jetstream.Stream.Config.max_msgs_per_subject config);
          equal bool true (Nats_eio.Jetstream.Stream.Config.allow_rollup config);
          equal bool true (Nats_eio.Jetstream.Stream.Config.allow_direct config);
          equal bool true (Nats_eio.Jetstream.Stream.Config.deny_delete config);
          equal int 3 (Nats_eio.Jetstream.Stream.Config.replicas config);
          (match Nats_eio.Jetstream.Stream.Config.placement config with
          | None -> fail "stream config lost placement"
          | Some value ->
              equal (option string) (Some "eu-west")
                (Nats_eio.Jetstream.Stream.Config.Placement.cluster value);
              equal (list string) [ "ssd" ]
                (Nats_eio.Jetstream.Stream.Config.Placement.tags value));
          (match Nats_eio.Jetstream.Stream.Config.compression config with
          | Nats_eio.Jetstream.Stream.Config.S2 -> ()
          | Nats_eio.Jetstream.Stream.Config.Uncompressed ->
              fail "stream config lost compression");
          (match Nats_eio.Jetstream.Stream.Config.metadata config with
          | [ ("owner", "users") ] -> ()
          | _ -> fail "stream config lost metadata");
          (match Nats_eio.Jetstream.Stream.Config.Placement.v () with
          | Error Nats_eio.Jetstream.Error.Empty_placement -> ()
          | Ok _ -> fail "placement accepted no constraints"
          | Error error ->
              fail
                (Format.asprintf "unexpected placement error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          let rich =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_replicas config 4)
          in
          equal int 4 (Nats_eio.Jetstream.Stream.Config.replicas rich);
          let rich =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_compression rich
                 Nats_eio.Jetstream.Stream.Config.Uncompressed)
          in
          (match Nats_eio.Jetstream.Stream.Config.compression rich with
          | Nats_eio.Jetstream.Stream.Config.Uncompressed -> ()
          | Nats_eio.Jetstream.Stream.Config.S2 ->
              fail "stream config updater retained compression");
          let rich =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_metadata rich
                 [ ("team", "platform") ])
          in
          (match Nats_eio.Jetstream.Stream.Config.metadata rich with
          | [ ("team", "platform") ] -> ()
          | _ -> fail "stream config updater lost metadata");
          let rich =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_placement rich None)
          in
          (match Nats_eio.Jetstream.Stream.Config.placement rich with
          | None -> ()
          | Some _ -> fail "stream config updater retained placement");
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_max_msgs_per_subject config
                 None)
          in
          equal (option int64) None
            (Nats_eio.Jetstream.Stream.Config.max_msgs_per_subject updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_rollup updated false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_rollup updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_direct updated false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_direct updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_deny_delete updated false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.deny_delete updated);
          let sealed =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_sealed config true)
          in
          equal bool true (Nats_eio.Jetstream.Stream.Config.sealed sealed);
          match
            Nats_eio.Jetstream.Stream.Config.v ~name:"KV_users"
              ~subjects:[ subject ] ~replicas:0 ()
          with
          | Error (Nats_eio.Jetstream.Error.Invalid_replicas 0) -> ()
          | Ok _ -> fail "stream config accepted zero replicas"
          | Error error ->
              fail
                (Format.asprintf "unexpected replica error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
      test "consumer config updaters preserve modeled fields" (fun () ->
          let span = Mtime.Span.of_uint64_ns 1_000_000L in
          let second_span = Mtime.Span.of_uint64_ns 2_000_000L in
          let subject = Nats.Subject.literal "orders.push" in
          let group = Nats.Queue_group.literal "workers" in
          let filter = Nats.Subject.Filter.literal "orders.*" in
          let filter_subjects =
            [
              Nats.Subject.Filter.literal "orders.created";
              Nats.Subject.Filter.literal "orders.updated";
            ]
          in
          let pause_until = ptime_of_rfc3339 "2026-08-13T12:00:00.000000000Z" in
          let config =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                 ~description:"before" ~deliver_subject:subject
                 ~deliver_group:group ~idle_heartbeat:span ~flow_control:true
                 ~deliver_policy:
                   (Nats_eio.Jetstream.Consumer.Config.By_start_sequence 1L)
                 ~ack_policy:Nats_eio.Jetstream.Consumer.Config.All
                 ~ack_wait:span ~max_deliver:5 ~filter_subjects
                 ~backoff:[ span; second_span ] ~pause_until
                 ~sample_frequency:10 ~rate_limit:64000L ~replicas:3
                 ~metadata:[ ("owner", "server") ]
                 ~replay_policy:Nats_eio.Jetstream.Consumer.Config.Original
                 ~max_ack_pending:(-1) ~max_waiting:10 ~max_batch:5
                 ~max_expires:span ~max_bytes:1024 ~headers_only:true
                 ~inactive_threshold:span ~mem_storage:true ())
          in
          let described =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_description config
                 (Some "after"))
          in
          equal (option string) (Some "after")
            (Nats_eio.Jetstream.Consumer.Config.description described);
          equal (option int) (Some (-1))
            (Nats_eio.Jetstream.Consumer.Config.max_ack_pending described);
          equal (option int) (Some 10)
            (Nats_eio.Jetstream.Consumer.Config.sample_frequency described);
          equal (option int64) (Some 64000L)
            (Nats_eio.Jetstream.Consumer.Config.rate_limit described);
          equal (option int) (Some 3)
            (Nats_eio.Jetstream.Consumer.Config.replicas described);
          equal (list string)
            [ "orders.created"; "orders.updated" ]
            (List.map Nats.Subject.Filter.to_string
               (Nats_eio.Jetstream.Consumer.Config.filter_subjects described));
          equal (list int64) [ 1_000_000L; 2_000_000L ]
            (List.map Mtime.Span.to_uint64_ns
               (Nats_eio.Jetstream.Consumer.Config.backoff described));
          (match Nats_eio.Jetstream.Consumer.Config.pause_until described with
          | Some value when Ptime.equal pause_until value -> ()
          | _ -> fail "consumer config updater lost pause deadline");
          (match Nats_eio.Jetstream.Consumer.Config.metadata described with
          | [ ("owner", "server") ] -> ()
          | _ -> fail "consumer config updater lost metadata");
          let cleared_sample_frequency =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_sample_frequency
                 described None)
          in
          let cleared_rate_limit =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_rate_limit
                 cleared_sample_frequency None)
          in
          let cleared_replicas =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_replicas
                 cleared_rate_limit None)
          in
          let cleared_metadata =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_metadata cleared_replicas
                 [])
          in
          equal (option int) None
            (Nats_eio.Jetstream.Consumer.Config.sample_frequency
               cleared_metadata);
          equal (option int64) None
            (Nats_eio.Jetstream.Consumer.Config.rate_limit cleared_metadata);
          equal (option int) None
            (Nats_eio.Jetstream.Consumer.Config.replicas cleared_metadata);
          (match
             Nats_eio.Jetstream.Consumer.Config.metadata cleared_metadata
           with
          | [] -> ()
          | _ -> fail "consumer config updater retained metadata");
          let singular =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_filter_subjects
                 cleared_metadata filter_subjects)
          in
          (match Nats_eio.Jetstream.Consumer.Config.filter_subject singular with
          | None -> ()
          | Some _ -> fail "consumer config updater retained singular filter");
          equal (list string)
            [ "orders.created"; "orders.updated" ]
            (List.map Nats.Subject.Filter.to_string
               (Nats_eio.Jetstream.Consumer.Config.filter_subjects singular));
          let singular =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_filter_subject singular
                 (Some filter))
          in
          (match Nats_eio.Jetstream.Consumer.Config.filter_subject singular with
          | Some value
            when String.equal (Nats.Subject.Filter.to_string value) "orders.*"
            ->
              ()
          | _ -> fail "consumer config updater did not set singular filter");
          (match
             Nats_eio.Jetstream.Consumer.Config.filter_subjects singular
           with
          | [] -> ()
          | _ -> fail "consumer config updater retained plural filters");
          let cleared_filters =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_filter_subjects singular
                 [])
          in
          (match
             Nats_eio.Jetstream.Consumer.Config.filter_subject cleared_filters
           with
          | None -> ()
          | Some _ -> fail "consumer config updater retained a cleared filter");
          equal (list int64) [ 1_000_000L; 2_000_000L ]
            (List.map Mtime.Span.to_uint64_ns
               (Nats_eio.Jetstream.Consumer.Config.backoff cleared_filters));
          let cleared_backoff =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_backoff cleared_filters
                 [])
          in
          (match Nats_eio.Jetstream.Consumer.Config.backoff cleared_backoff with
          | [] -> ()
          | _ -> fail "consumer config updater retained cleared backoff");
          let cleared_group =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_deliver_group
                 cleared_backoff None)
          in
          let cleared_subject =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_deliver_subject
                 cleared_group None)
          in
          (match
             Nats_eio.Jetstream.Consumer.Config.deliver_subject cleared_subject
           with
          | None -> ()
          | Some _ -> fail "consumer config updater retained delivery subject");
          (match
             Nats_eio.Jetstream.Consumer.Config.deliver_group cleared_subject
           with
          | None -> ()
          | Some _ -> fail "consumer config updater retained delivery group");
          let defaulted =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_max_ack_pending
                 cleared_subject None)
          in
          equal (option int) None
            (Nats_eio.Jetstream.Consumer.Config.max_ack_pending defaulted));
      test "consumer scalar configuration validates server limits" (fun () ->
          (match
             Nats_eio.Jetstream.Consumer.Config.v ~sample_frequency:(-1) ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_sample_frequency "-1")
            ->
              ()
          | _ -> fail "consumer config accepted an invalid sample frequency");
          (match Nats_eio.Jetstream.Consumer.Config.v ~rate_limit:(-1L) () with
          | Error (Nats_eio.Jetstream.Error.Invalid_consumer_rate_limit -1L) ->
              ()
          | _ -> fail "consumer config accepted a negative rate limit");
          (match Nats_eio.Jetstream.Consumer.Config.v ~replicas:(-1) () with
          | Error (Nats_eio.Jetstream.Error.Invalid_consumer_replicas -1) -> ()
          | _ -> fail "consumer config accepted negative replicas");
          (match Nats_eio.Jetstream.Consumer.Config.v ~rate_limit:1L () with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "rate_limit"; value = "requires deliver_subject" })
            ->
              ()
          | _ -> fail "consumer config accepted pull rate limiting");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~filter_subject:(Nats.Subject.Filter.literal "orders.*")
               ~filter_subjects:[ Nats.Subject.Filter.literal "orders.created" ]
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 {
                   field = "filter_subjects";
                   value = "exclusive with filter_subject";
                 }) ->
              ()
          | _ -> fail "consumer config accepted exclusive filter forms");
          match
            Nats_eio.Jetstream.Consumer.Config.v ~backoff:[ Mtime.Span.zero ] ()
          with
          | Ok _ -> ()
          | Error _ -> fail "consumer config rejected zero backoff");
      test "priority consumer configuration validates policy constraints"
        (fun () ->
          let timeout = Mtime.Span.(30 * s) in
          let config =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.v ~priority_groups:[ "blue" ]
                 ~priority_policy:
                   Nats_eio.Jetstream.Consumer.Config.Pinned_client
                 ~priority_timeout:timeout ())
          in
          equal (list string) [ "blue" ]
            (Nats_eio.Jetstream.Consumer.Config.priority_groups config);
          (match Nats_eio.Jetstream.Consumer.Config.priority_policy config with
          | Some Nats_eio.Jetstream.Consumer.Config.Pinned_client -> ()
          | _ -> fail "priority consumer lost its policy");
          equal int64 30_000_000_000L
            (Mtime.Span.to_uint64_ns
               (Option.get
                  (Nats_eio.Jetstream.Consumer.Config.priority_timeout config)));
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~priority_groups:[ "blue"; "green" ]
               ~priority_policy:Nats_eio.Jetstream.Consumer.Config.Pinned_client
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 {
                   field = "priority_groups";
                   value = "only one group is currently supported";
                 }) ->
              ()
          | _ -> fail "priority config accepted multiple groups");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~priority_groups:[ "bad.group" ]
               ~priority_policy:Nats_eio.Jetstream.Consumer.Config.Prioritized
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_priority_group
                 "bad.group") ->
              ()
          | _ -> fail "priority config accepted an invalid group name");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~priority_policy:Nats_eio.Jetstream.Consumer.Config.Prioritized
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 {
                   field = "priority_policy";
                   value = "requires priority_groups";
                 }) ->
              ()
          | _ -> fail "priority policy accepted an empty group list");
          (match
             Nats_eio.Jetstream.Consumer.Config.v ~priority_groups:[ "blue" ]
               ~priority_timeout:timeout ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 {
                   field = "priority_groups";
                   value = "requires priority_policy";
                 }) ->
              ()
          | _ -> fail "priority groups accepted no policy");
          (match
             Nats_eio.Jetstream.Consumer.Config.v ~priority_groups:[ "blue" ]
               ~priority_policy:Nats_eio.Jetstream.Consumer.Config.Overflow
               ~priority_timeout:timeout ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 {
                   field = "priority_timeout";
                   value = "requires pinned_client";
                 }) ->
              ()
          | _ -> fail "overflow policy accepted a pin timeout");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_subject:(Nats.Subject.literal "orders.push")
               ~priority_groups:[ "blue" ]
               ~priority_policy:Nats_eio.Jetstream.Consumer.Config.Prioritized
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "priority_policy"; value = "requires pull consumer" })
            ->
              ()
          | _ -> fail "priority config accepted a push consumer");
          match
            Nats_eio.Jetstream.Consumer.Config.v
              ~ack_policy:Nats_eio.Jetstream.Consumer.Config.No_ack
              ~priority_groups:[ "blue" ]
              ~priority_policy:Nats_eio.Jetstream.Consumer.Config.Overflow ()
          with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "ack_policy"; value = "requires explicit" }) ->
              ()
          | _ -> fail "overflow policy accepted implicit acknowledgement");
      test "priority consumer create emits policy and timeout" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ~priority_groups:[ "blue" ]
                     ~priority_policy:
                       Nats_eio.Jetstream.Consumer.Config.Pinned_client
                     ~priority_timeout:Mtime.Span.(30 * s)
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.create stream config));
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring
                     ~needle:"priority_groups\\\":[\\\"blue\\\"]" trace)
              then fail "priority consumer create omitted its groups";
              if
                not
                  (contains_substring
                     ~needle:"priority_policy\\\":\\\"pinned_client" trace)
              then fail "priority consumer create omitted its policy";
              if
                not
                  (contains_substring ~needle:"priority_timeout\\\":30000000000"
                     trace)
              then fail "priority consumer create omitted its timeout";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","priority_groups":["blue"],"priority_policy":"pinned_client","priority_timeout":30000000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              let consumer = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "worker" (Nats_eio.Jetstream.Consumer.name consumer);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer info exposes priority pin state" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let consumer = consumer connection in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.info consumer));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","priority_groups":["blue"],"priority_policy":"pinned_client","priority_timeout":120000000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"},"priority_groups":[{"group":"blue","pinned_client_id":"pin-1","pinned_ts":"2026-08-12T12:00:00.000000000Z"}]}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              let config = Nats_eio.Jetstream.Consumer.Info.config info in
              equal (list string) [ "blue" ]
                (Nats_eio.Jetstream.Consumer.Config.priority_groups config);
              (match Nats_eio.Jetstream.Consumer.Info.priority_groups info with
              | [ group ] -> (
                  equal string "blue"
                    (Nats_eio.Jetstream.Consumer.Priority_group.name group);
                  equal (option string) (Some "pin-1")
                    (Nats_eio.Jetstream.Consumer.Priority_group.pinned_client_id
                       group);
                  match
                    Nats_eio.Jetstream.Consumer.Priority_group.pinned_at group
                  with
                  | Some value
                    when Ptime.equal value
                           (ptime_of_rfc3339 "2026-08-12T12:00:00.000000000Z")
                    ->
                      ()
                  | _ -> fail "consumer info lost the pin timestamp")
              | _ -> fail "consumer info lost its priority group state");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer unpin sends the group request and clears its pin"
        (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let unpin_response, unpin_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await unpin_response;
                `Await second_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let consumer = consumer connection in
              let headers =
                match Nats.Header.of_list [ ("Nats-Pin-Id", "pin-unpin") ] with
                | Ok headers -> headers
                | Error error ->
                    fail (Format.asprintf "%a" Nats.Header.pp_error error)
              in
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1
                       ~group:"blue"));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok (delivery_wire_with_headers ~sid:1 ~headers "first"));
              ignore (expect_jetstream_ok (Eio.Promise.await first_result));
              let unpin_result, unpin_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve unpin_result_u
                    (Nats_eio.Jetstream.Consumer.unpin consumer ~group:"blue"));
              yield_n 5;
              let unpin_trace = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"CONSUMER.UNPIN.ORDERS.worker"
                     unpin_trace)
              then fail "consumer unpin used the wrong endpoint";
              if
                not
                  (contains_substring ~needle:"group\\\":\\\"blue" unpin_trace)
              then fail "consumer unpin omitted its group";
              Eio.Promise.resolve unpin_response_u (Ok (api_ok_wire ~sid:2));
              expect_jetstream_ok (Eio.Promise.await unpin_result);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1
                       ~group:"blue"));
              yield_n 5;
              let second_trace = Buffer.contents trace in
              if
                not
                  (Int.equal
                     (count_substring ~needle:"id\\\":\\\"pin-unpin"
                        second_trace)
                     0)
              then fail "consumer unpin left a stale local pin";
              Eio.Promise.resolve second_response_u
                (Ok (delivery_wire_with_sid ~sid:3 "second"));
              ignore (expect_jetstream_ok (Eio.Promise.await second_result));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer create emits modern config fields" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let pause_until =
                ptime_of_rfc3339 "2026-08-13T12:00:00.000000000Z"
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ~deliver_subject:(Nats.Subject.literal "orders.push")
                     ~filter_subjects:
                       [
                         Nats.Subject.Filter.literal "orders.created";
                         Nats.Subject.Filter.literal "orders.updated";
                       ]
                     ~backoff:
                       [
                         Mtime.Span.of_uint64_ns 1_000_000L;
                         Mtime.Span.of_uint64_ns 2_000_000L;
                       ]
                     ~pause_until ~sample_frequency:25 ~rate_limit:65536L
                     ~replicas:3
                     ~metadata:[ ("owner", "client") ]
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.create stream config));
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"CONSUMER.CREATE.ORDERS.worker"
                     trace)
              then fail "consumer create was not sent";
              if
                not (contains_substring ~needle:"sample_freq\\\":\\\"25%" trace)
              then fail "consumer create omitted sample frequency";
              if
                not
                  (contains_substring ~needle:"rate_limit_bps\\\":65536" trace)
              then fail "consumer create omitted rate limit";
              if not (contains_substring ~needle:"num_replicas\\\":3" trace)
              then fail "consumer create omitted replica count";
              if
                not
                  (contains_substring
                     ~needle:
                       "filter_subjects\\\":[\\\"orders.created\\\",\\\"orders.updated\\\"]"
                     trace)
              then fail "consumer create omitted plural filters";
              if
                not
                  (contains_substring ~needle:"backoff\\\":[1000000,2000000]"
                     trace)
              then fail "consumer create omitted backoff";
              if
                not
                  (contains_substring
                     ~needle:
                       "pause_until\\\":\\\"2026-08-13T12:00:00.000000000Z"
                     trace)
              then fail "consumer create omitted pause deadline";
              if
                not
                  (contains_substring
                     ~needle:"metadata\\\":{\\\"owner\\\":\\\"client\\\"}" trace)
              then fail "consumer create omitted metadata";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","sample_freq":"25%","rate_limit_bps":65536,"num_replicas":3,"metadata":{"owner":"client"},"replay_policy":"instant"}}|}));
              let consumer = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "worker" (Nats_eio.Jetstream.Consumer.name consumer);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer pause and resume use the dedicated control endpoint"
        (fun () ->
          let pause_response, pause_response_u = Eio.Promise.create () in
          let resume_response, resume_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await pause_response;
                `Await resume_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let consumer = consumer connection in
              let until = ptime_of_rfc3339 "2026-08-13T12:00:00.000000000Z" in
              let pause_result, pause_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve pause_result_u
                    (Nats_eio.Jetstream.Consumer.pause consumer ~until));
              yield_n 5;
              Eio.Promise.resolve pause_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"paused":true,"pause_until":"2026-08-13T12:00:00.000000000Z","pause_remaining":60000000000,"future_field":true}|}));
              let paused =
                expect_jetstream_ok (Eio.Promise.await pause_result)
              in
              equal bool true (Nats_eio.Jetstream.Consumer.Pause.paused paused);
              (match Nats_eio.Jetstream.Consumer.Pause.pause_until paused with
              | Some value when Ptime.equal until value -> ()
              | _ -> fail "pause response lost its deadline");
              equal (option int64) (Some 60_000_000_000L)
                (Option.map Mtime.Span.to_uint64_ns
                   (Nats_eio.Jetstream.Consumer.Pause.pause_remaining paused));
              let resume_result, resume_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve resume_result_u
                    (Nats_eio.Jetstream.Consumer.resume consumer));
              yield_n 5;
              Eio.Promise.resolve resume_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"paused":false,"pause_until":"0001-01-01T00:00:00.000000000+00:00"}|}));
              let resumed =
                expect_jetstream_ok (Eio.Promise.await resume_result)
              in
              equal bool false
                (Nats_eio.Jetstream.Consumer.Pause.paused resumed);
              (match Nats_eio.Jetstream.Consumer.Pause.pause_until resumed with
              | None -> ()
              | Some _ -> fail "consumer resume retained its deadline");
              equal (option int64) None
                (Option.map Mtime.Span.to_uint64_ns
                   (Nats_eio.Jetstream.Consumer.Pause.pause_remaining resumed));
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring
                     ~needle:"wrote \"PUB $JS.API.CONSUMER.PAUSE.ORDERS.worker"
                     trace)
              then fail "consumer pause did not use the pause endpoint";
              if
                not
                  (Int.equal
                     (count_substring
                        ~needle:
                          "wrote \"PUB $JS.API.CONSUMER.PAUSE.ORDERS.worker"
                        trace)
                     2)
              then fail "consumer resume did not use the pause endpoint";
              if
                not
                  (contains_substring
                     ~needle:
                       "pause_until\\\":\\\"2026-08-13T12:00:00.000000000Z"
                     trace)
              then fail "consumer pause omitted its deadline";
              if not (Int.equal (count_substring ~needle:" 0\\r\\n\"" trace) 1)
              then fail "consumer resume did not send an empty request";
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer info normalizes zero and rejects malformed pause deadlines"
        (fun () ->
          let zero_response, zero_response_u = Eio.Promise.create () in
          let malformed_response, malformed_response_u =
            Eio.Promise.create ()
          in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await zero_response;
                `Await malformed_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let consumer = consumer connection in
              let zero_result, zero_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve zero_result_u
                    (Nats_eio.Jetstream.Consumer.info consumer));
              yield_n 5;
              Eio.Promise.resolve zero_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","pause_until":"0001-01-01T00:00:00.000000000+00:00"}}|}));
              let zero_info =
                expect_jetstream_ok (Eio.Promise.await zero_result)
              in
              (match Nats_eio.Jetstream.Consumer.Info.pause_until zero_info with
              | None -> ()
              | Some _ -> fail "consumer info retained the zero pause deadline");
              let malformed_result, malformed_result_u =
                Eio.Promise.create ()
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve malformed_result_u
                    (Nats_eio.Jetstream.Consumer.info consumer));
              yield_n 5;
              Eio.Promise.resolve malformed_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","pause_until":"not-a-timestamp"}}|}));
              expect_jetstream_error (Eio.Promise.await malformed_result)
                (function
                | Nats_eio.Jetstream.Error.Invalid_config
                    (Nats_eio.Jetstream.Error.Invalid_consumer_pause_until
                       "not-a-timestamp") ->
                    true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream create emits retained config fields" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"KV_users"
                     ~subjects:[ Nats.Subject.Filter.literal "$KV.users.>" ]
                     ~description:"user values" ~max_msgs_per_subject:5L
                     ~allow_rollup:true ~allow_direct:true ~deny_delete:true
                     ~replicas:3
                     ~compression:Nats_eio.Jetstream.Stream.Config.S2
                     ~metadata:[ ("owner", "users") ]
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.create jetstream config));
              yield_n 5;
              let trace = Buffer.contents trace in
              if not (contains_substring ~needle:"STREAM.CREATE.KV_users" trace)
              then fail "stream create was not sent";
              if
                not
                  (contains_substring ~needle:"description\\\":\\\"user values"
                     trace)
              then fail "stream create omitted the description";
              if
                not
                  (contains_substring ~needle:"max_msgs_per_subject\\\":5" trace)
              then fail "stream create omitted the per-subject limit";
              if
                not
                  (contains_substring ~needle:"allow_rollup_hdrs\\\":true" trace)
              then fail "stream create omitted the rollup-header flag";
              if not (contains_substring ~needle:"allow_direct\\\":true" trace)
              then fail "stream create omitted the direct-read flag";
              if not (contains_substring ~needle:"deny_delete\\\":true" trace)
              then fail "stream create omitted the deny-delete flag";
              if not (contains_substring ~needle:"num_replicas\\\":3" trace)
              then fail "stream create omitted the replica count";
              if not (contains_substring ~needle:"compression\\\":\\\"s2" trace)
              then fail "stream create omitted compression";
              if
                not
                  (contains_substring
                     ~needle:"metadata\\\":{\\\"owner\\\":\\\"users\\\"}" trace)
              then fail "stream create omitted stream metadata";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"config":{"name":"KV_users","subjects":["$KV.users.>"],"description":"user values","storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":5,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":true,"allow_direct":true,"deny_delete":true,"num_replicas":3,"sealed":true,"metadata":{"owner":"test"}}}|}));
              let stream = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "KV_users" (Nats_eio.Jetstream.Stream.name stream);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream update preserves unknown server configuration" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let update_response, update_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await update_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS"
                     ~subjects:[ Nats.Subject.Filter.literal "orders.>" ]
                     ~description:"updated" ~max_msgs_per_subject:5L
                     ~allow_rollup:true ~allow_direct:true ~replicas:2
                     ~compression:Nats_eio.Jetstream.Stream.Config.S2
                     ~metadata:[ ("owner", "client") ]
                     ())
              in
              let placement =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.Placement.v ~cluster:"east"
                     ())
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.with_placement config
                     (Some placement))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.update stream config));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"config":{"name":"ORDERS","subjects":["orders.>"],"description":"before","storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":false,"allow_direct":false,"num_replicas":3,"sealed":true,"placement":{"cluster":"west"},"metadata":{"owner":"server"},"republish":{"src":"orders.in","dest":"orders.out"}},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
              yield_n 5;
              let trace = Buffer.contents trace in
              if not (contains_substring ~needle:"STREAM.UPDATE.ORDERS" trace)
              then fail "stream update was not sent";
              if
                not
                  (contains_substring ~needle:"description\\\":\\\"updated"
                     trace)
              then fail "stream update did not emit the changed description";
              if
                not
                  (contains_substring ~needle:"max_msgs_per_subject\\\":5" trace)
              then
                fail "stream update did not emit the changed per-subject limit";
              if
                not
                  (contains_substring ~needle:"allow_rollup_hdrs\\\":true" trace)
              then fail "stream update did not emit the changed rollup flag";
              if not (contains_substring ~needle:"allow_direct\\\":true" trace)
              then
                fail "stream update did not emit the changed direct-read flag";
              if count_substring ~needle:"num_replicas\\\":2" trace < 1 then
                fail "stream update did not replace the replica count";
              if count_substring ~needle:"sealed\\\":true" trace < 2 then
                fail "stream update discarded an unknown boolean field";
              if
                count_substring
                  ~needle:"metadata\\\":{\\\"owner\\\":\\\"client\\\"}" trace
                < 1
              then fail "stream update did not replace stream metadata";
              if
                count_substring
                  ~needle:
                    "republish\\\":{\\\"src\\\":\\\"orders.in\\\",\\\"dest\\\":\\\"orders.out\\\"}"
                  trace
                < 1
              then fail "stream update discarded an unmodeled object field";
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"config":{"name":"ORDERS","subjects":["orders.>"],"description":"updated","storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":5,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":true,"allow_direct":true,"num_replicas":2,"sealed":true,"placement":{"cluster":"east"},"compression":"s2","metadata":{"owner":"client"},"republish":{"src":"orders.in","dest":"orders.out"}},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              equal (option string) (Some "updated")
                (Nats_eio.Jetstream.Stream.Config.description
                   (Nats_eio.Jetstream.Stream.Info.config info));
              let config = Nats_eio.Jetstream.Stream.Info.config info in
              equal int 2 (Nats_eio.Jetstream.Stream.Config.replicas config);
              (match Nats_eio.Jetstream.Stream.Config.compression config with
              | Nats_eio.Jetstream.Stream.Config.S2 -> ()
              | Nats_eio.Jetstream.Stream.Config.Uncompressed ->
                  fail "stream update lost compression");
              (match Nats_eio.Jetstream.Stream.Config.metadata config with
              | [ ("owner", "client") ] -> ()
              | _ -> fail "stream update lost stream metadata");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream direct reads reject invalid replies" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await second_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              expect_jetstream_error
                (Nats_eio.Jetstream.Stream.get stream ~sequence:(-1L)) (function
                | Nats_eio.Jetstream.Error.Invalid_message_header
                    { name = "JSSequence"; value = "-1" } ->
                    true
                | _ -> false);
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Stream.get stream ~sequence:7L));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok (consumer_info_wire_with_sid ~sid:1 ""));
              expect_jetstream_error (Eio.Promise.await first_result) (function
                | Nats_eio.Jetstream.Error.Message_not_found -> true
                | _ -> false);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Stream.get stream ~sequence:8L));
              yield_n 5;
              Eio.Promise.resolve second_response_u
                (Ok
                   (direct_message_wire_with_sequence_header ~sid:2
                      ~stream:"ORDERS" ~subject:"orders.created"
                      ~sequence_value:"not-a-number"
                      ~timestamp:"2026-08-11T12:00:00.000000000Z" "malformed"));
              expect_jetstream_error (Eio.Promise.await second_result) (function
                | Nats_eio.Jetstream.Error.Invalid_message_header
                    { name = "JSSequence"; value = "not-a-number" } ->
                    true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test
        "consumer update uses the action envelope and preserves unknown \
         configuration" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let update_info_response, update_info_response_u =
            Eio.Promise.create ()
          in
          let update_response, update_response_u = Eio.Promise.create () in
          let preserve_info_response, preserve_info_response_u =
            Eio.Promise.create ()
          in
          let preserve_response, preserve_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await update_info_response;
                `Await update_response;
                `Await preserve_info_response;
                `Await preserve_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let consumer =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.bind stream ~name:"worker")
              in
              let mismatched =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"other" ())
              in
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.update consumer mismatched)
                (function
                | Nats_eio.Jetstream.Error.Unexpected_consumer_name
                    { expected = "worker"; actual = "other" } ->
                    true
                | _ -> false);
              let info_result, info_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve info_result_u
                    (Nats_eio.Jetstream.Consumer.info consumer));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"before","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","max_deliver":5,"max_ack_pending":-1,"filter_subjects":["orders.created","orders.updated"],"backoff":[1000000,2000000],"pause_until":"2026-08-13T12:00:00Z","sample_freq":"10%","rate_limit_bps":64000,"num_replicas":2,"metadata":{"owner":"server"},"future_field":true},"paused":true,"pause_remaining":60000000000}|}));
              let current_info =
                expect_jetstream_ok (Eio.Promise.await info_result)
              in
              equal bool true
                (Nats_eio.Jetstream.Consumer.Info.paused current_info);
              (match
                 Nats_eio.Jetstream.Consumer.Info.pause_until current_info
               with
              | Some value
                when Ptime.equal (ptime_of_rfc3339 "2026-08-13T12:00:00Z") value
                ->
                  ()
              | _ -> fail "consumer info lost pause deadline");
              equal (option int64) (Some 60_000_000_000L)
                (Option.map Mtime.Span.to_uint64_ns
                   (Nats_eio.Jetstream.Consumer.Info.pause_remaining
                      current_info));
              let preserved_config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_description
                     (Nats_eio.Jetstream.Consumer.Info.config current_info)
                     (Some "preserved"))
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_description
                     (Nats_eio.Jetstream.Consumer.Info.config current_info)
                     (Some "updated"))
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_durable_name config
                     None)
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_max_deliver config
                     (Some 7))
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_sample_frequency
                     config None)
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_rate_limit config
                     None)
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_replicas config None)
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_metadata config [])
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_filter_subjects
                     config [])
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_backoff config [])
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_pause_until config
                     None)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.update consumer config));
              yield_n 5;
              Eio.Promise.resolve update_info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"before","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","max_deliver":5,"max_ack_pending":-1,"filter_subjects":["orders.created","orders.updated"],"backoff":[1000000,2000000],"pause_until":"2026-08-13T12:00:00Z","sample_freq":"10%","rate_limit_bps":64000,"num_replicas":2,"metadata":{"owner":"server"},"future_field":true}}|}));
              yield_n 5;
              let trace_buffer = trace in
              let trace = Buffer.contents trace in
              let clear_trace =
                match
                  nth_substring_position
                    ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS.worker"
                    ~occurrence:1 trace
                with
                | Some position ->
                    String.sub trace position (String.length trace - position)
                | None -> fail "consumer update write was not traced"
              in
              if
                not
                  (contains_substring
                     ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS.worker"
                     clear_trace)
              then fail "consumer update was not sent to the named CREATE API";
              if contains_substring ~needle:"CONSUMER.UPDATE" clear_trace then
                fail "consumer update used an unsupported UPDATE subject";
              if
                not
                  (contains_substring ~needle:"\\\"action\\\":\\\"update"
                     clear_trace)
              then fail "consumer update omitted its action";
              if
                not
                  (contains_substring ~needle:"\\\"stream_name\\\":\\\"ORDERS"
                     clear_trace)
              then fail "consumer update omitted the stream name";
              if
                not
                  (contains_substring ~needle:"description\\\":\\\"updated"
                     clear_trace)
              then fail "consumer update omitted the changed description";
              if
                not (contains_substring ~needle:"max_deliver\\\":7" clear_trace)
              then fail "consumer update omitted the changed delivery limit";
              if
                not
                  (contains_substring ~needle:"max_ack_pending\\\":-1"
                     clear_trace)
              then fail "consumer update reset unlimited ack pending";
              if
                not
                  (contains_substring ~needle:"durable_name\\\":\\\"worker\\\""
                     clear_trace)
              then fail "consumer update dropped the durable identity";
              if
                not
                  (contains_substring ~needle:"sample_freq\\\":\\\"0%"
                     clear_trace)
              then fail "consumer update did not clear sample frequency";
              if
                not
                  (contains_substring ~needle:"rate_limit_bps\\\":0" clear_trace)
              then fail "consumer update did not clear rate limiting";
              if
                not
                  (contains_substring ~needle:"num_replicas\\\":0" clear_trace)
              then fail "consumer update did not restore replica inheritance";
              if
                not
                  (contains_substring ~needle:"filter_subject\\\":\\\"\\\""
                     clear_trace)
              then fail "consumer update did not clear singular filter";
              if
                not
                  (contains_substring ~needle:"filter_subjects\\\":[]"
                     clear_trace)
              then fail "consumer update did not clear plural filters";
              if not (contains_substring ~needle:"backoff\\\":[]" clear_trace)
              then fail "consumer update did not clear backoff";
              if
                not
                  (contains_substring ~needle:"2026-08-13T12:00:00Z" clear_trace)
              then fail "consumer update did not preserve pause deadline";
              if not (contains_substring ~needle:"metadata\\\":{}" clear_trace)
              then fail "consumer update did not clear metadata";
              if
                not
                  (contains_substring ~needle:"future_field\\\":true"
                     clear_trace)
              then
                fail "consumer update discarded an unknown configuration field";
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:3
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"updated","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","max_deliver":7,"max_ack_pending":-1,"filter_subject":"","filter_subjects":[],"backoff":[],"pause_until":"2026-08-13T12:00:00Z","sample_freq":"0%","rate_limit_bps":0,"num_replicas":0,"metadata":{},"future_field":true},"paused":true,"pause_remaining":60000000000,"num_pending":3}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              equal (option string) (Some "updated")
                (Nats_eio.Jetstream.Consumer.Config.description
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              equal (option int) (Some 7)
                (Nats_eio.Jetstream.Consumer.Config.max_deliver
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              equal (option int) (Some (-1))
                (Nats_eio.Jetstream.Consumer.Config.max_ack_pending
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              equal (option int) None
                (Nats_eio.Jetstream.Consumer.Config.sample_frequency
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              equal (option int64) None
                (Nats_eio.Jetstream.Consumer.Config.rate_limit
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              equal (option int) None
                (Nats_eio.Jetstream.Consumer.Config.replicas
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              (match
                 Nats_eio.Jetstream.Consumer.Config.filter_subject
                   (Nats_eio.Jetstream.Consumer.Info.config info)
               with
              | None -> ()
              | Some _ ->
                  fail "consumer update response retained singular filter");
              (match
                 Nats_eio.Jetstream.Consumer.Config.filter_subjects
                   (Nats_eio.Jetstream.Consumer.Info.config info)
               with
              | [] -> ()
              | _ -> fail "consumer update response retained plural filters");
              (match
                 Nats_eio.Jetstream.Consumer.Config.backoff
                   (Nats_eio.Jetstream.Consumer.Info.config info)
               with
              | [] -> ()
              | _ -> fail "consumer update response retained backoff");
              (match
                 Nats_eio.Jetstream.Consumer.Config.metadata
                   (Nats_eio.Jetstream.Consumer.Info.config info)
               with
              | [] -> ()
              | _ -> fail "consumer update response retained cleared metadata");
              let preserve_result, preserve_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve preserve_result_u
                    (Nats_eio.Jetstream.Consumer.update consumer
                       preserved_config));
              yield_n 5;
              Eio.Promise.resolve preserve_info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:4
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"updated","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","max_deliver":7,"max_ack_pending":-1,"pause_until":"2026-08-13T12:00:00Z","sample_freq":"0%","rate_limit_bps":0,"num_replicas":0,"metadata":{},"future_field":true},"paused":true,"pause_remaining":60000000000}|}));
              yield_n 5;
              let final_trace = Buffer.contents trace_buffer in
              let preserve_trace =
                match
                  nth_substring_position
                    ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS.worker"
                    ~occurrence:2 final_trace
                with
                | Some position ->
                    String.sub final_trace position
                      (String.length final_trace - position)
                | None -> fail "second consumer update write was not traced"
              in
              if
                not
                  (contains_substring ~needle:"description\\\":\\\"preserved"
                     preserve_trace)
              then
                fail "consumer update did not preserve description replacement";
              if
                not
                  (contains_substring ~needle:"sample_freq\\\":\\\"10%"
                     preserve_trace)
              then fail "consumer update did not preserve sample frequency";
              if
                not
                  (contains_substring ~needle:"rate_limit_bps\\\":64000"
                     preserve_trace)
              then fail "consumer update did not preserve rate limiting";
              if
                not
                  (contains_substring ~needle:"num_replicas\\\":2"
                     preserve_trace)
              then fail "consumer update did not preserve replica count";
              if
                not
                  (contains_substring ~needle:"filter_subject\\\":\\\"\\\""
                     preserve_trace)
              then
                fail "consumer update did not clear singular filter on preserve";
              if
                not
                  (contains_substring
                     ~needle:
                       "filter_subjects\\\":[\\\"orders.created\\\",\\\"orders.updated\\\"]"
                     preserve_trace)
              then fail "consumer update did not preserve plural filters";
              if
                not
                  (contains_substring ~needle:"backoff\\\":[1000000,2000000]"
                     preserve_trace)
              then fail "consumer update did not preserve backoff";
              if
                not
                  (contains_substring
                     ~needle:"metadata\\\":{\\\"owner\\\":\\\"server\\\"}"
                     preserve_trace)
              then fail "consumer update did not preserve metadata";
              Eio.Promise.resolve preserve_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:5
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"preserved","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","max_deliver":5,"max_ack_pending":-1,"filter_subject":"","filter_subjects":["orders.created","orders.updated"],"backoff":[1000000,2000000],"pause_until":"2026-08-13T12:00:00Z","sample_freq":"10%","rate_limit_bps":64000,"num_replicas":2,"metadata":{"owner":"server"},"future_field":true},"paused":true,"pause_remaining":60000000000}|}));
              let preserved_info =
                expect_jetstream_ok (Eio.Promise.await preserve_result)
              in
              equal (option int) (Some 10)
                (Nats_eio.Jetstream.Consumer.Config.sample_frequency
                   (Nats_eio.Jetstream.Consumer.Info.config preserved_info));
              equal (option int64) (Some 64000L)
                (Nats_eio.Jetstream.Consumer.Config.rate_limit
                   (Nats_eio.Jetstream.Consumer.Info.config preserved_info));
              equal (option int) (Some 2)
                (Nats_eio.Jetstream.Consumer.Config.replicas
                   (Nats_eio.Jetstream.Consumer.Info.config preserved_info));
              (match
                 Nats_eio.Jetstream.Consumer.Config.filter_subject
                   (Nats_eio.Jetstream.Consumer.Info.config preserved_info)
               with
              | None -> ()
              | Some _ ->
                  fail "consumer update response retained singular filter");
              equal (list string)
                [ "orders.created"; "orders.updated" ]
                (List.map Nats.Subject.Filter.to_string
                   (Nats_eio.Jetstream.Consumer.Config.filter_subjects
                      (Nats_eio.Jetstream.Consumer.Info.config preserved_info)));
              equal (list int64) [ 1_000_000L; 2_000_000L ]
                (List.map Mtime.Span.to_uint64_ns
                   (Nats_eio.Jetstream.Consumer.Config.backoff
                      (Nats_eio.Jetstream.Consumer.Info.config preserved_info)));
              (match
                 Nats_eio.Jetstream.Consumer.Config.metadata
                   (Nats_eio.Jetstream.Consumer.Info.config preserved_info)
               with
              | [ ("owner", "server") ] -> ()
              | _ -> fail "consumer update response lost preserved metadata");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer update retains modeled priority configuration" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let update_response, update_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await update_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ~description:"updated" ~priority_groups:[ "blue" ]
                     ~priority_policy:
                       Nats_eio.Jetstream.Consumer.Config.Pinned_client
                     ~priority_timeout:Mtime.Span.(30 * s)
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.update (consumer connection)
                       config));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"before","priority_groups":["blue"],"priority_policy":"pinned_client","priority_timeout":120000000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","future_field":true}}|}));
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring
                     ~needle:"priority_groups\\\":[\\\"blue\\\"]" trace)
              then fail "consumer update dropped priority groups";
              if
                not
                  (contains_substring
                     ~needle:"priority_policy\\\":\\\"pinned_client" trace)
              then fail "consumer update dropped priority policy";
              if
                not
                  (contains_substring ~needle:"priority_timeout\\\":30000000000"
                     trace)
              then fail "consumer update dropped priority timeout";
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","description":"updated","priority_groups":["blue"],"priority_policy":"pinned_client","priority_timeout":30000000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              equal (list string) [ "blue" ]
                (Nats_eio.Jetstream.Consumer.Config.priority_groups
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer update rejects priority identity changes" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await info_response; `Await hold ]
            (fun ~sw ~trace connection ->
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ~priority_groups:[ "green" ]
                     ~priority_policy:
                       Nats_eio.Jetstream.Consumer.Config.Prioritized ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.update (consumer connection)
                       config));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","priority_groups":["blue"],"priority_policy":"pinned_client","priority_timeout":120000000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Invalid_config
                    Nats_eio.Jetstream.Error.Invalid_consumer_priority_update ->
                    true
                | _ -> false);
              let trace = Buffer.contents trace in
              if contains_substring ~needle:"action\\\":\\\"update" trace then
                fail "priority identity rejection sent an update";
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream purge sends a filtered request and returns the count"
        (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"OBJ_assets")
              in
              let filter = Nats.Subject.Filter.literal "$O.assets.C.nuid" in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.purge stream ~subject:filter));
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not (contains_substring ~needle:"STREAM.PURGE.OBJ_assets" trace)
              then fail "stream purge was not sent";
              if
                not
                  (contains_substring
                     ~needle:"filter\\\":\\\"$O.assets.C.nuid\\\"" trace)
              then fail "stream purge omitted the subject filter";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"success":true,"purged":3}|}));
              equal int64 3L (expect_jetstream_ok (Eio.Promise.await result));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream direct reads preserve stored message metadata" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await second_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Stream.get stream ~sequence:7L));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok
                   (direct_message_wire ~sid:1 ~stream:"ORDERS"
                      ~subject:"orders.created" ~sequence:7L
                      ~timestamp:"2026-08-11T12:00:00.000000000Z" "stored"));
              let first =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "orders.created"
                (Nats.Subject.to_string
                   (Nats_eio.Jetstream.Stream.Message.subject first));
              equal int64 7L (Nats_eio.Jetstream.Stream.Message.sequence first);
              equal string "2026-08-11T12:00:00.000000000Z"
                (Nats_eio.Jetstream.Stream.Message.timestamp first);
              equal string "stored"
                (Nats_eio.Jetstream.Stream.Message.payload first);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Stream.get_last stream
                       ~subject:(Nats.Subject.literal "orders.created")));
              yield_n 5;
              Eio.Promise.resolve second_response_u
                (Ok
                   (direct_message_wire ~sid:2 ~stream:"ORDERS"
                      ~subject:"orders.created" ~sequence:8L
                      ~timestamp:"2026-08-11T12:00:01.000000000Z" "latest"));
              let second =
                expect_jetstream_ok (Eio.Promise.await second_result)
              in
              equal int64 8L (Nats_eio.Jetstream.Stream.Message.sequence second);
              equal string "latest"
                (Nats_eio.Jetstream.Stream.Message.payload second);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull fails permanently when its consumer is deleted" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:409 ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Pull.next pull) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "fetch shares consumer deletion classification and cleans up"
        (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.fetch (consumer connection)
                       ~batch:1));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:409 ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push config retains its delivery subject and queue group" (fun () ->
          let subject = Nats.Subject.literal "orders.push" in
          let group = Nats.Queue_group.literal "workers" in
          let idle_heartbeat = Mtime.Span.(1 * ms) in
          let config =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.v ~deliver_subject:subject
                 ~deliver_group:group ~idle_heartbeat ~flow_control:true ())
          in
          (match Nats_eio.Jetstream.Consumer.Config.deliver_subject config with
          | Some value ->
              equal string "orders.push" (Nats.Subject.to_string value)
          | None -> fail "push config lost its delivery subject");
          (match Nats_eio.Jetstream.Consumer.Config.deliver_group config with
          | Some value ->
              equal string "workers" (Nats.Queue_group.to_string value)
          | None -> fail "push config lost its queue group");
          (match Nats_eio.Jetstream.Consumer.Config.idle_heartbeat config with
          | Some value -> equal int64 1_000_000L (Mtime.Span.to_uint64_ns value)
          | None -> fail "consumer config lost its idle heartbeat");
          match Nats_eio.Jetstream.Consumer.Config.flow_control config with
          | Some true -> ()
          | Some false -> fail "consumer config changed flow control"
          | None -> fail "consumer config lost flow control");
      test "push subscribes using the server consumer configuration" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              if
                not
                  (contains_substring
                     ~needle:"wrote \"SUB orders.push workers 2\\r\\n\""
                     (Buffer.contents trace))
              then
                fail "push did not use the configured subject and queue group";
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "push-payload"));
              let message =
                expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.next push)
              in
              equal string "push-payload"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Push.next push) (function
                | Nats_eio.Jetstream.Error.Push_closed -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "owned push creates after subscribing and deletes on close"
        (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace ~clock connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v
                     ~deliver_subject:(Nats.Subject.literal "orders.push")
                     ~deliver_policy:Nats_eio.Jetstream.Consumer.Config.All
                     ~ack_policy:Nats_eio.Jetstream.Consumer.Config.No_ack
                     ~inactive_threshold:Mtime.Span.(5 * min)
                     ~mem_storage:true ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.create ~sw stream config));
              yield_n 5;
              let trace_value = Buffer.contents trace in
              let subscription_position =
                match
                  substring_position ~needle:"wrote \"SUB orders.push 1\\r\\n\""
                    trace_value
                with
                | Some position -> position
                | None -> fail "owned push did not subscribe before create"
              in
              let create_position =
                match
                  substring_position
                    ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS"
                    trace_value
                with
                | Some position -> position
                | None -> fail "owned push did not create a consumer"
              in
              if Int.compare subscription_position create_position >= 0 then
                fail "owned push created its consumer before subscribing";
              Eio.Promise.resolve create_response_u
                (Ok (owned_push_consumer_info_wire_with_sid ~sid:2));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok (owned_push_consumer_info_wire_with_sid ~sid:3));
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "worker"
                (Nats_eio.Jetstream.Consumer.name
                   (Nats_eio.Jetstream.Consumer.Push.consumer push));
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Push.close push));
              yield_n 5;
              wait_for_trace ~clock ~trace
                ~needle:"PUB $JS.API.CONSUMER.DELETE.ORDERS.worker" ~count:1;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:4));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "owned push rejects durable consumer configs" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ~deliver_subject:(Nats.Subject.literal "orders.push")
                     ())
              in
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Push.create ~sw stream config)
                (function
                | Nats_eio.Jetstream.Error.Invalid_config
                    (Nats_eio.Jetstream.Error.Invalid_consumer_policy { field })
                  ->
                    String.equal field "durable_name"
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push restores its subscription after reconnect" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let restore_info, restore_info_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:
              [ `Return info_wire; `Await info_response; `Await disconnect ]
            ~second_reads:
              [
                `Await reconnect_info;
                `Await restore_info;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw ~trace ~clock connection ->
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok push_heartbeat_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              Eio.Time.Mono.sleep clock 0.005;
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"SUB orders.push workers 2\\r\\n\"" ~count:2;
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.INFO.ORDERS.worker"
                ~count:2;
              Eio.Promise.resolve restore_info_u
                (Ok (push_heartbeat_consumer_info_wire_with_sid ~sid:3));
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "after-reconnect"));
              let message =
                expect_jetstream_ok (Eio.Promise.await next_result)
              in
              equal string "after-reconnect"
                (Nats_eio.Jetstream.Msg.payload message);
              (match
                 expect_core_event
                   (Nats_eio.Event_stream.next
                      (Nats_eio.Connection.events connection))
               with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected initial INFO, got %a"
                       Nats.Event.pp event));
              let events = Nats_eio.Connection.events connection in
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected initial CONNECTED, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Closed -> ()
              | event ->
                  fail
                    (Format.asprintf "expected disconnect CLOSED, got %a"
                       Nats.Event.pp event));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Disconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected disconnect lifecycle, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected disconnect event, got %a"
                       Nats_eio.Error.pp error));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Info _ -> ()
              | event ->
                  fail
                    (Format.asprintf "expected reconnect INFO, got %a"
                       Nats.Event.pp event));
              (match expect_core_event (Nats_eio.Event_stream.next events) with
              | Nats.Event.Connected -> ()
              | event ->
                  fail
                    (Format.asprintf "expected reconnect CONNECTED, got %a"
                       Nats.Event.pp event));
              (match Nats_eio.Event_stream.next events with
              | Ok Nats_eio.Event.Reconnected -> ()
              | Ok event ->
                  fail
                    (Format.asprintf "expected reconnected event, got %a"
                       Nats_eio.Event.pp event)
              | Error error ->
                  fail
                    (Format.asprintf "expected reconnected lifecycle, got %a"
                       Nats_eio.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push restores again after its delivery subject changes" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let disconnect_first, disconnect_first_u = Eio.Promise.create () in
          let reconnect_info_first, reconnect_info_first_u =
            Eio.Promise.create ()
          in
          let restore_info_first, restore_info_first_u =
            Eio.Promise.create ()
          in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let disconnect_second, disconnect_second_u = Eio.Promise.create () in
          let reconnect_info_second, reconnect_info_second_u =
            Eio.Promise.create ()
          in
          let restore_info_second, restore_info_second_u =
            Eio.Promise.create ()
          in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:
              [
                `Return info_wire; `Await info_response; `Await disconnect_first;
              ]
            ~second_reads:
              [
                `Await reconnect_info_first;
                `Await restore_info_first;
                `Await first_delivery;
                `Await disconnect_second;
              ]
            ~third_reads:
              [
                `Await reconnect_info_second;
                `Await restore_info_second;
                `Await second_delivery;
                `Await hold;
              ]
            (fun ~sw ~trace ~clock connection ->
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve disconnect_first_u (Error End_of_file);
              wait_for_trace ~clock ~trace
                ~needle:"jetstream-reconnect-network: connect to tcp" ~count:2;
              Eio.Promise.resolve reconnect_info_first_u (Ok info_wire);
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"SUB orders.push workers 2\\r\\n\"" ~count:2;
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.INFO.ORDERS.worker"
                ~count:2;
              Eio.Promise.resolve restore_info_first_u
                (Ok
                   (push_consumer_info_wire_with_sid_and_subject ~sid:3
                      ~subject:"orders.push.changed"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"SUB orders.push.changed workers " ~count:1;
              Eio.Promise.resolve first_delivery_u
                (Ok (delivery_wire_with_sid ~sid:4 "after-subject-change"));
              let first_message =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "after-subject-change"
                (Nats_eio.Jetstream.Msg.payload first_message);
              Eio.Promise.resolve disconnect_second_u (Error End_of_file);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next_with_timeout
                       ~timeout:Mtime.Span.(50 * ms)
                       push));
              wait_for_trace ~clock ~trace
                ~needle:"jetstream-reconnect-network: connect to tcp" ~count:3;
              Eio.Promise.resolve reconnect_info_second_u (Ok info_wire);
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.INFO.ORDERS.worker"
                ~count:3;
              Eio.Promise.resolve restore_info_second_u
                (Ok
                   (push_consumer_info_wire_with_sid_and_subject ~sid:5
                      ~subject:"orders.push.changed"));
              Eio.Promise.resolve second_delivery_u
                (Ok (delivery_wire_with_sid ~sid:4 "after-second-reconnect"));
              let second_message =
                expect_jetstream_ok (Eio.Promise.await second_result)
              in
              equal string "after-second-reconnect"
                (Nats_eio.Jetstream.Msg.payload second_message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push reports a deleted durable consumer after reconnect" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let restore_info, restore_info_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:
              [ `Return info_wire; `Await info_response; `Await disconnect ]
            ~second_reads:
              [ `Await reconnect_info; `Await restore_info; `Await hold ]
            (fun ~sw ~trace ~clock connection ->
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              wait_for_trace ~clock ~trace
                ~needle:"jetstream-reconnect-network: connect to tcp" ~count:2;
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.INFO.ORDERS.worker"
                ~count:2;
              Eio.Promise.resolve restore_info_u
                (Ok (consumer_not_found_wire ~sid:3));
              expect_jetstream_error (Eio.Promise.await next_result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push recreates an ephemeral consumer after reconnect" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let pause_response, pause_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let restore_info, restore_info_u = Eio.Promise.create () in
          let create_response, create_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await pause_response;
                `Await first_delivery;
                `Await disconnect;
              ]
            ~second_reads:
              [
                `Await reconnect_info;
                `Await restore_info;
                `Await create_response;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw ~trace ~clock connection ->
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","filter_subjects":["orders.created","orders.updated"],"backoff":[1000000,2000000],"sample_freq":"10%","rate_limit_bps":64000,"num_replicas":2,"metadata":{"owner":"server"},"replay_policy":"instant"}}|}));
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              let pause_until =
                ptime_of_rfc3339 "2026-08-13T12:00:00.000000000Z"
              in
              let pause_result, pause_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve pause_result_u
                    (Nats_eio.Jetstream.Consumer.pause
                       (Nats_eio.Jetstream.Consumer.Push.consumer push)
                       ~until:pause_until));
              yield_n 5;
              Eio.Promise.resolve pause_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:3
                      {|{"paused":true,"pause_until":"2026-08-13T12:00:00.000000000Z","pause_remaining":60000000000}|}));
              ignore (expect_jetstream_ok (Eio.Promise.await pause_result));
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "before-reconnect"));
              let first =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "before-reconnect"
                (Nats_eio.Jetstream.Msg.payload first);
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.INFO.ORDERS.worker"
                ~count:2;
              Eio.Promise.resolve restore_info_u
                (Ok (consumer_not_found_wire ~sid:4));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:1;
              if
                not
                  (contains_substring
                     ~needle:"deliver_policy\\\":\\\"by_start_sequence"
                     (Buffer.contents trace))
              then fail "ephemeral push did not resume by stream sequence";
              if
                not
                  (contains_substring ~needle:"opt_start_seq\\\":2"
                     (Buffer.contents trace))
              then fail "ephemeral push resumed from the wrong sequence";
              if
                not
                  (contains_substring
                     ~needle:
                       "filter_subjects\\\":[\\\"orders.created\\\",\\\"orders.updated\\\"]"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped plural filters on recreate";
              if
                not
                  (contains_substring ~needle:"backoff\\\":[1000000,2000000]"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped backoff on recreate";
              if
                not
                  (contains_substring ~needle:"2026-08-13T12:00:00.000000000Z"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped pause deadline on recreate";
              if
                not
                  (contains_substring ~needle:"sample_freq\\\":\\\"10%"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped sample frequency on recreate";
              if
                not
                  (contains_substring ~needle:"rate_limit_bps\\\":64000"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped rate limit on recreate";
              if
                not
                  (contains_substring ~needle:"num_replicas\\\":2"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped replica count on recreate";
              if
                not
                  (contains_substring
                     ~needle:"metadata\\\":{\\\"owner\\\":\\\"server\\\"}"
                     (Buffer.contents trace))
              then fail "ephemeral push dropped metadata on recreate";
              Eio.Promise.resolve create_response_u
                (Ok (ephemeral_push_create_wire_with_sid ~sid:5));
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "after-ephemeral-recreate"));
              let message =
                expect_jetstream_ok (Eio.Promise.await next_result)
              in
              equal string "after-ephemeral-recreate"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push timeout during reconnect leaves restoration available"
        (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let restore_info, restore_info_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:
              [ `Return info_wire; `Await info_response; `Await disconnect ]
            ~second_reads:
              [
                `Await reconnect_info;
                `Await restore_info;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw ~trace ~clock connection ->
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              let timeout_result, timeout_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve timeout_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next_with_timeout
                       ~timeout:Mtime.Span.(1 * ms)
                       push));
              yield_n 5;
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              (match Eio.Promise.await timeout_result with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "push reconnect timeout returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected push reconnect timeout: %a"
                       Nats_eio.Jetstream.Error.pp error));
              wait_for_trace ~clock ~trace
                ~needle:"jetstream-reconnect-network: connect to tcp" ~count:2;
              yield_n 5;
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.INFO.ORDERS.worker"
                ~count:2;
              Eio.Promise.resolve restore_info_u
                (Ok (push_consumer_info_wire_with_sid ~sid:3));
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "after-timeout"));
              let message =
                expect_jetstream_ok (Eio.Promise.await next_result)
              in
              equal string "after-timeout"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push rejects a pull consumer configuration" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await info_response; `Await hold ]
            (fun ~sw connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok pull_consumer_info_wire);
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Not_push_consumer -> true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push consumes heartbeats and answers stalled heartbeats" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let heartbeat, heartbeat_u = Eio.Promise.create () in
          let stalled_heartbeat, stalled_heartbeat_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await heartbeat;
                `Await stalled_heartbeat;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok push_heartbeat_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve heartbeat_u
                (Ok
                   (status_wire_with_sid ~sid:2 ~code:100
                      ~description:"Idle Heartbeat"));
              yield_n 5;
              Eio.Promise.resolve stalled_heartbeat_u
                (Ok
                   (control_status_wire ~sid:2 ~reply_to:"$JS.FC.ORDERS.token"
                      ~description:"Idle Heartbeat"));
              yield_n 5;
              if
                not
                  (contains_substring
                     ~needle:"wrote \"PUB $JS.FC.ORDERS.token 0\\r\\n\""
                     (Buffer.contents trace))
              then fail "push did not answer a stalled heartbeat";
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "after-heartbeat"));
              let message =
                expect_jetstream_ok (Eio.Promise.await next_result)
              in
              equal string "after-heartbeat"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push answers flow-control requests" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let flow_control, flow_control_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await flow_control;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok push_flow_control_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve flow_control_u
                (Ok
                   (control_status_wire ~sid:2 ~reply_to:"$JS.FC.ORDERS.token"
                      ~description:"FlowControl Request"));
              yield_n 5;
              if
                not
                  (contains_substring
                     ~needle:"wrote \"PUB $JS.FC.ORDERS.token 0\\r\\n\""
                     (Buffer.contents trace))
              then fail "push did not answer a flow-control request";
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "after-flow-control"));
              let message =
                expect_jetstream_ok (Eio.Promise.await next_result)
              in
              equal string "after-flow-control"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push reports a missing heartbeat" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await info_response; `Await hold ]
            (fun ~sw connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok push_heartbeat_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Push.next_with_timeout
                   ~timeout:Mtime.Span.(10 * ms)
                   push)
                (function
                  | Nats_eio.Jetstream.Error.Missing_heartbeat -> true
                  | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push timeout leaves its subscription available" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await delivery;
                `Await hold;
              ]
            (fun ~sw connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              (match
                 Nats_eio.Jetstream.Consumer.Push.next_with_timeout
                   ~timeout:Mtime.Span.(1 * ms)
                   push
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "push timeout unexpectedly returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected push timeout: %a"
                       Nats_eio.Jetstream.Error.pp error));
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "resumed"));
              let message =
                expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.next push)
              in
              equal string "resumed" (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push reports consumer deletion and fails permanently" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await info_response;
                `Await response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Push.next push));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (status_wire_with_sid ~sid:2 ~code:409
                      ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await next_result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Push.next push) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered consumer tracks consumer order across stream gaps"
        (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await first_delivery;
                `Await second_delivery;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let ordered_result, ordered_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ordered_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.v ~sw ~batch:1
                       ~expires:Mtime.Span.(10 * ms)
                       ~idle_heartbeat:Mtime.Span.(1 * ms)
                       ~filter_subject:
                         (Nats.Subject.Filter.literal "orders.created")
                       stream));
              yield_n 5;
              let create_wire =
                ordered_create_wire ~sid:1 ~name:"ordered-1"
                  ~deliver_policy:"all" ()
              in
              Eio.Promise.resolve create_response_u (Ok create_wire);
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              if
                not
                  (contains_substring
                     ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS"
                     (Buffer.contents trace))
              then fail "ordered consumer did not create an ephemeral consumer";
              if
                not
                  (contains_substring ~needle:"ack_policy\\\":\\\"none"
                     (Buffer.contents trace))
              then fail "ordered consumer did not force no acknowledgements";
              if
                not
                  (contains_substring ~needle:"mem_storage\\\":true"
                     (Buffer.contents trace))
              then fail "ordered consumer did not force memory storage";
              if
                not
                  (contains_substring
                     ~needle:"inactive_threshold\\\":300000000000"
                     (Buffer.contents trace))
              then fail "ordered consumer did not set an inactive threshold";
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered-1"
                      ~stream_sequence:10L ~consumer_sequence:1L "first"));
              let first =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "first" (Nats_eio.Jetstream.Msg.payload first);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered-1"
                      ~stream_sequence:12L ~consumer_sequence:2L "filtered"));
              let second =
                expect_jetstream_ok (Eio.Promise.await second_result)
              in
              equal string "filtered" (Nats_eio.Jetstream.Msg.payload second);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              yield_n 5;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:3));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Ordered.next ordered) (function
                | Nats_eio.Jetstream.Error.Ordered_closed -> true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered consumer recreates after a consumer sequence gap" (fun () ->
          let first_create, first_create_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let gap_delivery, gap_delivery_u = Eio.Promise.create () in
          let first_delete, first_delete_u = Eio.Promise.create () in
          let second_create, second_create_u = Eio.Promise.create () in
          let replay_delivery, replay_delivery_u = Eio.Promise.create () in
          let second_delete, second_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_create;
                `Await first_delivery;
                `Await gap_delivery;
                `Await first_delete;
                `Await second_create;
                `Await replay_delivery;
                `Await second_delete;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let ordered_result, ordered_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ordered_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.v ~sw ~batch:1
                       ~expires:Mtime.Span.(10 * ms)
                       ~idle_heartbeat:Mtime.Span.(1 * ms)
                       stream));
              yield_n 5;
              Eio.Promise.resolve first_create_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered-1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered-1"
                      ~stream_sequence:10L ~consumer_sequence:1L "before-gap"));
              let first =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "before-gap" (Nats_eio.Jetstream.Msg.payload first);
              let replay_result, replay_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve replay_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve gap_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered-1"
                      ~stream_sequence:12L ~consumer_sequence:3L "gap"));
              yield_n 5;
              Eio.Promise.resolve first_delete_u (Ok (api_ok_wire ~sid:3));
              yield_n 5;
              Eio.Promise.resolve second_create_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered-2"
                      ~deliver_policy:"by_start_sequence" ~opt_start_seq:11L ()));
              yield_n 5;
              if
                not
                  (contains_substring ~needle:"\\\"opt_start_seq\\\":11"
                     (Buffer.contents trace))
              then
                fail "ordered reset did not resume at the next stream sequence";
              Eio.Promise.resolve replay_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered-2"
                      ~stream_sequence:11L ~consumer_sequence:1L "replayed"));
              let replayed =
                expect_jetstream_ok (Eio.Promise.await replay_result)
              in
              equal string "replayed" (Nats_eio.Jetstream.Msg.payload replayed);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              yield_n 5;
              Eio.Promise.resolve second_delete_u (Ok (api_ok_wire ~sid:6));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered consumer recreates after a missing heartbeat" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let recreated, recreated_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await delete_response;
                `Await recreated;
                `Await delivery;
                `Await final_delete;
                `Await hold;
              ]
            (fun ~sw ~trace ~clock connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let ordered_result, ordered_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ordered_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.v ~sw ~batch:1
                       ~expires:Mtime.Span.(10 * ms)
                       ~idle_heartbeat:Mtime.Span.(1 * ms)
                       stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered-1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered-1"
                ~count:1;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:3));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:2;
              Eio.Promise.resolve recreated_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered-2"
                      ~deliver_policy:"all" ()));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.ordered-2"
                ~count:1;
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered-2"
                      ~stream_sequence:1L ~consumer_sequence:1L "after-reset"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "after-reset"
                (Nats_eio.Jetstream.Msg.payload message);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered-2"
                ~count:1;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:6));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered consumer recreates after consumer deletion" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let deletion, deletion_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let recreated, recreated_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await deletion;
                `Await delete_response;
                `Await recreated;
                `Await delivery;
                `Await final_delete;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let ordered_result, ordered_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ordered_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.v ~sw ~batch:1
                       ~expires:Mtime.Span.(10 * ms)
                       ~idle_heartbeat:Mtime.Span.(1 * ms)
                       stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered-1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve deletion_u
                (Ok
                   (status_wire_with_sid ~sid:2 ~code:409
                      ~description:"Consumer Deleted"));
              yield_n 5;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:3));
              yield_n 5;
              Eio.Promise.resolve recreated_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered-2"
                      ~deliver_policy:"all" ()));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered-2"
                      ~stream_sequence:1L ~consumer_sequence:1L "after-delete"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "after-delete"
                (Nats_eio.Jetstream.Msg.payload message);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              yield_n 5;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:6));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered timeout leaves the current consumer open" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await delivery;
                `Await delete_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let ordered_result, ordered_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ordered_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.v ~sw ~batch:1
                       ~expires:Mtime.Span.(10 * ms)
                       ~idle_heartbeat:Mtime.Span.(1 * ms)
                       stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered-1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              (match
                 Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout
                   ~timeout:Mtime.Span.(1 * ms)
                   ordered
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "ordered timeout unexpectedly returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected ordered timeout: %a"
                       Nats_eio.Jetstream.Error.pp error));
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered-1"
                      ~stream_sequence:1L ~consumer_sequence:1L "after-timeout"));
              let message =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Ordered.next ordered)
              in
              equal string "after-timeout"
                (Nats_eio.Jetstream.Msg.payload message);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              yield_n 5;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:3));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ack_sync waits for the server acknowledgement" (fun () ->
          let delivery, delivery_u = Eio.Promise.create () in
          let ack_response, ack_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await delivery;
                `Await ack_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve delivery_u (Ok (delivery_wire "payload"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              let ack_result, ack_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ack_result_u
                    (Nats_eio.Jetstream.Msg.ack_sync message));
              yield_n 5;
              Eio.Promise.resolve ack_response_u (Ok (ack_response_wire ~sid:2));
              expect_jetstream_ok (Eio.Promise.await ack_result);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ack_sync reports no responders" (fun () ->
          let delivery, delivery_u = Eio.Promise.create () in
          let ack_response, ack_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await delivery;
                `Await ack_response;
                `Await hold;
              ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve delivery_u (Ok (delivery_wire "payload"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              let ack_result, ack_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ack_result_u
                    (Nats_eio.Jetstream.Msg.ack_sync message));
              yield_n 5;
              Eio.Promise.resolve ack_response_u
                (Ok
                   (status_wire_with_sid ~sid:2 ~code:503
                      ~description:"No Responders"));
              expect_jetstream_error (Eio.Promise.await ack_result) (function
                | Nats_eio.Jetstream.Error.Connection
                    Nats_eio.Error.No_responders ->
                    true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ack_sync timeout is structured" (fun () ->
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await delivery; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve delivery_u (Ok (delivery_wire "payload"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              expect_jetstream_error
                (Nats_eio.Jetstream.Msg.ack_sync
                   ~timeout:Mtime.Span.(1 * ms)
                   message)
                (function
                  | Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout
                    ->
                      true
                  | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "delivery without an acknowledgement reply is rejected" (fun () ->
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await delivery; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok (delivery_without_ack_wire "payload"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Missing_ack_reply -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "queued heartbeat control is processed before deletion" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let config = Mtime.Span.(1 * ms) in
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(10 * ms)
                     ~idle_heartbeat:config (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (status_wire ~code:100 ~description:"Idle Heartbeat"
                   ^ status_wire ~code:409 ~description:"Consumer Deleted"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Consumer_deleted -> true
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "non-heartbeat status is not treated as idle heartbeat" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(10 * ms)
                     ~idle_heartbeat:Mtime.Span.(1 * ms)
                     (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:100 ~description:"Flow Control Request"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Unexpected_status
                    { code; description } ->
                    Int.equal code 100
                    && String.equal description "Flow Control Request"
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull retries an expired batch" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:
              [
                `Return info_wire;
                `Await response;
                `Return (delivery_wire "payload");
                `Await hold;
              ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:408 ~description:"Request Timeout"));
              (match Eio.Promise.await result with
              | Ok message ->
                  equal string "payload"
                    (Nats_eio.Jetstream.Msg.payload message)
              | Error error ->
                  fail
                    (Format.asprintf "expired pull did not retry: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull reports max-bytes conflicts" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (status_wire ~code:409
                      ~description:"message size exceeds maxbytes"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Conflict { code; description } ->
                    Int.equal code 409
                    && String.equal description "message size exceeds maxbytes"
                | _ -> false);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "one-shot fetch retains a pinned priority id" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await second_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let consumer = consumer connection in
              let headers =
                match Nats.Header.of_list [ ("Nats-Pin-Id", "pin-fetch") ] with
                | Ok headers -> headers
                | Error error ->
                    fail (Format.asprintf "%a" Nats.Header.pp_error error)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1
                       ~group:"blue" ~priority:4));
              yield_n 5;
              let first_trace = Buffer.contents trace in
              if
                not
                  (Int.equal
                     (count_substring ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker"
                        first_trace)
                     1)
              then fail "one-shot fetch did not send its request";
              if
                not
                  (Int.equal
                     (count_substring ~needle:"id\\\":\\\"pin-fetch" first_trace)
                     0)
              then fail "one-shot fetch sent a pin id too early";
              Eio.Promise.resolve first_response_u
                (Ok (delivery_wire_with_headers ~sid:1 ~headers "first-fetch"));
              (match expect_jetstream_ok (Eio.Promise.await result) with
              | [ message ] ->
                  equal string "first-fetch"
                    (Nats_eio.Jetstream.Msg.payload message)
              | _ -> fail "one-shot fetch returned the wrong first batch");
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1
                       ~group:"blue" ~priority:4));
              yield_n 5;
              let second_trace = Buffer.contents trace in
              if
                not
                  (Int.equal
                     (count_substring ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker"
                        second_trace)
                     2)
              then fail "one-shot fetch did not issue its second request";
              if
                not
                  (Int.equal
                     (count_substring ~needle:"id\\\":\\\"pin-fetch"
                        second_trace)
                     1)
              then fail "one-shot fetch did not retain its pin id";
              Eio.Promise.resolve second_response_u
                (Ok (delivery_wire_with_sid ~sid:2 "second-fetch"));
              (match expect_jetstream_ok (Eio.Promise.await result) with
              | [ message ] ->
                  equal string "second-fetch"
                    (Nats_eio.Jetstream.Msg.payload message)
              | _ -> fail "one-shot fetch returned the wrong second batch");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull carries priority options and recovers a lost pin" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let pin_lost, pin_lost_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await pin_lost;
                `Await second_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let headers =
                match Nats.Header.of_list [ ("Nats-Pin-Id", "pin-1") ] with
                | Ok headers -> headers
                | Error error ->
                    fail (Format.asprintf "%a" Nats.Header.pp_error error)
              in
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw ~batch:1 ~group:"blue"
                     ~min_pending:2L ~min_ack_pending:3L ~priority:4
                     (consumer connection))
              in
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              let first_trace = Buffer.contents trace in
              if
                not
                  (Int.equal
                     (count_substring ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker"
                        first_trace)
                     1)
              then fail "pull did not send its first priority request";
              if
                not
                  (Int.equal
                     (count_substring ~needle:"id\\\":\\\"pin-1" first_trace)
                     0)
              then fail "pull sent a pin id before receiving one";
              if
                not
                  (contains_substring ~needle:"group\\\":\\\"blue" first_trace)
              then fail "pull omitted its priority group";
              if
                not (contains_substring ~needle:"min_pending\\\":2" first_trace)
              then fail "pull omitted its pending threshold";
              if
                not
                  (contains_substring ~needle:"min_ack_pending\\\":3"
                     first_trace)
              then fail "pull omitted its ack-pending threshold";
              if not (contains_substring ~needle:"priority\\\":4" first_trace)
              then fail "pull omitted its priority";
              Eio.Promise.resolve first_response_u
                (Ok (delivery_wire_with_headers ~sid:1 ~headers "first"));
              let first =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "first" (Nats_eio.Jetstream.Msg.payload first);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Consumer.Pull.next pull));
              yield_n 5;
              let second_trace = Buffer.contents trace in
              if
                not
                  (Int.equal
                     (count_substring ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker"
                        second_trace)
                     2)
              then fail "pull did not issue its second request";
              if
                not
                  (Int.equal
                     (count_substring ~needle:"id\\\":\\\"pin-1" second_trace)
                     1)
              then fail "pull did not echo the server pin id";
              Eio.Promise.resolve pin_lost_u
                (Ok
                   (status_wire_with_sid ~sid:1 ~code:423
                      ~description:"Nats-Pin-Id mismatch"));
              yield_n 5;
              let third_trace = Buffer.contents trace in
              if
                not
                  (Int.equal
                     (count_substring ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker"
                        third_trace)
                     3)
              then fail "pull did not retry after losing its pin";
              if
                not
                  (Int.equal
                     (count_substring ~needle:"id\\\":\\\"pin-1" third_trace)
                     1)
              then fail "pull retained a stale pin id after a mismatch";
              Eio.Promise.resolve second_response_u
                (Ok (delivery_wire_with_sid ~sid:1 "second"));
              let second =
                expect_jetstream_ok (Eio.Promise.await second_result)
              in
              equal string "second" (Nats_eio.Jetstream.Msg.payload second);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "pull timeout leaves its outstanding request available" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(1 * ms)
                   pull
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "pull timeout unexpectedly returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected pull timeout: %a"
                       Nats_eio.Jetstream.Error.pp error));
              Eio.Promise.resolve response_u (Ok (delivery_wire "resumed"));
              (match Nats_eio.Jetstream.Consumer.Pull.next pull with
              | Ok message ->
                  equal string "resumed"
                    (Nats_eio.Jetstream.Msg.payload message)
              | Error error ->
                  fail
                    (Format.asprintf "timed-out pull did not resume: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "idle heartbeat silence fails a pull distinctly" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(10 * ms)
                     ~idle_heartbeat:Mtime.Span.(1 * ms)
                     (consumer connection))
              in
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(10 * ms)
                   pull
               with
              | Error Nats_eio.Jetstream.Error.Missing_heartbeat -> ()
              | Ok _ -> fail "silent heartbeat pull returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected heartbeat failure: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve response_u (Error End_of_file);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
