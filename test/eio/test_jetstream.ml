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

let api_error_wire ~sid ~code ~err_code ~description =
  let payload =
    Format.asprintf {|{"error":{"code":%d,"err_code":%d,"description":%S}}|}
      code err_code description
  in
  consumer_info_wire_with_sid ~sid payload

let push_consumer_info_wire =
  consumer_info_wire
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let push_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let owned_push_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"none","replay_policy":"instant"}}|}

let owned_push_create_wire_with_sid ~sid ~num_pending
    ~delivered_consumer_sequence =
  let payload =
    Format.asprintf
      {|{"stream_name":"ORDERS","name":"worker","delivered":{"consumer_seq":%Ld,"stream_seq":%Ld},"num_pending":%Ld,"config":{"deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"none","replay_policy":"instant"}}|}
      delivered_consumer_sequence delivered_consumer_sequence num_pending
  in
  consumer_info_wire_with_sid ~sid payload

let push_consumer_info_wire_with_sid_and_subject ~sid ~subject =
  let payload =
    Format.asprintf
      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"%s","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}
      subject
  in
  consumer_info_wire_with_sid ~sid payload

let push_heartbeat_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","idle_heartbeat":100000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let ephemeral_push_consumer_info_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"name":"worker","deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let ephemeral_push_create_wire_with_sid ~sid =
  consumer_info_wire_with_sid ~sid
    {|{"stream_name":"ORDERS","name":"worker","config":{"name":"worker","deliver_subject":"orders.push","deliver_group":"workers","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

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
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","idle_heartbeat":100000000,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

let push_flow_control_consumer_info_wire =
  consumer_info_wire
    {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_subject":"orders.push","deliver_group":"workers","idle_heartbeat":1000000000,"flow_control":true,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}

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

let ordered_heartbeat_wire ~sid ~consumer_sequence ~stream_sequence =
  let headers =
    match
      Nats.Header.of_list
        [
          ("Nats-Last-Consumer", Int64.to_string consumer_sequence);
          ("Nats-Last-Stream", Int64.to_string stream_sequence);
        ]
    with
    | Ok headers -> headers
    | Error error -> fail (Format.asprintf "%a" Nats.Header.pp_error error)
  in
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ~headers ""
  in
  operation_wire
    (Nats.Op.Hmsg
       {
         sid;
         message;
         status = Some { code = 100; description = "Idle Heartbeat" };
       })

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
          ("Nats-Stream", stream);
          ("Nats-Sequence", sequence_value);
          ("Nats-Time-Stamp", timestamp);
          ("Nats-Subject", subject);
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

let publish_ack_wire ~sid ~stream ~sequence =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "_INBOX.reply")
      (Format.asprintf "{\"stream\":%S,\"seq\":%Ld}" stream sequence)
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let no_responders_wire ~sid =
  let message =
    Nats.Message.v ~subject:(Nats.Subject.literal "_INBOX.reply") ""
  in
  operation_wire
    (Nats.Op.Hmsg
       {
         sid;
         message;
         status = Some { Nats.Op.code = 503; description = "no responders" };
       })

let publish_batch_ack_wire ~sid ~stream ~sequence ~batch ~count =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "_INBOX.reply")
      (Format.asprintf "{\"stream\":%S,\"seq\":%Ld,\"batch\":%S,\"count\":%d}"
         stream sequence batch count)
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let batch_flow_ack_wire ~sid ~sequence ~messages =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "_INBOX.reply")
      (Format.asprintf "{\"type\":\"ack\",\"seq\":%Ld,\"msgs\":%d}" sequence
         messages)
  in
  operation_wire (Nats.Op.Hmsg { sid; message; status = None })

let batch_flow_gap_wire ~sid ~expected ~actual =
  let message =
    Nats.Message.v
      ~subject:(Nats.Subject.literal "_INBOX.reply")
      (Format.asprintf "{\"type\":\"gap\",\"last_seq\":%Ld,\"seq\":%Ld}"
         expected actual)
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

let with_connection_traced ?config ?on_trace ~reads f =
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
              Buffer.add_char trace '\n';
              Option.iter (fun callback -> callback message) on_trace)
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

let wait_for_trace_count ~trace ~needle ~count =
  let seen = ref 0 in
  let attempts = ref 0 in
  while !seen < count && !attempts < 100 do
    Eio.Fiber.yield ();
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
                 ~allow_msg_ttl:true ~allow_msg_counter:true
                 ~allow_atomic_publish:true ~allow_msg_schedules:true
                 ~persist_mode:Nats_eio.Jetstream.Stream.Config.Async
                 ~allow_batch_publish:true
                 ~subject_delete_marker_ttl:Mtime.Span.(2 * s)
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
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.allow_msg_ttl config);
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.allow_msg_counter config);
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.allow_atomic_publish config);
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.allow_msg_schedules config);
          (match Nats_eio.Jetstream.Stream.Config.persist_mode config with
          | Nats_eio.Jetstream.Stream.Config.Async -> ()
          | Nats_eio.Jetstream.Stream.Config.Default ->
              fail "stream config lost async persistence mode");
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.allow_batch_publish config);
          (match
             Nats_eio.Jetstream.Stream.Config.subject_delete_marker_ttl config
           with
          | Some value ->
              equal bool true (Mtime.Span.equal value Mtime.Span.(2 * s))
          | None -> fail "stream config lost subject delete marker TTL");
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
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_msg_ttl config false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_msg_ttl updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_msg_counter config
                 false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_msg_counter updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_atomic_publish config
                 false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_atomic_publish updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_msg_schedules config
                 false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_msg_schedules updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_persist_mode config
                 Nats_eio.Jetstream.Stream.Config.Default)
          in
          (match Nats_eio.Jetstream.Stream.Config.persist_mode updated with
          | Nats_eio.Jetstream.Stream.Config.Default -> ()
          | Nats_eio.Jetstream.Stream.Config.Async ->
              fail "stream config updater retained async persistence mode");
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_allow_batch_publish config
                 false)
          in
          equal bool false
            (Nats_eio.Jetstream.Stream.Config.allow_batch_publish updated);
          let updated =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_subject_delete_marker_ttl
                 config None)
          in
          (match
             Nats_eio.Jetstream.Stream.Config.subject_delete_marker_ttl updated
           with
          | None -> ()
          | Some _ -> fail "stream config updater retained marker TTL");
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
      test "stream policy controls match the server configuration model"
        (fun () ->
          let subject = Nats.Subject.Filter.literal "orders.>" in
          let duplicate_window = Mtime.Span.of_uint64_ns 2_000_000_000L in
          let inactive_threshold = Mtime.Span.of_uint64_ns 30_000_000_000L in
          let consumer_limits =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.Consumer_limits.v
                 ~inactive_threshold ~max_ack_pending:1000 ())
          in
          let config =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS"
                 ~subjects:[ subject ]
                 ~discard:Nats_eio.Jetstream.Stream.Config.New
                 ~max_msgs_per_subject:10L ~max_consumers:12
                 ~discard_new_per_subject:true ~no_ack:true ~duplicate_window
                 ~deny_purge:true ~first_sequence:3L ~consumer_limits ())
          in
          equal (option int) (Some 12)
            (Nats_eio.Jetstream.Stream.Config.max_consumers config);
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.discard_new_per_subject config);
          equal bool true (Nats_eio.Jetstream.Stream.Config.no_ack config);
          (match Nats_eio.Jetstream.Stream.Config.duplicate_window config with
          | Some value ->
              equal bool true (Mtime.Span.equal value duplicate_window)
          | None -> fail "stream config lost duplicate window");
          equal bool true (Nats_eio.Jetstream.Stream.Config.deny_purge config);
          equal (option int64) (Some 3L)
            (Nats_eio.Jetstream.Stream.Config.first_sequence config);
          (match Nats_eio.Jetstream.Stream.Config.consumer_limits config with
          | None -> fail "stream config lost consumer limits"
          | Some value ->
              (match
                 Nats_eio.Jetstream.Stream.Config.Consumer_limits
                 .inactive_threshold value
               with
              | Some span ->
                  equal bool true (Mtime.Span.equal span inactive_threshold)
              | None -> fail "stream config lost inactive threshold");
              equal (option int) (Some 1000)
                (Nats_eio.Jetstream.Stream.Config.Consumer_limits
                 .max_ack_pending value));
          let cleared =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_max_consumers config None)
          in
          equal (option int) None
            (Nats_eio.Jetstream.Stream.Config.max_consumers cleared);
          let cleared =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_duplicate_window cleared
                 None)
          in
          (match Nats_eio.Jetstream.Stream.Config.duplicate_window cleared with
          | None -> ()
          | Some _ -> fail "stream updater retained duplicate window");
          let cleared =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_first_sequence cleared None)
          in
          equal (option int64) None
            (Nats_eio.Jetstream.Stream.Config.first_sequence cleared);
          let cleared =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_consumer_limits cleared
                 None)
          in
          (match Nats_eio.Jetstream.Stream.Config.consumer_limits cleared with
          | None -> ()
          | Some _ -> fail "stream updater retained consumer limits");
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid"
               ~subjects:[ subject ]
               ~duplicate_window:(Mtime.Span.of_uint64_ns 50_000_000L)
               ()
           with
          | Error Nats_eio.Jetstream.Error.Invalid_duplicate_window -> ()
          | Ok _ -> fail "stream accepted a duplicate window below 100ms"
          | Error error ->
              fail
                (Format.asprintf "unexpected duplicate-window error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid"
               ~subjects:[ subject ] ~discard_new_per_subject:true ()
           with
          | Error Nats_eio.Jetstream.Error.Invalid_discard_new_per_subject -> ()
          | Ok _ ->
              fail "stream accepted discard-new-per-subject without a limit"
          | Error error ->
              fail
                (Format.asprintf "unexpected discard policy error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid"
               ~subjects:[ subject ] ~allow_rollup:true ~deny_purge:true ()
           with
          | Error Nats_eio.Jetstream.Error.Deny_purge_and_rollup -> ()
          | Ok _ -> fail "stream accepted deny-purge with rollup headers"
          | Error error ->
              fail
                (Format.asprintf "unexpected purge policy error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          List.iter
            (fun max_consumers ->
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS"
                     ~subjects:[ subject ] ~max_consumers ())
              in
              equal (option int) None
                (Nats_eio.Jetstream.Stream.Config.max_consumers config))
            [ -1; 0 ];
          ignore
            (expect_jetstream_config_ok
               (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS"
                  ~subjects:[ subject ]
                  ~duplicate_window:(Mtime.Span.of_uint64_ns 100_000_000L)
                  ()));
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid"
               ~subjects:[ subject ]
               ~max_age:(Mtime.Span.of_uint64_ns 50_000_000L)
               ~duplicate_window:(Mtime.Span.of_uint64_ns 100_000_000L)
               ()
           with
          | Error Nats_eio.Jetstream.Error.Invalid_duplicate_window -> ()
          | Ok _ -> fail "stream accepted a duplicate window above max age"
          | Error error ->
              fail
                (Format.asprintf "unexpected duplicate age error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          let mirror_source =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.Source.v ~name:"ORDERS" ())
          in
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid" ~subjects:[]
               ~mirror:mirror_source ~first_sequence:3L ()
           with
          | Error Nats_eio.Jetstream.Error.Mirror_and_first_sequence -> ()
          | Ok _ -> fail "mirror accepted an initial sequence"
          | Error error ->
              fail
                (Format.asprintf "unexpected mirror sequence error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          match
            Nats_eio.Jetstream.Stream.Config.Consumer_limits.v
              ~max_ack_pending:(-2) ()
          with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_limit
                 { field = "max_ack_pending"; value = -2L }) ->
              ()
          | Ok _ -> fail "consumer limits accepted pending-ack value -2"
          | Error error ->
              fail
                (Format.asprintf "unexpected consumer limit error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
      test "stream wire rejects negative duplicate windows" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"ORDERS")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.info stream));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"config":{"name":"ORDERS","storage":"memory","retention":"limits","discard":"old","duplicate_window":-1},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Invalid_config
                    Nats_eio.Jetstream.Error.Invalid_duplicate_window ->
                    true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream source configuration is typed and composable" (fun () ->
          let filter = Nats.Subject.Filter.literal "orders.*" in
          let transform =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.Transform.v ~source:filter
                 ~destination:"archive.{{wildcard(1)}}" ())
          in
          let external_config =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.External.v
                 ~api_prefix:"$JS.eu.API" ~deliver_prefix:"$JS.eu.DELIVER" ())
          in
          let source =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.Source.v ~name:"ORDERS"
                 ~start:(Nats_eio.Jetstream.Stream.Config.Source.Sequence 7L)
                 ~subject_transforms:[ transform ] ~external_:external_config ())
          in
          let mirror =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS_MIRROR"
                 ~subjects:[] ~mirror:source ~mirror_direct:true ())
          in
          equal string "ORDERS"
            (Nats_eio.Jetstream.Stream.Config.Source.name
               (Option.get (Nats_eio.Jetstream.Stream.Config.mirror mirror)));
          equal bool true
            (Nats_eio.Jetstream.Stream.Config.mirror_direct mirror);
          (match
             Nats_eio.Jetstream.Stream.Config.Source.start
               (Option.get (Nats_eio.Jetstream.Stream.Config.mirror mirror))
           with
          | Some (Nats_eio.Jetstream.Stream.Config.Source.Sequence sequence) ->
              equal int64 7L sequence
          | _ -> fail "stream source lost its sequence start");
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid" ~subjects:[]
               ~mirror:source ~sources:[ source ] ()
           with
          | Error Nats_eio.Jetstream.Error.Mirror_and_sources -> ()
          | Ok _ -> fail "stream accepted mirror and sources together"
          | Error error ->
              fail
                (Format.asprintf "unexpected stream relationship error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          (match
             Nats_eio.Jetstream.Stream.Config.v ~name:"invalid"
               ~subjects:[ Nats.Subject.Filter.literal "orders" ]
               ~mirror:source ()
           with
          | Error Nats_eio.Jetstream.Error.Mirror_and_subjects -> ()
          | Ok _ -> fail "stream accepted mirror subjects"
          | Error error ->
              fail
                (Format.asprintf "unexpected mirror subject error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          (match
             Nats_eio.Jetstream.Stream.Config.Source.v ~name:"ORDERS"
               ~filter_subject:filter ~subject_transforms:[ transform ] ()
           with
          | Error Nats_eio.Jetstream.Error.Source_filter_and_transforms -> ()
          | Ok _ -> fail "source accepted a filter and transforms together"
          | Error error ->
              fail
                (Format.asprintf "unexpected source validation error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          (match
             Nats_eio.Jetstream.Stream.Config.Source.v ~name:"ORDERS"
               ~start:(Nats_eio.Jetstream.Stream.Config.Source.Sequence 0L) ()
           with
          | Error (Nats_eio.Jetstream.Error.Invalid_source_start_sequence 0L) ->
              ()
          | Ok _ -> fail "source accepted sequence zero"
          | Error error ->
              fail
                (Format.asprintf "unexpected source start error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          let republish =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.Republish.v
                 ~source:(Nats.Subject.Filter.literal "orders.>")
                 ~destination:"archive.>" ~headers_only:true ())
          in
          let mirror =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Stream.Config.with_republish mirror
                 (Some republish))
          in
          (match Nats_eio.Jetstream.Stream.Config.with_mirror mirror None with
          | Error Nats_eio.Jetstream.Error.Empty_subjects -> ()
          | Ok _ -> fail "stream cleared its last relation without subjects"
          | Error error ->
              fail
                (Format.asprintf "unexpected relation clearing error: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
          match Nats_eio.Jetstream.Stream.Config.republish mirror with
          | Some value ->
              equal bool true
                (Nats_eio.Jetstream.Stream.Config.Republish.headers_only value)
          | None -> fail "stream config lost republish");
      test "stream source fields use the JetStream wire contract" (fun () ->
          let info_payload =
            {|{"config":{"name":"ARCHIVE","subjects":[],"storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"max_consumers":12,"no_ack":true,"duplicate_window":2000000000,"first_seq":3,"consumer_limits":{"inactive_threshold":1000000000,"max_ack_pending":100,"limits_extra":"kept"},"allow_rollup_hdrs":false,"allow_direct":false,"mirror_direct":true,"sources":[{"name":"ORDERS","opt_start_time":"2026-08-16T12:00:00.000000000Z","subject_transforms":[{"src":"orders.*","dest":"archive.{{wildcard(1)}}","transform_extra":true}],"external":{"api":"$JS.eu.API","deliver":"$JS.eu.DELIVER","external_extra":"kept"},"source_extra":"kept"}],"republish":{"src":"orders.>","dest":"archive.>","headers_only":true,"republish_extra":true}},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}
          in
          let make_response ~sid =
            Ok (consumer_info_wire_with_sid ~sid info_payload)
          in
          let response, response_u = Eio.Promise.create () in
          let info_response, info_response_u = Eio.Promise.create () in
          let update_info_response, update_info_response_u =
            Eio.Promise.create ()
          in
          let update_response, update_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await response;
                `Await info_response;
                `Await update_info_response;
                `Await update_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let filter = Nats.Subject.Filter.literal "orders.*" in
              let transform =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.Transform.v ~source:filter
                     ~destination:"archive.{{wildcard(1)}}" ())
              in
              let external_config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.External.v
                     ~api_prefix:"$JS.eu.API" ~deliver_prefix:"$JS.eu.DELIVER"
                     ())
              in
              let source =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.Source.v ~name:"ORDERS"
                     ~start:
                       (Nats_eio.Jetstream.Stream.Config.Source.Time
                          (ptime_of_rfc3339 "2026-08-16T12:00:00.000000000Z"))
                     ~subject_transforms:[ transform ]
                     ~external_:external_config ())
              in
              let republish =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.Republish.v
                     ~source:(Nats.Subject.Filter.literal "orders.>")
                     ~destination:"archive.>" ~headers_only:true ())
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"ARCHIVE"
                     ~subjects:[] ~sources:[ source ] ~republish
                     ~mirror_direct:true ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.create jetstream config));
              yield_n 5;
              let create_trace = Buffer.contents trace in
              if
                not
                  (contains_substring
                     ~needle:"sources\\\":[{\\\"name\\\":\\\"ORDERS\\\""
                     create_trace)
              then fail "stream create omitted source configuration";
              if
                not
                  (contains_substring
                     ~needle:
                       "opt_start_time\\\":\\\"2026-08-16T12:00:00.000000000Z"
                     create_trace)
              then fail "stream create omitted source start time";
              if
                not
                  (contains_substring
                     ~needle:
                       "subject_transforms\\\":[{\\\"src\\\":\\\"orders.*\\\",\\\"dest\\\":\\\"archive.{{wildcard(1)}}"
                     create_trace)
              then fail "stream create omitted source transform";
              if
                not
                  (contains_substring
                     ~needle:
                       "external\\\":{\\\"api\\\":\\\"$JS.eu.API\\\",\\\"deliver\\\":\\\"$JS.eu.DELIVER\\\"}"
                     create_trace)
              then fail "stream create omitted external prefixes";
              if
                not
                  (contains_substring
                     ~needle:
                       "republish\\\":{\\\"src\\\":\\\"orders.>\\\",\\\"dest\\\":\\\"archive.>\\\",\\\"headers_only\\\":true}"
                     create_trace)
              then fail "stream create omitted republish configuration";
              if
                not
                  (contains_substring ~needle:"mirror_direct\\\":true"
                     create_trace)
              then fail "stream create omitted mirror direct flag";
              Eio.Promise.resolve response_u (make_response ~sid:1);
              let stream = expect_jetstream_ok (Eio.Promise.await result) in
              let info_result, info_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve info_result_u
                    (Nats_eio.Jetstream.Stream.info stream));
              yield_n 5;
              Eio.Promise.resolve info_response_u (make_response ~sid:2);
              let info = expect_jetstream_ok (Eio.Promise.await info_result) in
              let config = Nats_eio.Jetstream.Stream.Info.config info in
              equal (option int) (Some 12)
                (Nats_eio.Jetstream.Stream.Config.max_consumers config);
              equal bool true (Nats_eio.Jetstream.Stream.Config.no_ack config);
              equal (option int64) (Some 3L)
                (Nats_eio.Jetstream.Stream.Config.first_sequence config);
              (match
                 Nats_eio.Jetstream.Stream.Config.duplicate_window config
               with
              | Some value ->
                  equal bool true
                    (Mtime.Span.equal value
                       (Mtime.Span.of_uint64_ns 2_000_000_000L))
              | None -> fail "stream response lost duplicate window");
              (match
                 Nats_eio.Jetstream.Stream.Config.consumer_limits config
               with
              | Some limits ->
                  equal (option int) (Some 100)
                    (Nats_eio.Jetstream.Stream.Config.Consumer_limits
                     .max_ack_pending limits)
              | None -> fail "stream response lost consumer limits");
              let source =
                match Nats_eio.Jetstream.Stream.Config.sources config with
                | [ source ] -> source
                | _ -> fail "stream response lost source configuration"
              in
              (match Nats_eio.Jetstream.Stream.Config.Source.start source with
              | Some (Nats_eio.Jetstream.Stream.Config.Source.Time time) ->
                  equal bool true
                    (Ptime.equal time
                       (ptime_of_rfc3339 "2026-08-16T12:00:00.000000000Z"))
              | _ -> fail "stream response lost source start time");
              equal string "$JS.eu.API"
                (Nats_eio.Jetstream.Stream.Config.External.api_prefix
                   (Option.get
                      (Nats_eio.Jetstream.Stream.Config.Source.external_ source)));
              let updated_config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.with_description config
                     (Some "updated"))
              in
              let update_result, update_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve update_result_u
                    (Nats_eio.Jetstream.Stream.update stream updated_config));
              yield_n 5;
              Eio.Promise.resolve update_info_response_u (make_response ~sid:3);
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"source_extra\\\":\\\"kept" trace)
              then fail "stream update discarded the source's unknown field";
              if
                not
                  (contains_substring ~needle:"transform_extra\\\":true" trace)
              then fail "stream update discarded the transform's unknown field";
              if
                not
                  (contains_substring ~needle:"external_extra\\\":\\\"kept"
                     trace)
              then fail "stream update discarded the external unknown field";
              if
                not
                  (contains_substring ~needle:"republish_extra\\\":true" trace)
              then fail "stream update discarded the republish unknown field";
              if
                not
                  (contains_substring ~needle:"limits_extra\\\":\\\"kept\\\""
                     trace)
              then fail "stream update discarded nested consumer-limit fields";
              Eio.Promise.resolve update_response_u (make_response ~sid:4);
              ignore (expect_jetstream_ok (Eio.Promise.await update_result));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer config updaters preserve modeled fields" (fun () ->
          let span = Mtime.Span.of_uint64_ns 100_000_000L in
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
              (Nats_eio.Jetstream.Consumer.Config.v ~name:"worker"
                 ~durable_name:"worker" ~description:"before"
                 ~deliver_subject:subject ~deliver_group:group
                 ~idle_heartbeat:span ~flow_control:true
                 ~deliver_policy:
                   (Nats_eio.Jetstream.Consumer.Config.By_start_sequence 1L)
                 ~ack_policy:Nats_eio.Jetstream.Consumer.Config.All
                 ~ack_wait:span ~max_deliver:5 ~filter_subjects
                 ~backoff:[ span; second_span ] ~pause_until
                 ~sample_frequency:10 ~rate_limit:64000L ~replicas:3
                 ~metadata:[ ("owner", "server") ]
                 ~replay_policy:Nats_eio.Jetstream.Consumer.Config.Original
                 ~max_ack_pending:(-1) ~max_waiting:0 ~max_batch:5
                 ~max_expires:span ~max_bytes:1024 ~headers_only:true
                 ~inactive_threshold:span ~mem_storage:true ())
          in
          equal (option string) (Some "worker")
            (Nats_eio.Jetstream.Consumer.Config.name config);
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
          equal (list int64)
            [ 100_000_000L; 2_000_000L ]
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
          equal (list int64)
            [ 100_000_000L; 2_000_000L ]
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
          let cleared_flow_control =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_flow_control
                 cleared_backoff None)
          in
          let cleared_heartbeat =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_idle_heartbeat
                 cleared_flow_control None)
          in
          let cleared_group =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_deliver_group
                 cleared_heartbeat None)
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
          (match
             Nats_eio.Jetstream.Consumer.Config.v ~backoff:[ Mtime.Span.zero ]
               ()
           with
          | Ok _ -> ()
          | Error _ -> fail "consumer config rejected zero backoff");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_policy:
                 Nats_eio.Jetstream.Consumer.Config.Last_per_subject ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "deliver_policy" }) ->
              ()
          | _ -> fail "last-per-subject accepted no filter");
          (match
             Nats_eio.Jetstream.Consumer.Config.v ~max_deliver:1
               ~backoff:[ Mtime.Span.zero; Mtime.Span.zero ]
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "backoff" }) ->
              ()
          | _ -> fail "backoff exceeded max-deliver");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_subject:(Nats.Subject.literal "orders.push")
               ~max_waiting:1 ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "max_waiting" }) ->
              ()
          | _ -> fail "push config accepted max-waiting");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_subject:(Nats.Subject.literal "orders.push")
               ~flow_control:true ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "idle_heartbeat" }) ->
              ()
          | _ -> fail "flow control accepted no heartbeat");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_subject:(Nats.Subject.literal "orders.push")
               ~idle_heartbeat:Mtime.Span.(1 * ms)
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "idle_heartbeat" }) ->
              ()
          | _ -> fail "push config accepted a sub-minimum heartbeat");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~max_expires:(Mtime.Span.of_uint64_ns 1L)
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "max_expires" }) ->
              ()
          | _ -> fail "pull config accepted a sub-millisecond expiry");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~ack_policy:Nats_eio.Jetstream.Consumer.Config.Flow_control ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "ack_policy" }) ->
              ()
          | _ -> fail "flow-control acknowledgement accepted a pull consumer");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_subject:(Nats.Subject.literal "orders.push")
               ~ack_policy:Nats_eio.Jetstream.Consumer.Config.No_ack
               ~max_ack_pending:1 ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "max_ack_pending" }) ->
              ()
          | _ -> fail "no-ack push accepted max-ack-pending");
          (match
             Nats_eio.Jetstream.Consumer.Config.v
               ~deliver_subject:(Nats.Subject.literal "orders.push")
               ~ack_policy:Nats_eio.Jetstream.Consumer.Config.Flow_control
               ~ack_wait:Mtime.Span.(1 * s)
               ()
           with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "ack_wait" }) ->
              ()
          | _ -> fail "flow-control acknowledgement accepted ack-wait");
          match Nats_eio.Jetstream.Consumer.Config.v ~flow_control:true () with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_consumer_policy
                 { field = "flow_control" }) ->
              ()
          | _ -> fail "pull config accepted flow control");
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
          | Ok config ->
              equal (list string) [ "blue"; "green" ]
                (Nats_eio.Jetstream.Consumer.Config.priority_groups config)
          | Error error ->
              fail
                (Format.asprintf "priority config rejected multiple groups: %a"
                   Nats_eio.Jetstream.Error.pp_config error));
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
          (match
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
          let from_empty =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_priority
                 (expect_jetstream_config_ok
                    (Nats_eio.Jetstream.Consumer.Config.v ()))
                 ~groups:[ "blue" ]
                 ~policy:(Some Nats_eio.Jetstream.Consumer.Config.Pinned_client)
                 ~timeout:(Some timeout))
          in
          let cleared =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_priority from_empty
                 ~groups:[] ~policy:None ~timeout:None)
          in
          equal (list string) []
            (Nats_eio.Jetstream.Consumer.Config.priority_groups cleared);
          equal (option string) None
            (Option.map
               (function
                 | Nats_eio.Jetstream.Consumer.Config.Overflow -> "overflow"
                 | Nats_eio.Jetstream.Consumer.Config.Pinned_client ->
                     "pinned_client"
                 | Nats_eio.Jetstream.Consumer.Config.Prioritized ->
                     "prioritized")
               (Nats_eio.Jetstream.Consumer.Config.priority_policy cleared));
          let reprioritized =
            expect_jetstream_config_ok
              (Nats_eio.Jetstream.Consumer.Config.with_priority config
                 ~groups:[ "green" ]
                 ~policy:(Some Nats_eio.Jetstream.Consumer.Config.Prioritized)
                 ~timeout:None)
          in
          equal (list string) [ "green" ]
            (Nats_eio.Jetstream.Consumer.Config.priority_groups reprioritized);
          match
            Nats_eio.Jetstream.Consumer.Config.priority_policy reprioritized
          with
          | Some Nats_eio.Jetstream.Consumer.Config.Prioritized -> ()
          | _ -> fail "priority replacement lost its new policy");
      test "priority consumer create emits policy and timeout" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
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
                  (Nats_eio.Jetstream.Consumer.Config.v ~name:"worker"
                     ~durable_name:"worker"
                     ~deliver_subject:(Nats.Subject.literal "orders.push")
                     ~filter_subjects:
                       [
                         Nats.Subject.Filter.literal "orders.created";
                         Nats.Subject.Filter.literal "orders.updated";
                       ]
                     ~pause_until ~sample_frequency:25 ~rate_limit:65536L
                     ~ack_policy:Nats_eio.Jetstream.Consumer.Config.Flow_control
                     ~idle_heartbeat:Mtime.Span.(1 * s)
                     ~flow_control:true ~replicas:3
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
                not
                  (contains_substring ~needle:"\\\"action\\\":\\\"create" trace)
              then fail "consumer create omitted its create-only action";
              if
                not (contains_substring ~needle:"sample_freq\\\":\\\"25%" trace)
              then fail "consumer create omitted sample frequency";
              if not (contains_substring ~needle:"name\\\":\\\"worker" trace)
              then fail "consumer create omitted the consumer name";
              if
                not
                  (contains_substring ~needle:"ack_policy\\\":\\\"flow_control"
                     trace)
              then fail "consumer create omitted flow-control acknowledgements";
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
                      {|{"stream_name":"ORDERS","name":"worker","config":{"name":"worker","durable_name":"worker","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"flow_control","sample_freq":"25%","rate_limit_bps":65536,"num_replicas":3,"metadata":{"owner":"client"},"replay_policy":"instant"}}|}));
              let consumer = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "worker" (Nats_eio.Jetstream.Consumer.name consumer);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer create keeps an existing-name conflict server-owned"
        (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind
                     (expect_jetstream_ok (Nats_eio.Jetstream.v connection))
                     ~name:"ORDERS")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.create stream config));
              yield_n 5;
              if
                not
                  (contains_substring ~needle:"\\\"action\\\":\\\"create"
                     (Buffer.contents trace))
              then fail "consumer create did not request create-only semantics";
              Eio.Promise.resolve response_u
                (Ok
                   (api_error_wire ~sid:1 ~code:400 ~err_code:10013
                      ~description:"consumer already exists"));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Api { err_code = Some 10013; _ } ->
                    true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer names page through the names API" (fun () ->
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
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind
                     (expect_jetstream_ok (Nats_eio.Jetstream.v connection))
                     ~name:"ORDERS")
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.names stream));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"total":2,"offset":0,"limit":1,"consumers":["worker"]}|}));
              yield_n 5;
              if
                not
                  (contains_substring ~needle:"CONSUMER.NAMES.ORDERS"
                     (Buffer.contents trace))
              then fail "consumer names used the wrong endpoint";
              if
                not
                  (contains_substring ~needle:"\\\"offset\\\":1"
                     (Buffer.contents trace))
              then fail "consumer names did not request the next page";
              Eio.Promise.resolve second_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"total":2,"offset":1,"limit":1,"consumers":["archiver"]}|}));
              equal (list string) [ "worker"; "archiver" ]
                (expect_jetstream_ok (Eio.Promise.await result));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer create-or-update emits the empty action" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let stream =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Stream.bind
                     (expect_jetstream_ok (Nats_eio.Jetstream.v connection))
                     ~name:"ORDERS")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.create_or_update stream config));
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"CONSUMER.CREATE.ORDERS.worker"
                     trace)
              then fail "consumer create-or-update used the wrong endpoint";
              if
                not (contains_substring ~needle:"\\\"action\\\":\\\"\\\"" trace)
              then fail "consumer create-or-update omitted its empty action";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              let consumer = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "worker" (Nats_eio.Jetstream.Consumer.name consumer);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer reset sends a sequence and returns updated info" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let consumer = consumer connection in
              let invalid =
                Nats_eio.Jetstream.Consumer.reset_to_sequence consumer
                  ~sequence:0L
              in
              expect_jetstream_error invalid (function
                | Nats_eio.Jetstream.Error.Invalid_consumer_reset_sequence 0L ->
                    true
                | _ -> false);
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.reset_to_sequence consumer
                       ~sequence:42L));
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"CONSUMER.RESET.ORDERS.worker"
                     trace)
              then fail "consumer reset used the wrong endpoint";
              if not (contains_substring ~needle:"\\\"seq\\\":42" trace) then
                fail "consumer reset omitted its sequence";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"stream_name":"ORDERS","name":"worker","reset_seq":42,"delivered":{"consumer_seq":3,"stream_seq":41},"ack_floor":{"consumer_seq":2,"stream_seq":40},"config":{"durable_name":"worker","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              let reset = expect_jetstream_ok (Eio.Promise.await result) in
              equal int64 42L (Nats_eio.Jetstream.Consumer.Reset.sequence reset);
              equal string "worker"
                (Nats_eio.Jetstream.Consumer.Info.name
                   (Nats_eio.Jetstream.Consumer.Reset.info reset));
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
      test "consumer info preserves API error metadata" (fun () ->
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
                      {|{"error":{"code":404,"err_code":10014,"description":"consumer not found","retryable":false,"server":"js-a"}}|}));
              expect_jetstream_error (Eio.Promise.await result) (function
                | Nats_eio.Jetstream.Error.Api { metadata; _ } -> (
                    match metadata with
                    | Jsont.Object (members, _) ->
                        List.exists
                          (fun ((name, _), _) -> String.equal name "retryable")
                          members
                    | _ -> false)
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "account info decodes limits, usage, API stats, and tiers" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.account_info jetstream));
              yield_n 5;
              if
                not
                  (contains_substring ~needle:"$JS.API.INFO"
                     (Buffer.contents trace))
              then fail "account info used the wrong endpoint";
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"memory":11,"storage":22,"reserved_memory":3,"reserved_storage":4,"streams":5,"consumers":6,"limits":{"max_memory":101,"max_storage":102,"max_streams":7,"max_consumers":8,"max_ack_pending":9,"memory_max_stream_bytes":10,"storage_max_stream_bytes":11,"max_bytes_required":true},"domain":"EU","api":{"level":2,"total":12,"errors":13,"inflight":14},"tiers":{"R1":{"memory":21,"storage":22,"reserved_memory":23,"reserved_storage":24,"streams":25,"consumers":26,"limits":{"max_memory":201,"max_storage":202,"max_streams":27,"max_consumers":28,"max_ack_pending":29,"memory_max_stream_bytes":30,"storage_max_stream_bytes":31,"max_bytes_required":false}}},"future_field":true}|}));
              let account = expect_jetstream_ok (Eio.Promise.await result) in
              equal (option string) (Some "EU")
                (Nats_eio.Jetstream.Account.domain account);
              let tier = Nats_eio.Jetstream.Account.tier account in
              equal int64 11L (Nats_eio.Jetstream.Account.Tier.memory tier);
              equal int64 102L
                (Nats_eio.Jetstream.Account.Limits.max_storage
                   (Nats_eio.Jetstream.Account.Tier.limits tier));
              equal int 6 (Nats_eio.Jetstream.Account.Tier.consumers tier);
              let limits = Nats_eio.Jetstream.Account.Tier.limits tier in
              equal int64 101L
                (Nats_eio.Jetstream.Account.Limits.max_memory limits);
              equal bool true
                (Nats_eio.Jetstream.Account.Limits.max_bytes_required limits);
              let api = Nats_eio.Jetstream.Account.api account in
              equal int 2 (Nats_eio.Jetstream.Account.Api.level api);
              equal int64 14L (Nats_eio.Jetstream.Account.Api.inflight api);
              (match Nats_eio.Jetstream.Account.tiers account with
              | [ ("R1", tier) ] ->
                  equal int64 21L (Nats_eio.Jetstream.Account.Tier.memory tier)
              | _ -> fail "account info lost tiered usage");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "account info accepts unsigned usage sentinels" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.account_info jetstream));
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"memory":18446744073709551615,"storage":18446744073709551615,"reserved_memory":18446744073709551615,"reserved_storage":18446744073709551615}|}));
              let account = expect_jetstream_ok (Eio.Promise.await result) in
              let tier = Nats_eio.Jetstream.Account.tier account in
              equal int64 Int64.minus_one
                (Nats_eio.Jetstream.Account.Tier.memory tier);
              equal int64 Int64.minus_one
                (Nats_eio.Jetstream.Account.Tier.reserved_storage tier);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream names page and filter through the names API" (fun () ->
          let first_response, first_response_u = Eio.Promise.create () in
          let second_response, second_response_u = Eio.Promise.create () in
          let subject_response, subject_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await first_response;
                `Await second_response;
                `Await subject_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.names jetstream));
              yield_n 5;
              Eio.Promise.resolve first_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"total":2,"offset":0,"limit":1,"streams":["ORDERS"]}|}));
              yield_n 5;
              if
                not
                  (contains_substring ~needle:"\\\"offset\\\":1"
                     (Buffer.contents trace))
              then fail "stream names did not request the next page";
              Eio.Promise.resolve second_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"total":2,"offset":1,"limit":1,"streams":["EVENTS"]}|}));
              equal (list string) [ "ORDERS"; "EVENTS" ]
                (expect_jetstream_ok (Eio.Promise.await result));
              let subject_result, subject_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve subject_result_u
                    (Nats_eio.Jetstream.Stream.name_by_subject jetstream
                       ~subject:(Nats.Subject.literal "orders.created")));
              yield_n 5;
              let trace = Buffer.contents trace in
              if not (contains_substring ~needle:"STREAM.NAMES" trace) then
                fail "stream subject lookup did not use names";
              if
                not
                  (contains_substring ~needle:"subject\\\":\\\"orders.created"
                     trace)
              then fail "stream subject lookup omitted its filter";
              Eio.Promise.resolve subject_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:3
                      {|{"total":1,"offset":0,"limit":1,"streams":["ORDERS"]}|}));
              equal string "ORDERS"
                (expect_jetstream_ok (Eio.Promise.await subject_result));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream lookup and create-or-update classify a missing stream"
        (fun () ->
          let lookup_response, lookup_response_u = Eio.Promise.create () in
          let update_response, update_response_u = Eio.Promise.create () in
          let create_response, create_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await lookup_response;
                `Await update_response;
                `Await create_response;
                `Await hold;
              ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let lookup_result, lookup_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve lookup_result_u
                    (Nats_eio.Jetstream.Stream.lookup jetstream ~name:"ORDERS"));
              yield_n 5;
              Eio.Promise.resolve lookup_response_u
                (Ok
                   (api_error_wire ~sid:1 ~code:404 ~err_code:10059
                      ~description:"stream not found"));
              expect_jetstream_error (Eio.Promise.await lookup_result) (function
                | Nats_eio.Jetstream.Error.Stream_not_found -> true
                | _ -> false);
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS"
                     ~subjects:[ Nats.Subject.Filter.literal "orders.>" ]
                     ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.create_or_update jetstream config));
              yield_n 5;
              Eio.Promise.resolve update_response_u
                (Ok
                   (api_error_wire ~sid:2 ~code:404 ~err_code:10059
                      ~description:"stream not found"));
              yield_n 5;
              if
                not
                  (contains_substring ~needle:"STREAM.CREATE.ORDERS"
                     (Buffer.contents trace))
              then fail "stream create-or-update did not fall back to create";
              Eio.Promise.resolve create_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:3
                      {|{"config":{"name":"ORDERS","subjects":["orders.>"],"storage":"memory","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":false,"allow_direct":false,"num_replicas":1,"sealed":false}}|}));
              let stream = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "ORDERS" (Nats_eio.Jetstream.Stream.name stream);
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
              let consumer_limits =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.Consumer_limits.v
                     ~inactive_threshold:
                       (Mtime.Span.of_uint64_ns 1_000_000_000L)
                     ~max_ack_pending:100 ())
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"KV_users"
                     ~subjects:[ Nats.Subject.Filter.literal "$KV.users.>" ]
                     ~description:"user values"
                     ~discard:Nats_eio.Jetstream.Stream.Config.New
                     ~max_msgs_per_subject:5L ~max_consumers:12
                     ~discard_new_per_subject:true ~no_ack:true
                     ~duplicate_window:(Mtime.Span.of_uint64_ns 2_000_000_000L)
                     ~first_sequence:3L ~consumer_limits ~allow_rollup:true
                     ~allow_direct:true ~deny_delete:true
                     ~allow_msg_counter:true
                     ~persist_mode:Nats_eio.Jetstream.Stream.Config.Async
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
              if not (contains_substring ~needle:"deny_purge\\\":false" trace)
              then fail "stream create omitted the deny-purge false value";
              if not (contains_substring ~needle:"max_consumers\\\":12" trace)
              then fail "stream create omitted the max-consumers limit";
              if
                not
                  (contains_substring ~needle:"discard_new_per_subject\\\":true"
                     trace)
              then fail "stream create omitted the per-subject discard flag";
              if not (contains_substring ~needle:"no_ack\\\":true" trace) then
                fail "stream create omitted the no-ack flag";
              if
                not
                  (contains_substring ~needle:"allow_msg_counter\\\":true" trace)
              then fail "stream create omitted the message-counter flag";
              if
                not
                  (contains_substring ~needle:"persist_mode\\\":\\\"async" trace)
              then fail "stream create omitted the persistence mode";
              if
                not
                  (contains_substring ~needle:"duplicate_window\\\":2000000000"
                     trace)
              then fail "stream create omitted the duplicate window";
              if not (contains_substring ~needle:"first_seq\\\":3" trace) then
                fail "stream create omitted the first sequence";
              if not (contains_substring ~needle:"consumer_limits\\\":{" trace)
              then fail "stream create omitted consumer limits";
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
                      {|{"config":{"name":"KV_users","subjects":["$KV.users.>"],"description":"user values","storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":5,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_msg_counter":true,"persist_mode":"async","allow_rollup_hdrs":true,"allow_direct":true,"deny_delete":true,"num_replicas":3,"sealed":true,"metadata":{"owner":"test"}}}|}));
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
              let republish =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.Republish.v
                     ~source:(Nats.Subject.Filter.literal "orders.in")
                     ~destination:"orders.out" ())
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS"
                     ~subjects:[ Nats.Subject.Filter.literal "orders.>" ]
                     ~description:"updated" ~max_msgs_per_subject:5L
                     ~discard:Nats_eio.Jetstream.Stream.Config.New
                     ~allow_rollup:false ~allow_direct:true ~replicas:2
                     ~compression:Nats_eio.Jetstream.Stream.Config.S2
                     ~metadata:[ ("owner", "client") ]
                     ~republish ())
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
                      {|{"config":{"name":"ORDERS","subjects":["orders.>"],"description":"before","storage":"file","retention":"limits","discard":"new","max_msgs":-1,"max_msgs_per_subject":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_msg_ttl":true,"allow_rollup_hdrs":false,"allow_direct":false,"deny_delete":true,"deny_purge":true,"num_replicas":3,"sealed":true,"placement":{"cluster":"west"},"metadata":{"owner":"server"},"republish":{"src":"orders.in","dest":"orders.out"}},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
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
              if count_substring ~needle:"allow_rollup_hdrs\\\":false" trace < 2
              then fail "stream update did not preserve the rollup safety flag";
              if not (contains_substring ~needle:"allow_direct\\\":true" trace)
              then
                fail "stream update did not emit the changed direct-read flag";
              if count_substring ~needle:"deny_delete\\\":true" trace < 2 then
                fail "stream update cleared deny-delete";
              if count_substring ~needle:"deny_purge\\\":true" trace < 2 then
                fail "stream update cleared deny-purge";
              if not (contains_substring ~needle:"no_ack\\\":false" trace) then
                fail "stream update omitted the no-ack false value";
              List.iter
                (fun field ->
                  if contains_substring ~needle:field trace then
                    fail
                      ("stream update sent unsupported default field " ^ field))
                [
                  "allow_msg_counter\\\":false";
                  "allow_atomic\\\":false";
                  "allow_msg_schedules\\\":false";
                  "persist_mode\\\":\\\"default";
                  "allow_batched\\\":false";
                  "subject_delete_marker_ttl\\\":0";
                ];
              if not (contains_substring ~needle:"allow_msg_ttl\\\":true" trace)
              then fail "stream update cleared an enabled message TTL flag";
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
              then fail "stream update discarded the modeled republish field";
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"config":{"name":"ORDERS","subjects":["orders.>"],"description":"updated","storage":"file","retention":"limits","discard":"new","max_msgs":-1,"max_msgs_per_subject":5,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_msg_ttl":true,"allow_msg_counter":true,"persist_mode":"async","allow_rollup_hdrs":false,"allow_direct":true,"deny_delete":true,"deny_purge":true,"num_replicas":2,"sealed":true,"placement":{"cluster":"east"},"compression":"s2","metadata":{"owner":"client"},"republish":{"src":"orders.in","dest":"orders.out"}},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
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
              equal bool true
                (Nats_eio.Jetstream.Stream.Config.allow_msg_counter config);
              equal bool true
                (Nats_eio.Jetstream.Stream.Config.deny_delete config);
              equal bool true
                (Nats_eio.Jetstream.Stream.Config.deny_purge config);
              (match Nats_eio.Jetstream.Stream.Config.persist_mode config with
              | Nats_eio.Jetstream.Stream.Config.Async -> ()
              | Nats_eio.Jetstream.Stream.Config.Default ->
                  fail "stream update lost persistence mode");
              (match Nats_eio.Jetstream.Stream.Config.metadata config with
              | [ ("owner", "client") ] -> ()
              | _ -> fail "stream update lost stream metadata");
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "stream update omits absent version-gated defaults" (fun () ->
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
                  (Nats_eio.Jetstream.Stream.bind jetstream ~name:"LEGACY")
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Stream.Config.v ~name:"LEGACY"
                     ~subjects:[ Nats.Subject.Filter.literal "legacy.>" ]
                     ~description:"updated" ())
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Stream.update stream config));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:1
                      {|{"config":{"name":"LEGACY","subjects":["legacy.>"],"description":"before","storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":false,"allow_direct":false,"deny_delete":false,"deny_purge":false,"num_replicas":1,"sealed":false},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
              yield_n 5;
              let trace = Buffer.contents trace in
              if not (contains_substring ~needle:"STREAM.UPDATE.LEGACY" trace)
              then fail "legacy stream update was not sent";
              List.iter
                (fun field ->
                  if contains_substring ~needle:field trace then
                    fail
                      ("legacy stream update sent unsupported default field "
                     ^ field))
                [
                  "allow_msg_ttl\\\":false";
                  "allow_msg_counter\\\":false";
                  "allow_atomic\\\":false";
                  "allow_msg_schedules\\\":false";
                  "persist_mode\\\":\\\"default";
                  "allow_batched\\\":false";
                  "subject_delete_marker_ttl\\\":0";
                ];
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"config":{"name":"LEGACY","subjects":["legacy.>"],"description":"updated","storage":"file","retention":"limits","discard":"old","max_msgs":-1,"max_msgs_per_subject":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"allow_rollup_hdrs":false,"allow_direct":false,"deny_delete":false,"deny_purge":false,"num_replicas":1,"sealed":false},"state":{"messages":0,"bytes":0,"first_seq":0,"last_seq":0,"consumer_count":0}}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              equal (option string) (Some "updated")
                (Nats_eio.Jetstream.Stream.Config.description
                   (Nats_eio.Jetstream.Stream.Info.config info));
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
                    { name = "Nats-Sequence"; value = "-1" } ->
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
                    { name = "Nats-Sequence"; value = "not-a-number" } ->
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
              if contains_substring ~needle:"priority_timeout\\\":0" clear_trace
              then
                fail
                  "consumer update sent an unsupported default priority timeout";
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
      test "consumer update replaces priority identity fields" (fun () ->
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
                  (Nats_eio.Jetstream.Consumer.Config.with_priority
                     (expect_jetstream_config_ok
                        (Nats_eio.Jetstream.Consumer.Config.v
                           ~durable_name:"worker" ()))
                     ~groups:[ "green" ]
                     ~policy:
                       (Some Nats_eio.Jetstream.Consumer.Config.Prioritized)
                     ~timeout:None)
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
              yield_n 5;
              let trace = Buffer.contents trace in
              if
                not
                  (contains_substring
                     ~needle:"priority_groups\\\":[\\\"green\\\"]" trace)
              then fail "priority update dropped the replacement groups";
              if
                not
                  (contains_substring
                     ~needle:"priority_policy\\\":\\\"prioritized" trace)
              then fail "priority update dropped the replacement policy";
              if not (contains_substring ~needle:"priority_timeout\\\":0" trace)
              then fail "priority update did not clear the old pin timeout";
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","priority_groups":["green"],"priority_policy":"prioritized","priority_timeout":0,"deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              equal (list string) [ "green" ]
                (Nats_eio.Jetstream.Consumer.Config.priority_groups
                   (Nats_eio.Jetstream.Consumer.Info.config info));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "consumer update clears priority identity fields" (fun () ->
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
              let pinned =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:"worker"
                     ~priority_groups:[ "blue" ]
                     ~priority_policy:
                       Nats_eio.Jetstream.Consumer.Config.Pinned_client
                     ~priority_timeout:Mtime.Span.(30 * s)
                     ())
              in
              let config =
                expect_jetstream_config_ok
                  (Nats_eio.Jetstream.Consumer.Config.with_priority pinned
                     ~groups:[] ~policy:None ~timeout:None)
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
              yield_n 5;
              let trace = Buffer.contents trace in
              let update_start =
                match
                  nth_substring_position
                    ~needle:
                      "jetstream-server: wrote \"PUB \
                       $JS.API.CONSUMER.CREATE.ORDERS.worker"
                    ~occurrence:1 trace
                with
                | Some position -> position
                | None -> fail "priority clear update was not sent"
              in
              let update_trace =
                String.sub trace update_start
                  (String.length trace - update_start)
              in
              if contains_substring ~needle:"priority_groups" update_trace then
                fail "priority clear update retained priority groups";
              if contains_substring ~needle:"priority_policy" update_trace then
                fail "priority clear update retained priority policy";
              if
                not
                  (contains_substring ~needle:"priority_timeout\\\":0"
                     update_trace)
              then fail "priority clear update omitted the cleared timeout";
              Eio.Promise.resolve update_response_u
                (Ok
                   (consumer_info_wire_with_sid ~sid:2
                      {|{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant"}}|}));
              let info = expect_jetstream_ok (Eio.Promise.await result) in
              let config = Nats_eio.Jetstream.Consumer.Info.config info in
              equal (list string) []
                (Nats_eio.Jetstream.Consumer.Config.priority_groups config);
              equal (option string) None
                (Option.map
                   (function
                     | Nats_eio.Jetstream.Consumer.Config.Overflow -> "overflow"
                     | Nats_eio.Jetstream.Consumer.Config.Pinned_client ->
                         "pinned_client"
                     | Nats_eio.Jetstream.Consumer.Config.Prioritized ->
                         "prioritized")
                   (Nats_eio.Jetstream.Consumer.Config.priority_policy config));
              equal (option int64) None
                (Option.map Mtime.Span.to_uint64_ns
                   (Nats_eio.Jetstream.Consumer.Config.priority_timeout config));
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
      test "stream message deletion supports ordinary and secure requests"
        (fun () ->
          let ordinary_response, ordinary_response_u = Eio.Promise.create () in
          let secure_response, secure_response_u = Eio.Promise.create () in
          let failed_response, failed_response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [
                `Return info_wire;
                `Await ordinary_response;
                `Await secure_response;
                `Await failed_response;
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
              let ordinary_result, ordinary_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve ordinary_result_u
                    (Nats_eio.Jetstream.Stream.delete_message stream
                       ~sequence:7L));
              yield_n 5;
              let trace_after_ordinary = Buffer.contents trace in
              if
                not
                  (contains_substring ~needle:"STREAM.MSG.DELETE.ORDERS"
                     trace_after_ordinary)
              then fail "ordinary message deletion was not sent";
              if
                not
                  (contains_substring ~needle:"seq\\\":7" trace_after_ordinary)
              then fail "ordinary message deletion omitted the sequence";
              if
                not
                  (contains_substring ~needle:"no_erase\\\":true"
                     trace_after_ordinary)
              then fail "ordinary message deletion did not request no erase";
              Eio.Promise.resolve ordinary_response_u
                (Ok (consumer_info_wire_with_sid ~sid:1 "{\"success\":true}"));
              expect_jetstream_ok (Eio.Promise.await ordinary_result);
              let secure_result, secure_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve secure_result_u
                    (Nats_eio.Jetstream.Stream.secure_delete_message stream
                       ~sequence:8L));
              yield_n 5;
              let trace_after_secure = Buffer.contents trace in
              if not (contains_substring ~needle:"seq\\\":8" trace_after_secure)
              then fail "secure message deletion omitted the sequence";
              if
                Int.compare
                  (count_substring ~needle:"no_erase\\\":true"
                     trace_after_secure)
                  1
                <> 0
              then fail "secure message deletion requested no erase";
              Eio.Promise.resolve secure_response_u
                (Ok (consumer_info_wire_with_sid ~sid:2 "{\"success\":true}"));
              expect_jetstream_ok (Eio.Promise.await secure_result);
              let failed_result, failed_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve failed_result_u
                    (Nats_eio.Jetstream.Stream.delete_message stream
                       ~sequence:9L));
              yield_n 5;
              Eio.Promise.resolve failed_response_u
                (Ok (consumer_info_wire_with_sid ~sid:3 "{\"success\":false}"));
              (match Eio.Promise.await failed_result with
              | Error
                  (Nats_eio.Jetstream.Error.Message_delete_failed
                     { sequence = 9L; secure = false }) ->
                  ()
              | Ok () ->
                  fail "unsuccessful message deletion unexpectedly succeeded"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected message deletion error: %a"
                       Nats_eio.Jetstream.Error.pp error));
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
      test "fetch no-wait sends the no-wait pull shape" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.fetch_no_wait
                       (consumer connection) ~batch:3));
              yield_n 5;
              let request_wire = Buffer.contents trace in
              if
                not (contains_substring ~needle:"no_wait\\\":true" request_wire)
              then fail "no-wait pull did not set no_wait";
              if contains_substring ~needle:"expires\\\":" request_wire then
                fail "no-wait pull unexpectedly set expires";
              Eio.Promise.resolve response_u
                (Ok (status_wire ~code:404 ~description:"No Messages"));
              equal int 0
                (List.length (expect_jetstream_ok (Eio.Promise.await result)));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "push config retains its delivery subject and queue group" (fun () ->
          let subject = Nats.Subject.literal "orders.push" in
          let group = Nats.Queue_group.literal "workers" in
          let idle_heartbeat = Mtime.Span.(100 * ms) in
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
          | Some value ->
              equal int64 100_000_000L (Mtime.Span.to_uint64_ns value)
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
          let delete_retry_response, delete_retry_response_u =
            Eio.Promise.create ()
          in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await info_response;
                `Await delete_response;
                `Await delete_retry_response;
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
                (Ok
                   (owned_push_create_wire_with_sid ~sid:2 ~num_pending:3L
                      ~delivered_consumer_sequence:2L));
              yield_n 5;
              Eio.Promise.resolve info_response_u
                (Ok (owned_push_consumer_info_wire_with_sid ~sid:3));
              let push = expect_jetstream_ok (Eio.Promise.await result) in
              equal int64 5L
                (Nats_eio.Jetstream.Consumer.Push.initial_pending push);
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
              Eio.Promise.resolve delete_response_u
                (Ok
                   (api_error_wire ~sid:4 ~code:500 ~err_code:10001
                      ~description:"temporary delete failure"));
              expect_jetstream_error (Eio.Promise.await close_result) (function
                | Nats_eio.Jetstream.Error.Api { err_code = Some 10001; _ } ->
                    true
                | _ -> false);
              let retry_result, retry_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve retry_result_u
                    (Nats_eio.Jetstream.Consumer.Push.close push));
              yield_n 5;
              wait_for_trace ~clock ~trace
                ~needle:"PUB $JS.API.CONSUMER.DELETE.ORDERS.worker" ~count:2;
              Eio.Promise.resolve delete_retry_response_u
                (Ok (api_ok_wire ~sid:5));
              expect_jetstream_ok (Eio.Promise.await retry_result);
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
      test "push recreates a named ephemeral consumer after reconnect"
        (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let pause_response, pause_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let restore_info, restore_info_u = Eio.Promise.create () in
          let create_response, create_response_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
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
                `Await final_delete;
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
                      {|{"stream_name":"ORDERS","name":"worker","config":{"name":"worker","deliver_subject":"orders.push","deliver_policy":"all","ack_policy":"explicit","filter_subjects":["orders.created","orders.updated"],"backoff":[1000000,2000000],"sample_freq":"10%","rate_limit_bps":64000,"num_replicas":2,"metadata":{"owner":"server"},"replay_policy":"instant"}}|}));
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
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS.worker"
                ~count:1;
              if
                not
                  (contains_substring
                     ~needle:"deliver_policy\\\":\\\"by_start_sequence"
                     (Buffer.contents trace))
              then fail "ephemeral push did not resume by stream sequence";
              if
                not
                  (contains_substring ~needle:"\\\"action\\\":\\\"create"
                     (Buffer.contents trace))
              then fail "ephemeral push recreation was not create-only";
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
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Push.close push));
              wait_for_trace ~clock ~trace
                ~needle:"PUB $JS.API.CONSUMER.DELETE.ORDERS.worker" ~count:1;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:6));
              expect_jetstream_ok (Eio.Promise.await close_result);
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
                   ~timeout:Mtime.Span.(250 * ms)
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
                       ~expires:Mtime.Span.(500 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~filter_subjects:
                         [ Nats.Subject.Filter.literal "orders.created" ]
                       ~replay_policy:
                         Nats_eio.Jetstream.Consumer.Config.Original
                       ~headers_only:true
                       ~inactive_threshold:Mtime.Span.(2 * min)
                       ~max_reset_attempts:3
                       ~metadata:[ ("owner", "ordered") ]
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              let create_wire =
                ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                  (contains_substring ~needle:"num_replicas\\\":1"
                     (Buffer.contents trace))
              then fail "ordered consumer did not force one replica";
              if
                not
                  (contains_substring
                     ~needle:"inactive_threshold\\\":120000000000"
                     (Buffer.contents trace))
              then fail "ordered consumer did not set an inactive threshold";
              if
                not
                  (contains_substring ~needle:"name\\\":\\\"ordered_1"
                     (Buffer.contents trace))
              then fail "ordered consumer did not use its name prefix";
              if
                not
                  (contains_substring
                     ~needle:"filter_subjects\\\":[\\\"orders.created\\\"]"
                     (Buffer.contents trace))
              then fail "ordered consumer omitted plural filters";
              if
                not
                  (contains_substring ~needle:"replay_policy\\\":\\\"original"
                     (Buffer.contents trace))
              then fail "ordered consumer omitted replay policy";
              if
                not
                  (contains_substring ~needle:"headers_only\\\":true"
                     (Buffer.contents trace))
              then fail "ordered consumer omitted headers-only delivery";
              if
                not
                  (contains_substring
                     ~needle:"metadata\\\":{\\\"owner\\\":\\\"ordered\\\"}"
                     (Buffer.contents trace))
              then fail "ordered consumer omitted metadata";
              let first_result, first_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve first_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve first_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve first_create_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:12L ~consumer_sequence:3L "gap"));
              yield_n 5;
              Eio.Promise.resolve first_delete_u (Ok (api_ok_wire ~sid:3));
              yield_n 5;
              Eio.Promise.resolve second_create_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered_2"
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
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered_2"
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
      test "ordered consumer recreates after a heartbeat sequence gap"
        (fun () ->
          let first_create, first_create_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let heartbeat, heartbeat_u = Eio.Promise.create () in
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
                `Await heartbeat;
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve first_create_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:10L ~consumer_sequence:1L "before-gap"));
              ignore (expect_jetstream_ok (Eio.Promise.await first_result));
              let replay_result, replay_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve replay_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve heartbeat_u
                (Ok
                   (ordered_heartbeat_wire ~sid:2 ~consumer_sequence:3L
                      ~stream_sequence:12L));
              wait_for_trace_count ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:1;
              Eio.Promise.resolve first_delete_u (Ok (api_ok_wire ~sid:3));
              wait_for_trace_count ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:2;
              if
                not
                  (contains_substring ~needle:"\\\"opt_start_seq\\\":11"
                     (Buffer.contents trace))
              then fail "heartbeat reset did not resume at the next sequence";
              Eio.Promise.resolve second_create_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered_2"
                      ~deliver_policy:"by_start_sequence" ~opt_start_seq:11L ()));
              wait_for_trace_count ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.ordered_2"
                ~count:1;
              Eio.Promise.resolve replay_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered_2"
                      ~stream_sequence:11L ~consumer_sequence:1L "replayed"));
              let replayed =
                expect_jetstream_ok (Eio.Promise.await replay_result)
              in
              equal string "replayed" (Nats_eio.Jetstream.Msg.payload replayed);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace_count ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_2"
                ~count:1;
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Time.Mono.sleep clock 0.25;
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:1;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:3));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:2;
              Eio.Promise.resolve recreated_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered_2"
                      ~deliver_policy:"all" ()));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.ordered_2"
                ~count:1;
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered_2"
                      ~stream_sequence:1L ~consumer_sequence:1L "after-reset"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "after-reset"
                (Nats_eio.Jetstream.Msg.payload message);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_2"
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_create_wire ~sid:4 ~name:"ordered_2"
                      ~deliver_policy:"all" ()));
              yield_n 5;
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered_2"
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
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
      test "ordered recovery timeout after teardown fails the session"
        (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let gap_delivery, gap_delivery_u = Eio.Promise.create () in
          let delete_response, delete_response_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await first_delivery;
                `Await gap_delivery;
                `Await delete_response;
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
                       ~expires:Mtime.Span.(1 * s)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:10L ~consumer_sequence:1L "first"));
              ignore (expect_jetstream_ok (Eio.Promise.await first_result));
              let recovery_result, recovery_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve recovery_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout
                       ~timeout:Mtime.Span.(100 * ms)
                       ordered));
              yield_n 5;
              Eio.Promise.resolve gap_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:12L ~consumer_sequence:3L "gap"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:1;
              Eio.Promise.resolve delete_response_u (Ok (api_ok_wire ~sid:3));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:2;
              Eio.Time.Mono.sleep clock 0.2;
              (match Eio.Promise.await recovery_result with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "ordered recovery unexpectedly returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected ordered recovery result: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Ordered.next ordered) (function
                | Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout ->
                    true
                | _ -> false);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_2"
                ~count:1;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:5));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered cleanup retries honor reset limits" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let gap_delivery, gap_delivery_u = Eio.Promise.create () in
          let first_delete, first_delete_u = Eio.Promise.create () in
          let second_delete, second_delete_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await first_delivery;
                `Await gap_delivery;
                `Await first_delete;
                `Await second_delete;
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~max_reset_attempts:1 ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:10L ~consumer_sequence:1L "first"));
              ignore (expect_jetstream_ok (Eio.Promise.await first_result));
              let recovery_result, recovery_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve recovery_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout
                       ~timeout:Mtime.Span.(1 * s)
                       ordered));
              yield_n 5;
              Eio.Promise.resolve gap_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:12L ~consumer_sequence:3L "gap"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:1;
              Eio.Promise.resolve first_delete_u
                (Ok
                   (api_error_wire ~sid:3 ~code:503 ~err_code:10008
                      ~description:"JetStream not available"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:2;
              Eio.Promise.resolve second_delete_u
                (Ok
                   (api_error_wire ~sid:4 ~code:503 ~err_code:10008
                      ~description:"JetStream not available"));
              (match Eio.Promise.await recovery_result with
              | Error
                  (Nats_eio.Jetstream.Error.Api
                     { code = 503; err_code = Some 10008; _ }) ->
                  ()
              | Ok _ -> fail "ordered recovery unexpectedly returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected ordered recovery result: %a"
                       Nats_eio.Jetstream.Error.pp error));
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:3;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:5));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered consumer recreates after transport reconnect" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let first_delivery, first_delivery_u = Eio.Promise.create () in
          let disconnect, disconnect_u = Eio.Promise.create () in
          let reconnect_info, reconnect_info_u = Eio.Promise.create () in
          let transient_delete, transient_delete_u = Eio.Promise.create () in
          let previous_delete, previous_delete_u = Eio.Promise.create () in
          let transient_create, transient_create_u = Eio.Promise.create () in
          let recreated, recreated_u = Eio.Promise.create () in
          let second_delivery, second_delivery_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_reconnecting_connection_traced
            ~first_reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await first_delivery;
                `Await disconnect;
              ]
            ~second_reads:
              [
                `Await reconnect_info;
                `Await transient_delete;
                `Await previous_delete;
                `Await transient_create;
                `Await recreated;
                `Await second_delivery;
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
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
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:10L ~consumer_sequence:1L
                      "before-reconnect"));
              let first =
                expect_jetstream_ok (Eio.Promise.await first_result)
              in
              equal string "before-reconnect"
                (Nats_eio.Jetstream.Msg.payload first);
              Eio.Promise.resolve disconnect_u (Error End_of_file);
              wait_for_trace ~clock ~trace
                ~needle:"jetstream-reconnect-network: connect to tcp" ~count:2;
              Eio.Promise.resolve reconnect_info_u (Ok info_wire);
              let second_result, second_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve second_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:1;
              Eio.Promise.resolve transient_delete_u
                (Ok
                   (api_error_wire ~sid:3 ~code:503 ~err_code:10008
                      ~description:"JetStream not available"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:2;
              Eio.Promise.resolve previous_delete_u (Ok (api_ok_wire ~sid:4));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:2;
              Eio.Promise.resolve transient_create_u
                (Ok
                   (api_error_wire ~sid:5 ~code:503 ~err_code:10008
                      ~description:"JetStream not available"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:3;
              Eio.Promise.resolve recreated_u
                (Ok
                   (ordered_create_wire ~sid:6 ~name:"ordered_3"
                      ~deliver_policy:"by_start_sequence" ~opt_start_seq:11L ()));
              if
                not
                  (contains_substring ~needle:"\\\"opt_start_seq\\\":11"
                     (Buffer.contents trace))
              then
                fail
                  "ordered reconnect did not resume at the next stream sequence";
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.ordered_3"
                ~count:1;
              Eio.Promise.resolve second_delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:7 ~consumer:"ordered_3"
                      ~stream_sequence:11L ~consumer_sequence:1L
                      "after-reconnect"));
              let second =
                expect_jetstream_ok (Eio.Promise.await second_result)
              in
              equal string "after-reconnect"
                (Nats_eio.Jetstream.Msg.payload second);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_3"
                ~count:1;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:8));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ordered consumer recreates after a pull no-responders status"
        (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let pull_failure, pull_failure_u = Eio.Promise.create () in
          let previous_delete, previous_delete_u = Eio.Promise.create () in
          let recreated, recreated_u = Eio.Promise.create () in
          let delivery, delivery_u = Eio.Promise.create () in
          let final_delete, final_delete_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [
                `Return info_wire;
                `Await create_response;
                `Await pull_failure;
                `Await previous_delete;
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
                       ~expires:Mtime.Span.(200 * ms)
                       ~idle_heartbeat:Mtime.Span.(100 * ms)
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.next ordered));
              yield_n 5;
              Eio.Promise.resolve pull_failure_u
                (Ok
                   (status_wire_with_sid ~sid:2 ~code:503
                      ~description:"No Responders"));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_1"
                ~count:1;
              Eio.Promise.resolve previous_delete_u (Ok (api_ok_wire ~sid:3));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.CREATE.ORDERS" ~count:2;
              Eio.Promise.resolve recreated_u
                (Ok
                   (ordered_create_wire ~sid:4 ~name:"ordered_2"
                      ~deliver_policy:"all" ()));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.ordered_2"
                ~count:1;
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:5 ~consumer:"ordered_2"
                      ~stream_sequence:1L ~consumer_sequence:1L
                      "after-no-responders"));
              let message = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "after-no-responders"
                (Nats_eio.Jetstream.Msg.payload message);
              let close_result, close_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve close_result_u
                    (Nats_eio.Jetstream.Consumer.Ordered.close ordered));
              wait_for_trace ~clock ~trace
                ~needle:"wrote \"PUB $JS.API.CONSUMER.DELETE.ORDERS.ordered_2"
                ~count:1;
              Eio.Promise.resolve final_delete_u (Ok (api_ok_wire ~sid:6));
              expect_jetstream_ok (Eio.Promise.await close_result);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "ack publishes without waiting for a response" (fun () ->
          let delivery, delivery_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await delivery; `Await hold ]
            (fun ~sw ~trace connection ->
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
              expect_jetstream_ok (Nats_eio.Jetstream.Msg.ack message);
              if
                not
                  (contains_substring
                     ~needle:"wrote \"PUB $JS.ACK.ORDERS.worker"
                     (Buffer.contents trace))
              then fail "ack did not publish its acknowledgement subject";
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
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
              let config = Mtime.Span.(100 * ms) in
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(200 * ms)
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
                     ~expires:Mtime.Span.(200 * ms)
                     ~idle_heartbeat:Mtime.Span.(100 * ms)
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
      test "continuous consumption buffers and drains stop-after messages"
        (fun () ->
          let first, first_u = Eio.Promise.create () in
          let second, second_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [ `Return info_wire; `Await first; `Await second; `Await hold ]
            (fun ~sw ~trace connection ->
              let consume =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Consume.v ~sw ~batch:1
                     ~max_messages:2 ~stop_after:2 (consumer connection))
              in
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker" ~count:1;
              Eio.Promise.resolve first_u
                (Ok (delivery_wire_with_sid ~sid:1 "one"));
              wait_for_trace_count ~trace
                ~needle:"CONSUMER.MSG.NEXT.ORDERS.worker" ~count:2;
              Eio.Promise.resolve second_u
                (Ok (delivery_wire_with_sid ~sid:1 "two"));
              let first_message =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Consume.next consume)
              in
              let second_message =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Consume.next consume)
              in
              equal string "one" (Nats_eio.Jetstream.Msg.payload first_message);
              equal string "two" (Nats_eio.Jetstream.Msg.payload second_message);
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Consume.next consume) (function
                | Nats_eio.Jetstream.Error.Pull_closed -> true
                | _ -> false);
              equal bool true
                (Nats_eio.Jetstream.Consumer.Consume.closed consume);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "continuous stop makes buffered messages unavailable" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let consume =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Consume.v ~sw ~max_messages:1
                     (consumer connection))
              in
              yield_n 5;
              Eio.Promise.resolve response_u
                (Ok (delivery_wire_with_sid ~sid:1 "discard-me"));
              yield_n 5;
              Nats_eio.Jetstream.Consumer.Consume.stop consume;
              expect_jetstream_error
                (Nats_eio.Jetstream.Consumer.Consume.next consume) (function
                | Nats_eio.Jetstream.Error.Pull_closed -> true
                | _ -> false);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "continuous consumption closes with its switch" (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          let request_seen, request_seen_u = Eio.Promise.create () in
          let request_prefix =
            "jetstream-server: wrote \"PUB \
             $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker"
          in
          let request_reported = ref false in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await hold ]
            ~on_trace:(fun message ->
              if
                (not !request_reported)
                && String.length message >= String.length request_prefix
                && String.equal
                     (String.sub message 0 (String.length request_prefix))
                     request_prefix
              then (
                request_reported := true;
                Eio.Promise.resolve request_seen_u ()))
            (fun ~sw ~trace connection ->
              let consume_ready, consume_ready_u = Eio.Promise.create () in
              let release_consume, release_consume_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Switch.run (fun consume_sw ->
                      let consume =
                        expect_jetstream_ok
                          (Nats_eio.Jetstream.Consumer.Consume.v ~sw:consume_sw
                             ~max_messages:1
                             ~expires:Mtime.Span.(3 * s)
                             ~idle_heartbeat:Mtime.Span.(1 * s)
                             (consumer connection))
                      in
                      Eio.Promise.resolve consume_ready_u consume;
                      Eio.Promise.await release_consume));
              let consume = Eio.Promise.await consume_ready in
              let next_result, next_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve next_result_u
                    (Nats_eio.Jetstream.Consumer.Consume.next consume));
              Eio.Promise.await request_seen;
              Eio.Promise.resolve release_consume_u ();
              yield_n 5;
              (match Eio.Promise.await next_result with
              | Error Nats_eio.Jetstream.Error.Pull_closed -> ()
              | Ok _ -> fail "continuous consume returned a message"
              | Error error ->
                  fail
                    (Format.asprintf "continuous consume failed: %a; trace:\n%s"
                       Nats_eio.Jetstream.Error.pp error (Buffer.contents trace)));
              equal bool true
                (Nats_eio.Jetstream.Consumer.Consume.closed consume);
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
      test "cancelling pull next preserves session ownership" (fun () ->
          let cancellation, cancellation_u = Eio.Promise.create () in
          let result, result_u = Eio.Promise.create () in
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw (consumer connection))
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Cancel.sub (fun cancel ->
                      Eio.Promise.resolve cancellation_u cancel;
                      try
                        ignore (Nats_eio.Jetstream.Consumer.Pull.next pull);
                        Eio.Promise.resolve result_u `Completed
                      with Eio.Cancel.Cancelled _ ->
                        Eio.Promise.resolve result_u `Cancelled));
              let cancel = Eio.Promise.await cancellation in
              yield_n 5;
              Eio.Cancel.cancel cancel (Failure "cancel pull");
              (match Eio.Promise.await result with
              | `Cancelled -> ()
              | `Completed -> fail "pull next unexpectedly completed");
              Eio.Promise.resolve response_u (Ok (delivery_wire "after-cancel"));
              let message =
                expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              equal string "after-cancel"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Pull.close pull);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "cancelling push next preserves session ownership" (fun () ->
          let info_response, info_response_u = Eio.Promise.create () in
          let cancellation, cancellation_u = Eio.Promise.create () in
          let result, result_u = Eio.Promise.create () in
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
              let push_result, push_result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve push_result_u
                    (Nats_eio.Jetstream.Consumer.Push.v ~sw
                       (consumer connection)));
              yield_n 5;
              Eio.Promise.resolve info_response_u (Ok push_consumer_info_wire);
              let push = expect_jetstream_ok (Eio.Promise.await push_result) in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Cancel.sub (fun cancel ->
                      Eio.Promise.resolve cancellation_u cancel;
                      try
                        ignore (Nats_eio.Jetstream.Consumer.Push.next push);
                        Eio.Promise.resolve result_u `Completed
                      with Eio.Cancel.Cancelled _ ->
                        Eio.Promise.resolve result_u `Cancelled));
              let cancel = Eio.Promise.await cancellation in
              yield_n 5;
              Eio.Cancel.cancel cancel (Failure "cancel push");
              (match Eio.Promise.await result with
              | `Cancelled -> ()
              | `Completed -> fail "push next unexpectedly completed");
              Eio.Promise.resolve delivery_u
                (Ok (delivery_wire_with_sid ~sid:2 "after-cancel"));
              let message =
                expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.next push)
              in
              equal string "after-cancel"
                (Nats_eio.Jetstream.Msg.payload message);
              expect_jetstream_ok (Nats_eio.Jetstream.Consumer.Push.close push);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "cancelling ordered next preserves session ownership" (fun () ->
          let create_response, create_response_u = Eio.Promise.create () in
          let cancellation, cancellation_u = Eio.Promise.create () in
          let result, result_u = Eio.Promise.create () in
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
                       ~name_prefix:"ordered" stream));
              yield_n 5;
              Eio.Promise.resolve create_response_u
                (Ok
                   (ordered_create_wire ~sid:1 ~name:"ordered_1"
                      ~deliver_policy:"all" ()));
              let ordered =
                expect_jetstream_ok (Eio.Promise.await ordered_result)
              in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Cancel.sub (fun cancel ->
                      Eio.Promise.resolve cancellation_u cancel;
                      try
                        ignore
                          (Nats_eio.Jetstream.Consumer.Ordered.next ordered);
                        Eio.Promise.resolve result_u `Completed
                      with Eio.Cancel.Cancelled _ ->
                        Eio.Promise.resolve result_u `Cancelled));
              let cancel = Eio.Promise.await cancellation in
              yield_n 5;
              Eio.Cancel.cancel cancel (Failure "cancel ordered");
              (match Eio.Promise.await result with
              | `Cancelled -> ()
              | `Completed -> fail "ordered next unexpectedly completed");
              Eio.Promise.resolve delivery_u
                (Ok
                   (ordered_delivery_wire ~sid:2 ~consumer:"ordered_1"
                      ~stream_sequence:1L ~consumer_sequence:1L "after-cancel"));
              let message =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Ordered.next ordered)
              in
              equal string "after-cancel"
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
      test "idle heartbeat silence fails a pull distinctly" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw connection ->
              let pull =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Consumer.Pull.v ~sw
                     ~expires:Mtime.Span.(500 * ms)
                     ~idle_heartbeat:Mtime.Span.(100 * ms)
                     (consumer connection))
              in
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(250 * ms)
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
      test "asynchronous publishing carries typed options and settles futures"
        (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace ~clock connection ->
              ignore trace;
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let options =
                match
                  Nats_eio.Jetstream.Publish_options.with_msg_id "async-id"
                    Nats_eio.Jetstream.Publish_options.empty
                with
                | Error error ->
                    fail
                      (Format.asprintf "option construction failed: %a"
                         Nats_eio.Jetstream.Error.pp error)
                | Ok options -> options
              in
              let options =
                match
                  Nats_eio.Jetstream.Publish_options.with_ttl
                    Mtime.Span.(2 * s)
                    options
                with
                | Error error ->
                    fail
                      (Format.asprintf "TTL option construction failed: %a"
                         Nats_eio.Jetstream.Error.pp error)
                | Ok options -> options
              in
              let publisher =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publisher.v ~sw ~clock ~max_pending:2
                     jetstream)
              in
              let future =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publisher.publish ~options publisher
                     (Nats.Subject.literal "orders.created")
                     "async-payload")
              in
              (match
                 Nats.Header.find "Nats-Msg-Id"
                   (Nats.Message.headers
                      (Nats_eio.Jetstream.Publish.message future))
               with
              | Some "async-id" -> ()
              | Some value -> fail ("unexpected message id " ^ value)
              | None -> fail "message id header was not attached");
              (match
                 Nats.Header.find "Nats-TTL"
                   (Nats.Message.headers
                      (Nats_eio.Jetstream.Publish.message future))
               with
              | Some value -> equal string "2s" value
              | None -> fail "message TTL header was not attached");
              equal int 1 (Nats_eio.Jetstream.Publisher.pending publisher);
              yield_n 8;
              Eio.Promise.resolve response_u
                (Ok (publish_ack_wire ~sid:1 ~stream:"ORDERS" ~sequence:7L));
              let ack =
                expect_jetstream_ok (Nats_eio.Jetstream.Publish.await future)
              in
              equal string "ORDERS" (Nats_eio.Jetstream.Publish_ack.stream ack);
              equal int64 7L (Nats_eio.Jetstream.Publish_ack.sequence ack);
              equal int 0 (Nats_eio.Jetstream.Publisher.pending publisher);
              expect_jetstream_ok
                (Nats_eio.Jetstream.Publisher.await_all publisher);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "publish options reject invalid optimistic-concurrency values"
        (fun () ->
          match
            Nats_eio.Jetstream.Publish_options.with_expected_last_sequence (-1L)
              Nats_eio.Jetstream.Publish_options.empty
          with
          | Error
              (Nats_eio.Jetstream.Error.Invalid_publish_option
                 { field = "expected_last_sequence"; _ }) ->
              ()
          | Ok _ -> fail "negative expected sequence was accepted"
          | Error error ->
              fail
                (Format.asprintf "unexpected option error: %a"
                   Nats_eio.Jetstream.Error.pp error));
      test "synchronous publishing retries no-responders with typed policy"
        (fun () ->
          let first, first_u = Eio.Promise.create () in
          let second, second_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:
              [ `Return info_wire; `Await first; `Await second; `Await hold ]
            (fun ~sw ~trace ~clock connection ->
              ignore trace;
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let options =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publish_options.with_retry
                     ~wait:Mtime.Span.(1 * ms)
                     ~attempts:(Some 1) Nats_eio.Jetstream.Publish_options.empty)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.publish ~options jetstream
                       (Nats.Subject.literal "orders.created")
                       "payload"));
              yield_n 8;
              Eio.Promise.resolve first_u (Ok (no_responders_wire ~sid:1));
              Eio.Time.Mono.sleep clock 0.005;
              Eio.Promise.resolve second_u
                (Ok (publish_ack_wire ~sid:2 ~stream:"ORDERS" ~sequence:9L));
              let ack = expect_jetstream_ok (Eio.Promise.await result) in
              equal int64 9L (Nats_eio.Jetstream.Publish_ack.sequence ack);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "synchronous publish timeout covers retry waits" (fun () ->
          let first, first_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await first; `Await hold ]
            (fun ~sw ~trace connection ->
              ignore trace;
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let options =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publish_options.with_retry
                     ~wait:Mtime.Span.(10 * ms)
                     ~attempts:(Some 1) Nats_eio.Jetstream.Publish_options.empty)
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.publish
                       ~timeout:Mtime.Span.(1 * ms)
                       ~options jetstream
                       (Nats.Subject.literal "orders.created")
                       "payload"));
              yield_n 8;
              Eio.Promise.resolve first_u (Ok (no_responders_wire ~sid:1));
              (match Eio.Promise.await result with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> fail "publish retried after its deadline"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected timeout error: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "synchronous publishing rejects asynchronous stall options"
        (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw connection ->
              ignore sw;
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let options =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publish_options.with_stall_wait
                     Mtime.Span.(1 * ms)
                     Nats_eio.Jetstream.Publish_options.empty)
              in
              (match
                 Nats_eio.Jetstream.publish ~options jetstream
                   (Nats.Subject.literal "orders.created")
                   "payload"
               with
              | Error
                  (Nats_eio.Jetstream.Error.Invalid_publish_option
                     { field = "stall_wait"; _ }) ->
                  ()
              | Ok _ -> fail "synchronous publish accepted stall_wait"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected publish error: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "cancelling an asynchronous publish settles and releases it"
        (fun () ->
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:[ `Return info_wire; `Await hold ]
            (fun ~sw ~trace ~clock connection ->
              ignore trace;
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let publisher =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publisher.v ~sw ~clock jetstream)
              in
              let future =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publisher.publish publisher
                     (Nats.Subject.literal "orders.created")
                     "payload")
              in
              yield_n 8;
              expect_jetstream_ok (Nats_eio.Jetstream.Publish.cancel future);
              (match Nats_eio.Jetstream.Publish.await future with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Closed) ->
                  ()
              | Ok _ -> fail "cancelled publish returned an acknowledgement"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected cancellation error: %a"
                       Nats_eio.Jetstream.Error.pp error));
              equal int 0 (Nats_eio.Jetstream.Publisher.pending publisher);
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "publish options emit the Go JetStream headers" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced_clock
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace ~clock connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let options =
                let ( let* ) = Result.bind in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_msg_id "message-1"
                    Nats_eio.Jetstream.Publish_options.empty
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_expected_stream
                    "ORDERS" options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_expected_last_msg_id
                    "message-0" options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_expected_last_sequence
                    5L options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options
                  .with_expected_last_sequence_for_subject ~sequence:3L
                    ~subject:(Nats.Subject.literal "orders.created")
                    options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_ttl
                    Mtime.Span.(2 * s)
                    options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_schedule
                    (Nats_eio.Jetstream.Publish_options.Cron "@daily") options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_schedule_target
                    (Nats.Subject.literal "orders.scheduled")
                    options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_schedule_source
                    (Nats.Subject.literal "orders.source")
                    options
                in
                let* options =
                  Nats_eio.Jetstream.Publish_options.with_schedule_ttl
                    Nats_eio.Jetstream.Publish_options.Never options
                in
                Nats_eio.Jetstream.Publish_options.with_schedule_timezone "UTC"
                  options
              in
              let options = expect_jetstream_ok options in
              let publisher =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publisher.v ~sw ~clock ~max_pending:1
                     jetstream)
              in
              let future =
                expect_jetstream_ok
                  (Nats_eio.Jetstream.Publisher.publish ~options publisher
                     (Nats.Subject.literal "orders.created")
                     "payload")
              in
              let headers =
                Nats.Message.headers (Nats_eio.Jetstream.Publish.message future)
              in
              let check_header name expected =
                match Nats.Header.find name headers with
                | Some value -> equal string expected value
                | None -> fail ("missing " ^ name)
              in
              check_header "Nats-Msg-Id" "message-1";
              check_header "Nats-Expected-Stream" "ORDERS";
              check_header "Nats-Expected-Last-Msg-Id" "message-0";
              check_header "Nats-Expected-Last-Sequence" "5";
              check_header "Nats-Expected-Last-Subject-Sequence" "3";
              check_header "Nats-Expected-Last-Subject-Sequence-Subject"
                "orders.created";
              check_header "Nats-TTL" "2s";
              check_header "Nats-Schedule" "@daily";
              check_header "Nats-Schedule-Target" "orders.scheduled";
              check_header "Nats-Schedule-Source" "orders.source";
              check_header "Nats-Schedule-TTL" "never";
              check_header "Nats-Schedule-Time-Zone" "UTC";
              yield_n 8;
              Eio.Promise.resolve response_u
                (Ok (publish_ack_wire ~sid:1 ~stream:"ORDERS" ~sequence:8L));
              ignore
                (expect_jetstream_ok (Nats_eio.Jetstream.Publish.await future));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test
        "atomic batches stage control headers and preserve batch \
         acknowledgements" (fun () ->
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:[ `Return info_wire; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let messages =
                [
                  Nats.Message.v
                    ~subject:(Nats.Subject.literal "orders.created")
                    "first";
                  Nats.Message.v
                    ~subject:(Nats.Subject.literal "orders.updated")
                    "second";
                ]
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Atomic_batch.publish ~id:"batch-1"
                       jetstream messages));
              yield_n 8;
              let trace = Buffer.contents trace in
              if not (contains_substring ~needle:"Nats-Batch-Id: batch-1" trace)
              then fail "atomic batch id was not sent";
              if not (contains_substring ~needle:"Nats-Batch-Sequence: 1" trace)
              then fail "atomic batch sequence was not sent";
              if not (contains_substring ~needle:"Nats-Batch-Sequence: 2" trace)
              then fail "atomic commit sequence was not sent";
              if not (contains_substring ~needle:"Nats-Batch-Commit: 1" trace)
              then fail "atomic commit marker was not sent";
              Eio.Promise.resolve response_u
                (Ok
                   (publish_batch_ack_wire ~sid:1 ~stream:"ORDERS" ~sequence:8L
                      ~batch:"batch-1" ~count:2));
              let ack = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "batch-1"
                (Option.get (Nats_eio.Jetstream.Publish_ack.batch ack));
              equal int64 2L
                (Option.get (Nats_eio.Jetstream.Publish_ack.count ack));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "fast batches report a server sequence gap" (fun () ->
          let flow, flow_u = Eio.Promise.create () in
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [ `Return info_wire; `Await flow; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              ignore trace;
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let messages =
                [
                  Nats.Message.v
                    ~subject:(Nats.Subject.literal "orders.1")
                    "one";
                  Nats.Message.v
                    ~subject:(Nats.Subject.literal "orders.2")
                    "two";
                ]
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Batch.publish ~flow:2 ~id:"gap-1"
                       jetstream messages));
              yield_n 8;
              Eio.Promise.resolve flow_u
                (Ok (batch_flow_gap_wire ~sid:1 ~expected:1L ~actual:2L));
              yield_n 8;
              Eio.Promise.resolve response_u
                (Ok
                   (publish_batch_ack_wire ~sid:1 ~stream:"ORDERS" ~sequence:9L
                      ~batch:"gap-1" ~count:2));
              (match Eio.Promise.await result with
              | Error
                  (Nats_eio.Jetstream.Error.Batch_gap
                     { expected = 1L; actual = 2L }) ->
                  ()
              | Ok _ -> fail "fast batch gap was accepted"
              | Error error ->
                  fail
                    (Format.asprintf "unexpected fast batch error: %a"
                       Nats_eio.Jetstream.Error.pp error));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
      test "fast batches use the flow reply protocol" (fun () ->
          let flow, flow_u = Eio.Promise.create () in
          let response, response_u = Eio.Promise.create () in
          let hold, hold_u = Eio.Promise.create () in
          with_connection_traced
            ~reads:
              [ `Return info_wire; `Await flow; `Await response; `Await hold ]
            (fun ~sw ~trace connection ->
              let jetstream =
                expect_jetstream_ok (Nats_eio.Jetstream.v connection)
              in
              let messages =
                [
                  Nats.Message.v
                    ~subject:(Nats.Subject.literal "orders.1")
                    "one";
                  Nats.Message.v
                    ~subject:(Nats.Subject.literal "orders.2")
                    "two";
                ]
              in
              let result, result_u = Eio.Promise.create () in
              Eio.Fiber.fork ~sw (fun () ->
                  Eio.Promise.resolve result_u
                    (Nats_eio.Jetstream.Batch.publish ~flow:2 ~id:"fast-1"
                       jetstream messages));
              yield_n 8;
              let trace = Buffer.contents trace in
              if not (contains_substring ~needle:"fast-1.2.fail.1.0.$FI" trace)
              then fail "fast batch start reply subject was not sent";
              if not (contains_substring ~needle:"fast-1.2.fail.2.2.$FI" trace)
              then fail "fast batch commit reply subject was not sent";
              Eio.Promise.resolve flow_u
                (Ok (batch_flow_ack_wire ~sid:1 ~sequence:0L ~messages:2));
              yield_n 8;
              Eio.Promise.resolve response_u
                (Ok
                   (publish_batch_ack_wire ~sid:1 ~stream:"ORDERS" ~sequence:9L
                      ~batch:"fast-1" ~count:2));
              let ack = expect_jetstream_ok (Eio.Promise.await result) in
              equal string "ORDERS" (Nats_eio.Jetstream.Publish_ack.stream ack);
              equal int64 9L (Nats_eio.Jetstream.Publish_ack.sequence ack);
              equal int64 2L
                (Option.get (Nats_eio.Jetstream.Publish_ack.count ack));
              expect_ok (Nats_eio.Connection.close connection);
              Eio.Promise.resolve hold_u (Error End_of_file)));
    ]
