let expect_packet = function
  | Ok packet -> packet
  | Error error ->
      invalid_arg
        (Format.asprintf "packet fixture: %a" Nats.Packet.pp_error error)

let expect_operation = function
  | Ok operation -> operation
  | Error error ->
      invalid_arg
        (Format.asprintf "codec fixture: %a" Nats.Codec.pp_error error)

let expect_client = function
  | Ok transition -> transition
  | Error error ->
      invalid_arg (Format.asprintf "client fixture: %a" Nats.Error.pp error)

let expect_headers = function
  | Ok headers -> headers
  | Error error ->
      invalid_arg
        (Format.asprintf "header fixture: %a" Nats.Header.pp_error error)

let repeating_reader chunks =
  let slices = Array.map Bytesrw.Bytes.Slice.of_string chunks in
  let next = ref 0 in
  Bytesrw.Bytes.Reader.make (fun () ->
      let slice = Array.get slices !next in
      next :=
        if Int.equal (!next + 1) (Array.length slices) then 0 else !next + 1;
      slice)

let chunks_of_size size value =
  let length = String.length value in
  let count = (length + size - 1) / size in
  Array.init count (fun index ->
      let first = index * size in
      String.sub value first (Int.min size (length - first)))

let payload size = String.init size (fun index -> Char.chr (index land 0xff))

let message_wire sid payload =
  Printf.sprintf "MSG bench.events %d %d\r\n%s\r\n" sid (String.length payload)
    payload

let header_entries =
  List.init 16 (fun index ->
      ( Printf.sprintf "X-Bench-%02d" index,
        String.make 16 (Char.chr (65 + index)) ))

let headers = expect_headers (Nats.Header.of_list header_entries)

let header_block =
  let buffer = Buffer.create 512 in
  Buffer.add_string buffer "NATS/1.0\r\n";
  List.iter
    (fun (name, value) ->
      Buffer.add_string buffer name;
      Buffer.add_string buffer ": ";
      Buffer.add_string buffer value;
      Buffer.add_string buffer "\r\n")
    header_entries;
  Buffer.add_string buffer "\r\n";
  Buffer.contents buffer

let header_message_wire payload =
  let header_bytes = String.length header_block in
  let total_bytes = header_bytes + String.length payload in
  Printf.sprintf "HMSG bench.events 1 %d %d\r\n%s%s\r\n" header_bytes
    total_bytes header_block payload

let payload_0 = Thumper.black_box ""
let payload_64 = Thumper.black_box (payload 64)
let payload_4096 = Thumper.black_box (payload 4096)
let payload_1m = Thumper.black_box (payload (1024 * 1024))
let ping_wire = Thumper.black_box "PING\r\n"
let message_0_wire = Thumper.black_box (message_wire 1 payload_0)
let message_64_wire = Thumper.black_box (message_wire 1 payload_64)
let message_4096_wire = Thumper.black_box (message_wire 1 payload_4096)
let message_1m_wire = Thumper.black_box (message_wire 1 payload_1m)

let header_message_4096_wire =
  Thumper.black_box (header_message_wire payload_4096)

let coalesced_message_64_wire =
  Thumper.black_box
    (String.concat "" (List.init 16 (Fun.const message_64_wire)))

let subject = Thumper.black_box (Nats.Subject.literal "bench.events")
let message_64 = Thumper.black_box (Nats.Message.v ~subject payload_64)
let message_4096 = Thumper.black_box (Nats.Message.v ~subject payload_4096)
let message_1m = Thumper.black_box (Nats.Message.v ~subject payload_1m)

let header_message_4096 =
  Thumper.black_box (Nats.Message.v ~headers ~subject payload_4096)

let pub_64 = Thumper.black_box (Nats.Op.Pub message_64)
let pub_4096 = Thumper.black_box (Nats.Op.Pub message_4096)
let pub_1m = Thumper.black_box (Nats.Op.Pub message_1m)

let hpub_4096 =
  Thumper.black_box
    (Nats.Op.Hpub { message = header_message_4096; status = None })

let info_wire =
  "INFO {\"server_id\":\"bench\",\"version\":\"2.14.6\",\"proto\":1,"
  ^ "\"max_payload\":2097152,\"headers\":true,\"no_responders\":true,"
  ^ "\"connect_urls\":[]}\r\n"

let connected_client subscription_count =
  let initial = Nats.Client.v Nats.Config.default in
  let info =
    expect_client
      (Nats.Client.incoming ~eod:true initial ~now:Mtime.min_stamp
         (Bytesrw.Bytes.Reader.of_string info_wire))
  in
  let connected =
    expect_client
      (Nats.Client.outgoing info.state
         (Nats.Client.Connect
            { credentials = Nats.Client.Connect.v (); tls_required = false }))
  in
  let state = ref connected.state in
  let remaining = ref subscription_count in
  let filter = Nats.Subject.Filter.literal "bench.events" in
  while not (Int.equal !remaining 0) do
    let subscribed =
      expect_client
        (Nats.Client.outgoing !state
           (Nats.Client.Subscribe { subject = filter; queue_group = None }))
    in
    state := subscribed.state;
    remaining := !remaining - 1
  done;
  !state

let one_subscription = Thumper.black_box (connected_client 1)
let many_subscriptions = Thumper.black_box (connected_client 1024)

let packet_case name chunks =
  Thumper.bench_with_setup name
    ~setup:(fun () -> repeating_reader chunks)
    (fun reader -> expect_packet (Nats.Packet.read ~eod:true reader))

let codec_case name chunks =
  Thumper.bench_with_setup name
    ~setup:(fun () -> repeating_reader chunks)
    (fun reader -> expect_operation (Nats.Codec.read ~eod:true reader))

let coalesced_codec_case =
  Thumper.bench_with_setup "msg-64-coalesced-16"
    ~setup:(fun () -> repeating_reader [| coalesced_message_64_wire |])
    (fun reader ->
      let remaining = ref 16 in
      let payload_bytes = ref 0 in
      while not (Int.equal !remaining 0) do
        (match expect_operation (Nats.Codec.read ~eod:true reader) with
        | Nats.Op.Msg { message; sid = _ } ->
            payload_bytes :=
              !payload_bytes + String.length (Nats.Message.payload message)
        | Nats.Op.Info _ | Nats.Op.Connect _ | Nats.Op.Pub _ | Nats.Op.Hpub _
        | Nats.Op.Sub _ | Nats.Op.Unsub _ | Nats.Op.Hmsg _ | Nats.Op.Ping
        | Nats.Op.Pong | Nats.Op.Ok | Nats.Op.Server_error _ ->
            invalid_arg "coalesced codec fixture");
        remaining := !remaining - 1
      done;
      !payload_bytes)

let client_case name state wire =
  Thumper.bench_with_setup name
    ~setup:(fun () -> repeating_reader [| wire |])
    (fun reader ->
      expect_client
        (Nats.Client.incoming ~eod:true state ~now:Mtime.min_stamp reader))

let budgets =
  [ Thumper.Budget.no_slower_than 0.10; Thumper.Budget.no_more_alloc_than 0.0 ]

let () =
  Thumper.run "nats-protocol" ~budgets
    [
      Thumper.group "packet"
        [
          packet_case "ping" [| ping_wire |];
          packet_case "msg-0" [| message_0_wire |];
          packet_case "msg-64" [| message_64_wire |];
          packet_case "msg-4096" [| message_4096_wire |];
          packet_case "msg-1m" [| message_1m_wire |];
          packet_case "hmsg-4096-headers-16" [| header_message_4096_wire |];
          packet_case "msg-4096-fragmented-1024"
            (chunks_of_size 1024 message_4096_wire);
        ];
      Thumper.group "codec"
        [
          codec_case "ping" [| ping_wire |];
          codec_case "msg-64" [| message_64_wire |];
          codec_case "msg-4096" [| message_4096_wire |];
          codec_case "hmsg-4096-headers-16" [| header_message_4096_wire |];
          coalesced_codec_case;
        ];
      Thumper.group "encode"
        [
          Thumper.bench "wire-pub-64" (fun () -> Nats.Codec.encode pub_64);
          Thumper.bench "wire-pub-4096" (fun () -> Nats.Codec.encode pub_4096);
          Thumper.bench "wire-pub-1m" (fun () -> Nats.Codec.encode pub_1m);
          Thumper.bench "wire-hpub-4096-headers-16" (fun () ->
              Nats.Codec.encode hpub_4096);
        ];
      Thumper.group "client"
        [
          client_case "deliver-one-subscription" one_subscription
            message_64_wire;
          client_case "deliver-1024-subscriptions" many_subscriptions
            message_64_wire;
          client_case "unknown-sid-1024-subscriptions" many_subscriptions
            (Thumper.black_box (message_wire 2048 payload_64));
        ];
    ]
