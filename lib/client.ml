type phase = Awaiting_info | Awaiting_connect | Connected | Draining | Closed

module Connect = struct
  type t = {
    auth_token : string option;
    user : string option;
    pass : string option;
    jwt : string option;
    nkey : string option;
    signature : string option;
  }

  let v ?auth_token ?user ?pass ?jwt ?nkey ?signature () =
    { auth_token; user; pass; jwt; nkey; signature }
end

type connect_json = {
  verbose : bool;
  pedantic : bool;
  tls_required : bool;
  name : string option;
  lang : string;
  version : string;
  protocol : int;
  echo : bool;
  headers : bool;
  no_responders : bool;
  auth_token : string option;
  user : string option;
  pass : string option;
  jwt : string option;
  nkey : string option;
  signature : string option;
}

let connect_codec =
  Jsont.Object.map ~kind:"NATS CONNECT"
    (fun
      verbose
      pedantic
      tls_required
      name
      lang
      version
      protocol
      echo
      headers
      no_responders
      auth_token
      user
      pass
      jwt
      nkey
      signature
    ->
      {
        verbose;
        pedantic;
        tls_required;
        name;
        lang;
        version;
        protocol;
        echo;
        headers;
        no_responders;
        auth_token;
        user;
        pass;
        jwt;
        nkey;
        signature;
      })
  |> Jsont.Object.mem "verbose" Jsont.bool ~enc:(fun value -> value.verbose)
  |> Jsont.Object.mem "pedantic" Jsont.bool ~enc:(fun value -> value.pedantic)
  |> Jsont.Object.mem "tls_required" Jsont.bool ~enc:(fun value ->
      value.tls_required)
  |> Jsont.Object.opt_mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "lang" Jsont.string ~enc:(fun value -> value.lang)
  |> Jsont.Object.mem "version" Jsont.string ~enc:(fun value -> value.version)
  |> Jsont.Object.mem "protocol" Jsont.int ~enc:(fun value -> value.protocol)
  |> Jsont.Object.mem "echo" Jsont.bool ~enc:(fun value -> value.echo)
  |> Jsont.Object.mem "headers" Jsont.bool ~enc:(fun value -> value.headers)
  |> Jsont.Object.mem "no_responders" Jsont.bool ~enc:(fun value ->
      value.no_responders)
  |> Jsont.Object.opt_mem "auth_token" Jsont.string ~enc:(fun value ->
      value.auth_token)
  |> Jsont.Object.opt_mem "user" Jsont.string ~enc:(fun value -> value.user)
  |> Jsont.Object.opt_mem "pass" Jsont.string ~enc:(fun value -> value.pass)
  |> Jsont.Object.opt_mem "jwt" Jsont.string ~enc:(fun value -> value.jwt)
  |> Jsont.Object.opt_mem "nkey" Jsont.string ~enc:(fun value -> value.nkey)
  |> Jsont.Object.opt_mem "sig" Jsont.string ~enc:(fun value -> value.signature)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

type subscription = {
  sid : int;
  subject : Subject.Filter.t;
  queue_group : Queue_group.t option;
  remaining : int option;
}

type t = {
  config : Config.t;
  phase : phase;
  info : Info.t option;
  next_sid : int;
  subscriptions : subscription list;
  pending_flushes : int;
  pending_liveness_pings : int;
  next_ping : Mtime.t option;
}

type command =
  | Connect of Connect.t
  | Publish of Message.t
  | Subscribe of {
      subject : Subject.Filter.t;
      queue_group : Queue_group.t option;
    }
  | Unsubscribe of { sid : int }
  | Auto_unsubscribe of { sid : int; max_messages : int }
  | Flush
  | Drain
  | Close

type delivery = { sid : int; message : Message.t; status : Op.status option }

type transition = {
  state : t;
  output : string list;
  events : Event.t list;
  deliveries : delivery list;
  subscription_id : int option;
}

let v config =
  {
    config;
    phase = Awaiting_info;
    info = None;
    next_sid = 1;
    subscriptions = [];
    pending_flushes = 0;
    pending_liveness_pings = 0;
    next_ping = None;
  }

let phase value = value.phase
let info value = value.info
let subscriptions value = List.rev value.subscriptions

let empty_transition state =
  { state; output = []; events = []; deliveries = []; subscription_id = None }

let closed_transition state =
  {
    state = { state with phase = Closed; next_ping = None };
    output = [];
    events = [ Event.Closed ];
    deliveries = [];
    subscription_id = None;
  }

let next_ping config now =
  match Config.ping_interval config with
  | None -> None
  | Some interval -> Mtime.add_span now interval

let touch state now = { state with next_ping = next_ping state.config now }
let error_of_codec error = Error.Codec error

let encode ~limits operation =
  match Codec.encode ~limits operation with
  | Ok output -> Ok output
  | Error error -> Error (error_of_codec error)

let negotiated_limits state =
  match state.info with
  | None -> Packet.default_limits
  | Some info ->
      { Packet.default_limits with max_payload_bytes = Info.max_payload info }

let encode_with_state state operation =
  encode ~limits:(negotiated_limits state) operation

let encode_all state operations =
  let rec loop remaining reversed =
    match remaining with
    | [] -> Ok (List.rev reversed)
    | operation :: tail -> (
        match encode_with_state state operation with
        | Error error -> Error error
        | Ok output -> loop tail (output :: reversed))
  in
  loop operations []

let find_subscription (sid : int) (subscriptions : subscription list) =
  List.find_opt
    (fun (subscription : subscription) -> Int.equal subscription.sid sid)
    subscriptions

let remove_subscription (sid : int) (subscriptions : subscription list) =
  List.filter
    (fun (subscription : subscription) -> not (Int.equal subscription.sid sid))
    subscriptions

let require_connected state =
  match state.phase with
  | Connected -> Ok ()
  | Awaiting_info | Awaiting_connect -> Error Error.Not_connected
  | Draining -> Error Error.Draining
  | Closed -> Error Error.Closed

let server_error_message value =
  let length = String.length value in
  if
    length >= 2
    && Char.equal (String.get value 0) '\''
    && Char.equal (String.get value (length - 1)) '\''
  then String.sub value 1 (length - 2)
  else value

let info_events info =
  if Info.lame_duck_mode info then [ Event.Info info; Event.Lame_duck_mode ]
  else [ Event.Info info ]

let incoming_message (state : t) now sid message status =
  match find_subscription sid state.subscriptions with
  | None -> Ok (empty_transition state)
  | Some subscription ->
      let subscriptions =
        match subscription.remaining with
        | None -> state.subscriptions
        | Some 1 -> remove_subscription sid state.subscriptions
        | Some remaining ->
            let replacement =
              { subscription with remaining = Some (remaining - 1) }
            in
            List.map
              (fun (value : subscription) ->
                if Int.equal value.sid sid then replacement else value)
              state.subscriptions
      in
      Ok
        {
          state = touch { state with subscriptions } now;
          output = [];
          events = [];
          deliveries = [ { sid; message; status } ];
          subscription_id = None;
        }

let handle_operation state now operation =
  match state.phase with
  | Closed -> Error Error.Closed
  | Awaiting_info -> (
      match operation with
      | Op.Info json -> (
          match Info.of_string json with
          | Error error -> Error (Error.Invalid_info error)
          | Ok info ->
              Ok
                {
                  state =
                    touch
                      { state with phase = Awaiting_connect; info = Some info }
                      now;
                  output = [];
                  events = info_events info;
                  deliveries = [];
                  subscription_id = None;
                })
      | Op.Ping ->
          encode_with_state state Op.Pong
          |> Result.map (fun output ->
              { (empty_transition (touch state now)) with output = [ output ] })
      | _ -> Error (Error.Unexpected_operation operation))
  | Awaiting_connect -> (
      match operation with
      | Op.Info json -> (
          match Info.of_string json with
          | Error error -> Error (Error.Invalid_info error)
          | Ok info ->
              Ok
                {
                  state = touch { state with info = Some info } now;
                  output = [];
                  events = info_events info;
                  deliveries = [];
                  subscription_id = None;
                })
      | Op.Ping ->
          encode_with_state state Op.Pong
          |> Result.map (fun output ->
              { (empty_transition (touch state now)) with output = [ output ] })
      | _ -> Error (Error.Unexpected_operation operation))
  | Connected | Draining -> (
      match operation with
      | Op.Info json -> (
          match Info.of_string json with
          | Error error -> Error (Error.Invalid_info error)
          | Ok info ->
              Ok
                {
                  state = touch { state with info = Some info } now;
                  output = [];
                  events = info_events info;
                  deliveries = [];
                  subscription_id = None;
                })
      | Op.Ping ->
          encode_with_state state Op.Pong
          |> Result.map (fun output ->
              { (empty_transition (touch state now)) with output = [ output ] })
      | Op.Pong ->
          let pending_flushes = state.pending_flushes in
          let pending_liveness_pings = state.pending_liveness_pings in
          let events, pending_flushes, pending_liveness_pings =
            if pending_flushes > 0 then
              ( [ Event.Flush_completed ],
                pending_flushes - 1,
                pending_liveness_pings )
            else if pending_liveness_pings > 0 then
              ([], pending_flushes, pending_liveness_pings - 1)
            else ([ Event.Protocol_notice Event.Pong ], 0, 0)
          in
          Ok
            {
              state =
                touch { state with pending_flushes; pending_liveness_pings } now;
              output = [];
              events;
              deliveries = [];
              subscription_id = None;
            }
      | Op.Ok ->
          Ok
            {
              state = touch state now;
              output = [];
              events = [ Event.Protocol_notice Event.Ok ];
              deliveries = [];
              subscription_id = None;
            }
      | Op.Server_error message ->
          Ok
            {
              state = touch state now;
              output = [];
              events =
                [
                  Event.Server_error { message = server_error_message message };
                ];
              deliveries = [];
              subscription_id = None;
            }
      | Op.Msg { sid; message } -> incoming_message state now sid message None
      | Op.Hmsg { sid; message; status } ->
          incoming_message state now sid message status
      | _ -> Error (Error.Unexpected_operation operation))

let incoming ?(eod = false) state ~now reader =
  match Codec.read ~eod reader with
  | Ok operation -> handle_operation state now operation
  | Error (Codec.Packet Packet.Need_more) -> Ok (empty_transition state)
  | Error (Codec.Packet Packet.End_of_input) -> Ok (closed_transition state)
  | Error (Codec.Packet error) -> Error (Error.Packet error)
  | Error error -> Error (Error.Codec error)

let outgoing_connect state (credentials : Connect.t) =
  match state.phase with
  | Awaiting_info -> Error Error.Info_not_received
  | Awaiting_connect -> (
      let info =
        match state.info with Some info -> info | None -> assert false
      in
      let value : connect_json =
        {
          verbose = false;
          pedantic = false;
          tls_required = false;
          name = Config.name state.config;
          lang = Config.language state.config;
          version = Config.version state.config;
          protocol = Config.protocol state.config;
          echo = not (Config.no_echo state.config);
          headers = Config.headers state.config;
          no_responders =
            Config.no_responders state.config && Info.no_responders info;
          auth_token = credentials.auth_token;
          user = credentials.user;
          pass = credentials.pass;
          jwt = credentials.jwt;
          nkey = credentials.nkey;
          signature = credentials.signature;
        }
      in
      match Jsont_bytesrw.encode_string' connect_codec value with
      | Error error -> Error (Error.Invalid_info (Info.Invalid_json error))
      | Ok json -> (
          match encode_with_state state (Op.Connect json) with
          | Error error -> Error error
          | Ok output ->
              Ok
                {
                  state = { state with phase = Connected };
                  output = [ output ];
                  events = [ Event.Connected ];
                  deliveries = [];
                  subscription_id = None;
                }))
  | Connected -> Error Error.Already_connected
  | Draining -> Error Error.Draining
  | Closed -> Error Error.Closed

let outgoing_publish state message =
  match require_connected state with
  | Error error -> Error error
  | Ok () -> (
      let headers = Message.headers message in
      if
        (not (Header.is_empty headers))
        &&
        match state.info with
        | Some info -> not (Info.headers info)
        | None -> true
      then Error Error.Headers_not_negotiated
      else
        let operation =
          if Header.is_empty headers then Op.Pub message
          else Op.Hpub { message; status = None }
        in
        match encode_with_state state operation with
        | Error (Error.Codec (Codec.Packet (Packet.Payload_too_large _))) ->
            let limit =
              match state.info with
              | Some info -> Info.max_payload info
              | None -> Packet.default_limits.max_payload_bytes
            in
            Error
              (Error.Max_payload_exceeded
                 { size = String.length (Message.payload message); limit })
        | Error error -> Error error
        | Ok output -> Ok { (empty_transition state) with output = [ output ] })

let outgoing_subscribe state subject queue_group =
  match require_connected state with
  | Error error -> Error error
  | Ok () -> (
      let sid = state.next_sid in
      let operation = Op.Sub { subject; queue_group; sid } in
      match encode_with_state state operation with
      | Error error -> Error error
      | Ok output ->
          let subscription = { sid; subject; queue_group; remaining = None } in
          Ok
            {
              state =
                {
                  state with
                  next_sid = sid + 1;
                  subscriptions = subscription :: state.subscriptions;
                };
              output = [ output ];
              events = [];
              deliveries = [];
              subscription_id = Some sid;
            })

let outgoing_unsubscribe state sid max_messages =
  match require_connected state with
  | Error error -> Error error
  | Ok () -> (
      if sid <= 0 then Error (Error.Invalid_subscription_id sid)
      else
        match find_subscription sid state.subscriptions with
        | None -> Error (Error.Unknown_subscription { sid })
        | Some _ -> (
            let operation = Op.Unsub { sid; max_messages } in
            match encode_with_state state operation with
            | Error error -> Error error
            | Ok output ->
                let subscriptions =
                  match max_messages with
                  | None -> remove_subscription sid state.subscriptions
                  | Some max_messages ->
                      List.map
                        (fun (subscription : subscription) ->
                          if Int.equal subscription.sid sid then
                            { subscription with remaining = Some max_messages }
                          else subscription)
                        state.subscriptions
                in
                Ok
                  {
                    (empty_transition { state with subscriptions }) with
                    output = [ output ];
                  }))

let outgoing_flush state =
  match state.phase with
  | Connected | Draining -> (
      match encode_with_state state Op.Ping with
      | Error error -> Error error
      | Ok output ->
          Ok
            {
              (empty_transition
                 { state with pending_flushes = state.pending_flushes + 1 })
              with
              output = [ output ];
            })
  | Awaiting_info | Awaiting_connect -> Error Error.Not_connected
  | Closed -> Error Error.Closed

let outgoing_drain state =
  match state.phase with
  | Connected -> (
      let operations =
        List.map
          (fun (subscription : subscription) ->
            Op.Unsub { sid = subscription.sid; max_messages = None })
          (List.rev state.subscriptions)
      in
      let operations = operations @ [ Op.Ping ] in
      match encode_all state operations with
      | Error error -> Error error
      | Ok output ->
          Ok
            {
              state =
                {
                  state with
                  phase = Draining;
                  subscriptions = [];
                  pending_flushes = state.pending_flushes + 1;
                };
              output;
              events = [ Event.Draining ];
              deliveries = [];
              subscription_id = None;
            })
  | Awaiting_info | Awaiting_connect -> Error Error.Not_connected
  | Draining -> Error Error.Draining
  | Closed -> Error Error.Closed

let outgoing state command =
  match command with
  | Connect credentials -> outgoing_connect state credentials
  | Publish message -> outgoing_publish state message
  | Subscribe { subject; queue_group } ->
      outgoing_subscribe state subject queue_group
  | Unsubscribe { sid } -> outgoing_unsubscribe state sid None
  | Auto_unsubscribe { sid; max_messages } ->
      if max_messages <= 0 then Error (Error.Invalid_max_messages max_messages)
      else outgoing_unsubscribe state sid (Some max_messages)
  | Flush -> outgoing_flush state
  | Drain -> outgoing_drain state
  | Close -> (
      match state.phase with
      | Closed -> Error Error.Closed
      | _ -> Ok (closed_transition state))

let timer state ~now =
  match (state.phase, state.next_ping) with
  | Connected, Some deadline -> (
      if Mtime.is_earlier now ~than:deadline then empty_transition state
      else
        let maximum = Config.max_pings_without_pong state.config in
        if state.pending_liveness_pings >= maximum then closed_transition state
        else
          let state =
            {
              state with
              next_ping = next_ping state.config now;
              pending_liveness_pings = state.pending_liveness_pings + 1;
            }
          in
          match encode_with_state state Op.Ping with
          | Error _ -> closed_transition state
          | Ok output -> { (empty_transition state) with output = [ output ] })
  | _ -> empty_transition state

let next_timeout state =
  match state.phase with
  | Connected -> state.next_ping
  | Awaiting_info | Awaiting_connect | Draining | Closed -> None
