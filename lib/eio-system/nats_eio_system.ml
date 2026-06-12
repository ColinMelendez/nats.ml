module Error = struct
  type api = {
    code : int;
    err_code : int option;
    description : string;
    payload : Jsont.json;
  }

  type t =
    | Connection of Nats_eio.Error.t
    | Decode of Jsont.Error.t
    | Encode of Jsont.Error.t
    | Invalid_identifier of { kind : string; value : string }
    | Invalid_client_id of int64
    | Invalid_selector_tag of string
    | Invalid_timeout
    | Invalid_option of { name : string; reason : string }
    | Invalid_target_endpoint of { target : string; endpoint : string }
    | Unexpected_response of string
    | Unexpected_status of { code : int; description : string }
    | Server of api

  let pp_json ppf value = Format.fprintf ppf "%a" Jsont.pp_json value

  let pp ppf = function
    | Connection error ->
        Format.fprintf ppf "connection: %a" Nats_eio.Error.pp error
    | Decode error -> Format.fprintf ppf "JSON decode: %a" Jsont.Error.pp error
    | Encode error -> Format.fprintf ppf "JSON encode: %a" Jsont.Error.pp error
    | Invalid_identifier { kind; value } ->
        Format.fprintf ppf "invalid %s identifier %S" kind value
    | Invalid_client_id client_id ->
        Format.fprintf ppf "invalid client id %Ld" client_id
    | Invalid_selector_tag tag ->
        Format.fprintf ppf "invalid empty selector tag %S" tag
    | Invalid_timeout ->
        Format.pp_print_string ppf "invalid system request timeout"
    | Invalid_option { name; reason } ->
        Format.fprintf ppf "invalid system option %s: %s" name reason
    | Invalid_target_endpoint { target; endpoint } ->
        Format.fprintf ppf "endpoint %s is not valid for target %s" endpoint
          target
    | Unexpected_response message -> Format.pp_print_string ppf message
    | Unexpected_status { code; description } ->
        Format.fprintf ppf "system request status %d: %s" code description
    | Server { code; err_code; description; payload } -> (
        match err_code with
        | None ->
            Format.fprintf ppf "system API error %d: %s (%a)" code description
              pp_json payload
        | Some err_code ->
            Format.fprintf ppf "system API error %d/%d: %s (%a)" code err_code
              description pp_json payload)
end

type t = { connection : Nats_eio.Connection.t }
type system = t

let ( let* ) = Result.bind
let v connection = { connection }
let connection system = system.connection

let invalid_character character =
  let code = Char.code character in
  Int.compare code 32 <= 0 || Int.equal code 127

let validate_identifier ~kind value =
  let length = String.length value in
  if Int.equal length 0 then Error (Error.Invalid_identifier { kind; value })
  else
    let invalid = ref false in
    for index = 0 to length - 1 do
      let character = String.get value index in
      if
        invalid_character character
        || Char.equal character '.' || Char.equal character '*'
        || Char.equal character '>'
      then invalid := true
    done;
    if !invalid then Error (Error.Invalid_identifier { kind; value }) else Ok ()

let validate_text ~kind value =
  if Int.equal (String.length value) 0 then
    Error (Error.Invalid_identifier { kind; value })
  else
    let invalid = ref false in
    for index = 0 to String.length value - 1 do
      if invalid_character (String.get value index) then invalid := true
    done;
    if !invalid then Error (Error.Invalid_identifier { kind; value }) else Ok ()

module Target = struct
  type t = All | Server of string | Account of string

  let all = All

  let server value =
    match validate_identifier ~kind:"server" value with
    | Ok () -> Ok (Server value)
    | Error error -> Error error

  let account value =
    match validate_identifier ~kind:"account" value with
    | Ok () -> Ok (Account value)
    | Error error -> Error error

  let pp ppf = function
    | All -> Format.pp_print_string ppf "all servers"
    | Server value -> Format.fprintf ppf "server %S" value
    | Account value -> Format.fprintf ppf "account %S" value
end

module Selector = struct
  type t = {
    server_name : string option;
    cluster : string option;
    host : string option;
    exact_match : bool;
    tags : string list;
    domain : string option;
  }

  let empty =
    {
      server_name = None;
      cluster = None;
      host = None;
      exact_match = false;
      tags = [];
      domain = None;
    }

  let validate_optional ~kind = function
    | None -> Ok None
    | Some value -> (
        match validate_text ~kind value with
        | Ok () -> Ok (Some value)
        | Error error -> Error error)

  let v ?server_name ?cluster ?host ?(exact_match = false) ?(tags = []) ?domain
      () =
    let* server_name = validate_optional ~kind:"server name" server_name in
    let* cluster = validate_optional ~kind:"cluster" cluster in
    let* host = validate_optional ~kind:"host" host in
    let* domain = validate_optional ~kind:"JetStream domain" domain in
    let invalid_tag = ref None in
    List.iter
      (fun tag ->
        match validate_text ~kind:"selector tag" tag with
        | Ok () -> ()
        | Error _ -> if Option.is_none !invalid_tag then invalid_tag := Some tag)
      tags;
    match !invalid_tag with
    | Some tag -> Error (Error.Invalid_selector_tag tag)
    | None -> Ok { server_name; cluster; host; exact_match; tags; domain }

  let server_name value = value.server_name
  let cluster value = value.cluster
  let host value = value.host
  let exact_match value = value.exact_match
  let tags value = value.tags
  let domain value = value.domain
end

let json_member name value = Jsont.Json.mem (Jsont.Json.name name) value

let selector_json (selector : Selector.t) =
  let members = ref [] in
  let add name value = members := json_member name value :: !members in
  Option.iter
    (fun value -> add "server_name" (Jsont.Json.string value))
    selector.server_name;
  Option.iter
    (fun value -> add "cluster" (Jsont.Json.string value))
    selector.cluster;
  Option.iter (fun value -> add "host" (Jsont.Json.string value)) selector.host;
  if selector.exact_match then add "exact_match" (Jsont.Json.bool true);
  (match selector.tags with
  | [] -> ()
  | _ -> add "tags" (Jsont.Json.list (List.map Jsont.Json.string selector.tags)));
  Option.iter
    (fun value -> add "domain" (Jsont.Json.string value))
    selector.domain;
  Jsont.Json.object' (List.rev !members)

let encode_json value =
  match Jsont_bytesrw.encode_string' Jsont.json value with
  | Ok value -> Ok value
  | Error error -> Error (Error.Encode error)

let decode_json value =
  match Jsont_bytesrw.decode_string' Jsont.json value with
  | Ok value -> Ok value
  | Error error -> Error (Error.Decode error)

let json_field name = function
  | Jsont.Object (members, _) ->
      Option.map snd (Jsont.Json.find_mem name members)
  | _ -> None

let decode_with codec value =
  match Jsont.Json.decode' codec value with
  | Ok value -> Ok value
  | Error error -> Error (Error.Decode error)

let required_field name codec json =
  match json_field name json with
  | Some value -> decode_with codec value
  | None ->
      Error (Error.Unexpected_response ("system response is missing " ^ name))

let decode_api_error json =
  let* code = required_field "code" Jsont.int json in
  let* description = required_field "description" Jsont.string json in
  let* err_code =
    match json_field "err_code" json with
    | None -> Ok None
    | Some value -> decode_with Jsont.int value |> Result.map Option.some
  in
  Ok { Error.code; err_code; description; payload = json }

module Monitor = struct
  module Options = struct
    type t = Jsont.mem list

    type sort =
      | Cid
      | Start
      | Subs
      | Pending
      | Out_msgs
      | In_msgs
      | Out_bytes
      | In_bytes
      | Last
      | Idle
      | Uptime
      | Stop
      | Reason
      | Rtt

    type state = Open | Closed | All

    let empty = []

    let build f =
      let members = ref [] in
      let* () = f members in
      Ok (List.rev !members)

    let add name value members = members := json_member name value :: !members

    let add_option name encode value members =
      Option.iter (fun value -> add name (encode value) members) value;
      Ok ()

    let add_nonnegative_int name value members =
      match value with
      | None -> Ok ()
      | Some value when Int.compare value 0 >= 0 ->
          add name (Jsont.Json.int value) members;
          Ok ()
      | Some value ->
          Error
            (Error.Invalid_option
               { name; reason = Format.asprintf "%d is negative" value })

    let add_nonnegative_int64 name value members =
      match value with
      | None -> Ok ()
      | Some value when Int64.compare value 0L >= 0 ->
          add name (Jsont.Json.int64 value) members;
          Ok ()
      | Some value ->
          Error
            (Error.Invalid_option
               { name; reason = Format.asprintf "%Ld is negative" value })

    let sort = function
      | Cid -> "cid"
      | Start -> "start"
      | Subs -> "subs"
      | Pending -> "pending"
      | Out_msgs -> "msgs_to"
      | In_msgs -> "msgs_from"
      | Out_bytes -> "bytes_to"
      | In_bytes -> "bytes_from"
      | Last -> "last"
      | Idle -> "idle"
      | Uptime -> "uptime"
      | Stop -> "stop"
      | Reason -> "reason"
      | Rtt -> "rtt"

    let state = function Open -> 0 | Closed -> 1 | All -> 2

    let connz ?sort:sort_value ?auth ?subscriptions ?subscriptions_detail
        ?offset ?limit ?cid ?mqtt_client ?state:state_value ?user ?account
        ?filter_subject () =
      build (fun members ->
          let* () =
            add_option "sort"
              (fun value -> Jsont.Json.string (sort value))
              sort_value members
          in
          let* () = add_option "auth" Jsont.Json.bool auth members in
          let* () =
            add_option "subscriptions" Jsont.Json.bool subscriptions members
          in
          let* () =
            add_option "subscriptions_detail" Jsont.Json.bool
              subscriptions_detail members
          in
          let* () = add_nonnegative_int "offset" offset members in
          let* () = add_nonnegative_int "limit" limit members in
          let* () = add_nonnegative_int64 "cid" cid members in
          let* () =
            add_option "mqtt_client" Jsont.Json.string mqtt_client members
          in
          let* () =
            add_option "state"
              (fun value -> Jsont.Json.int (state value))
              state_value members
          in
          let* () = add_option "user" Jsont.Json.string user members in
          let* () = add_option "acc" Jsont.Json.string account members in
          add_option "filter_subject" Jsont.Json.string filter_subject members)

    let subsz ?offset ?limit ?subscriptions ?account ?test () =
      build (fun members ->
          let* () = add_nonnegative_int "offset" offset members in
          let* () = add_nonnegative_int "limit" limit members in
          let* () =
            add_option "subscriptions" Jsont.Json.bool subscriptions members
          in
          let* () = add_option "account" Jsont.Json.string account members in
          add_option "test" Jsont.Json.string test members)

    let routez ?subscriptions ?subscriptions_detail () =
      build (fun members ->
          let* () =
            add_option "subscriptions" Jsont.Json.bool subscriptions members
          in
          add_option "subscriptions_detail" Jsont.Json.bool subscriptions_detail
            members)

    let gatewayz ?name ?accounts ?account_name ?account_subscriptions
        ?account_subscriptions_detail () =
      build (fun members ->
          let* () = add_option "name" Jsont.Json.string name members in
          let* () = add_option "accounts" Jsont.Json.bool accounts members in
          let* () =
            add_option "account_name" Jsont.Json.string account_name members
          in
          let* () =
            add_option "subscriptions" Jsont.Json.bool account_subscriptions
              members
          in
          add_option "subscriptions_detail" Jsont.Json.bool
            account_subscriptions_detail members)

    let leafz ?subscriptions ?account () =
      build (fun members ->
          let* () =
            add_option "subscriptions" Jsont.Json.bool subscriptions members
          in
          add_option "account" Jsont.Json.string account members)

    let accountz ?account () =
      build (fun members ->
          add_option "account" Jsont.Json.string account members)

    let account_statz ?accounts ?include_unused () =
      build (fun members ->
          let* () =
            add_option "accounts"
              (fun values ->
                Jsont.Json.list (List.map Jsont.Json.string values))
              accounts members
          in
          add_option "include_unused" Jsont.Json.bool include_unused members)

    let jsz ?account ?accounts ?streams ?consumer ?direct_consumer ?config
        ?leader_only ?offset ?limit ?raft_groups ?stream_leader_only () =
      build (fun members ->
          let* () = add_option "account" Jsont.Json.string account members in
          let* () = add_option "accounts" Jsont.Json.bool accounts members in
          let* () = add_option "streams" Jsont.Json.bool streams members in
          let* () = add_option "consumer" Jsont.Json.bool consumer members in
          let* () =
            add_option "direct_consumer" Jsont.Json.bool direct_consumer members
          in
          let* () = add_option "config" Jsont.Json.bool config members in
          let* () =
            add_option "leader_only" Jsont.Json.bool leader_only members
          in
          let* () = add_nonnegative_int "offset" offset members in
          let* () = add_nonnegative_int "limit" limit members in
          let* () = add_option "raft" Jsont.Json.bool raft_groups members in
          add_option "stream_leader_only" Jsont.Json.bool stream_leader_only
            members)

    let healthz ?js_enabled ?js_enabled_only ?js_server_only ?js_meta_only
        ?account ?stream ?consumer ?details () =
      build (fun members ->
          let* () =
            add_option "js-enabled" Jsont.Json.bool js_enabled members
          in
          let* () =
            add_option "js-enabled-only" Jsont.Json.bool js_enabled_only members
          in
          let* () =
            add_option "js-server-only" Jsont.Json.bool js_server_only members
          in
          let* () =
            add_option "js-meta-only" Jsont.Json.bool js_meta_only members
          in
          let* () = add_option "account" Jsont.Json.string account members in
          let* () = add_option "stream" Jsont.Json.string stream members in
          let* () = add_option "consumer" Jsont.Json.string consumer members in
          add_option "details" Jsont.Json.bool details members)

    let profilez ?name ?debug ?duration_ns () =
      build (fun members ->
          let* () = add_option "name" Jsont.Json.string name members in
          let* () = add_nonnegative_int "debug" debug members in
          add_nonnegative_int64 "duration" duration_ns members)

    let ipqueuesz ?all ?filter () =
      build (fun members ->
          let* () = add_option "all" Jsont.Json.bool all members in
          add_option "filter" Jsont.Json.string filter members)

    let raftz ?account ?group () =
      build (fun members ->
          let* () = add_option "account" Jsont.Json.string account members in
          add_option "group" Jsont.Json.string group members)
  end

  module Endpoint = struct
    type t =
      | Idz
      | Statz
      | Varz
      | Subsz
      | Connz
      | Routez
      | Gatewayz
      | Leafz
      | Accountz
      | Jsz
      | Healthz
      | Profilez
      | Expvarz
      | Ipqueuesz
      | Raftz
      | Account_info
      | Account_stats
      | Account_connections

    let to_string = function
      | Idz -> "IDZ"
      | Statz -> "STATSZ"
      | Varz -> "VARZ"
      | Subsz -> "SUBSZ"
      | Connz -> "CONNZ"
      | Routez -> "ROUTEZ"
      | Gatewayz -> "GATEWAYZ"
      | Leafz -> "LEAFZ"
      | Accountz -> "ACCOUNTZ"
      | Jsz -> "JSZ"
      | Healthz -> "HEALTHZ"
      | Profilez -> "PROFILEZ"
      | Expvarz -> "EXPVARZ"
      | Ipqueuesz -> "IPQUEUESZ"
      | Raftz -> "RAFTZ"
      | Account_info -> "INFO"
      | Account_stats -> "STATZ"
      | Account_connections -> "CONNS"

    let pp ppf value = Format.pp_print_string ppf (to_string value)
  end

  type response = {
    endpoint : Endpoint.t;
    payload : Jsont.json;
    server : Jsont.json option;
    data : Jsont.json option;
    error : Error.api option;
  }

  let endpoint value = value.endpoint
  let payload value = value.payload
  let server value = value.server
  let data value = value.data
  let error value = value.error

  let response_of_json endpoint payload =
    let* error =
      match json_field "error" payload with
      | None -> Ok None
      | Some value -> decode_api_error value |> Result.map Option.some
    in
    Ok
      {
        endpoint;
        payload;
        server = json_field "server" payload;
        data = json_field "data" payload;
        error;
      }

  let endpoint_allowed target endpoint =
    match target with
    | Target.All | Target.Server _ -> (
        match endpoint with
        | Endpoint.Account_info | Endpoint.Account_stats
        | Endpoint.Account_connections ->
            false
        | _ -> true)
    | Target.Account _ -> (
        match endpoint with
        | Endpoint.Subsz | Endpoint.Connz | Endpoint.Leafz | Endpoint.Jsz
        | Endpoint.Account_info | Endpoint.Account_stats
        | Endpoint.Account_connections ->
            true
        | _ -> false)

  let target_name = function
    | Target.All -> "all servers"
    | Target.Server value -> "server " ^ value
    | Target.Account value -> "account " ^ value

  let subject target endpoint =
    let endpoint = Endpoint.to_string endpoint in
    match target with
    | Target.All -> Nats.Subject.literal ("$SYS.REQ.SERVER.PING." ^ endpoint)
    | Target.Server server ->
        Nats.Subject.literal ("$SYS.REQ.SERVER." ^ server ^ "." ^ endpoint)
    | Target.Account account ->
        Nats.Subject.literal ("$SYS.REQ.ACCOUNT." ^ account ^ "." ^ endpoint)

  let default_timeout = Mtime.Span.(5 * s)

  let validate_timeout timeout =
    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
      Error Error.Invalid_timeout
    else Ok ()

  let request_json selector (options : Options.t) =
    match selector_json selector with
    | Jsont.Object (members, _) -> Jsont.Json.object' (members @ options)
    | _ -> Jsont.Json.object' options

  let one system ~timeout ~target ~endpoint ~selector ~options =
    let subject = subject target endpoint in
    let* payload = encode_json (request_json selector options) in
    let message = Nats.Message.v ~subject payload in
    match
      Nats_eio.Connection.request_msg ~timeout system.connection message
    with
    | Error error -> Error (Error.Connection error)
    | Ok message ->
        let* payload = decode_json (Nats.Message.payload message) in
        response_of_json endpoint payload

  let collect system ~deadline ~target ~endpoint ~selector ~options =
    let inbox = Nats_eio.Connection.fresh_inbox system.connection in
    let filter = Nats.Subject.Filter.literal (Nats.Subject.to_string inbox) in
    match
      Nats_eio.Connection.subscribe system.connection ~replay_on_reconnect:false
        filter
    with
    | Error error -> Error (Error.Connection error)
    | Ok subscription -> (
        let finish result =
          Eio.Cancel.protect (fun () ->
              match
                Nats_eio.Connection.Subscription.unsubscribe subscription
              with
              | Ok () -> result
              | Error error -> (
                  match result with
                  | Ok _ -> Error (Error.Connection error)
                  | Error _ -> result))
        in
        let operation () =
          if
            Mtime.compare (Nats_eio.Connection.now system.connection) deadline
            >= 0
          then Ok []
          else
            let* payload = encode_json (request_json selector options) in
            let request_subject = subject target endpoint in
            let request =
              Nats.Message.v ~subject:request_subject ~reply_to:inbox payload
            in
            match Nats_eio.Connection.publish_msg system.connection request with
            | Error error -> Error (Error.Connection error)
            | Ok () ->
                let rec loop responses =
                  let now = Nats_eio.Connection.now system.connection in
                  if Mtime.compare now deadline >= 0 then
                    Ok (List.rev responses)
                  else
                    let remaining = Mtime.span now deadline in
                    match
                      Nats_eio.Connection.Subscription.next_with_timeout
                        ~timeout:remaining subscription
                    with
                    | Error Nats_eio.Error.Timeout -> Ok (List.rev responses)
                    | Error error -> Error (Error.Connection error)
                    | Ok { message; status = None } ->
                        let* payload =
                          decode_json (Nats.Message.payload message)
                        in
                        let* response = response_of_json endpoint payload in
                        loop (response :: responses)
                    | Ok { status = Some { code = 503; _ }; _ } ->
                        Ok (List.rev responses)
                    | Ok { status = Some { code; description }; _ } ->
                        Error (Error.Unexpected_status { code; description })
                in
                loop []
        in
        let cleanup () =
          Eio.Cancel.protect (fun () ->
              ignore (Nats_eio.Connection.Subscription.unsubscribe subscription))
        in
        try finish (operation ()) with
        | Eio.Cancel.Cancelled _ as cancellation ->
            cleanup ();
            raise cancellation
        | exception_value ->
            cleanup ();
            raise exception_value)

  let request ?(timeout = default_timeout) ?(selector = Selector.empty)
      ?(options = Options.empty) system ~target endpoint =
    let* () = validate_timeout timeout in
    if not (endpoint_allowed target endpoint) then
      Error
        (Error.Invalid_target_endpoint
           {
             target = target_name target;
             endpoint = Endpoint.to_string endpoint;
           })
    else
      match target with
      | Target.Server _ ->
          one system ~timeout ~target ~endpoint ~selector ~options
          |> Result.map (fun response -> [ response ])
      | Target.All | Target.Account _ ->
          let deadline =
            match
              Mtime.add_span (Nats_eio.Connection.now system.connection) timeout
            with
            | Some deadline -> deadline
            | None -> Mtime.max_stamp
          in
          collect system ~deadline ~target ~endpoint ~selector ~options
end

let control_payload selector extra =
  let selector = selector_json selector in
  match selector with
  | Jsont.Object (members, _) -> Jsont.Json.object' (members @ extra)
  | _ -> Jsont.Json.object' extra

let control_request ?(timeout = Monitor.default_timeout)
    ?(selector = Selector.empty) system ~server ~operation ~payload =
  let* () =
    match validate_identifier ~kind:"server" server with
    | Ok () -> Ok ()
    | Error error -> Error error
  in
  let subject =
    Nats.Subject.literal ("$SYS.REQ.SERVER." ^ server ^ "." ^ operation)
  in
  if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
    Error Error.Invalid_timeout
  else
    let* payload = encode_json (payload selector) in
    let request = Nats.Message.v ~subject payload in
    match
      Nats_eio.Connection.request_msg ~timeout system.connection request
    with
    | Error error -> Error (Error.Connection error)
    | Ok message -> (
        let* payload = decode_json (Nats.Message.payload message) in
        match json_field "error" payload with
        | None -> Ok ()
        | Some error ->
            let* error = decode_api_error error in
            Error (Error.Server error))

module Control = struct
  let reload ?timeout ?selector system ~server =
    control_request ?timeout ?selector system ~server ~operation:"RELOAD"
      ~payload:(fun selector -> selector_json selector)

  let client_request client_id selector =
    control_payload selector [ json_member "cid" (Jsont.Json.int64 client_id) ]

  let validate_client_id client_id =
    if Int64.compare client_id 0L < 0 then
      Error (Error.Invalid_client_id client_id)
    else Ok ()

  let kick ?timeout ?selector system ~server ~client_id =
    let* () = validate_client_id client_id in
    control_request ?timeout ?selector system ~server ~operation:"KICK"
      ~payload:(client_request client_id)

  let client_lame_duck ?timeout ?selector system ~server ~client_id =
    let* () = validate_client_id client_id in
    control_request ?timeout ?selector system ~server ~operation:"LDM"
      ~payload:(client_request client_id)
end

module Events = struct
  type scope = All | Servers | Accounts | Server of string | Account of string

  type event =
    | Server_stats of { server_id : string; payload : Jsont.json }
    | Server_shutdown of { server_id : string; payload : Jsont.json }
    | Server_lame_duck of { server_id : string; payload : Jsont.json }
    | Server_auth_error of { server_id : string; payload : Jsont.json }
    | Account_connect of { account_id : string; payload : Jsont.json }
    | Account_disconnect of { account_id : string; payload : Jsont.json }
    | Account_leafnode_connect of { account_id : string; payload : Jsont.json }
    | Account_server_connections of {
        account_id : string;
        payload : Jsont.json;
      }
    | Unknown of { subject : string; payload : Jsont.json }

  type t = {
    connection : Nats_eio.Connection.t;
    subscriptions : Nats_eio.Connection.Subscription.t list;
    pending : Nats_eio.Connection.Subscription.delivery list ref;
  }

  let filters = function
    | All ->
        Ok
          [
            Nats.Subject.Filter.literal "$SYS.SERVER.>";
            Nats.Subject.Filter.literal "$SYS.ACCOUNT.>";
          ]
    | Servers -> Ok [ Nats.Subject.Filter.literal "$SYS.SERVER.>" ]
    | Accounts -> Ok [ Nats.Subject.Filter.literal "$SYS.ACCOUNT.>" ]
    | Server value ->
        let* () = validate_identifier ~kind:"server" value in
        Ok [ Nats.Subject.Filter.literal ("$SYS.SERVER." ^ value ^ ".>") ]
    | Account value ->
        let* () = validate_identifier ~kind:"account" value in
        Ok [ Nats.Subject.Filter.literal ("$SYS.ACCOUNT." ^ value ^ ".>") ]

  let unsubscribe subscriptions =
    let first_error = ref None in
    List.iter
      (fun subscription ->
        match !first_error with
        | Some _ ->
            ignore (Nats_eio.Connection.Subscription.unsubscribe subscription)
        | None -> (
            match Nats_eio.Connection.Subscription.unsubscribe subscription with
            | Ok () -> ()
            | Error error -> first_error := Some error))
      subscriptions;
    match !first_error with
    | None -> Ok ()
    | Some error -> Error (Error.Connection error)

  let subscribe ?(scope = All) (system : system) =
    let* filters = filters scope in
    let cleanup subscriptions =
      Eio.Cancel.protect (fun () -> ignore (unsubscribe subscriptions))
    in
    let rec loop remaining subscriptions =
      match remaining with
      | [] ->
          Ok { connection = system.connection; subscriptions; pending = ref [] }
      | filter :: rest -> (
          try
            match Nats_eio.Connection.subscribe system.connection filter with
            | Ok subscription -> loop rest (subscription :: subscriptions)
            | Error error ->
                cleanup subscriptions;
                Error (Error.Connection error)
          with
          | Eio.Cancel.Cancelled _ as cancellation ->
              cleanup subscriptions;
              raise cancellation
          | exception_value ->
              cleanup subscriptions;
              raise exception_value)
    in
    loop filters []

  let make_event subject payload =
    match String.split_on_char '.' subject with
    | [ "$SYS"; "SERVER"; server_id; "STATSZ" ] ->
        Server_stats { server_id; payload }
    | [ "$SYS"; "SERVER"; server_id; "SHUTDOWN" ] ->
        Server_shutdown { server_id; payload }
    | [ "$SYS"; "SERVER"; server_id; "LAMEDUCK" ] ->
        Server_lame_duck { server_id; payload }
    | [ "$SYS"; "SERVER"; server_id; "CLIENT"; "AUTH"; "ERR" ] ->
        Server_auth_error { server_id; payload }
    | [ "$SYS"; "ACCOUNT"; account_id; "CONNECT" ] ->
        Account_connect { account_id; payload }
    | [ "$SYS"; "ACCOUNT"; account_id; "DISCONNECT" ] ->
        Account_disconnect { account_id; payload }
    | [ "$SYS"; "ACCOUNT"; account_id; "LEAFNODE"; "CONNECT" ] ->
        Account_leafnode_connect { account_id; payload }
    | [ "$SYS"; "ACCOUNT"; account_id; "SERVER"; "CONNS" ] ->
        Account_server_connections { account_id; payload }
    | _ -> Unknown { subject; payload }

  let next_delivery
      (delivery :
        (Nats_eio.Connection.Subscription.delivery, Nats_eio.Error.t) result) =
    match delivery with
    | Error error -> Error (Error.Connection error)
    | Ok { message; status = None } ->
        let* payload = decode_json (Nats.Message.payload message) in
        Ok
          (make_event
             (Nats.Subject.to_string (Nats.Message.subject message))
             payload)
    | Ok { status = Some { code; description }; _ } ->
        Error (Error.Unexpected_status { code; description })

  type raw_delivery =
    (Nats_eio.Connection.Subscription.delivery, Nats_eio.Error.t) result

  let combine pending first second =
    match (first, second) with
    | Ok first, Ok second ->
        pending := second :: !pending;
        Ok first
    | Ok _, _ -> first
    | _, Ok _ -> second
    | Error Nats_eio.Error.Timeout, result
    | result, Error Nats_eio.Error.Timeout ->
        result
    | first, _ -> first

  let next_any pending subscriptions =
    match !pending with
    | delivery :: rest ->
        pending := rest;
        Ok delivery
    | [] -> (
        match subscriptions with
        | [] -> Error Nats_eio.Error.Closed
        | [ subscription ] -> Nats_eio.Connection.Subscription.next subscription
        | left :: right :: _ ->
            Eio.Fiber.first ~combine:(combine pending)
              (fun () -> Nats_eio.Connection.Subscription.next left)
              (fun () -> Nats_eio.Connection.Subscription.next right))

  let next_any_with_timeout ~timeout pending subscriptions : raw_delivery =
    match !pending with
    | delivery :: rest ->
        pending := rest;
        Ok delivery
    | [] -> (
        match subscriptions with
        | [] -> Error Nats_eio.Error.Closed
        | [ subscription ] ->
            Nats_eio.Connection.Subscription.next_with_timeout ~timeout
              subscription
        | left :: right :: _ ->
            Eio.Fiber.first ~combine:(combine pending)
              (fun () ->
                Nats_eio.Connection.Subscription.next_with_timeout ~timeout left)
              (fun () ->
                Nats_eio.Connection.Subscription.next_with_timeout ~timeout
                  right))

  let next value = next_delivery (next_any value.pending value.subscriptions)

  let next_with_timeout ~timeout value =
    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
      Error Error.Invalid_timeout
    else
      let deadline =
        match
          Mtime.add_span (Nats_eio.Connection.now value.connection) timeout
        with
        | Some deadline -> deadline
        | None -> Mtime.max_stamp
      in
      let rec loop () =
        let now = Nats_eio.Connection.now value.connection in
        if Mtime.compare now deadline >= 0 then
          Error (Error.Connection Nats_eio.Error.Timeout)
        else
          let remaining = Mtime.span now deadline in
          match
            next_any_with_timeout ~timeout:remaining value.pending
              value.subscriptions
          with
          | Error Nats_eio.Error.Timeout -> loop ()
          | result -> next_delivery result
      in
      loop ()

  let close value =
    Eio.Cancel.protect (fun () -> unsubscribe value.subscriptions)
end
