module Core_error = Error

module Error = struct
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Empty_subjects
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_max_age
    | Empty_consumer_name
    | Invalid_consumer_name_character of { position : int; character : char }
    | Invalid_consumer_limit of { field : string; value : int64 }
    | Invalid_consumer_span of { field : string }
    | Invalid_consumer_policy of { field : string; value : string }

  type api = { code : int; err_code : int option; description : string }
  type list_kind = Streams | Consumers

  type t =
    | Connection of Connection.error
    | Decode of Jsont.Error.t
    | Encode of Jsont.Error.t
    | Api of api
    | Missing_field of string
    | Invalid_prefix of Nats.Subject.error
    | Invalid_subject of Nats.Subject.error
    | Invalid_config of config
    | Invalid_headers of Nats.Header.error
    | Empty_msg_id
    | Msg_id_already_set
    | Unexpected_stream_name of { expected : string; actual : string }
    | Unexpected_consumer_name of { expected : string; actual : string }
    | Invalid_batch of int
    | Invalid_max_bytes of int
    | Invalid_fetch_span
    | Invalid_idle_heartbeat
    | Idle_heartbeat_expires_too_short
    | Missing_heartbeat
    | Missing_ack_reply
    | Invalid_ack_reply of string
    | Consumer_deleted
    | Conflict of { code : int; description : string }
    | Unexpected_status of { code : int; description : string }
    | Incomplete_list of { kind : list_kind; missing : string list }
    | Pull_closed

  let pp_config ppf = function
    | Empty_name -> Format.pp_print_string ppf "stream name is empty"
    | Invalid_name_character { position; character } ->
        Format.fprintf ppf "invalid stream-name character %C at position %d"
          character position
    | Empty_subjects ->
        Format.pp_print_string ppf "a stream must capture at least one subject"
    | Invalid_limit { field; value } ->
        Format.fprintf ppf "invalid %s limit %Ld" field value
    | Invalid_max_age ->
        Format.pp_print_string ppf "stream max age must not be negative"
    | Empty_consumer_name -> Format.pp_print_string ppf "consumer name is empty"
    | Invalid_consumer_name_character { position; character } ->
        Format.fprintf ppf "invalid consumer-name character %C at position %d"
          character position
    | Invalid_consumer_limit { field; value } ->
        Format.fprintf ppf "invalid consumer %s limit %Ld" field value
    | Invalid_consumer_span { field } ->
        Format.fprintf ppf "consumer %s must be positive" field
    | Invalid_consumer_policy { field; value } ->
        Format.fprintf ppf "invalid consumer %s policy %S" field value

  let pp_api ppf { code; err_code; description } =
    match err_code with
    | None -> Format.fprintf ppf "JetStream API error %d: %s" code description
    | Some err_code ->
        Format.fprintf ppf "JetStream API error %d/%d: %s" code err_code
          description

  let pp ppf = function
    | Connection error ->
        Format.fprintf ppf "connection: %a" Core_error.pp error
    | Decode error -> Format.fprintf ppf "JSON decode: %a" Jsont.Error.pp error
    | Encode error -> Format.fprintf ppf "JSON encode: %a" Jsont.Error.pp error
    | Api error -> pp_api ppf error
    | Missing_field field ->
        Format.fprintf ppf "missing JetStream field %S" field
    | Invalid_prefix error ->
        Format.fprintf ppf "invalid JetStream API prefix: %a"
          Nats.Subject.pp_error error
    | Invalid_subject error ->
        Format.fprintf ppf "invalid JetStream subject: %a" Nats.Subject.pp_error
          error
    | Invalid_config error ->
        Format.fprintf ppf "invalid JetStream config: %a" pp_config error
    | Invalid_headers error ->
        Format.fprintf ppf "invalid JetStream headers: %a" Nats.Header.pp_error
          error
    | Empty_msg_id -> Format.pp_print_string ppf "JetStream message id is empty"
    | Msg_id_already_set ->
        Format.pp_print_string ppf
          "Nats-Msg-Id is already present in the publish headers"
    | Unexpected_stream_name { expected; actual } ->
        Format.fprintf ppf "JetStream response named stream %S, expected %S"
          actual expected
    | Unexpected_consumer_name { expected; actual } ->
        Format.fprintf ppf "JetStream response named consumer %S, expected %S"
          actual expected
    | Invalid_batch value ->
        Format.fprintf ppf
          "JetStream fetch batch must be between 1 and 256, got %d" value
    | Invalid_max_bytes value ->
        Format.fprintf ppf
          "JetStream fetch max_bytes must not be negative, got %d" value
    | Invalid_fetch_span ->
        Format.pp_print_string ppf "JetStream fetch expiry must be positive"
    | Invalid_idle_heartbeat ->
        Format.pp_print_string ppf "JetStream idle heartbeat must be positive"
    | Idle_heartbeat_expires_too_short ->
        Format.pp_print_string ppf
          "JetStream pull expiry must be at least twice the idle heartbeat"
    | Missing_heartbeat ->
        Format.pp_print_string ppf
          "JetStream pull idle heartbeat was not received"
    | Missing_ack_reply ->
        Format.pp_print_string ppf
          "JetStream delivery has no acknowledgement reply"
    | Invalid_ack_reply subject ->
        Format.fprintf ppf "invalid JetStream acknowledgement subject %S"
          subject
    | Consumer_deleted ->
        Format.pp_print_string ppf "JetStream consumer was deleted"
    | Conflict { code; description } ->
        Format.fprintf ppf "JetStream pull conflict %d: %s" code description
    | Unexpected_status { code; description } ->
        Format.fprintf ppf "unexpected JetStream pull status %d: %s" code
          description
    | Incomplete_list { kind; missing } -> (
        let kind =
          match kind with Streams -> "stream" | Consumers -> "consumer"
        in
        match missing with
        | [] -> Format.fprintf ppf "incomplete JetStream %s list" kind
        | _ :: _ ->
            Format.fprintf ppf "incomplete JetStream %s list; missing: %s" kind
              (String.concat ", " missing))
    | Pull_closed ->
        Format.pp_print_string ppf "JetStream pull consumer is closed"
end

type config_error = Error.config
type api_error = Error.api
type error = Error.t
type t = { connection : Connection.t; prefix : string }

let v ?(prefix = "$JS.API") connection =
  match Nats.Subject.of_string prefix with
  | Ok validated_prefix ->
      Ok { connection; prefix = Nats.Subject.to_string validated_prefix }
  | Error error -> Error (Error.Invalid_prefix error)

let of_connection ?prefix connection = v ?prefix connection
let connection value = value.connection
let prefix value = value.prefix

let api_subject value suffix =
  Nats.Subject.literal (String.concat "." (value.prefix :: suffix))

let request_msg ?timeout value message =
  match Connection.request_msg ?timeout value.connection message with
  | Ok response -> Ok response
  | Error error -> Error (Error.Connection error)

let decode codec message =
  match Jsont_bytesrw.decode_string' codec (Nats.Message.payload message) with
  | Ok value -> Ok value
  | Error error -> Error (Error.Decode error)

let encode codec value =
  match Jsont_bytesrw.encode_string' codec value with
  | Ok payload -> Ok payload
  | Error error -> Error (Error.Encode error)

let api_error_codec =
  Jsont.Object.map ~kind:"JetStream API error" (fun code err_code description ->
      { Error.code; err_code; description })
  |> Jsont.Object.mem "code" Jsont.int ~enc:(fun value -> value.Error.code)
  |> Jsont.Object.opt_mem "err_code" Jsont.int ~enc:(fun value ->
      value.Error.err_code)
  |> Jsont.Object.mem "description" Jsont.string ~enc:(fun value ->
      value.Error.description)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

module Stream = struct
  module Config = struct
    type storage = Memory | File
    type retention = Limits | Interest | Work_queue
    type discard = Old | New

    type t = {
      name : string;
      subjects : Nats.Subject.Filter.t list;
      storage : storage;
      retention : retention;
      discard : discard;
      max_msgs : int64 option;
      max_bytes : int64 option;
      max_age : Mtime.Span.t option;
      max_msg_size : int64 option;
    }

    type error = config_error

    let allowed_name_character character =
      let code = Char.code character in
      (code >= Char.code 'A' && code <= Char.code 'Z')
      || (code >= Char.code 'a' && code <= Char.code 'z')
      || (code >= Char.code '0' && code <= Char.code '9')
      || Char.equal character '_' || Char.equal character '-'

    let validate_name name =
      let length = String.length name in
      if Int.equal length 0 then Error Error.Empty_name
      else
        let invalid = ref None in
        for position = 0 to length - 1 do
          match !invalid with
          | Some _ -> ()
          | None ->
              let character = String.get name position in
              if not (allowed_name_character character) then
                invalid :=
                  Some (Error.Invalid_name_character { position; character })
        done;
        match !invalid with None -> Ok () | Some error -> Error error

    let validate_limit field = function
      | None -> Ok ()
      | Some value when Int64.compare value (-1L) >= 0 -> Ok ()
      | Some value -> Error (Error.Invalid_limit { field; value })

    let v_internal ~allow_empty_subjects ~name ~subjects ?(storage = File)
        ?(retention = Limits) ?(discard = Old) ?max_msgs ?max_bytes ?max_age
        ?max_msg_size () =
      let max_age =
        match max_age with
        | Some value when Int.equal (Mtime.Span.compare value Mtime.Span.zero) 0
          ->
            None
        | value -> value
      in
      match validate_name name with
      | Error error -> Error error
      | Ok ()
        when Int.equal (List.length subjects) 0 && not allow_empty_subjects ->
          Error Error.Empty_subjects
      | Ok () -> (
          match validate_limit "max_msgs" max_msgs with
          | Error error -> Error error
          | Ok () -> (
              match validate_limit "max_bytes" max_bytes with
              | Error error -> Error error
              | Ok () -> (
                  match validate_limit "max_msg_size" max_msg_size with
                  | Error error -> Error error
                  | Ok () -> (
                      match max_age with
                      | Some value
                        when Mtime.Span.compare value Mtime.Span.zero < 0 ->
                          Error Error.Invalid_max_age
                      | _ ->
                          Ok
                            {
                              name;
                              subjects;
                              storage;
                              retention;
                              discard;
                              max_msgs;
                              max_bytes;
                              max_age;
                              max_msg_size;
                            }))))

    let v ~name ~subjects ?storage ?retention ?discard ?max_msgs ?max_bytes
        ?max_age ?max_msg_size () =
      v_internal ~allow_empty_subjects:false ~name ~subjects ?storage ?retention
        ?discard ?max_msgs ?max_bytes ?max_age ?max_msg_size ()

    let name value = value.name
    let subjects value = value.subjects
    let storage value = value.storage
    let retention value = value.retention
    let discard value = value.discard
    let max_msgs value = value.max_msgs
    let max_bytes value = value.max_bytes
    let max_age value = value.max_age
    let max_msg_size value = value.max_msg_size

    let rebuild value ~name ~subjects ~storage ~retention ~discard ~max_msgs
        ~max_bytes ~max_age ~max_msg_size =
      v_internal
        ~allow_empty_subjects:(Int.equal (List.length value.subjects) 0)
        ~name ~subjects ~storage ~retention ~discard ?max_msgs ?max_bytes
        ?max_age ?max_msg_size ()

    let with_name value name =
      rebuild value ~name ~subjects:value.subjects ~storage:value.storage
        ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes
        ~max_age:value.max_age ~max_msg_size:value.max_msg_size

    let with_subjects value subjects =
      rebuild value ~name:value.name ~subjects ~storage:value.storage
        ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes
        ~max_age:value.max_age ~max_msg_size:value.max_msg_size

    let with_storage value storage =
      rebuild value ~name:value.name ~subjects:value.subjects ~storage
        ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes
        ~max_age:value.max_age ~max_msg_size:value.max_msg_size

    let with_retention value retention =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes
        ~max_age:value.max_age ~max_msg_size:value.max_msg_size

    let with_discard value discard =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes
        ~max_age:value.max_age ~max_msg_size:value.max_msg_size

    let with_max_msgs value max_msgs =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size

    let with_max_bytes value max_bytes =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size

    let with_max_age value max_age =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes ~max_age
        ~max_msg_size:value.max_msg_size

    let with_max_msg_size value max_msg_size =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_bytes:value.max_bytes
        ~max_age:value.max_age ~max_msg_size
  end

  module Info = struct
    type t = {
      config : Config.t;
      messages : int64;
      bytes : int64;
      first_sequence : int64;
      last_sequence : int64;
      consumer_count : int;
    }

    let config value = value.config
    let messages value = value.messages
    let bytes value = value.bytes
    let first_sequence value = value.first_sequence
    let last_sequence value = value.last_sequence
    let consumer_count value = value.consumer_count

    let pp ppf value =
      Format.fprintf ppf "JetStream stream %S (messages=%Ld, bytes=%Ld)"
        (Config.name value.config) value.messages value.bytes
  end

  type jetstream = t
  type t = { jetstream : jetstream; name : string }

  type wire_config = {
    name : string;
    subjects : string list;
    storage : Config.storage;
    retention : Config.retention;
    discard : Config.discard;
    max_msgs : int64 option;
    max_bytes : int64 option;
    max_age : int64 option;
    max_msg_size : int64 option;
    unknown : Jsont.json;
  }

  let storage_codec =
    Jsont.enum [ ("memory", Config.Memory); ("file", Config.File) ]

  let retention_codec =
    Jsont.enum
      [
        ("limits", Config.Limits);
        ("interest", Config.Interest);
        ("workqueue", Config.Work_queue);
      ]

  let discard_codec = Jsont.enum [ ("old", Config.Old); ("new", Config.New) ]

  let wire_config_codec =
    Jsont.Object.map ~kind:"JetStream stream config"
      (fun
        name
        subjects
        storage
        retention
        discard
        max_msgs
        max_bytes
        max_age
        max_msg_size
        unknown
      ->
        {
          name;
          subjects = Option.value ~default:[] subjects;
          storage;
          retention;
          discard;
          max_msgs;
          max_bytes;
          max_age;
          max_msg_size;
          unknown;
        })
    |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
    |> Jsont.Object.opt_mem "subjects" (Jsont.list Jsont.string)
         ~enc:(fun value ->
           match value.subjects with [] -> None | subjects -> Some subjects)
    |> Jsont.Object.mem "storage" storage_codec ~enc:(fun value ->
        value.storage)
    |> Jsont.Object.mem "retention" retention_codec ~enc:(fun value ->
        value.retention)
    |> Jsont.Object.mem "discard" discard_codec ~enc:(fun value ->
        value.discard)
    |> Jsont.Object.opt_mem "max_msgs" Jsont.int64 ~enc:(fun value ->
        value.max_msgs)
    |> Jsont.Object.opt_mem "max_bytes" Jsont.int64 ~enc:(fun value ->
        value.max_bytes)
    |> Jsont.Object.opt_mem "max_age" Jsont.int64 ~enc:(fun value ->
        value.max_age)
    |> Jsont.Object.opt_mem "max_msg_size" Jsont.int64 ~enc:(fun value ->
        value.max_msg_size)
    |> Jsont.Object.keep_unknown
         ~enc:(fun value -> value.unknown)
         Jsont.json_mems
    |> Jsont.Object.finish

  type wire_state = {
    messages : int64;
    bytes : int64;
    first_seq : int64;
    last_seq : int64;
    consumer_count : int;
  }

  let wire_state_codec =
    Jsont.Object.map ~kind:"JetStream stream state"
      (fun messages bytes first_seq last_seq consumer_count ->
        { messages; bytes; first_seq; last_seq; consumer_count })
    |> Jsont.Object.mem "messages" Jsont.int64 ~enc:(fun value ->
        value.messages)
    |> Jsont.Object.mem "bytes" Jsont.int64 ~enc:(fun value -> value.bytes)
    |> Jsont.Object.mem "first_seq" Jsont.int64 ~enc:(fun value ->
        value.first_seq)
    |> Jsont.Object.mem "last_seq" Jsont.int64 ~enc:(fun value ->
        value.last_seq)
    |> Jsont.Object.mem "consumer_count" Jsont.int ~enc:(fun value ->
        value.consumer_count)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  type response = {
    error : api_error option;
    config : wire_config option;
    state : wire_state option;
  }

  let response_codec =
    Jsont.Object.map ~kind:"JetStream stream response"
      (fun error config state -> { error; config; state })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.opt_mem "config" wire_config_codec ~enc:(fun value ->
        value.config)
    |> Jsont.Object.opt_mem "state" wire_state_codec ~enc:(fun value ->
        value.state)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  type list_request = { offset : int; subject : string option }

  let list_request_codec =
    Jsont.Object.map ~kind:"JetStream stream list request"
      (fun offset subject -> { offset; subject })
    |> Jsont.Object.mem "offset" Jsont.int ~enc:(fun value -> value.offset)
    |> Jsont.Object.opt_mem "subject" Jsont.string ~enc:(fun value ->
        value.subject)
    |> Jsont.Object.finish

  type list_response = {
    error : api_error option;
    total : int;
    offset : int;
    limit : int;
    streams : response list;
    missing : string list;
  }

  let list_response_codec =
    Jsont.Object.map ~kind:"JetStream stream list response"
      (fun error total offset limit streams missing ->
        {
          error;
          total;
          offset;
          limit;
          streams;
          missing = Option.value ~default:[] missing;
        })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.mem "total" Jsont.int ~enc:(fun value -> value.total)
    |> Jsont.Object.mem "offset" Jsont.int ~enc:(fun value -> value.offset)
    |> Jsont.Object.mem "limit" Jsont.int ~enc:(fun value -> value.limit)
    |> Jsont.Object.mem "streams" (Jsont.list response_codec) ~enc:(fun value ->
        value.streams)
    |> Jsont.Object.opt_mem "missing" (Jsont.list Jsont.string)
         ~enc:(fun value ->
           match value.missing with [] -> None | missing -> Some missing)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  let wire_config value =
    {
      name = Config.name value;
      subjects = List.map Nats.Subject.Filter.to_string (Config.subjects value);
      storage = Config.storage value;
      retention = Config.retention value;
      discard = Config.discard value;
      max_msgs = Config.max_msgs value;
      max_bytes = Config.max_bytes value;
      max_age = Option.map Mtime.Span.to_uint64_ns (Config.max_age value);
      max_msg_size = Config.max_msg_size value;
      unknown = Jsont.Json.object' [];
    }

  let wire_config_for_update ~current value =
    let subjects =
      List.map Nats.Subject.Filter.to_string (Config.subjects value)
    in
    {
      current with
      name = Config.name value;
      subjects =
        (match subjects with [] -> current.subjects | _ :: _ -> subjects);
      storage = Config.storage value;
      retention = Config.retention value;
      discard = Config.discard value;
      max_msgs = Some (Option.value ~default:(-1L) (Config.max_msgs value));
      max_bytes = Some (Option.value ~default:(-1L) (Config.max_bytes value));
      max_age =
        Some
          (Option.value ~default:0L
             (Option.map Mtime.Span.to_uint64_ns (Config.max_age value)));
      max_msg_size =
        Some (Option.value ~default:(-1L) (Config.max_msg_size value));
    }

  let config_of_wire value =
    let subjects =
      List.fold_left
        (fun result subject ->
          match result with
          | Error _ -> result
          | Ok subjects -> (
              match Nats.Subject.Filter.of_string subject with
              | Ok subject -> Ok (subject :: subjects)
              | Error error -> Error (Error.Invalid_subject error)))
        (Ok []) value.subjects
    in
    match subjects with
    | Error error -> Error error
    | Ok subjects -> (
        let subjects = List.rev subjects in
        let max_msgs =
          match value.max_msgs with Some -1L -> None | value -> value
        in
        let max_bytes =
          match value.max_bytes with Some -1L -> None | value -> value
        in
        let max_msg_size =
          match value.max_msg_size with Some -1L -> None | value -> value
        in
        let max_age =
          match value.max_age with
          | None | Some 0L -> None
          | Some nanoseconds -> Some (Mtime.Span.of_uint64_ns nanoseconds)
        in
        match
          Config.v_internal ~allow_empty_subjects:true ~name:value.name
            ~subjects ~storage:value.storage ~retention:value.retention
            ~discard:value.discard ?max_msgs ?max_bytes ?max_age ?max_msg_size
            ()
        with
        | Ok config -> Ok config
        | Error error -> Error (Error.Invalid_config error))

  let decode_response message =
    match decode response_codec message with
    | Error error -> Error error
    | Ok { error = Some error; _ } -> Error (Error.Api error)
    | Ok response -> Ok response

  let decode_list_response message =
    match decode list_response_codec message with
    | Error error -> Error error
    | Ok { error = Some error; _ } -> Error (Error.Api error)
    | Ok response -> Ok response

  let encode_config value = encode wire_config_codec (wire_config value)
  let encode_wire_config value = encode wire_config_codec value
  let name (value : t) = value.name

  let bind jetstream ~name =
    match Config.validate_name name with
    | Ok () -> Ok { jetstream; name }
    | Error error -> Error (Error.Invalid_config error)

  let create jetstream config =
    let name = Config.name config in
    let subject = api_subject jetstream [ "STREAM"; "CREATE"; name ] in
    match encode_config config with
    | Error error -> Error error
    | Ok payload -> (
        match request_msg jetstream (Nats.Message.v ~subject payload) with
        | Error error -> Error error
        | Ok message -> (
            match decode_response message with
            | Error error -> Error error
            | Ok { config = None; _ } -> Error (Error.Missing_field "config")
            | Ok { config = Some response; _ } -> (
                if not (String.equal response.name name) then
                  Error
                    (Error.Unexpected_stream_name
                       { expected = name; actual = response.name })
                else
                  match config_of_wire response with
                  | Ok _ -> Ok { jetstream; name }
                  | Error error -> Error error)))

  let info_of_response ?expected_name (response : response) =
    match response.error with
    | Some error -> Error (Error.Api error)
    | None -> (
        match (response.config, response.state) with
        | None, _ -> Error (Error.Missing_field "config")
        | _, None -> Error (Error.Missing_field "state")
        | Some config, Some state -> (
            match config_of_wire config with
            | Error error -> Error error
            | Ok config -> (
                match expected_name with
                | Some expected
                  when not (String.equal (Config.name config) expected) ->
                    Error
                      (Error.Unexpected_stream_name
                         { expected; actual = Config.name config })
                | _ ->
                    Ok
                      {
                        Info.config;
                        messages = state.messages;
                        bytes = state.bytes;
                        first_sequence = state.first_seq;
                        last_sequence = state.last_seq;
                        consumer_count = state.consumer_count;
                      })))

  let info_response stream =
    let subject =
      api_subject stream.jetstream [ "STREAM"; "INFO"; stream.name ]
    in
    match request_msg stream.jetstream (Nats.Message.v ~subject "") with
    | Error error -> Error error
    | Ok message -> decode_response message

  let update (stream : t) (config : Config.t) =
    let name = Config.name config in
    if not (String.equal name stream.name) then
      Error
        (Error.Unexpected_stream_name { expected = stream.name; actual = name })
    else
      match info_response stream with
      | Error error -> Error error
      | Ok { config = None; _ } -> Error (Error.Missing_field "config")
      | Ok ({ config = Some current; _ } as response) -> (
          match info_of_response ~expected_name:stream.name response with
          | Error error -> Error error
          | Ok _ -> (
              let subject =
                api_subject stream.jetstream [ "STREAM"; "UPDATE"; stream.name ]
              in
              let update_config = wire_config_for_update ~current config in
              match encode_wire_config update_config with
              | Error error -> Error error
              | Ok payload -> (
                  match
                    request_msg stream.jetstream
                      (Nats.Message.v ~subject payload)
                  with
                  | Error error -> Error error
                  | Ok message -> (
                      match decode_response message with
                      | Error error -> Error error
                      | Ok response ->
                          info_of_response ~expected_name:stream.name response))
              ))

  let list ?subject jetstream =
    let offset = ref 0 in
    let infos = ref [] in
    let result = ref None in
    while Option.is_none !result do
      let request =
        {
          offset = !offset;
          subject = Option.map Nats.Subject.Filter.to_string subject;
        }
      in
      match encode list_request_codec request with
      | Error error -> result := Some (Error error)
      | Ok payload -> (
          let subject = api_subject jetstream [ "STREAM"; "LIST" ] in
          match request_msg jetstream (Nats.Message.v ~subject payload) with
          | Error error -> result := Some (Error error)
          | Ok message -> (
              match decode_list_response message with
              | Error error -> result := Some (Error error)
              | Ok { total; offset = page_offset; limit; streams; missing } -> (
                  match missing with
                  | _ :: _ ->
                      result :=
                        Some
                          (Error
                             (Error.Incomplete_list
                                { kind = Error.Streams; missing }))
                  | [] -> (
                      let returned = List.length streams in
                      let window =
                        if Int.compare page_offset total < 0 then
                          Int.min limit (total - page_offset)
                        else 0
                      in
                      if
                        (not (Int.equal page_offset !offset))
                        || not (Int.equal returned window)
                      then
                        result :=
                          Some
                            (Error
                               (Error.Incomplete_list
                                  { kind = Error.Streams; missing = [] }))
                      else
                        let page =
                          List.fold_left
                            (fun page response ->
                              match page with
                              | Error _ -> page
                              | Ok infos -> (
                                  match info_of_response response with
                                  | Ok info -> Ok (info :: infos)
                                  | Error error -> Error error))
                            (Ok []) streams
                        in
                        match page with
                        | Error error -> result := Some (Error error)
                        | Ok page ->
                            infos :=
                              List.fold_left
                                (fun infos info -> info :: infos)
                                !infos (List.rev page);
                            let next_offset = page_offset + window in
                            if Int.compare page_offset total >= 0 then
                              result := Some (Ok (List.rev !infos))
                            else if Int.compare next_offset !offset <= 0 then
                              result :=
                                Some
                                  (Error
                                     (Error.Incomplete_list
                                        { kind = Error.Streams; missing = [] }))
                            else if Int.compare next_offset total >= 0 then
                              result := Some (Ok (List.rev !infos))
                            else offset := next_offset))))
    done;
    match !result with Some result -> result | None -> assert false

  let info stream =
    match info_response stream with
    | Error error -> Error error
    | Ok response -> info_of_response ~expected_name:stream.name response

  let delete stream =
    let subject =
      api_subject stream.jetstream [ "STREAM"; "DELETE"; stream.name ]
    in
    match request_msg stream.jetstream (Nats.Message.v ~subject "") with
    | Error error -> Error error
    | Ok message -> (
        match decode_response message with
        | Ok _ -> Ok ()
        | Error error -> Error error)
end

module Msg = struct
  type jetstream = t

  type metadata = {
    stream : string;
    consumer : string;
    domain : string option;
    num_delivered : int64;
    stream_sequence : int64;
    consumer_sequence : int64;
    timestamp : int64;
    num_pending : int64;
  }

  type t = {
    jetstream : jetstream;
    message : Nats.Message.t;
    ack_subject : Nats.Subject.t;
    metadata : metadata;
  }

  let parse_int64 subject value =
    match Int64.of_string_opt value with
    | Some value when Int64.compare value 0L >= 0 -> Ok value
    | _ -> Error (Error.Invalid_ack_reply subject)

  let metadata ~subject ~domain ~stream ~consumer ~num_delivered
      ~stream_sequence ~consumer_sequence ~timestamp ~num_pending =
    if String.equal stream "" || String.equal consumer "" then
      Error (Error.Invalid_ack_reply subject)
    else
      let ( let* ) value f =
        match value with Error error -> Error error | Ok value -> f value
      in
      let* num_delivered = parse_int64 subject num_delivered in
      let* stream_sequence = parse_int64 subject stream_sequence in
      let* consumer_sequence = parse_int64 subject consumer_sequence in
      let* timestamp = parse_int64 subject timestamp in
      let* num_pending = parse_int64 subject num_pending in
      Ok
        {
          stream;
          consumer;
          domain;
          num_delivered;
          stream_sequence;
          consumer_sequence;
          timestamp;
          num_pending;
        }

  let metadata_of_reply reply =
    let subject = Nats.Subject.to_string reply in
    match String.split_on_char '.' subject with
    | "$JS" :: "ACK" :: fields -> (
        match fields with
        | [
         stream;
         consumer;
         num_delivered;
         stream_sequence;
         consumer_sequence;
         timestamp;
         num_pending;
        ] ->
            metadata ~subject ~domain:None ~stream ~consumer ~num_delivered
              ~stream_sequence ~consumer_sequence ~timestamp ~num_pending
        | domain :: account_hash :: stream :: consumer :: num_delivered
          :: stream_sequence :: consumer_sequence :: timestamp :: num_pending
          :: _ ->
            if String.equal domain "" || String.equal account_hash "" then
              Error (Error.Invalid_ack_reply subject)
            else
              metadata ~subject
                ~domain:(if String.equal domain "_" then None else Some domain)
                ~stream ~consumer ~num_delivered ~stream_sequence
                ~consumer_sequence ~timestamp ~num_pending
        | _ -> Error (Error.Invalid_ack_reply subject))
    | _ -> Error (Error.Invalid_ack_reply subject)

  let of_message ~jetstream ~stream_name ~consumer_name message =
    match Nats.Message.reply_to message with
    | None -> Error Error.Missing_ack_reply
    | Some ack_subject -> (
        match metadata_of_reply ack_subject with
        | Error error -> Error error
        | Ok metadata ->
            if not (String.equal metadata.stream stream_name) then
              Error
                (Error.Unexpected_stream_name
                   { expected = stream_name; actual = metadata.stream })
            else if not (String.equal metadata.consumer consumer_name) then
              Error
                (Error.Unexpected_consumer_name
                   { expected = consumer_name; actual = metadata.consumer })
            else Ok { jetstream; message; ack_subject; metadata })

  let message value = value.message
  let subject value = Nats.Message.subject value.message
  let payload value = Nats.Message.payload value.message
  let headers value = Nats.Message.headers value.message
  let stream value = value.metadata.stream
  let consumer value = value.metadata.consumer
  let domain value = value.metadata.domain
  let timestamp value = value.metadata.timestamp
  let num_delivered value = value.metadata.num_delivered
  let stream_sequence value = value.metadata.stream_sequence
  let consumer_sequence value = value.metadata.consumer_sequence
  let num_pending value = value.metadata.num_pending

  let respond value payload =
    match
      Connection.publish value.jetstream.connection value.ack_subject payload
    with
    | Ok () -> Ok ()
    | Error error -> Error (Error.Connection error)

  let ack value = respond value "+ACK"

  let ack_sync ?timeout value =
    match
      Connection.request ?timeout value.jetstream.connection value.ack_subject
        "+ACK"
    with
    | Ok _ -> Ok ()
    | Error error -> Error (Error.Connection error)

  let nak ?delay value =
    match delay with
    | None -> respond value "-NAK"
    | Some delay ->
        respond value
          (Format.asprintf "-NAK {\"delay\":%Ld}"
             (Mtime.Span.to_uint64_ns delay))

  let term ?reason value =
    match reason with
    | None -> respond value "+TERM"
    | Some reason -> respond value ("+TERM " ^ reason)

  let in_progress value = respond value "+WPI"
end

module Consumer = struct
  module Config = struct
    type ack_policy = No_ack | All | Explicit

    type deliver_policy =
      | All
      | Last
      | New
      | By_start_sequence of int64
      | By_start_time of string
      | Last_per_subject

    type replay_policy = Instant | Original

    type t = {
      durable_name : string option;
      description : string option;
      deliver_policy : deliver_policy;
      ack_policy : ack_policy;
      ack_wait : Mtime.Span.t option;
      max_deliver : int option;
      filter_subject : Nats.Subject.Filter.t option;
      replay_policy : replay_policy;
      max_ack_pending : int option;
      max_waiting : int option;
      max_batch : int option;
      max_expires : Mtime.Span.t option;
      max_bytes : int option;
      headers_only : bool option;
      inactive_threshold : Mtime.Span.t option;
      mem_storage : bool option;
    }

    type error = config_error

    let allowed_name_character character =
      let code = Char.code character in
      (code >= Char.code 'A' && code <= Char.code 'Z')
      || (code >= Char.code 'a' && code <= Char.code 'z')
      || (code >= Char.code '0' && code <= Char.code '9')
      || Char.equal character '_' || Char.equal character '-'

    let validate_name = function
      | None -> Ok ()
      | Some name -> (
          let length = String.length name in
          if Int.equal length 0 then Error Error.Empty_consumer_name
          else
            let invalid = ref None in
            for position = 0 to length - 1 do
              match !invalid with
              | Some _ -> ()
              | None ->
                  let character = String.get name position in
                  if not (allowed_name_character character) then
                    invalid :=
                      Some
                        (Error.Invalid_consumer_name_character
                           { position; character })
            done;
            match !invalid with None -> Ok () | Some error -> Error error)

    let validate_limit field = function
      | None -> Ok ()
      | Some value when Int.compare value (-1) >= 0 -> Ok ()
      | Some value ->
          Error
            (Error.Invalid_consumer_limit { field; value = Int64.of_int value })

    let validate_span error = function
      | None -> Ok ()
      | Some value when Mtime.Span.compare value Mtime.Span.zero > 0 -> Ok ()
      | Some _ -> Error error

    let normalize_span = function
      | Some value when Int.equal (Mtime.Span.compare value Mtime.Span.zero) 0
        ->
          None
      | value -> value

    let validate_deliver_policy = function
      | By_start_sequence sequence when Int64.compare sequence 1L < 0 ->
          Error
            (Error.Invalid_consumer_policy
               { field = "opt_start_seq"; value = Int64.to_string sequence })
      | By_start_time value when String.equal value "" ->
          Error
            (Error.Invalid_consumer_policy { field = "opt_start_time"; value })
      | _ -> Ok ()

    let v ?durable_name ?description ?(deliver_policy = All)
        ?(ack_policy = Explicit) ?ack_wait ?max_deliver ?filter_subject
        ?(replay_policy = Instant) ?max_ack_pending ?max_waiting ?max_batch
        ?max_expires ?max_bytes ?headers_only ?inactive_threshold ?mem_storage
        () =
      let max_expires = normalize_span max_expires in
      let inactive_threshold = normalize_span inactive_threshold in
      let ( let* ) value f =
        match value with Error error -> Error error | Ok value -> f value
      in
      let* () = validate_name durable_name in
      let* () = validate_deliver_policy deliver_policy in
      let* () =
        validate_span
          (Error.Invalid_consumer_span { field = "ack_wait" })
          ack_wait
      in
      let* () =
        validate_span
          (Error.Invalid_consumer_span { field = "max_expires" })
          max_expires
      in
      let* () =
        validate_span
          (Error.Invalid_consumer_span { field = "inactive_threshold" })
          inactive_threshold
      in
      let* () = validate_limit "max_deliver" max_deliver in
      let* () = validate_limit "max_ack_pending" max_ack_pending in
      let* () = validate_limit "max_waiting" max_waiting in
      let* () = validate_limit "max_batch" max_batch in
      let* () = validate_limit "max_bytes" max_bytes in
      Ok
        {
          durable_name;
          description;
          deliver_policy;
          ack_policy;
          ack_wait;
          max_deliver;
          filter_subject;
          replay_policy;
          max_ack_pending;
          max_waiting;
          max_batch;
          max_expires;
          max_bytes;
          headers_only;
          inactive_threshold;
          mem_storage;
        }

    let durable_name value = value.durable_name
    let description value = value.description
    let deliver_policy value = value.deliver_policy
    let ack_policy value = value.ack_policy
    let ack_wait value = value.ack_wait
    let max_deliver value = value.max_deliver
    let filter_subject value = value.filter_subject
    let replay_policy value = value.replay_policy
    let max_ack_pending value = value.max_ack_pending
    let max_waiting value = value.max_waiting
    let max_batch value = value.max_batch
    let max_expires value = value.max_expires
    let max_bytes value = value.max_bytes
    let headers_only value = value.headers_only
    let inactive_threshold value = value.inactive_threshold
    let mem_storage value = value.mem_storage
  end

  type wire_config = {
    durable_name : string option;
    description : string option;
    deliver_policy : string;
    opt_start_seq : int64 option;
    opt_start_time : string option;
    ack_policy : Config.ack_policy;
    ack_wait : int64 option;
    max_deliver : int option;
    filter_subject : string option;
    replay_policy : Config.replay_policy;
    max_ack_pending : int option;
    max_waiting : int option;
    max_batch : int option;
    max_expires : int64 option;
    max_bytes : int option;
    headers_only : bool option;
    inactive_threshold : int64 option;
    mem_storage : bool option;
    unknown : Jsont.json;
  }

  let ack_policy_codec =
    Jsont.enum
      [
        ("none", Config.No_ack);
        ("all", Config.All);
        ("explicit", Config.Explicit);
      ]

  let replay_policy_codec =
    Jsont.enum [ ("instant", Config.Instant); ("original", Config.Original) ]

  let wire_config_codec =
    Jsont.Object.map ~kind:"JetStream consumer config"
      (fun
        durable_name
        description
        deliver_policy
        opt_start_seq
        opt_start_time
        ack_policy
        ack_wait
        max_deliver
        filter_subject
        replay_policy
        max_ack_pending
        max_waiting
        max_batch
        max_expires
        max_bytes
        headers_only
        inactive_threshold
        mem_storage
        unknown
      ->
        {
          durable_name;
          description;
          deliver_policy;
          opt_start_seq;
          opt_start_time;
          ack_policy;
          ack_wait;
          max_deliver;
          filter_subject;
          replay_policy;
          max_ack_pending;
          max_waiting;
          max_batch;
          max_expires;
          max_bytes;
          headers_only;
          inactive_threshold;
          mem_storage;
          unknown;
        })
    |> Jsont.Object.opt_mem "durable_name" Jsont.string ~enc:(fun value ->
        value.durable_name)
    |> Jsont.Object.opt_mem "description" Jsont.string ~enc:(fun value ->
        value.description)
    |> Jsont.Object.mem "deliver_policy" Jsont.string ~enc:(fun value ->
        value.deliver_policy)
    |> Jsont.Object.opt_mem "opt_start_seq" Jsont.int64 ~enc:(fun value ->
        value.opt_start_seq)
    |> Jsont.Object.opt_mem "opt_start_time" Jsont.string ~enc:(fun value ->
        value.opt_start_time)
    |> Jsont.Object.mem "ack_policy" ack_policy_codec ~enc:(fun value ->
        value.ack_policy)
    |> Jsont.Object.opt_mem "ack_wait" Jsont.int64 ~enc:(fun value ->
        value.ack_wait)
    |> Jsont.Object.opt_mem "max_deliver" Jsont.int ~enc:(fun value ->
        value.max_deliver)
    |> Jsont.Object.opt_mem "filter_subject" Jsont.string ~enc:(fun value ->
        value.filter_subject)
    |> Jsont.Object.mem "replay_policy" replay_policy_codec ~enc:(fun value ->
        value.replay_policy)
    |> Jsont.Object.opt_mem "max_ack_pending" Jsont.int ~enc:(fun value ->
        value.max_ack_pending)
    |> Jsont.Object.opt_mem "max_waiting" Jsont.int ~enc:(fun value ->
        value.max_waiting)
    |> Jsont.Object.opt_mem "max_batch" Jsont.int ~enc:(fun value ->
        value.max_batch)
    |> Jsont.Object.opt_mem "max_expires" Jsont.int64 ~enc:(fun value ->
        value.max_expires)
    |> Jsont.Object.opt_mem "max_bytes" Jsont.int ~enc:(fun value ->
        value.max_bytes)
    |> Jsont.Object.opt_mem "headers_only" Jsont.bool ~enc:(fun value ->
        value.headers_only)
    |> Jsont.Object.opt_mem "inactive_threshold" Jsont.int64 ~enc:(fun value ->
        value.inactive_threshold)
    |> Jsont.Object.opt_mem "mem_storage" Jsont.bool ~enc:(fun value ->
        value.mem_storage)
    |> Jsont.Object.keep_unknown
         ~enc:(fun value -> value.unknown)
         Jsont.json_mems
    |> Jsont.Object.finish

  type create_request = { stream_name : string; config : wire_config }

  let create_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer create request"
      (fun stream_name config -> { stream_name; config })
    |> Jsont.Object.mem "stream_name" Jsont.string ~enc:(fun value ->
        value.stream_name)
    |> Jsont.Object.mem "config" wire_config_codec ~enc:(fun value ->
        value.config)
    |> Jsont.Object.finish

  let wire_config value =
    let deliver_policy, opt_start_seq, opt_start_time =
      match Config.deliver_policy value with
      | Config.All -> ("all", None, None)
      | Config.Last -> ("last", None, None)
      | Config.New -> ("new", None, None)
      | Config.By_start_sequence sequence ->
          ("by_start_sequence", Some sequence, None)
      | Config.By_start_time time -> ("by_start_time", None, Some time)
      | Config.Last_per_subject -> ("last_per_subject", None, None)
    in
    {
      durable_name = Config.durable_name value;
      description = Config.description value;
      deliver_policy;
      opt_start_seq;
      opt_start_time;
      ack_policy = Config.ack_policy value;
      ack_wait = Option.map Mtime.Span.to_uint64_ns (Config.ack_wait value);
      max_deliver = Config.max_deliver value;
      filter_subject =
        Option.map Nats.Subject.Filter.to_string (Config.filter_subject value);
      replay_policy = Config.replay_policy value;
      max_ack_pending = Config.max_ack_pending value;
      max_waiting = Config.max_waiting value;
      max_batch = Config.max_batch value;
      max_expires =
        Option.map Mtime.Span.to_uint64_ns (Config.max_expires value);
      max_bytes = Config.max_bytes value;
      headers_only = Config.headers_only value;
      inactive_threshold =
        Option.map Mtime.Span.to_uint64_ns (Config.inactive_threshold value);
      mem_storage = Config.mem_storage value;
      unknown = Jsont.Json.object' [];
    }

  let config_of_wire value =
    let normalize_limit = function Some -1 -> None | value -> value in
    let deliver_policy =
      match value.deliver_policy with
      | "all" -> Ok Config.All
      | "last" -> Ok Config.Last
      | "new" -> Ok Config.New
      | "by_start_sequence" -> (
          match value.opt_start_seq with
          | Some sequence -> Ok (Config.By_start_sequence sequence)
          | None -> Error (Error.Missing_field "opt_start_seq"))
      | "by_start_time" -> (
          match value.opt_start_time with
          | Some time -> Ok (Config.By_start_time time)
          | None -> Error (Error.Missing_field "opt_start_time"))
      | "last_per_subject" -> Ok Config.Last_per_subject
      | value ->
          Error
            (Error.Invalid_config
               (Error.Invalid_consumer_policy
                  { field = "deliver_policy"; value }))
    in
    let filter_subject =
      match value.filter_subject with
      | None -> Ok None
      | Some "" -> Ok None
      | Some subject -> (
          match Nats.Subject.Filter.of_string subject with
          | Ok subject -> Ok (Some subject)
          | Error error -> Error (Error.Invalid_subject error))
    in
    match deliver_policy with
    | Error error -> Error error
    | Ok deliver_policy -> (
        match filter_subject with
        | Error error -> Error error
        | Ok filter_subject -> (
            let ack_wait = Option.map Mtime.Span.of_uint64_ns value.ack_wait in
            let max_expires =
              Option.map Mtime.Span.of_uint64_ns value.max_expires
            in
            let inactive_threshold =
              Option.map Mtime.Span.of_uint64_ns value.inactive_threshold
            in
            let max_deliver = normalize_limit value.max_deliver in
            let max_ack_pending = normalize_limit value.max_ack_pending in
            let max_waiting = normalize_limit value.max_waiting in
            let max_batch = normalize_limit value.max_batch in
            let max_bytes = normalize_limit value.max_bytes in
            match
              Config.v ?durable_name:value.durable_name
                ?description:value.description ~deliver_policy
                ~ack_policy:value.ack_policy ?ack_wait ?max_deliver
                ?filter_subject ~replay_policy:value.replay_policy
                ?max_ack_pending ?max_waiting ?max_batch ?max_expires ?max_bytes
                ?headers_only:value.headers_only ?inactive_threshold
                ?mem_storage:value.mem_storage ()
            with
            | Ok config -> Ok (config, value.unknown)
            | Error error -> Error (Error.Invalid_config error)))

  type wire_sequence = {
    consumer_sequence : int64 option;
    stream_sequence : int64 option;
  }

  let wire_sequence_codec =
    Jsont.Object.map ~kind:"JetStream consumer sequence"
      (fun consumer_sequence stream_sequence ->
        { consumer_sequence; stream_sequence })
    |> Jsont.Object.opt_mem "consumer_seq" Jsont.int64 ~enc:(fun value ->
        value.consumer_sequence)
    |> Jsont.Object.opt_mem "stream_seq" Jsont.int64 ~enc:(fun value ->
        value.stream_sequence)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  type response = {
    error : api_error option;
    stream_name : string option;
    name : string option;
    config : wire_config option;
    created : string option;
    delivered : wire_sequence option;
    ack_floor : wire_sequence option;
    num_ack_pending : int option;
    num_redelivered : int option;
    num_waiting : int option;
    num_pending : int64 option;
    unknown : Jsont.json;
  }

  let response_codec =
    Jsont.Object.map ~kind:"JetStream consumer response"
      (fun
        error
        stream_name
        name
        config
        created
        delivered
        ack_floor
        num_ack_pending
        num_redelivered
        num_waiting
        num_pending
        unknown
      ->
        {
          error;
          stream_name;
          name;
          config;
          created;
          delivered;
          ack_floor;
          num_ack_pending;
          num_redelivered;
          num_waiting;
          num_pending;
          unknown;
        })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.opt_mem "stream_name" Jsont.string ~enc:(fun value ->
        value.stream_name)
    |> Jsont.Object.opt_mem "name" Jsont.string ~enc:(fun value -> value.name)
    |> Jsont.Object.opt_mem "config" wire_config_codec ~enc:(fun value ->
        value.config)
    |> Jsont.Object.opt_mem "created" Jsont.string ~enc:(fun value ->
        value.created)
    |> Jsont.Object.opt_mem "delivered" wire_sequence_codec ~enc:(fun value ->
        value.delivered)
    |> Jsont.Object.opt_mem "ack_floor" wire_sequence_codec ~enc:(fun value ->
        value.ack_floor)
    |> Jsont.Object.opt_mem "num_ack_pending" Jsont.int ~enc:(fun value ->
        value.num_ack_pending)
    |> Jsont.Object.opt_mem "num_redelivered" Jsont.int ~enc:(fun value ->
        value.num_redelivered)
    |> Jsont.Object.opt_mem "num_waiting" Jsont.int ~enc:(fun value ->
        value.num_waiting)
    |> Jsont.Object.opt_mem "num_pending" Jsont.int64 ~enc:(fun value ->
        value.num_pending)
    |> Jsont.Object.keep_unknown
         ~enc:(fun value -> value.unknown)
         Jsont.json_mems
    |> Jsont.Object.finish

  let decode_response message =
    match decode response_codec message with
    | Error error -> Error error
    | Ok { error = Some error; _ } -> Error (Error.Api error)
    | Ok response -> Ok response

  type list_request = { offset : int }

  let list_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer list request" (fun offset ->
        { offset })
    |> Jsont.Object.mem "offset" Jsont.int ~enc:(fun value -> value.offset)
    |> Jsont.Object.finish

  type list_response = {
    error : api_error option;
    total : int;
    offset : int;
    limit : int;
    consumers : response list;
    missing : string list;
  }

  let list_response_codec =
    Jsont.Object.map ~kind:"JetStream consumer list response"
      (fun error total offset limit consumers missing ->
        {
          error;
          total;
          offset;
          limit;
          consumers;
          missing = Option.value ~default:[] missing;
        })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.mem "total" Jsont.int ~enc:(fun value -> value.total)
    |> Jsont.Object.mem "offset" Jsont.int ~enc:(fun value -> value.offset)
    |> Jsont.Object.mem "limit" Jsont.int ~enc:(fun value -> value.limit)
    |> Jsont.Object.mem "consumers" (Jsont.list response_codec)
         ~enc:(fun value -> value.consumers)
    |> Jsont.Object.opt_mem "missing" (Jsont.list Jsont.string)
         ~enc:(fun value ->
           match value.missing with [] -> None | missing -> Some missing)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  let decode_list_response message =
    match decode list_response_codec message with
    | Error error -> Error error
    | Ok { error = Some error; _ } -> Error (Error.Api error)
    | Ok response -> Ok response

  module Info = struct
    type t = {
      name : string;
      stream_name : string;
      created : string option;
      config : Config.t;
      unknown : Jsont.json;
      config_unknown : Jsont.json;
      delivered : wire_sequence option;
      ack_floor : wire_sequence option;
      num_ack_pending : int;
      num_redelivered : int;
      num_waiting : int;
      num_pending : int64;
    }

    let name value = value.name
    let stream_name value = value.stream_name
    let created value = value.created
    let config value = value.config
    let unknown value = value.unknown
    let config_unknown value = value.config_unknown

    let delivered_consumer_sequence value =
      Option.bind value.delivered (fun sequence -> sequence.consumer_sequence)

    let delivered_stream_sequence value =
      Option.bind value.delivered (fun sequence -> sequence.stream_sequence)

    let ack_floor_consumer_sequence value =
      Option.bind value.ack_floor (fun sequence -> sequence.consumer_sequence)

    let ack_floor_stream_sequence value =
      Option.bind value.ack_floor (fun sequence -> sequence.stream_sequence)

    let num_ack_pending value = value.num_ack_pending
    let num_redelivered value = value.num_redelivered
    let num_waiting value = value.num_waiting
    let num_pending value = value.num_pending

    let pp ppf value =
      Format.fprintf ppf "JetStream consumer %S on stream %S (pending=%Ld)"
        value.name value.stream_name value.num_pending
  end

  let info_of_response ~stream ?expected_name (response : response) =
    match response.error with
    | Some error -> Error (Error.Api error)
    | None -> (
        match (response.stream_name, response.name, response.config) with
        | None, _, _ -> Error (Error.Missing_field "stream_name")
        | _, None, _ -> Error (Error.Missing_field "name")
        | _, _, None -> Error (Error.Missing_field "config")
        | Some stream_name, Some name, Some config -> (
            if not (String.equal stream_name (Stream.name stream)) then
              Error
                (Error.Unexpected_stream_name
                   { expected = Stream.name stream; actual = stream_name })
            else
              match expected_name with
              | Some expected when not (String.equal name expected) ->
                  Error
                    (Error.Unexpected_consumer_name { expected; actual = name })
              | _ -> (
                  match config_of_wire config with
                  | Error error -> Error error
                  | Ok (config, config_unknown) ->
                      Ok
                        {
                          Info.name;
                          stream_name;
                          created = response.created;
                          config;
                          unknown = response.unknown;
                          config_unknown;
                          delivered = response.delivered;
                          ack_floor = response.ack_floor;
                          num_ack_pending =
                            Option.value ~default:0 response.num_ack_pending;
                          num_redelivered =
                            Option.value ~default:0 response.num_redelivered;
                          num_waiting =
                            Option.value ~default:0 response.num_waiting;
                          num_pending =
                            Option.value ~default:0L response.num_pending;
                        })))

  type jetstream = t
  type stream = Stream.t
  type t = { jetstream : jetstream; stream : stream; name : string }

  type next_request = {
    expires : int64;
    batch : int;
    max_bytes : int option;
    idle_heartbeat : int64 option;
  }

  let next_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer pull request"
      (fun expires batch max_bytes idle_heartbeat ->
        { expires; batch; max_bytes; idle_heartbeat })
    |> Jsont.Object.mem "expires" Jsont.int64 ~enc:(fun value -> value.expires)
    |> Jsont.Object.mem "batch" Jsont.int ~enc:(fun value -> value.batch)
    |> Jsont.Object.opt_mem "max_bytes" Jsont.int ~enc:(fun value ->
        value.max_bytes)
    |> Jsont.Object.opt_mem "idle_heartbeat" Jsont.int64 ~enc:(fun value ->
        value.idle_heartbeat)
    |> Jsont.Object.finish

  let default_fetch_expires = Mtime.Span.(5 * s)
  let fetch_expiry_leeway = Mtime.Span.(10 * ms)

  let add_fetch_expiry_leeway expires =
    let expires_ns = Mtime.Span.to_uint64_ns expires in
    let leeway_ns = Mtime.Span.to_uint64_ns fetch_expiry_leeway in
    if Int64.compare expires_ns (Int64.sub Int64.max_int leeway_ns) >= 0 then
      Mtime.Span.max_span
    else Mtime.Span.of_uint64_ns (Int64.add expires_ns leeway_ns)

  let validate_fetch ~batch ~expires ~max_bytes ~idle_heartbeat =
    let ( let* ) value f =
      match value with Error error -> Error error | Ok value -> f value
    in
    let* () =
      if Int.compare batch 1 >= 0 && Int.compare batch 256 <= 0 then Ok ()
      else Error (Error.Invalid_batch batch)
    in
    let* () =
      if Mtime.Span.compare expires Mtime.Span.zero > 0 then Ok ()
      else Error Error.Invalid_fetch_span
    in
    let* () =
      match max_bytes with
      | None -> Ok ()
      | Some value when Int.compare value 0 >= 0 -> Ok ()
      | Some value -> Error (Error.Invalid_max_bytes value)
    in
    match idle_heartbeat with
    | None -> Ok ()
    | Some heartbeat ->
        if Mtime.Span.compare heartbeat Mtime.Span.zero <= 0 then
          Error Error.Invalid_idle_heartbeat
        else
          let heartbeat_ns = Mtime.Span.to_uint64_ns heartbeat in
          let max_heartbeat_ns =
            Int64.div (Mtime.Span.to_uint64_ns expires) 2L
          in
          if Int64.compare heartbeat_ns max_heartbeat_ns <= 0 then Ok ()
          else Error Error.Idle_heartbeat_expires_too_short

  let heartbeat_deadline_at connection = function
    | None -> None
    | Some heartbeat -> (
        let timeout_ns = Int64.mul (Mtime.Span.to_uint64_ns heartbeat) 2L in
        let timeout = Mtime.Span.of_uint64_ns timeout_ns in
        match Mtime.add_span (Connection.now connection) timeout with
        | Some deadline -> Some deadline
        | None -> Some Mtime.max_stamp)

  let earliest_deadline first second =
    match (first, second) with
    | None, None -> None
    | Some deadline, None | None, Some deadline -> Some deadline
    | Some first, Some second ->
        Some (if Mtime.compare first second <= 0 then first else second)

  let contains ~needle value =
    let value_length = String.length value in
    let needle_length = String.length needle in
    if needle_length = 0 then true
    else if needle_length > value_length then false
    else
      let found = ref false in
      let index = ref 0 in
      while (not !found) && !index <= value_length - needle_length do
        if String.equal (String.sub value !index needle_length) needle then
          found := true;
        incr index
      done;
      !found

  type status_classification =
    | Status_idle_heartbeat
    | Status_request_expired
    | Status_batch_completed
    | Status_max_bytes
    | Status_consumer_deleted
    | Status_conflict
    | Status_unexpected

  let classify_status status =
    let code = status.Nats.Op.code in
    let description = status.Nats.Op.description in
    let normalized = String.lowercase_ascii description in
    if contains ~needle:"consumer deleted" normalized then
      Status_consumer_deleted
    else if Int.equal code 100 && contains ~needle:"idle heartbeat" normalized
    then Status_idle_heartbeat
    else if Int.equal code 408 then Status_request_expired
    else if
      Int.equal code 409
      && contains ~needle:"message size exceeds maxbytes" normalized
    then Status_max_bytes
    else if Int.equal code 409 && contains ~needle:"batch completed" normalized
    then Status_batch_completed
    else if Int.equal code 409 then Status_conflict
    else Status_unexpected

  let status_result status =
    let code = status.Nats.Op.code in
    let description = status.Nats.Op.description in
    match classify_status status with
    | Status_request_expired | Status_batch_completed | Status_max_bytes ->
        Ok ()
    | Status_consumer_deleted -> Error Error.Consumer_deleted
    | Status_conflict -> Error (Error.Conflict { code; description })
    | Status_idle_heartbeat | Status_unexpected ->
        Error (Error.Unexpected_status { code; description })

  let pull_status_result status =
    let code = status.Nats.Op.code in
    let description = status.Nats.Op.description in
    match classify_status status with
    | Status_request_expired | Status_batch_completed -> Ok ()
    | Status_max_bytes | Status_conflict ->
        Error (Error.Conflict { code; description })
    | Status_consumer_deleted -> Error Error.Consumer_deleted
    | Status_idle_heartbeat | Status_unexpected ->
        Error (Error.Unexpected_status { code; description })

  let release_subscription subscription =
    match
      Eio.Cancel.protect (fun () ->
          Connection.Subscription.unsubscribe subscription)
    with
    | Ok () -> None
    | Error error -> Some error

  let with_fetch_subscription consumer f =
    let connection = consumer.jetstream.connection in
    let inbox = Connection.fresh_inbox connection in
    let filter = Nats.Subject.Filter.literal (Nats.Subject.to_string inbox) in
    match Connection.subscribe connection filter with
    | Error error -> Error (Error.Connection error)
    | Ok subscription -> (
        let cleanup_error = ref None in
        let result =
          Fun.protect
            ~finally:(fun () ->
              cleanup_error := release_subscription subscription)
            (fun () -> f ~inbox subscription)
        in
        match (result, !cleanup_error) with
        | Ok value, None -> Ok value
        | Ok value, Some cleanup_error ->
            ignore cleanup_error;
            Ok value
        | Error error, _ -> Error error)

  let fetch ?expires ?idle_heartbeat ?max_bytes consumer ~batch =
    let expires = Option.value expires ~default:default_fetch_expires in
    match validate_fetch ~batch ~expires ~max_bytes ~idle_heartbeat with
    | Error error -> Error error
    | Ok () -> (
        let request =
          {
            expires = Mtime.Span.to_uint64_ns expires;
            batch;
            max_bytes;
            idle_heartbeat = Option.map Mtime.Span.to_uint64_ns idle_heartbeat;
          }
        in
        match encode next_request_codec request with
        | Error error -> Error error
        | Ok payload ->
            with_fetch_subscription consumer (fun ~inbox subscription ->
                let subject =
                  api_subject consumer.jetstream
                    [
                      "CONSUMER";
                      "MSG";
                      "NEXT";
                      Stream.name consumer.stream;
                      consumer.name;
                    ]
                in
                let connection = consumer.jetstream.connection in
                match
                  Connection.publish connection ~reply_to:inbox subject payload
                with
                | Error error -> Error (Error.Connection error)
                | Ok () -> (
                    let messages = ref [] in
                    let count = ref 0 in
                    let deadline =
                      let local_expires = add_fetch_expiry_leeway expires in
                      match
                        Mtime.add_span (Connection.now connection) local_expires
                      with
                      | Some deadline -> deadline
                      | None -> Mtime.max_stamp
                    in
                    let heartbeat_deadline =
                      ref (heartbeat_deadline_at connection idle_heartbeat)
                    in
                    let terminal = ref None in
                    while
                      Int.compare !count batch < 0 && Option.is_none !terminal
                    do
                      let current = Connection.now connection in
                      let wait_deadline =
                        Option.value
                          (earliest_deadline !heartbeat_deadline (Some deadline))
                          ~default:deadline
                      in
                      let wait_result =
                        if Mtime.compare current wait_deadline >= 0 then
                          Error Core_error.Timeout
                        else
                          let remaining = Mtime.span current wait_deadline in
                          match
                            Connection.Subscription.next_with_timeout
                              ~timeout:remaining subscription
                          with
                          | Error error -> Error error
                          | Ok delivery -> Ok delivery
                      in
                      match wait_result with
                      | Error Core_error.Timeout -> (
                          let now = Connection.now connection in
                          match !heartbeat_deadline with
                          | Some heartbeat_deadline
                            when Mtime.compare now heartbeat_deadline >= 0 ->
                              terminal := Some (Error Error.Missing_heartbeat)
                          | _ -> terminal := Some (Ok (List.rev !messages)))
                      | Error error ->
                          terminal := Some (Error (Error.Connection error))
                      | Ok delivery -> (
                          match delivery.status with
                          | None -> (
                              match
                                Msg.of_message ~jetstream:consumer.jetstream
                                  ~stream_name:(Stream.name consumer.stream)
                                  ~consumer_name:consumer.name delivery.message
                              with
                              | Error error -> terminal := Some (Error error)
                              | Ok message ->
                                  heartbeat_deadline :=
                                    heartbeat_deadline_at connection
                                      idle_heartbeat;
                                  messages := message :: !messages;
                                  count := !count + 1)
                          | Some status -> (
                              match classify_status status with
                              | Status_idle_heartbeat -> (
                                  match idle_heartbeat with
                                  | Some _ ->
                                      heartbeat_deadline :=
                                        heartbeat_deadline_at connection
                                          idle_heartbeat
                                  | None -> (
                                      match status_result status with
                                      | Ok () ->
                                          terminal :=
                                            Some (Ok (List.rev !messages))
                                      | Error error ->
                                          terminal := Some (Error error)))
                              | _ -> (
                                  match status_result status with
                                  | Ok () ->
                                      terminal := Some (Ok (List.rev !messages))
                                  | Error error ->
                                      terminal := Some (Error error))))
                    done;
                    match !terminal with
                    | Some result -> result
                    | None -> Ok (List.rev !messages))))

  module Pull = struct
    type consumer = t
    type state = Open | Closed | Failed of Error.t

    type t = {
      consumer : consumer;
      connection : Connection.t;
      subscription : Connection.Subscription.t;
      inbox : Nats.Subject.t;
      subject : Nats.Subject.t;
      payload : string;
      batch : int;
      mutable remaining : int;
      idle_heartbeat : Mtime.Span.t option;
      mutable heartbeat_deadline : Mtime.t option;
      mutable state : state;
      mutable hook : Eio.Switch.hook option;
    }

    let fail pull error =
      match pull.state with
      | Open ->
          pull.state <- Failed error;
          ignore (release_subscription pull.subscription)
      | Closed | Failed _ -> ()

    let connection_error pull error =
      let error = Error.Connection error in
      fail pull error;
      Error error

    let subscription_error pull error =
      match (pull.state, error) with
      | Closed, (Core_error.Closed | Core_error.Draining) ->
          Error Error.Pull_closed
      | _, error -> connection_error pull error

    let close pull =
      match pull.state with
      | Closed -> Ok ()
      | Open | Failed _ -> (
          pull.state <- Closed;
          Option.iter
            (fun hook -> ignore (Eio.Switch.try_remove_hook hook))
            pull.hook;
          pull.hook <- None;
          match release_subscription pull.subscription with
          | None -> Ok ()
          | Some error -> Error (Error.Connection error))

    let ensure_request pull =
      if Int.compare pull.remaining 0 > 0 then Ok ()
      else
        match pull.state with
        | Closed -> Error Error.Pull_closed
        | Failed error -> Error error
        | Open -> (
            match
              Connection.publish pull.connection ~reply_to:pull.inbox
                pull.subject pull.payload
            with
            | Ok () -> (
                match pull.state with
                | Open ->
                    pull.remaining <- pull.batch;
                    pull.heartbeat_deadline <-
                      heartbeat_deadline_at pull.connection pull.idle_heartbeat;
                    Ok ()
                | Closed -> Error Error.Pull_closed
                | Failed error -> Error error)
            | Error error -> connection_error pull error)

    let timeout_error = Error.Connection (Core_error.Invalid_timeout "pull")
    let timed_out = Error.Connection Core_error.Timeout

    let heartbeat_missed pull =
      match pull.heartbeat_deadline with
      | Some deadline ->
          Mtime.compare (Connection.now pull.connection) deadline >= 0
      | None -> false

    let consume_delivery pull (delivery : Connection.Subscription.delivery) =
      match delivery.status with
      | Some status -> (
          match classify_status status with
          | Status_idle_heartbeat -> (
              match pull.idle_heartbeat with
              | Some _ ->
                  pull.heartbeat_deadline <-
                    heartbeat_deadline_at pull.connection pull.idle_heartbeat;
                  Ok None
              | None -> (
                  match pull_status_result status with
                  | Ok () ->
                      pull.remaining <- 0;
                      pull.heartbeat_deadline <- None;
                      Ok None
                  | Error error -> Error error))
          | _ -> (
              match pull_status_result status with
              | Ok () ->
                  pull.remaining <- 0;
                  pull.heartbeat_deadline <- None;
                  Ok None
              | Error error -> Error error))
      | None -> (
          match
            Msg.of_message ~jetstream:pull.consumer.jetstream
              ~stream_name:(Stream.name pull.consumer.stream)
              ~consumer_name:pull.consumer.name delivery.message
          with
          | Error error -> Error error
          | Ok message ->
              pull.remaining <- pull.remaining - 1;
              if Int.equal pull.remaining 0 then pull.heartbeat_deadline <- None
              else
                pull.heartbeat_deadline <-
                  heartbeat_deadline_at pull.connection pull.idle_heartbeat;
              Ok (Some message))

    let next_loop pull ~deadline =
      let result = ref None in
      let handle_delivery delivery =
        match consume_delivery pull delivery with
        | Ok None -> ()
        | Ok (Some message) -> result := Some (Ok message)
        | Error error ->
            fail pull error;
            result := Some (Error error)
      in
      while Option.is_none !result do
        match pull.state with
        | Closed -> result := Some (Error Error.Pull_closed)
        | Failed error -> result := Some (Error error)
        | Open -> (
            match
              Connection.Subscription.next_nonblocking pull.subscription
            with
            | Some (Ok delivery) -> handle_delivery delivery
            | Some (Error error) ->
                result := Some (subscription_error pull error)
            | None -> (
                let deadline_reached =
                  match deadline with
                  | Some deadline ->
                      Mtime.compare (Connection.now pull.connection) deadline
                      >= 0
                  | None -> false
                in
                if heartbeat_missed pull then (
                  let error = Error.Missing_heartbeat in
                  fail pull error;
                  result := Some (Error error))
                else if deadline_reached then result := Some (Error timed_out)
                else
                  match ensure_request pull with
                  | Error error -> result := Some (Error error)
                  | Ok () -> (
                      let wait_result =
                        match
                          earliest_deadline deadline pull.heartbeat_deadline
                        with
                        | None ->
                            Connection.Subscription.next pull.subscription
                        | Some wait_deadline ->
                            let now = Connection.now pull.connection in
                            if Mtime.compare now wait_deadline >= 0 then
                              Error Core_error.Timeout
                            else
                              let timeout = Mtime.span now wait_deadline in
                              Connection.Subscription.next_with_timeout ~timeout
                                pull.subscription
                      in
                      match wait_result with
                      | Error Core_error.Timeout ->
                          if heartbeat_missed pull then (
                            let error = Error.Missing_heartbeat in
                            fail pull error;
                            result := Some (Error error))
                          else result := Some (Error timed_out)
                      | Error error ->
                          result := Some (subscription_error pull error)
                      | Ok delivery -> handle_delivery delivery)))
      done;
      match !result with Some result -> result | None -> assert false

    let v ~sw ?(batch = 1) ?expires ?idle_heartbeat ?max_bytes consumer =
      let expires = Option.value expires ~default:default_fetch_expires in
      match validate_fetch ~batch ~expires ~max_bytes ~idle_heartbeat with
      | Error error -> Error error
      | Ok () -> (
          let request =
            {
              expires = Mtime.Span.to_uint64_ns expires;
              batch;
              max_bytes;
              idle_heartbeat = Option.map Mtime.Span.to_uint64_ns idle_heartbeat;
            }
          in
          match encode next_request_codec request with
          | Error error -> Error error
          | Ok payload -> (
              let connection = consumer.jetstream.connection in
              let inbox = Connection.fresh_inbox connection in
              let filter =
                Nats.Subject.Filter.literal (Nats.Subject.to_string inbox)
              in
              match
                Connection.subscribe connection ~replay_on_reconnect:false
                  filter
              with
              | Error error -> Error (Error.Connection error)
              | Ok subscription ->
                  let subject =
                    api_subject consumer.jetstream
                      [
                        "CONSUMER";
                        "MSG";
                        "NEXT";
                        Stream.name consumer.stream;
                        consumer.name;
                      ]
                  in
                  let pull =
                    {
                      consumer;
                      connection;
                      subscription;
                      inbox;
                      subject;
                      payload;
                      batch;
                      remaining = 0;
                      idle_heartbeat;
                      heartbeat_deadline = None;
                      state = Open;
                      hook = None;
                    }
                  in
                  let hook =
                    Eio.Switch.on_release_cancellable sw (fun () ->
                        ignore (close pull))
                  in
                  pull.hook <- Some hook;
                  Ok pull))

    let next pull = next_loop pull ~deadline:None

    let next_with_timeout ~timeout pull =
      if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
        Error timeout_error
      else
        let deadline =
          match Mtime.add_span (Connection.now pull.connection) timeout with
          | Some deadline -> deadline
          | None -> Mtime.max_stamp
        in
        next_loop pull ~deadline:(Some deadline)

    let iter pull ~f =
      let result = ref None in
      while Option.is_none !result do
        match next pull with
        | Ok message -> f message
        | Error Error.Pull_closed -> result := Some (Ok ())
        | Error error -> result := Some (Error error)
      done;
      match !result with Some result -> result | None -> assert false
  end

  let bind (stream : Stream.t) ~name =
    match Config.validate_name (Some name) with
    | Ok () -> Ok { jetstream = stream.jetstream; stream; name }
    | Error error -> Error (Error.Invalid_config error)

  let name value = value.name
  let stream value = value.stream

  let create (stream : Stream.t) config =
    let jetstream = stream.jetstream in
    let stream_name = Stream.name stream in
    let subject =
      match Config.durable_name config with
      | None -> api_subject jetstream [ "CONSUMER"; "CREATE"; stream_name ]
      | Some name ->
          api_subject jetstream [ "CONSUMER"; "CREATE"; stream_name; name ]
    in
    let request = { stream_name; config = wire_config config } in
    match encode create_request_codec request with
    | Error error -> Error error
    | Ok payload -> (
        match request_msg jetstream (Nats.Message.v ~subject payload) with
        | Error error -> Error error
        | Ok message -> (
            match decode_response message with
            | Error error -> Error error
            | Ok { name = None; _ } -> Error (Error.Missing_field "name")
            | Ok { name = Some name; config = None; _ } ->
                Error (Error.Missing_field "config")
            | Ok { name = Some name; config = Some response_config; _ } -> (
                match Config.durable_name config with
                | Some expected when not (String.equal expected name) ->
                    Error
                      (Error.Unexpected_consumer_name
                         { expected; actual = name })
                | _ -> (
                    match config_of_wire response_config with
                    | Ok _ -> Ok { jetstream; stream; name }
                    | Error error -> Error error))))

  let info consumer =
    let subject =
      api_subject consumer.jetstream
        [ "CONSUMER"; "INFO"; Stream.name consumer.stream; consumer.name ]
    in
    match request_msg consumer.jetstream (Nats.Message.v ~subject "") with
    | Error error -> Error error
    | Ok message -> (
        match decode_response message with
        | Error error -> Error error
        | Ok response ->
            info_of_response ~stream:consumer.stream
              ~expected_name:consumer.name response)

  let list (stream : stream) =
    let offset = ref 0 in
    let infos = ref [] in
    let result = ref None in
    while Option.is_none !result do
      let request = { offset = !offset } in
      match encode list_request_codec request with
      | Error error -> result := Some (Error error)
      | Ok payload -> (
          let subject =
            api_subject stream.jetstream
              [ "CONSUMER"; "LIST"; Stream.name stream ]
          in
          match
            request_msg stream.jetstream (Nats.Message.v ~subject payload)
          with
          | Error error -> result := Some (Error error)
          | Ok message -> (
              match decode_list_response message with
              | Error error -> result := Some (Error error)
              | Ok { total; offset = page_offset; limit; consumers; missing }
                -> (
                  match missing with
                  | _ :: _ ->
                      result :=
                        Some
                          (Error
                             (Error.Incomplete_list
                                { kind = Error.Consumers; missing }))
                  | [] -> (
                      let returned = List.length consumers in
                      let window =
                        if Int.compare page_offset total < 0 then
                          Int.min limit (total - page_offset)
                        else 0
                      in
                      if
                        (not (Int.equal page_offset !offset))
                        || not (Int.equal returned window)
                      then
                        result :=
                          Some
                            (Error
                               (Error.Incomplete_list
                                  { kind = Error.Consumers; missing = [] }))
                      else
                        let page =
                          List.fold_left
                            (fun page response ->
                              match page with
                              | Error _ -> page
                              | Ok infos -> (
                                  match
                                    info_of_response ~stream ?expected_name:None
                                      response
                                  with
                                  | Ok info -> Ok (info :: infos)
                                  | Error error -> Error error))
                            (Ok []) consumers
                        in
                        match page with
                        | Error error -> result := Some (Error error)
                        | Ok page ->
                            infos :=
                              List.fold_left
                                (fun infos info -> info :: infos)
                                !infos (List.rev page);
                            let next_offset = page_offset + window in
                            if Int.compare page_offset total >= 0 then
                              result := Some (Ok (List.rev !infos))
                            else if Int.compare next_offset !offset <= 0 then
                              result :=
                                Some
                                  (Error
                                     (Error.Incomplete_list
                                        { kind = Error.Consumers; missing = [] }))
                            else if Int.compare next_offset total >= 0 then
                              result := Some (Ok (List.rev !infos))
                            else offset := next_offset))))
    done;
    match !result with Some result -> result | None -> assert false

  let delete consumer =
    let subject =
      api_subject consumer.jetstream
        [ "CONSUMER"; "DELETE"; Stream.name consumer.stream; consumer.name ]
    in
    match request_msg consumer.jetstream (Nats.Message.v ~subject "") with
    | Error error -> Error error
    | Ok message -> (
        match decode_response message with
        | Ok _ -> Ok ()
        | Error error -> Error error)
end

module Publish_ack = struct
  type t = {
    stream : string;
    sequence : int64;
    duplicate : bool;
    domain : string option;
  }

  let stream value = value.stream
  let sequence value = value.sequence
  let duplicate value = value.duplicate
  let domain value = value.domain

  let pp ppf value =
    Format.fprintf ppf
      "JetStream publish ack(stream=%S, sequence=%Ld, duplicate=%b)"
      value.stream value.sequence value.duplicate
end

type publish_response = {
  error : api_error option;
  stream : string option;
  sequence : int64 option;
  duplicate : bool option;
  domain : string option;
}

let publish_response_codec =
  Jsont.Object.map ~kind:"JetStream publish acknowledgement"
    (fun error stream sequence duplicate domain ->
      { error; stream; sequence; duplicate; domain })
  |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
      value.error)
  |> Jsont.Object.opt_mem "stream" Jsont.string ~enc:(fun value -> value.stream)
  |> Jsont.Object.opt_mem "seq" Jsont.int64 ~enc:(fun value -> value.sequence)
  |> Jsont.Object.opt_mem "duplicate" Jsont.bool ~enc:(fun value ->
      value.duplicate)
  |> Jsont.Object.opt_mem "domain" Jsont.string ~enc:(fun value -> value.domain)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let publish ?timeout ?(headers = Nats.Header.empty) ?msg_id jetstream subject
    payload =
  let headers =
    match msg_id with
    | None -> Ok headers
    | Some value when String.equal value "" -> Error Error.Empty_msg_id
    | Some _ when Nats.Header.mem "Nats-Msg-Id" headers ->
        Error Error.Msg_id_already_set
    | Some value -> (
        match Nats.Header.add ~name:"Nats-Msg-Id" ~value headers with
        | Ok headers -> Ok headers
        | Error error -> Error (Error.Invalid_headers error))
  in
  match headers with
  | Error error -> Error error
  | Ok headers -> (
      let message = Nats.Message.v ~subject ~headers payload in
      match request_msg ?timeout jetstream message with
      | Error error -> Error error
      | Ok response -> (
          match decode publish_response_codec response with
          | Error error -> Error error
          | Ok { error = Some error; _ } -> Error (Error.Api error)
          | Ok { stream = None; _ } -> Error (Error.Missing_field "stream")
          | Ok { sequence = None; _ } -> Error (Error.Missing_field "seq")
          | Ok
              {
                stream = Some stream;
                sequence = Some sequence;
                duplicate;
                domain;
                _;
              } ->
              Ok
                {
                  Publish_ack.stream;
                  sequence;
                  duplicate = Option.value ~default:false duplicate;
                  domain;
                }))
