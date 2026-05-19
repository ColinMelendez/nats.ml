module Core_error = Error

module Error = struct
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Empty_subjects
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_max_age

  type api = { code : int; err_code : int option; description : string }

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
        Format.fprintf ppf "invalid JetStream stream config: %a" pp_config error
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

    let v ~name ~subjects ?(storage = File) ?(retention = Limits)
        ?(discard = Old) ?max_msgs ?max_bytes ?max_age ?max_msg_size () =
      let max_age =
        match max_age with
        | Some value when Int.equal (Mtime.Span.compare value Mtime.Span.zero) 0
          ->
            None
        | value -> value
      in
      match validate_name name with
      | Error error -> Error error
      | Ok () when Int.equal (List.length subjects) 0 ->
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

    let name value = value.name
    let subjects value = value.subjects
    let storage value = value.storage
    let retention value = value.retention
    let discard value = value.discard
    let max_msgs value = value.max_msgs
    let max_bytes value = value.max_bytes
    let max_age value = value.max_age
    let max_msg_size value = value.max_msg_size
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
      ->
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
        })
    |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
    |> Jsont.Object.mem "subjects" (Jsont.list Jsont.string) ~enc:(fun value ->
        value.subjects)
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
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

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
          Config.v ~name:value.name ~subjects ~storage:value.storage
            ~retention:value.retention ~discard:value.discard ?max_msgs
            ?max_bytes ?max_age ?max_msg_size ()
        with
        | Ok config -> Ok config
        | Error error -> Error (Error.Invalid_config error))

  let decode_response message =
    match decode response_codec message with
    | Error error -> Error error
    | Ok { error = Some error; _ } -> Error (Error.Api error)
    | Ok response -> Ok response

  let encode_config value = encode wire_config_codec (wire_config value)
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

  let info stream =
    let subject =
      api_subject stream.jetstream [ "STREAM"; "INFO"; stream.name ]
    in
    match request_msg stream.jetstream (Nats.Message.v ~subject "") with
    | Error error -> Error error
    | Ok message -> (
        match decode_response message with
        | Error error -> Error error
        | Ok { config = None; _ } -> Error (Error.Missing_field "config")
        | Ok { state = None; _ } -> Error (Error.Missing_field "state")
        | Ok { config = Some config; state = Some state } -> (
            match config_of_wire config with
            | Error error -> Error error
            | Ok config ->
                if not (String.equal (Config.name config) stream.name) then
                  Error
                    (Error.Unexpected_stream_name
                       { expected = stream.name; actual = Config.name config })
                else
                  Ok
                    {
                      Info.config;
                      messages = state.messages;
                      bytes = state.bytes;
                      first_sequence = state.first_seq;
                      last_sequence = state.last_seq;
                      consumer_count = state.consumer_count;
                    }))

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
