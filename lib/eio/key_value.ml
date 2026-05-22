module Core_error = Error

module Entry = struct
  type operation = Put | Delete | Purge

  type t = {
    bucket : string;
    key : string;
    value : string;
    revision : int64;
    timestamp : string;
    operation : operation;
  }

  let bucket value = value.bucket
  let key value = value.key
  let value value = value.value
  let revision value = value.revision
  let timestamp value = value.timestamp
  let operation value = value.operation

  let pp_operation ppf = function
    | Put -> Format.pp_print_string ppf "put"
    | Delete -> Format.pp_print_string ppf "delete"
    | Purge -> Format.pp_print_string ppf "purge"
end

module Error = struct
  type config =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_history of int
    | Invalid_ttl
    | Invalid_limit of { field : string; value : int64 }

  type key =
    | Empty_key
    | Invalid_key_character of { position : int; character : char }
    | Invalid_key_dots

  type t =
    | Connection of Connection.error
    | Jetstream of Jetstream.Error.t
    | Invalid_config of config
    | Invalid_key of { value : string; reason : key }
    | Invalid_revision of int64
    | Invalid_headers of Nats.Header.error
    | Invalid_operation of string
    | Key_not_found
    | Key_deleted of Entry.t
    | Key_exists
    | Revision_mismatch of { expected : int64 }
    | Closed

  let pp_config ppf = function
    | Empty_bucket -> Format.pp_print_string ppf "bucket name is empty"
    | Invalid_bucket_character { position; character } ->
        Format.fprintf ppf "invalid bucket-name character %C at position %d"
          character position
    | Invalid_history value ->
        Format.fprintf ppf "key-value history must be between 1 and 64, got %d"
          value
    | Invalid_ttl ->
        Format.pp_print_string ppf "key-value TTL must not be negative"
    | Invalid_limit { field; value } ->
        Format.fprintf ppf "invalid key-value %s limit %Ld" field value

  let pp_key ppf = function
    | Empty_key -> Format.pp_print_string ppf "key is empty"
    | Invalid_key_character { position; character } ->
        Format.fprintf ppf "invalid key character %C at position %d" character
          position
    | Invalid_key_dots ->
        Format.pp_print_string ppf
          "key must not start or end with a dot or contain consecutive dots"

  let pp ppf = function
    | Connection error ->
        Format.fprintf ppf "connection: %a" Core_error.pp error
    | Jetstream error ->
        Format.fprintf ppf "JetStream: %a" Jetstream.Error.pp error
    | Invalid_config error ->
        Format.fprintf ppf "invalid key-value config: %a" pp_config error
    | Invalid_key { value; reason } ->
        Format.fprintf ppf "invalid key %S: %a" value pp_key reason
    | Invalid_revision value ->
        Format.fprintf ppf "invalid key-value revision %Ld" value
    | Invalid_headers error ->
        Format.fprintf ppf "invalid key-value headers: %a" Nats.Header.pp_error
          error
    | Invalid_operation value ->
        Format.fprintf ppf "invalid key-value operation %S" value
    | Key_not_found -> Format.pp_print_string ppf "key was not found"
    | Key_deleted entry ->
        Format.fprintf ppf "key %S was deleted at revision %Ld"
          (Entry.key entry) (Entry.revision entry)
    | Key_exists -> Format.pp_print_string ppf "key already exists"
    | Revision_mismatch { expected } ->
        Format.fprintf ppf "key revision did not match expected revision %Ld"
          expected
    | Closed -> Format.pp_print_string ppf "key-value handle is closed"
end

module Config = struct
  type storage = Memory | File

  type t = {
    bucket : string;
    history : int;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    max_value_size : int64 option;
    storage : storage;
  }

  type error = Error.config

  let allowed_bucket_character character =
    let code = Char.code character in
    (code >= Char.code 'A' && code <= Char.code 'Z')
    || (code >= Char.code 'a' && code <= Char.code 'z')
    || (code >= Char.code '0' && code <= Char.code '9')
    || Char.equal character '_' || Char.equal character '-'

  let validate_bucket bucket =
    let length = String.length bucket in
    if Int.equal length 0 then Error Error.Empty_bucket
    else
      let invalid = ref None in
      for position = 0 to length - 1 do
        match !invalid with
        | Some _ -> ()
        | None ->
            let character = String.get bucket position in
            if not (allowed_bucket_character character) then
              invalid :=
                Some (Error.Invalid_bucket_character { position; character })
      done;
      match !invalid with None -> Ok () | Some error -> Error error

  let validate_limit field = function
    | None -> Ok ()
    | Some value when Int64.compare value (-1L) >= 0 -> Ok ()
    | Some value -> Error (Error.Invalid_limit { field; value })

  let v ~bucket ?(history = 1) ?ttl ?max_bytes ?max_value_size ?(storage = File)
      () =
    match validate_bucket bucket with
    | Error error -> Error error
    | Ok () when history < 1 || history > 64 ->
        Error (Error.Invalid_history history)
    | Ok () -> (
        match ttl with
        | Some value when Mtime.Span.compare value Mtime.Span.zero < 0 ->
            Error Error.Invalid_ttl
        | _ -> (
            match validate_limit "max_bytes" max_bytes with
            | Error error -> Error error
            | Ok () -> (
                match validate_limit "max_value_size" max_value_size with
                | Error error -> Error error
                | Ok () ->
                    Ok
                      {
                        bucket;
                        history;
                        ttl =
                          (match ttl with
                          | Some value
                            when Int.equal
                                   (Mtime.Span.compare value Mtime.Span.zero)
                                   0 ->
                              None
                          | value -> value);
                        max_bytes;
                        max_value_size;
                        storage;
                      })))

  let bucket value = value.bucket
  let history value = value.history
  let ttl value = value.ttl
  let max_bytes value = value.max_bytes
  let max_value_size value = value.max_value_size
  let storage value = value.storage
end

module Status = struct
  type t = {
    bucket : string;
    values : int64;
    bytes : int64;
    first_revision : int64;
    last_revision : int64;
    consumer_count : int;
    history : int64 option;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    max_value_size : int64 option;
    storage : Config.storage;
  }

  let bucket value = value.bucket
  let values value = value.values
  let bytes value = value.bytes
  let first_revision value = value.first_revision
  let last_revision value = value.last_revision
  let consumer_count value = value.consumer_count
  let history value = value.history
  let ttl value = value.ttl
  let max_bytes value = value.max_bytes
  let max_value_size value = value.max_value_size
  let storage value = value.storage
end

type t = {
  jetstream : Jetstream.t;
  stream : Jetstream.Stream.t;
  bucket : string;
}

let bucket value = value.bucket

let map_jetstream_error = function
  | Jetstream.Error.Connection error -> Error.Connection error
  | error -> Error.Jetstream error

let map_config_error error = Error.Invalid_config error
let key_error value reason = Error (Error.Invalid_key { value; reason })

let validate_key value =
  let length = String.length value in
  if Int.equal length 0 then Error Error.Empty_key
  else if
    Char.equal (String.get value 0) '.'
    || Char.equal (String.get value (length - 1)) '.'
  then Error Error.Invalid_key_dots
  else
    let invalid = ref None in
    let previous_dot = ref false in
    for position = 0 to length - 1 do
      match !invalid with
      | Some _ -> ()
      | None ->
          let character = String.get value position in
          let allowed =
            let code = Char.code character in
            (code >= Char.code 'A' && code <= Char.code 'Z')
            || (code >= Char.code 'a' && code <= Char.code 'z')
            || (code >= Char.code '0' && code <= Char.code '9')
            || Char.equal character '-' || Char.equal character '_'
            || Char.equal character '=' || Char.equal character '/'
            || Char.equal character '.'
          in
          if not allowed then
            invalid :=
              Some (Error.Invalid_key_character { position; character })
          else if Char.equal character '.' && !previous_dot then
            invalid := Some Error.Invalid_key_dots
          else previous_dot := Char.equal character '.'
    done;
    match !invalid with None -> Ok () | Some error -> Error error

module Key = struct
  type t = string

  let of_string value =
    match validate_key value with
    | Error error -> Error error
    | Ok () -> Ok value

  let to_string value = value
end

let key_subject value key =
  Nats.Subject.literal ("$KV." ^ value.bucket ^ "." ^ key)

let stream_name bucket = "KV_" ^ bucket

let stream_for_config config jetstream =
  let subject =
    Nats.Subject.Filter.literal ("$KV." ^ Config.bucket config ^ ".>")
  in
  match
    Jetstream.Stream.Config.v
      ~name:(stream_name (Config.bucket config))
      ~subjects:[ subject ]
      ~storage:
        (match Config.storage config with
        | Memory -> Jetstream.Stream.Config.Memory
        | File -> Jetstream.Stream.Config.File)
      ~retention:Jetstream.Stream.Config.Limits
      ~discard:Jetstream.Stream.Config.Old
      ~max_msgs_per_subject:(Int64.of_int (Config.history config))
      ?max_bytes:(Config.max_bytes config) ?max_age:(Config.ttl config)
      ?max_msg_size:(Config.max_value_size config)
      ~allow_rollup:true ~allow_direct:true ()
  with
  | Error error ->
      Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
  | Ok config -> Ok (config, jetstream)

let create jetstream config =
  match stream_for_config config jetstream with
  | Error error -> Error error
  | Ok (stream_config, jetstream) -> (
      match Jetstream.Stream.create jetstream stream_config with
      | Error error -> Error (map_jetstream_error error)
      | Ok stream -> Ok { jetstream; stream; bucket = Config.bucket config })

let bind jetstream ~bucket =
  match Config.v ~bucket () with
  | Error error -> Error (map_config_error error)
  | Ok _ -> (
      match Jetstream.Stream.bind jetstream ~name:(stream_name bucket) with
      | Error error -> Error (map_jetstream_error error)
      | Ok stream -> Ok { jetstream; stream; bucket })

let open_ jetstream ~bucket =
  match bind jetstream ~bucket with
  | Error error -> Error error
  | Ok value -> (
      match Jetstream.Stream.info value.stream with
      | Ok _ -> Ok value
      | Error error -> Error (map_jetstream_error error))

let delete_bucket value =
  match Jetstream.Stream.delete value.stream with
  | Ok () -> Ok ()
  | Error error -> Error (map_jetstream_error error)

let status value =
  match Jetstream.Stream.info value.stream with
  | Error error -> Error (map_jetstream_error error)
  | Ok info ->
      let config = Jetstream.Stream.Info.config info in
      let storage =
        match Jetstream.Stream.Config.storage config with
        | Jetstream.Stream.Config.Memory -> Config.Memory
        | Jetstream.Stream.Config.File -> Config.File
      in
      Ok
        {
          Status.bucket = value.bucket;
          values = Jetstream.Stream.Info.messages info;
          bytes = Jetstream.Stream.Info.bytes info;
          first_revision = Jetstream.Stream.Info.first_sequence info;
          last_revision = Jetstream.Stream.Info.last_sequence info;
          consumer_count = Jetstream.Stream.Info.consumer_count info;
          history = Jetstream.Stream.Config.max_msgs_per_subject config;
          ttl = Jetstream.Stream.Config.max_age config;
          max_bytes = Jetstream.Stream.Config.max_bytes config;
          max_value_size = Jetstream.Stream.Config.max_msg_size config;
          storage;
        }

let operation_of_headers headers =
  match Nats.Header.find "KV-Operation" headers with
  | None -> Ok Entry.Put
  | Some "DEL" -> Ok Entry.Delete
  | Some "PURGE" -> Ok Entry.Purge
  | Some value -> Error (Error.Invalid_operation value)

let entry_of_message value ~key message =
  let expected_subject = key_subject value key in
  if
    not
      (Nats.Subject.equal expected_subject
         (Jetstream.Stream.Message.subject message))
  then Error Error.Key_not_found
  else
    match operation_of_headers (Jetstream.Stream.Message.headers message) with
    | Error error -> Error error
    | Ok operation ->
        Ok
          {
            Entry.bucket = value.bucket;
            key;
            value = Jetstream.Stream.Message.payload message;
            revision = Jetstream.Stream.Message.sequence message;
            timestamp = Jetstream.Stream.Message.timestamp message;
            operation;
          }

let read_last value key =
  match
    Jetstream.Stream.get_last value.stream ~subject:(key_subject value key)
  with
  | Error Jetstream.Error.Message_not_found -> Error Error.Key_not_found
  | Error error -> Error (map_jetstream_error error)
  | Ok message -> entry_of_message value ~key message

let read_revision value key revision =
  match Jetstream.Stream.get value.stream ~sequence:revision with
  | Error Jetstream.Error.Message_not_found -> Error Error.Key_not_found
  | Error error -> Error (map_jetstream_error error)
  | Ok message -> entry_of_message value ~key message

let visible_entry entry =
  match Entry.operation entry with
  | Entry.Put -> Ok entry
  | Entry.Delete | Entry.Purge -> Error (Error.Key_deleted entry)

let get value ~key =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () -> (
      match read_last value key with
      | Ok entry -> visible_entry entry
      | Error error -> Error error)

let get_revision value ~key ~revision =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () when Int64.compare revision 0L < 0 ->
      Error (Error.Invalid_revision revision)
  | Ok () -> (
      match read_revision value key revision with
      | Ok entry -> visible_entry entry
      | Error error -> Error error)

let header name value headers =
  match Nats.Header.add ~name ~value headers with
  | Ok headers -> Ok headers
  | Error error -> Error (Error.Invalid_headers error)

let expected_headers expected headers =
  match expected with
  | None -> Ok headers
  | Some revision when Int64.compare revision 0L < 0 ->
      Error (Error.Invalid_revision revision)
  | Some revision ->
      header "Nats-Expected-Last-Subject-Sequence" (Int64.to_string revision)
        headers

let publish value ~key ?expected ?operation payload =
  let headers = expected_headers expected Nats.Header.empty in
  match headers with
  | Error error -> Error error
  | Ok headers -> (
      let headers =
        match operation with
        | None -> Ok headers
        | Some Entry.Delete -> header "KV-Operation" "DEL" headers
        | Some Entry.Purge -> (
            match header "KV-Operation" "PURGE" headers with
            | Error error -> Error error
            | Ok headers -> header "Nats-Rollup" "sub" headers)
        | Some Entry.Put -> Ok headers
      in
      match headers with
      | Error error -> Error error
      | Ok headers -> (
          match
            Jetstream.publish value.jetstream ~headers (key_subject value key)
              payload
          with
          | Ok ack -> Ok (Jetstream.Publish_ack.sequence ack)
          | Error error -> Error (map_jetstream_error error)))

let is_wrong_last_sequence = function
  | Error.Jetstream (Jetstream.Error.Api { err_code = Some 10071; _ })
  | Error.Jetstream (Jetstream.Error.Api { err_code = Some 10164; _ }) ->
      true
  | _ -> false

let map_cas_error ~expected error =
  if is_wrong_last_sequence error then
    Error (Error.Revision_mismatch { expected })
  else Error error

let put value ~key payload =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () -> publish value ~key payload

let update value ~key ~revision payload =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () when Int64.compare revision 0L < 0 ->
      Error (Error.Invalid_revision revision)
  | Ok () -> (
      match publish value ~key ~expected:revision payload with
      | Ok sequence -> Ok sequence
      | Error error -> map_cas_error ~expected:revision error)

let create_key value ~key payload =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () -> (
      match publish value ~key ~expected:0L payload with
      | Ok sequence -> Ok sequence
      | Error error when is_wrong_last_sequence error -> (
          match read_last value key with
          | Error (Error.Key_deleted entry) ->
              update value ~key ~revision:(Entry.revision entry) payload
          | Error Error.Key_not_found -> Error Error.Key_exists
          | Error other -> Error other
          | Ok _ -> Error Error.Key_exists)
      | Error error -> Error error)

let delete ?expected_revision value ~key =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () -> (
      match
        publish value ~key ?expected:expected_revision ~operation:Entry.Delete
          ""
      with
      | Ok sequence -> Ok sequence
      | Error error -> (
          match expected_revision with
          | Some expected -> map_cas_error ~expected error
          | None -> Error error))

let purge ?expected_revision value ~key =
  match validate_key key with
  | Error reason -> key_error key reason
  | Ok () -> (
      match
        publish value ~key ?expected:expected_revision ~operation:Entry.Purge ""
      with
      | Ok sequence -> Ok sequence
      | Error error -> (
          match expected_revision with
          | Some expected -> map_cas_error ~expected error
          | None -> Error error))
