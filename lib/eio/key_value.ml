module Core_error = Error

module Config = struct
  type storage = Memory | File

  module Placement = Jetstream.Stream.Config.Placement
  module Source = Jetstream.Stream.Config.Source
  module Republish = Jetstream.Stream.Config.Republish

  type compression = Jetstream.Stream.Config.compression =
    | Uncompressed
    | S2

  type t = {
    bucket : string;
    description : string option;
    history : int;
    ttl : Mtime.Span.t option;
    limit_marker_ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    max_value_size : int64 option;
    storage : storage;
    replicas : int;
    placement : Placement.t option;
    mirror : Source.t option;
    sources : Source.t list;
    republish : Republish.t option;
    compression : compression;
    metadata : (string * string) list;
  }

  type error =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_history of int
    | Invalid_ttl
    | Invalid_limit_marker_ttl
    | Invalid_replicas of int
    | Mirror_and_sources
    | Invalid_limit of { field : string; value : int64 }

  let allowed_bucket_character character =
    let code = Char.code character in
    (code >= Char.code 'A' && code <= Char.code 'Z')
    || (code >= Char.code 'a' && code <= Char.code 'z')
    || (code >= Char.code '0' && code <= Char.code '9')
    || Char.equal character '_' || Char.equal character '-'

  let validate_bucket bucket =
    let length = String.length bucket in
    if Int.equal length 0 then Error Empty_bucket
    else
      let invalid = ref None in
      for position = 0 to length - 1 do
        match !invalid with
        | Some _ -> ()
        | None ->
            let character = String.get bucket position in
            if not (allowed_bucket_character character) then
              invalid := Some (Invalid_bucket_character { position; character })
      done;
      match !invalid with None -> Ok () | Some error -> Error error

  let validate_limit field = function
    | None -> Ok ()
    | Some value when Int64.compare value (-1L) >= 0 -> Ok ()
    | Some value -> Error (Invalid_limit { field; value })

  let normalize_limit = function Some -1L -> None | value -> value

  let v ~bucket ?description ?(history = 1) ?ttl ?limit_marker_ttl ?max_bytes
      ?max_value_size ?(storage = File) ?(replicas = 1) ?placement ?mirror
      ?(sources = []) ?republish ?(compression = Uncompressed)
      ?(metadata = []) () =
    match validate_bucket bucket with
    | Error error -> Error error
    | Ok () when Int.compare history 1 < 0 || Int.compare history 64 > 0 ->
        Error (Invalid_history history)
    | Ok () when Int.compare replicas 1 < 0 || Int.compare replicas 5 > 0 ->
        Error (Invalid_replicas replicas)
    | Ok () when Option.is_some mirror && List.length sources > 0 ->
        Error Mirror_and_sources
    | Ok () -> (
        match ttl with
        | Some value when Mtime.Span.compare value Mtime.Span.zero < 0 ->
            Error Invalid_ttl
        | _ -> (
            match limit_marker_ttl with
            | Some value when Mtime.Span.compare value Mtime.Span.zero < 0 ->
                Error Invalid_limit_marker_ttl
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
                            limit_marker_ttl =
                              (match limit_marker_ttl with
                              | Some value
                                when Int.equal
                                       (Mtime.Span.compare value Mtime.Span.zero)
                                       0 ->
                                  None
                              | value -> value);
                            max_bytes = normalize_limit max_bytes;
                            max_value_size = normalize_limit max_value_size;
                            storage;
                            description;
                            replicas;
                            placement;
                            mirror;
                            sources;
                            republish;
                            compression;
                            metadata;
                          }))))

  let bucket value = value.bucket
  let description value = value.description
  let history value = value.history
  let ttl value = value.ttl
  let limit_marker_ttl value = value.limit_marker_ttl
  let max_bytes value = value.max_bytes
  let max_value_size value = value.max_value_size
  let storage value = value.storage
  let replicas value = value.replicas
  let placement value = value.placement
  let mirror value = value.mirror
  let sources value = value.sources
  let republish value = value.republish
  let compression value = value.compression
  let metadata value = value.metadata
end

module Key = struct
  type t = string

  type error =
    | Empty_key
    | Invalid_key_character of { position : int; character : char }
    | Invalid_key_dots

  let allowed_key_character character =
    let code = Char.code character in
    (code >= Char.code 'A' && code <= Char.code 'Z')
    || (code >= Char.code 'a' && code <= Char.code 'z')
    || (code >= Char.code '0' && code <= Char.code '9')
    || Char.equal character '-' || Char.equal character '_'
    || Char.equal character '=' || Char.equal character '/'
    || Char.equal character '.'

  let validate value =
    let length = String.length value in
    if Int.equal length 0 then Error Empty_key
    else if
      Char.equal (String.get value 0) '.'
      || Char.equal (String.get value (length - 1)) '.'
    then Error Invalid_key_dots
    else
      let invalid = ref None in
      let previous_dot = ref false in
      for position = 0 to length - 1 do
        match !invalid with
        | Some _ -> ()
        | None ->
            let character = String.get value position in
            if not (allowed_key_character character) then
              invalid := Some (Invalid_key_character { position; character })
            else if Char.equal character '.' && !previous_dot then
              invalid := Some Invalid_key_dots
            else previous_dot := Char.equal character '.'
      done;
      match !invalid with None -> Ok () | Some error -> Error error

  let of_string value =
    match validate value with Error error -> Error error | Ok () -> Ok value

  let to_string value = value
end

module Entry = struct
  type operation = Put | Delete | Purge

  type t = {
    bucket : string;
    key : Key.t;
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
  type config = Config.error
  type key = Key.error

  type t =
    | Connection of Connection.error
    | Jetstream of Jetstream.Error.t
    | Invalid_config of config
    | Invalid_key of { value : string; reason : key }
    | Invalid_revision of int64
    | Invalid_key_ttl
    | Invalid_marker_ttl
    | Invalid_purge_age
    | Invalid_watch_filters
    | Invalid_headers of Nats.Header.error
    | Invalid_operation of string
    | Invalid_filter of { value : string; reason : Nats.Subject.error }
    | Invalid_message_subject of string
    | Invalid_timestamp of int64
    | Invalid_timestamp_text of string
    | Key_not_found
    | Key_deleted of Entry.t
    | Key_exists
    | Revision_mismatch of { expected : int64 }
    | Closed

  let pp_config ppf = function
    | Config.Empty_bucket -> Format.pp_print_string ppf "bucket name is empty"
    | Config.Invalid_bucket_character { position; character } ->
        Format.fprintf ppf "invalid bucket-name character %C at position %d"
          character position
    | Config.Invalid_history value ->
        Format.fprintf ppf "key-value history must be between 1 and 64, got %d"
          value
    | Config.Invalid_ttl ->
        Format.pp_print_string ppf "key-value TTL must not be negative"
    | Config.Invalid_limit_marker_ttl ->
        Format.pp_print_string ppf
          "key-value limit marker TTL must not be negative"
    | Config.Invalid_replicas value ->
        Format.fprintf ppf "key-value replicas must be between 1 and 5, got %d"
          value
    | Config.Mirror_and_sources ->
        Format.pp_print_string ppf
          "a key-value bucket cannot configure both a mirror and sources"
    | Config.Invalid_limit { field; value } ->
        Format.fprintf ppf "invalid key-value %s limit %Ld" field value

  let pp_key ppf = function
    | Key.Empty_key -> Format.pp_print_string ppf "key is empty"
    | Key.Invalid_key_character { position; character } ->
        Format.fprintf ppf "invalid key character %C at position %d" character
          position
    | Key.Invalid_key_dots ->
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
    | Invalid_key_ttl ->
        Format.pp_print_string ppf "key-value key TTL must be positive"
    | Invalid_marker_ttl ->
        Format.pp_print_string ppf "key-value purge marker TTL must be positive"
    | Invalid_purge_age ->
        Format.pp_print_string ppf
          "key-value purge age must be positive or explicitly remove all \
           markers"
    | Invalid_watch_filters ->
        Format.pp_print_string ppf
          "key-value watch accepts either key or keys filters, not both"
    | Invalid_headers error ->
        Format.fprintf ppf "invalid key-value headers: %a" Nats.Header.pp_error
          error
    | Invalid_operation value ->
        Format.fprintf ppf "invalid key-value operation %S" value
    | Invalid_filter { value; reason } ->
        Format.fprintf ppf "invalid key-value filter %S: %a" value
          Nats.Subject.pp_error reason
    | Invalid_message_subject value ->
        Format.fprintf ppf "unexpected key-value message subject %S" value
    | Invalid_timestamp value ->
        Format.fprintf ppf "invalid JetStream message timestamp %Ld" value
    | Invalid_timestamp_text value ->
        Format.fprintf ppf "invalid JetStream message timestamp %S" value
    | Key_not_found -> Format.pp_print_string ppf "key was not found"
    | Key_deleted entry ->
        Format.fprintf ppf "key %S was deleted at revision %Ld"
          (Key.to_string (Entry.key entry))
          (Entry.revision entry)
    | Key_exists -> Format.pp_print_string ppf "key already exists"
    | Revision_mismatch { expected } ->
        Format.fprintf ppf "key revision did not match expected revision %Ld"
          expected
    | Closed -> Format.pp_print_string ppf "key-value watch is closed"
end

module Status = struct
  type t = {
    bucket : string;
    description : string option;
    values : int64;
    bytes : int64;
    first_revision : int64;
    last_revision : int64;
    history : int64 option;
    ttl : Mtime.Span.t option;
    limit_marker_ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    max_value_size : int64 option;
    storage : Config.storage;
    replicas : int;
    placement : Config.Placement.t option;
    mirror : Config.Source.t option;
    sources : Config.Source.t list;
    republish : Config.Republish.t option;
    compression : Config.compression;
    metadata : (string * string) list;
  }

  let bucket value = value.bucket
  let description value = value.description
  let values value = value.values
  let bytes value = value.bytes
  let first_revision value = value.first_revision
  let last_revision value = value.last_revision
  let history value = value.history
  let ttl value = value.ttl
  let limit_marker_ttl value = value.limit_marker_ttl
  let max_bytes value = value.max_bytes
  let max_value_size value = value.max_value_size
  let storage value = value.storage
  let replicas value = value.replicas
  let placement value = value.placement
  let mirror value = value.mirror
  let sources value = value.sources
  let republish value = value.republish
  let compression value = value.compression
  let metadata value = value.metadata
end

type t = {
  jetstream : Jetstream.t;
  stream : Jetstream.Stream.t;
  bucket : string;
}

type bucket = t
type purge_age = Default | Any | Older_than of Mtime.Span.t

let bucket value = value.bucket

let map_jetstream_error = function
  | Jetstream.Error.Connection error -> Error.Connection error
  | error -> Error.Jetstream error

let map_config_error error = Error.Invalid_config error
let stream_name bucket = "KV_" ^ bucket

let key_subject value key =
  Nats.Subject.literal ("$KV." ^ value.bucket ^ "." ^ Key.to_string key)

let stream_for_config config jetstream =
  let subjects =
    match Config.mirror config with
    | Some _ -> []
    | None ->
        [ Nats.Subject.Filter.literal ("$KV." ^ Config.bucket config ^ ".>") ]
  in
  match
    Jetstream.Stream.Config.v
      ~name:(stream_name (Config.bucket config))
      ~subjects
      ?description:(Config.description config)
      ~storage:
        (match Config.storage config with
        | Config.Memory -> Jetstream.Stream.Config.Memory
        | Config.File -> Jetstream.Stream.Config.File)
      ~retention:Jetstream.Stream.Config.Limits
      ~discard:Jetstream.Stream.Config.New
      ~replicas:(Config.replicas config)
      ?placement:(Config.placement config)
      ?mirror:(Config.mirror config)
      ~sources:(Config.sources config)
      ?republish:(Config.republish config)
      ~compression:(Config.compression config)
      ~metadata:(Config.metadata config)
      ~max_msgs_per_subject:(Int64.of_int (Config.history config))
      ?max_bytes:(Config.max_bytes config) ?max_age:(Config.ttl config)
      ?max_msg_size:(Config.max_value_size config)
      ~allow_msg_ttl:(Option.is_some (Config.limit_marker_ttl config))
      ?subject_delete_marker_ttl:(Config.limit_marker_ttl config)
      ~allow_rollup:true ~allow_direct:true ~deny_delete:true ()
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

let status_of_info (value : t) info =
  let config = Jetstream.Stream.Info.config info in
  let storage =
    match Jetstream.Stream.Config.storage config with
    | Jetstream.Stream.Config.Memory -> Config.Memory
    | Jetstream.Stream.Config.File -> Config.File
  in
  {
    Status.bucket = value.bucket;
    description = Jetstream.Stream.Config.description config;
    values = Jetstream.Stream.Info.messages info;
    bytes = Jetstream.Stream.Info.bytes info;
    first_revision = Jetstream.Stream.Info.first_sequence info;
    last_revision = Jetstream.Stream.Info.last_sequence info;
    history = Jetstream.Stream.Config.max_msgs_per_subject config;
    ttl = Jetstream.Stream.Config.max_age config;
    limit_marker_ttl = Jetstream.Stream.Config.subject_delete_marker_ttl config;
    max_bytes = Jetstream.Stream.Config.max_bytes config;
    max_value_size = Jetstream.Stream.Config.max_msg_size config;
    storage;
    replicas = Jetstream.Stream.Config.replicas config;
    placement = Jetstream.Stream.Config.placement config;
    mirror = Jetstream.Stream.Config.mirror config;
    sources = Jetstream.Stream.Config.sources config;
    republish = Jetstream.Stream.Config.republish config;
    compression = Jetstream.Stream.Config.compression config;
    metadata = Jetstream.Stream.Config.metadata config;
  }

let status value =
  match Jetstream.Stream.info value.stream with
  | Error error -> Error (map_jetstream_error error)
  | Ok info -> Ok (status_of_info value info)

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

let get value key =
  match read_last value key with
  | Ok entry -> visible_entry entry
  | Error error -> Error error

let get_revision value key ~revision =
  if Int64.compare revision 0L <= 0 then Error (Error.Invalid_revision revision)
  else
    match read_revision value key revision with
    | Ok entry -> visible_entry entry
    | Error error -> Error error

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

let ttl_header_value ttl = Int64.to_string (Mtime.Span.to_uint64_ns ttl) ^ "ns"

let ttl_headers ttl headers =
  match ttl with
  | None -> Ok headers
  | Some ttl when Mtime.Span.compare ttl Mtime.Span.zero <= 0 ->
      Error Error.Invalid_key_ttl
  | Some ttl -> header "Nats-TTL" (ttl_header_value ttl) headers

let publish value ~key ?expected ?operation ?ttl payload =
  match expected_headers expected Nats.Header.empty with
  | Error error -> Error error
  | Ok headers -> (
      let headers =
        match operation with
        | None | Some Entry.Put -> Ok headers
        | Some Entry.Delete -> header "KV-Operation" "DEL" headers
        | Some Entry.Purge -> (
            match header "KV-Operation" "PURGE" headers with
            | Error error -> Error error
            | Ok headers -> header "Nats-Rollup" "sub" headers)
      in
      match headers with
      | Error error -> Error error
      | Ok headers -> (
          match ttl_headers ttl headers with
          | Error error -> Error error
          | Ok headers -> (
              match
                Jetstream.publish value.jetstream ~headers
                  (key_subject value key) payload
              with
              | Ok ack -> Ok (Jetstream.Publish_ack.sequence ack)
              | Error error -> Error (map_jetstream_error error))))

let is_wrong_last_sequence = function
  | Error.Jetstream (Jetstream.Error.Api { err_code = Some 10071; _ })
  | Error.Jetstream (Jetstream.Error.Api { err_code = Some 10164; _ }) ->
      true
  | _ -> false

let map_cas_error ~expected error =
  if is_wrong_last_sequence error then
    Error (Error.Revision_mismatch { expected })
  else Error error

let put value key payload = publish value ~key payload

let update_with_ttl ?ttl value key ~revision payload =
  if Int64.compare revision 0L <= 0 then Error (Error.Invalid_revision revision)
  else
    match publish value ~key ~expected:revision ?ttl payload with
    | Ok sequence -> Ok sequence
    | Error error -> map_cas_error ~expected:revision error

let update value key ~revision payload =
  update_with_ttl value key ~revision payload

let create_key ?ttl value key payload =
  match publish value ~key ~expected:0L ?ttl payload with
  | Ok sequence -> Ok sequence
  | Error error when is_wrong_last_sequence error -> (
      match read_last value key with
      | Ok entry -> (
          match Entry.operation entry with
          | Entry.Delete | Entry.Purge ->
              update_with_ttl ?ttl value key ~revision:(Entry.revision entry)
                payload
          | Entry.Put -> Error Error.Key_exists)
      | Error Error.Key_not_found -> Error error
      | Error other -> Error other)
  | Error error -> Error error

let delete ?expected_revision value key =
  match
    publish value ~key ?expected:expected_revision ~operation:Entry.Delete ""
  with
  | Ok sequence -> Ok sequence
  | Error error -> (
      match expected_revision with
      | Some expected -> map_cas_error ~expected error
      | None -> Error error)

let purge ?expected_revision ?marker_ttl value key =
  match marker_ttl with
  | Some ttl when Mtime.Span.compare ttl Mtime.Span.zero <= 0 ->
      Error Error.Invalid_marker_ttl
  | _ -> (
      match
        publish value ~key ?expected:expected_revision ~operation:Entry.Purge
          ?ttl:marker_ttl ""
      with
      | Ok sequence -> Ok sequence
      | Error error -> (
          match expected_revision with
          | Some expected -> map_cas_error ~expected error
          | None -> Error error))

module Key_set = Set.Make (String)

let make_filter value pattern =
  let pattern = Option.value pattern ~default:">" in
  let subject = "$KV." ^ value.bucket ^ "." ^ pattern in
  match Nats.Subject.Filter.of_string subject with
  | Error reason -> Error (Error.Invalid_filter { value = pattern; reason })
  | Ok filter -> (
      let tokens = String.split_on_char '.' pattern in
      let invalid = ref None in
      List.iter
        (fun token ->
          match !invalid with
          | Some _ -> ()
          | None when String.equal token "*" || String.equal token ">" -> ()
          | None -> (
              match Key.of_string token with
              | Ok _ -> ()
              | Error reason ->
                  invalid := Some (Error.Invalid_key { value = token; reason })))
        tokens;
      match !invalid with None -> Ok filter | Some error -> Error error)

let make_filters value patterns =
  let patterns = match patterns with [] -> [ ">" ] | patterns -> patterns in
  let result = ref [] in
  let error = ref None in
  List.iter
    (fun pattern ->
      match (!error, make_filter value (Some pattern)) with
      | Some _, _ -> ()
      | None, Ok filter -> result := filter :: !result
      | None, Error value -> error := Some value)
    patterns;
  match !error with Some error -> Error error | None -> Ok (List.rev !result)

let timestamp_of_nanoseconds value =
  if Int64.compare value 0L < 0 then Error (Error.Invalid_timestamp value)
  else
    let billion = 1_000_000_000L in
    let day_seconds = 86_400L in
    let seconds = Int64.div value billion in
    let fraction = Int64.rem value billion in
    let days = Int64.div seconds day_seconds in
    let seconds_in_day = Int64.rem seconds day_seconds in
    let z = Int64.add days 719_468L in
    let era = Int64.div z 146_097L in
    let doe = Int64.sub z (Int64.mul era 146_097L) in
    let yoe =
      Int64.div
        (Int64.sub
           (Int64.add
              (Int64.sub doe (Int64.div doe 1_460L))
              (Int64.div doe 36_524L))
           (Int64.div doe 146_096L))
        365L
    in
    let year = Int64.add yoe (Int64.mul era 400L) in
    let doy =
      Int64.sub doe
        (Int64.sub
           (Int64.add (Int64.mul 365L yoe) (Int64.div yoe 4L))
           (Int64.div yoe 100L))
    in
    let month_part = Int64.div (Int64.add (Int64.mul 5L doy) 2L) 153L in
    let day =
      Int64.add
        (Int64.sub doy
           (Int64.div (Int64.add (Int64.mul 153L month_part) 2L) 5L))
        1L
    in
    let month =
      Int64.add month_part
        (if Int64.compare month_part 10L < 0 then 3L else -9L)
    in
    let year =
      Int64.add year (if Int64.compare month 2L <= 0 then 1L else 0L)
    in
    let hour = Int64.div seconds_in_day 3_600L in
    let minute = Int64.div (Int64.rem seconds_in_day 3_600L) 60L in
    let second = Int64.rem seconds_in_day 60L in
    Ok
      (Format.asprintf "%04Ld-%02Ld-%02LdT%02Ld:%02Ld:%02Ld.%09LdZ" year month
         day hour minute second fraction)

let key_of_delivery value message =
  let subject = Nats.Subject.to_string (Jetstream.Msg.subject message) in
  let prefix = "$KV." ^ value.bucket ^ "." in
  let prefix_length = String.length prefix in
  if
    String.length subject <= prefix_length
    || not (String.equal prefix (String.sub subject 0 prefix_length))
  then Error (Error.Invalid_message_subject subject)
  else
    let key =
      String.sub subject prefix_length (String.length subject - prefix_length)
    in
    match Key.of_string key with
    | Ok key -> Ok key
    | Error reason -> Error (Error.Invalid_key { value = key; reason })

let entry_of_delivery value message =
  match key_of_delivery value message with
  | Error error -> Error error
  | Ok key -> (
      match operation_of_headers (Jetstream.Msg.headers message) with
      | Error error -> Error error
      | Ok operation -> (
          match timestamp_of_nanoseconds (Jetstream.Msg.timestamp message) with
          | Error error -> Error error
          | Ok timestamp ->
              Ok
                {
                  Entry.bucket = value.bucket;
                  key;
                  value = Jetstream.Msg.payload message;
                  revision = Jetstream.Msg.stream_sequence message;
                  timestamp;
                  operation;
                }))

let one_shot_config ~filter ~deliver_policy ~headers_only =
  match
    Jetstream.Consumer.Config.v ~deliver_policy
      ~ack_policy:Jetstream.Consumer.Config.No_ack ~filter_subject:filter
      ~headers_only
      ~inactive_threshold:Mtime.Span.(5 * min)
      ~mem_storage:true ()
  with
  | Ok config -> Ok config
  | Error error ->
      Error (Error.Jetstream (Jetstream.Error.Invalid_config error))

let with_temporary_consumer value config f =
  match Jetstream.Consumer.create value.stream config with
  | Error error -> Error (map_jetstream_error error)
  | Ok consumer -> (
      let cleanup_error = ref None in
      let result =
        Fun.protect
          ~finally:(fun () ->
            cleanup_error :=
              Some
                (Eio.Cancel.protect (fun () ->
                     Jetstream.Consumer.delete consumer)))
          (fun () -> f consumer)
      in
      match (result, !cleanup_error) with
      | Ok result, Some (Ok ()) -> Ok result
      | Ok _, Some (Error error) -> Error (map_jetstream_error error)
      | Error error, _ -> Error error
      | Ok _, None -> assert false)

let one_shot_batch = 256
let one_shot_expires = Mtime.Span.(1 * s)
let max_empty_fetches = 3

let drain_messages consumer =
  let acc = ref [] in
  let empty_fetches = ref 0 in
  let result = ref None in
  while Option.is_none !result do
    match
      Jetstream.Consumer.fetch ~expires:one_shot_expires consumer
        ~batch:one_shot_batch
    with
    | Error error -> result := Some (Error (map_jetstream_error error))
    | Ok messages -> (
        acc := List.rev_append messages !acc;
        let last_pending =
          List.fold_left
            (fun _ message -> Some (Jetstream.Msg.num_pending message))
            None messages
        in
        if
          match last_pending with
          | Some pending -> Int64.equal pending 0L
          | None -> false
        then result := Some (Ok (List.rev !acc))
        else
          match Jetstream.Consumer.info consumer with
          | Error error -> result := Some (Error (map_jetstream_error error))
          | Ok info
            when Int64.equal (Jetstream.Consumer.Info.num_pending info) 0L ->
              result := Some (Ok (List.rev !acc))
          | Ok _ ->
              (match messages with
              | [] -> incr empty_fetches
              | _ -> empty_fetches := 0);
              if Int.compare !empty_fetches max_empty_fetches >= 0 then
                result := Some (Error (Error.Connection Core_error.Timeout)))
  done;
  match !result with Some result -> result | None -> assert false

let collect_messages value config =
  with_temporary_consumer value config (fun consumer ->
      match Jetstream.Consumer.info consumer with
      | Error error -> Error (map_jetstream_error error)
      | Ok info when Int64.equal (Jetstream.Consumer.Info.num_pending info) 0L
        ->
          Ok []
      | Ok _ -> drain_messages consumer)

let keys ?filter value =
  match make_filter value filter with
  | Error error -> Error error
  | Ok filter -> (
      match
        one_shot_config ~filter
          ~deliver_policy:Jetstream.Consumer.Config.Last_per_subject
          ~headers_only:true
      with
      | Error error -> Error error
      | Ok config -> (
          match collect_messages value config with
          | Error error -> Error error
          | Ok messages -> (
              let result =
                List.fold_left
                  (fun result message ->
                    match result with
                    | Error _ -> result
                    | Ok (seen, keys) -> (
                        match key_of_delivery value message with
                        | Error error -> Error error
                        | Ok key -> (
                            match
                              operation_of_headers
                                (Jetstream.Msg.headers message)
                            with
                            | Error error -> Error error
                            | Ok Entry.Put ->
                                let key_string = Key.to_string key in
                                if Key_set.mem key_string seen then
                                  Ok (seen, keys)
                                else
                                  Ok (Key_set.add key_string seen, key :: keys)
                            | Ok (Entry.Delete | Entry.Purge) -> Ok (seen, keys)
                            )))
                  (Ok (Key_set.empty, []))
                  messages
              in
              match result with
              | Error error -> Error error
              | Ok (_, keys) -> Ok (List.rev keys))))

let history value key =
  let filter =
    Nats.Subject.Filter.literal (Nats.Subject.to_string (key_subject value key))
  in
  match
    one_shot_config ~filter ~deliver_policy:Jetstream.Consumer.Config.All
      ~headers_only:false
  with
  | Error error -> Error error
  | Ok config -> (
      match collect_messages value config with
      | Error error -> Error error
      | Ok messages -> (
          let result =
            List.fold_left
              (fun result message ->
                match result with
                | Error _ -> result
                | Ok entries -> (
                    match entry_of_delivery value message with
                    | Error error -> Error error
                    | Ok entry -> Ok (entry :: entries)))
              (Ok []) messages
          in
          match result with
          | Error error -> Error error
          | Ok entries -> Ok (List.rev entries)))

let purge_deletes ?older_than value =
  let age = Option.value ~default:Default older_than in
  let cutoff =
    match age with
    | Any -> Ok None
    | Default | Older_than _ -> (
        let span =
          match age with
          | Default -> Mtime.Span.(30 * min)
          | Older_than span -> span
          | Any -> Mtime.Span.zero
        in
        if Mtime.Span.compare span Mtime.Span.zero <= 0 then
          Error Error.Invalid_purge_age
        else
          match
            Ptime.Span.of_float_s (Mtime.Span.to_float_ns span /. 1_000_000_000.)
          with
          | None -> Error Error.Invalid_purge_age
          | Some span -> (
              match Ptime.sub_span (Ptime_clock.now ()) span with
              | None -> Error Error.Invalid_purge_age
              | Some limit -> Ok (Some limit)))
  in
  let marker_keep entry =
    match cutoff with
    | Error error -> Error error
    | Ok None -> Ok None
    | Ok (Some limit) -> (
        match Ptime.of_rfc3339 ~strict:true (Entry.timestamp entry) with
        | Error _ ->
            Error (Error.Invalid_timestamp_text (Entry.timestamp entry))
        | Ok (timestamp, _, _) when Ptime.compare timestamp limit > 0 ->
            Ok (Some 1L)
        | Ok _ -> Ok None)
  in
  let purge_entry entry =
    match Entry.operation entry with
    | Entry.Put -> Ok ()
    | Entry.Delete | Entry.Purge -> (
        match marker_keep entry with
        | Error error -> Error error
        | Ok keep -> (
            let subject =
              Nats.Subject.Filter.literal
                (Nats.Subject.to_string (key_subject value (Entry.key entry)))
            in
            match Jetstream.Stream.purge ?keep ~subject value.stream with
            | Ok _ -> Ok ()
            | Error error -> Error (map_jetstream_error error)))
  in
  let filter = Nats.Subject.Filter.literal ("$KV." ^ value.bucket ^ ".>") in
  let config =
    one_shot_config ~filter
      ~deliver_policy:Jetstream.Consumer.Config.Last_per_subject
      ~headers_only:false
  in
  match config with
  | Error error -> Error error
  | Ok config -> (
      match collect_messages value config with
      | Error error -> Error error
      | Ok messages ->
          let result = ref (Ok ()) in
          List.iter
            (fun message ->
              match !result with
              | Error _ -> ()
              | Ok () -> (
                  match entry_of_delivery value message with
                  | Error error -> result := Error error
                  | Ok entry -> result := purge_entry entry))
            messages;
          !result)

module Watch = struct
  type delivery = New | Last_per_subject | All
  type event = Initial_done | Entry of Entry.t
  type initial = Marker | Retained | Live

  type t = {
    value : bucket;
    push : Jetstream.Consumer.Push.t;
    connection : Connection.t;
    ignore_deletes : bool;
    mutable initial : initial;
    initial_pending : int64 option;
    mutable initial_received : int64;
  }

  let map_error = function
    | Jetstream.Error.Connection error -> Error.Connection error
    | Jetstream.Error.Push_closed -> Error.Closed
    | error -> Error.Jetstream error

  let watch_config value ~filters ~delivery ~meta_only ~resume_from_revision =
    let connection = Jetstream.connection value.jetstream in
    let deliver_subject = Connection.fresh_inbox connection in
    let deliver_policy =
      match resume_from_revision with
      | Some revision -> Jetstream.Consumer.Config.By_start_sequence revision
      | None -> (
          match delivery with
          | New -> Jetstream.Consumer.Config.New
          | Last_per_subject -> Jetstream.Consumer.Config.Last_per_subject
          | All -> Jetstream.Consumer.Config.All)
    in
    let config =
      match filters with
      | [ filter ] ->
          Jetstream.Consumer.Config.v ~deliver_subject ~deliver_policy
            ~ack_policy:Jetstream.Consumer.Config.No_ack ~filter_subject:filter
            ~idle_heartbeat:Mtime.Span.(5 * s)
            ~flow_control:true ~headers_only:meta_only
            ~inactive_threshold:Mtime.Span.(5 * min)
            ~mem_storage:true ()
      | filters ->
          Jetstream.Consumer.Config.v ~deliver_subject ~deliver_policy
            ~ack_policy:Jetstream.Consumer.Config.No_ack
            ~filter_subjects:filters
            ~idle_heartbeat:Mtime.Span.(5 * s)
            ~flow_control:true ~headers_only:meta_only
            ~inactive_threshold:Mtime.Span.(5 * min)
            ~mem_storage:true ()
    in
    match config with
    | Ok config -> Ok config
    | Error error ->
        Error (Error.Jetstream (Jetstream.Error.Invalid_config error))

  let v ~sw ?key ?keys ?(delivery = Last_per_subject) ?(ignore_deletes = false)
      ?(meta_only = false) ?resume_from_revision value =
    match (key, keys) with
    | Some _, Some _ -> Error Error.Invalid_watch_filters
    | _ -> (
        let patterns =
          match (key, keys) with
          | Some key, None -> [ key ]
          | None, Some keys -> keys
          | None, None -> []
          | Some _, Some _ -> []
        in
        let resume_result =
          match resume_from_revision with
          | None -> Ok None
          | Some revision when Int64.compare revision 0L > 0 ->
              Ok (Some revision)
          | Some revision -> Error (Error.Invalid_revision revision)
        in
        match (make_filters value patterns, resume_result) with
        | Error error, _ -> Error error
        | _, Error error -> Error error
        | Ok filters, Ok resume_from_revision -> (
            match
              watch_config value ~filters ~delivery ~meta_only
                ~resume_from_revision
            with
            | Error error -> Error error
            | Ok config -> (
                match
                  Jetstream.Consumer.Push.create ~sw value.stream config
                with
                | Error error -> Error (map_error error)
                | Ok push ->
                    let initial_pending =
                      match (delivery, resume_from_revision) with
                      | New, None -> None
                      | Last_per_subject, None | All, None | _, Some _ ->
                          Some (Jetstream.Consumer.Push.initial_pending push)
                    in
                    let initial =
                      match initial_pending with
                      | None | Some 0L -> Marker
                      | Some _ -> Retained
                    in
                    Ok
                      {
                        value;
                        push;
                        connection = Jetstream.connection value.jetstream;
                        ignore_deletes;
                        initial;
                        initial_pending;
                        initial_received = 0L;
                      })))

  let next_message watch deadline =
    match deadline with
    | None -> Jetstream.Consumer.Push.next watch.push
    | Some deadline ->
        let now = Connection.now watch.connection in
        if Mtime.compare now deadline >= 0 then
          Error (Jetstream.Error.Connection Core_error.Timeout)
        else
          Jetstream.Consumer.Push.next_with_timeout
            ~timeout:(Mtime.span now deadline) watch.push

  let next_until watch deadline =
    let result = ref None in
    while Option.is_none !result do
      match watch.initial with
      | Marker ->
          watch.initial <- Live;
          result := Some (Ok Initial_done)
      | Retained | Live -> (
          match next_message watch deadline with
          | Error error -> result := Some (Error (map_error error))
          | Ok message -> (
              match entry_of_delivery watch.value message with
              | Error error -> result := Some (Error error)
              | Ok entry -> (
                  let initial_complete =
                    match watch.initial with
                    | Retained ->
                        if
                          Int64.compare watch.initial_received Int64.max_int < 0
                        then
                          watch.initial_received <-
                            Int64.add watch.initial_received 1L;
                        let received_enough =
                          match watch.initial_pending with
                          | Some pending ->
                              Int64.compare watch.initial_received pending >= 0
                          | None -> false
                        in
                        received_enough
                        || Int64.equal (Jetstream.Msg.num_pending message) 0L
                    | Marker | Live -> false
                  in
                  if initial_complete then watch.initial <- Marker;
                  match (watch.ignore_deletes, Entry.operation entry) with
                  | true, (Entry.Delete | Entry.Purge) -> ()
                  | _ -> result := Some (Ok (Entry entry)))))
    done;
    match !result with Some result -> result | None -> assert false

  let next watch = next_until watch None

  let next_with_timeout ~timeout watch =
    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
      Error (Error.Connection (Core_error.Invalid_timeout "watch"))
    else
      let deadline =
        match Mtime.add_span (Connection.now watch.connection) timeout with
        | Some deadline -> deadline
        | None -> Mtime.max_stamp
      in
      next_until watch (Some deadline)

  let iter watch ~f =
    let result = ref None in
    while Option.is_none !result do
      match next watch with
      | Ok event -> f event
      | Error Error.Closed -> result := Some (Ok ())
      | Error error -> result := Some (Error error)
    done;
    match !result with Some result -> result | None -> assert false

  let close watch =
    match Jetstream.Consumer.Push.close watch.push with
    | Ok () -> Ok ()
    | Error error -> Error (map_error error)
end

module Key_lister = struct
  type t = { watch : Watch.t; mutable done_ : bool }

  let v ~sw ?(filters = []) value =
    match
      Watch.v ~sw ~keys:filters ~ignore_deletes:true ~meta_only:true value
    with
    | Error error -> Error error
    | Ok watch -> Ok { watch; done_ = false }

  let next lister =
    if lister.done_ then Ok None
    else
      match Watch.next lister.watch with
      | Error error -> Error error
      | Ok Watch.Initial_done ->
          lister.done_ <- true;
          Ok None
      | Ok (Watch.Entry entry) -> Ok (Some (Entry.key entry))

  let next_with_timeout ~timeout lister =
    if lister.done_ then Ok None
    else
      match Watch.next_with_timeout ~timeout lister.watch with
      | Error error -> Error error
      | Ok Watch.Initial_done ->
          lister.done_ <- true;
          Ok None
      | Ok (Watch.Entry entry) -> Ok (Some (Entry.key entry))

  let close lister = Watch.close lister.watch
end

let manager_update jetstream config =
  match stream_for_config config jetstream with
  | Error error -> Error error
  | Ok (stream_config, jetstream) -> (
      match
        Jetstream.Stream.bind jetstream
          ~name:(Jetstream.Stream.Config.name stream_config)
      with
      | Error error -> Error (map_jetstream_error error)
      | Ok stream -> (
          match Jetstream.Stream.update stream stream_config with
          | Error error -> Error (map_jetstream_error error)
          | Ok _ -> Ok { jetstream; stream; bucket = Config.bucket config }))

let manager_create_or_update jetstream config =
  match stream_for_config config jetstream with
  | Error error -> Error error
  | Ok (stream_config, jetstream) -> (
      match Jetstream.Stream.create_or_update jetstream stream_config with
      | Error error -> Error (map_jetstream_error error)
      | Ok stream -> Ok { jetstream; stream; bucket = Config.bucket config })

let manager_delete jetstream ~bucket =
  match Config.v ~bucket () with
  | Error error -> Error (map_config_error error)
  | Ok _ -> (
      match Jetstream.Stream.bind jetstream ~name:(stream_name bucket) with
      | Error error -> Error (map_jetstream_error error)
      | Ok stream -> (
          match Jetstream.Stream.delete stream with
          | Ok () -> Ok ()
          | Error error -> Error (map_jetstream_error error)))

let manager_bucket_name ~prefix name =
  let prefix_length = String.length prefix in
  if
    String.length name <= prefix_length
    || not (String.equal (String.sub name 0 prefix_length) prefix)
  then None
  else Some (String.sub name prefix_length (String.length name - prefix_length))

let manager_entries jetstream =
  let subject = Nats.Subject.Filter.literal "$KV.*.>" in
  match Jetstream.Stream.list ~subject jetstream with
  | Error error -> Error (map_jetstream_error error)
  | Ok infos ->
      let entries = ref [] in
      let result = ref None in
      List.iter
        (fun info ->
          match !result with
          | Some _ -> ()
          | None -> (
              let name =
                Jetstream.Stream.Config.name
                  (Jetstream.Stream.Info.config info)
              in
              match manager_bucket_name ~prefix:"KV_" name with
              | None -> ()
              | Some bucket -> (
                  match Config.v ~bucket () with
                  | Error error -> result := Some (Error (map_config_error error))
                  | Ok _ -> entries := (bucket, info) :: !entries)))
        infos;
      match !result with
      | Some result -> result
      | None -> Ok (List.rev !entries)

let manager_statuses jetstream entries =
  let statuses = ref [] in
  let result = ref None in
  List.iter
    (fun (bucket, info) ->
      match !result with
      | Some _ -> ()
      | None -> (
          match Jetstream.Stream.bind jetstream ~name:(stream_name bucket) with
          | Error error -> result := Some (Error (map_jetstream_error error))
          | Ok stream ->
              let value = { jetstream; stream; bucket } in
              statuses := status_of_info value info :: !statuses))
    entries;
  match !result with Some result -> result | None -> Ok (List.rev !statuses)

module Manager = struct
  let open_ = open_
  let create = create
  let update = manager_update
  let create_or_update = manager_create_or_update
  let delete = manager_delete

  let names jetstream =
    match manager_entries jetstream with
    | Error error -> Error error
    | Ok entries -> Ok (List.map fst entries)

  let statuses jetstream =
    match manager_entries jetstream with
    | Error error -> Error error
    | Ok entries -> manager_statuses jetstream entries
end
