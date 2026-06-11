module Core_error = Error
module String_map = Map.Make (String)

let ( let* ) value f =
  match value with Error error -> Error error | Ok value -> f value

module Config = struct
  type storage = Memory | File
  type compression = Jetstream.Stream.Config.compression = Uncompressed | S2

  module Placement = Jetstream.Stream.Config.Placement

  type t = {
    bucket : string;
    description : string option;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    storage : storage;
    replicas : int;
    placement : Placement.t option;
    compression : compression;
    metadata : (string * string) list;
  }

  type error =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_ttl
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_replicas of int

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
      let failure = ref None in
      for position = 0 to length - 1 do
        match !failure with
        | Some _ -> ()
        | None ->
            let character = String.get bucket position in
            if not (allowed_bucket_character character) then
              failure := Some (Invalid_bucket_character { position; character })
      done;
      match !failure with None -> Ok () | Some error -> Error error

  let normalize_limit = function
    | Some value when Int64.equal value 0L -> None
    | Some -1L -> None
    | value -> value

  let validate_limit field = function
    | None -> Ok ()
    | Some value when Int64.compare value (-1L) >= 0 -> Ok ()
    | Some value -> Error (Invalid_limit { field; value })

  let v ~bucket ?description ?ttl ?max_bytes ?(storage = File) ?(replicas = 1)
      ?placement ?(compression = Jetstream.Stream.Config.Uncompressed)
      ?(metadata = []) () =
    match validate_bucket bucket with
    | Error error -> Error error
    | Ok () -> (
        match ttl with
        | Some value when Mtime.Span.compare value Mtime.Span.zero < 0 ->
            Error Invalid_ttl
        | _ -> (
            match validate_limit "max_bytes" max_bytes with
            | Error error -> Error error
            | Ok () when replicas < 1 || replicas > 5 ->
                Error (Invalid_replicas replicas)
            | Ok () ->
                Ok
                  {
                    bucket;
                    description;
                    ttl =
                      (match ttl with
                      | Some value
                        when Int.equal
                               (Mtime.Span.compare value Mtime.Span.zero)
                               0 ->
                          None
                      | value -> value);
                    max_bytes = normalize_limit max_bytes;
                    storage;
                    replicas;
                    placement;
                    compression;
                    metadata;
                  }))

  let bucket value = value.bucket
  let description value = value.description
  let ttl value = value.ttl
  let max_bytes value = value.max_bytes
  let storage value = value.storage
  let replicas value = value.replicas
  let placement value = value.placement
  let compression value = value.compression
  let metadata value = value.metadata
end

module Name = struct
  type t = string
  type error = Empty_name

  let of_string value =
    if Int.equal (String.length value) 0 then Error Empty_name else Ok value

  let to_string value = value
end

module Link = struct
  type t = { bucket : string; name : Name.t option }
  type error = Config.error

  let v ~bucket ?name () =
    match Config.v ~bucket () with
    | Error error -> Error error
    | Ok _ -> Ok { bucket; name }

  let bucket value = value.bucket
  let name value = value.name
end

module Meta = struct
  type t = {
    name : Name.t;
    description : string option;
    headers : Nats.Header.t;
    metadata : (string * string) list;
    chunk_size : int;
  }

  type error = Invalid_chunk_size of int

  let default_chunk_size = 128 * 1024

  let v ~name ?description ?(headers = Nats.Header.empty) ?(metadata = [])
      ?(chunk_size = default_chunk_size) () =
    if Int.compare chunk_size 0 <= 0 then Error (Invalid_chunk_size chunk_size)
    else Ok { name; description; headers; metadata; chunk_size }

  let name value = value.name
  let description value = value.description
  let headers value = value.headers
  let metadata value = value.metadata
  let chunk_size value = value.chunk_size
end

module Info = struct
  type t = {
    bucket : string;
    name : Name.t;
    description : string option;
    headers : Nats.Header.t;
    metadata : (string * string) list;
    link : Link.t option;
    nuid : string;
    size : int64;
    chunks : int64;
    chunk_size : int;
    digest : string;
    deleted : bool;
    timestamp : string;
  }

  let bucket value = value.bucket
  let name value = value.name
  let description value = value.description
  let headers value = value.headers
  let metadata value = value.metadata
  let link value = value.link
  let nuid value = value.nuid
  let size value = value.size
  let chunks value = value.chunks
  let chunk_size value = value.chunk_size
  let digest value = value.digest
  let deleted value = value.deleted
  let timestamp value = value.timestamp
end

module Error = struct
  type config = Config.error
  type name = Name.error
  type meta = Meta.error

  type t =
    | Connection of Connection.error
    | Jetstream of Jetstream.Error.t
    | Decode of Jsont.Error.t
    | Encode of Jsont.Error.t
    | Invalid_config of config
    | Invalid_name of { value : string; reason : name }
    | Invalid_meta of meta
    | Invalid_headers of Nats.Header.error
    | Unexpected_bucket of { expected : string; actual : string }
    | Unexpected_object_name of { expected : string; actual : string }
    | Invalid_link_bucket of config
    | Invalid_link_name of { value : string; reason : name }
    | Invalid_metadata of string
    | Invalid_metadata_subject of string
    | File of { operation : string; message : string }
    | Not_found
    | Deleted of Info.t
    | Object_already_exists of Info.t
    | No_link_to_link
    | Link_to_bucket of Info.t
    | Link_to_deleted of { bucket : string; name : string }
    | Link_cycle of string list
    | Link_depth_exceeded of { limit : int; info : Info.t }
    | Closed
    | Size_mismatch of { expected : int64; actual : int64 }
    | Chunk_count_mismatch of { expected : int64; actual : int64 }
    | Digest_mismatch of { expected : string; actual : string }
    | Invalid_chunk_subject of string
    | Cleanup_failed of { info : Info.t option; error : Jetstream.Error.t }

  let pp_config ppf = function
    | Config.Empty_bucket -> Format.pp_print_string ppf "bucket name is empty"
    | Config.Invalid_bucket_character { position; character } ->
        Format.fprintf ppf "invalid bucket-name character %C at position %d"
          character position
    | Config.Invalid_ttl ->
        Format.pp_print_string ppf "object-store TTL must not be negative"
    | Config.Invalid_limit { field; value } ->
        Format.fprintf ppf "invalid object-store %s limit %Ld" field value
    | Config.Invalid_replicas value ->
        Format.fprintf ppf
          "object-store replicas must be between 1 and 5, got %d" value

  let pp_name ppf = function
    | Name.Empty_name -> Format.pp_print_string ppf "object name is empty"

  let pp_meta ppf = function
    | Meta.Invalid_chunk_size value ->
        Format.fprintf ppf "object chunk size must be positive, got %d" value

  let pp ppf = function
    | Connection error ->
        Format.fprintf ppf "connection: %a" Core_error.pp error
    | Jetstream error ->
        Format.fprintf ppf "JetStream: %a" Jetstream.Error.pp error
    | Decode error -> Format.fprintf ppf "JSON decode: %a" Jsont.Error.pp error
    | Encode error -> Format.fprintf ppf "JSON encode: %a" Jsont.Error.pp error
    | Invalid_config error ->
        Format.fprintf ppf "invalid object-store config: %a" pp_config error
    | Invalid_name { value; reason } ->
        Format.fprintf ppf "invalid object name %S: %a" value pp_name reason
    | Invalid_meta error ->
        Format.fprintf ppf "invalid object metadata: %a" pp_meta error
    | Invalid_headers error ->
        Format.fprintf ppf "invalid object headers: %a" Nats.Header.pp_error
          error
    | Unexpected_bucket { expected; actual } ->
        Format.fprintf ppf "metadata bucket %S does not match %S" actual
          expected
    | Unexpected_object_name { expected; actual } ->
        Format.fprintf ppf "metadata object name %S does not match %S" actual
          expected
    | Invalid_link_bucket error ->
        Format.fprintf ppf "invalid link bucket: %a" pp_config error
    | Invalid_link_name { value; reason } ->
        Format.fprintf ppf "invalid link object name %S: %a" value pp_name
          reason
    | Invalid_metadata field ->
        Format.fprintf ppf "invalid object metadata field %S" field
    | Invalid_metadata_subject subject ->
        Format.fprintf ppf "unexpected object metadata subject %S" subject
    | File { operation; message } ->
        Format.fprintf ppf "object file %s failed: %s" operation message
    | Not_found -> Format.pp_print_string ppf "object was not found"
    | Deleted _ -> Format.pp_print_string ppf "object is deleted"
    | Object_already_exists _ ->
        Format.pp_print_string ppf "an object with that name already exists"
    | No_link_to_link ->
        Format.pp_print_string ppf "an object link cannot target another link"
    | Link_to_bucket info ->
        Format.fprintf ppf "bucket link %S cannot be read as object content"
          (Info.bucket info)
    | Link_to_deleted { bucket; name } ->
        Format.fprintf ppf "link target %S/%S is deleted" bucket name
    | Link_cycle path ->
        Format.fprintf ppf "object link cycle: %s" (String.concat " -> " path)
    | Link_depth_exceeded { limit; info } ->
        Format.fprintf ppf "object link depth exceeded %d at %S" limit
          (Name.to_string (Info.name info))
    | Closed -> Format.pp_print_string ppf "object-store watch is closed"
    | Size_mismatch { expected; actual } ->
        Format.fprintf ppf "object size %Ld does not match expected %Ld" actual
          expected
    | Chunk_count_mismatch { expected; actual } ->
        Format.fprintf ppf "object chunk count %Ld does not match expected %Ld"
          actual expected
    | Digest_mismatch { expected; actual } ->
        Format.fprintf ppf "object digest %S does not match expected %S" actual
          expected
    | Invalid_chunk_subject subject ->
        Format.fprintf ppf "unexpected object chunk subject %S" subject
    | Cleanup_failed { info; error } ->
        (match info with
        | None -> Format.pp_print_string ppf "object cleanup failed"
        | Some _ ->
            Format.pp_print_string ppf "object committed; cleanup failed");
        Format.fprintf ppf ": %a" Jetstream.Error.pp error
end

module Status = struct
  type t = {
    bucket : string;
    description : string option;
    messages : int64;
    bytes : int64;
    first_sequence : int64;
    last_sequence : int64;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    storage : Config.storage;
    replicas : int;
    placement : Config.Placement.t option;
    compression : Config.compression;
    metadata : (string * string) list;
    sealed : bool;
  }

  let bucket value = value.bucket
  let description value = value.description
  let messages value = value.messages
  let bytes value = value.bytes
  let first_sequence value = value.first_sequence
  let last_sequence value = value.last_sequence
  let ttl value = value.ttl
  let max_bytes value = value.max_bytes
  let storage value = value.storage
  let replicas value = value.replicas
  let placement value = value.placement
  let compression value = value.compression
  let metadata value = value.metadata
  let sealed value = value.sealed
end

type t = {
  jetstream : Jetstream.t;
  stream : Jetstream.Stream.t;
  bucket : string;
}

type bucket = t
type wire_link = { bucket : string; name : string option }
type wire_options = { chunk_size : int option; link : wire_link option }

type wire_info = {
  name : string;
  description : string option;
  headers : string list String_map.t option;
  metadata : string String_map.t option;
  options : wire_options option;
  bucket : string;
  nuid : string;
  size : int64;
  mtime : string option;
  chunks : int64;
  digest : string option;
  deleted : bool;
}

let wire_link_codec =
  Jsont.Object.map ~kind:"NATS object link" (fun bucket name ->
      { bucket; name })
  |> Jsont.Object.mem "bucket" Jsont.string ~enc:(fun (value : wire_link) ->
      value.bucket)
  |> Jsont.Object.opt_mem "name" Jsont.string ~enc:(fun (value : wire_link) ->
      value.name)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let wire_options_codec =
  Jsont.Object.map ~kind:"NATS object options" (fun chunk_size link ->
      { chunk_size; link })
  |> Jsont.Object.opt_mem "max_chunk_size" Jsont.int
       ~enc:(fun (value : wire_options) -> value.chunk_size)
  |> Jsont.Object.opt_mem "link" wire_link_codec
       ~enc:(fun (value : wire_options) -> value.link)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let headers_codec = Jsont.Object.as_string_map (Jsont.list Jsont.string)
let metadata_codec = Jsont.Object.as_string_map Jsont.string

let wire_info_codec =
  Jsont.Object.map ~kind:"NATS object metadata"
    (fun
      name
      description
      headers
      metadata
      options
      bucket
      nuid
      size
      mtime
      chunks
      digest
      deleted
    ->
      {
        name;
        description;
        headers;
        metadata;
        options;
        bucket;
        nuid;
        size;
        mtime;
        chunks;
        digest;
        deleted = Option.value ~default:false deleted;
      })
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun (value : wire_info) ->
      value.name)
  |> Jsont.Object.opt_mem "description" Jsont.string
       ~enc:(fun (value : wire_info) -> value.description)
  |> Jsont.Object.opt_mem "headers" headers_codec
       ~enc:(fun (value : wire_info) -> value.headers)
  |> Jsont.Object.opt_mem "metadata" metadata_codec
       ~enc:(fun (value : wire_info) -> value.metadata)
  |> Jsont.Object.opt_mem "options" wire_options_codec
       ~enc:(fun (value : wire_info) -> value.options)
  |> Jsont.Object.mem "bucket" Jsont.string ~enc:(fun (value : wire_info) ->
      value.bucket)
  |> Jsont.Object.mem "nuid" Jsont.string ~enc:(fun (value : wire_info) ->
      value.nuid)
  |> Jsont.Object.mem "size" Jsont.int64 ~enc:(fun (value : wire_info) ->
      value.size)
  |> Jsont.Object.opt_mem "mtime" Jsont.string ~enc:(fun (value : wire_info) ->
      value.mtime)
  |> Jsont.Object.mem "chunks" Jsont.int64 ~enc:(fun (value : wire_info) ->
      value.chunks)
  |> Jsont.Object.opt_mem "digest" Jsont.string ~enc:(fun (value : wire_info) ->
      value.digest)
  |> Jsont.Object.opt_mem "deleted" Jsont.bool ~enc:(fun (value : wire_info) ->
      Some value.deleted)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let default_chunk_size = 128 * 1024
let zero_mtime = "0001-01-01T00:00:00Z"
let digest_prefix = "SHA-256="

let map_jetstream_error = function
  | Jetstream.Error.Connection error -> Error.Connection error
  | error -> Error.Jetstream error

let map_config_error error = Error.Invalid_config error
let stream_name bucket = "OBJ_" ^ bucket
let chunk_filter bucket = Nats.Subject.Filter.literal ("$O." ^ bucket ^ ".C.>")

let metadata_filter bucket =
  Nats.Subject.Filter.literal ("$O." ^ bucket ^ ".M.>")

let chunk_subject bucket nuid = "$O." ^ bucket ^ ".C." ^ nuid

let encode_name name =
  Base64.encode_string ~alphabet:Base64.uri_safe_alphabet name

let metadata_subject bucket name =
  "$O." ^ bucket ^ ".M." ^ encode_name (Name.to_string name)

let stream_config config =
  match
    Jetstream.Stream.Config.v
      ~name:(stream_name (Config.bucket config))
      ~subjects:
        [
          chunk_filter (Config.bucket config);
          metadata_filter (Config.bucket config);
        ]
      ?description:(Config.description config)
      ~storage:
        (match Config.storage config with
        | Config.Memory -> Jetstream.Stream.Config.Memory
        | Config.File -> Jetstream.Stream.Config.File)
      ~retention:Jetstream.Stream.Config.Limits
      ~discard:Jetstream.Stream.Config.New ?max_bytes:(Config.max_bytes config)
      ?max_age:(Config.ttl config) ~replicas:(Config.replicas config)
      ?placement:(Config.placement config)
      ~compression:(Config.compression config)
      ~metadata:(Config.metadata config) ~allow_rollup:true ~allow_direct:true
      ()
  with
  | Error error ->
      Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
  | Ok config -> Ok config

let create jetstream config =
  match stream_config config with
  | Error error -> Error error
  | Ok stream_config -> (
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
    messages = Jetstream.Stream.Info.messages info;
    bytes = Jetstream.Stream.Info.bytes info;
    first_sequence = Jetstream.Stream.Info.first_sequence info;
    last_sequence = Jetstream.Stream.Info.last_sequence info;
    ttl = Jetstream.Stream.Config.max_age config;
    max_bytes = Jetstream.Stream.Config.max_bytes config;
    storage;
    replicas = Jetstream.Stream.Config.replicas config;
    placement = Jetstream.Stream.Config.placement config;
    compression = Jetstream.Stream.Config.compression config;
    metadata = Jetstream.Stream.Config.metadata config;
    sealed = Jetstream.Stream.Config.sealed config;
  }

let status value =
  match Jetstream.Stream.info value.stream with
  | Error error -> Error (map_jetstream_error error)
  | Ok info -> Ok (status_of_info value info)

let update_config (value : t) (config : Config.t) =
  if not (String.equal (Config.bucket config) value.bucket) then
    Error
      (Error.Unexpected_bucket
         { expected = value.bucket; actual = Config.bucket config })
  else
    let stream_config_error error =
      Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
    in
    match Jetstream.Stream.info value.stream with
    | Error error -> Error (map_jetstream_error error)
    | Ok info -> (
        let current = Jetstream.Stream.Info.config info in
        let apply result =
          match result with
          | Ok value -> Ok value
          | Error error -> stream_config_error error
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_name current
               (stream_name value.bucket))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_subjects current
               [ chunk_filter value.bucket; metadata_filter value.bucket ])
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_description current
               (Config.description config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_storage current
               (match Config.storage config with
               | Config.Memory -> Jetstream.Stream.Config.Memory
               | Config.File -> Jetstream.Stream.Config.File))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_replicas current
               (Config.replicas config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_placement current
               (Config.placement config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_compression current
               (Config.compression config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_metadata current
               (Config.metadata config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_max_age current (Config.ttl config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_max_bytes current
               (Config.max_bytes config))
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_retention current
               Jetstream.Stream.Config.Limits)
        in
        let* current =
          apply
            (Jetstream.Stream.Config.with_discard current
               Jetstream.Stream.Config.New)
        in
        let* current =
          apply (Jetstream.Stream.Config.with_allow_rollup current true)
        in
        let* current =
          apply (Jetstream.Stream.Config.with_allow_direct current true)
        in
        match Jetstream.Stream.update value.stream current with
        | Error error -> Error (map_jetstream_error error)
        | Ok info -> Ok (status_of_info value info))

let headers_to_wire headers =
  let add map (name, value) =
    String_map.update name
      (function
        | None -> Some [ value ] | Some values -> Some (value :: values))
      map
  in
  let values =
    List.fold_left add String_map.empty (Nats.Header.to_list headers)
  in
  if String_map.is_empty values then None
  else Some (String_map.map List.rev values)

let metadata_to_wire metadata =
  let add map (name, value) = String_map.add name value map in
  let values = List.fold_left add String_map.empty metadata in
  if String_map.is_empty values then None else Some values

let headers_of_wire headers =
  let entries =
    match headers with
    | None -> []
    | Some values ->
        String_map.bindings values
        |> List.concat_map (fun (name, values) ->
            List.map (fun value -> (name, value)) values)
  in
  match Nats.Header.of_list entries with
  | Ok headers -> Ok headers
  | Error error -> Error (Error.Invalid_headers error)

let metadata_of_wire metadata =
  match metadata with None -> [] | Some values -> String_map.bindings values

let link_of_wire (link : wire_link option) =
  match link with
  | None -> Ok None
  | Some (link : wire_link) -> (
      match Config.v ~bucket:link.bucket () with
      | Error error -> Error (Error.Invalid_link_bucket error)
      | Ok _ -> (
          match link.name with
          | None -> Ok (Some { Link.bucket = link.bucket; name = None })
          | Some name -> (
              match Name.of_string name with
              | Error reason ->
                  Error (Error.Invalid_link_name { value = name; reason })
              | Ok name ->
                  Ok (Some { Link.bucket = link.bucket; name = Some name }))))

let positive_metadata_int field value =
  match value with
  | None -> Ok default_chunk_size
  | Some value when Int.compare value 0 > 0 -> Ok value
  | Some _ -> Error (Error.Invalid_metadata field)

let nonnegative_metadata_int64 field value =
  if Int64.compare value 0L >= 0 then Ok value
  else Error (Error.Invalid_metadata field)

let info_of_wire ~bucket ~requested_name ~timestamp wire =
  if not (String.equal wire.bucket bucket) then
    Error (Error.Unexpected_bucket { expected = bucket; actual = wire.bucket })
  else
    let requested_name = Name.to_string requested_name in
    if not (String.equal wire.name requested_name) then
      Error
        (Error.Unexpected_object_name
           { expected = requested_name; actual = wire.name })
    else
      match Name.of_string wire.name with
      | Error reason -> Error (Error.Invalid_name { value = wire.name; reason })
      | Ok name ->
          let* headers = headers_of_wire wire.headers in
          let* size = nonnegative_metadata_int64 "size" wire.size in
          let* chunks = nonnegative_metadata_int64 "chunks" wire.chunks in
          let* () =
            if Int64.equal chunks 0L || not (String.equal wire.nuid "") then
              Ok ()
            else Error (Error.Invalid_metadata "nuid")
          in
          let* chunk_size =
            positive_metadata_int "options.max_chunk_size"
              (Option.bind wire.options (fun value -> value.chunk_size))
          in
          let* link =
            link_of_wire (Option.bind wire.options (fun value -> value.link))
          in
          Ok
            {
              Info.bucket;
              name;
              description = wire.description;
              headers;
              metadata = metadata_of_wire wire.metadata;
              link;
              nuid = wire.nuid;
              size;
              chunks;
              chunk_size;
              digest = Option.value ~default:"" wire.digest;
              deleted = wire.deleted;
              timestamp;
            }

let wire_of_object ~bucket ~name ~description ~headers ~metadata ~options ~nuid
    ~size ~chunks ~digest ~deleted =
  {
    name = Name.to_string name;
    description;
    headers = headers_to_wire headers;
    metadata = metadata_to_wire metadata;
    options;
    bucket;
    nuid;
    size;
    mtime = Some zero_mtime;
    chunks;
    digest;
    deleted;
  }

let encode_wire wire =
  match Jsont_bytesrw.encode_string' wire_info_codec wire with
  | Ok payload -> Ok payload
  | Error error -> Error (Error.Encode error)

let decode_message (value : t) ~requested_name message =
  match
    Jsont_bytesrw.decode_string' wire_info_codec
      (Jetstream.Stream.Message.payload message)
  with
  | Error error -> Error (Error.Decode error)
  | Ok wire ->
      info_of_wire ~bucket:value.bucket ~requested_name
        ~timestamp:(Jetstream.Stream.Message.timestamp message)
        wire

let read_info_raw ?timeout (value : t) (name : Name.t) =
  let subject = Nats.Subject.literal (metadata_subject value.bucket name) in
  match Jetstream.Stream.get_last ?timeout value.stream ~subject with
  | Error Jetstream.Error.Message_not_found -> Error Error.Not_found
  | Error error -> Error (map_jetstream_error error)
  | Ok message -> decode_message value ~requested_name:name message

let info ?timeout ?(include_deleted = false) value name =
  match read_info_raw ?timeout value name with
  | Error error -> Error error
  | Ok info when Info.deleted info && not include_deleted ->
      Error Error.Not_found
  | Ok info -> Ok info

let new_nuid value =
  let connection = Jetstream.connection value.jetstream in
  encode_name (Nats.Subject.to_string (Connection.fresh_inbox connection))

type upload = { size : int64; chunks : int64; digest : string; nuid : string }

let next_chunk reader chunk_size =
  let buffer = Bytes.create chunk_size in
  let written = ref 0 in
  let finished = ref false in
  while Int.compare !written chunk_size < 0 && not !finished do
    let slice = Bytesrw.Bytes.Reader.read reader in
    if Bytesrw.Bytes.Slice.is_eod slice then finished := true
    else
      let available = Bytesrw.Bytes.Slice.length slice in
      let length = Int.min available (chunk_size - !written) in
      Bytes.blit
        (Bytesrw.Bytes.Slice.bytes slice)
        (Bytesrw.Bytes.Slice.first slice)
        buffer !written length;
      written := !written + length;
      if Int.compare length available < 0 then
        Bytesrw.Bytes.Reader.push_back reader
          (Bytesrw.Bytes.Slice.sub slice ~first:length
             ~length:(available - length))
  done;
  if Int.equal !written 0 then None
  else Some (Bytes.sub_string buffer 0 !written)

let digest_string context =
  digest_prefix
  ^ Base64.encode_string ~alphabet:Base64.uri_safe_alphabet
      (Digestif.SHA256.to_raw_string (Digestif.SHA256.get context))

let timeout_deadline value timeout =
  match timeout with
  | None -> None
  | Some timeout -> (
      match
        Mtime.add_span
          (Connection.now (Jetstream.connection value.jetstream))
          timeout
      with
      | Some deadline -> Some deadline
      | None -> Some Mtime.max_stamp)

let remaining_timeout value deadline =
  match deadline with
  | None -> Ok None
  | Some deadline ->
      let now = Connection.now (Jetstream.connection value.jetstream) in
      if Mtime.compare now deadline >= 0 then
        Error (Error.Connection Core_error.Timeout)
      else Ok (Some (Mtime.span now deadline))

let purge_chunks ?timeout value subject =
  match
    Jetstream.Stream.purge ?timeout
      ~subject:(Nats.Subject.Filter.literal subject)
      value.stream
  with
  | Ok _ -> Ok ()
  | Error error -> Error error

let upload_chunks ?deadline value ~nuid ~chunk_size reader =
  let digest = ref (Digestif.SHA256.init ()) in
  let size = ref 0L in
  let chunks = ref 0L in
  let result = ref None in
  let finished = ref false in
  while (not !finished) && Option.is_none !result do
    match next_chunk reader chunk_size with
    | None -> finished := true
    | Some chunk -> (
        digest := Digestif.SHA256.feed_string !digest chunk;
        match remaining_timeout value deadline with
        | Error error -> result := Some (Error error)
        | Ok publish_timeout -> (
            match
              Jetstream.publish ?timeout:publish_timeout value.jetstream
                (Nats.Subject.literal (chunk_subject value.bucket nuid))
                chunk
            with
            | Error error -> result := Some (Error (map_jetstream_error error))
            | Ok _ ->
                size := Int64.add !size (Int64.of_int (String.length chunk));
                chunks := Int64.add !chunks 1L))
  done;
  match !result with
  | Some result -> result
  | None ->
      Ok
        { size = !size; chunks = !chunks; digest = digest_string !digest; nuid }

let metadata_headers =
  match Nats.Header.of_list [ ("Nats-Rollup", "sub") ] with
  | Ok headers -> headers
  | Error _ -> assert false

let put ?timeout value meta reader =
  let deadline = timeout_deadline value timeout in
  let name = Meta.name meta in
  let old =
    match remaining_timeout value deadline with
    | Error error -> Error error
    | Ok info_timeout -> (
        match read_info_raw ?timeout:info_timeout value name with
        | Ok info -> Ok (Some info)
        | Error Error.Not_found -> Ok None
        | Error error -> Error error)
  in
  match old with
  | Error error -> Error error
  | Ok old ->
      let nuid = new_nuid value in
      let committed = ref false in
      let cleanup () =
        if not !committed then
          ignore
            (Eio.Cancel.protect (fun () ->
                 ignore (purge_chunks value (chunk_subject value.bucket nuid))))
      in
      Fun.protect ~finally:cleanup (fun () ->
          match
            upload_chunks ?deadline value ~nuid
              ~chunk_size:(Meta.chunk_size meta) reader
          with
          | Error error -> Error error
          | Ok upload -> (
              let wire =
                wire_of_object ~bucket:value.bucket ~name
                  ~description:(Meta.description meta)
                  ~headers:(Meta.headers meta) ~metadata:(Meta.metadata meta)
                  ~options:
                    (Some
                       { chunk_size = Some (Meta.chunk_size meta); link = None })
                  ~nuid:upload.nuid ~size:upload.size ~chunks:upload.chunks
                  ~digest:(Some upload.digest) ~deleted:false
              in
              match encode_wire wire with
              | Error error -> Error error
              | Ok payload -> (
                  match remaining_timeout value deadline with
                  | Error error -> Error error
                  | Ok publish_timeout -> (
                      match
                        Jetstream.publish ?timeout:publish_timeout
                          ~headers:metadata_headers value.jetstream
                          (Nats.Subject.literal
                             (metadata_subject value.bucket name))
                          payload
                      with
                      | Error error -> Error (map_jetstream_error error)
                      | Ok _ -> (
                          committed := true;
                          let result =
                            match remaining_timeout value deadline with
                            | Error error -> Error error
                            | Ok info_timeout ->
                                read_info_raw ?timeout:info_timeout value name
                          in
                          let cleanup_result =
                            match old with
                            | Some old_info when not (Info.deleted old_info)
                              -> (
                                match remaining_timeout value deadline with
                                | Error _ ->
                                    Error
                                      (Jetstream.Error.Connection
                                         Core_error.Timeout)
                                | Ok purge_timeout -> (
                                    match
                                      purge_chunks ?timeout:purge_timeout value
                                        (chunk_subject value.bucket
                                           (Info.nuid old_info))
                                    with
                                    | Ok () -> Ok ()
                                    | Error error -> Error error))
                            | _ -> Ok ()
                          in
                          match result with
                          | Error error -> Error error
                          | Ok info -> (
                              match cleanup_result with
                              | Error error ->
                                  Error
                                    (Error.Cleanup_failed
                                       { info = Some info; error })
                              | Ok () -> Ok info))))))

let put_string ?timeout value meta payload =
  put ?timeout value meta (Bytesrw.Bytes.Reader.of_string payload)

let reader_of_flow flow =
  let buffer = Cstruct.create (64 * 1024) in
  Bytesrw.Bytes.Reader.make (fun () ->
      try
        let length = Eio.Flow.single_read flow buffer in
        if Int.equal length 0 then Bytesrw.Bytes.Slice.eod
        else
          let bytes = Cstruct.to_bytes (Cstruct.sub buffer 0 length) in
          Bytesrw.Bytes.Slice.of_bytes bytes
      with End_of_file -> Bytesrw.Bytes.Slice.eod)

let writer_of_flow flow =
  Bytesrw.Bytes.Writer.make (fun slice ->
      if not (Bytesrw.Bytes.Slice.is_eod slice) then
        let bytes = Bytesrw.Bytes.Slice.bytes slice in
        let cstruct =
          Cstruct.of_bytes ~off:(Bytesrw.Bytes.Slice.first slice)
            ~len:(Bytesrw.Bytes.Slice.length slice) bytes
        in
        Eio.Flow.write flow [ cstruct ])

let file_error ~operation error =
  Error
    (Error.File
       { operation; message = Format.asprintf "%a" Eio.Exn.pp error })

let protect_file ~operation f =
  try f () with
  | Eio.Io _ as error -> file_error ~operation error
  | Sys_error message -> Error (Error.File { operation; message })

let file_basename path =
  match Eio.Path.split path with
  | None -> Error (Error.Invalid_name { value = ""; reason = Name.Empty_name })
  | Some (_, basename) -> (
      match Name.of_string basename with
      | Ok name -> Ok name
      | Error reason -> Error (Error.Invalid_name { value = basename; reason }))

let put_file ?timeout ?name ?description ?headers ?metadata ?chunk_size value
    path =
  let name = match name with Some name -> Ok name | None -> file_basename path in
  match name with
  | Error error -> Error error
  | Ok name -> (
      match Meta.v ~name ?description ?headers ?metadata ?chunk_size () with
      | Error error -> Error (Error.Invalid_meta error)
      | Ok meta ->
          protect_file ~operation:"read" (fun () ->
              Eio.Path.with_open_in path (fun flow ->
                  put ?timeout value meta (reader_of_flow flow))))

let link_to_wire (link : Link.t) =
  {
    bucket = Link.bucket link;
    name = Option.map Name.to_string (Link.name link);
  }

let options_of_info info =
  Some
    {
      chunk_size =
        (match Info.link info with
        | None -> Some (Info.chunk_size info)
        | Some _ -> None);
      link = Option.map link_to_wire (Info.link info);
    }

let timestamp_of_nanoseconds value =
  if Int64.compare value 0L < 0 then Error (Error.Invalid_metadata "timestamp")
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

let get_content ~deadline (value : t) info writer =
  let expected_size = Info.size info in
  let expected_chunks = Info.chunks info in
  let expected_digest = Info.digest info in
  let verify digest size chunks =
    let actual_digest = digest_string digest in
    if Int64.compare size expected_size <> 0 then
      Error (Error.Size_mismatch { expected = expected_size; actual = size })
    else if Int64.compare chunks expected_chunks <> 0 then
      Error
        (Error.Chunk_count_mismatch
           { expected = expected_chunks; actual = chunks })
    else if not (String.equal actual_digest expected_digest) then
      Error
        (Error.Digest_mismatch
           { expected = expected_digest; actual = actual_digest })
    else Ok ()
  in
  if Int64.equal expected_chunks 0L then (
    let digest = Digestif.SHA256.init () in
    match verify digest 0L 0L with
    | Error error -> Error error
    | Ok () ->
        Bytesrw.Bytes.Writer.write_eod writer;
        Ok info)
  else
    Eio.Switch.run (fun sw ->
        let filter =
          Nats.Subject.Filter.literal
            (chunk_subject value.bucket (Info.nuid info))
        in
        match
          Jetstream.Consumer.Ordered.v ~sw ~batch:1 ?expires:None
            ?filter_subject:(Some filter) value.stream
        with
        | Error error -> Error (map_jetstream_error error)
        | Ok ordered -> (
            let digest = ref (Digestif.SHA256.init ()) in
            let size = ref 0L in
            let chunks = ref 0L in
            let result = ref None in
            let next_message () =
              match remaining_timeout value deadline with
              | Error error -> Error error
              | Ok None -> (
                  match Jetstream.Consumer.Ordered.next ordered with
                  | Ok message -> Ok message
                  | Error error -> Error (map_jetstream_error error))
              | Ok (Some timeout) -> (
                  match
                    Jetstream.Consumer.Ordered.next_with_timeout ~timeout
                      ordered
                  with
                  | Ok message -> Ok message
                  | Error error -> Error (map_jetstream_error error))
            in
            let read_chunks () =
              while
                Int64.compare !chunks expected_chunks < 0
                && Option.is_none !result
              do
                match next_message () with
                | Error error -> result := Some (Error error)
                | Ok message ->
                    let actual_subject =
                      Nats.Subject.to_string (Jetstream.Msg.subject message)
                    in
                    let expected_subject =
                      chunk_subject value.bucket (Info.nuid info)
                    in
                    if not (String.equal actual_subject expected_subject) then
                      result :=
                        Some
                          (Error (Error.Invalid_chunk_subject actual_subject))
                    else
                      let payload = Jetstream.Msg.payload message in
                      Bytesrw.Bytes.Writer.write_string writer payload;
                      digest := Digestif.SHA256.feed_string !digest payload;
                      size :=
                        Int64.add !size (Int64.of_int (String.length payload));
                      chunks := Int64.add !chunks 1L;
                      if
                        Int64.equal (Jetstream.Msg.num_pending message) 0L
                        && Int64.compare !chunks expected_chunks < 0
                      then
                        result :=
                          Some
                            (Error
                               (Error.Chunk_count_mismatch
                                  {
                                    expected = expected_chunks;
                                    actual = !chunks;
                                  }))
              done
            in
            let result =
              Fun.protect
                ~finally:(fun () ->
                  ignore
                    (Eio.Cancel.protect (fun () ->
                         ignore (Jetstream.Consumer.Ordered.close ordered))))
                (fun () ->
                  read_chunks ();
                  match !result with
                  | Some result -> result
                  | None -> verify !digest !size !chunks)
            in
            match result with
            | Error error -> Error error
            | Ok () ->
                Bytesrw.Bytes.Writer.write_eod writer;
                Ok info))

let resolve_link ~max_links value info deadline =
  let rec loop depth path value info =
    match Info.link info with
    | None -> Ok (value, info)
    | Some link -> (
        match Link.name link with
        | None -> Error (Error.Link_to_bucket info)
        | Some name -> (
            if Int.compare depth max_links >= 0 then
              Error (Error.Link_depth_exceeded { limit = max_links; info })
            else
              let identity = Link.bucket link ^ "/" ^ Name.to_string name in
              if List.exists (String.equal identity) path then
                Error (Error.Link_cycle (List.rev (identity :: path)))
              else
                match bind value.jetstream ~bucket:(Link.bucket link) with
                | Error error -> Error error
                | Ok target -> (
                    match remaining_timeout target deadline with
                    | Error error -> Error error
                    | Ok timeout -> (
                        match read_info_raw ?timeout target name with
                        | Error Error.Not_found -> Error Error.Not_found
                        | Error error -> Error error
                        | Ok target_info when Info.deleted target_info ->
                            Error
                              (Error.Link_to_deleted
                                 {
                                   bucket = Link.bucket link;
                                   name = Name.to_string name;
                                 })
                        | Ok target_info ->
                            loop (Int.add depth 1) (identity :: path) target
                              target_info))))
  in
  let identity = Info.bucket info ^ "/" ^ Name.to_string (Info.name info) in
  loop 0 [ identity ] value info

let get ?timeout ?(include_deleted = false) ?(max_links = 10) value name writer
    =
  if Int.compare max_links 0 < 0 then Error (Error.Invalid_metadata "max_links")
  else
    let deadline = timeout_deadline value timeout in
    match remaining_timeout value deadline with
    | Error error -> Error error
    | Ok info_timeout -> (
        match read_info_raw ?timeout:info_timeout value name with
        | Error error -> Error error
        | Ok info when Info.deleted info ->
            if include_deleted then Error (Error.Deleted info)
            else Error Error.Not_found
        | Ok info -> (
            match resolve_link ~max_links value info deadline with
            | Error error -> Error error
            | Ok (resolved_value, resolved_info) ->
                get_content ~deadline resolved_value resolved_info writer))

let get_string ?timeout ?include_deleted ?max_links value name =
  let buffer = Buffer.create 0 in
  let writer = Bytesrw.Bytes.Writer.of_buffer buffer in
  match get ?timeout ?include_deleted ?max_links value name writer with
  | Error error -> Error error
  | Ok _ -> Ok (Buffer.contents buffer)

let get_file ?timeout ?include_deleted ?max_links value name path =
  protect_file ~operation:"write" (fun () ->
      Eio.Path.with_open_out ~create:(`Or_truncate 0o600) path (fun flow ->
          get ?timeout ?include_deleted ?max_links value name
            (writer_of_flow flow)))

let publish_metadata ~deadline value name wire =
  match encode_wire wire with
  | Error error -> Error error
  | Ok payload -> (
      match remaining_timeout value deadline with
      | Error error -> Error error
      | Ok timeout -> (
          match
            Jetstream.publish ?timeout ~headers:metadata_headers value.jetstream
              (Nats.Subject.literal (metadata_subject value.bucket name))
              payload
          with
          | Ok _ -> Ok ()
          | Error error -> Error (map_jetstream_error error)))

let update ?timeout ?name value meta =
  let deadline = timeout_deadline value timeout in
  let old_name = Option.value name ~default:(Meta.name meta) in
  let new_name = Meta.name meta in
  let same_name =
    String.equal (Name.to_string old_name) (Name.to_string new_name)
  in
  let check_target () =
    if same_name then Ok ()
    else
      match remaining_timeout value deadline with
      | Error error -> Error error
      | Ok timeout -> (
          match read_info_raw ?timeout value new_name with
          | Ok info when not (Info.deleted info) ->
              Error (Error.Object_already_exists info)
          | Ok _ | Error Error.Not_found -> Ok ()
          | Error error -> Error error)
  in
  match remaining_timeout value deadline with
  | Error error -> Error error
  | Ok info_timeout -> (
      match read_info_raw ?timeout:info_timeout value old_name with
      | Error error -> Error error
      | Ok old when Info.deleted old -> Error (Error.Deleted old)
      | Ok old -> (
          let* () = check_target () in
          let digest =
            match Info.link old with
            | Some _ -> None
            | None -> Some (Info.digest old)
          in
          let wire =
            wire_of_object ~bucket:value.bucket ~name:new_name
              ~description:(Meta.description meta) ~headers:(Meta.headers meta)
              ~metadata:(Meta.metadata meta) ~options:(options_of_info old)
              ~nuid:(Info.nuid old) ~size:(Info.size old)
              ~chunks:(Info.chunks old) ~digest ~deleted:false
          in
          let* () = publish_metadata ~deadline value new_name wire in
          let* info_timeout = remaining_timeout value deadline in
          let* info = read_info_raw ?timeout:info_timeout value new_name in
          if same_name then Ok info
          else
            match remaining_timeout value deadline with
            | Error _ ->
                Error
                  (Error.Cleanup_failed
                     {
                       info = Some info;
                       error = Jetstream.Error.Connection Core_error.Timeout;
                     })
            | Ok purge_timeout -> (
                match
                  purge_chunks ?timeout:purge_timeout value
                    (metadata_subject value.bucket old_name)
                with
                | Ok () -> Ok info
                | Error error ->
                    Error (Error.Cleanup_failed { info = Some info; error }))))

let check_link_target ~deadline value link =
  match Link.name link with
  | None -> Ok ()
  | Some name -> (
      match bind value.jetstream ~bucket:(Link.bucket link) with
      | Error error -> Error error
      | Ok target -> (
          match remaining_timeout target deadline with
          | Error error -> Error error
          | Ok timeout -> (
              match read_info_raw ?timeout target name with
              | Error error -> Error error
              | Ok info when Info.deleted info ->
                  Error
                    (Error.Link_to_deleted
                       { bucket = Link.bucket link; name = Name.to_string name })
              | Ok info when Option.is_some (Info.link info) ->
                  Error Error.No_link_to_link
              | Ok _ -> Ok ())))

let put_link ?timeout value meta link =
  let deadline = timeout_deadline value timeout in
  let name = Meta.name meta in
  let existing =
    match remaining_timeout value deadline with
    | Error error -> Error error
    | Ok info_timeout -> (
        match read_info_raw ?timeout:info_timeout value name with
        | Error Error.Not_found -> Ok ()
        | Error error -> Error error
        | Ok old when Option.is_none (Info.link old) ->
            Error (Error.Object_already_exists old)
        | Ok _ -> Ok ())
  in
  match existing with
  | Error error -> Error error
  | Ok () -> (
      match check_link_target ~deadline value link with
      | Error error -> Error error
      | Ok () -> (
          let nuid = new_nuid value in
          let wire =
            wire_of_object ~bucket:value.bucket ~name
              ~description:(Meta.description meta) ~headers:(Meta.headers meta)
              ~metadata:(Meta.metadata meta)
              ~options:
                (Some { chunk_size = None; link = Some (link_to_wire link) })
              ~nuid ~size:0L ~chunks:0L ~digest:None ~deleted:false
          in
          match publish_metadata ~deadline value name wire with
          | Error error -> Error error
          | Ok () -> (
              match remaining_timeout value deadline with
              | Error error -> Error error
              | Ok info_timeout -> (
                  match read_info_raw ?timeout:info_timeout value name with
                  | Error error -> Error error
                  | Ok info -> Ok info))))

let info_of_delivery (value : t) message =
  match
    Jsont_bytesrw.decode_string' wire_info_codec (Jetstream.Msg.payload message)
  with
  | Error error -> Error (Error.Decode error)
  | Ok wire -> (
      match Name.of_string wire.name with
      | Error reason -> Error (Error.Invalid_name { value = wire.name; reason })
      | Ok name ->
          let actual_subject =
            Nats.Subject.to_string (Jetstream.Msg.subject message)
          in
          let expected_subject = metadata_subject value.bucket name in
          if not (String.equal actual_subject expected_subject) then
            Error (Error.Invalid_metadata_subject actual_subject)
          else
            let* timestamp =
              timestamp_of_nanoseconds (Jetstream.Msg.timestamp message)
            in
            info_of_wire ~bucket:value.bucket ~requested_name:name ~timestamp
              wire)

module Watch = struct
  type delivery = New | Last_per_subject | All
  type event = Initial_done | Info of Info.t
  type initial = Marker | Retained | Live

  type t = {
    value : bucket;
    push : Jetstream.Consumer.Push.t;
    connection : Connection.t;
    ignore_deletes : bool;
    mutable initial : initial;
  }

  let map_error = function
    | Jetstream.Error.Connection error -> Error.Connection error
    | Jetstream.Error.Push_closed -> Error.Closed
    | error -> Error.Jetstream error

  let v ~sw ?name ?(delivery = Last_per_subject) ?(ignore_deletes = false)
      (value : bucket) =
    let filter =
      match name with
      | None -> metadata_filter value.bucket
      | Some name ->
          Nats.Subject.Filter.literal (metadata_subject value.bucket name)
    in
    let connection = Jetstream.connection value.jetstream in
    let deliver_subject = Connection.fresh_inbox connection in
    let deliver_policy =
      match delivery with
      | New -> Jetstream.Consumer.Config.New
      | Last_per_subject -> Jetstream.Consumer.Config.Last_per_subject
      | All -> Jetstream.Consumer.Config.All
    in
    match
      Jetstream.Consumer.Config.v ~deliver_subject ~deliver_policy
        ~ack_policy:Jetstream.Consumer.Config.No_ack ~filter_subject:filter
        ~idle_heartbeat:Mtime.Span.(5 * s)
        ~flow_control:true ~headers_only:false
        ~inactive_threshold:Mtime.Span.(5 * min)
        ~mem_storage:true ()
    with
    | Error error ->
        Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
    | Ok config -> (
        match Jetstream.Consumer.Push.create ~sw value.stream config with
        | Error error -> Error (map_error error)
        | Ok push ->
            let initial_pending =
              match delivery with
              | New -> None
              | Last_per_subject | All ->
                  Some (Jetstream.Consumer.Push.initial_pending push)
            in
            let initial =
              match initial_pending with
              | None | Some 0L -> Marker
              | Some _ -> Retained
            in
            Ok { value; push; connection; ignore_deletes; initial })

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
              match info_of_delivery watch.value message with
              | Error error -> result := Some (Error error)
              | Ok info ->
                  let initial_complete =
                    match watch.initial with
                    | Retained ->
                        Int64.equal (Jetstream.Msg.num_pending message) 0L
                    | Marker | Live -> false
                  in
                  if initial_complete then watch.initial <- Marker;
                  if watch.ignore_deletes && Info.deleted info then ()
                  else result := Some (Ok (Info info))))
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

let list ?timeout ?(include_deleted = false) value =
  let deadline = timeout_deadline value timeout in
  Eio.Switch.run (fun sw ->
      match
        Watch.v ~sw ~delivery:Watch.Last_per_subject
          ~ignore_deletes:(not include_deleted) value
      with
      | Error error -> Error error
      | Ok watch ->
          let result = ref None in
          let infos = ref [] in
          let next_event () =
            match remaining_timeout value deadline with
            | Error error -> Error error
            | Ok None -> Watch.next watch
            | Ok (Some timeout) -> Watch.next_with_timeout ~timeout watch
          in
          let collect () =
            while Option.is_none !result do
              match next_event () with
              | Error error -> result := Some (Error error)
              | Ok Watch.Initial_done -> result := Some (Ok (List.rev !infos))
              | Ok (Watch.Info info) -> infos := info :: !infos
            done
          in
          let result =
            Fun.protect
              ~finally:(fun () -> ignore (Watch.close watch))
              (fun () ->
                collect ();
                match !result with
                | Some result -> result
                | None -> assert false)
          in
          result)

let seal value =
  match Jetstream.Stream.info value.stream with
  | Error error -> Error (map_jetstream_error error)
  | Ok stream_info -> (
      match
        Jetstream.Stream.Config.with_sealed
          (Jetstream.Stream.Info.config stream_info)
          true
      with
      | Error error ->
          Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
      | Ok config -> (
          match Jetstream.Stream.update value.stream config with
          | Error error -> Error (map_jetstream_error error)
          | Ok _ -> status value))

let delete ?timeout value name =
  let deadline = timeout_deadline value timeout in
  match remaining_timeout value deadline with
  | Error error -> Error error
  | Ok info_timeout -> (
      match read_info_raw ?timeout:info_timeout value name with
      | Error error -> Error error
      | Ok info when Info.deleted info -> Ok ()
      | Ok info -> (
          let wire =
            wire_of_object ~bucket:value.bucket ~name:(Info.name info)
              ~description:(Info.description info) ~headers:(Info.headers info)
              ~metadata:(Info.metadata info) ~options:(options_of_info info)
              ~nuid:(Info.nuid info) ~size:0L ~chunks:0L ~digest:None
              ~deleted:true
          in
          match encode_wire wire with
          | Error error -> Error error
          | Ok payload -> (
              match remaining_timeout value deadline with
              | Error error -> Error error
              | Ok publish_timeout -> (
                  match
                    Jetstream.publish ?timeout:publish_timeout
                      ~headers:metadata_headers value.jetstream
                      (Nats.Subject.literal
                         (metadata_subject value.bucket name))
                      payload
                  with
                  | Error error -> Error (map_jetstream_error error)
                  | Ok _ -> (
                      if String.equal (Info.nuid info) "" then Ok ()
                      else
                        match remaining_timeout value deadline with
                        | Error _ ->
                            Error
                              (Error.Cleanup_failed
                                 {
                                   info = Some info;
                                   error =
                                     Jetstream.Error.Connection
                                       Core_error.Timeout;
                                 })
                        | Ok purge_timeout -> (
                            match
                              purge_chunks ?timeout:purge_timeout value
                                (chunk_subject value.bucket (Info.nuid info))
                            with
                            | Ok () -> Ok ()
                            | Error error ->
                                Error
                                  (Error.Cleanup_failed
                                     { info = Some info; error })))))))

let manager_update jetstream config =
  match stream_config config with
  | Error error -> Error error
  | Ok stream_config -> (
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
  match stream_config config with
  | Error error -> Error error
  | Ok stream_config -> (
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
  let subject = Nats.Subject.Filter.literal "$O.*.>" in
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
              match manager_bucket_name ~prefix:"OBJ_" name with
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
