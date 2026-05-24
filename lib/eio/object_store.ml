module Core_error = Error

module Error = struct
  type config =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_ttl
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_replicas of int
    | Empty_metadata_key
    | Duplicate_metadata_key of string

  type meta =
    | Invalid_chunk_size of int
    | Duplicate_attribute of string
    | Invalid_link of { bucket : string; name : string option }

  type t =
    | Connection of Connection.error
    | Jetstream of Jetstream.Error.t
    | Invalid_config of config
    | Invalid_meta of meta
    | Invalid_name of string
    | Invalid_headers of Nats.Header.error
    | Invalid_message_subject of string
    | Invalid_timestamp of int64
    | Missing_info_field of string
    | Unexpected_bucket of { expected : string; actual : string }
    | Invalid_digest of string
    | Link_not_allowed
    | Bucket_link_not_readable of string
    | Link_to_deleted of { bucket : string; name : string }
    | Object_exists of { bucket : string; name : string }
    | Update_deleted of { name : string }
    | Object_not_found
    | Object_deleted of { name : string }
    | Incomplete_object of {
        name : string;
        expected_size : int64;
        actual_size : int64;
        expected_chunks : int64;
        actual_chunks : int64;
      }
    | Digest_mismatch of { name : string; expected : string; actual : string }
    | Link_cycle of string list
    | Closed
    | Io of exn

  let pp_config ppf = function
    | Empty_bucket -> Format.pp_print_string ppf "object-store bucket is empty"
    | Invalid_bucket_character { position; character } ->
        Format.fprintf ppf "invalid object-store bucket character %C at position %d"
          character position
    | Invalid_ttl ->
        Format.pp_print_string ppf "object-store TTL must not be negative"
    | Invalid_limit { field; value } ->
        Format.fprintf ppf "invalid object-store %s limit %Ld" field value
    | Invalid_replicas value ->
        Format.fprintf ppf "invalid object-store replica count %d" value
    | Empty_metadata_key ->
        Format.pp_print_string ppf
          "object-store metadata keys must not be empty"
    | Duplicate_metadata_key key ->
        Format.fprintf ppf "object-store metadata repeats key %S" key

  let pp_meta ppf = function
    | Invalid_chunk_size value ->
        Format.fprintf ppf "invalid object-store chunk size %d" value
    | Duplicate_attribute name ->
        Format.fprintf ppf "object metadata repeats attribute %S" name
    | Invalid_link { bucket; name } ->
        Format.fprintf ppf "invalid object link target %S%a" bucket
          (fun ppf -> function
            | None -> ()
            | Some name -> Format.fprintf ppf "/%s" name)
          name

  let pp ppf = function
    | Connection error ->
        Format.fprintf ppf "connection: %a" Core_error.pp error
    | Jetstream error ->
        Format.fprintf ppf "JetStream: %a" Jetstream.Error.pp error
    | Invalid_config error ->
        Format.fprintf ppf "invalid object-store config: %a" pp_config error
    | Invalid_meta error ->
        Format.fprintf ppf "invalid object metadata: %a" pp_meta error
    | Invalid_name value ->
        Format.fprintf ppf "invalid object name %S" value
    | Invalid_headers error ->
        Format.fprintf ppf "invalid object headers: %a" Nats.Header.pp_error
          error
    | Invalid_message_subject value ->
        Format.fprintf ppf "unexpected object-store message subject %S" value
    | Invalid_timestamp value ->
        Format.fprintf ppf "invalid object-store message timestamp %Ld" value
    | Missing_info_field field ->
        Format.fprintf ppf "missing object-info field %S" field
    | Unexpected_bucket { expected; actual } ->
        Format.fprintf ppf "object belongs to bucket %S, expected %S" actual
          expected
    | Invalid_digest value ->
        Format.fprintf ppf "invalid object digest %S" value
    | Link_not_allowed ->
        Format.pp_print_string ppf "object uploads cannot create links"
    | Bucket_link_not_readable bucket ->
        Format.fprintf ppf "bucket link %S cannot be read as an object" bucket
    | Link_to_deleted { bucket; name } ->
        Format.fprintf ppf "link target %S/%S is deleted" bucket name
    | Object_exists { bucket; name } ->
        Format.fprintf ppf "object %S already exists in bucket %S" name bucket
    | Update_deleted { name } ->
        Format.fprintf ppf "cannot update deleted object %S" name
    | Object_not_found -> Format.pp_print_string ppf "object was not found"
    | Object_deleted { name } ->
        Format.fprintf ppf "object %S was deleted" name
    | Incomplete_object
        {
          name;
          expected_size;
          actual_size;
          expected_chunks;
          actual_chunks;
        } ->
        Format.fprintf ppf
          "object %S was incomplete (size %Ld/%Ld, chunks %Ld/%Ld)" name
          actual_size expected_size actual_chunks expected_chunks
    | Digest_mismatch { name; expected; actual } ->
        Format.fprintf ppf "object %S digest mismatch (expected %S, got %S)"
          name expected actual
    | Link_cycle names ->
        Format.fprintf ppf "object link cycle: %s" (String.concat " -> " names)
    | Closed -> Format.pp_print_string ppf "object-store watch is closed"
    | Io error -> Format.fprintf ppf "I/O error: %s" (Printexc.to_string error)
end

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
            invalid := Some (Error.Invalid_bucket_character { position; character })
    done;
    match !invalid with None -> Ok () | Some error -> Error error

let validate_name name =
  if Int.equal (String.length name) 0 then Error (Error.Invalid_name name)
  else Ok ()

module Meta = struct
  type link =
    | Object of { bucket : string; name : string }
    | Bucket of { bucket : string }

  type t = {
    description : string;
    headers : Nats.Header.t;
    attributes : (string * string) list;
    chunk_size : int option;
    link : link option;
  }

  type error = Error.meta

  let duplicate_attribute attributes =
    let seen = ref [] in
    let duplicate = ref None in
    List.iter
      (fun (name, _) ->
        match !duplicate with
        | Some _ -> ()
        | None ->
            if List.exists (String.equal name) !seen then
              duplicate := Some name
            else seen := name :: !seen)
      attributes;
    !duplicate

  let validate_link = function
    | None -> Ok ()
    | Some (Object { bucket; name }) -> (
        match validate_bucket bucket with
        | Error _ -> Error (Error.Invalid_link { bucket; name = Some name })
        | Ok () -> (
            match validate_name name with
            | Error _ -> Error (Error.Invalid_link { bucket; name = Some name })
            | Ok () -> Ok ()))
    | Some (Bucket { bucket }) -> (
        match validate_bucket bucket with
        | Error _ -> Error (Error.Invalid_link { bucket; name = None })
        | Ok () -> Ok ())

  let v ?(description = "") ?(headers = Nats.Header.empty) ?(attributes = [])
      ?chunk_size ?link () =
    match chunk_size with
    | Some value when value <= 0 -> Error (Error.Invalid_chunk_size value)
    | _ -> (
        match duplicate_attribute attributes with
        | Some name -> Error (Error.Duplicate_attribute name)
        | None -> (
            match validate_link link with
            | Error error -> Error error
            | Ok () -> Ok { description; headers; attributes; chunk_size; link }))

  let description value = value.description
  let headers value = value.headers
  let attributes value = value.attributes
  let chunk_size value = value.chunk_size
  let link value = value.link
end

module Info = struct
  type t = {
    name : string;
    bucket : string;
    nuid : string;
    size : int64;
    chunks : int64;
    modified : string;
    digest : string;
    deleted : bool;
    meta : Meta.t;
  }

  let name value = value.name
  let bucket value = value.bucket
  let nuid value = value.nuid
  let size value = value.size
  let chunks value = value.chunks
  let modified value = value.modified
  let digest value = value.digest
  let deleted value = value.deleted
  let meta value = value.meta
  let link value = Meta.link value.meta
  let is_link value = Option.is_some (Meta.link value.meta)

  let pp ppf value =
    Format.fprintf ppf "Object %S in %S (size=%Ld, chunks=%Ld)" value.name
      value.bucket value.size value.chunks
end

module Config = struct
  type storage = Memory | File
  module Placement = Jetstream.Stream.Config.Placement
  type compression = Jetstream.Stream.Config.compression = Off | S2
  type placement = Placement.t

  type t = {
    bucket : string;
    description : string option;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    storage : storage;
    replicas : int;
    placement : placement option;
    compression : compression;
    metadata : (string * string) list;
  }

  type error = Error.config

  let validate_limit field = function
    | None -> Ok ()
    | Some value when Int64.compare value (-1L) >= 0 -> Ok ()
    | Some value -> Error (Error.Invalid_limit { field; value })

  let validate_replicas value =
    if Int.compare value 1 < 0 || Int.compare value 5 > 0 then
      Error (Error.Invalid_replicas value)
    else Ok ()

  let validate_metadata metadata =
    let seen = ref [] in
    let invalid = ref None in
    List.iter
      (fun pair ->
        match !invalid with
        | Some _ -> ()
        | None ->
            let key = fst pair in
            if String.equal key "" then invalid := Some Error.Empty_metadata_key
            else if List.exists (String.equal key) !seen then
              invalid := Some (Error.Duplicate_metadata_key key)
            else seen := key :: !seen)
      metadata;
    match !invalid with None -> Ok () | Some error -> Error error

  let v ~bucket ?description ?ttl ?max_bytes ?(storage = File) ?(replicas = 1)
      ?placement ?(compression = Off) ?(metadata = []) () =
    match validate_bucket bucket with
    | Error error -> Error error
    | Ok () -> (
        match ttl with
        | Some value when Mtime.Span.compare value Mtime.Span.zero < 0 ->
            Error Error.Invalid_ttl
        | _ -> (
            match validate_limit "max_bytes" max_bytes with
            | Error error -> Error error
            | Ok () -> (
                match validate_replicas replicas with
                | Error error -> Error error
                | Ok () -> (
                    match validate_metadata metadata with
                    | Error error -> Error error
                    | Ok () ->
                        let max_bytes =
                          match max_bytes with Some 0L -> None | value -> value
                        in
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
                            max_bytes;
                            storage;
                            replicas;
                            placement;
                            compression;
                            metadata;
                          }))))

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

module Status = struct
  type t = {
    bucket : string;
    description : string option;
    values : int64;
    bytes : int64;
    first_sequence : int64;
    last_sequence : int64;
    consumer_count : int;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    storage : Config.storage;
    replicas : int;
    placement : Config.placement option;
    compression : Config.compression;
    metadata : (string * string) list;
    sealed : bool;
  }

  let config value =
    Config.v ~bucket:value.bucket ?description:value.description ?ttl:value.ttl
      ?max_bytes:value.max_bytes ~storage:value.storage ~replicas:value.replicas
      ?placement:value.placement ~compression:value.compression
      ~metadata:value.metadata ()

  let bucket value = value.bucket
  let description value = value.description
  let values value = value.values
  let bytes value = value.bytes
  let first_sequence value = value.first_sequence
  let last_sequence value = value.last_sequence
  let consumer_count value = value.consumer_count
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

let map_jetstream_error = function
  | Jetstream.Error.Connection error -> Error.Connection error
  | error -> Error.Jetstream error

let map_config_error error = Error.Invalid_config error
let bucket value = value.bucket

let stream_name bucket = "OBJ_" ^ bucket
let chunk_prefix bucket = "$O." ^ bucket ^ ".C."
let meta_prefix bucket = "$O." ^ bucket ^ ".M."
let object_stream_name_prefix = "OBJ_"

let object_stream_subjects bucket =
  [ chunk_prefix bucket ^ ">"; meta_prefix bucket ^ ">" ]

let has_subject expected subjects =
  List.exists
    (fun subject ->
      String.equal expected (Nats.Subject.Filter.to_string subject))
    subjects

let bucket_of_stream_info info =
  let config = Jetstream.Stream.Info.config info in
  let name = Jetstream.Stream.Config.name config in
  let prefix_length = String.length object_stream_name_prefix in
  if String.length name <= prefix_length then None
  else if
    not
      (String.equal object_stream_name_prefix (String.sub name 0 prefix_length))
  then None
  else
    let bucket =
      String.sub name prefix_length (String.length name - prefix_length)
    in
    match validate_bucket bucket with
    | Error _ -> None
    | Ok () ->
        let subjects = Jetstream.Stream.Config.subjects config in
        let required = object_stream_subjects bucket in
        if List.for_all (fun subject -> has_subject subject subjects) required
        then Some bucket
        else None

let encoded_name name =
  Base64.encode_string ~pad:true ~alphabet:Base64.uri_safe_alphabet name

let meta_subject value name =
  Nats.Subject.literal (meta_prefix value.bucket ^ encoded_name name)

let chunk_subject value nuid =
  Nats.Subject.literal (chunk_prefix value.bucket ^ nuid)

let stream_for_config config jetstream =
  let chunk_filter =
    Nats.Subject.Filter.literal (chunk_prefix (Config.bucket config) ^ ">")
  in
  let meta_filter =
    Nats.Subject.Filter.literal (meta_prefix (Config.bucket config) ^ ">")
  in
  let storage =
    match Config.storage config with
    | Config.Memory -> Jetstream.Stream.Config.Memory
    | Config.File -> Jetstream.Stream.Config.File
  in
  match
    Jetstream.Stream.Config.v ~name:(stream_name (Config.bucket config))
      ~subjects:[ chunk_filter; meta_filter ]
      ?description:(Config.description config) ~storage
      ~retention:Jetstream.Stream.Config.Limits
      ~discard:Jetstream.Stream.Config.New ?max_bytes:(Config.max_bytes config)
      ?max_age:(Config.ttl config) ~replicas:(Config.replicas config)
      ?placement:(Config.placement config)
      ~compression:(Config.compression config)
      ~metadata:(Config.metadata config) ~allow_rollup:true ~allow_direct:true
      ()
  with
  | Error error -> Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
  | Ok value -> Ok value

let create jetstream config =
  match stream_for_config config jetstream with
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

let status_of_stream_info ~bucket info =
  let config = Jetstream.Stream.Info.config info in
  let storage =
    match Jetstream.Stream.Config.storage config with
    | Jetstream.Stream.Config.Memory -> Config.Memory
    | Jetstream.Stream.Config.File -> Config.File
  in
  {
    Status.bucket;
    description = Jetstream.Stream.Config.description config;
    values = Jetstream.Stream.Info.messages info;
    bytes = Jetstream.Stream.Info.bytes info;
    first_sequence = Jetstream.Stream.Info.first_sequence info;
    last_sequence = Jetstream.Stream.Info.last_sequence info;
    consumer_count = Jetstream.Stream.Info.consumer_count info;
    ttl = Jetstream.Stream.Config.max_age config;
    max_bytes = Jetstream.Stream.Config.max_bytes config;
    storage;
    replicas = Jetstream.Stream.Config.replicas config;
    placement = Jetstream.Stream.Config.placement config;
    compression = Jetstream.Stream.Config.compression config;
    metadata = Jetstream.Stream.Config.metadata config;
    sealed = Jetstream.Stream.Info.sealed info;
  }

let status value =
  match Jetstream.Stream.info value.stream with
  | Error error -> Error (map_jetstream_error error)
  | Ok info -> Ok (status_of_stream_info ~bucket:value.bucket info)

let stream_config_for_update ~current config =
  let result =
    let ( let* ) value f =
      match value with Error error -> Error error | Ok value -> f value
    in
    let* value =
      Jetstream.Stream.Config.with_description current
        (Config.description config)
    in
    let* value =
      Jetstream.Stream.Config.with_storage value
        (match Config.storage config with
        | Config.Memory -> Jetstream.Stream.Config.Memory
        | Config.File -> Jetstream.Stream.Config.File)
    in
    let* value =
      Jetstream.Stream.Config.with_max_bytes value (Config.max_bytes config)
    in
    let* value =
      Jetstream.Stream.Config.with_max_age value (Config.ttl config)
    in
    let* value =
      Jetstream.Stream.Config.with_replicas value (Config.replicas config)
    in
    let* value =
      Jetstream.Stream.Config.with_placement value (Config.placement config)
    in
    let* value =
      Jetstream.Stream.Config.with_compression value (Config.compression config)
    in
    let* value =
      Jetstream.Stream.Config.with_metadata value (Config.metadata config)
    in
    Ok value
  in
  match result with
  | Ok value -> Ok value
  | Error error ->
      Error (Error.Jetstream (Jetstream.Error.Invalid_config error))

let update value config =
  if not (String.equal value.bucket (Config.bucket config)) then
    Error
      (Error.Unexpected_bucket
         { expected = value.bucket; actual = Config.bucket config })
  else
    match Jetstream.Stream.info value.stream with
    | Error error -> Error (map_jetstream_error error)
    | Ok current -> (
        match
          stream_config_for_update
            ~current:(Jetstream.Stream.Info.config current)
            config
        with
        | Error error -> Error error
        | Ok stream_config -> (
            match Jetstream.Stream.update value.stream stream_config with
            | Error error -> Error (map_jetstream_error error)
            | Ok info -> Ok (status_of_stream_info ~bucket:value.bucket info)))

let list_buckets jetstream =
  let subject = Nats.Subject.Filter.literal "$O.>" in
  match Jetstream.Stream.list ~subject jetstream with
  | Error error -> Error (map_jetstream_error error)
  | Ok infos ->
      let statuses = ref [] in
      List.iter
        (fun info ->
          match bucket_of_stream_info info with
          | None -> ()
          | Some bucket ->
              statuses := status_of_stream_info ~bucket info :: !statuses)
        infos;
      Ok (List.rev !statuses)

type wire_link = { bucket : string; name : string option }

let wire_link_codec =
  Jsont.Object.map ~kind:"NATS object link"
    (fun bucket name -> { bucket; name })
  |> Jsont.Object.mem "bucket" Jsont.string ~enc:(fun value -> value.bucket)
  |> Jsont.Object.opt_mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

type wire_options = { link : wire_link option; chunk_size : int option }

let wire_options_codec =
  Jsont.Object.map ~kind:"NATS object options"
    (fun link chunk_size -> { link; chunk_size })
  |> Jsont.Object.opt_mem "link" wire_link_codec ~enc:(fun value -> value.link)
  |> Jsont.Object.opt_mem "max_chunk_size" Jsont.int ~enc:(fun value ->
      value.chunk_size)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

type wire_info = {
  name : string;
  description : string option;
  headers : Jsont.json option;
  attributes : Jsont.json option;
  options : wire_options option;
  bucket : string;
  nuid : string;
  size : int64;
  modified : string option;
  chunks : int64;
  digest : string option;
  deleted : bool option;
}

let wire_info_codec =
  Jsont.Object.map ~kind:"NATS object info"
    (fun name description headers attributes options bucket nuid size modified
        chunks digest deleted ->
      {
        name;
        description;
        headers;
        attributes;
        options;
        bucket;
        nuid;
        size;
        modified;
        chunks;
        digest;
        deleted;
      })
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.opt_mem "description" Jsont.string ~enc:(fun value ->
      value.description)
  |> Jsont.Object.opt_mem "headers" Jsont.json ~enc:(fun value -> value.headers)
  |> Jsont.Object.opt_mem "metadata" Jsont.json ~enc:(fun value ->
      value.attributes)
  |> Jsont.Object.opt_mem "options" wire_options_codec ~enc:(fun value ->
      value.options)
  |> Jsont.Object.mem "bucket" Jsont.string ~enc:(fun value -> value.bucket)
  |> Jsont.Object.mem "nuid" Jsont.string ~enc:(fun value -> value.nuid)
  |> Jsont.Object.mem "size" Jsont.int64 ~enc:(fun value -> value.size)
  |> Jsont.Object.opt_mem "mtime" Jsont.string ~enc:(fun value ->
      value.modified)
  |> Jsont.Object.mem "chunks" Jsont.int64 ~enc:(fun value -> value.chunks)
  |> Jsont.Object.opt_mem "digest" Jsont.string ~enc:(fun value -> value.digest)
  |> Jsont.Object.opt_mem "deleted" Jsont.bool ~enc:(fun value -> value.deleted)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let encode codec value =
  match Jsont_bytesrw.encode_string' codec value with
  | Ok value -> Ok value
  | Error error -> Error (Error.Jetstream (Jetstream.Error.Encode error))

let decode codec value =
  match Jsont_bytesrw.decode_string' codec value with
  | Ok value -> Ok value
  | Error error -> Error (Error.Jetstream (Jetstream.Error.Decode error))

let json_string_object pairs =
  Jsont.Json.object'
    (List.map
       (fun (name, value) -> Jsont.Json.mem (Jsont.Json.name name) (Jsont.Json.string value))
       pairs)

let json_header_object pairs =
  Jsont.Json.object'
    (List.map
       (fun (name, values) ->
         Jsont.Json.mem (Jsont.Json.name name)
           (Jsont.Json.list (List.map Jsont.Json.string values)))
       pairs)

let string_object ~field = function
  | Jsont.Object (members, _) ->
      let values = ref [] in
      let failure = ref None in
      List.iter
        (fun (name, value) ->
          match !failure with
          | Some _ -> ()
          | None -> (
              match value with
              | Jsont.String (value, _) ->
                  let name = fst name in
                  if List.exists (fun (known, _) -> String.equal known name) !values
                  then failure := Some (Error.Invalid_meta (Error.Duplicate_attribute name))
                  else values := (name, value) :: !values
              | _ -> failure := Some (Error.Missing_info_field field)))
        members;
      (match !failure with
      | Some error -> Error error
      | None -> Ok (List.rev !values))
  | _ -> Error (Error.Missing_info_field field)

let header_object ~field = function
  | Jsont.Object (members, _) ->
      let values = ref [] in
      let failure = ref None in
      List.iter
        (fun (name, value) ->
          match !failure with
          | Some _ -> ()
          | None -> (
              match value with
              | Jsont.Array (items, _) ->
                  let result = ref [] in
                  List.iter
                    (fun item ->
                      match (!failure, item) with
                      | Some _, _ -> ()
                      | None, Jsont.String (value, _) ->
                          result := value :: !result
                      | None, _ ->
                          failure := Some (Error.Missing_info_field field))
                    items;
                  (match !failure with
                  | Some _ -> ()
                  | None -> values := (fst name, List.rev !result) :: !values)
              | _ -> failure := Some (Error.Missing_info_field field)))
        members;
      (match !failure with
      | Some error -> Error error
      | None -> Ok (List.rev !values))
  | _ -> Error (Error.Missing_info_field field)

let link_of_wire (value : wire_link) =
  match value.name with
  | Some "" | None -> Meta.Bucket { bucket = value.bucket }
  | Some name -> Meta.Object { bucket = value.bucket; name }

let wire_link_of_link = function
  | Meta.Object { bucket; name } -> { bucket; name = Some name }
  | Meta.Bucket { bucket } -> { bucket; name = None }

let meta_of_wire ~description ~headers ~attributes ~options =
  let headers =
    match headers with
    | None -> Ok Nats.Header.empty
    | Some value -> (
        match header_object ~field:"headers" value with
        | Error error -> Error error
        | Ok values -> (
            let values =
              List.concat_map
                (fun (name, values) ->
                  List.map (fun value -> (name, value)) values)
                values
            in
            match Nats.Header.of_list values with
            | Ok headers -> Ok headers
            | Error error -> Error (Error.Invalid_headers error)))
  in
  match headers with
  | Error error -> Error error
  | Ok headers -> (
      let attributes =
        match attributes with
        | None -> Ok []
        | Some value -> string_object ~field:"metadata" value
      in
      match attributes with
      | Error error -> Error error
      | Ok attributes ->
          let description = Option.value ~default:"" description in
          let link, chunk_size =
            match options with
            | None -> (None, None)
            | Some { link; chunk_size } -> (Option.map link_of_wire link, chunk_size)
          in
          (match Meta.v ~description ~headers ~attributes ?chunk_size ?link () with
          | Ok value -> Ok value
          | Error error -> Error (Error.Invalid_meta error)))

let wire_info_of_info value =
  let meta = Info.meta value in
  let options =
    Some
      {
        link = Option.map wire_link_of_link (Meta.link meta);
        chunk_size = Meta.chunk_size meta;
      }
  in
  {
    name = Info.name value;
    description = Some (Meta.description meta);
    headers =
      Some
        (json_header_object
           (List.fold_right
              (fun (name, value) result ->
                match result with
                | (known, values) :: tail when String.equal known name ->
                    (known, value :: values) :: tail
                | _ -> (name, [ value ]) :: result)
              (Nats.Header.to_list (Meta.headers meta)) []));
    attributes = Some (json_string_object (Meta.attributes meta));
    options;
    bucket = Info.bucket value;
    nuid = Info.nuid value;
    size = Info.size value;
    modified = Some (Info.modified value);
    chunks = Info.chunks value;
    digest = Some (Info.digest value);
    deleted = Some (Info.deleted value);
  }

let info_of_wire value =
  match meta_of_wire ~description:value.description ~headers:value.headers
          ~attributes:value.attributes ~options:value.options with
  | Error error -> Error error
  | Ok meta ->
      Ok
        {
          Info.name = value.name;
          bucket = value.bucket;
          nuid = value.nuid;
          size = value.size;
          chunks = value.chunks;
          modified = Option.value ~default:"" value.modified;
          digest = Option.value ~default:"" value.digest;
          deleted = Option.value ~default:false value.deleted;
          meta;
        }

let encode_info value = encode wire_info_codec (wire_info_of_info value)

let decode_info message =
  match decode wire_info_codec (Jetstream.Stream.Message.payload message) with
  | Error error -> Error error
  | Ok value -> info_of_wire value

let get_info ?(show_deleted = false) value ~name =
  match validate_name name with
  | Error error -> Error error
  | Ok () -> (
      match Jetstream.Stream.get_last value.stream ~subject:(meta_subject value name) with
      | Error Jetstream.Error.Message_not_found -> Error Error.Object_not_found
      | Error error -> Error (map_jetstream_error error)
      | Ok message -> (
          match decode_info message with
          | Error error -> Error error
          | Ok info ->
              if not (String.equal (Info.bucket info) value.bucket) then
                Error
                  (Error.Unexpected_bucket
                     { expected = value.bucket; actual = Info.bucket info })
              else if not (String.equal (Info.name info) name) then
                Error (Error.Invalid_name (Info.name info))
              else if
                not
                  (Nats.Subject.equal
                     (Jetstream.Stream.Message.subject message)
                     (meta_subject value name))
              then
                Error
                  (Error.Invalid_message_subject
                     (Nats.Subject.to_string
                        (Jetstream.Stream.Message.subject message)))
              else if Info.deleted info && not show_deleted then
                Error (Error.Object_not_found)
              else
                Ok
                  {
                    info with
                    Info.modified = Jetstream.Stream.Message.timestamp message;
                  }))

let default_chunk_size = 128 * 1024
let digest_prefix = "SHA-256="
let nuid_alphabet =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

let digest_of_context context =
  let raw = Digestif.SHA256.to_raw_string (Digestif.SHA256.get context) in
  digest_prefix
  ^ Base64.encode_string ~pad:true ~alphabet:Base64.uri_safe_alphabet raw

let fresh_nuid value =
  let seed =
    Nats.Subject.to_string
      (Connection.fresh_inbox (Jetstream.connection value.jetstream))
  in
  let context =
    Digestif.SHA256.feed_string (Digestif.SHA256.init ()) seed
  in
  let raw = Digestif.SHA256.to_raw_string (Digestif.SHA256.get context) in
  let nuid = Bytes.create 22 in
  for position = 0 to Bytes.length nuid - 1 do
    let index = Char.code (String.get raw position) mod String.length nuid_alphabet in
    Bytes.set nuid position (String.get nuid_alphabet index)
  done;
  Bytes.to_string nuid

let rollup_headers () =
  match Nats.Header.of_list [ ("Nats-Rollup", "sub") ] with
  | Ok headers -> Ok headers
  | Error error -> Error (Error.Invalid_headers error)

let purge_chunks value nuid =
  let subject = Nats.Subject.Filter.literal (Nats.Subject.to_string (chunk_subject value nuid)) in
  match Jetstream.Stream.purge value.stream ~filter:subject with
  | Ok () -> Ok ()
  | Error error -> Error (map_jetstream_error error)

let protect_purge_chunks value nuid =
  Eio.Cancel.protect (fun () -> ignore (purge_chunks value nuid))

let meta_with_chunk_size meta chunk_size =
  match
    Meta.v ~description:(Meta.description meta) ~headers:(Meta.headers meta)
      ~attributes:(Meta.attributes meta) ~chunk_size ()
  with
  | Ok value -> Ok value
  | Error error -> Error (Error.Invalid_meta error)

let effective_chunk_size explicit meta =
  match explicit with
  | Some value when value <= 0 -> Error (Error.Invalid_meta (Error.Invalid_chunk_size value))
  | Some value -> Ok value
  | None -> Ok (Option.value ~default:default_chunk_size (Meta.chunk_size meta))

let make_info ~bucket ~name ~nuid ~size ~chunks ~digest ~meta =
  {
    Info.name;
    bucket;
    nuid;
    size;
    chunks;
    modified = "";
    digest;
    deleted = false;
    meta;
  }

let publish_info value info =
  match encode_info info with
  | Error error -> Error error
  | Ok payload -> (
      match rollup_headers () with
      | Error error -> Error error
      | Ok headers -> (
          match
            Jetstream.publish ~headers value.jetstream (meta_subject value (Info.name info))
              payload
          with
          | Ok _ -> Ok ()
          | Error error -> Error (map_jetstream_error error)))

let upload value ~name ~meta ~chunk_size ~source ~previous =
  let nuid = fresh_nuid value in
  let chunk_subject = chunk_subject value nuid in
  let digest = ref (Digestif.SHA256.init ()) in
  let size = ref 0L in
  let chunks = ref 0L in
  let metadata_published = ref false in
  let cleanup () =
    if not !metadata_published then protect_purge_chunks value nuid
  in
  let result =
    Fun.protect ~finally:cleanup (fun () ->
        let buffer = Cstruct.create chunk_size in
        let eof = ref false in
        let failure = ref None in
        while not !eof && Option.is_none !failure do
          try
            let count = Eio.Flow.single_read source buffer in
            if count > 0 then
              let payload = Cstruct.to_string (Cstruct.sub buffer 0 count) in
              match Jetstream.publish value.jetstream chunk_subject payload with
              | Error error -> failure := Some (map_jetstream_error error)
              | Ok _ ->
                  digest := Digestif.SHA256.feed_string !digest payload;
                  size := Int64.add !size (Int64.of_int count);
                  chunks := Int64.add !chunks 1L
          with
          | End_of_file -> eof := true
          | Eio.Io _ as error -> failure := Some (Error.Io error)
        done;
        match !failure with
        | Some error -> Error error
        | None ->
            let info =
              make_info ~bucket:value.bucket ~name ~nuid ~size:!size
                ~chunks:!chunks ~digest:(digest_of_context !digest) ~meta
            in
            match publish_info value info with
            | Error error -> Error error
            | Ok () ->
                metadata_published := true;
                Ok ())
  in
  match result with
  | Error error -> Error error
  | Ok () -> (
      match previous with
      | Some previous
        when not (Info.deleted previous)
             && not (String.equal (Info.nuid previous) nuid) ->
          purge_chunks value (Info.nuid previous)
      | _ -> Ok ())

let put ?chunk_size value ~name ?meta ~source () =
  match validate_name name with
  | Error error -> Error error
  | Ok () -> (
      let meta =
        match meta with
        | Some value -> Ok value
        | None -> (
            match Meta.v () with
            | Ok value -> Ok value
            | Error error -> Error (Error.Invalid_meta error))
      in
      match meta with
      | Error error -> Error error
      | Ok meta when Option.is_some (Meta.link meta) -> Error Error.Link_not_allowed
      | Ok meta -> (
          match effective_chunk_size chunk_size meta with
          | Error error -> Error error
          | Ok chunk_size -> (
              match meta_with_chunk_size meta chunk_size with
              | Error error -> Error error
              | Ok meta -> (
                  let previous = get_info ~show_deleted:true value ~name in
                  match previous with
                  | Error Error.Object_not_found ->
                      (match upload value ~name ~meta ~chunk_size ~source ~previous:None with
                      | Error error -> Error error
                      | Ok () -> get_info value ~name)
                  | Error error -> Error error
                  | Ok previous -> (
                      match upload value ~name ~meta ~chunk_size ~source ~previous:(Some previous) with
                      | Error error -> Error error
                      | Ok () -> get_info value ~name)))))

let put_string ?chunk_size value ~name ?meta payload =
  put ?chunk_size value ~name ?meta ~source:(Eio.Flow.string_source payload) ()

let valid_digest value =
  let prefix_length = String.length digest_prefix in
  String.length value >= prefix_length
  && String.equal (String.sub value 0 prefix_length) digest_prefix

let resolve_info value info =
  let rec loop value info seen =
    match Meta.link (Info.meta info) with
    | None -> Ok (value, info)
    | Some (Meta.Bucket { bucket }) -> Error (Error.Bucket_link_not_readable bucket)
    | Some (Meta.Object { bucket; name }) ->
        let identity = bucket ^ "/" ^ name in
        if List.exists (String.equal identity) seen then
          Error (Error.Link_cycle (List.rev (identity :: seen)))
        else (
          match bind value.jetstream ~bucket with
          | Error error -> Error error
          | Ok target -> (
              match get_info ~show_deleted:true target ~name with
              | Error error -> Error error
              | Ok target_info when Info.deleted target_info ->
                  Error (Error.Link_to_deleted { bucket; name })
              | Ok target_info -> loop target target_info (identity :: seen)))
  in
  loop value info [ Info.bucket info ^ "/" ^ Info.name info ]

let stream_object ~sw value info ~sink =
  if not (valid_digest (Info.digest info)) then
    Error (Error.Invalid_digest (Info.digest info))
  else if Int64.equal (Info.chunks info) 0L then
    let actual_digest = digest_of_context (Digestif.SHA256.init ()) in
    if not (Int64.equal (Info.size info) 0L) then
      Error
        (Error.Incomplete_object
           {
             name = Info.name info;
             expected_size = Info.size info;
             actual_size = 0L;
             expected_chunks = Info.chunks info;
             actual_chunks = 0L;
           })
    else if not (String.equal actual_digest (Info.digest info)) then
      Error
        (Error.Digest_mismatch
           { name = Info.name info; expected = Info.digest info; actual = actual_digest })
    else Ok ()
  else
    let filter =
      Nats.Subject.Filter.literal
        (Nats.Subject.to_string (chunk_subject value (Info.nuid info)))
    in
    match
      Jetstream.Consumer.Ordered.v ~sw ~batch:1
        ~expires:Mtime.Span.(30 * s) ~idle_heartbeat:Mtime.Span.(5 * s)
        ~filter_subject:filter value.stream
    with
    | Error error -> Error (map_jetstream_error error)
    | Ok ordered ->
        let actual_size = ref 0L in
        let actual_chunks = ref 0L in
        let digest = ref (Digestif.SHA256.init ()) in
        let result = ref None in
        let finish result =
          ignore
            (Eio.Cancel.protect (fun () ->
                 match Jetstream.Consumer.Ordered.close ordered with
                 | Ok () -> ()
                 | Error _ -> ()))
        in
        (try
           while Option.is_none !result
                 && Int64.compare !actual_chunks (Info.chunks info) < 0 do
             match Jetstream.Consumer.Ordered.next ordered with
             | Error (Jetstream.Error.Connection Core_error.Timeout) ->
                 result :=
                   Some
                     (Error
                        (Error.Incomplete_object
                           {
                             name = Info.name info;
                             expected_size = Info.size info;
                             actual_size = !actual_size;
                             expected_chunks = Info.chunks info;
                             actual_chunks = !actual_chunks;
                           }))
             | Error error -> result := Some (Error (map_jetstream_error error))
             | Ok message ->
                 if not (Nats.Subject.equal (Jetstream.Msg.subject message)
                           (chunk_subject value (Info.nuid info)))
                 then
                   result :=
                     Some
                       (Error
                          (Error.Invalid_message_subject
                             (Nats.Subject.to_string (Jetstream.Msg.subject message))))
                 else
                   let payload = Jetstream.Msg.payload message in
                   Eio.Flow.copy_string payload sink;
                   digest := Digestif.SHA256.feed_string !digest payload;
                   actual_size :=
                     Int64.add !actual_size (Int64.of_int (String.length payload));
                   actual_chunks := Int64.add !actual_chunks 1L
           done;
           if Option.is_none !result then
             if not (Int64.equal !actual_size (Info.size info)) then
               result :=
                 Some
                   (Error
                      (Error.Incomplete_object
                         {
                           name = Info.name info;
                           expected_size = Info.size info;
                           actual_size = !actual_size;
                           expected_chunks = Info.chunks info;
                           actual_chunks = !actual_chunks;
                         }))
             else
               let actual_digest = digest_of_context !digest in
               if not (String.equal actual_digest (Info.digest info)) then
                 result :=
                   Some
                     (Error
                        (Error.Digest_mismatch
                           {
                             name = Info.name info;
                             expected = Info.digest info;
                             actual = actual_digest;
                           }))
               else result := Some (Ok ())
         with Eio.Io _ as error -> result := Some (Error (Error.Io error)));
        finish !result;
        match !result with Some result -> result | None -> assert false

let get ~sw ?(show_deleted = false) value ~name ~sink =
  match get_info ~show_deleted value ~name with
  | Error error -> Error error
  | Ok info when Info.deleted info -> Error (Error.Object_deleted { name })
  | Ok info -> (
      match resolve_info value info with
      | Error error -> Error error
      | Ok (value, info) -> (
          match stream_object ~sw value info ~sink with
          | Error error -> Error error
          | Ok () -> Ok info))

let get_string ~sw ?show_deleted value ~name =
  let buffer = Buffer.create 4096 in
  match get ~sw ?show_deleted value ~name ~sink:(Eio.Flow.buffer_sink buffer) with
  | Error error -> Error error
  | Ok _ -> Ok (Buffer.contents buffer)

let timestamp_of_nanoseconds value =
  let billion = 1_000_000_000L in
  let seconds = Int64.div value billion in
  let fraction = Int64.rem value billion in
  let seconds, fraction =
    if Int64.compare fraction 0L < 0 then
      (Int64.sub seconds 1L, Int64.add fraction billion)
    else (seconds, fraction)
  in
  try
    let time = Unix.gmtime (Int64.to_float seconds) in
    Ok
      (Format.asprintf "%04d-%02d-%02dT%02d:%02d:%02d.%09LdZ"
         (time.Unix.tm_year + 1900) (time.Unix.tm_mon + 1) time.Unix.tm_mday
         time.Unix.tm_hour time.Unix.tm_min time.Unix.tm_sec fraction)
  with Unix.Unix_error _ | Invalid_argument _ -> Error (Error.Invalid_timestamp value)

let info_of_delivery (value : t) message =
  let subject = Nats.Subject.to_string (Jetstream.Msg.subject message) in
  let prefix = meta_prefix value.bucket in
  let prefix_length = String.length prefix in
  if String.length subject <= prefix_length
     || not (String.equal prefix (String.sub subject 0 prefix_length))
  then Error (Error.Invalid_message_subject subject)
  else
    match decode wire_info_codec (Jetstream.Msg.payload message) with
    | Error error -> Error error
    | Ok wire -> (
        match info_of_wire wire with
        | Error error -> Error error
        | Ok info -> (
            if not (String.equal (Info.bucket info) value.bucket) then
              Error
                (Error.Unexpected_bucket
                   { expected = value.bucket; actual = Info.bucket info })
            else if
              not
                (Nats.Subject.equal
                   (Jetstream.Msg.subject message)
                   (meta_subject value (Info.name info)))
            then
              Error (Error.Invalid_message_subject subject)
            else
              match timestamp_of_nanoseconds (Jetstream.Msg.timestamp message) with
              | Error error -> Error error
              | Ok modified -> Ok { info with modified = modified }))

let one_shot_config filter =
  match
    Jetstream.Consumer.Config.v
      ~deliver_policy:Jetstream.Consumer.Config.Last_per_subject
      ~ack_policy:Jetstream.Consumer.Config.No_ack ~filter_subject:filter
      ~inactive_threshold:Mtime.Span.(5 * min) ~mem_storage:true ()
  with
  | Ok config -> Ok config
  | Error error -> Error (Error.Jetstream (Jetstream.Error.Invalid_config error))

let with_temporary_consumer (value : t) config f =
  match Jetstream.Consumer.create value.stream config with
  | Error error -> Error (map_jetstream_error error)
  | Ok consumer ->
      Fun.protect
        ~finally:(fun () ->
          ignore
            (Eio.Cancel.protect (fun () ->
                 ignore (Jetstream.Consumer.delete consumer))))
        (fun () -> f consumer)

let drain_messages consumer =
  let messages = ref [] in
  let empty_fetches = ref 0 in
  let result = ref None in
  while Option.is_none !result do
    match
      Jetstream.Consumer.fetch ~expires:Mtime.Span.(1 * s) consumer ~batch:256
    with
    | Error error -> result := Some (Error (map_jetstream_error error))
    | Ok batch ->
        messages := List.rev_append batch !messages;
        let pending =
          List.fold_left
            (fun value message -> Some (Jetstream.Msg.num_pending message))
            None batch
        in
        if
          match pending with Some value -> Int64.equal value 0L | None -> false
        then result := Some (Ok (List.rev !messages))
        else
          match Jetstream.Consumer.info consumer with
          | Error error -> result := Some (Error (map_jetstream_error error))
          | Ok info when Int64.equal (Jetstream.Consumer.Info.num_pending info) 0L
            ->
              result := Some (Ok (List.rev !messages))
          | Ok _ ->
              if List.length batch = 0 then incr empty_fetches
              else empty_fetches := 0;
              if !empty_fetches >= 3 then
                result := Some (Error (Error.Connection Core_error.Timeout))
  done;
  match !result with Some result -> result | None -> assert false

let list ?(show_deleted = false) (value : t) =
  let filter =
    Nats.Subject.Filter.literal (meta_prefix value.bucket ^ ">")
  in
  match one_shot_config filter with
  | Error error -> Error error
  | Ok config -> (
      match with_temporary_consumer value config (fun consumer ->
          match Jetstream.Consumer.info consumer with
          | Error error -> Error (map_jetstream_error error)
          | Ok info when Int64.equal (Jetstream.Consumer.Info.num_pending info) 0L
            -> Ok []
          | Ok _ -> drain_messages consumer)
      with
      | Error error -> Error error
      | Ok messages ->
          let result = ref (Ok []) in
          List.iter
            (fun message ->
              match !result with
              | Error _ -> ()
              | Ok infos -> (
                  match info_of_delivery value message with
                  | Error error -> result := Error error
                  | Ok info when Info.deleted info && not show_deleted -> ()
                  | Ok info -> result := Ok (info :: infos)))
            messages;
          match !result with
          | Error error -> Error error
          | Ok infos -> Ok (List.rev infos))

let delete value ~name =
  match get_info ~show_deleted:true value ~name with
  | Error error -> Error error
  | Ok info when Info.deleted info -> Ok ()
  | Ok info ->
      let deleted =
        { info with Info.size = 0L; chunks = 0L; digest = ""; deleted = true }
      in
      (match publish_info value deleted with
      | Error error -> Error error
      | Ok () -> purge_chunks value (Info.nuid info))

let update_meta value ~name meta =
  match get_info ~show_deleted:true value ~name with
  | Error error -> Error error
  | Ok info when Info.deleted info -> Error (Error.Update_deleted { name })
  | Ok info -> (
      match
        Meta.v ~description:(Meta.description meta) ~headers:(Meta.headers meta)
          ~attributes:(Meta.attributes meta) ?chunk_size:(Meta.chunk_size (Info.meta info))
          ?link:(Meta.link (Info.meta info)) ()
      with
      | Error error -> Error (Error.Invalid_meta error)
      | Ok meta ->
          let updated = { info with Info.meta } in
          match publish_info value updated with
          | Error error -> Error error
          | Ok () -> get_info value ~name)

let link value ~name ~target =
  match validate_name name with
  | Error error -> Error error
  | Ok () when Info.deleted target ->
      Error
        (Error.Link_to_deleted
           { bucket = Info.bucket target; name = Info.name target })
  | Ok () when Info.is_link target -> Error Error.Link_not_allowed
  | Ok () -> (
      match get_info ~show_deleted:true value ~name with
      | Ok existing when not (Info.deleted existing) ->
          Error (Error.Object_exists { bucket = value.bucket; name })
      | Error Error.Object_not_found | Ok _ -> (
          match
            Meta.v ~description:(Meta.description (Info.meta target))
              ~headers:(Meta.headers (Info.meta target))
              ~attributes:(Meta.attributes (Info.meta target))
              ~link:(Meta.Object { bucket = Info.bucket target; name = Info.name target })
              ()
          with
          | Error error -> Error (Error.Invalid_meta error)
          | Ok meta ->
              let info =
                make_info ~bucket:value.bucket ~name ~nuid:(fresh_nuid value)
                  ~size:0L ~chunks:0L ~digest:"" ~meta
              in
              (match publish_info value info with
              | Error error -> Error error
              | Ok () -> get_info value ~name))
      | Error error -> Error error)

let link_bucket (value : t) ~name ~(bucket : t) =
  match validate_name name with
  | Error error -> Error error
  | Ok () -> (
      match get_info ~show_deleted:true value ~name with
      | Ok existing when not (Info.deleted existing) ->
          Error (Error.Object_exists { bucket = value.bucket; name })
      | Error Error.Object_not_found | Ok _ -> (
          match Meta.v ~link:(Meta.Bucket { bucket = bucket.bucket }) () with
          | Error error -> Error (Error.Invalid_meta error)
          | Ok meta ->
              let info =
                make_info ~bucket:value.bucket ~name ~nuid:(fresh_nuid value)
                  ~size:0L ~chunks:0L ~digest:"" ~meta
              in
              (match publish_info value info with
              | Error error -> Error error
              | Ok () -> get_info value ~name))
      | Error error -> Error error)

let seal value =
  match Jetstream.Stream.info value.stream with
  | Error error -> Error (map_jetstream_error error)
  | Ok info -> (
      match Jetstream.Stream.Config.with_sealed
              (Jetstream.Stream.Info.config info) true with
      | Error error -> Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
      | Ok config -> (
          match Jetstream.Stream.update value.stream config with
          | Ok _ -> Ok ()
          | Error error -> Error (map_jetstream_error error)))

type bucket = t

module Watch = struct
  type delivery = New | Last_per_subject | All
  type event = Initial_done | Info of Info.t
  type initial = Before | Marker_pending | Live
  type state = Open | Closed | Failed of Error.t

  type t = {
    bucket : bucket;
    ordered : Jetstream.Consumer.Ordered.t;
    ignore_deletes : bool;
    mutable initial : initial;
    mutable state : state;
  }

  let map_error = function
    | Jetstream.Error.Connection error -> Error.Connection error
    | Jetstream.Error.Ordered_closed -> Error.Closed
    | error -> Error.Jetstream error

  let fail watch error =
    match watch.state with
    | Open ->
        watch.state <- Failed error;
        ignore (Jetstream.Consumer.Ordered.close watch.ordered)
    | Closed | Failed _ -> ()

  let update_initial watch message =
    match watch.initial with
    | Before when Int64.equal (Jetstream.Msg.num_pending message) 0L ->
        watch.initial <- Marker_pending
    | Before | Marker_pending | Live -> ()

  let initial_from_info delivery info =
    match delivery with
    | New -> Marker_pending
    | Last_per_subject | All ->
        if Int64.equal (Jetstream.Consumer.Info.num_pending info) 0L then
          Marker_pending
        else Before

  let v ~sw ?name ?(delivery = Last_per_subject) ?(ignore_deletes = false)
      (value : bucket) =
    let filter =
      match name with
      | None -> Ok (Nats.Subject.Filter.literal (meta_prefix value.bucket ^ ">"))
      | Some name -> (
          match validate_name name with
          | Error error -> Error error
          | Ok () ->
              Ok
                (Nats.Subject.Filter.literal
                   (Nats.Subject.to_string (meta_subject value name))))
    in
    match filter with
    | Error error -> Error error
    | Ok filter ->
        let deliver_policy =
          match delivery with
          | New -> Jetstream.Consumer.Config.New
          | Last_per_subject -> Jetstream.Consumer.Config.Last_per_subject
          | All -> Jetstream.Consumer.Config.All
        in
        (match
           Jetstream.Consumer.Ordered.v ~sw ~deliver_policy
             ~filter_subject:filter value.stream
         with
        | Error error -> Error (map_error error)
        | Ok ordered -> (
            match Jetstream.Consumer.Ordered.info ordered with
            | Error error ->
                ignore (Jetstream.Consumer.Ordered.close ordered);
                Error (map_error error)
            | Ok info ->
                Ok
                  {
                    bucket = value;
                    ordered;
                    ignore_deletes;
                    initial = initial_from_info delivery info;
                    state = Open;
                  }))

  let next_loop watch deadline =
    let result = ref None in
    let next_message () =
      match deadline with
      | None -> Jetstream.Consumer.Ordered.next watch.ordered
      | Some deadline ->
          let connection = Jetstream.connection watch.bucket.jetstream in
          let now = Connection.now connection in
          if Mtime.compare now deadline >= 0 then
            Error (Jetstream.Error.Connection Core_error.Timeout)
          else
            Jetstream.Consumer.Ordered.next_with_timeout
              ~timeout:(Mtime.span now deadline) watch.ordered
    in
    while Option.is_none !result do
      match watch.state with
      | Closed -> result := Some (Error Error.Closed)
      | Failed error -> result := Some (Error error)
      | Open -> (
          match watch.initial with
          | Marker_pending ->
              watch.initial <- Live;
              result := Some (Ok Initial_done)
          | Before | Live -> (
              match next_message () with
              | Error error ->
                  let error = map_error error in
                  (match error with
                  | Error.Connection Core_error.Timeout -> ()
                  | Error.Closed -> watch.state <- Closed
                  | _ -> fail watch error);
                  result := Some (Error error)
              | Ok message -> (
                  update_initial watch message;
                  match info_of_delivery watch.bucket message with
                  | Error error ->
                      fail watch error;
                      result := Some (Error error)
                  | Ok info when
                      watch.ignore_deletes && Info.deleted info ->
                      ()
                  | Ok info -> result := Some (Ok (Info info)))))
    done;
    match !result with Some result -> result | None -> assert false

  let next watch = next_loop watch None

  let next_with_timeout ~timeout watch =
    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
      Error (Error.Connection (Core_error.Invalid_timeout "object watch"))
    else
      let connection = Jetstream.connection watch.bucket.jetstream in
      let deadline =
        match Mtime.add_span (Connection.now connection) timeout with
        | Some value -> value
        | None -> Mtime.max_stamp
      in
      next_loop watch (Some deadline)

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
    match watch.state with
    | Closed -> Ok ()
    | Open | Failed _ ->
        watch.state <- Closed;
        (match Jetstream.Consumer.Ordered.close watch.ordered with
        | Ok () -> Ok ()
        | Error error -> Error (map_error error))
end
