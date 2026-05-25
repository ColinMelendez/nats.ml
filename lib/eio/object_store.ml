module Core_error = Error
module String_map = Map.Make (String)

let ( let* ) value f = match value with Error error -> Error error | Ok value -> f value

module Config = struct
  type storage = Memory | File

  type t = {
    bucket : string;
    description : string option;
    ttl : Mtime.Span.t option;
    max_bytes : int64 option;
    storage : storage;
  }

  type error =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_ttl
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

  let normalize_limit = function Some value when Int64.equal value 0L -> None | Some (-1L) -> None | value -> value

  let validate_limit field = function
    | None -> Ok ()
    | Some value when Int64.compare value (-1L) >= 0 -> Ok ()
    | Some value -> Error (Invalid_limit { field; value })

  let v ~bucket ?description ?ttl ?max_bytes ?(storage = File) () =
    match validate_bucket bucket with
    | Error error -> Error error
    | Ok () -> (
        match ttl with
        | Some value when Mtime.Span.compare value Mtime.Span.zero < 0 ->
            Error Invalid_ttl
        | _ -> (
            match validate_limit "max_bytes" max_bytes with
            | Error error -> Error error
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
                  }))

  let bucket value = value.bucket
  let description value = value.description
  let ttl value = value.ttl
  let max_bytes value = value.max_bytes
  let storage value = value.storage
end

module Name = struct
  type t = string
  type error = Empty_name

  let of_string value =
    if Int.equal (String.length value) 0 then Error Empty_name else Ok value

  let to_string value = value
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

module Link = struct
  type t = { bucket : string; name : Name.t option }

  let bucket value = value.bucket
  let name value = value.name
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
    | Not_found
    | Deleted of Info.t
    | Unsupported_link of Info.t
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

  let pp_name ppf = function
    | Name.Empty_name -> Format.pp_print_string ppf "object name is empty"

  let pp_meta ppf = function
    | Meta.Invalid_chunk_size value ->
        Format.fprintf ppf "object chunk size must be positive, got %d" value

  let pp ppf = function
    | Connection error -> Format.fprintf ppf "connection: %a" Core_error.pp error
    | Jetstream error -> Format.fprintf ppf "JetStream: %a" Jetstream.Error.pp error
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
        Format.fprintf ppf "metadata bucket %S does not match %S" actual expected
    | Unexpected_object_name { expected; actual } ->
        Format.fprintf ppf "metadata object name %S does not match %S" actual
          expected
    | Invalid_link_bucket error ->
        Format.fprintf ppf "invalid link bucket: %a" pp_config error
    | Invalid_link_name { value; reason } ->
        Format.fprintf ppf "invalid link object name %S: %a" value pp_name reason
    | Invalid_metadata field ->
        Format.fprintf ppf "invalid object metadata field %S" field
    | Not_found -> Format.pp_print_string ppf "object was not found"
    | Deleted _ -> Format.pp_print_string ppf "object is deleted"
    | Unsupported_link _ -> Format.pp_print_string ppf "object links are unsupported"
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
        | Some _ -> Format.pp_print_string ppf "object committed; cleanup failed");
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
end

type t = {
  jetstream : Jetstream.t;
  stream : Jetstream.Stream.t;
  bucket : string;
}

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
  Jsont.Object.map ~kind:"NATS object link" (fun bucket name -> { bucket; name })
  |> Jsont.Object.mem "bucket" Jsont.string ~enc:(fun (value : wire_link) ->
      value.bucket)
  |> Jsont.Object.opt_mem "name" Jsont.string ~enc:(fun (value : wire_link) ->
      value.name)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let wire_options_codec =
  Jsont.Object.map ~kind:"NATS object options" (fun chunk_size link ->
      { chunk_size; link })
  |> Jsont.Object.opt_mem "max_chunk_size" Jsont.int ~enc:(fun (value : wire_options) ->
      value.chunk_size)
  |> Jsont.Object.opt_mem "link" wire_link_codec ~enc:(fun (value : wire_options) ->
      value.link)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let headers_codec = Jsont.Object.as_string_map (Jsont.list Jsont.string)
let metadata_codec = Jsont.Object.as_string_map Jsont.string

let wire_info_codec =
  Jsont.Object.map ~kind:"NATS object metadata" (fun name description headers
      metadata options bucket nuid size mtime chunks digest deleted ->
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
  |> Jsont.Object.opt_mem "description" Jsont.string ~enc:(fun (value : wire_info) ->
      value.description)
  |> Jsont.Object.opt_mem "headers" headers_codec ~enc:(fun (value : wire_info) ->
      value.headers)
  |> Jsont.Object.opt_mem "metadata" metadata_codec ~enc:(fun (value : wire_info) ->
      value.metadata)
  |> Jsont.Object.opt_mem "options" wire_options_codec ~enc:(fun (value : wire_info) ->
      value.options)
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
let metadata_filter bucket = Nats.Subject.Filter.literal ("$O." ^ bucket ^ ".M.>")
let chunk_subject bucket nuid = "$O." ^ bucket ^ ".C." ^ nuid

let encode_name name =
  Base64.encode_string ~alphabet:Base64.uri_safe_alphabet name

let metadata_subject bucket name =
  "$O." ^ bucket ^ ".M." ^ encode_name (Name.to_string name)

let stream_config config =
  match
    Jetstream.Stream.Config.v ~name:(stream_name (Config.bucket config))
      ~subjects:[ chunk_filter (Config.bucket config); metadata_filter (Config.bucket config) ]
      ?description:(Config.description config)
      ~storage:
        (match Config.storage config with
        | Config.Memory -> Jetstream.Stream.Config.Memory
        | Config.File -> Jetstream.Stream.Config.File)
      ~retention:Jetstream.Stream.Config.Limits
      ~discard:Jetstream.Stream.Config.New ?max_bytes:(Config.max_bytes config)
      ?max_age:(Config.ttl config) ~allow_rollup:true ~allow_direct:true ()
  with
  | Error error -> Error (Error.Jetstream (Jetstream.Error.Invalid_config error))
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
          description = Jetstream.Stream.Config.description config;
          messages = Jetstream.Stream.Info.messages info;
          bytes = Jetstream.Stream.Info.bytes info;
          first_sequence = Jetstream.Stream.Info.first_sequence info;
          last_sequence = Jetstream.Stream.Info.last_sequence info;
          ttl = Jetstream.Stream.Config.max_age config;
          max_bytes = Jetstream.Stream.Config.max_bytes config;
          storage;
        }

let headers_to_wire headers =
  let add map (name, value) =
    String_map.update name
      (function None -> Some [ value ] | Some values -> Some (value :: values))
      map
  in
  let values = List.fold_left add String_map.empty (Nats.Header.to_list headers) in
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
            link_of_wire
              (Option.bind wire.options (fun value -> value.link))
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
        ~timestamp:(Jetstream.Stream.Message.timestamp message) wire

let read_info_raw ?timeout (value : t) (name : Name.t) =
  let subject = Nats.Subject.literal (metadata_subject value.bucket name) in
  match Jetstream.Stream.get_last ?timeout value.stream ~subject with
  | Error Jetstream.Error.Message_not_found -> Error Error.Not_found
  | Error error -> Error (map_jetstream_error error)
  | Ok message -> decode_message value ~requested_name:name message

let info ?timeout ?(include_deleted = false) value name =
  match read_info_raw ?timeout value name with
  | Error error -> Error error
  | Ok info when Info.deleted info && not include_deleted -> Error Error.Not_found
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
      Bytes.blit (Bytesrw.Bytes.Slice.bytes slice)
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
          (Connection.now (Jetstream.connection value.jetstream)) timeout
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
      ~subject:(Nats.Subject.Filter.literal subject) value.stream
  with
  | Ok _ -> Ok ()
  | Error error -> Error error

let upload_chunks ?deadline value ~nuid ~chunk_size reader =
  let digest = ref (Digestif.SHA256.init ()) in
  let size = ref 0L in
  let chunks = ref 0L in
  let result = ref None in
  let finished = ref false in
  while not !finished && Option.is_none !result do
    match next_chunk reader chunk_size with
    | None -> finished := true
    | Some chunk -> (
        digest := Digestif.SHA256.feed_string !digest chunk;
        match remaining_timeout value deadline with
        | Error error -> result := Some (Error error)
        | Ok publish_timeout -> (
            match
              Jetstream.publish ?timeout:publish_timeout value.jetstream
                (Nats.Subject.literal (chunk_subject value.bucket nuid)) chunk
            with
            | Error error -> result := Some (Error (map_jetstream_error error))
            | Ok _ ->
                size := Int64.add !size (Int64.of_int (String.length chunk));
                chunks := Int64.add !chunks 1L))
  done;
  match !result with
  | Some result -> result
  | None -> Ok { size = !size; chunks = !chunks; digest = digest_string !digest; nuid }

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
                 ignore
                   (purge_chunks value
                      (chunk_subject value.bucket nuid))))
      in
      Fun.protect ~finally:cleanup (fun () ->
          match
            upload_chunks ?deadline value ~nuid
              ~chunk_size:(Meta.chunk_size meta)
              reader
          with
          | Error error -> Error error
          | Ok upload ->
              let wire =
                wire_of_object ~bucket:value.bucket ~name
                  ~description:(Meta.description meta)
                  ~headers:(Meta.headers meta) ~metadata:(Meta.metadata meta)
                  ~options:
                    (Some
                       {
                         chunk_size = Some (Meta.chunk_size meta);
                         link = None;
                       })
                  ~nuid:upload.nuid ~size:upload.size ~chunks:upload.chunks
                  ~digest:(Some upload.digest) ~deleted:false
              in
              (match encode_wire wire with
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
                      | Ok _ ->
                          committed := true;
                          let result =
                            match remaining_timeout value deadline with
                            | Error error -> Error error
                            | Ok info_timeout ->
                                read_info_raw ?timeout:info_timeout value name
                          in
                          let cleanup_result =
                            match old with
                            | Some old_info when not (Info.deleted old_info) ->
                                (match remaining_timeout value deadline with
                                | Error _ ->
                                    Error
                                      (Jetstream.Error.Connection
                                         Core_error.Timeout)
                                | Ok purge_timeout ->
                                    (match
                                       purge_chunks ?timeout:purge_timeout value
                                         (chunk_subject value.bucket
                                            (Info.nuid old_info))
                                     with
                                    | Ok () -> Ok ()
                                    | Error error ->
                                        Error error))
                            | _ -> Ok ()
                          in
                          (match result with
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

let get ?timeout ?(include_deleted = false) value name writer =
  let deadline = timeout_deadline value timeout in
  let info_timeout = remaining_timeout value deadline in
  match info_timeout with
  | Error error -> Error error
  | Ok info_timeout -> (
      match read_info_raw ?timeout:info_timeout value name with
      | Error error -> Error error
      | Ok info when Info.deleted info ->
          if include_deleted then Error (Error.Deleted info) else Error Error.Not_found
      | Ok info -> (
          match Info.link info with
          | Some _ -> Error (Error.Unsupported_link info)
          | None ->
              let expected_size = Info.size info in
              let expected_chunks = Info.chunks info in
              let expected_digest = Info.digest info in
              let verify digest size chunks =
                let actual_digest = digest_string digest in
                if Int64.compare size expected_size <> 0 then
                  Error
                    (Error.Size_mismatch { expected = expected_size; actual = size })
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
              if Int64.equal expected_chunks 0L then
                let digest = Digestif.SHA256.init () in
                match verify digest 0L 0L with
                | Error error -> Error error
                | Ok () ->
                    Bytesrw.Bytes.Writer.write_eod writer;
                    Ok info
              else
                Eio.Switch.run (fun sw ->
                    let filter =
                      Nats.Subject.Filter.literal
                        (chunk_subject value.bucket (Info.nuid info))
                    in
                    match
                      Jetstream.Consumer.Ordered.v ~sw ~batch:1
                        ?expires:None ?filter_subject:(Some filter) value.stream
                    with
                    | Error error -> Error (map_jetstream_error error)
                    | Ok ordered ->
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
                          | Ok (Some timeout) ->
                              (match
                                 Jetstream.Consumer.Ordered.next_with_timeout
                                   ~timeout ordered
                               with
                              | Ok message -> Ok message
                              | Error error -> Error (map_jetstream_error error))
                        in
                        let read_chunks () =
                          while Int64.compare !chunks expected_chunks < 0
                                && Option.is_none !result do
                            match next_message () with
                            | Error error -> result := Some (Error error)
                            | Ok message ->
                                let actual_subject =
                                  Nats.Subject.to_string
                                    (Jetstream.Msg.subject message)
                                in
                                let expected_subject =
                                  chunk_subject value.bucket (Info.nuid info)
                                in
                                if not (String.equal actual_subject expected_subject) then
                                  result :=
                                    Some (Error (Error.Invalid_chunk_subject actual_subject))
                                else
                                  let payload = Jetstream.Msg.payload message in
                                  Bytesrw.Bytes.Writer.write_string writer payload;
                                  digest := Digestif.SHA256.feed_string !digest payload;
                                  size :=
                                    Int64.add !size
                                      (Int64.of_int (String.length payload));
                                  chunks := Int64.add !chunks 1L;
                                  if
                                    Int64.equal
                                      (Jetstream.Msg.num_pending message)
                                      0L
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
                                     ignore
                                       (Jetstream.Consumer.Ordered.close ordered))))
                            (fun () ->
                              read_chunks ();
                              match !result with
                              | Some result -> result
                              | None -> verify !digest !size !chunks)
                        in
                        (match result with
                        | Error error -> Error error
                        | Ok () ->
                            Bytesrw.Bytes.Writer.write_eod writer;
                            Ok info))))

let get_string ?timeout ?include_deleted value name =
  let buffer = Buffer.create 0 in
  let writer = Bytesrw.Bytes.Writer.of_buffer buffer in
  match get ?timeout ?include_deleted value name writer with
  | Error error -> Error error
  | Ok _ -> Ok (Buffer.contents buffer)

let delete ?timeout value name =
  let deadline = timeout_deadline value timeout in
  match remaining_timeout value deadline with
  | Error error -> Error error
  | Ok info_timeout -> (
      match read_info_raw ?timeout:info_timeout value name with
  | Error error -> Error error
  | Ok info when Info.deleted info -> Ok ()
  | Ok info ->
      let wire =
        wire_of_object ~bucket:value.bucket ~name:(Info.name info)
          ~description:(Info.description info) ~headers:(Info.headers info)
          ~metadata:(Info.metadata info)
          ~options:
            (Some
               {
                 chunk_size = Some (Info.chunk_size info);
                 link =
                   (match Info.link info with
                   | None -> None
                   | Some link ->
                       Some
                         {
                           bucket = Link.bucket link;
                           name =
                             Option.map Name.to_string (Link.name link);
                         });
               })
          ~nuid:(Info.nuid info) ~size:0L ~chunks:0L ~digest:None ~deleted:true
      in
      (match encode_wire wire with
      | Error error -> Error error
      | Ok payload -> (
          match remaining_timeout value deadline with
          | Error error -> Error error
          | Ok publish_timeout -> (
              match
                Jetstream.publish ?timeout:publish_timeout
                  ~headers:metadata_headers value.jetstream
                  (Nats.Subject.literal (metadata_subject value.bucket name))
                  payload
              with
              | Error error -> Error (map_jetstream_error error)
              | Ok _ ->
                  if String.equal (Info.nuid info) "" then Ok ()
                  else
                    match remaining_timeout value deadline with
                    | Error _ ->
                        Error
                          (Error.Cleanup_failed
                             {
                               info = Some info;
                               error =
                                 Jetstream.Error.Connection Core_error.Timeout;
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
                                 { info = Some info; error }))))))
