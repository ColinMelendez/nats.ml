module Core_error = Error
module String_map = Map.Make (String)

module Error = struct
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Empty_subjects
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_max_age
    | Invalid_replicas of int
    | Empty_placement
    | Empty_placement_cluster
    | Empty_placement_tag
    | Empty_consumer_name
    | Invalid_consumer_name_character of { position : int; character : char }
    | Invalid_consumer_limit of { field : string; value : int64 }
    | Invalid_consumer_span of { field : string }
    | Invalid_consumer_sample_frequency of string
    | Invalid_consumer_rate_limit of int64
    | Invalid_consumer_replicas of int
    | Invalid_consumer_pause_until of string
    | Invalid_consumer_priority_group of string
    | Invalid_consumer_priority_timestamp of string
    | Invalid_consumer_priority_update
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
    | Message_not_found
    | Invalid_message_header of { name : string; value : string }
    | Empty_msg_id
    | Msg_id_already_set
    | Unexpected_stream_name of { expected : string; actual : string }
    | Unexpected_consumer_name of { expected : string; actual : string }
    | Invalid_batch of int
    | Invalid_max_bytes of int
    | Invalid_priority_group of string
    | Invalid_priority_threshold of { field : string; value : int64 }
    | Invalid_priority of int
    | Invalid_fetch_span
    | Invalid_idle_heartbeat
    | Idle_heartbeat_expires_too_short
    | Missing_heartbeat
    | Missing_ack_reply
    | Invalid_ack_reply of string
    | Not_push_consumer
    | Consumer_deleted
    | Conflict of { code : int; description : string }
    | Unexpected_status of { code : int; description : string }
    | Incomplete_list of { kind : list_kind; missing : string list }
    | Pull_closed
    | Push_closed
    | Ordered_closed

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
    | Invalid_replicas value ->
        Format.fprintf ppf "stream replicas must be between 1 and 5, got %d"
          value
    | Empty_placement ->
        Format.pp_print_string ppf
          "stream placement needs a cluster name or at least one tag"
    | Empty_placement_cluster ->
        Format.pp_print_string ppf "stream placement cluster name is empty"
    | Empty_placement_tag ->
        Format.pp_print_string ppf "stream placement contains an empty tag"
    | Empty_consumer_name -> Format.pp_print_string ppf "consumer name is empty"
    | Invalid_consumer_name_character { position; character } ->
        Format.fprintf ppf "invalid consumer-name character %C at position %d"
          character position
    | Invalid_consumer_limit { field; value } ->
        Format.fprintf ppf "invalid consumer %s limit %Ld" field value
    | Invalid_consumer_span { field } ->
        Format.fprintf ppf "consumer %s must be positive" field
    | Invalid_consumer_sample_frequency value ->
        Format.fprintf ppf
          "consumer sample frequency %S must be a non-negative integer" value
    | Invalid_consumer_rate_limit value ->
        Format.fprintf ppf "consumer rate limit must not be negative, got %Ld"
          value
    | Invalid_consumer_replicas value ->
        Format.fprintf ppf "consumer replicas must not be negative, got %d"
          value
    | Invalid_consumer_pause_until value ->
        Format.fprintf ppf "invalid consumer pause deadline %S" value
    | Invalid_consumer_priority_group value ->
        Format.fprintf ppf "invalid consumer priority group %S" value
    | Invalid_consumer_priority_timestamp value ->
        Format.fprintf ppf "invalid consumer priority timestamp %S" value
    | Invalid_consumer_priority_update ->
        Format.pp_print_string ppf
          "consumer priority groups and policy cannot be changed by update"
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
    | Message_not_found ->
        Format.pp_print_string ppf "JetStream message was not found"
    | Invalid_message_header { name; value } ->
        Format.fprintf ppf "invalid JetStream message header %S=%S" name value
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
    | Invalid_priority_group value ->
        Format.fprintf ppf "invalid JetStream priority group %S" value
    | Invalid_priority_threshold { field; value } ->
        Format.fprintf ppf
          "JetStream priority %s threshold must not be negative, got %Ld" field
          value
    | Invalid_priority value ->
        Format.fprintf ppf
          "JetStream pull priority must be between 0 and 9, got %d" value
    | Invalid_fetch_span ->
        Format.pp_print_string ppf "JetStream fetch expiry must be positive"
    | Invalid_idle_heartbeat ->
        Format.pp_print_string ppf "JetStream idle heartbeat must be positive"
    | Idle_heartbeat_expires_too_short ->
        Format.pp_print_string ppf
          "JetStream pull expiry must be at least twice the idle heartbeat"
    | Missing_heartbeat ->
        Format.pp_print_string ppf
          "JetStream consumer idle heartbeat was not received"
    | Missing_ack_reply ->
        Format.pp_print_string ppf
          "JetStream delivery has no acknowledgement reply"
    | Invalid_ack_reply subject ->
        Format.fprintf ppf "invalid JetStream acknowledgement subject %S"
          subject
    | Not_push_consumer ->
        Format.pp_print_string ppf "JetStream consumer has no delivery subject"
    | Consumer_deleted ->
        Format.pp_print_string ppf "JetStream consumer was deleted"
    | Conflict { code; description } ->
        Format.fprintf ppf "JetStream consumer conflict %d: %s" code description
    | Unexpected_status { code; description } ->
        Format.fprintf ppf "unexpected JetStream consumer status %d: %s" code
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
    | Push_closed ->
        Format.pp_print_string ppf "JetStream push consumer is closed"
    | Ordered_closed ->
        Format.pp_print_string ppf "JetStream ordered consumer is closed"
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
    type compression = Uncompressed | S2

    module Placement = struct
      type t = { cluster : string option; tags : string list }
      type error = config_error

      let v ?cluster ?(tags = []) () =
        match cluster with
        | Some value when Int.equal (String.length value) 0 ->
            Error Error.Empty_placement_cluster
        | _ when Option.is_none cluster && Int.equal (List.length tags) 0 ->
            Error Error.Empty_placement
        | _ when List.exists (fun tag -> Int.equal (String.length tag) 0) tags
          ->
            Error Error.Empty_placement_tag
        | _ -> Ok { cluster; tags }

      let cluster value = value.cluster
      let tags value = value.tags
    end

    type t = {
      name : string;
      subjects : Nats.Subject.Filter.t list;
      description : string option;
      storage : storage;
      replicas : int;
      placement : Placement.t option;
      compression : compression;
      metadata : (string * string) list;
      retention : retention;
      discard : discard;
      max_msgs : int64 option;
      max_msgs_per_subject : int64 option;
      max_bytes : int64 option;
      max_age : Mtime.Span.t option;
      max_msg_size : int64 option;
      allow_rollup : bool;
      allow_direct : bool;
      deny_delete : bool;
      sealed : bool;
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

    let validate_replicas value =
      if value < 1 || value > 5 then Error (Error.Invalid_replicas value)
      else Ok ()

    let v_internal ~allow_empty_subjects ~name ~subjects ?description
        ?(storage = File) ?(replicas = 1) ?placement
        ?(compression = Uncompressed) ?(metadata = []) ?(retention = Limits)
        ?(discard = Old) ?max_msgs ?max_msgs_per_subject ?max_bytes ?max_age
        ?max_msg_size ?(allow_rollup = false) ?(allow_direct = false)
        ?(deny_delete = false) ?(sealed = false) () =
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
          match validate_replicas replicas with
          | Error error -> Error error
          | Ok () -> (
              match validate_limit "max_msgs" max_msgs with
              | Error error -> Error error
              | Ok () -> (
                  match validate_limit "max_bytes" max_bytes with
                  | Error error -> Error error
                  | Ok () -> (
                      match
                        validate_limit "max_msgs_per_subject"
                          max_msgs_per_subject
                      with
                      | Error error -> Error error
                      | Ok () -> (
                          match validate_limit "max_msg_size" max_msg_size with
                          | Error error -> Error error
                          | Ok () -> (
                              match max_age with
                              | Some value
                                when Mtime.Span.compare value Mtime.Span.zero
                                     < 0 ->
                                  Error Error.Invalid_max_age
                              | _ ->
                                  Ok
                                    {
                                      name;
                                      subjects;
                                      description;
                                      storage;
                                      replicas;
                                      placement;
                                      compression;
                                      metadata;
                                      retention;
                                      discard;
                                      max_msgs;
                                      max_msgs_per_subject;
                                      max_bytes;
                                      max_age;
                                      max_msg_size;
                                      allow_rollup;
                                      allow_direct;
                                      deny_delete;
                                      sealed;
                                    }))))))

    let v ~name ~subjects ?description ?storage ?replicas ?placement
        ?compression ?metadata ?retention ?discard ?max_msgs
        ?max_msgs_per_subject ?max_bytes ?max_age ?max_msg_size ?allow_rollup
        ?allow_direct ?deny_delete ?sealed () =
      v_internal ~allow_empty_subjects:false ~name ~subjects ?description
        ?storage ?replicas ?placement ?compression ?metadata ?retention ?discard
        ?max_msgs ?max_msgs_per_subject ?max_bytes ?max_age ?max_msg_size
        ?allow_rollup ?allow_direct ?deny_delete ?sealed ()

    let name value = value.name
    let subjects value = value.subjects
    let description value = value.description
    let storage value = value.storage
    let replicas value = value.replicas
    let placement value = value.placement
    let compression value = value.compression
    let metadata value = value.metadata
    let retention value = value.retention
    let discard value = value.discard
    let max_msgs value = value.max_msgs
    let max_msgs_per_subject value = value.max_msgs_per_subject
    let max_bytes value = value.max_bytes
    let max_age value = value.max_age
    let max_msg_size value = value.max_msg_size
    let allow_rollup value = value.allow_rollup
    let allow_direct value = value.allow_direct
    let deny_delete value = value.deny_delete
    let sealed value = value.sealed

    let rebuild ?sealed ?replicas ?placement ?compression ?metadata value ~name
        ~subjects ~storage ~retention ~discard ~max_msgs ~max_msgs_per_subject
        ~max_bytes ~max_age ~max_msg_size ~allow_rollup ~allow_direct
        ~deny_delete =
      let replicas = Option.value ~default:value.replicas replicas in
      let placement = Option.value ~default:value.placement placement in
      let compression = Option.value ~default:value.compression compression in
      let metadata = Option.value ~default:value.metadata metadata in
      v_internal
        ~allow_empty_subjects:(Int.equal (List.length value.subjects) 0)
        ~name ~subjects ?description:value.description ~storage ~retention
        ~replicas ?placement ~compression ~metadata ~discard ?max_msgs
        ?max_bytes ?max_msgs_per_subject ?max_age ?max_msg_size ~allow_rollup
        ~allow_direct ~deny_delete
        ~sealed:(Option.value sealed ~default:value.sealed)
        ()

    let with_name value name =
      rebuild value ~name ~subjects:value.subjects ~storage:value.storage
        ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_description value description =
      v_internal
        ~allow_empty_subjects:(Int.equal (List.length value.subjects) 0)
        ~name:value.name ~subjects:value.subjects ?description
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ?max_msgs:value.max_msgs
        ?max_msgs_per_subject:value.max_msgs_per_subject
        ?max_bytes:value.max_bytes ?max_age:value.max_age
        ?max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete
        ~sealed:value.sealed ~replicas:value.replicas ?placement:value.placement
        ~compression:value.compression ~metadata:value.metadata ()

    let with_subjects value subjects =
      rebuild value ~name:value.name ~subjects ~storage:value.storage
        ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_storage value storage =
      rebuild value ~name:value.name ~subjects:value.subjects ~storage
        ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_retention value retention =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_discard value discard =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_max_msgs value max_msgs =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_max_bytes value max_bytes =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject ~max_bytes
        ~max_age:value.max_age ~max_msg_size:value.max_msg_size
        ~allow_rollup:value.allow_rollup ~allow_direct:value.allow_direct
        ~deny_delete:value.deny_delete

    let with_max_age value max_age =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age ~max_msg_size:value.max_msg_size
        ~allow_rollup:value.allow_rollup ~allow_direct:value.allow_direct
        ~deny_delete:value.deny_delete

    let with_max_msg_size value max_msg_size =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age ~max_msg_size
        ~allow_rollup:value.allow_rollup ~allow_direct:value.allow_direct
        ~deny_delete:value.deny_delete

    let with_max_msgs_per_subject value max_msgs_per_subject =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs ~max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_allow_rollup value allow_rollup =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_allow_direct value allow_direct =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct ~deny_delete:value.deny_delete

    let with_deny_delete value deny_delete =
      rebuild value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete

    let with_sealed value sealed =
      rebuild ~sealed value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_replicas value replicas =
      rebuild ~replicas value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_placement value placement =
      v_internal
        ~allow_empty_subjects:(Int.equal (List.length value.subjects) 0)
        ~name:value.name ~subjects:value.subjects ?description:value.description
        ~storage:value.storage ~replicas:value.replicas ?placement
        ~compression:value.compression ~metadata:value.metadata
        ~retention:value.retention ~discard:value.discard
        ?max_msgs:value.max_msgs
        ?max_msgs_per_subject:value.max_msgs_per_subject
        ?max_bytes:value.max_bytes ?max_age:value.max_age
        ?max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete
        ~sealed:value.sealed ()

    let with_compression value compression =
      rebuild ~compression value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete

    let with_metadata value metadata =
      rebuild ~metadata value ~name:value.name ~subjects:value.subjects
        ~storage:value.storage ~retention:value.retention ~discard:value.discard
        ~max_msgs:value.max_msgs
        ~max_msgs_per_subject:value.max_msgs_per_subject
        ~max_bytes:value.max_bytes ~max_age:value.max_age
        ~max_msg_size:value.max_msg_size ~allow_rollup:value.allow_rollup
        ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete
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

  module Message = struct
    type t = {
      subject : Nats.Subject.t;
      sequence : int64;
      timestamp : string;
      headers : Nats.Header.t;
      payload : string;
    }

    let subject value = value.subject
    let sequence value = value.sequence
    let timestamp value = value.timestamp
    let headers value = value.headers
    let payload value = value.payload
  end

  type jetstream = t
  type t = { jetstream : jetstream; name : string }

  type wire_config = {
    name : string;
    subjects : string list;
    description : string option;
    storage : Config.storage;
    replicas : int;
    placement : wire_placement option;
    compression : Config.compression;
    metadata : string String_map.t option;
    retention : Config.retention;
    discard : Config.discard;
    max_msgs : int64 option;
    max_msgs_per_subject : int64 option;
    max_bytes : int64 option;
    max_age : int64 option;
    max_msg_size : int64 option;
    allow_rollup : bool;
    allow_direct : bool;
    deny_delete : bool;
    sealed : bool;
    unknown : Jsont.json;
  }

  and wire_placement = { cluster : string option; tags : string list option }

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

  let compression_codec =
    Jsont.enum
      [
        ("s2", Config.S2);
        ("none", Config.Uncompressed);
        ("", Config.Uncompressed);
      ]

  let wire_placement_codec =
    Jsont.Object.map ~kind:"JetStream placement" (fun cluster tags ->
        { cluster; tags })
    |> Jsont.Object.opt_mem "cluster" Jsont.string ~enc:(fun value ->
        value.cluster)
    |> Jsont.Object.opt_mem "tags" (Jsont.list Jsont.string) ~enc:(fun value ->
        value.tags)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  let metadata_codec = Jsont.Object.as_string_map Jsont.string

  let metadata_to_wire metadata =
    let values =
      List.fold_left
        (fun values (name, value) -> String_map.add name value values)
        String_map.empty metadata
    in
    if String_map.is_empty values then None else Some values

  let metadata_of_wire metadata =
    match metadata with None -> [] | Some values -> String_map.bindings values

  let placement_to_wire placement =
    Option.map
      (fun value ->
        {
          cluster = Config.Placement.cluster value;
          tags =
            (match Config.Placement.tags value with
            | [] -> None
            | tags -> Some tags);
        })
      placement

  let wire_config_codec =
    Jsont.Object.map ~kind:"JetStream stream config"
      (fun
        name
        subjects
        description
        storage
        replicas
        placement
        compression
        metadata
        retention
        discard
        max_msgs
        max_msgs_per_subject
        max_bytes
        max_age
        max_msg_size
        allow_rollup
        allow_direct
        deny_delete
        sealed
        unknown
      ->
        {
          name;
          subjects = Option.value ~default:[] subjects;
          description;
          storage;
          replicas = Option.value ~default:1 replicas;
          placement;
          compression = Option.value ~default:Config.Uncompressed compression;
          metadata;
          retention;
          discard;
          max_msgs;
          max_msgs_per_subject;
          max_bytes;
          max_age;
          max_msg_size;
          allow_rollup = Option.value ~default:false allow_rollup;
          allow_direct = Option.value ~default:false allow_direct;
          deny_delete = Option.value ~default:false deny_delete;
          sealed = Option.value ~default:false sealed;
          unknown;
        })
    |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
    |> Jsont.Object.opt_mem "subjects" (Jsont.list Jsont.string)
         ~enc:(fun value ->
           match value.subjects with [] -> None | subjects -> Some subjects)
    |> Jsont.Object.opt_mem "description" Jsont.string ~enc:(fun value ->
        value.description)
    |> Jsont.Object.mem "storage" storage_codec ~enc:(fun value ->
        value.storage)
    |> Jsont.Object.opt_mem "num_replicas" Jsont.int ~enc:(fun value ->
        Some value.replicas)
    |> Jsont.Object.opt_mem "placement" wire_placement_codec ~enc:(fun value ->
        value.placement)
    |> Jsont.Object.opt_mem "compression" compression_codec ~enc:(fun value ->
        match value.compression with
        | Config.Uncompressed -> None
        | Config.S2 -> Some Config.S2)
    |> Jsont.Object.opt_mem "metadata" metadata_codec ~enc:(fun value ->
        value.metadata)
    |> Jsont.Object.mem "retention" retention_codec ~enc:(fun value ->
        value.retention)
    |> Jsont.Object.mem "discard" discard_codec ~enc:(fun value ->
        value.discard)
    |> Jsont.Object.opt_mem "max_msgs" Jsont.int64 ~enc:(fun value ->
        value.max_msgs)
    |> Jsont.Object.opt_mem "max_msgs_per_subject" Jsont.int64
         ~enc:(fun value -> value.max_msgs_per_subject)
    |> Jsont.Object.opt_mem "max_bytes" Jsont.int64 ~enc:(fun value ->
        value.max_bytes)
    |> Jsont.Object.opt_mem "max_age" Jsont.int64 ~enc:(fun value ->
        value.max_age)
    |> Jsont.Object.opt_mem "max_msg_size" Jsont.int64 ~enc:(fun value ->
        value.max_msg_size)
    |> Jsont.Object.opt_mem "allow_rollup_hdrs" Jsont.bool ~enc:(fun value ->
        Some value.allow_rollup)
    |> Jsont.Object.opt_mem "allow_direct" Jsont.bool ~enc:(fun value ->
        Some value.allow_direct)
    |> Jsont.Object.opt_mem "deny_delete" Jsont.bool ~enc:(fun value ->
        Some value.deny_delete)
    |> Jsont.Object.opt_mem "sealed" Jsont.bool ~enc:(fun value ->
        Some value.sealed)
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
      description = Config.description value;
      storage = Config.storage value;
      replicas = Config.replicas value;
      placement = placement_to_wire (Config.placement value);
      compression = Config.compression value;
      metadata = metadata_to_wire (Config.metadata value);
      retention = Config.retention value;
      discard = Config.discard value;
      max_msgs = Config.max_msgs value;
      max_msgs_per_subject = Config.max_msgs_per_subject value;
      max_bytes = Config.max_bytes value;
      max_age = Option.map Mtime.Span.to_uint64_ns (Config.max_age value);
      max_msg_size = Config.max_msg_size value;
      allow_rollup = Config.allow_rollup value;
      allow_direct = Config.allow_direct value;
      deny_delete = Config.deny_delete value;
      sealed = Config.sealed value;
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
      description = Config.description value;
      storage = Config.storage value;
      replicas = Config.replicas value;
      placement = placement_to_wire (Config.placement value);
      compression = Config.compression value;
      metadata =
        Some
          (Option.value ~default:String_map.empty
             (metadata_to_wire (Config.metadata value)));
      retention = Config.retention value;
      discard = Config.discard value;
      max_msgs = Some (Option.value ~default:(-1L) (Config.max_msgs value));
      max_msgs_per_subject =
        Some (Option.value ~default:(-1L) (Config.max_msgs_per_subject value));
      max_bytes = Some (Option.value ~default:(-1L) (Config.max_bytes value));
      max_age =
        Some
          (Option.value ~default:0L
             (Option.map Mtime.Span.to_uint64_ns (Config.max_age value)));
      max_msg_size =
        Some (Option.value ~default:(-1L) (Config.max_msg_size value));
      allow_rollup = Config.allow_rollup value;
      allow_direct = Config.allow_direct value;
      deny_delete = Config.deny_delete value;
      sealed = current.sealed || Config.sealed value;
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
        let max_msgs_per_subject =
          match value.max_msgs_per_subject with
          | Some -1L -> None
          | value -> value
        in
        let max_msg_size =
          match value.max_msg_size with Some -1L -> None | value -> value
        in
        let max_age =
          match value.max_age with
          | None | Some 0L -> None
          | Some nanoseconds -> Some (Mtime.Span.of_uint64_ns nanoseconds)
        in
        let placement =
          match value.placement with
          | None -> Ok None
          | Some placement ->
              Config.Placement.v ?cluster:placement.cluster ?tags:placement.tags
                ()
              |> Result.map Option.some
        in
        match placement with
        | Error error -> Error (Error.Invalid_config error)
        | Ok placement -> (
            match
              Config.v_internal ~allow_empty_subjects:true ~name:value.name
                ~subjects ?description:value.description ~storage:value.storage
                ~replicas:value.replicas ?placement
                ~compression:value.compression
                ~metadata:(metadata_of_wire value.metadata)
                ~retention:value.retention ~discard:value.discard ?max_msgs
                ?max_msgs_per_subject ?max_bytes ?max_age ?max_msg_size
                ~allow_rollup:value.allow_rollup
                ~allow_direct:value.allow_direct ~deny_delete:value.deny_delete
                ~sealed:value.sealed ()
            with
            | Ok config -> Ok config
            | Error error -> Error (Error.Invalid_config error)))

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

  type message_get_request = { sequence : int64 }

  let message_get_request_codec =
    Jsont.Object.map ~kind:"JetStream direct message request" (fun sequence ->
        { sequence })
    |> Jsont.Object.mem "seq" Jsont.int64 ~enc:(fun value -> value.sequence)
    |> Jsont.Object.finish

  let message_header message name =
    match Nats.Header.find name (Nats.Message.headers message) with
    | Some value -> Ok value
    | None -> Error (Error.Missing_field name)

  let message_sequence value =
    match Int64.of_string_opt value with
    | Some sequence when Int64.compare sequence 0L >= 0 -> Ok sequence
    | _ -> Error (Error.Invalid_message_header { name = "JSSequence"; value })

  let stored_message (stream : t) message =
    let headers = Nats.Message.headers message in
    if
      Nats.Header.is_empty headers
      && String.equal (Nats.Message.payload message) ""
    then Error Error.Message_not_found
    else
      let ( let* ) value f =
        match value with Error error -> Error error | Ok value -> f value
      in
      let* response_stream = message_header message "JSStream" in
      if not (String.equal response_stream stream.name) then
        Error
          (Error.Unexpected_stream_name
             { expected = stream.name; actual = response_stream })
      else
        let* sequence_header = message_header message "JSSequence" in
        let* sequence = message_sequence sequence_header in
        let* subject_header = message_header message "JSSubject" in
        let* subject =
          match Nats.Subject.of_string subject_header with
          | Ok subject -> Ok subject
          | Error error -> Error (Error.Invalid_subject error)
        in
        let* timestamp = message_header message "JSTimeStamp" in
        Ok
          {
            Message.subject;
            sequence;
            timestamp;
            headers;
            payload = Nats.Message.payload message;
          }

  let get ?timeout stream ~sequence =
    if Int64.compare sequence 0L < 0 then
      Error
        (Error.Invalid_message_header
           { name = "JSSequence"; value = Int64.to_string sequence })
    else
      match encode message_get_request_codec { sequence } with
      | Error error -> Error error
      | Ok payload -> (
          let subject =
            api_subject stream.jetstream [ "DIRECT"; "GET"; stream.name ]
          in
          match
            request_msg ?timeout stream.jetstream
              (Nats.Message.v ~subject payload)
          with
          | Error error -> Error error
          | Ok message -> stored_message stream message)

  let get_last ?timeout stream ~subject =
    let subject =
      api_subject stream.jetstream
        [ "DIRECT"; "GET"; stream.name; Nats.Subject.to_string subject ]
    in
    match
      request_msg ?timeout stream.jetstream (Nats.Message.v ~subject "")
    with
    | Error error -> Error error
    | Ok message -> stored_message stream message

  type purge_request = { filter : string option }

  let purge_request_codec =
    Jsont.Object.map ~kind:"JetStream stream purge request" (fun filter ->
        { filter })
    |> Jsont.Object.opt_mem "filter" Jsont.string ~enc:(fun value ->
        value.filter)
    |> Jsont.Object.finish

  type purge_response = { error : api_error option; purged : int64 }

  let purge_response_codec =
    Jsont.Object.map ~kind:"JetStream stream purge response"
      (fun error purged -> { error; purged })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.mem "purged" Jsont.int64 ~enc:(fun value -> value.purged)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  let purge ?timeout ?subject stream =
    let request =
      { filter = Option.map Nats.Subject.Filter.to_string subject }
    in
    match encode purge_request_codec request with
    | Error error -> Error error
    | Ok payload -> (
        let subject =
          api_subject stream.jetstream [ "STREAM"; "PURGE"; stream.name ]
        in
        match
          request_msg ?timeout stream.jetstream
            (Nats.Message.v ~subject payload)
        with
        | Error error -> Error error
        | Ok message -> (
            match decode purge_response_codec message with
            | Error error -> Error error
            | Ok { error = Some error; _ } -> Error (Error.Api error)
            | Ok { error = None; purged } -> Ok purged))

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
    type priority_policy = Overflow | Pinned_client | Prioritized

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
      deliver_subject : Nats.Subject.t option;
      deliver_group : Nats.Queue_group.t option;
      idle_heartbeat : Mtime.Span.t option;
      flow_control : bool option;
      deliver_policy : deliver_policy;
      ack_policy : ack_policy;
      ack_wait : Mtime.Span.t option;
      max_deliver : int option;
      filter_subject : Nats.Subject.Filter.t option;
      filter_subjects : Nats.Subject.Filter.t list;
      backoff : Mtime.Span.t list;
      pause_until : Ptime.t option;
      priority_groups : string list;
      priority_policy : priority_policy option;
      priority_timeout : Mtime.Span.t option;
      sample_frequency : int option;
      rate_limit : int64 option;
      replicas : int option;
      metadata : (string * string) list;
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

    let validate_sample_frequency = function
      | None -> Ok ()
      | Some value when value >= 0 -> Ok ()
      | Some value ->
          Error (Error.Invalid_consumer_sample_frequency (Int.to_string value))

    let validate_rate_limit = function
      | None -> Ok ()
      | Some value when Int64.compare value 0L >= 0 -> Ok ()
      | Some value -> Error (Error.Invalid_consumer_rate_limit value)

    let validate_replicas = function
      | None -> Ok ()
      | Some value when value >= 0 -> Ok ()
      | Some value -> Error (Error.Invalid_consumer_replicas value)

    let valid_priority_group_character character =
      let code = Char.code character in
      (code >= Char.code 'A' && code <= Char.code 'Z')
      || (code >= Char.code 'a' && code <= Char.code 'z')
      || (code >= Char.code '0' && code <= Char.code '9')
      || Char.equal character '/' || Char.equal character '_'
      || Char.equal character '-' || Char.equal character '='

    let validate_priority_group group =
      let length = String.length group in
      if Int.equal length 0 || Int.compare length 16 > 0 then
        Error (Error.Invalid_consumer_priority_group group)
      else
        let invalid = ref false in
        for position = 0 to length - 1 do
          if not (valid_priority_group_character (String.get group position))
          then invalid := true
        done;
        if !invalid then Error (Error.Invalid_consumer_priority_group group)
        else Ok ()

    let validate_priority_groups groups =
      match groups with
      | [] -> Ok ()
      | [ group ] -> validate_priority_group group
      | _ :: _ :: _ ->
          Error
            (Error.Invalid_consumer_policy
               {
                 field = "priority_groups";
                 value = "only one group is currently supported";
               })

    let v ?durable_name ?description ?deliver_subject ?deliver_group
        ?idle_heartbeat ?flow_control ?(deliver_policy = All)
        ?(ack_policy = Explicit) ?ack_wait ?max_deliver ?filter_subject
        ?(filter_subjects = []) ?(backoff = []) ?pause_until
        ?(priority_groups = []) ?priority_policy ?priority_timeout
        ?sample_frequency ?rate_limit ?replicas ?(metadata = [])
        ?(replay_policy = Instant) ?max_ack_pending ?max_waiting ?max_batch
        ?max_expires ?max_bytes ?headers_only ?inactive_threshold ?mem_storage
        () =
      let max_expires = normalize_span max_expires in
      let inactive_threshold = normalize_span inactive_threshold in
      let idle_heartbeat = normalize_span idle_heartbeat in
      let priority_timeout = normalize_span priority_timeout in
      let ( let* ) value f =
        match value with Error error -> Error error | Ok value -> f value
      in
      let* () = validate_name durable_name in
      let* () =
        match (deliver_subject, deliver_group) with
        | None, Some _ ->
            Error
              (Error.Invalid_consumer_policy
                 { field = "deliver_group"; value = "requires deliver_subject" })
        | _ -> Ok ()
      in
      let* () = validate_deliver_policy deliver_policy in
      let* () =
        match (filter_subject, filter_subjects) with
        | Some _, _ :: _ ->
            Error
              (Error.Invalid_consumer_policy
                 {
                   field = "filter_subjects";
                   value = "exclusive with filter_subject";
                 })
        | _ -> Ok ()
      in
      let* () = validate_sample_frequency sample_frequency in
      let* () = validate_rate_limit rate_limit in
      let* () = validate_replicas replicas in
      let* () = validate_priority_groups priority_groups in
      let* () =
        match (priority_policy, priority_groups) with
        | None, [] -> Ok ()
        | None, _ :: _ ->
            Error
              (Error.Invalid_consumer_policy
                 {
                   field = "priority_groups";
                   value = "requires priority_policy";
                 })
        | Some _, [] ->
            Error
              (Error.Invalid_consumer_policy
                 {
                   field = "priority_policy";
                   value = "requires priority_groups";
                 })
        | Some _, _ :: _ -> Ok ()
      in
      let* () =
        match (priority_policy, deliver_subject) with
        | Some _, Some _ ->
            Error
              (Error.Invalid_consumer_policy
                 { field = "priority_policy"; value = "requires pull consumer" })
        | _ -> Ok ()
      in
      let* () =
        match (priority_policy, ack_policy) with
        | Some (Overflow | Pinned_client), (No_ack | All) ->
            Error
              (Error.Invalid_consumer_policy
                 { field = "ack_policy"; value = "requires explicit" })
        | _ -> Ok ()
      in
      let* () =
        match (priority_policy, priority_timeout) with
        | None, Some _ ->
            Error
              (Error.Invalid_consumer_policy
                 {
                   field = "priority_timeout";
                   value = "requires priority_policy";
                 })
        | Some (Overflow | Prioritized), Some _ ->
            Error
              (Error.Invalid_consumer_policy
                 {
                   field = "priority_timeout";
                   value = "requires pinned_client";
                 })
        | _ -> Ok ()
      in
      let* () =
        validate_span
          (Error.Invalid_consumer_span { field = "priority_timeout" })
          priority_timeout
      in
      let* () =
        match (deliver_subject, rate_limit) with
        | None, Some value when Int64.compare value 0L > 0 ->
            Error
              (Error.Invalid_consumer_policy
                 { field = "rate_limit"; value = "requires deliver_subject" })
        | _ -> Ok ()
      in
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
      let* () =
        validate_span
          (Error.Invalid_consumer_span { field = "idle_heartbeat" })
          idle_heartbeat
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
          deliver_subject;
          deliver_group;
          idle_heartbeat;
          flow_control;
          deliver_policy;
          ack_policy;
          ack_wait;
          max_deliver;
          filter_subject;
          filter_subjects;
          backoff;
          pause_until;
          priority_groups;
          priority_policy;
          priority_timeout;
          sample_frequency;
          rate_limit;
          replicas;
          metadata;
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
    let deliver_subject value = value.deliver_subject
    let deliver_group value = value.deliver_group
    let idle_heartbeat value = value.idle_heartbeat
    let flow_control value = value.flow_control
    let deliver_policy value = value.deliver_policy
    let ack_policy value = value.ack_policy
    let ack_wait value = value.ack_wait
    let max_deliver value = value.max_deliver
    let filter_subject value = value.filter_subject
    let filter_subjects value = value.filter_subjects
    let backoff value = value.backoff
    let pause_until value = value.pause_until
    let priority_groups value = value.priority_groups
    let priority_policy value = value.priority_policy
    let priority_timeout value = value.priority_timeout
    let sample_frequency value = value.sample_frequency
    let rate_limit value = value.rate_limit
    let replicas value = value.replicas
    let metadata value = value.metadata
    let replay_policy value = value.replay_policy
    let max_ack_pending value = value.max_ack_pending
    let max_waiting value = value.max_waiting
    let max_batch value = value.max_batch
    let max_expires value = value.max_expires
    let max_bytes value = value.max_bytes
    let headers_only value = value.headers_only
    let inactive_threshold value = value.inactive_threshold
    let mem_storage value = value.mem_storage

    let rebuild ~durable_name ~description ~deliver_subject ~deliver_group
        ~idle_heartbeat ~flow_control ~deliver_policy ~ack_policy ~ack_wait
        ~max_deliver ~filter_subject ~filter_subjects ~backoff ~pause_until
        ~priority_groups ~priority_policy ~priority_timeout ~sample_frequency
        ~rate_limit ~replicas ~metadata ~replay_policy ~max_ack_pending
        ~max_waiting ~max_batch ~max_expires ~max_bytes ~headers_only
        ~inactive_threshold ~mem_storage =
      v ?durable_name ?description ?deliver_subject ?deliver_group
        ?idle_heartbeat ?flow_control ~deliver_policy ~ack_policy ?ack_wait
        ?max_deliver ?filter_subject ~filter_subjects ~backoff ?sample_frequency
        ?pause_until ~priority_groups ?priority_policy ?priority_timeout
        ?rate_limit ?replicas ~metadata ~replay_policy ?max_ack_pending
        ?max_waiting ?max_batch ?max_expires ?max_bytes ?headers_only
        ?inactive_threshold ?mem_storage ()

    let rebuild_modern value ~sample_frequency ~rate_limit ~replicas ~metadata =
      let sample_frequency =
        Option.value ~default:value.sample_frequency sample_frequency
      in
      let rate_limit = Option.value ~default:value.rate_limit rate_limit in
      let replicas = Option.value ~default:value.replicas replicas in
      let metadata = Option.value ~default:value.metadata metadata in
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout ~sample_frequency ~rate_limit
        ~replicas ~metadata ~replay_policy:value.replay_policy
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let rebuild_delivery value ~filter_subject ~filter_subjects ~backoff =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject ~filter_subjects ~backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~replay_policy:value.replay_policy
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let rebuild_priority value ~priority_groups ~priority_policy
        ~priority_timeout =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups ~priority_policy
        ~priority_timeout ~sample_frequency:value.sample_frequency
        ~rate_limit:value.rate_limit ~replicas:value.replicas
        ~metadata:value.metadata ~max_ack_pending:value.max_ack_pending
        ~max_waiting:value.max_waiting ~max_batch:value.max_batch
        ~max_expires:value.max_expires ~max_bytes:value.max_bytes
        ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_durable_name value durable_name =
      rebuild ~durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_description value description =
      rebuild ~durable_name:value.durable_name ~description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_deliver_subject value deliver_subject =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject ~deliver_group:value.deliver_group
        ~idle_heartbeat:value.idle_heartbeat ~flow_control:value.flow_control
        ~deliver_policy:value.deliver_policy ~ack_policy:value.ack_policy
        ~ack_wait:value.ack_wait ~max_deliver:value.max_deliver
        ~filter_subject:value.filter_subject ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_deliver_group value deliver_group =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject ~deliver_group
        ~idle_heartbeat:value.idle_heartbeat ~flow_control:value.flow_control
        ~deliver_policy:value.deliver_policy ~ack_policy:value.ack_policy
        ~ack_wait:value.ack_wait ~max_deliver:value.max_deliver
        ~filter_subject:value.filter_subject ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_idle_heartbeat value idle_heartbeat =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_flow_control value flow_control =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_deliver_policy value deliver_policy =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_ack_policy value ack_policy =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy ~ack_wait:value.ack_wait ~max_deliver:value.max_deliver
        ~filter_subject:value.filter_subject ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_ack_wait value ack_wait =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait ~max_deliver:value.max_deliver
        ~filter_subject:value.filter_subject ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_max_deliver value max_deliver =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait ~max_deliver
        ~filter_subject:value.filter_subject ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_filter_subject value filter_subject =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject
        ~replay_policy:value.replay_policy ~filter_subjects:[]
        ~backoff:value.backoff ~pause_until:value.pause_until
        ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_filter_subjects value filter_subjects =
      rebuild_delivery value ~filter_subject:None ~filter_subjects
        ~backoff:value.backoff

    let with_backoff value backoff =
      rebuild_delivery value ~filter_subject:value.filter_subject
        ~filter_subjects:value.filter_subjects ~backoff

    let with_pause_until value pause_until =
      match
        rebuild_modern value ~sample_frequency:(Some value.sample_frequency)
          ~rate_limit:(Some value.rate_limit) ~replicas:(Some value.replicas)
          ~metadata:(Some value.metadata)
      with
      | Error error -> Error error
      | Ok rebuilt -> Ok { rebuilt with pause_until }

    let with_priority_groups value priority_groups =
      rebuild_priority value ~priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout

    let with_priority_policy value priority_policy =
      rebuild_priority value ~priority_groups:value.priority_groups
        ~priority_policy ~priority_timeout:value.priority_timeout

    let with_priority_timeout value priority_timeout =
      rebuild_priority value ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy ~priority_timeout

    let with_sample_frequency value (sample_frequency : int option) =
      rebuild_modern value ~sample_frequency:(Some sample_frequency)
        ~rate_limit:(Some value.rate_limit) ~replicas:(Some value.replicas)
        ~metadata:(Some value.metadata)

    let with_rate_limit value (rate_limit : int64 option) =
      rebuild_modern value ~sample_frequency:(Some value.sample_frequency)
        ~rate_limit:(Some rate_limit) ~replicas:(Some value.replicas)
        ~metadata:(Some value.metadata)

    let with_replicas value (replicas : int option) =
      rebuild_modern value ~sample_frequency:(Some value.sample_frequency)
        ~rate_limit:(Some value.rate_limit) ~replicas:(Some replicas)
        ~metadata:(Some value.metadata)

    let with_metadata value metadata =
      rebuild_modern value ~sample_frequency:(Some value.sample_frequency)
        ~rate_limit:(Some value.rate_limit) ~replicas:(Some value.replicas)
        ~metadata:(Some metadata)

    let with_replay_policy value replay_policy =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy ~filter_subjects:value.filter_subjects
        ~backoff:value.backoff ~pause_until:value.pause_until
        ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_max_ack_pending value max_ack_pending =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata ~max_ack_pending
        ~max_waiting:value.max_waiting ~max_batch:value.max_batch
        ~max_expires:value.max_expires ~max_bytes:value.max_bytes
        ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_max_waiting value max_waiting =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_max_batch value max_batch =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch ~max_expires:value.max_expires ~max_bytes:value.max_bytes
        ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_max_expires value max_expires =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires ~max_bytes:value.max_bytes
        ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_max_bytes value max_bytes =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires ~max_bytes
        ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_headers_only value headers_only =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only
        ~inactive_threshold:value.inactive_threshold
        ~mem_storage:value.mem_storage

    let with_inactive_threshold value inactive_threshold =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold ~mem_storage:value.mem_storage

    let with_mem_storage value mem_storage =
      rebuild ~durable_name:value.durable_name ~description:value.description
        ~deliver_subject:value.deliver_subject
        ~deliver_group:value.deliver_group ~idle_heartbeat:value.idle_heartbeat
        ~flow_control:value.flow_control ~deliver_policy:value.deliver_policy
        ~ack_policy:value.ack_policy ~ack_wait:value.ack_wait
        ~max_deliver:value.max_deliver ~filter_subject:value.filter_subject
        ~replay_policy:value.replay_policy
        ~filter_subjects:value.filter_subjects ~backoff:value.backoff
        ~pause_until:value.pause_until ~priority_groups:value.priority_groups
        ~priority_policy:value.priority_policy
        ~priority_timeout:value.priority_timeout
        ~sample_frequency:value.sample_frequency ~rate_limit:value.rate_limit
        ~replicas:value.replicas ~metadata:value.metadata
        ~max_ack_pending:value.max_ack_pending ~max_waiting:value.max_waiting
        ~max_batch:value.max_batch ~max_expires:value.max_expires
        ~max_bytes:value.max_bytes ~headers_only:value.headers_only
        ~inactive_threshold:value.inactive_threshold ~mem_storage
  end

  type wire_config = {
    durable_name : string option;
    description : string option;
    deliver_subject : string option;
    deliver_group : string option;
    idle_heartbeat : int64 option;
    flow_control : bool option;
    deliver_policy : string;
    opt_start_seq : int64 option;
    opt_start_time : string option;
    ack_policy : Config.ack_policy;
    ack_wait : int64 option;
    max_deliver : int option;
    filter_subject : string option;
    filter_subjects : string list option;
    backoff : int64 list option;
    pause_until : string option;
    priority_groups : string list option;
    priority_policy : string option;
    priority_timeout : int64 option;
    sample_frequency : string option;
    rate_limit : int64 option;
    replicas : int option;
    metadata : string String_map.t option;
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

  let metadata_codec = Jsont.Object.as_string_map Jsont.string

  let metadata_to_wire metadata =
    let values =
      List.fold_left
        (fun values (name, value) -> String_map.add name value values)
        String_map.empty metadata
    in
    if String_map.is_empty values then None else Some values

  let metadata_of_wire metadata =
    match metadata with None -> [] | Some values -> String_map.bindings values

  let sample_frequency_to_wire =
    Option.map (fun value -> Int.to_string value ^ "%")

  let sample_frequency_of_wire = function
    | None | Some "" -> Ok None
    | Some raw -> (
        let length = String.length raw in
        let digits_length =
          if length > 0 && Char.equal (String.get raw (length - 1)) '%' then
            length - 1
          else length
        in
        let invalid = ref (Int.equal digits_length 0) in
        for position = 0 to digits_length - 1 do
          let character = String.get raw position in
          if character < '0' || character > '9' then invalid := true
        done;
        if !invalid then
          Error
            (Error.Invalid_config (Error.Invalid_consumer_sample_frequency raw))
        else
          match int_of_string_opt (String.sub raw 0 digits_length) with
          | None ->
              Error
                (Error.Invalid_config
                   (Error.Invalid_consumer_sample_frequency raw))
          | Some 0 -> Ok None
          | Some value -> Ok (Some value))

  let filter_subjects_to_wire subjects =
    match subjects with
    | [] -> None
    | subjects -> Some (List.map Nats.Subject.Filter.to_string subjects)

  let filter_subjects_of_wire subjects =
    match subjects with
    | None -> Ok []
    | Some subjects -> (
        let parsed = ref [] in
        let parse_error = ref None in
        List.iter
          (fun subject ->
            match !parse_error with
            | Some _ -> ()
            | None -> (
                match Nats.Subject.Filter.of_string subject with
                | Ok subject -> parsed := subject :: !parsed
                | Error error -> parse_error := Some error))
          subjects;
        match !parse_error with
        | Some error -> Error (Error.Invalid_subject error)
        | None -> Ok (List.rev !parsed))

  let backoff_to_wire backoff =
    match backoff with
    | [] -> None
    | backoff -> Some (List.map Mtime.Span.to_uint64_ns backoff)

  let backoff_of_wire backoff =
    match backoff with
    | None -> []
    | Some backoff -> List.map Mtime.Span.of_uint64_ns backoff

  let wire_config_codec =
    Jsont.Object.map ~kind:"JetStream consumer config"
      (fun
        durable_name
        description
        deliver_subject
        deliver_group
        idle_heartbeat
        flow_control
        deliver_policy
        opt_start_seq
        opt_start_time
        ack_policy
        ack_wait
        max_deliver
        filter_subject
        filter_subjects
        backoff
        pause_until
        priority_groups
        priority_policy
        priority_timeout
        sample_frequency
        rate_limit
        replicas
        metadata
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
          deliver_subject;
          deliver_group;
          idle_heartbeat;
          flow_control;
          deliver_policy;
          opt_start_seq;
          opt_start_time;
          ack_policy;
          ack_wait;
          max_deliver;
          filter_subject;
          filter_subjects;
          backoff;
          pause_until;
          priority_groups;
          priority_policy;
          priority_timeout;
          sample_frequency;
          rate_limit;
          replicas;
          metadata;
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
    |> Jsont.Object.opt_mem "deliver_subject" Jsont.string ~enc:(fun value ->
        value.deliver_subject)
    |> Jsont.Object.opt_mem "deliver_group" Jsont.string ~enc:(fun value ->
        value.deliver_group)
    |> Jsont.Object.opt_mem "idle_heartbeat" Jsont.int64 ~enc:(fun value ->
        value.idle_heartbeat)
    |> Jsont.Object.opt_mem "flow_control" Jsont.bool ~enc:(fun value ->
        value.flow_control)
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
    |> Jsont.Object.opt_mem "filter_subjects" (Jsont.list Jsont.string)
         ~enc:(fun value -> value.filter_subjects)
    |> Jsont.Object.opt_mem "backoff" (Jsont.list Jsont.int64)
         ~enc:(fun value -> value.backoff)
    |> Jsont.Object.opt_mem "pause_until" Jsont.string ~enc:(fun value ->
        value.pause_until)
    |> Jsont.Object.opt_mem "priority_groups" (Jsont.list Jsont.string)
         ~enc:(fun value -> value.priority_groups)
    |> Jsont.Object.opt_mem "priority_policy" Jsont.string ~enc:(fun value ->
        value.priority_policy)
    |> Jsont.Object.opt_mem "priority_timeout" Jsont.int64 ~enc:(fun value ->
        value.priority_timeout)
    |> Jsont.Object.opt_mem "sample_freq" Jsont.string ~enc:(fun value ->
        value.sample_frequency)
    |> Jsont.Object.opt_mem "rate_limit_bps" Jsont.int64 ~enc:(fun value ->
        value.rate_limit)
    |> Jsont.Object.opt_mem "num_replicas" Jsont.int ~enc:(fun value ->
        value.replicas)
    |> Jsont.Object.opt_mem "metadata" metadata_codec ~enc:(fun value ->
        value.metadata)
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

  type create_request = {
    stream_name : string;
    config : wire_config;
    action : string option;
  }

  let create_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer create request"
      (fun stream_name config action -> { stream_name; config; action })
    |> Jsont.Object.mem "stream_name" Jsont.string ~enc:(fun value ->
        value.stream_name)
    |> Jsont.Object.mem "config" wire_config_codec ~enc:(fun value ->
        value.config)
    |> Jsont.Object.opt_mem "action" Jsont.string ~enc:(fun value ->
        value.action)
    |> Jsont.Object.finish

  let priority_policy_to_wire = function
    | None -> None
    | Some Config.Overflow -> Some "overflow"
    | Some Config.Pinned_client -> Some "pinned_client"
    | Some Config.Prioritized -> Some "prioritized"

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
      deliver_subject =
        Option.map Nats.Subject.to_string (Config.deliver_subject value);
      deliver_group =
        Option.map Nats.Queue_group.to_string (Config.deliver_group value);
      idle_heartbeat =
        Option.map Mtime.Span.to_uint64_ns (Config.idle_heartbeat value);
      flow_control = Config.flow_control value;
      deliver_policy;
      opt_start_seq;
      opt_start_time;
      ack_policy = Config.ack_policy value;
      ack_wait = Option.map Mtime.Span.to_uint64_ns (Config.ack_wait value);
      max_deliver = Config.max_deliver value;
      filter_subject =
        Option.map Nats.Subject.Filter.to_string (Config.filter_subject value);
      filter_subjects = filter_subjects_to_wire (Config.filter_subjects value);
      backoff = backoff_to_wire (Config.backoff value);
      pause_until =
        Option.map
          (Ptime.to_rfc3339 ~frac_s:9 ~tz_offset_s:0)
          (Config.pause_until value);
      priority_groups =
        (match Config.priority_groups value with
        | [] -> None
        | groups -> Some groups);
      priority_policy = priority_policy_to_wire (Config.priority_policy value);
      priority_timeout =
        Option.map Mtime.Span.to_uint64_ns (Config.priority_timeout value);
      sample_frequency =
        sample_frequency_to_wire (Config.sample_frequency value);
      rate_limit = Config.rate_limit value;
      replicas = Config.replicas value;
      metadata = metadata_to_wire (Config.metadata value);
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

  let is_zero_pause_until value =
    let (year, month, day), ((hour, minute, second), _) =
      Ptime.to_date_time ~tz_offset_s:0 value
    in
    Int.equal year 1 && Int.equal month 1 && Int.equal day 1 && Int.equal hour 0
    && Int.equal minute 0 && Int.equal second 0
    && Ptime.Span.equal (Ptime.frac_s value) Ptime.Span.zero

  let pause_until_of_wire = function
    | None | Some "" -> Ok None
    | Some raw -> (
        match Ptime.of_rfc3339 ~strict:true raw with
        | Ok (value, _, _) when is_zero_pause_until value -> Ok None
        | Ok (value, _, _) -> Ok (Some value)
        | Error _ ->
            Error
              (Error.Invalid_config (Error.Invalid_consumer_pause_until raw)))

  let priority_policy_of_wire = function
    | None | Some "" | Some "none" -> Ok None
    | Some "overflow" -> Ok (Some Config.Overflow)
    | Some "pinned_client" -> Ok (Some Config.Pinned_client)
    | Some "prioritized" -> Ok (Some Config.Prioritized)
    | Some value ->
        Error
          (Error.Invalid_config
             (Error.Invalid_consumer_policy { field = "priority_policy"; value }))

  let priority_groups_of_wire = function
    | None -> Ok []
    | Some groups -> (
        match Config.validate_priority_groups groups with
        | Ok () -> Ok groups
        | Error error -> Error (Error.Invalid_config error))

  let priority_timeout_of_wire = function
    | None | Some 0L -> Ok None
    | Some value when Int64.compare value 0L > 0 ->
        Ok (Some (Mtime.Span.of_uint64_ns value))
    | Some _ ->
        Error
          (Error.Invalid_config
             (Error.Invalid_consumer_span { field = "priority_timeout" }))

  let config_of_wire value =
    let ( let* ) value f =
      match value with Error error -> Error error | Ok value -> f value
    in
    let normalize_max_deliver = function Some -1 -> None | value -> value in
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
    let filter_subjects = filter_subjects_of_wire value.filter_subjects in
    let backoff = backoff_of_wire value.backoff in
    let pause_until = pause_until_of_wire value.pause_until in
    let priority_groups = priority_groups_of_wire value.priority_groups in
    let priority_policy = priority_policy_of_wire value.priority_policy in
    let priority_timeout = priority_timeout_of_wire value.priority_timeout in
    let deliver_subject =
      match value.deliver_subject with
      | None | Some "" -> Ok None
      | Some subject -> (
          match Nats.Subject.of_string subject with
          | Ok subject -> Ok (Some subject)
          | Error error -> Error (Error.Invalid_subject error))
    in
    let deliver_group =
      match value.deliver_group with
      | None | Some "" -> Ok None
      | Some group -> (
          match Nats.Queue_group.of_string group with
          | Ok group -> Ok (Some group)
          | Error error -> Error (Error.Invalid_subject error))
    in
    let idle_heartbeat =
      Option.map Mtime.Span.of_uint64_ns value.idle_heartbeat
    in
    let sample_frequency = sample_frequency_of_wire value.sample_frequency in
    let replicas =
      match value.replicas with Some 0 -> None | replicas -> replicas
    in
    let rate_limit =
      match value.rate_limit with Some 0L -> None | rate_limit -> rate_limit
    in
    let* deliver_policy = deliver_policy in
    let* filter_subject = filter_subject in
    let* filter_subjects = filter_subjects in
    let* deliver_subject = deliver_subject in
    let* deliver_group = deliver_group in
    let* sample_frequency = sample_frequency in
    let* pause_until = pause_until in
    let* priority_groups = priority_groups in
    let* priority_policy = priority_policy in
    let* priority_timeout = priority_timeout in
    let ack_wait = Option.map Mtime.Span.of_uint64_ns value.ack_wait in
    let max_expires = Option.map Mtime.Span.of_uint64_ns value.max_expires in
    let inactive_threshold =
      Option.map Mtime.Span.of_uint64_ns value.inactive_threshold
    in
    let max_deliver = normalize_max_deliver value.max_deliver in
    let max_ack_pending = value.max_ack_pending in
    let max_waiting = value.max_waiting in
    let max_batch = value.max_batch in
    let max_bytes = value.max_bytes in
    match
      Config.v ?durable_name:value.durable_name ?description:value.description
        ?deliver_subject ?deliver_group ?idle_heartbeat
        ?flow_control:value.flow_control ~deliver_policy
        ~ack_policy:value.ack_policy ?ack_wait ?max_deliver ?filter_subject
        ~filter_subjects ~backoff ?pause_until ~priority_groups ?priority_policy
        ?priority_timeout ?sample_frequency ?rate_limit ?replicas
        ~metadata:(metadata_of_wire value.metadata)
        ~replay_policy:value.replay_policy ?max_ack_pending ?max_waiting
        ?max_batch ?max_expires ?max_bytes ?headers_only:value.headers_only
        ?inactive_threshold ?mem_storage:value.mem_storage ()
    with
    | Ok config -> Ok (config, value.unknown)
    | Error error -> Error (Error.Invalid_config error)

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

  type wire_priority_group = {
    group : string;
    pinned_client_id : string option;
    pinned_ts : string option;
  }

  let wire_priority_group_codec =
    Jsont.Object.map ~kind:"JetStream consumer priority group"
      (fun group pinned_client_id pinned_ts ->
        { group; pinned_client_id; pinned_ts })
    |> Jsont.Object.mem "group" Jsont.string ~enc:(fun value -> value.group)
    |> Jsont.Object.opt_mem "pinned_client_id" Jsont.string ~enc:(fun value ->
        value.pinned_client_id)
    |> Jsont.Object.opt_mem "pinned_ts" Jsont.string ~enc:(fun value ->
        value.pinned_ts)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  module Priority_group = struct
    type t = {
      name : string;
      pinned_client_id : string option;
      pinned_at : Ptime.t option;
    }

    let make ~name ~pinned_client_id ~pinned_at =
      { name; pinned_client_id; pinned_at }

    let name value = value.name
    let pinned_client_id value = value.pinned_client_id
    let pinned_at value = value.pinned_at
  end

  let priority_timestamp_of_wire = function
    | None | Some "" -> Ok None
    | Some raw -> (
        match Ptime.of_rfc3339 ~strict:true raw with
        | Ok (value, _, _) when is_zero_pause_until value -> Ok None
        | Ok (value, _, _) -> Ok (Some value)
        | Error _ ->
            Error
              (Error.Invalid_config
                 (Error.Invalid_consumer_priority_timestamp raw)))

  let priority_group_of_wire (value : wire_priority_group) =
    let ( let* ) value f =
      match value with Error error -> Error error | Ok value -> f value
    in
    let* () =
      match Config.validate_priority_group value.group with
      | Ok () -> Ok ()
      | Error error -> Error (Error.Invalid_config error)
    in
    let* pinned_at = priority_timestamp_of_wire value.pinned_ts in
    Ok
      (Priority_group.make ~name:value.group
         ~pinned_client_id:value.pinned_client_id ~pinned_at)

  let priority_group_states_of_wire = function
    | None -> Ok []
    | Some values ->
        List.fold_left
          (fun result value ->
            match result with
            | Error _ -> result
            | Ok values -> (
                match priority_group_of_wire value with
                | Error error -> Error error
                | Ok value -> Ok (value :: values)))
          (Ok []) values
        |> Result.map List.rev

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
    paused : bool option;
    pause_remaining : int64 option;
    priority_groups : wire_priority_group list option;
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
        paused
        pause_remaining
        priority_groups
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
          paused;
          pause_remaining;
          priority_groups;
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
    |> Jsont.Object.opt_mem "paused" Jsont.bool ~enc:(fun value -> value.paused)
    |> Jsont.Object.opt_mem "pause_remaining" Jsont.int64 ~enc:(fun value ->
        value.pause_remaining)
    |> Jsont.Object.opt_mem "priority_groups"
         (Jsont.list wire_priority_group_codec) ~enc:(fun value ->
           value.priority_groups)
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
      paused : bool;
      pause_remaining : Mtime.Span.t option;
      priority_groups : Priority_group.t list;
    }

    let name value = value.name
    let stream_name value = value.stream_name
    let created value = value.created
    let config value = value.config
    let paused value = value.paused
    let pause_until value = Config.pause_until value.config
    let pause_remaining value = value.pause_remaining
    let priority_groups value = value.priority_groups
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

  module Pause = struct
    type t = {
      paused : bool;
      pause_until : Ptime.t option;
      pause_remaining : Mtime.Span.t option;
    }

    let paused value = value.paused
    let pause_until value = value.pause_until
    let pause_remaining value = value.pause_remaining
  end

  type pause_request = { pause_until : string option }

  let pause_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer pause request"
      (fun pause_until -> { pause_until })
    |> Jsont.Object.opt_mem "pause_until" Jsont.string ~enc:(fun value ->
        value.pause_until)
    |> Jsont.Object.finish

  type pause_response = {
    error : api_error option;
    paused : bool;
    pause_until : string option;
    pause_remaining : int64 option;
  }

  let pause_response_codec =
    Jsont.Object.map ~kind:"JetStream consumer pause response"
      (fun error paused pause_until pause_remaining ->
        { error; paused; pause_until; pause_remaining })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.mem "paused" Jsont.bool ~enc:(fun value -> value.paused)
    |> Jsont.Object.opt_mem "pause_until" Jsont.string ~enc:(fun value ->
        value.pause_until)
    |> Jsont.Object.opt_mem "pause_remaining" Jsont.int64 ~enc:(fun value ->
        value.pause_remaining)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  let decode_pause_response message =
    match decode pause_response_codec message with
    | Error error -> Error error
    | Ok { error = Some error; _ } -> Error (Error.Api error)
    | Ok response -> (
        match pause_until_of_wire response.pause_until with
        | Error error -> Error error
        | Ok pause_until ->
            Ok
              {
                Pause.paused = response.paused;
                pause_until;
                pause_remaining =
                  Option.map Mtime.Span.of_uint64_ns response.pause_remaining;
              })

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
                  | Ok (config, config_unknown) -> (
                      match
                        priority_group_states_of_wire response.priority_groups
                      with
                      | Error error -> Error error
                      | Ok priority_groups ->
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
                              paused =
                                Option.value ~default:false response.paused;
                              pause_remaining =
                                Option.map Mtime.Span.of_uint64_ns
                                  response.pause_remaining;
                              priority_groups;
                            }))))

  let rec equal_string_lists left right =
    match (left, right) with
    | [], [] -> true
    | left :: left_tail, right :: right_tail ->
        String.equal left right && equal_string_lists left_tail right_tail
    | _ -> false

  let normalized_priority_policy = function
    | None | Some "" | Some "none" -> None
    | Some value -> Some value

  let validate_priority_update ~(current : wire_config) value =
    let current_groups = Option.value ~default:[] current.priority_groups in
    let desired_groups = Config.priority_groups value in
    let current_policy = normalized_priority_policy current.priority_policy in
    let desired_policy =
      priority_policy_to_wire (Config.priority_policy value)
    in
    let same_policy =
      match (current_policy, desired_policy) with
      | None, None -> true
      | Some current, Some desired -> String.equal current desired
      | None, Some _ | Some _, None -> false
    in
    if equal_string_lists current_groups desired_groups && same_policy then
      Ok ()
    else Error (Error.Invalid_config Error.Invalid_consumer_priority_update)

  let wire_config_for_update ~current value =
    let metadata = Config.metadata value in
    let value = wire_config value in
    let priority_timeout =
      match normalized_priority_policy current.priority_policy with
      | Some "pinned_client" ->
          Some (Option.value ~default:0L value.priority_timeout)
      | _ -> current.priority_timeout
    in
    {
      value with
      durable_name =
        (match (current.durable_name, value.durable_name) with
        | Some durable_name, None -> Some durable_name
        | _ -> value.durable_name);
      idle_heartbeat = Some (Option.value ~default:0L value.idle_heartbeat);
      ack_wait = Some (Option.value ~default:0L value.ack_wait);
      max_deliver = Some (Option.value ~default:(-1) value.max_deliver);
      filter_subject = Some (Option.value ~default:"" value.filter_subject);
      filter_subjects = Some (Option.value ~default:[] value.filter_subjects);
      backoff = Some (Option.value ~default:[] value.backoff);
      sample_frequency =
        Some (Option.value ~default:"0%" value.sample_frequency);
      rate_limit = Some (Option.value ~default:0L value.rate_limit);
      replicas = Some (Option.value ~default:0 value.replicas);
      metadata =
        Some
          (Option.value ~default:String_map.empty (metadata_to_wire metadata));
      max_ack_pending = Some (Option.value ~default:0 value.max_ack_pending);
      max_waiting = Some (Option.value ~default:0 value.max_waiting);
      max_batch = Some (Option.value ~default:0 value.max_batch);
      max_expires = Some (Option.value ~default:0L value.max_expires);
      max_bytes = Some (Option.value ~default:0 value.max_bytes);
      inactive_threshold =
        Some (Option.value ~default:0L value.inactive_threshold);
      pause_until = current.pause_until;
      priority_groups = current.priority_groups;
      priority_policy = current.priority_policy;
      priority_timeout;
      unknown = current.unknown;
    }

  type jetstream = t
  type stream = Stream.t

  type t = {
    jetstream : jetstream;
    stream : stream;
    name : string;
    pause_until : Ptime.t option ref;
    priority_pins : string String_map.t ref;
  }

  let priority_pin consumer ~group =
    String_map.find_opt group !(consumer.priority_pins)

  let set_priority_pin consumer ~group ~pin =
    match pin with
    | None ->
        consumer.priority_pins :=
          String_map.remove group !(consumer.priority_pins)
    | Some pin ->
        consumer.priority_pins :=
          String_map.add group pin !(consumer.priority_pins)

  type next_request = {
    expires : int64;
    batch : int;
    max_bytes : int option;
    idle_heartbeat : int64 option;
    group : string option;
    min_pending : int64 option;
    min_ack_pending : int64 option;
    id : string option;
    priority : int option;
  }

  let next_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer pull request"
      (fun
        expires
        batch
        max_bytes
        idle_heartbeat
        group
        min_pending
        min_ack_pending
        id
        priority
      ->
        {
          expires;
          batch;
          max_bytes;
          idle_heartbeat;
          group;
          min_pending;
          min_ack_pending;
          id;
          priority;
        })
    |> Jsont.Object.mem "expires" Jsont.int64 ~enc:(fun value -> value.expires)
    |> Jsont.Object.mem "batch" Jsont.int ~enc:(fun value -> value.batch)
    |> Jsont.Object.opt_mem "max_bytes" Jsont.int ~enc:(fun value ->
        value.max_bytes)
    |> Jsont.Object.opt_mem "idle_heartbeat" Jsont.int64 ~enc:(fun value ->
        value.idle_heartbeat)
    |> Jsont.Object.opt_mem "group" Jsont.string ~enc:(fun value -> value.group)
    |> Jsont.Object.opt_mem "min_pending" Jsont.int64 ~enc:(fun value ->
        value.min_pending)
    |> Jsont.Object.opt_mem "min_ack_pending" Jsont.int64 ~enc:(fun value ->
        value.min_ack_pending)
    |> Jsont.Object.opt_mem "id" Jsont.string ~enc:(fun value -> value.id)
    |> Jsont.Object.opt_mem "priority" Jsont.int ~enc:(fun value ->
        value.priority)
    |> Jsont.Object.finish

  let default_fetch_expires = Mtime.Span.(5 * s)
  let fetch_expiry_leeway = Mtime.Span.(10 * ms)

  let add_fetch_expiry_leeway expires =
    let expires_ns = Mtime.Span.to_uint64_ns expires in
    let leeway_ns = Mtime.Span.to_uint64_ns fetch_expiry_leeway in
    if Int64.compare expires_ns (Int64.sub Int64.max_int leeway_ns) >= 0 then
      Mtime.Span.max_span
    else Mtime.Span.of_uint64_ns (Int64.add expires_ns leeway_ns)

  let validate_pull_options ~group ~min_pending ~min_ack_pending ~priority =
    let ( let* ) value f =
      match value with Error error -> Error error | Ok value -> f value
    in
    let* () =
      match group with
      | None
        when Option.is_some min_pending
             || Option.is_some min_ack_pending
             || Option.is_some priority ->
          Error (Error.Invalid_priority_group "")
      | None -> Ok ()
      | Some group -> (
          match Config.validate_priority_group group with
          | Ok () -> Ok ()
          | Error _ -> Error (Error.Invalid_priority_group group))
    in
    let* () =
      match min_pending with
      | None -> Ok ()
      | Some value when Int64.compare value 0L >= 0 -> Ok ()
      | Some value ->
          Error
            (Error.Invalid_priority_threshold { field = "min_pending"; value })
    in
    let* () =
      match min_ack_pending with
      | None -> Ok ()
      | Some value when Int64.compare value 0L >= 0 -> Ok ()
      | Some value ->
          Error
            (Error.Invalid_priority_threshold
               { field = "min_ack_pending"; value })
    in
    match priority with
    | None -> Ok ()
    | Some value when Int.compare value 0 >= 0 && Int.compare value 9 <= 0 ->
        Ok ()
    | Some value -> Error (Error.Invalid_priority value)

  let validate_fetch ~batch ~expires ~max_bytes ~idle_heartbeat ~group
      ~min_pending ~min_ack_pending ~priority =
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
    let* () =
      validate_pull_options ~group ~min_pending ~min_ack_pending ~priority
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
    | Status_flow_control
    | Status_request_expired
    | Status_batch_completed
    | Status_max_bytes
    | Status_pin_lost
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
    else if
      Int.equal code 100
      && (contains ~needle:"flowcontrol" normalized
         || contains ~needle:"flow control" normalized)
    then Status_flow_control
    else if Int.equal code 408 then Status_request_expired
    else if Int.equal code 423 then Status_pin_lost
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
    | Status_pin_lost | Status_conflict ->
        Error (Error.Conflict { code; description })
    | Status_idle_heartbeat | Status_flow_control | Status_unexpected ->
        Error (Error.Unexpected_status { code; description })

  let pull_status_result status =
    let code = status.Nats.Op.code in
    let description = status.Nats.Op.description in
    match classify_status status with
    | Status_request_expired | Status_batch_completed -> Ok ()
    | Status_max_bytes | Status_pin_lost | Status_conflict ->
        Error (Error.Conflict { code; description })
    | Status_consumer_deleted -> Error Error.Consumer_deleted
    | Status_idle_heartbeat | Status_flow_control | Status_unexpected ->
        Error (Error.Unexpected_status { code; description })

  let push_status_error status =
    let code = status.Nats.Op.code in
    let description = status.Nats.Op.description in
    match classify_status status with
    | Status_consumer_deleted -> Error.Consumer_deleted
    | Status_max_bytes | Status_pin_lost | Status_conflict ->
        Error.Conflict { code; description }
    | Status_idle_heartbeat | Status_flow_control | Status_request_expired
    | Status_batch_completed | Status_unexpected ->
        Error.Unexpected_status { code; description }

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

  let fetch ?expires ?idle_heartbeat ?max_bytes ?group ?min_pending
      ?min_ack_pending ?priority consumer ~batch =
    let expires = Option.value expires ~default:default_fetch_expires in
    match
      validate_fetch ~batch ~expires ~max_bytes ~idle_heartbeat ~group
        ~min_pending ~min_ack_pending ~priority
    with
    | Error error -> Error error
    | Ok () ->
        let base_request =
          {
            expires = Mtime.Span.to_uint64_ns expires;
            batch;
            max_bytes;
            idle_heartbeat = Option.map Mtime.Span.to_uint64_ns idle_heartbeat;
            group;
            min_pending;
            min_ack_pending;
            id = None;
            priority;
          }
        in
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
            let publish_request () =
              let id =
                Option.bind group (fun group -> priority_pin consumer ~group)
              in
              let request = { base_request with batch = batch - !count; id } in
              match encode next_request_codec request with
              | Error error -> Error error
              | Ok payload -> (
                  match
                    Connection.publish connection ~reply_to:inbox subject
                      payload
                  with
                  | Error error -> Error (Error.Connection error)
                  | Ok () -> Ok ())
            in
            match publish_request () with
            | Error error -> Error error
            | Ok () -> (
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
                              (match
                                 Nats.Header.find "Nats-Pin-Id"
                                   (Msg.headers message)
                               with
                              | Some pin_id when not (String.equal pin_id "") ->
                                  Option.iter
                                    (fun group ->
                                      set_priority_pin consumer ~group
                                        ~pin:(Some pin_id))
                                    group
                              | Some _ | None -> ());
                              heartbeat_deadline :=
                                heartbeat_deadline_at connection idle_heartbeat;
                              messages := message :: !messages;
                              count := !count + 1)
                      | Some status -> (
                          match classify_status status with
                          | Status_pin_lost -> (
                              Option.iter
                                (fun group ->
                                  set_priority_pin consumer ~group ~pin:None)
                                group;
                              heartbeat_deadline :=
                                heartbeat_deadline_at connection idle_heartbeat;
                              match publish_request () with
                              | Ok () -> ()
                              | Error error -> terminal := Some (Error error))
                          | Status_idle_heartbeat -> (
                              match idle_heartbeat with
                              | Some _ ->
                                  heartbeat_deadline :=
                                    heartbeat_deadline_at connection
                                      idle_heartbeat
                              | None -> (
                                  match status_result status with
                                  | Ok () ->
                                      terminal := Some (Ok (List.rev !messages))
                                  | Error error ->
                                      terminal := Some (Error error)))
                          | _ -> (
                              match status_result status with
                              | Ok () ->
                                  terminal := Some (Ok (List.rev !messages))
                              | Error error -> terminal := Some (Error error))))
                done;
                match !terminal with
                | Some result -> result
                | None -> Ok (List.rev !messages)))

  module Pull = struct
    type consumer = t
    type state = Open | Closed | Failed of Error.t

    type t = {
      consumer : consumer;
      connection : Connection.t;
      subscription : Connection.Subscription.t;
      inbox : Nats.Subject.t;
      subject : Nats.Subject.t;
      request : next_request;
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
            let id =
              Option.bind pull.request.group (fun group ->
                  priority_pin pull.consumer ~group)
            in
            let request = { pull.request with id } in
            match encode next_request_codec request with
            | Error error -> Error error
            | Ok payload -> (
                match
                  Connection.publish pull.connection ~reply_to:pull.inbox
                    pull.subject payload
                with
                | Ok () -> (
                    match pull.state with
                    | Open ->
                        pull.remaining <- pull.batch;
                        pull.heartbeat_deadline <-
                          heartbeat_deadline_at pull.connection
                            pull.idle_heartbeat;
                        Ok ()
                    | Closed -> Error Error.Pull_closed
                    | Failed error -> Error error)
                | Error error -> connection_error pull error))

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
          | Status_pin_lost ->
              Option.iter
                (fun group -> set_priority_pin pull.consumer ~group ~pin:None)
                pull.request.group;
              pull.remaining <- 0;
              pull.heartbeat_deadline <- None;
              Ok None
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
              (match Nats.Header.find "Nats-Pin-Id" (Msg.headers message) with
              | Some pin_id when not (String.equal pin_id "") ->
                  Option.iter
                    (fun group ->
                      set_priority_pin pull.consumer ~group ~pin:(Some pin_id))
                    pull.request.group
              | Some _ | None -> ());
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
                        | None -> Connection.Subscription.next pull.subscription
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

    let v ~sw ?(batch = 1) ?expires ?idle_heartbeat ?max_bytes ?group
        ?min_pending ?min_ack_pending ?priority consumer =
      let expires = Option.value expires ~default:default_fetch_expires in
      match
        validate_fetch ~batch ~expires ~max_bytes ~idle_heartbeat ~group
          ~min_pending ~min_ack_pending ~priority
      with
      | Error error -> Error error
      | Ok () -> (
          let request =
            {
              expires = Mtime.Span.to_uint64_ns expires;
              batch;
              max_bytes;
              idle_heartbeat = Option.map Mtime.Span.to_uint64_ns idle_heartbeat;
              group;
              min_pending;
              min_ack_pending;
              id = None;
              priority;
            }
          in
          let connection = consumer.jetstream.connection in
          let inbox = Connection.fresh_inbox connection in
          let filter =
            Nats.Subject.Filter.literal (Nats.Subject.to_string inbox)
          in
          match
            Connection.subscribe connection ~replay_on_reconnect:false filter
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
                  request;
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
              Ok pull)

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
    | Ok () ->
        Ok
          {
            jetstream = stream.jetstream;
            stream;
            name;
            pause_until = ref None;
            priority_pins = ref String_map.empty;
          }
    | Error error -> Error (Error.Invalid_config error)

  let name value = value.name
  let stream value = value.stream

  let create ?timeout (stream : Stream.t) config =
    let jetstream = stream.jetstream in
    let stream_name = Stream.name stream in
    let subject =
      match Config.durable_name config with
      | None -> api_subject jetstream [ "CONSUMER"; "CREATE"; stream_name ]
      | Some name ->
          api_subject jetstream [ "CONSUMER"; "CREATE"; stream_name; name ]
    in
    let request = { stream_name; config = wire_config config; action = None } in
    match encode create_request_codec request with
    | Error error -> Error error
    | Ok payload -> (
        match
          request_msg ?timeout jetstream (Nats.Message.v ~subject payload)
        with
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
                    | Ok (actual_config, _) ->
                        Ok
                          {
                            jetstream;
                            stream;
                            name;
                            pause_until = ref (Config.pause_until actual_config);
                            priority_pins = ref String_map.empty;
                          }
                    | Error error -> Error error))))

  let info_response ?timeout consumer =
    let subject =
      api_subject consumer.jetstream
        [ "CONSUMER"; "INFO"; Stream.name consumer.stream; consumer.name ]
    in
    match
      request_msg ?timeout consumer.jetstream (Nats.Message.v ~subject "")
    with
    | Error error -> Error error
    | Ok message -> (
        match decode_response message with
        | Error error -> Error error
        | Ok response -> Ok response)

  let info ?timeout consumer =
    match info_response ?timeout consumer with
    | Error error -> Error error
    | Ok response -> (
        match
          info_of_response ~stream:consumer.stream ~expected_name:consumer.name
            response
        with
        | Error error -> Error error
        | Ok info ->
            consumer.pause_until := Info.pause_until info;
            Ok info)

  let pause ?timeout consumer ~until =
    let request =
      { pause_until = Some (Ptime.to_rfc3339 ~frac_s:9 ~tz_offset_s:0 until) }
    in
    let subject =
      api_subject consumer.jetstream
        [ "CONSUMER"; "PAUSE"; Stream.name consumer.stream; consumer.name ]
    in
    match encode pause_request_codec request with
    | Error error -> Error error
    | Ok payload -> (
        match
          request_msg ?timeout consumer.jetstream
            (Nats.Message.v ~subject payload)
        with
        | Error error -> Error error
        | Ok message -> (
            match decode_pause_response message with
            | Error error -> Error error
            | Ok response ->
                consumer.pause_until := Pause.pause_until response;
                Ok response))

  let resume ?timeout consumer =
    let subject =
      api_subject consumer.jetstream
        [ "CONSUMER"; "PAUSE"; Stream.name consumer.stream; consumer.name ]
    in
    match
      request_msg ?timeout consumer.jetstream (Nats.Message.v ~subject "")
    with
    | Error error -> Error error
    | Ok message -> (
        match decode_pause_response message with
        | Error error -> Error error
        | Ok response ->
            consumer.pause_until := Pause.pause_until response;
            Ok response)

  type unpin_request = { group : string }

  let unpin_request_codec =
    Jsont.Object.map ~kind:"JetStream consumer unpin request" (fun group ->
        { group })
    |> Jsont.Object.mem "group" Jsont.string ~enc:(fun value -> value.group)
    |> Jsont.Object.finish

  type unpin_response = { error : api_error option }

  let unpin_response_codec =
    Jsont.Object.map ~kind:"JetStream consumer unpin response" (fun error ->
        { error })
    |> Jsont.Object.opt_mem "error" api_error_codec ~enc:(fun value ->
        value.error)
    |> Jsont.Object.skip_unknown |> Jsont.Object.finish

  let decode_unpin_response message =
    match decode unpin_response_codec message with
    | Error error -> Error error
    | Ok { error = Some error } -> Error (Error.Api error)
    | Ok { error = None } -> Ok ()

  let unpin ?timeout consumer ~group =
    match Config.validate_priority_group group with
    | Error _ -> Error (Error.Invalid_priority_group group)
    | Ok () -> (
        let subject =
          api_subject consumer.jetstream
            [ "CONSUMER"; "UNPIN"; Stream.name consumer.stream; consumer.name ]
        in
        match encode unpin_request_codec { group } with
        | Error error -> Error error
        | Ok payload -> (
            match
              request_msg ?timeout consumer.jetstream
                (Nats.Message.v ~subject payload)
            with
            | Error error -> Error error
            | Ok message -> (
                match decode_unpin_response message with
                | Error error -> Error error
                | Ok () ->
                    set_priority_pin consumer ~group ~pin:None;
                    Ok ())))

  let update ?timeout consumer config =
    match Config.durable_name config with
    | Some actual when not (String.equal actual consumer.name) ->
        Error
          (Error.Unexpected_consumer_name { expected = consumer.name; actual })
    | _ -> (
        match info_response ?timeout consumer with
        | Error error -> Error error
        | Ok response -> (
            match
              info_of_response ~stream:consumer.stream
                ~expected_name:consumer.name response
            with
            | Error error -> Error error
            | Ok current -> (
                consumer.pause_until := Info.pause_until current;
                match response.config with
                | None -> Error (Error.Missing_field "config")
                | Some current -> (
                    match validate_priority_update ~current config with
                    | Error error -> Error error
                    | Ok () -> (
                        let subject =
                          api_subject consumer.jetstream
                            [
                              "CONSUMER";
                              "CREATE";
                              Stream.name consumer.stream;
                              consumer.name;
                            ]
                        in
                        let request =
                          {
                            stream_name = Stream.name consumer.stream;
                            config = wire_config_for_update ~current config;
                            action = Some "update";
                          }
                        in
                        match encode create_request_codec request with
                        | Error error -> Error error
                        | Ok payload -> (
                            match
                              request_msg ?timeout consumer.jetstream
                                (Nats.Message.v ~subject payload)
                            with
                            | Error error -> Error error
                            | Ok message -> (
                                match decode_response message with
                                | Error error -> Error error
                                | Ok response -> (
                                    match
                                      info_of_response ~stream:consumer.stream
                                        ~expected_name:consumer.name response
                                    with
                                    | Error error -> Error error
                                    | Ok info ->
                                        consumer.pause_until :=
                                          Info.pause_until info;
                                        Ok info))))))))

  let delete ?timeout consumer =
    let subject =
      api_subject consumer.jetstream
        [ "CONSUMER"; "DELETE"; Stream.name consumer.stream; consumer.name ]
    in
    match
      request_msg ?timeout consumer.jetstream (Nats.Message.v ~subject "")
    with
    | Error error -> Error error
    | Ok message -> (
        match decode_response message with
        | Ok _ -> Ok ()
        | Error error -> Error error)

  module Push = struct
    type consumer = t
    type state = Open | Closed | Failed of Error.t

    type t = {
      mutable consumer : consumer;
      connection : Connection.t;
      mutable subscription : Connection.Subscription.t;
      mutable config : Config.t;
      mutable last_stream_sequence : int64 option;
      mutable idle_heartbeat : Mtime.Span.t option;
      mutable recovery_pending : bool;
      mutable heartbeat_deadline : Mtime.t option;
      mutable state : state;
      mutable hook : Eio.Switch.hook option;
      owns_consumer : bool;
      initial_pending : int64;
    }

    let fail push error =
      match push.state with
      | Open ->
          push.state <- Failed error;
          ignore (release_subscription push.subscription)
      | Closed | Failed _ -> ()

    let connection_error push error =
      let error = Error.Connection error in
      fail push error;
      Error error

    let subscription_error push error =
      match (push.state, error) with
      | Closed, (Core_error.Closed | Core_error.Draining) ->
          Error Error.Push_closed
      | _, error -> connection_error push error

    let close push =
      match push.state with
      | Closed -> Ok ()
      | Open | Failed _ -> (
          push.state <- Closed;
          Option.iter
            (fun hook -> ignore (Eio.Switch.try_remove_hook hook))
            push.hook;
          push.hook <- None;
          let subscription_result =
            match release_subscription push.subscription with
            | None -> Ok ()
            | Some error -> Error (Error.Connection error)
          in
          let consumer_result =
            if push.owns_consumer then
              match Eio.Cancel.protect (fun () -> delete push.consumer) with
              | Ok () -> Ok ()
              | Error error -> Error error
            else Ok ()
          in
          match subscription_result with
          | Error error -> Error error
          | Ok () -> consumer_result)

    let heartbeat_missed push =
      match push.heartbeat_deadline with
      | Some deadline ->
          Mtime.compare (Connection.now push.connection) deadline >= 0
      | None -> false

    let reset_heartbeat push =
      push.heartbeat_deadline <-
        heartbeat_deadline_at push.connection push.idle_heartbeat

    let timeout_error = Error.Connection Core_error.Timeout

    let remaining_timeout push deadline =
      match deadline with
      | None -> Ok None
      | Some deadline ->
          let now = Connection.now push.connection in
          if Mtime.compare now deadline >= 0 then Error timeout_error
          else Ok (Some (Mtime.span now deadline))

    let is_timeout = function
      | Error.Connection Core_error.Timeout -> true
      | _ -> false

    let is_core_timeout = function Core_error.Timeout -> true | _ -> false

    let is_recoverable = function
      | Error.Connection Core_error.Disconnected -> true
      | _ -> false

    let is_missing_consumer = function
      | Error.Consumer_deleted -> true
      | Error.Api { err_code = Some 10014; _ } -> true
      | _ -> false

    let next_stream_sequence sequence =
      if Int64.equal sequence Int64.max_int then Int64.max_int
      else Int64.add sequence 1L

    let resume_config push =
      let deliver_policy =
        match push.last_stream_sequence with
        | None -> Config.New
        | Some sequence ->
            Config.By_start_sequence (next_stream_sequence sequence)
      in
      match
        Config.v
          ?durable_name:(Config.durable_name push.config)
          ?description:(Config.description push.config)
          ?deliver_subject:(Config.deliver_subject push.config)
          ?deliver_group:(Config.deliver_group push.config)
          ?idle_heartbeat:(Config.idle_heartbeat push.config)
          ?flow_control:(Config.flow_control push.config)
          ~deliver_policy
          ~ack_policy:(Config.ack_policy push.config)
          ?ack_wait:(Config.ack_wait push.config)
          ?max_deliver:(Config.max_deliver push.config)
          ?filter_subject:(Config.filter_subject push.config)
          ~filter_subjects:(Config.filter_subjects push.config)
          ~backoff:(Config.backoff push.config)
          ?pause_until:!(push.consumer.pause_until)
          ?sample_frequency:(Config.sample_frequency push.config)
          ?rate_limit:(Config.rate_limit push.config)
          ?replicas:(Config.replicas push.config)
          ~metadata:(Config.metadata push.config)
          ~replay_policy:(Config.replay_policy push.config)
          ?max_ack_pending:(Config.max_ack_pending push.config)
          ?max_waiting:(Config.max_waiting push.config)
          ?max_batch:(Config.max_batch push.config)
          ?max_expires:(Config.max_expires push.config)
          ?max_bytes:(Config.max_bytes push.config)
          ?headers_only:(Config.headers_only push.config)
          ?inactive_threshold:(Config.inactive_threshold push.config)
          ?mem_storage:(Config.mem_storage push.config)
          ()
      with
      | Ok config -> Ok config
      | Error error -> Error (Error.Invalid_config error)

    let option_equal equal first second =
      match (first, second) with
      | None, None -> true
      | Some first, Some second -> equal first second
      | None, Some _ | Some _, None -> false

    let same_delivery_config first second =
      option_equal Nats.Subject.equal
        (Config.deliver_subject first)
        (Config.deliver_subject second)
      && option_equal Nats.Queue_group.equal
           (Config.deliver_group first)
           (Config.deliver_group second)

    let replace_subscription push config subject =
      let filter =
        Nats.Subject.Filter.literal (Nats.Subject.to_string subject)
      in
      match
        Connection.subscribe push.connection
          ?queue_group:(Config.deliver_group config)
          filter
      with
      | Error error -> Error (Error.Connection error)
      | Ok subscription -> (
          let old_subscription = push.subscription in
          push.subscription <- subscription;
          match release_subscription old_subscription with
          | None -> Ok ()
          | Some error -> Error (Error.Connection error))

    let restore_consumer push ~deadline =
      match remaining_timeout push deadline with
      | Error error -> Error error
      | Ok timeout -> (
          match info ?timeout push.consumer with
          | Ok info -> (
              let config = Info.config info in
              match Config.deliver_subject config with
              | None -> Error Error.Not_push_consumer
              | Some subject -> (
                  let subscription_result =
                    if same_delivery_config push.config config then Ok ()
                    else replace_subscription push config subject
                  in
                  match subscription_result with
                  | Error error -> Error error
                  | Ok () ->
                      push.config <- config;
                      push.idle_heartbeat <- Config.idle_heartbeat config;
                      push.recovery_pending <- false;
                      reset_heartbeat push;
                      Ok ()))
          | Error error when is_missing_consumer error -> (
              match Config.durable_name push.config with
              | Some _ -> Error Error.Consumer_deleted
              | None -> (
                  match remaining_timeout push deadline with
                  | Error error -> Error error
                  | Ok timeout -> (
                      match resume_config push with
                      | Error error -> Error error
                      | Ok config -> (
                          match create ?timeout push.consumer.stream config with
                          | Error error -> Error error
                          | Ok consumer ->
                              push.consumer <- consumer;
                              push.config <- config;
                              push.recovery_pending <- false;
                              reset_heartbeat push;
                              Ok ()))))
          | Error error -> Error error)

    let recover push ~deadline =
      match Connection.Subscription.recovery push.subscription with
      | Connection.Subscription.Detached _ -> (
          push.heartbeat_deadline <- None;
          let from = Connection.Subscription.recovery push.subscription in
          match remaining_timeout push deadline with
          | Error error -> Error (`Timeout error)
          | Ok timeout -> (
              match
                Connection.Subscription.await_recovery ?timeout ~from
                  push.subscription
              with
              | Ok _ ->
                  push.recovery_pending <- true;
                  Ok `Retry
              | Error error when is_core_timeout error ->
                  Error (`Timeout timeout_error)
              | Error error -> Stdlib.Error (`Fatal (Error.Connection error))))
      | Connection.Subscription.Attached _ when push.recovery_pending -> (
          match restore_consumer push ~deadline with
          | Ok () -> Ok `Ready
          | Error error when is_timeout error -> Error (`Timeout error)
          | Error error when is_recoverable error -> Ok `Retry
          | Error error -> Stdlib.Error (`Fatal error))
      | Connection.Subscription.Attached _ -> Ok `Ready

    let respond_flow_control push subject =
      match Connection.publish push.connection subject "" with
      | Ok () -> Ok ()
      | Error error -> Error (Error.Connection error)

    let consume_delivery push (delivery : Connection.Subscription.delivery) =
      match delivery.status with
      | Some status -> (
          let control_result =
            match
              (classify_status status, Nats.Message.reply_to delivery.message)
            with
            | Status_idle_heartbeat, None -> Ok ()
            | Status_idle_heartbeat, Some subject ->
                respond_flow_control push subject
            | Status_flow_control, Some subject ->
                respond_flow_control push subject
            | Status_flow_control, None -> Error (push_status_error status)
            | _ -> Error (push_status_error status)
          in
          match control_result with
          | Error error -> Error error
          | Ok () ->
              reset_heartbeat push;
              Ok None)
      | None -> (
          match
            Msg.of_message ~jetstream:push.consumer.jetstream
              ~stream_name:(Stream.name push.consumer.stream)
              ~consumer_name:push.consumer.name delivery.message
          with
          | Error error -> Error error
          | Ok message ->
              push.last_stream_sequence <- Some (Msg.stream_sequence message);
              reset_heartbeat push;
              Ok (Some message))

    let next_loop push ~deadline =
      let result = ref None in
      let handle_delivery delivery =
        match consume_delivery push delivery with
        | Ok None -> ()
        | Ok (Some message) -> result := Some (Ok message)
        | Error error ->
            fail push error;
            result := Some (Error error)
      in
      while Option.is_none !result do
        match push.state with
        | Closed -> result := Some (Error Error.Push_closed)
        | Failed error -> result := Some (Error error)
        | Open -> (
            match recover push ~deadline with
            | Error (`Timeout error) -> result := Some (Error error)
            | Error (`Fatal error) ->
                fail push error;
                result := Some (Error error)
            | Ok `Retry -> ()
            | Ok `Ready -> (
                match
                  Connection.Subscription.next_or_recovery_nonblocking
                    push.subscription
                with
                | Some (Ok (Connection.Subscription.Delivery delivery)) ->
                    handle_delivery delivery
                | Some (Ok Connection.Subscription.Recovery) ->
                    push.recovery_pending <- true
                | Some (Error error) ->
                    result := Some (subscription_error push error)
                | None -> (
                    let deadline_reached =
                      match deadline with
                      | Some deadline ->
                          Mtime.compare
                            (Connection.now push.connection)
                            deadline
                          >= 0
                      | None -> false
                    in
                    if heartbeat_missed push then (
                      let error = Error.Missing_heartbeat in
                      fail push error;
                      result := Some (Error error))
                    else if deadline_reached then
                      result :=
                        Some (Error (Error.Connection Core_error.Timeout))
                    else
                      let wait_deadline =
                        earliest_deadline deadline push.heartbeat_deadline
                      in
                      let wait_result =
                        match wait_deadline with
                        | None ->
                            Connection.Subscription.next_or_recovery
                              push.subscription
                        | Some wait_deadline ->
                            let now = Connection.now push.connection in
                            if Mtime.compare now wait_deadline >= 0 then
                              Error Core_error.Timeout
                            else
                              let timeout = Mtime.span now wait_deadline in
                              Connection.Subscription
                              .next_or_recovery_with_timeout ~timeout
                                push.subscription
                      in
                      match wait_result with
                      | Ok (Connection.Subscription.Delivery delivery) ->
                          handle_delivery delivery
                      | Ok Connection.Subscription.Recovery ->
                          push.recovery_pending <- true
                      | Error Core_error.Timeout ->
                          if heartbeat_missed push then (
                            let error = Error.Missing_heartbeat in
                            fail push error;
                            result := Some (Error error))
                          else
                            result :=
                              Some (Error (Error.Connection Core_error.Timeout))
                      | Error error ->
                          result := Some (subscription_error push error))))
      done;
      match !result with Some result -> result | None -> assert false

    let next push = next_loop push ~deadline:None

    let next_with_timeout ~timeout push =
      if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
        Error (Error.Connection (Core_error.Invalid_timeout "push"))
      else
        let deadline =
          match Mtime.add_span (Connection.now push.connection) timeout with
          | Some deadline -> deadline
          | None -> Mtime.max_stamp
        in
        next_loop push ~deadline:(Some deadline)

    let iter push ~f =
      let result = ref None in
      while Option.is_none !result do
        match next push with
        | Ok message -> f message
        | Error Error.Push_closed -> result := Some (Ok ())
        | Error error -> result := Some (Error error)
      done;
      match !result with Some result -> result | None -> assert false

    let make ~sw ~owns_consumer ~initial_pending ~consumer ~subscription ~config
        =
      let connection = consumer.jetstream.connection in
      let push =
        {
          consumer;
          connection;
          subscription;
          config;
          last_stream_sequence = None;
          idle_heartbeat = Config.idle_heartbeat config;
          recovery_pending = false;
          heartbeat_deadline =
            heartbeat_deadline_at connection (Config.idle_heartbeat config);
          state = Open;
          hook = None;
          owns_consumer;
          initial_pending;
        }
      in
      let hook =
        Eio.Switch.on_release_cancellable sw (fun () ->
            Eio.Cancel.protect (fun () -> ignore (close push)))
      in
      push.hook <- Some hook;
      Ok push

    let v ~sw consumer =
      match info consumer with
      | Error error -> Error error
      | Ok info -> (
          let config = Info.config info in
          match Config.deliver_subject config with
          | None -> Error Error.Not_push_consumer
          | Some subject -> (
              let connection = consumer.jetstream.connection in
              let filter =
                Nats.Subject.Filter.literal (Nats.Subject.to_string subject)
              in
              match
                Connection.subscribe connection
                  ?queue_group:(Config.deliver_group config)
                  filter
              with
              | Error error -> Error (Error.Connection error)
              | Ok subscription ->
                  make ~sw ~owns_consumer:false
                    ~initial_pending:(Info.num_pending info) ~consumer
                    ~subscription ~config))

    let create ~sw (stream : Stream.t) config =
      if Option.is_some (Config.durable_name config) then
        Error
          (Error.Invalid_config
             (Error.Invalid_consumer_policy
                {
                  field = "durable_name";
                  value = "owned push consumers must be ephemeral";
                }))
      else
        let connection = stream.jetstream.connection in
        let config =
          {
            config with
            deliver_subject =
              (match Config.deliver_subject config with
              | Some subject -> Some subject
              | None -> Some (Connection.fresh_inbox connection));
            inactive_threshold =
              (match config.inactive_threshold with
              | Some threshold -> Some threshold
              | None -> Some Mtime.Span.(5 * min));
            mem_storage =
              (match config.mem_storage with
              | Some value -> Some value
              | None -> Some true);
          }
        in
        match Config.deliver_subject config with
        | None -> assert false
        | Some subject -> (
            let filter =
              Nats.Subject.Filter.literal (Nats.Subject.to_string subject)
            in
            match
              Connection.subscribe connection
                ?queue_group:(Config.deliver_group config)
                filter
            with
            | Error error -> Error (Error.Connection error)
            | Ok subscription ->
                let active_subscription = ref subscription in
                let created_consumer = ref None in
                let transferred = ref false in
                let cleanup () =
                  if not !transferred then (
                    transferred := true;
                    ignore (release_subscription !active_subscription);
                    match !created_consumer with
                    | None -> ()
                    | Some consumer ->
                        ignore (Eio.Cancel.protect (fun () -> delete consumer)))
                in
                Fun.protect ~finally:cleanup (fun () ->
                    match create stream config with
                    | Error error -> Error error
                    | Ok consumer -> (
                        created_consumer := Some consumer;
                        match info consumer with
                        | Error error -> Error error
                        | Ok info -> (
                            let actual_config = Info.config info in
                            match Config.deliver_subject actual_config with
                            | None -> Error Error.Not_push_consumer
                            | Some actual_subject -> (
                                let subscription_result =
                                  if
                                    Nats.Subject.equal subject actual_subject
                                    && option_equal Nats.Queue_group.equal
                                         (Config.deliver_group config)
                                         (Config.deliver_group actual_config)
                                  then Ok !active_subscription
                                  else
                                    match
                                      Connection.subscribe connection
                                        ?queue_group:
                                          (Config.deliver_group actual_config)
                                        (Nats.Subject.Filter.literal
                                           (Nats.Subject.to_string
                                              actual_subject))
                                    with
                                    | Error error ->
                                        Error (Error.Connection error)
                                    | Ok replacement -> (
                                        active_subscription := replacement;
                                        match
                                          release_subscription subscription
                                        with
                                        | None -> Ok replacement
                                        | Some error ->
                                            Error (Error.Connection error))
                                in
                                match subscription_result with
                                | Error error -> Error error
                                | Ok subscription -> (
                                    match
                                      make ~sw ~owns_consumer:true
                                        ~initial_pending:(Info.num_pending info)
                                        ~consumer ~subscription
                                        ~config:actual_config
                                    with
                                    | Error error -> Error error
                                    | Ok push ->
                                        transferred := true;
                                        Ok push))))))

    let consumer push = push.consumer
    let initial_pending push = push.initial_pending
  end

  module Ordered = struct
    type stream = Stream.t
    type consumer = t
    type state = Open | Closed | Failed of Error.t

    type t = {
      stream : stream;
      connection : Connection.t;
      sw : Eio.Switch.t;
      batch : int;
      expires : Mtime.Span.t;
      idle_heartbeat : Mtime.Span.t;
      max_bytes : int option;
      initial_deliver_policy : Config.deliver_policy;
      filter_subject : Nats.Subject.Filter.t option;
      mutable consumer : consumer option;
      mutable pull : Pull.t option;
      mutable consumer_sequence : int64;
      mutable stream_sequence : int64 option;
      mutable state : state;
      mutable hook : Eio.Switch.hook option;
    }

    let default_expires = Mtime.Span.(30 * s)
    let default_idle_heartbeat = Mtime.Span.(5 * s)
    let inactive_threshold = Mtime.Span.(5 * min)
    let cleanup_timeout = Mtime.Span.(1 * s)
    let timeout_error = Error.Connection Core_error.Timeout

    let fail ordered error =
      match ordered.state with
      | Open ->
          ordered.state <- Failed error;
          Option.iter (fun pull -> ignore (Pull.close pull)) ordered.pull;
          ordered.pull <- None
      | Closed | Failed _ -> ()

    let remaining_timeout ordered deadline =
      match deadline with
      | None -> Ok None
      | Some deadline ->
          let now = Connection.now ordered.connection in
          if Mtime.compare now deadline >= 0 then Error timeout_error
          else Ok (Some (Mtime.span now deadline))

    let stop_current_pull ordered =
      match ordered.pull with
      | None -> ()
      | Some pull ->
          ordered.pull <- None;
          ignore (Pull.close pull)

    let delete_current_consumer ordered ?timeout () =
      match ordered.consumer with
      | None -> ()
      | Some consumer ->
          ordered.consumer <- None;
          ignore (delete ?timeout consumer)

    let cleanup_current_consumer ordered ~deadline =
      match remaining_timeout ordered deadline with
      | Error _ -> ordered.consumer <- None
      | Ok timeout ->
          let timeout =
            match timeout with
            | None -> Some cleanup_timeout
            | Some timeout ->
                Some
                  (if Mtime.Span.compare timeout cleanup_timeout < 0 then
                     timeout
                   else cleanup_timeout)
          in
          delete_current_consumer ordered ?timeout ()

    let await_connection ordered ~deadline =
      match remaining_timeout ordered deadline with
      | Error error -> Error error
      | Ok timeout -> (
          match Connection.await_reconnect ?timeout ordered.connection with
          | Ok () -> Ok ()
          | Error error -> Error (Error.Connection error))

    let next_stream_sequence sequence =
      if Int64.equal sequence Int64.max_int then Int64.max_int
      else Int64.add sequence 1L

    let consumer_config ordered =
      let deliver_policy =
        match ordered.stream_sequence with
        | None -> ordered.initial_deliver_policy
        | Some sequence ->
            Config.By_start_sequence (next_stream_sequence sequence)
      in
      match
        Config.v ~deliver_policy ~ack_policy:Config.No_ack
          ?filter_subject:ordered.filter_subject ~inactive_threshold
          ~mem_storage:true ()
      with
      | Ok config -> Ok config
      | Error error -> Error (Error.Invalid_config error)

    let create_generation ordered ~deadline =
      match ordered.state with
      | Closed -> Error Error.Ordered_closed
      | Failed error -> Error error
      | Open -> (
          match consumer_config ordered with
          | Error error -> Error error
          | Ok config -> (
              match remaining_timeout ordered deadline with
              | Error error -> Error error
              | Ok timeout -> (
                  match create ?timeout ordered.stream config with
                  | Error error -> Error error
                  | Ok consumer -> (
                      ordered.consumer <- Some consumer;
                      match remaining_timeout ordered deadline with
                      | Error error ->
                          ordered.consumer <- None;
                          Error error
                      | Ok _ -> (
                          match
                            Pull.v ~sw:ordered.sw ~batch:ordered.batch
                              ~expires:ordered.expires
                              ~idle_heartbeat:ordered.idle_heartbeat
                              ?max_bytes:ordered.max_bytes consumer
                          with
                          | Error error ->
                              cleanup_current_consumer ordered ~deadline;
                              Error error
                          | Ok pull ->
                              ordered.pull <- Some pull;
                              ordered.consumer_sequence <- 0L;
                              Ok ())))))

    let reconnectable_api_error { Error.code; err_code; _ } =
      Int.equal code 408 || Int.equal code 500 || Int.equal code 502
      || Int.equal code 503 || Int.equal code 504
      ||
      match err_code with
      | Some 10008 | Some 10023 -> true
      | Some _ | None -> false

    let reconnectable_recreate_error = function
      | Error.Api api -> reconnectable_api_error api
      | Error.Connection
          ( Core_error.Disconnected | Core_error.Io _ | Core_error.Tls _
          | Core_error.Timeout | Core_error.No_responders ) ->
          true
      | _ -> false

    let recreate ordered ~deadline =
      stop_current_pull ordered;
      let previous_consumer = ordered.consumer in
      ordered.consumer <- None;
      let rec attempt ~cleanup_previous =
        match await_connection ordered ~deadline with
        | Error error -> Error error
        | Ok () -> (
            if cleanup_previous then (
              ordered.consumer <- previous_consumer;
              cleanup_current_consumer ordered ~deadline);
            match create_generation ordered ~deadline with
            | Ok () -> Ok ()
            | Error error when reconnectable_recreate_error error ->
                attempt ~cleanup_previous:false
            | Error error -> Error error)
      in
      attempt ~cleanup_previous:true

    let recoverable = function
      | Error.Missing_heartbeat | Error.Consumer_deleted -> true
      | Error.Connection (Core_error.Disconnected | Core_error.No_responders) ->
          true
      | _ -> false

    let timed_out = function
      | Error.Connection Core_error.Timeout -> true
      | _ -> false

    let accept_message ordered message =
      let expected = next_stream_sequence ordered.consumer_sequence in
      if Int64.equal (Msg.consumer_sequence message) expected then (
        ordered.consumer_sequence <- Msg.consumer_sequence message;
        ordered.stream_sequence <- Some (Msg.stream_sequence message);
        Ok true)
      else Ok false

    let next_from_pull ordered ~deadline pull =
      match deadline with
      | None -> Pull.next pull
      | Some deadline -> (
          match remaining_timeout ordered (Some deadline) with
          | Error error -> Error error
          | Ok (Some timeout) -> Pull.next_with_timeout ~timeout pull
          | Ok None -> assert false)

    let next_loop ordered ~deadline =
      let result = ref None in
      while Option.is_none !result do
        match ordered.state with
        | Closed -> result := Some (Error Error.Ordered_closed)
        | Failed error -> result := Some (Error error)
        | Open -> (
            match ordered.pull with
            | None -> (
                match recreate ordered ~deadline with
                | Ok () -> ()
                | Error error ->
                    fail ordered error;
                    result := Some (Error error))
            | Some pull -> (
                match next_from_pull ordered ~deadline pull with
                | Ok message -> (
                    match accept_message ordered message with
                    | Error error ->
                        fail ordered error;
                        result := Some (Error error)
                    | Ok true -> result := Some (Ok message)
                    | Ok false -> (
                        match recreate ordered ~deadline with
                        | Ok () -> ()
                        | Error error ->
                            fail ordered error;
                            result := Some (Error error)))
                | Error error when recoverable error -> (
                    match recreate ordered ~deadline with
                    | Ok () -> ()
                    | Error recreate_error ->
                        fail ordered recreate_error;
                        result := Some (Error recreate_error))
                | Error error ->
                    if not (timed_out error) then fail ordered error;
                    result := Some (Error error)))
      done;
      match !result with Some result -> result | None -> assert false

    let close ordered =
      match ordered.state with
      | Closed -> Ok ()
      | Open | Failed _ -> (
          ordered.state <- Closed;
          Option.iter
            (fun hook -> ignore (Eio.Switch.try_remove_hook hook))
            ordered.hook;
          ordered.hook <- None;
          let pull_result =
            match ordered.pull with
            | None -> Ok ()
            | Some pull ->
                ordered.pull <- None;
                Pull.close pull
          in
          let delete_result =
            match ordered.consumer with
            | None -> Ok ()
            | Some consumer ->
                ordered.consumer <- None;
                delete consumer
          in
          match pull_result with
          | Error error -> Error error
          | Ok () -> delete_result)

    let v ~sw ?batch ?expires ?idle_heartbeat ?max_bytes
        ?(deliver_policy = Config.All) ?filter_subject (stream : stream) =
      let batch = Option.value batch ~default:1 in
      let expires = Option.value expires ~default:default_expires in
      let idle_heartbeat =
        Option.value idle_heartbeat ~default:default_idle_heartbeat
      in
      match
        validate_fetch ~batch ~expires ~max_bytes
          ~idle_heartbeat:(Some idle_heartbeat) ~group:None ~min_pending:None
          ~min_ack_pending:None ~priority:None
      with
      | Error error -> Error error
      | Ok () -> (
          let ordered =
            {
              stream;
              connection = stream.jetstream.connection;
              sw;
              batch;
              expires;
              idle_heartbeat;
              max_bytes;
              initial_deliver_policy = deliver_policy;
              filter_subject;
              consumer = None;
              pull = None;
              consumer_sequence = 0L;
              stream_sequence = None;
              state = Open;
              hook = None;
            }
          in
          match create_generation ordered ~deadline:None with
          | Error error -> Error error
          | Ok () ->
              let hook =
                Eio.Switch.on_release_cancellable sw (fun () ->
                    Eio.Cancel.protect (fun () -> ignore (close ordered)))
              in
              ordered.hook <- Some hook;
              Ok ordered)

    let next ordered = next_loop ordered ~deadline:None

    let next_with_timeout ~timeout ordered =
      if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
        Error (Error.Connection (Core_error.Invalid_timeout "ordered"))
      else
        let deadline =
          match Mtime.add_span (Connection.now ordered.connection) timeout with
          | Some deadline -> deadline
          | None -> Mtime.max_stamp
        in
        next_loop ordered ~deadline:(Some deadline)

    let iter ordered ~f =
      let result = ref None in
      while Option.is_none !result do
        match next ordered with
        | Ok message -> f message
        | Error Error.Ordered_closed -> result := Some (Ok ())
        | Error error -> result := Some (Error error)
      done;
      match !result with Some result -> result | None -> assert false
  end

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
