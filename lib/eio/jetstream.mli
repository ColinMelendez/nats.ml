(** Typed JetStream management and publishing over a Core NATS connection. *)

module Error : sig
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Empty_subjects
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_max_age
    | Invalid_duplicate_window
    | Invalid_first_sequence of int64
    | Invalid_subject_delete_marker_ttl
    | Invalid_replicas of int
    | Empty_placement
    | Empty_placement_cluster
    | Empty_placement_tag
    | Mirror_and_sources
    | Mirror_and_subjects
    | Mirror_and_first_sequence
    | Invalid_discard_new_per_subject
    | Deny_purge_and_rollup
    | Source_filter_and_transforms
    | Invalid_source_start
    | Invalid_source_start_sequence of int64
    | Invalid_source_start_time of string
    | Empty_external_api_prefix
    | Invalid_external_prefix of { field : string; error : Nats.Subject.error }
    | Invalid_transform_destination of string
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

  type api = {
    code : int;
    err_code : int option;
    description : string;
    metadata : Jsont.json;
  }
  (** A structured error returned by a JetStream API endpoint. [metadata] is a
      JSON object containing fields not interpreted by this version of the
      client. *)

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
    | Invalid_publish_option of { field : string; reason : string }
    | Publish_stalled
    | Batch_gap of { expected : int64; actual : int64 }
    | Batch_flow_error of { sequence : int64; error : api }
    | Message_not_found
    | Stream_not_found
    | Consumer_not_found
    | Message_delete_failed of { sequence : int64; secure : bool }
    | Invalid_message_header of { name : string; value : string }
    | Empty_msg_id
    | Msg_id_already_set
    | Unexpected_stream_name of { expected : string; actual : string }
    | Unexpected_consumer_name of { expected : string; actual : string }
    | Invalid_batch of int
    | Invalid_consume_limit of { field : string; value : int }
    | Invalid_max_bytes of int
    | Invalid_priority_group of string
    | Invalid_priority_threshold of { field : string; value : int64 }
    | Invalid_priority of int
    | Invalid_consumer_reset_sequence of int64
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

  val pp_config : Format.formatter -> config -> unit
  val pp_api : Format.formatter -> api -> unit
  val pp : Format.formatter -> t -> unit
end

type t

type jetstream = t

val v : ?prefix:string -> Connection.t -> (t, Error.t) result
(** [v connection] creates a JetStream capability using the default [$JS.API]
    management prefix. [prefix] can select a domain-specific API prefix such as
    [$JS.eu.API]. The capability owns no resources. *)

val of_connection : ?prefix:string -> Connection.t -> (t, Error.t) result
val connection : t -> Connection.t
val prefix : t -> string

module Account : sig
  module Limits : sig
    type t

    val max_memory : t -> int64
    val max_storage : t -> int64
    val max_streams : t -> int
    val max_consumers : t -> int
    val max_ack_pending : t -> int
    val memory_max_stream_bytes : t -> int64
    val storage_max_stream_bytes : t -> int64
    val max_bytes_required : t -> bool
  end

  module Tier : sig
    type t

    val memory : t -> int64
    (** Usage values use the server's unsigned 64-bit wire representation. The
        server's unsigned maximum sentinel for an unlimited reservation is
        exposed as [Int64.minus_one]. *)

    val storage : t -> int64
    val reserved_memory : t -> int64
    val reserved_storage : t -> int64
    val streams : t -> int
    val consumers : t -> int
    val limits : t -> Limits.t
  end

  module Api : sig
    type t

    val level : t -> int
    val total : t -> int64
    val errors : t -> int64
    val inflight : t -> int64
  end

  type t

  val domain : t -> string option
  val tier : t -> Tier.t
  val tiers : t -> (string * Tier.t) list
  val api : t -> Api.t
end

val account_info : ?timeout:Mtime.Span.t -> t -> (Account.t, Error.t) result
(** [account_info ?timeout jetstream] returns usage, limits, API statistics, and
    domain-tier information for the current JetStream account. *)

module Stream : sig
  type jetstream = t

  module Config : sig
    (** Validated stream configuration values and their persistent updates. A
        configuration may describe ordinary capture subjects, a mirror, or
        one or more sources. *)

    type storage = Memory | File
    type retention = Limits | Interest | Work_queue
    type discard = Old | New

    type compression =
      | Uncompressed
      | S2  (** The server-side stream compression policy. *)

    type persist_mode = Default | Async
    (** The server's stream persistence acknowledgement policy. *)

    module Placement : sig
      type t
      type error = Error.config

      val v : ?cluster:string -> ?tags:string list -> unit -> (t, error) result
      (** [v ?cluster ?tags ()] validates a placement constraint. At least one
          of [cluster] and [tags] must be supplied. *)

      val cluster : t -> string option
      val tags : t -> string list
    end

    module Consumer_limits : sig
      type t
      type error = Error.config

      val v :
        ?inactive_threshold:Mtime.Span.t ->
        ?max_ack_pending:int ->
        unit ->
        (t, error) result
      (** [v ?inactive_threshold ?max_ack_pending ()] limits the defaults that
          consumers inherit from this stream. A zero duration or zero pending
          count means that the stream leaves that consumer setting unspecified;
          [-1] keeps the server's unlimited pending-ack value. *)

      val inactive_threshold : t -> Mtime.Span.t option
      val max_ack_pending : t -> int option
    end

    module Transform : sig
      type t
      (** A subject mapping used by a stream, source, or republish rule. *)
      type error = Error.config

      val v :
        ?source:Nats.Subject.Filter.t ->
        destination:string ->
        unit ->
        (t, error) result
      (** [v ?source ~destination ()] builds a subject mapping. An omitted
          [source] means all subjects; an empty [destination] filters matching
          messages when used in a source transform. *)

      val source : t -> Nats.Subject.Filter.t option
      val destination : t -> string
    end

    module External : sig
      type t
      (** Cross-account or cross-domain JetStream API and delivery prefixes. *)
      type error = Error.config

      val v :
        api_prefix:string ->
        ?deliver_prefix:string ->
        unit ->
        (t, error) result
      (** [v ~api_prefix ?deliver_prefix ()] qualifies a source in another
          account or JetStream domain using the server's wire prefixes. *)

      val api_prefix : t -> string
      val deliver_prefix : t -> string option
    end

    module Source : sig
      type start =
        | Sequence of int64
        | Time of Ptime.t
      (** A source's optional replay start point. Sequence numbers are
          positive; times are encoded as RFC3339 timestamps. *)

      type t
      (** A mirror or source stream reference. *)
      type error = Error.config

      val v :
        name:string ->
        ?start:start ->
        ?filter_subject:Nats.Subject.Filter.t ->
        ?subject_transforms:Transform.t list ->
        ?external_:External.t ->
        unit ->
        (t, error) result
      (** [v ~name ()] describes a mirrored or sourced stream. A source may
          have one filter or a non-empty list of transforms, but not both. *)

      val name : t -> string
      val start : t -> start option
      val filter_subject : t -> Nats.Subject.Filter.t option
      val subject_transforms : t -> Transform.t list
      val external_ : t -> External.t option
    end

    module Republish : sig
      type t
      (** A post-storage subject republish rule. *)
      type error = Error.config

      val v :
        ?source:Nats.Subject.Filter.t ->
        destination:string ->
        ?headers_only:bool ->
        unit ->
        (t, error) result
      (** [v ?source ~destination ?headers_only ()] configures immediate
          republishing after a message is stored. *)

      val source : t -> Nats.Subject.Filter.t option
      val destination : t -> string
      val headers_only : t -> bool
    end

    type t
    type error = Error.config

    val v :
      name:string ->
      subjects:Nats.Subject.Filter.t list ->
      ?description:string ->
      ?storage:storage ->
      ?replicas:int ->
      ?placement:Placement.t ->
      ?mirror:Source.t ->
      ?sources:Source.t list ->
      ?subject_transform:Transform.t ->
      ?republish:Republish.t ->
      ?mirror_direct:bool ->
      ?compression:compression ->
      ?metadata:(string * string) list ->
      ?retention:retention ->
      ?discard:discard ->
      ?max_msgs:int64 ->
      ?max_msgs_per_subject:int64 ->
      ?max_bytes:int64 ->
      ?max_age:Mtime.Span.t ->
      ?max_msg_size:int64 ->
      ?max_consumers:int ->
      ?discard_new_per_subject:bool ->
      ?no_ack:bool ->
      ?duplicate_window:Mtime.Span.t ->
      ?allow_msg_ttl:bool ->
      ?allow_msg_counter:bool ->
      ?allow_atomic_publish:bool ->
      ?allow_msg_schedules:bool ->
      ?persist_mode:persist_mode ->
      ?allow_batch_publish:bool ->
      ?subject_delete_marker_ttl:Mtime.Span.t ->
      ?allow_rollup:bool ->
      ?allow_direct:bool ->
      ?deny_delete:bool ->
      ?deny_purge:bool ->
      ?first_sequence:int64 ->
      ?consumer_limits:Consumer_limits.t ->
      ?sealed:bool ->
      unit ->
      (t, error) result
    (** [v] validates a stream name, capture filters, and limits. Limits use
        [-1] for the JetStream unlimited value when supplied. [replicas] must be
        between 1 and 5. A mirror requires an empty [subjects] list and cannot
        be combined with [sources]. [deny_delete] controls whether stream-level
        message deletion is rejected. *)

    val name : t -> string
    val subjects : t -> Nats.Subject.Filter.t list
    val description : t -> string option
    val storage : t -> storage
    val replicas : t -> int
    val placement : t -> Placement.t option
    val mirror : t -> Source.t option
    (** [mirror config] is the configured mirror, if any. *)
    val sources : t -> Source.t list
    (** [sources config] is the ordered list of source streams. *)
    val subject_transform : t -> Transform.t option
    (** [subject_transform config] is the input subject mapping, if any. *)
    val republish : t -> Republish.t option
    (** [republish config] is the post-storage republish rule, if any. *)
    val mirror_direct : t -> bool
    (** [mirror_direct config] controls direct reads through a mirror. *)
    val compression : t -> compression
    val metadata : t -> (string * string) list
    val retention : t -> retention
    val discard : t -> discard
    val max_msgs : t -> int64 option
    val max_msgs_per_subject : t -> int64 option
    val max_bytes : t -> int64 option
    val max_age : t -> Mtime.Span.t option
    val max_msg_size : t -> int64 option
    val max_consumers : t -> int option
    val discard_new_per_subject : t -> bool
    val no_ack : t -> bool
    val duplicate_window : t -> Mtime.Span.t option
    val allow_msg_ttl : t -> bool
    val allow_msg_counter : t -> bool
    val allow_atomic_publish : t -> bool
    val allow_msg_schedules : t -> bool
    val persist_mode : t -> persist_mode
    val allow_batch_publish : t -> bool
    val subject_delete_marker_ttl : t -> Mtime.Span.t option
    val allow_rollup : t -> bool

    val allow_direct : t -> bool
    (** [allow_direct config] is [true] when direct message reads are enabled.
    *)

    val deny_delete : t -> bool
    (** [deny_delete config] is [true] when stream-level message deletion is
        rejected. *)

    val deny_purge : t -> bool
    (** [deny_purge config] is [true] when purging the stream is rejected. *)

    val first_sequence : t -> int64 option
    (** [first_sequence config] is the first sequence retained when the stream
        is initialized, if explicitly configured. *)

    val consumer_limits : t -> Consumer_limits.t option
    (** [consumer_limits config] contains stream-level consumer defaults. *)

    val sealed : t -> bool
    (** [sealed config] is [true] when the stream rejects further writes. *)

    val with_name : t -> string -> (t, error) result
    (** [with_name config name] validates [name] while preserving the other
        fields. *)

    val with_description : t -> string option -> (t, error) result
    (** [with_description config value] replaces the stream description. *)

    val with_subjects : t -> Nats.Subject.Filter.t list -> (t, error) result
    (** [with_subjects config subjects] validates [subjects] while preserving
        the other fields. An existing server-side mirror may retain an empty
        subject list. *)

    val with_storage : t -> storage -> (t, error) result
    (** [with_storage config storage] preserves all fields except storage. *)

    val with_replicas : t -> int -> (t, error) result
    (** [with_replicas config value] validates and replaces the replica count.
    *)

    val with_placement : t -> Placement.t option -> (t, error) result
    (** [with_placement config value] replaces the placement constraint. *)

    val with_mirror : t -> Source.t option -> (t, error) result
    (** [with_mirror config value] replaces the mirror configuration. *)

    val with_sources : t -> Source.t list -> (t, error) result
    (** [with_sources config value] replaces the source list. *)

    val with_subject_transform : t -> Transform.t option -> (t, error) result
    (** [with_subject_transform config value] replaces the input subject
        transform. *)

    val with_republish : t -> Republish.t option -> (t, error) result
    (** [with_republish config value] replaces the republish configuration. *)

    val with_mirror_direct : t -> bool -> (t, error) result
    (** [with_mirror_direct config value] replaces mirror direct-read access. *)

    val with_compression : t -> compression -> (t, error) result
    (** [with_compression config value] replaces the storage compression mode.
    *)

    val with_metadata : t -> (string * string) list -> (t, error) result
    (** [with_metadata config value] replaces bucket-level stream metadata. *)

    val with_retention : t -> retention -> (t, error) result
    (** [with_retention config retention] preserves all fields except retention.
    *)

    val with_discard : t -> discard -> (t, error) result
    (** [with_discard config discard] preserves all fields except discard. *)

    val with_max_msgs : t -> int64 option -> (t, error) result
    (** [with_max_msgs config value] validates and replaces the message limit.
        [None] means unlimited. *)

    val with_max_msgs_per_subject : t -> int64 option -> (t, error) result
    (** [with_max_msgs_per_subject config value] validates and replaces the
        per-subject message limit. [None] means unlimited. *)

    val with_max_bytes : t -> int64 option -> (t, error) result
    (** [with_max_bytes config value] validates and replaces the byte limit.
        [None] means unlimited. *)

    val with_max_age : t -> Mtime.Span.t option -> (t, error) result
    (** [with_max_age config value] validates and replaces the age limit. [None]
        means unlimited. *)

    val with_max_msg_size : t -> int64 option -> (t, error) result
    (** [with_max_msg_size config value] validates and replaces the per-message
        size limit. [None] means unlimited. *)

    val with_max_consumers : t -> int option -> (t, error) result
    (** [with_max_consumers config value] replaces the stream consumer limit.
        [None] means unlimited. *)

    val with_discard_new_per_subject : t -> bool -> (t, error) result
    (** [with_discard_new_per_subject config value] replaces per-subject
        rejection when the stream uses [New] discard policy. *)

    val with_no_ack : t -> bool -> (t, error) result
    (** [with_no_ack config value] replaces whether the stream disables message
        acknowledgements. *)

    val with_duplicate_window :
      t -> Mtime.Span.t option -> (t, error) result
    (** [with_duplicate_window config value] replaces the duplicate detection
        window. [None] requests the server default. *)

    val with_allow_msg_ttl : t -> bool -> (t, error) result
    (** [with_allow_msg_ttl config value] replaces whether message-level TTL
        headers are accepted by the stream. *)

    val with_allow_msg_counter : t -> bool -> (t, error) result
    (** [with_allow_msg_counter config value] replaces whether per-message
        counters are enabled for the stream. *)

    val with_allow_atomic_publish : t -> bool -> (t, error) result
    (** [with_allow_atomic_publish config value] replaces whether atomic batch
        publishing is accepted by the stream. *)

    val with_allow_msg_schedules : t -> bool -> (t, error) result
    (** [with_allow_msg_schedules config value] replaces whether scheduled
        messages are accepted by the stream. *)

    val with_persist_mode : t -> persist_mode -> (t, error) result
    (** [with_persist_mode config value] replaces when stream writes are
        flushed relative to their publish acknowledgements. *)

    val with_allow_batch_publish : t -> bool -> (t, error) result
    (** [with_allow_batch_publish config value] replaces whether fast batch
        publishing is accepted by the stream. *)

    val with_subject_delete_marker_ttl :
      t -> Mtime.Span.t option -> (t, error) result
    (** [with_subject_delete_marker_ttl config value] replaces the server-side
        lifetime of subject delete markers. [None] disables the limit. *)

    val with_allow_rollup : t -> bool -> (t, error) result
    (** [with_allow_rollup config value] replaces whether rollup headers are
        accepted by the stream. *)

    val with_allow_direct : t -> bool -> (t, error) result
    (** [with_allow_direct config value] replaces whether direct message reads
        are accepted by the stream. *)

    val with_deny_delete : t -> bool -> (t, error) result
    (** [with_deny_delete config value] replaces whether deleting the stream's
        messages through the stream API is rejected. *)

    val with_deny_purge : t -> bool -> (t, error) result
    (** [with_deny_purge config value] replaces whether purging the stream is
        rejected. *)

    val with_first_sequence : t -> int64 option -> (t, error) result
    (** [with_first_sequence config value] replaces the initial retained
        sequence. [None] leaves the server default. *)

    val with_consumer_limits :
      t -> Consumer_limits.t option -> (t, error) result
    (** [with_consumer_limits config value] replaces stream-level consumer
        defaults. *)

    val with_sealed : t -> bool -> (t, error) result
    (** [with_sealed config value] replaces the stream sealed flag. *)
  end

  module Info : sig
    type t

    val config : t -> Config.t
    val messages : t -> int64
    val bytes : t -> int64
    val first_sequence : t -> int64
    val last_sequence : t -> int64
    val consumer_count : t -> int
    val pp : Format.formatter -> t -> unit
  end

  module Message : sig
    type t

    val subject : t -> Nats.Subject.t
    val sequence : t -> int64

    val timestamp : t -> string
    (** [timestamp message] is the server timestamp in its RFC3339 wire form.
        Keeping the wire form avoids imposing a wall-clock representation on
        callers that only need to preserve or display it. *)

    val headers : t -> Nats.Header.t
    val payload : t -> string
  end

  type t

  val bind : jetstream -> name:string -> (t, Error.t) result
  (** [bind jetstream ~name] creates a local handle without contacting the
      server. It is useful for existing streams. *)

  val lookup : jetstream -> name:string -> (t, Error.t) result
  (** [lookup jetstream ~name] validates that [name] exists on the server and
      returns a handle for it. *)

  val create : jetstream -> Config.t -> (t, Error.t) result
  (** [create jetstream config] creates the server-side stream and returns a
      handle for it. *)

  val create_or_update : jetstream -> Config.t -> (t, Error.t) result
  (** [create_or_update jetstream config] updates an existing stream or creates
      it when it is not present. *)

  val update : t -> Config.t -> (Info.t, Error.t) result
  (** [update stream config] applies the modeled fields in [config] to an
      existing stream and returns the server's resulting stream information. The
      operation reads the current server configuration first, preserving fields
      not modeled by {!Config.t}; every modeled field is replaced, and [None]
      clears its corresponding limit. Values omitted from [Config.v] use that
      constructor's defaults. Concurrent changes use last-writer-wins semantics.
      The configuration name must equal [name stream]. *)

  val list :
    ?subject:Nats.Subject.Filter.t -> jetstream -> (Info.t list, Error.t) result
  (** [list jetstream] returns detailed information for all matching streams.
      The optional [subject] filters streams by their captured subjects. *)

  val names :
    ?subject:Nats.Subject.Filter.t -> jetstream -> (string list, Error.t) result
  (** [names ?subject jetstream] returns names for all matching streams. The
      request is paged internally; [subject] filters captured subjects. *)

  val name_by_subject :
    jetstream -> subject:Nats.Subject.t -> (string, Error.t) result
  (** [name_by_subject jetstream ~subject] returns the first stream capturing
      [subject], or [Stream_not_found] when none does. *)

  val name : t -> string
  val info : t -> (Info.t, Error.t) result

  val get :
    ?timeout:Mtime.Span.t -> t -> sequence:int64 -> (Message.t, Error.t) result
  (** [get stream ~sequence] retrieves one stored message by stream sequence
      through JetStream's direct message API. *)

  val get_last :
    ?timeout:Mtime.Span.t ->
    t ->
    subject:Nats.Subject.t ->
    (Message.t, Error.t) result
  (** [get_last stream ~subject] retrieves the latest stored message for an
      exact subject through JetStream's direct message API. *)

  val purge :
    ?timeout:Mtime.Span.t ->
    ?subject:Nats.Subject.Filter.t ->
    ?keep:int64 ->
    t ->
    (int64, Error.t) result
  (** [purge ?subject ?keep stream] removes messages from [stream]. With
      [subject], only messages matching the subject filter are removed. With
      [keep], the newest [keep] matching messages are retained. The result is
      the number of messages the server purged. *)

  val delete_message :
    ?timeout:Mtime.Span.t -> t -> sequence:int64 -> (unit, Error.t) result
  (** [delete_message stream ~sequence] marks one stored message as deleted
      without overwriting its contents. *)

  val secure_delete_message :
    ?timeout:Mtime.Span.t -> t -> sequence:int64 -> (unit, Error.t) result
  (** [secure_delete_message stream ~sequence] deletes one stored message and
      asks the server to overwrite its contents. *)

  val delete : t -> (unit, Error.t) result
end

module Msg : sig
  type t

  val message : t -> Nats.Message.t
  val subject : t -> Nats.Subject.t
  val payload : t -> string
  val headers : t -> Nats.Header.t
  val stream : t -> string
  val consumer : t -> string
  val domain : t -> string option

  val timestamp : t -> int64
  (** [timestamp message] is the server's Unix-epoch timestamp in nanoseconds.
  *)

  val num_delivered : t -> int64
  val stream_sequence : t -> int64
  val consumer_sequence : t -> int64
  val num_pending : t -> int64

  val ack : t -> (unit, Error.t) result
  (** [ack message] publishes [+ACK] without waiting for a server response.
      [Ok ()] means that the acknowledgement was accepted by the local
      connection. The server's acknowledgement policy is not inferred from the
      availability of this function; in particular, ordered consumers use
      [No_ack]. *)

  val ack_sync : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
  (** [ack_sync ?timeout message] sends [+ACK] and waits for the server to
      acknowledge receiving it. This response is a request-level confirmation,
      not a durability guarantee. [timeout] defaults to the connection request
      timeout. A missing response returns [Error (Connection Timeout)] and a
      server without a responder returns [Error (Connection No_responders)]. *)

  val nak : ?delay:Mtime.Span.t -> t -> (unit, Error.t) result
  val term : ?reason:string -> t -> (unit, Error.t) result
  val in_progress : t -> (unit, Error.t) result
end

module Consumer : sig
  module Config : sig
    type ack_policy = No_ack | All | Explicit | Flow_control
    type priority_policy = Overflow | Pinned_client | Prioritized

    type deliver_policy =
      | All
      | Last
      | New
      | By_start_sequence of int64
      | By_start_time of string
      | Last_per_subject

    type replay_policy = Instant | Original
    type t
    type error = Error.config

    val v :
      ?name:string ->
      ?durable_name:string ->
      ?description:string ->
      ?deliver_subject:Nats.Subject.t ->
      ?deliver_group:Nats.Queue_group.t ->
      ?idle_heartbeat:Mtime.Span.t ->
      ?flow_control:bool ->
      ?deliver_policy:deliver_policy ->
      ?ack_policy:ack_policy ->
      ?ack_wait:Mtime.Span.t ->
      ?max_deliver:int ->
      ?filter_subject:Nats.Subject.Filter.t ->
      ?filter_subjects:Nats.Subject.Filter.t list ->
      ?backoff:Mtime.Span.t list ->
      ?pause_until:Ptime.t ->
      ?priority_groups:string list ->
      ?priority_policy:priority_policy ->
      ?priority_timeout:Mtime.Span.t ->
      ?sample_frequency:int ->
      ?rate_limit:int64 ->
      ?replicas:int ->
      ?metadata:(string * string) list ->
      ?replay_policy:replay_policy ->
      ?max_ack_pending:int ->
      ?max_waiting:int ->
      ?max_batch:int ->
      ?max_expires:Mtime.Span.t ->
      ?max_bytes:int ->
      ?headers_only:bool ->
      ?inactive_threshold:Mtime.Span.t ->
      ?mem_storage:bool ->
      unit ->
      (t, error) result

    val name : t -> string option
    val durable_name : t -> string option
    val description : t -> string option
    val deliver_subject : t -> Nats.Subject.t option
    val deliver_group : t -> Nats.Queue_group.t option
    val idle_heartbeat : t -> Mtime.Span.t option
    val flow_control : t -> bool option
    val deliver_policy : t -> deliver_policy
    val ack_policy : t -> ack_policy
    val ack_wait : t -> Mtime.Span.t option
    val max_deliver : t -> int option
    val filter_subject : t -> Nats.Subject.Filter.t option

    val filter_subjects : t -> Nats.Subject.Filter.t list
    (** [filter_subjects config] returns the multi-subject filters. The list is
        empty when the singular filter form is in use or no filter is set. *)

    val backoff : t -> Mtime.Span.t list
    (** [backoff config] returns the redelivery delay schedule. *)

    val pause_until : t -> Ptime.t option
    (** [pause_until config] is the server-side pause deadline, when set. *)

    val priority_groups : t -> string list
    (** [priority_groups config] returns the configured priority group names. *)

    val priority_policy : t -> priority_policy option
    (** [priority_policy config] is the pull-consumer priority policy, when
        configured. *)

    val priority_timeout : t -> Mtime.Span.t option
    (** [priority_timeout config] is the pinned-client grace period, when
        configured. *)

    val sample_frequency : t -> int option
    (** [sample_frequency config] is the delivery sample percentage. *)

    val rate_limit : t -> int64 option
    (** [rate_limit config] is the push rate limit in bits per second. *)

    val replicas : t -> int option
    (** [replicas config] is the explicit replica count; [None] inherits the
        stream's replica count. *)

    val metadata : t -> (string * string) list
    (** [metadata config] returns consumer metadata. *)

    val replay_policy : t -> replay_policy

    val max_ack_pending : t -> int option
    (** [Some (-1)] means unlimited; [None] leaves the server default when a
        consumer is created. *)

    val max_waiting : t -> int option
    val max_batch : t -> int option
    val max_expires : t -> Mtime.Span.t option
    val max_bytes : t -> int option
    val headers_only : t -> bool option
    val inactive_threshold : t -> Mtime.Span.t option
    val mem_storage : t -> bool option

    val with_name : t -> string option -> (t, error) result
    (** [with_name config value] replaces the optional consumer name. *)

    val with_durable_name : t -> string option -> (t, error) result
    (** [with_durable_name config value] replaces the durable identity. *)

    val with_description : t -> string option -> (t, error) result
    (** [with_description config value] replaces the consumer description. *)

    val with_deliver_subject : t -> Nats.Subject.t option -> (t, error) result
    (** [with_deliver_subject config value] replaces the push delivery subject.
    *)

    val with_deliver_group : t -> Nats.Queue_group.t option -> (t, error) result
    (** [with_deliver_group config value] replaces the push queue group. *)

    val with_idle_heartbeat : t -> Mtime.Span.t option -> (t, error) result
    (** [with_idle_heartbeat config value] replaces the idle heartbeat. *)

    val with_flow_control : t -> bool option -> (t, error) result
    (** [with_flow_control config value] replaces flow control. *)

    val with_deliver_policy : t -> deliver_policy -> (t, error) result
    (** [with_deliver_policy config value] replaces the delivery policy. *)

    val with_ack_policy : t -> ack_policy -> (t, error) result
    (** [with_ack_policy config value] replaces the acknowledgement policy. *)

    val with_ack_wait : t -> Mtime.Span.t option -> (t, error) result
    (** [with_ack_wait config value] replaces the acknowledgement timeout. *)

    val with_max_deliver : t -> int option -> (t, error) result
    (** [with_max_deliver config value] replaces the delivery-attempt limit. *)

    val with_filter_subject :
      t -> Nats.Subject.Filter.t option -> (t, error) result
    (** [with_filter_subject config value] replaces the singular subject filter
        and clears any multi-subject filters. *)

    val with_filter_subjects :
      t -> Nats.Subject.Filter.t list -> (t, error) result
    (** [with_filter_subjects config value] replaces the multi-subject filters
        and clears the singular filter. An empty list clears all filters. *)

    val with_backoff : t -> Mtime.Span.t list -> (t, error) result
    (** [with_backoff config value] replaces the redelivery delay schedule.
        [Mtime.Span.t] values are non-negative; an empty list clears the
        schedule. *)

    val with_pause_until : t -> Ptime.t option -> (t, error) result
    (** [with_pause_until config value] replaces the pause deadline. [None]
        requests an unpaused configuration when creating a consumer. Updating a
        consumer does not change its pause state; use {!Consumer.resume} or
        {!Consumer.pause} for that operation. *)

    val with_priority_groups : t -> string list -> (t, error) result
    (** [with_priority_groups config groups] replaces the priority group names.
        Priority groups are pull-only; each name is at most sixteen ASCII
        letters, digits, [/], [_], [-], or [=] characters. *)

    val with_priority_policy : t -> priority_policy option -> (t, error) result
    (** [with_priority_policy config policy] replaces the priority policy. A
        policy requires at least one priority group. *)

    val with_priority_timeout : t -> Mtime.Span.t option -> (t, error) result
    (** [with_priority_timeout config timeout] replaces the pinned-client grace
        period. It is meaningful only with {!Pinned_client}. *)

    val with_sample_frequency : t -> int option -> (t, error) result
    (** [with_sample_frequency config value] replaces the delivery sample
        percentage. Values must be non-negative. [None] clears sampling. *)

    val with_rate_limit : t -> int64 option -> (t, error) result
    (** [with_rate_limit config value] replaces the push rate limit in bits per
        second. A positive value requires a push delivery subject. *)

    val with_replicas : t -> int option -> (t, error) result
    (** [with_replicas config value] replaces the explicit replica count. [None]
        inherits the stream's replica count. *)

    val with_metadata : t -> (string * string) list -> (t, error) result
    (** [with_metadata config value] replaces consumer metadata. *)

    val with_replay_policy : t -> replay_policy -> (t, error) result
    (** [with_replay_policy config value] replaces the replay policy. *)

    val with_max_ack_pending : t -> int option -> (t, error) result
    (** [with_max_ack_pending config value] replaces the outstanding-ack limit.
        [Some (-1)] means unlimited and [None] leaves the server default when
        creating a consumer. *)

    val with_max_waiting : t -> int option -> (t, error) result
    (** [with_max_waiting config value] replaces the waiting-pull limit. *)

    val with_max_batch : t -> int option -> (t, error) result
    (** [with_max_batch config value] replaces the pull batch limit. *)

    val with_max_expires : t -> Mtime.Span.t option -> (t, error) result
    (** [with_max_expires config value] replaces the pull expiry limit. *)

    val with_max_bytes : t -> int option -> (t, error) result
    (** [with_max_bytes config value] replaces the pull byte limit. *)

    val with_headers_only : t -> bool option -> (t, error) result
    (** [with_headers_only config value] replaces headers-only delivery. *)

    val with_inactive_threshold : t -> Mtime.Span.t option -> (t, error) result
    (** [with_inactive_threshold config value] replaces inactivity cleanup. *)

    val with_mem_storage : t -> bool option -> (t, error) result
    (** [with_mem_storage config value] replaces consumer state storage. *)
  end

  module Priority_group : sig
    type t

    val name : t -> string
    (** [name group] is the configured priority-group name. *)

    val pinned_client_id : t -> string option
    (** [pinned_client_id group] is the current server-issued pin, when set. *)

    val pinned_at : t -> Ptime.t option
    (** [pinned_at group] is the server timestamp at which the pin was set. *)
  end

  module Info : sig
    type t

    val name : t -> string
    val stream_name : t -> string
    val created : t -> string option
    val config : t -> Config.t
    val paused : t -> bool
    val pause_until : t -> Ptime.t option
    val pause_remaining : t -> Mtime.Span.t option

    val priority_groups : t -> Priority_group.t list
    (** [priority_groups info] reports the server's current pin state for each
        configured priority group. *)

    val unknown : t -> Jsont.json
    val config_unknown : t -> Jsont.json
    val delivered_consumer_sequence : t -> int64 option
    val delivered_stream_sequence : t -> int64 option
    val ack_floor_consumer_sequence : t -> int64 option
    val ack_floor_stream_sequence : t -> int64 option
    val num_ack_pending : t -> int
    val num_redelivered : t -> int
    val num_waiting : t -> int
    val num_pending : t -> int64
    val pp : Format.formatter -> t -> unit
  end

  module Pause : sig
    type t

    val paused : t -> bool
    val pause_until : t -> Ptime.t option
    val pause_remaining : t -> Mtime.Span.t option
  end

  module Reset : sig
    type t

    val sequence : t -> int64
    (** [sequence reset] is the stream sequence selected by the server. *)

    val info : t -> Info.t
    (** [info reset] is the consumer state after the reset. *)
  end

  type jetstream = t
  type stream = Stream.t
  type t

  val bind : stream -> name:string -> (t, Error.t) result
  (** [bind stream ~name] creates a local handle without contacting the server.
  *)

  val lookup : stream -> name:string -> (t, Error.t) result
  (** [lookup stream ~name] validates that [name] exists on the server and
      returns a handle for it. *)

  val create :
    ?timeout:Mtime.Span.t -> stream -> Config.t -> (t, Error.t) result
  (** [create ?timeout stream config] creates a server-side consumer and returns
      its name. *)

  val create_or_update :
    ?timeout:Mtime.Span.t -> stream -> Config.t -> (t, Error.t) result
  (** [create_or_update ?timeout stream config] creates a consumer or updates
      the existing consumer with the same name. *)

  val update :
    ?timeout:Mtime.Span.t -> t -> Config.t -> (Info.t, Error.t) result
  (** [update ?timeout consumer config] applies the modeled fields in [config]
      to an existing consumer and returns its resulting information. The
      configuration is a full replacement of the fields modeled by {!Config.t};
      use the {!Config.with_description} family to derive a replacement from
      {!Info.config}. The operation reads the current server configuration first
      and preserves fields not modeled by {!Config.t}. [pause_until] is read
      from the current server configuration and is preserved; pause state is
      changed only by {!pause} and {!resume}. The consumer identity remains tied
      to [name consumer]; a supplied durable name must match it, while an
      omitted durable name retains an existing durable identity. Concurrent
      changes use last-writer-wins semantics. *)

  val pause :
    ?timeout:Mtime.Span.t -> t -> until:Ptime.t -> (Pause.t, Error.t) result
  (** [pause ?timeout consumer ~until] pauses [consumer] until the supplied UTC
      deadline. *)

  val resume : ?timeout:Mtime.Span.t -> t -> (Pause.t, Error.t) result
  (** [resume ?timeout consumer] clears the consumer pause deadline. *)

  val unpin :
    ?timeout:Mtime.Span.t -> t -> group:string -> (unit, Error.t) result
  (** [unpin ?timeout consumer ~group] asks the server to select another
      pinned-client member for [group]. The name must satisfy the priority-group
      syntax; the server reports whether the group is configured. *)

  val list : stream -> (Info.t list, Error.t) result
  (** [list stream] returns detailed information for all consumers on [stream].
  *)

  val names : ?timeout:Mtime.Span.t -> stream -> (string list, Error.t) result
  (** [names ?timeout stream] returns the names of all consumers on [stream].
      The request is paged internally. *)

  val name : t -> string
  val stream : t -> stream

  val fetch :
    ?expires:Mtime.Span.t ->
    ?idle_heartbeat:Mtime.Span.t ->
    ?max_bytes:int ->
    ?group:string ->
    ?min_pending:int64 ->
    ?min_ack_pending:int64 ->
    ?priority:int ->
    t ->
    batch:int ->
    (Msg.t list, Error.t) result
  (** [fetch consumer ~batch] requests up to [batch] messages and returns an
      empty or partial list when the server expires the pull request. When
      [idle_heartbeat] is set, status-100 idle heartbeats keep the request
      alive; failure to receive one within two heartbeat intervals returns
      [Missing_heartbeat]. The heartbeat must be positive and no greater than
      half of [expires]. [group] selects a configured priority group;
      [min_pending] and [min_ack_pending] are overflow thresholds; and
      [priority] selects a prioritized-policy level from zero (highest) to nine
      (lowest). Priority options require [group]. On a pinned-client consumer,
      the handle retains the [Nats-Pin-Id] from a delivery for later fetches and
      retries a 423 pin mismatch without the stale id. *)

  val fetch_no_wait : t -> batch:int -> (Msg.t list, Error.t) result
  (** [fetch_no_wait consumer ~batch] requests up to [batch] messages that are
      available when the request reaches the server. It returns an empty or
      partial list without waiting for future messages. [batch] has the same
      validation as {!fetch}. *)

  module Pull : sig
    type consumer = t
    type t

    val v :
      sw:Eio.Switch.t ->
      ?batch:int ->
      ?expires:Mtime.Span.t ->
      ?idle_heartbeat:Mtime.Span.t ->
      ?max_bytes:int ->
      ?group:string ->
      ?min_pending:int64 ->
      ?min_ack_pending:int64 ->
      ?priority:int ->
      consumer ->
      (t, Error.t) result
    (** [v ~sw consumer] opens a persistent pull session using a fresh reply
        inbox. The default batch is one and the default server expiry is five
        seconds. With [idle_heartbeat], status-100 idle heartbeats keep the
        outstanding request alive and a missing heartbeat fails the session with
        [Missing_heartbeat]. [group] joins a configured priority group;
        [min_pending] and [min_ack_pending] are overflow thresholds, and
        [priority] is a prioritized-policy value between zero and nine. The
        session uses the consumer handle's private per-group table for any
        server-issued pinned-client id on later requests and clears it after a
        423 pin-mismatch response. The heartbeat must be positive and no greater
        than half of [expires]. The session owns its subscription and closes it
        when [sw] releases. A session is not transparently restored after a
        transport loss; recreate it after receiving
        [Error (Connection Disconnected)]. A pull session is single-owner: do
        not call [next] or [next_with_timeout] concurrently on the same value.
        Cancellation of a blocked read propagates without closing the session;
        explicitly call [close] when the session is no longer needed. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next pull] waits for the next message. Empty pull batches and the
        JetStream [408], [batch completed], and configured idle-heartbeat
        statuses are handled internally. A priority-group 423 pin mismatch
        clears the private pin and retries the request. Other statuses,
        including [message size exceeds maxbytes], fail the session. Messages
        are not acknowledged automatically. Explicit closure returns
        [Pull_closed]. *)

    val next_with_timeout : timeout:Mtime.Span.t -> t -> (Msg.t, Error.t) result
    (** [next_with_timeout ~timeout pull] bounds the wait, including retries
        after empty server batches and configured idle-heartbeat statuses. A
        timeout returns [Error (Connection Timeout)] and leaves the pull session
        open with its current server request outstanding. A missing heartbeat
        fails the session with [Missing_heartbeat]. A transport loss returns
        [Error (Connection Disconnected)]. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter pull ~f] repeatedly calls [f] for delivered messages until the
        session fails or is closed. It returns [Ok ()] for an explicit close and
        does not acknowledge messages. *)

    val close : t -> (unit, Error.t) result
    (** [close pull] stops the session and is idempotent. An outstanding pull
        request is abandoned; messages not received by the client may be
        redelivered according to the consumer's acknowledgement policy. *)
  end

  module Consume : sig
    type consumer = t
    type t

    val v :
      sw:Eio.Switch.t ->
      ?batch:int ->
      ?expires:Mtime.Span.t ->
      ?idle_heartbeat:Mtime.Span.t ->
      ?max_bytes:int ->
      ?group:string ->
      ?min_pending:int64 ->
      ?min_ack_pending:int64 ->
      ?priority:int ->
      ?max_messages:int ->
      ?stop_after:int ->
      consumer ->
      (t, Error.t) result
    (** [v ~sw consumer] starts a bounded, background pull loop. Messages are
        made available through {!next}; the loop replenishes the queue as the
        consumer removes messages. [max_messages] bounds the buffered messages
        and defaults to [500]. The default expiry is thirty seconds and the
        default idle heartbeat is fifteen seconds. Unless supplied, [batch]
        follows [max_messages] and is reduced to [stop_after] for the first
        request. [stop_after] ends the loop after that many messages have been
        admitted to the queue. The other pull options have the same validation
        and wire semantics as {!Pull.v}. The session owns its resources and is
        cancelled with [sw]. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next consume] waits for the next buffered message. It returns
        [Pull_closed] after {!stop}, {!drain}, or a completed [stop_after], and
        returns the worker error after already-buffered messages have been
        drained. A consume session is single-owner: do not call [next]
        concurrently on the same value. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter consume ~f] processes messages until the session is stopped or
        completes. It does not acknowledge messages. *)

    val stop : t -> unit
    (** [stop consume] cancels the worker and discards buffered messages from
        the caller's point of view. Subsequent {!next} calls return
        [Pull_closed]. *)

    val drain : t -> unit
    (** [drain consume] cancels the worker but preserves messages already in the
        buffer for {!next} before it returns [Pull_closed]. *)

    val close : t -> unit
    (** [close consume] is an alias for {!stop}. *)

    val closed : t -> bool
    (** [closed consume] is [true] after the worker has been stopped, drained,
        or completed. *)
  end

  module Push : sig
    type consumer = t
    type t

    val v : sw:Eio.Switch.t -> consumer -> (t, Error.t) result
    (** [v ~sw consumer] subscribes to the delivery subject configured on a push
        consumer. It reads the server-side configuration and returns
        [Not_push_consumer] when no delivery subject is configured. The
        configured queue group is used for the subscription. Idle-heartbeat
        status frames are consumed transparently, and flow-control requests are
        answered with an empty message. A missing heartbeat fails the session
        with [Missing_heartbeat]. The session owns its subscription and closes
        it when [sw] releases. A replayable delivery subscription is restored
        after transport recovery; durable consumers are checked with [info],
        while missing ephemeral consumers are recreated from their last
        configuration. The session is single-owner: do not call [next] or
        [next_with_timeout] concurrently on one value. Cancellation of a blocked
        read propagates without closing the session; explicitly call [close]
        when the session is no longer needed. *)

    val create : sw:Eio.Switch.t -> Stream.t -> Config.t -> (t, Error.t) result
    (** [create ~sw stream config] creates and owns an ephemeral push consumer.
        If [config] has no delivery subject, a fresh inbox is chosen. Durable
        names are rejected. A five-minute inactive threshold and memory storage
        are supplied when absent. The delivery subscription is installed before
        the consumer is created, so retained messages cannot race the initial
        subscription. The consumer is deleted when [close] is called or [sw]
        releases. *)

    val consumer : t -> consumer
    (** [consumer push] is the current server-side consumer. An ephemeral
        consumer may change after recovery. *)

    val initial_pending : t -> int64
    (** [initial_pending push] is the pending count established by the setup.
        For [Push.create], this is the consumer-create response's pending count,
        including deliveries already reported by that response before setup
        completed. For [Push.v], it is the later consumer-info response's
        pending count. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next push] waits for the next delivered message. Messages are not
        acknowledged automatically. Idle-heartbeat frames are consumed and
        flow-control requests are answered with an empty message. Other status
        frames fail the handle instead of being treated as data. A configured
        heartbeat that is not received within two intervals fails with
        [Missing_heartbeat]. During reconnect recovery, heartbeat deadlines are
        suspended until the subscription is replayed and the consumer is
        confirmed or recreated. Cancellation of a blocked read propagates
        without closing the session; explicitly call [close] when the session is
        no longer needed. *)

    val next_with_timeout : timeout:Mtime.Span.t -> t -> (Msg.t, Error.t) result
    (** [next_with_timeout ~timeout push] bounds the wait with an absolute
        caller deadline across reconnect restoration; control frames do not
        extend it. A timeout leaves an open push handle active. A missing
        configured heartbeat fails the handle with [Missing_heartbeat]. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter push ~f] invokes [f] for each message until the handle is closed
        or fails. It does not acknowledge messages and has the same single-owner
        rule as [next]. *)

    val close : t -> (unit, Error.t) result
    (** [close push] stops the subscription, deletes an owned consumer, and is
        idempotent. *)
  end

  module Ordered : sig
    type stream = Stream.t
    type t

    val v :
      sw:Eio.Switch.t ->
      ?batch:int ->
      ?expires:Mtime.Span.t ->
      ?idle_heartbeat:Mtime.Span.t ->
      ?max_bytes:int ->
      ?deliver_policy:Config.deliver_policy ->
      ?filter_subject:Nats.Subject.Filter.t ->
      ?filter_subjects:Nats.Subject.Filter.t list ->
      ?replay_policy:Config.replay_policy ->
      ?headers_only:bool ->
      ?inactive_threshold:Mtime.Span.t ->
      ?max_reset_attempts:int ->
      ?metadata:(string * string) list ->
      ?name_prefix:string ->
      stream ->
      (t, Error.t) result
    (** [v ~sw stream] creates a client-managed ephemeral pull consumer. The
        initial delivery policy defaults to [All], and [filter_subject] is
        exclusive with [filter_subjects]. Ordered sessions always use [No_ack],
        one-replica memory storage, and a five-minute inactive threshold unless
        [inactive_threshold] is supplied; they request idle heartbeats (five
        seconds by default) to detect a lost consumer. [headers_only],
        [replay_policy], [metadata], and [name_prefix] are applied to every
        generation. [max_reset_attempts] bounds one recovery cycle; [0] or an
        omitted value means unlimited retries. The session owns its pull
        subscription and recreates the ephemeral consumer after a
        consumer-sequence gap, a missing heartbeat, consumer deletion, or a
        non-replayed transport disconnect. Recreated consumers resume at the
        next stream sequence. When [name_prefix] is omitted, the session uses
        a unique internal name prefix so an uncertain create response can be
        cleaned up; supplying [name_prefix] makes generation names predictable
        to the caller. The consumer identity is not preserved across every
        recovery. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next ordered] returns the next message in consumer order. A call may
        perform consumer deletion and recreation before returning. Stream
        sequence numbers may skip when a filter is used; consumer sequence
        numbers must remain consecutive. Cancellation propagates without closing
        the session; explicitly call [close] when the session is no longer
        needed. *)

    val initial_pending : t -> int64 option
    (** [initial_pending ordered] is the pending count reported when the
        initial ephemeral consumer was created. [None] means that the server
        did not include the count in its creation response. The value does not
        change when the session recreates its consumer. *)

    val next_with_timeout : timeout:Mtime.Span.t -> t -> (Msg.t, Error.t) result
    (** [next_with_timeout ~timeout ordered] uses an absolute caller deadline
        across waiting and ordered-consumer recreation. A normal timeout leaves
        the current session open; a timeout after the old consumer has been torn
        down fails the session. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter ordered ~f] invokes [f] for each ordered message until the session
        is closed or fails. Messages are not acknowledged. *)

    val close : t -> (unit, Error.t) result
    (** [close ordered] stops the pull session, waits for a best-effort delete
        of its current ephemeral consumer, and is idempotent. Explicit closure
        returns [Ordered_closed] from subsequent reads. *)

    val release : t -> (unit, Error.t) result
    (** [release ordered] stops the pull session and starts a best-effort delete
        of its current ephemeral consumer without waiting for the server's
        response. It is intended for deadline- and cancellation-sensitive
        cleanup; use [close] when confirmed deletion is required. *)
  end

  val info : ?timeout:Mtime.Span.t -> t -> (Info.t, Error.t) result
  val delete : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result

  val reset : ?timeout:Mtime.Span.t -> t -> (Reset.t, Error.t) result
  (** [reset ?timeout consumer] resets delivery to the server-selected
      acknowledgement floor. *)

  val reset_to_sequence :
    ?timeout:Mtime.Span.t -> t -> sequence:int64 -> (Reset.t, Error.t) result
  (** [reset_to_sequence ?timeout consumer ~sequence] resets delivery to a
      positive stream sequence compatible with the consumer policy. *)
end

module Publish_ack : sig
  type t

  val stream : t -> string
  val sequence : t -> int64
  val duplicate : t -> bool
  val domain : t -> string option
  val batch : t -> string option
  val count : t -> int64 option
  val pp : Format.formatter -> t -> unit
end

module Publish_options : sig
  (** Immutable publish headers and retry controls. Options are validated when
      constructed and applied only if the corresponding header is absent from
      the caller's message. *)

  type schedule = At of Ptime.t | Every of Mtime.Span.t | Cron of string
  (** Scheduled delivery. [Every] requires an interval of at least one second;
      [Cron] is passed to the server unchanged. *)

  type schedule_ttl = Duration of Mtime.Span.t | Never

  type t

  val empty : t

  val with_msg_id : string -> t -> (t, Error.t) result
  val with_expected_stream : string -> t -> (t, Error.t) result
  val with_expected_last_msg_id : string -> t -> (t, Error.t) result
  val with_expected_last_sequence : int64 -> t -> (t, Error.t) result
  val with_expected_last_subject_sequence : int64 -> t -> (t, Error.t) result

  val with_expected_last_sequence_for_subject :
    sequence:int64 -> subject:Nats.Subject.t -> t -> (t, Error.t) result

  val with_ttl : Mtime.Span.t -> t -> (t, Error.t) result
  val with_schedule : schedule -> t -> (t, Error.t) result
  val with_schedule_target : Nats.Subject.t -> t -> (t, Error.t) result
  (** The target is required when [schedule] is set. *)

  val with_schedule_source : Nats.Subject.t -> t -> (t, Error.t) result
  val with_schedule_ttl : schedule_ttl -> t -> (t, Error.t) result
  val with_schedule_timezone : string -> t -> (t, Error.t) result
  (** A time zone is valid only with a [Cron] schedule. *)

  val with_retry :
    wait:Mtime.Span.t -> attempts:int option -> t -> (t, Error.t) result
  (** [attempts] counts retries after the initial request. [None] retries until
      a non-[No_responders] result or cancellation. *)

  val with_stall_wait : Mtime.Span.t -> t -> (t, Error.t) result
  (** The stall limit applies to {!Publisher.publish}; synchronous [publish]
      rejects this option. *)
end

module Publish : sig
  type t

  val await : t -> (Publish_ack.t, Error.t) result
  (** [await publish] waits for the publish acknowledgement. *)

  val cancel : t -> (unit, Error.t) result
  (** [cancel publish] cancels the outstanding request, if any. *)

  val message : t -> Nats.Message.t
  (** [message publish] is the immutable message submitted for publishing. *)
end

module Publisher : sig
  type t

  val v :
    sw:Eio.Switch.t ->
    clock:_ Eio.Time.Mono.t ->
    ?max_pending:int ->
    ?stall_wait:Mtime.Span.t ->
    ?ack_timeout:Mtime.Span.t ->
    jetstream ->
    (t, Error.t) result
  (** [v ~sw ~clock ?max_pending ?stall_wait ?ack_timeout jetstream] creates a
      switch-owned asynchronous publisher. A bounded publisher waits up to
      [stall_wait] for a pending slot; the default bound is 256 and the
      default stall wait is 200 ms. [ack_timeout] defaults to the connection's
      request timeout. *)

  val publish :
    ?headers:Nats.Header.t ->
    ?msg_id:string ->
    ?options:Publish_options.t ->
    t ->
    Nats.Subject.t ->
    string ->
    (Publish.t, Error.t) result
  (** [publish publisher subject payload] submits a publish without waiting for
      its acknowledgement. Only [No_responders] failures are retried, using
      the retry policy in [options]. *)

  val pending : t -> int
  (** [pending publisher] is the number of submitted publishes not yet
      settled. *)

  val await_all : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
  (** [await_all ?timeout publisher] waits until all submitted publishes have
      settled. *)
end

module Atomic_batch : sig
  val publish :
    ?timeout:Mtime.Span.t ->
    id:string ->
    jetstream ->
    Nats.Message.t list ->
    (Publish_ack.t, Error.t) result
  (** [publish ?timeout ~id jetstream messages] stages and commits [messages]
      as one server-side atomic batch. The server either makes the complete
      batch visible or discards it. Messages must not carry reply subjects or
      batch-control headers. *)
end

module Batch : sig
  type gap = Fail | Allow

  val publish :
    ?timeout:Mtime.Span.t ->
    ?flow:int ->
    ?gap:gap ->
    id:string ->
    jetstream ->
    Nats.Message.t list ->
    (Publish_ack.t, Error.t) result
  (** [publish ?timeout ?flow ?gap ~id jetstream messages] publishes a fast
      batch using the server flow-control reply protocol. [flow] is the
      requested acknowledgement interval; zero asks the server for its
      default. [gap] controls whether the server rejects sequence gaps. The
      returned acknowledgement includes the server batch and count when they
      are present. *)
end

val publish :
  ?timeout:Mtime.Span.t ->
  ?headers:Nats.Header.t ->
  ?msg_id:string ->
  ?options:Publish_options.t ->
  t ->
  Nats.Subject.t ->
  string ->
  (Publish_ack.t, Error.t) result
(** [publish js subject payload] publishes through Core NATS and waits for the
    JetStream publish acknowledgement. [msg_id], when supplied, is encoded as
    the [Nats-Msg-Id] header. The application subject is used for publishing;
    the [$JS.API] prefix is reserved for management operations. *)
