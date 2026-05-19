(** Typed JetStream management and publishing over a Core NATS connection. *)

module Error : sig
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

  val pp_config : Format.formatter -> config -> unit
  val pp_api : Format.formatter -> api -> unit
  val pp : Format.formatter -> t -> unit
end

type t

val v : ?prefix:string -> Connection.t -> (t, Error.t) result
(** [v connection] creates a JetStream capability using the default [$JS.API]
    management prefix. [prefix] can select a domain-specific API prefix such as
    [$JS.eu.API]. The capability owns no resources. *)

val of_connection : ?prefix:string -> Connection.t -> (t, Error.t) result
val connection : t -> Connection.t
val prefix : t -> string

module Stream : sig
  type jetstream = t

  module Config : sig
    type storage = Memory | File
    type retention = Limits | Interest | Work_queue
    type discard = Old | New
    type t
    type error = Error.config

    val v :
      name:string ->
      subjects:Nats.Subject.Filter.t list ->
      ?storage:storage ->
      ?retention:retention ->
      ?discard:discard ->
      ?max_msgs:int64 ->
      ?max_bytes:int64 ->
      ?max_age:Mtime.Span.t ->
      ?max_msg_size:int64 ->
      unit ->
      (t, error) result
    (** [v] validates a stream name, capture filters, and limits. Limits use
        [-1] for the JetStream unlimited value when supplied. *)

    val name : t -> string
    val subjects : t -> Nats.Subject.Filter.t list
    val storage : t -> storage
    val retention : t -> retention
    val discard : t -> discard
    val max_msgs : t -> int64 option
    val max_bytes : t -> int64 option
    val max_age : t -> Mtime.Span.t option
    val max_msg_size : t -> int64 option
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

  type t

  val bind : jetstream -> name:string -> (t, Error.t) result
  (** [bind jetstream ~name] creates a local handle without contacting the
      server. It is useful for existing streams. *)

  val create : jetstream -> Config.t -> (t, Error.t) result
  (** [create jetstream config] creates the server-side stream and returns a
      handle for it. *)

  val name : t -> string
  val info : t -> (Info.t, Error.t) result
  val delete : t -> (unit, Error.t) result
end

module Consumer : sig
  module Config : sig
    type ack_policy = No_ack | All | Explicit

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
      ?durable_name:string ->
      ?description:string ->
      ?deliver_policy:deliver_policy ->
      ?ack_policy:ack_policy ->
      ?ack_wait:Mtime.Span.t ->
      ?max_deliver:int ->
      ?filter_subject:Nats.Subject.Filter.t ->
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

    val durable_name : t -> string option
    val description : t -> string option
    val deliver_policy : t -> deliver_policy
    val ack_policy : t -> ack_policy
    val ack_wait : t -> Mtime.Span.t option
    val max_deliver : t -> int option
    val filter_subject : t -> Nats.Subject.Filter.t option
    val replay_policy : t -> replay_policy
    val max_ack_pending : t -> int option
    val max_waiting : t -> int option
    val max_batch : t -> int option
    val max_expires : t -> Mtime.Span.t option
    val max_bytes : t -> int option
    val headers_only : t -> bool option
    val inactive_threshold : t -> Mtime.Span.t option
    val mem_storage : t -> bool option
  end

  module Info : sig
    type t

    val name : t -> string
    val stream_name : t -> string
    val created : t -> string option
    val config : t -> Config.t
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

  type jetstream = t
  type stream = Stream.t
  type t

  val bind : stream -> name:string -> (t, Error.t) result
  (** [bind stream ~name] creates a local handle without contacting the server.
  *)

  val create : stream -> Config.t -> (t, Error.t) result
  (** [create stream config] creates a server-side consumer and returns its
      name. *)

  val name : t -> string
  val stream : t -> stream
  val info : t -> (Info.t, Error.t) result
  val delete : t -> (unit, Error.t) result
end

module Publish_ack : sig
  type t

  val stream : t -> string
  val sequence : t -> int64
  val duplicate : t -> bool
  val domain : t -> string option
  val pp : Format.formatter -> t -> unit
end

val publish :
  ?timeout:Mtime.Span.t ->
  ?headers:Nats.Header.t ->
  ?msg_id:string ->
  t ->
  Nats.Subject.t ->
  string ->
  (Publish_ack.t, Error.t) result
(** [publish js subject payload] publishes through Core NATS and waits for the
    JetStream publish acknowledgement. [msg_id], when supplied, is encoded as
    the [Nats-Msg-Id] header. The application subject is used for publishing;
    the [$JS.API] prefix is reserved for management operations. *)
