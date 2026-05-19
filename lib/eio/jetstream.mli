(** Typed JetStream management and publishing over a Core NATS connection. *)

module Error : sig
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
