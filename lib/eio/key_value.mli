(** JetStream-backed key-value buckets. *)

module Entry : sig
  type operation = Put | Delete | Purge
  type t

  val bucket : t -> string
  val key : t -> string
  val value : t -> string
  val revision : t -> int64
  val timestamp : t -> string
  val operation : t -> operation
  val pp_operation : Format.formatter -> operation -> unit
end

module Error : sig
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

  val pp_config : Format.formatter -> config -> unit
  val pp_key : Format.formatter -> key -> unit
  val pp : Format.formatter -> t -> unit
end

module Key : sig
  type t

  val of_string : string -> (t, Error.key) result
  val to_string : t -> string
end

module Config : sig
  type storage = Memory | File
  type t
  type error = Error.config

  val v :
    bucket:string ->
    ?history:int ->
    ?ttl:Mtime.Span.t ->
    ?max_bytes:int64 ->
    ?max_value_size:int64 ->
    ?storage:storage ->
    unit ->
    (t, error) result

  val bucket : t -> string
  val history : t -> int
  val ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val max_value_size : t -> int64 option
  val storage : t -> storage
end

module Status : sig
  type t

  val bucket : t -> string
  val values : t -> int64
  val bytes : t -> int64
  val first_revision : t -> int64
  val last_revision : t -> int64
  val consumer_count : t -> int
  val history : t -> int64 option
  val ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val max_value_size : t -> int64 option
  val storage : t -> Config.storage
end

type t

val create : Jetstream.t -> Config.t -> (t, Error.t) result
(** [create jetstream config] creates the [KV_<bucket>] JetStream stream. *)

val open_ : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [open_ jetstream ~bucket] opens an existing key-value bucket. *)

val bind : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [bind jetstream ~bucket] creates a local handle without contacting the
    server. *)

val bucket : t -> string
val status : t -> (Status.t, Error.t) result
val delete_bucket : t -> (unit, Error.t) result
val put : t -> key:string -> string -> (int64, Error.t) result
val create_key : t -> key:string -> string -> (int64, Error.t) result

val update :
  t -> key:string -> revision:int64 -> string -> (int64, Error.t) result

val delete :
  ?expected_revision:int64 -> t -> key:string -> (int64, Error.t) result

val purge :
  ?expected_revision:int64 -> t -> key:string -> (int64, Error.t) result

val get : t -> key:string -> (Entry.t, Error.t) result
(** [get bucket ~key] returns the latest non-tombstone entry. A delete or purge
    returns [Key_deleted] with the tombstone entry. *)

val get_revision :
  t -> key:string -> revision:int64 -> (Entry.t, Error.t) result
(** [get_revision bucket ~key ~revision] returns one exact stream revision. *)
