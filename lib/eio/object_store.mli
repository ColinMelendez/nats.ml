(** JetStream-backed NATS Object Store buckets.

    Buckets store object metadata separately from acknowledged, incrementally
    transferred chunks. Use {!put} and {!get} for large objects and {!Watch}
    for metadata changes. *)

module Error : sig
  type config =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_ttl
    | Invalid_limit of { field : string; value : int64 }

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

  val pp_config : Format.formatter -> config -> unit
  val pp_meta : Format.formatter -> meta -> unit
  val pp : Format.formatter -> t -> unit
end

module Meta : sig
  type link =
    | Object of { bucket : string; name : string }
    | Bucket of { bucket : string }

  type t
  type error = Error.meta

  val v :
    ?description:string ->
    ?headers:Nats.Header.t ->
    ?attributes:(string * string) list ->
    ?chunk_size:int ->
    ?link:link ->
    unit ->
    (t, error) result

  val description : t -> string
  val headers : t -> Nats.Header.t
  val attributes : t -> (string * string) list
  val chunk_size : t -> int option
  val link : t -> link option
end

module Info : sig
  type t
  (** The metadata for one object or object link. *)

  val name : t -> string
  val bucket : t -> string
  val nuid : t -> string
  val size : t -> int64
  val chunks : t -> int64
  val modified : t -> string
  val digest : t -> string
  val deleted : t -> bool
  val meta : t -> Meta.t
  val link : t -> Meta.link option
  val is_link : t -> bool
  val pp : Format.formatter -> t -> unit
end

module Config : sig
  type storage = Memory | File
  type t
  type error = Error.config

  val v :
    bucket:string ->
    ?description:string ->
    ?ttl:Mtime.Span.t ->
    ?max_bytes:int64 ->
    ?storage:storage ->
    unit ->
    (t, error) result

  val bucket : t -> string
  val description : t -> string option
  val ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val storage : t -> storage
end

module Status : sig
  type t

  val config : t -> (Config.t, Error.config) result
  (** [config status] converts the modeled status fields into a configuration
      suitable for a full-replacement bucket update. The sealed state is not
      part of the returned configuration; {!update} preserves it from the
      server-side stream. *)

  val bucket : t -> string
  val description : t -> string option
  val values : t -> int64
  val bytes : t -> int64
  val first_sequence : t -> int64
  val last_sequence : t -> int64
  val consumer_count : t -> int
  val ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val storage : t -> Config.storage
  val sealed : t -> bool
end

type t
(** A handle to an existing or newly created object-store bucket. *)

val create : Jetstream.t -> Config.t -> (t, Error.t) result
(** [create jetstream config] creates the [OBJ_<bucket>] JetStream stream. *)

val list_buckets : Jetstream.t -> (Status.t list, Error.t) result
(** [list_buckets jetstream] returns statuses for all recognized Object Store
    buckets. Streams outside the Object Store subject convention are ignored;
    incomplete JetStream pages and malformed responses remain errors. *)

val open_ : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [open_ jetstream ~bucket] opens an existing object-store bucket. *)

val bind : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [bind jetstream ~bucket] creates a local handle without contacting the
    server. *)

val bucket : t -> string
val status : t -> (Status.t, Error.t) result

val update : t -> Config.t -> (Status.t, Error.t) result
(** [update bucket config] replaces the modeled bucket configuration. The
    bucket name must match, and the current sealed state and Object Store
    invariants are preserved. *)
val delete_bucket : t -> (unit, Error.t) result

val get_info :
  ?show_deleted:bool -> t -> name:string -> (Info.t, Error.t) result
(** [get_info bucket ~name] returns the latest metadata for [name]. Deleted
    objects are treated as absent unless [show_deleted] is true. *)

val put :
  ?chunk_size:int ->
  t ->
  name:string ->
  ?meta:Meta.t ->
  source:_ Eio.Flow.source ->
  unit ->
  (Info.t, Error.t) result
(** [put bucket ~name source] streams [source] into the bucket. Each chunk is
    acknowledged by JetStream before the next chunk is read. The metadata
    message is published only after the source reaches end of file. An
    existing object is replaced after the new metadata is committed. Links
    cannot be uploaded with this operation. *)

val put_string :
  ?chunk_size:int ->
  t ->
  name:string ->
  ?meta:Meta.t ->
  string ->
  (Info.t, Error.t) result

val get :
  sw:Eio.Switch.t ->
  ?show_deleted:bool ->
  t ->
  name:string ->
  sink:_ Eio.Flow.sink ->
  (Info.t, Error.t) result
(** [get ~sw bucket ~name sink] streams the object's chunks into [sink] and
    verifies both the advertised size and digest before returning. Object
    links are resolved recursively; bucket links are not readable as objects. *)

val get_string :
  sw:Eio.Switch.t ->
  ?show_deleted:bool ->
  t ->
  name:string ->
  (string, Error.t) result

val delete : t -> name:string -> (unit, Error.t) result

val update_meta : t -> name:string -> Meta.t -> (Info.t, Error.t) result
(** [update_meta bucket ~name meta] replaces an object's description, headers,
    and attributes. The existing chunk size and link target are preserved. *)

val list : ?show_deleted:bool -> t -> (Info.t list, Error.t) result
(** [list bucket] returns the latest metadata for each object. It is not an
    atomic snapshot of concurrent writes. *)

val link : t -> name:string -> target:Info.t -> (Info.t, Error.t) result
val link_bucket : t -> name:string -> bucket:t -> (Info.t, Error.t) result
val seal : t -> (unit, Error.t) result
(** [seal bucket] prevents further writes to the bucket through JetStream. *)

type bucket = t

module Watch : sig
  type delivery = New | Last_per_subject | All
  type event = Initial_done | Info of Info.t
  type t

  val v :
    sw:Eio.Switch.t ->
    ?name:string ->
    ?delivery:delivery ->
    ?ignore_deletes:bool ->
    bucket ->
    (t, Error.t) result
  (** [v ~sw bucket] watches metadata for the bucket. [name] selects one exact
      object. [Last_per_subject] emits one retained value per object, [All]
      emits all retained metadata, and [New] emits only subsequent metadata.
      Each watch emits [Initial_done] once after its retained initial values;
      timeout errors from {!next_with_timeout} do not close the watch. *)

  val next : t -> (event, Error.t) result
  val next_with_timeout : timeout:Mtime.Span.t -> t -> (event, Error.t) result
  val iter : t -> f:(event -> unit) -> (unit, Error.t) result
  val close : t -> (unit, Error.t) result
end
