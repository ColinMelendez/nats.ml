(** JetStream-backed streaming object stores.

    An object store is a bucket of named, immutable-content objects. Object
    content crosses the API as {!Bytesrw.Bytes.Reader.t} and
    {!Bytesrw.Bytes.Writer.t}; the string functions are convenience wrappers
    for small values. *)

module Config : sig
  type storage = Memory | File
  (** The JetStream storage backend used by the bucket. *)

  type t
  (** A validated object-store configuration. *)

  type error =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_ttl
    | Invalid_limit of { field : string; value : int64 }

  val v :
    bucket:string ->
    ?description:string ->
    ?ttl:Mtime.Span.t ->
    ?max_bytes:int64 ->
    ?storage:storage ->
    unit ->
    (t, error) result
  (** [v ~bucket ()] validates a bucket configuration.

      A zero TTL and [-1] byte limit mean unlimited. The default storage
      backend is {!File}. *)

  val bucket : t -> string
  val description : t -> string option
  val ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val storage : t -> storage
end

module Name : sig
  type t
  (** A non-empty object name. Names are encoded before becoming NATS subjects,
      so they may contain path separators and other ordinary text. *)

  type error = Empty_name

  val of_string : string -> (t, error) result
  val to_string : t -> string
end

module Meta : sig
  type t
  (** Metadata supplied when writing an object. *)

  type error = Invalid_chunk_size of int

  val v :
    name:Name.t ->
    ?description:string ->
    ?headers:Nats.Header.t ->
    ?metadata:(string * string) list ->
    ?chunk_size:int ->
    unit ->
    (t, error) result
  (** [v ~name ()] uses a 128 KiB chunk size. *)

  val name : t -> Name.t
  val description : t -> string option
  val headers : t -> Nats.Header.t
  val metadata : t -> (string * string) list
  val chunk_size : t -> int
end

module Link : sig
  type t
  (** A link carried by an object metadata record. A [None] name points to a
      whole bucket; [Some name] points to one object. *)

  val bucket : t -> string
  val name : t -> Name.t option
end

module Info : sig
  type t
  (** Server metadata for one object.

      [timestamp] is the metadata message's server RFC3339 timestamp. It is
      distinct from the wire [mtime] field, which is intentionally not exposed
      as a local wall-clock value. *)

  val bucket : t -> string
  val name : t -> Name.t
  val description : t -> string option
  val headers : t -> Nats.Header.t
  val metadata : t -> (string * string) list
  val link : t -> Link.t option
  val nuid : t -> string
  val size : t -> int64
  val chunks : t -> int64
  val chunk_size : t -> int
  val digest : t -> string
  val deleted : t -> bool
  val timestamp : t -> string
end

module Error : sig
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

  val pp_config : Format.formatter -> config -> unit
  val pp_name : Format.formatter -> name -> unit
  val pp_meta : Format.formatter -> meta -> unit
  val pp : Format.formatter -> t -> unit
end

module Status : sig
  type t
  (** A bucket-oriented snapshot of the backing stream. *)

  val bucket : t -> string
  val description : t -> string option
  val messages : t -> int64
  val bytes : t -> int64
  val first_sequence : t -> int64
  val last_sequence : t -> int64
  val ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val storage : t -> Config.storage
end

type t
(** A local capability for one object-store bucket. *)

val create : Jetstream.t -> Config.t -> (t, Error.t) result
(** [create jetstream config] creates the backing stream and returns its
    bucket capability. *)

val bind : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [bind jetstream ~bucket] creates a local handle without contacting the
    server. *)

val open_ : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [open_ jetstream ~bucket] binds to and checks the existing backing stream. *)

val delete_bucket : t -> (unit, Error.t) result
(** [delete_bucket bucket] deletes the backing JetStream stream. *)

val status : t -> (Status.t, Error.t) result

val info :
  ?timeout:Mtime.Span.t ->
  ?include_deleted:bool ->
  t ->
  Name.t ->
  (Info.t, Error.t) result
(** [info ?include_deleted bucket name] reads the latest metadata record.
    Deleted records are treated as {!Error.Not_found} unless explicitly
    included. *)

val put :
  ?timeout:Mtime.Span.t ->
  t ->
  Meta.t ->
  Bytesrw.Bytes.Reader.t ->
  (Info.t, Error.t) result
(** [put bucket meta reader] uploads chunks incrementally, publishes metadata
    as the commit point, verifies the server record, and removes superseded
    chunks. An interrupted pre-commit upload is purged best-effort. *)

val put_string :
  ?timeout:Mtime.Span.t ->
  t ->
  Meta.t ->
  string ->
  (Info.t, Error.t) result

val get :
  ?timeout:Mtime.Span.t ->
  ?include_deleted:bool ->
  t ->
  Name.t ->
  Bytesrw.Bytes.Writer.t ->
  (Info.t, Error.t) result
(** [get bucket name writer] streams and verifies the object's chunks before
    writing end-of-data to [writer]. *)

val get_string :
  ?timeout:Mtime.Span.t ->
  ?include_deleted:bool ->
  t ->
  Name.t ->
  (string, Error.t) result

val delete :
  ?timeout:Mtime.Span.t ->
  t ->
  Name.t ->
  (unit, Error.t) result
(** [delete bucket name] publishes a deleted metadata marker and purges the
    object's chunks. *)
