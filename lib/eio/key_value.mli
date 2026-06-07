(** JetStream-backed key-value buckets.

    Buckets expose typed keys, revisioned entries, compare-and-set mutations,
    and status without exposing the backing stream as the primary abstraction.
    Construct a bucket with {!create}, {!open_}, or {!bind}; construct keys with
    {!Key.of_string}. *)

module Config : sig
  (** {1:types Types} *)

  type storage = Memory | File  (** The storage backend used by the bucket. *)

  type t
  (** A validated bucket configuration. *)

  type error =
    | Empty_bucket
    | Invalid_bucket_character of { position : int; character : char }
    | Invalid_history of int
    | Invalid_ttl
    | Invalid_limit_marker_ttl
    | Invalid_limit of { field : string; value : int64 }
        (** Errors produced while validating a bucket configuration. *)

  (** {1:constructors Constructors} *)

  val v :
    bucket:string ->
    ?history:int ->
    ?ttl:Mtime.Span.t ->
    ?limit_marker_ttl:Mtime.Span.t ->
    ?max_bytes:int64 ->
    ?max_value_size:int64 ->
    ?storage:storage ->
    unit ->
    (t, error) result
  (** [v ~bucket ()] validates a bucket configuration.

      [history] defaults to [1] and must be in [1, 64]. [ttl] defaults to an
      unlimited lifetime; a zero span has the same meaning. [limit_marker_ttl]
      defaults to disabled; a zero span has the same meaning and a positive span
      enables server-side per-message TTLs. Limit options use [None] for the
      unlimited value and accept [-1] when supplied for direct JetStream
      correspondence; supplied [-1] is normalized to [None]. [storage] defaults
      to {!File}. *)

  (** {1:queries Queries} *)

  val bucket : t -> string
  (** [bucket config] is the bucket name. *)

  val history : t -> int
  (** [history config] is the number of retained revisions per key. *)

  val ttl : t -> Mtime.Span.t option
  (** [ttl config] is the optional revision lifetime. *)

  val limit_marker_ttl : t -> Mtime.Span.t option
  (** [limit_marker_ttl config] is the optional lifetime of delete markers.
      Setting it enables server-side per-message TTLs for key creation and purge
      markers. *)

  val max_bytes : t -> int64 option
  (** [max_bytes config] is the optional bucket byte limit. *)

  val max_value_size : t -> int64 option
  (** [max_value_size config] is the optional value-size limit. *)

  val storage : t -> storage
  (** [storage config] is the configured storage backend. *)
end

module Key : sig
  (** Valid bucket-relative key names. *)

  type t
  (** A key with non-empty dot-separated tokens and no wildcard tokens. *)

  type error =
    | Empty_key
    | Invalid_key_character of { position : int; character : char }
    | Invalid_key_dots  (** Errors produced while validating a key. *)

  val of_string : string -> (t, error) result
  (** [of_string value] validates an external key. *)

  val to_string : t -> string
  (** [to_string key] is the bucket-relative wire representation of [key]. *)
end

module Entry : sig
  (** One retained bucket revision, including tombstones. *)

  type operation =
    | Put
    | Delete
    | Purge  (** The operation that produced an entry. *)

  type t
  (** An immutable bucket entry.

      [timestamp] is the server's RFC3339 wire timestamp. [revision] is the
      JetStream stream sequence and is positive for entries returned by the
      server. *)

  val bucket : t -> string
  (** [bucket entry] is the owning bucket name. *)

  val key : t -> Key.t
  (** [key entry] is the validated key carried by [entry]. *)

  val value : t -> string
  (** [value entry] is the stored payload. Tombstones have an empty value. *)

  val revision : t -> int64
  (** [revision entry] is the JetStream stream sequence of [entry]. *)

  val timestamp : t -> string
  (** [timestamp entry] is the server timestamp in RFC3339 wire form. *)

  val operation : t -> operation
  (** [operation entry] is the operation represented by [entry]. *)

  val pp_operation : Format.formatter -> operation -> unit
  (** [pp_operation ppf operation] formats an operation for diagnostics. *)
end

module Error : sig
  (** Recoverable bucket and JetStream errors. *)

  type config = Config.error
  type key = Key.error

  type t =
    | Connection of Connection.error
    | Jetstream of Jetstream.Error.t
    | Invalid_config of config
    | Invalid_key of { value : string; reason : key }
    | Invalid_revision of int64
    | Invalid_key_ttl
    | Invalid_marker_ttl
    | Invalid_purge_age
    | Invalid_watch_filters
    | Invalid_headers of Nats.Header.error
    | Invalid_operation of string
    | Invalid_filter of { value : string; reason : Nats.Subject.error }
    | Invalid_message_subject of string
    | Invalid_timestamp of int64
    | Invalid_timestamp_text of string
    | Key_not_found
    | Key_deleted of Entry.t
    | Key_exists
    | Revision_mismatch of { expected : int64 }
    | Closed
        (** Errors returned by bucket operations. [Key_deleted] carries the
            tombstone that hides a key. [Revision_mismatch] is produced only for
            a server compare-and-set rejection. [Closed] is returned by a watch
            after explicit or switch-owned closure. *)

  val pp_config : Format.formatter -> config -> unit
  (** [pp_config ppf error] formats a configuration error. *)

  val pp_key : Format.formatter -> key -> unit
  (** [pp_key ppf error] formats a key validation error. *)

  val pp : Format.formatter -> t -> unit
  (** [pp ppf error] formats an error for diagnostics. *)
end

module Status : sig
  (** A bucket-oriented snapshot of the backing stream. *)

  type t
  (** A status snapshot obtained from {!status}. *)

  val bucket : t -> string
  val values : t -> int64
  val bytes : t -> int64
  val first_revision : t -> int64
  val last_revision : t -> int64
  val history : t -> int64 option
  val ttl : t -> Mtime.Span.t option
  val limit_marker_ttl : t -> Mtime.Span.t option
  val max_bytes : t -> int64 option
  val max_value_size : t -> int64 option
  val storage : t -> Config.storage
end

type t
(** A local capability for one bucket. *)

type bucket = t
(** The bucket capability consumed by {!Watch}. *)

type purge_age =
  | Default
  | Any
  | Older_than of Mtime.Span.t
      (** The age policy used by {!purge_deletes}. [Default] keeps recent
          markers for thirty minutes, [Any] removes every current marker, and
          [Older_than] keeps markers newer than the supplied positive span.
          Keeping a recent marker still removes older revisions for that key,
          matching JetStream's per-subject [keep:1] purge semantics. *)

module Watch : sig
  type delivery =
    | New
    | Last_per_subject
    | All  (** The retained messages delivered before the initial marker. *)

  type event =
    | Initial_done
    | Entry of Entry.t
        (** A watch event. [Initial_done] is emitted once after the retained
            snapshot selected by [delivery]. *)

  type t
  (** An owned, cancellable key-value watch. Calls to [next] are single-owner;
      do not call [next] or [next_with_timeout] concurrently. *)

  val v :
    sw:Eio.Switch.t ->
    ?key:string ->
    ?keys:string list ->
    ?delivery:delivery ->
    ?ignore_deletes:bool ->
    ?meta_only:bool ->
    ?resume_from_revision:int64 ->
    bucket ->
    (t, Error.t) result
  (** [v ~sw ?key ?keys ?delivery ?ignore_deletes ?meta_only
       ?resume_from_revision value] watches bucket-relative key filters. [key]
      is a shorthand for one filter; [keys] adds multiple filters and cannot be
      supplied together with [key]. The default delivery policy is
      [Last_per_subject]. [Initial_done] follows the retained snapshot; [New]
      emits it immediately. Delete and purge entries are delivered unless
      [ignore_deletes] is true. [meta_only] suppresses values while retaining
      entry metadata. [resume_from_revision] starts delivery at a positive
      JetStream stream revision, inclusively; pass the last processed revision
      plus one when resuming after an entry. The watch owns an ephemeral server
      and closes it with [sw]. Delivery order is the order observed by the push
      session; reconnect recovery does not provide the stronger gap-detection
      guarantees of an ordered consumer. *)

  val next : t -> (event, Error.t) result
  (** [next watch] returns the next watch event. *)

  val next_with_timeout : timeout:Mtime.Span.t -> t -> (event, Error.t) result
  (** [next_with_timeout ~timeout watch] bounds the wait across skipped
      tombstones and control events. A timeout leaves the watch open. *)

  val iter : t -> f:(event -> unit) -> (unit, Error.t) result
  (** [iter watch ~f] invokes [f] until the watch is closed or fails. *)

  val close : t -> (unit, Error.t) result
  (** [close watch] stops delivery, deletes the owned consumer, and is
      idempotent. *)
end

module Key_lister : sig
  type t
  (** An Eio-owned stream of current live keys. Entries may repeat when keys
      change while the initial listing is in progress. *)

  val v :
    sw:Eio.Switch.t -> ?filters:string list -> bucket -> (t, Error.t) result
  (** [v ~sw ?filters value] lists live keys matching the supplied
      bucket-relative filters. An empty list matches every key. *)

  val next : t -> (Key.t option, Error.t) result
  (** [next lister] returns the next key, or [Ok None] after the initial listing
      marker. Calls are single-owner. *)

  val next_with_timeout :
    timeout:Mtime.Span.t -> t -> (Key.t option, Error.t) result
  (** [next_with_timeout ~timeout lister] bounds one read. *)

  val close : t -> (unit, Error.t) result
  (** [close lister] closes the underlying watch. *)
end

val create : Jetstream.t -> Config.t -> (t, Error.t) result
(** [create jetstream config] creates the bucket's JetStream stream and returns
    a local bucket capability. *)

val open_ : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [open_ jetstream ~bucket] validates and checks an existing bucket. *)

val bind : Jetstream.t -> bucket:string -> (t, Error.t) result
(** [bind jetstream ~bucket] creates a local handle without contacting the
    server. *)

val bucket : t -> string
(** [bucket value] is the bucket name. *)

val status : t -> (Status.t, Error.t) result
(** [status value] returns the current bucket limits and stream counters. *)

val delete_bucket : t -> (unit, Error.t) result
(** [delete_bucket value] deletes the backing stream. *)

val put : t -> Key.t -> string -> (int64, Error.t) result
(** [put value key payload] appends a new value and returns its revision. *)

val create_key :
  ?ttl:Mtime.Span.t -> t -> Key.t -> string -> (int64, Error.t) result
(** [create_key ?ttl value key payload] creates [key] only when its current
    revision is zero. [ttl] sets a per-message lifetime for the created value;
    it requires the bucket's [limit_marker_ttl] capability. A tombstoned key is
    resurrected with a compare-and-set update at the tombstone revision. *)

val update : t -> Key.t -> revision:int64 -> string -> (int64, Error.t) result
(** [update value key ~revision payload] replaces [key] only when its current
    positive revision equals [revision]. *)

val delete : ?expected_revision:int64 -> t -> Key.t -> (int64, Error.t) result
(** [delete ?expected_revision value key] appends a delete tombstone. *)

val purge :
  ?expected_revision:int64 ->
  ?marker_ttl:Mtime.Span.t ->
  t ->
  Key.t ->
  (int64, Error.t) result
(** [purge ?expected_revision ?marker_ttl value key] appends a purge tombstone
    that rolls up older revisions for the subject. [marker_ttl] expires the
    purge marker after the supplied positive span and requires
    [limit_marker_ttl] to be enabled in the bucket. *)

val get : t -> Key.t -> (Entry.t, Error.t) result
(** [get value key] returns the latest entry for [key]. A delete or purge
    returns [Error (Key_deleted tombstone)]. *)

val get_revision : t -> Key.t -> revision:int64 -> (Entry.t, Error.t) result
(** [get_revision value key ~revision] returns the exact retained revision for
    [key]. [revision] must be positive. A revision belonging to another key
    returns [Key_not_found]. *)

val keys : ?filter:string -> t -> (Key.t list, Error.t) result
(** [keys ?filter value] returns live keys in server delivery order.

    [filter] is a bucket-relative NATS filter using exact tokens, [*], and a
    terminal [>]. It defaults to [>]. Tombstoned keys are omitted. *)

val purge_deletes : ?older_than:purge_age -> t -> (unit, Error.t) result
(** [purge_deletes ?older_than value] removes current delete and purge markers.
    By default markers newer than thirty minutes are retained; [Any] removes all
    markers. *)

val history : t -> Key.t -> (Entry.t list, Error.t) result
(** [history value key] returns retained entries for [key], oldest first. Put,
    delete, and purge entries are included. *)
