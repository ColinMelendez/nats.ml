(** Typed NATS Services over Core subscriptions and request/reply. *)

module Error : sig
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Invalid_version of string
    | Duplicate_metadata of string

  type endpoint =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Duplicate_metadata of string

  type t =
    | Connection of Connection.error
    | Invalid_config of config
    | Invalid_endpoint of endpoint
    | Invalid_group_subject of Nats.Subject.error
    | Duplicate_endpoint of string
    | Invalid_headers of Nats.Header.error
    | Invalid_service_error of { code : string; description : string }
    | No_reply_subject
    | Already_responded
    | No_response
    | Service_error of { code : string; description : string }
    | Handler_raised
    | Encode of Jsont.Error.t
    | Stopped

  val pp_config : Format.formatter -> config -> unit
  val pp_endpoint : Format.formatter -> endpoint -> unit
  val pp : Format.formatter -> t -> unit
end

module Config : sig
  type queue_policy = Default | Queue of Nats.Queue_group.t | Disabled
  type t

  val v :
    name:string ->
    version:string ->
    ?description:string ->
    ?metadata:(string * string) list ->
    ?queue:queue_policy ->
    unit ->
    (t, Error.t) result

  val name : t -> string
  val version : t -> string
  val description : t -> string option
  val metadata : t -> (string * string) list
  val queue : t -> queue_policy
end

module Request : sig
  type t

  val subject : t -> Nats.Subject.t
  val reply : t -> Nats.Subject.t option
  val headers : t -> Nats.Header.t
  val payload : t -> string
  val respond : ?headers:Nats.Header.t -> t -> string -> (unit, Error.t) result

  val respond_error :
    code:string ->
    description:string ->
    ?headers:Nats.Header.t ->
    ?payload:string ->
    t ->
    (unit, Error.t) result
end

module Endpoint : sig
  type t
  type handler = Request.t -> unit

  val v :
    name:string ->
    ?subject:Nats.Subject.Filter.t ->
    ?metadata:(string * string) list ->
    ?queue:Config.queue_policy ->
    handler ->
    (t, Error.t) result

  val name : t -> string
  val subject : t -> Nats.Subject.Filter.t
  val metadata : t -> (string * string) list option
  val queue : t -> Config.queue_policy
end

module Group : sig
  type t

  val name : t -> string
  val subject : t -> Nats.Subject.t
  val add_endpoint : t -> Endpoint.t -> (unit, Error.t) result

  val add_group :
    ?queue:Config.queue_policy -> t -> name:string -> (t, Error.t) result
end

module Info : sig
  type endpoint
  type t

  val name : t -> string
  val id : t -> string
  val version : t -> string
  val description : t -> string option
  val metadata : t -> (string * string) list
  val endpoints : t -> endpoint list
  val endpoint_name : endpoint -> string
  val endpoint_subject : endpoint -> Nats.Subject.Filter.t
  val endpoint_queue : endpoint -> Nats.Queue_group.t option
  val endpoint_metadata : endpoint -> (string * string) list option
end

module Stats : sig
  type endpoint
  type t

  val name : t -> string
  val id : t -> string
  val version : t -> string
  val metadata : t -> (string * string) list
  val started : t -> string
  val endpoints : t -> endpoint list
  val endpoint_name : endpoint -> string
  val endpoint_subject : endpoint -> Nats.Subject.Filter.t
  val endpoint_queue : endpoint -> Nats.Queue_group.t option
  val endpoint_metadata : endpoint -> (string * string) list option
  val num_requests : endpoint -> int64
  val num_errors : endpoint -> int64
  val last_error : endpoint -> string
  val processing_time : endpoint -> int64
  val average_processing_time : endpoint -> int64
end

type t
(** A Service owns its monitoring and endpoint subscriptions, but not the
    connection that carries them. *)

val v :
  sw:Eio.Switch.t ->
  clock:_ Eio.Time.clock ->
  ?random:Random.State.t ->
  Connection.t ->
  Config.t ->
  (t, Error.t) result

val name : t -> string
val id : t -> string
val add_endpoint : t -> Endpoint.t -> (unit, Error.t) result

val add_group :
  ?queue:Config.queue_policy -> t -> name:string -> (Group.t, Error.t) result

val info : t -> Info.t
val stats : t -> Stats.t

val stop : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
(** [stop service] drains only subscriptions owned by [service], waits for
    endpoint handlers already in progress, and leaves the parent connection
    usable. It is idempotent after a successful stop. *)
