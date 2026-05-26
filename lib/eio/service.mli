(** Typed NATS Services over Core subscriptions and request/reply. *)

module Error : sig
  (** Recoverable service validation, request, encoding, and lifecycle errors.
  *)

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
  (** [pp_config ppf error] formats a configuration validation error. *)

  val pp_endpoint : Format.formatter -> endpoint -> unit
  (** [pp_endpoint ppf error] formats an endpoint validation error. *)

  val pp : Format.formatter -> t -> unit
  (** [pp ppf error] formats a service error for diagnostics. *)
end

module Config : sig
  (** Queue selection for service endpoints. [Default] inherits the enclosing
      service or group policy; [Disabled] omits the queue group. *)
  type queue_policy = Default | Queue of Nats.Queue_group.t | Disabled

  type t
  (** A validated service identity and default endpoint policy. *)

  val v :
    name:string ->
    version:string ->
    ?description:string ->
    ?metadata:(string * string) list ->
    ?queue:queue_policy ->
    unit ->
    (t, Error.t) result
  (** [v ~name ~version ?description ?metadata ?queue ()] validates a service
      identity. Names use letters, digits, [-], and [_]. Versions use the
      semantic-version grammar accepted by NATS Services. Metadata keys must be
      unique. The default queue policy is [Default]. *)

  val name : t -> string
  (** [name config] is the service name. *)

  val version : t -> string
  (** [version config] is the service version. *)

  val description : t -> string option
  (** [description config] is the optional service description. *)

  val metadata : t -> (string * string) list
  (** [metadata config] is the service metadata in insertion order. *)

  val queue : t -> queue_policy
  (** [queue config] is the service's default endpoint queue policy. *)
end

module Request : sig
  type t
  (** One incoming service request and its single reply opportunity. *)

  val subject : t -> Nats.Subject.t
  (** [subject request] is the request subject. *)

  val reply : t -> Nats.Subject.t option
  (** [reply request] is the optional reply subject. *)

  val headers : t -> Nats.Header.t
  (** [headers request] are the incoming request headers. *)

  val payload : t -> string
  (** [payload request] is the incoming request payload. *)

  val respond : ?headers:Nats.Header.t -> t -> string -> (unit, Error.t) result
  (** [respond ?headers request payload] publishes one successful reply. A
      request without a reply subject, a repeated response, or a failed publish
      returns a structured error. *)

  val respond_error :
    code:string ->
    description:string ->
    ?headers:Nats.Header.t ->
    ?payload:string ->
    t ->
    (unit, Error.t) result
  (** [respond_error ~code ~description ?headers ?payload request] publishes a
      reply carrying NATS Service error headers. [code] and [description] must
      be non-empty. *)
end

module Endpoint : sig
  type t
  (** A named service endpoint and its request handler. *)

  type handler = Request.t -> (unit, Error.t) result
  (** A handler runs once per incoming delivery. Returning [Error e] records an
      endpoint failure and keeps the service worker alive. Exceptions are also
      recorded as handler failures. *)

  val v :
    name:string ->
    ?subject:Nats.Subject.Filter.t ->
    ?metadata:(string * string) list ->
    ?queue:Config.queue_policy ->
    handler ->
    (t, Error.t) result
  (** [v ~name ?subject ?metadata ?queue handler] validates an endpoint.
      [subject] defaults to the endpoint name; [queue] defaults to [Default]. *)

  val name : t -> string
  (** [name endpoint] is the endpoint name. *)

  val subject : t -> Nats.Subject.Filter.t
  (** [subject endpoint] is the endpoint's relative subscription filter. *)

  val metadata : t -> (string * string) list option
  (** [metadata endpoint] is the optional endpoint metadata. *)

  val queue : t -> Config.queue_policy
  (** [queue endpoint] is the endpoint's queue policy. *)
end

module Group : sig
  type t
  (** A namespace for composing endpoint subjects and queue policy. *)

  val name : t -> string
  (** [name group] is the group's final path component. *)

  val subject : t -> Nats.Subject.t
  (** [subject group] is the full group subject prefix. *)

  val add_endpoint : t -> Endpoint.t -> (unit, Error.t) result
  (** [add_endpoint group endpoint] subscribes the endpoint under the group. *)

  val add_group :
    ?queue:Config.queue_policy -> t -> name:string -> (t, Error.t) result
  (** [add_group ?queue group ~name] creates a nested group. *)
end

module Info : sig
  type endpoint
  (** A monitoring snapshot of service identity and endpoint declarations. *)

  type t

  val name : t -> string
  (** [name info] is the service name. *)

  val id : t -> string
  (** [id info] is the service instance id. *)

  val version : t -> string
  (** [version info] is the service version. *)

  val description : t -> string option
  (** [description info] is the optional service description. *)

  val metadata : t -> (string * string) list
  (** [metadata info] is the service metadata. *)

  val endpoints : t -> endpoint list
  (** [endpoints info] is the endpoint snapshot in registration order. *)

  val endpoint_name : endpoint -> string
  (** [endpoint_name endpoint] is the endpoint name. *)

  val endpoint_subject : endpoint -> Nats.Subject.Filter.t
  (** [endpoint_subject endpoint] is the full endpoint filter. *)

  val endpoint_queue : endpoint -> Nats.Queue_group.t option
  (** [endpoint_queue endpoint] is the effective queue group, if any. *)

  val endpoint_metadata : endpoint -> (string * string) list option
  (** [endpoint_metadata endpoint] is the endpoint metadata. *)
end

module Stats : sig
  type endpoint
  (** A monitoring snapshot of service endpoint processing statistics. *)

  type t

  val name : t -> string
  (** [name stats] is the service name. *)

  val id : t -> string
  (** [id stats] is the service instance id. *)

  val version : t -> string
  (** [version stats] is the service version. *)

  val metadata : t -> (string * string) list
  (** [metadata stats] is the service metadata. *)

  val started : t -> string
  (** [started stats] is the RFC3339 UTC service start timestamp. *)

  val endpoints : t -> endpoint list
  (** [endpoints stats] is the endpoint statistics snapshot in registration
      order. *)

  val endpoint_name : endpoint -> string
  (** [endpoint_name endpoint] is the endpoint name. *)

  val endpoint_subject : endpoint -> Nats.Subject.Filter.t
  (** [endpoint_subject endpoint] is the full endpoint filter. *)

  val endpoint_queue : endpoint -> Nats.Queue_group.t option
  (** [endpoint_queue endpoint] is the effective queue group, if any. *)

  val endpoint_metadata : endpoint -> (string * string) list option
  (** [endpoint_metadata endpoint] is the endpoint metadata. *)

  val num_requests : endpoint -> int64
  (** [num_requests endpoint] is the number of delivered requests. *)

  val num_errors : endpoint -> int64
  (** [num_errors endpoint] is the number of handler or response failures. *)

  val last_error : endpoint -> string
  (** [last_error endpoint] is the latest diagnostic, or [""]. *)

  val processing_time : endpoint -> int64
  (** [processing_time endpoint] is cumulative processing time in nanoseconds.
  *)

  val average_processing_time : endpoint -> int64
  (** [average_processing_time endpoint] is the average processing time in
      nanoseconds, or [0]. *)
end

type t
(** A service owns its monitoring and endpoint subscriptions, but not the
    connection that carries them. [sw] should be a child lifetime of the
    connection lifetime. *)

val v :
  sw:Eio.Switch.t ->
  clock:_ Eio.Time.clock ->
  ?random:Random.State.t ->
  Connection.t ->
  Config.t ->
  (t, Error.t) result
(** [v ~sw ~clock ?random connection config] subscribes the service monitoring
    subjects and starts its session. [sw] must be a child lifetime of the
    connection lifetime. The generated service id is stable for the session;
    [clock] supplies the wall-clock start timestamp. *)

val name : t -> string
(** [name service] is the configured service name. *)

val id : t -> string
(** [id service] is the generated service instance id. *)

val add_endpoint : t -> Endpoint.t -> (unit, Error.t) result
(** [add_endpoint service endpoint] subscribes and starts [endpoint]. *)

val add_group :
  ?queue:Config.queue_policy -> t -> name:string -> (Group.t, Error.t) result
(** [add_group ?queue service ~name] creates a top-level endpoint namespace. *)

val info : t -> Info.t
(** [info service] returns a consistent endpoint declaration snapshot. *)

val stats : t -> Stats.t
(** [stats service] returns a consistent processing statistics snapshot. *)

val stop : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
(** [stop service] drains only subscriptions owned by [service], waits for
    endpoint and monitoring workers already in progress, and leaves the parent
    connection usable. It is idempotent after a successful stop. *)
