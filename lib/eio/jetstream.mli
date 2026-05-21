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
    | Empty_msg_id
    | Msg_id_already_set
    | Unexpected_stream_name of { expected : string; actual : string }
    | Unexpected_consumer_name of { expected : string; actual : string }
    | Invalid_batch of int
    | Invalid_max_bytes of int
    | Invalid_fetch_span
    | Invalid_idle_heartbeat
    | Idle_heartbeat_expires_too_short
    | Missing_heartbeat
    | Missing_ack_reply
    | Invalid_ack_reply of string
    | Not_push_consumer
    | Unsupported_push_option of { field : string; value : string }
    | Consumer_deleted
    | Conflict of { code : int; description : string }
    | Unexpected_status of { code : int; description : string }
    | Incomplete_list of { kind : list_kind; missing : string list }
    | Pull_closed
    | Push_closed

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

    val with_name : t -> string -> (t, error) result
    (** [with_name config name] validates [name] while preserving the other
        fields. *)

    val with_subjects : t -> Nats.Subject.Filter.t list -> (t, error) result
    (** [with_subjects config subjects] validates [subjects] while preserving
        the other fields. An existing server-side mirror may retain an empty
        subject list. *)

    val with_storage : t -> storage -> (t, error) result
    (** [with_storage config storage] preserves all fields except storage. *)

    val with_retention : t -> retention -> (t, error) result
    (** [with_retention config retention] preserves all fields except retention.
    *)

    val with_discard : t -> discard -> (t, error) result
    (** [with_discard config discard] preserves all fields except discard. *)

    val with_max_msgs : t -> int64 option -> (t, error) result
    (** [with_max_msgs config value] validates and replaces the message limit.
        [None] means unlimited. *)

    val with_max_bytes : t -> int64 option -> (t, error) result
    (** [with_max_bytes config value] validates and replaces the byte limit.
        [None] means unlimited. *)

    val with_max_age : t -> Mtime.Span.t option -> (t, error) result
    (** [with_max_age config value] validates and replaces the age limit. [None]
        means unlimited. *)

    val with_max_msg_size : t -> int64 option -> (t, error) result
    (** [with_max_msg_size config value] validates and replaces the per-message
        size limit. [None] means unlimited. *)
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

  val name : t -> string
  val info : t -> (Info.t, Error.t) result
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
  (** [timestamp message] is the server's Unix-epoch timestamp in
      nanoseconds. *)

  val num_delivered : t -> int64
  val stream_sequence : t -> int64
  val consumer_sequence : t -> int64
  val num_pending : t -> int64
  val ack : t -> (unit, Error.t) result
  (** [ack_sync ?timeout message] sends [+ACK] and waits for the server to
      acknowledge receiving it. [timeout] defaults to the connection request
      timeout. A missing response returns [Error (Connection Timeout)] and a
      server without a responder returns [Error (Connection No_responders)]. *)
  val ack_sync : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
  val nak : ?delay:Mtime.Span.t -> t -> (unit, Error.t) result
  val term : ?reason:string -> t -> (unit, Error.t) result
  val in_progress : t -> (unit, Error.t) result
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
      ?deliver_subject:Nats.Subject.t ->
      ?deliver_group:Nats.Queue_group.t ->
      ?idle_heartbeat:Mtime.Span.t ->
      ?flow_control:bool ->
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
    val deliver_subject : t -> Nats.Subject.t option
    val deliver_group : t -> Nats.Queue_group.t option
    val idle_heartbeat : t -> Mtime.Span.t option
    val flow_control : t -> bool option
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

  val list : stream -> (Info.t list, Error.t) result
  (** [list stream] returns detailed information for all consumers on [stream].
  *)

  val name : t -> string
  val stream : t -> stream

  val fetch :
    ?expires:Mtime.Span.t ->
    ?idle_heartbeat:Mtime.Span.t ->
    ?max_bytes:int ->
    t ->
    batch:int ->
    (Msg.t list, Error.t) result
  (** [fetch consumer ~batch] requests up to [batch] messages and returns an
      empty or partial list when the server expires the pull request. When
      [idle_heartbeat] is set, status-100 idle heartbeats keep the request
      alive; failure to receive one within two heartbeat intervals returns
      [Missing_heartbeat]. The heartbeat must be positive and no greater than
      half of [expires]. *)

  module Pull : sig
    type consumer = t
    type t

    val v :
      sw:Eio.Switch.t ->
      ?batch:int ->
      ?expires:Mtime.Span.t ->
      ?idle_heartbeat:Mtime.Span.t ->
      ?max_bytes:int ->
      consumer ->
      (t, Error.t) result
    (** [v ~sw consumer] opens a persistent pull session using a fresh reply
        inbox. The default batch is one and the default server expiry is five
        seconds. With [idle_heartbeat], status-100 idle heartbeats keep the
        outstanding request alive and a missing heartbeat fails the session with
        [Missing_heartbeat]. The heartbeat must be positive and no greater than
        half of [expires]. The session owns its subscription and closes it when
        [sw] releases. A session is not transparently restored after a transport
        loss; recreate it after receiving [Error (Connection Disconnected)]. A
        pull session is single-owner: do not call [next] or [next_with_timeout]
        concurrently on the same value. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next pull] waits for the next message. Empty pull batches and the
        JetStream [408], [batch completed], and configured idle-heartbeat
        statuses are handled internally. Other statuses, including
        [message size exceeds maxbytes], fail the session. Messages are not
        acknowledged automatically. Explicit closure returns [Pull_closed]. *)

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

  module Push : sig
    type consumer = t
    type t

    val v : sw:Eio.Switch.t -> consumer -> (t, Error.t) result
    (** [v ~sw consumer] subscribes to the delivery subject configured on a
        push consumer. It reads the server-side configuration and returns
        [Not_push_consumer] when no delivery subject is configured. Consumers
        with [idle_heartbeat] or [flow_control] enabled return
        [Unsupported_push_option] until push control frames are supported. The
        configured queue group is used for the subscription. The session owns
        its subscription and closes it when [sw] releases. It is not
        transparently restored after a transport loss; recreate it after
        [Error (Connection Disconnected)]. The session is single-owner: do not
        call [next] or [next_with_timeout] concurrently on one value. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next push] waits for the next delivered message. Messages are not
        acknowledged automatically. Status frames fail the handle instead of
        being treated as data. *)

    val next_with_timeout : timeout:Mtime.Span.t -> t -> (Msg.t, Error.t) result
    (** [next_with_timeout ~timeout push] bounds the wait and leaves an open
        push handle active after a timeout. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter push ~f] invokes [f] for each message until the handle is closed
        or fails. It does not acknowledge messages and has the same
        single-owner rule as [next]. *)

    val close : t -> (unit, Error.t) result
    (** [close push] stops the subscription and is idempotent. *)
  end

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
