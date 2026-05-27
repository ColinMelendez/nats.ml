(** Typed JetStream management and publishing over a Core NATS connection. *)

module Error : sig
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Empty_subjects
    | Invalid_limit of { field : string; value : int64 }
    | Invalid_max_age
    | Invalid_replicas of int
    | Empty_placement
    | Empty_placement_cluster
    | Empty_placement_tag
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
    | Message_not_found
    | Invalid_message_header of { name : string; value : string }
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
    type compression = Uncompressed | S2
    (** The server-side stream compression policy. *)

    module Placement : sig
      type t
      type error = Error.config

      val v :
        ?cluster:string ->
        ?tags:string list ->
        unit ->
        (t, error) result
      (** [v ?cluster ?tags ()] validates a placement constraint. At least one
          of [cluster] and [tags] must be supplied. *)

      val cluster : t -> string option
      val tags : t -> string list
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
      ?compression:compression ->
      ?metadata:(string * string) list ->
      ?retention:retention ->
      ?discard:discard ->
      ?max_msgs:int64 ->
      ?max_msgs_per_subject:int64 ->
      ?max_bytes:int64 ->
      ?max_age:Mtime.Span.t ->
      ?max_msg_size:int64 ->
      ?allow_rollup:bool ->
      ?allow_direct:bool ->
      ?deny_delete:bool ->
      ?sealed:bool ->
      unit ->
      (t, error) result
    (** [v] validates a stream name, capture filters, and limits. Limits use
        [-1] for the JetStream unlimited value when supplied. [replicas] must be
        between 1 and 5. [deny_delete] controls whether stream-level message
        deletion is rejected. *)

    val name : t -> string
    val subjects : t -> Nats.Subject.Filter.t list
    val description : t -> string option
    val storage : t -> storage
    val replicas : t -> int
    val placement : t -> Placement.t option
    val compression : t -> compression
    val metadata : t -> (string * string) list
    val retention : t -> retention
    val discard : t -> discard
    val max_msgs : t -> int64 option
    val max_msgs_per_subject : t -> int64 option
    val max_bytes : t -> int64 option
    val max_age : t -> Mtime.Span.t option
    val max_msg_size : t -> int64 option
    val allow_rollup : t -> bool
    val allow_direct : t -> bool
    (** [allow_direct config] is [true] when direct message reads are enabled.
    *)
    val deny_delete : t -> bool
    (** [deny_delete config] is [true] when stream-level message deletion is
    rejected. *)
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
    (** [with_replicas config value] validates and replaces the replica count. *)

    val with_placement : t -> Placement.t option -> (t, error) result
    (** [with_placement config value] replaces the placement constraint. *)

    val with_compression : t -> compression -> (t, error) result
    (** [with_compression config value] replaces the storage compression mode. *)

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

    val with_allow_rollup : t -> bool -> (t, error) result
    (** [with_allow_rollup config value] replaces whether rollup headers are
        accepted by the stream. *)

    val with_allow_direct : t -> bool -> (t, error) result
    (** [with_allow_direct config value] replaces whether direct message reads
        are accepted by the stream. *)

    val with_deny_delete : t -> bool -> (t, error) result
    (** [with_deny_delete config value] replaces whether deleting the stream's
        messages through the stream API is rejected. *)

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
  val get :
    ?timeout:Mtime.Span.t -> t -> sequence:int64 -> (Message.t, Error.t) result
  (** [get stream ~sequence] retrieves one stored message by stream sequence
      through JetStream's direct message API. *)

  val get_last :
    ?timeout:Mtime.Span.t -> t -> subject:Nats.Subject.t -> (Message.t, Error.t) result
  (** [get_last stream ~subject] retrieves the latest stored message for an
      exact subject through JetStream's direct message API. *)

  val purge :
    ?timeout:Mtime.Span.t ->
    ?subject:Nats.Subject.Filter.t ->
    t ->
    (int64, Error.t) result
  (** [purge ?subject stream] removes messages from [stream]. With [subject],
      only messages matching the subject filter are removed. The result is the
      number of messages the server purged. *)

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
    (** [Some (-1)] means unlimited; [None] leaves the server default when a
        consumer is created. *)
    val max_waiting : t -> int option
    val max_batch : t -> int option
    val max_expires : t -> Mtime.Span.t option
    val max_bytes : t -> int option
    val headers_only : t -> bool option
    val inactive_threshold : t -> Mtime.Span.t option
    val mem_storage : t -> bool option

    val with_durable_name : t -> string option -> (t, error) result
    (** [with_durable_name config value] replaces the durable identity. *)

    val with_description : t -> string option -> (t, error) result
    (** [with_description config value] replaces the consumer description. *)

    val with_deliver_subject :
      t -> Nats.Subject.t option -> (t, error) result
    (** [with_deliver_subject config value] replaces the push delivery subject.
    *)

    val with_deliver_group :
      t -> Nats.Queue_group.t option -> (t, error) result
    (** [with_deliver_group config value] replaces the push queue group. *)

    val with_idle_heartbeat :
      t -> Mtime.Span.t option -> (t, error) result
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
    (** [with_filter_subject config value] replaces the subject filter. *)

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

    val with_inactive_threshold :
      t -> Mtime.Span.t option -> (t, error) result
    (** [with_inactive_threshold config value] replaces inactivity cleanup. *)

    val with_mem_storage : t -> bool option -> (t, error) result
    (** [with_mem_storage config value] replaces consumer state storage. *)
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

  val create :
    ?timeout:Mtime.Span.t -> stream -> Config.t -> (t, Error.t) result
  (** [create ?timeout stream config] creates a server-side consumer and
      returns its name. *)

  val update :
    ?timeout:Mtime.Span.t -> t -> Config.t -> (Info.t, Error.t) result
  (** [update ?timeout consumer config] applies the modeled fields in [config]
      to an existing consumer and returns its resulting information. The
      configuration is a full replacement of the fields modeled by {!Config.t};
      use the {!Config.with_description} family to derive a replacement from
      {!Info.config}. The operation reads the current server configuration first
      and preserves fields not modeled by {!Config.t}. The consumer identity
      remains tied to [name consumer]; a supplied durable name must match it,
      while an omitted durable name retains an existing durable identity.
      Concurrent changes use last-writer-wins semantics. *)

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
        [Not_push_consumer] when no delivery subject is configured. The
        configured queue group is used for the subscription. Idle-heartbeat
        status frames are consumed transparently, and flow-control requests
        are answered with an empty message. A missing heartbeat fails the
        session with [Missing_heartbeat]. The session owns its subscription
        and closes it when [sw] releases. A replayable delivery subscription
        is restored after transport recovery; durable consumers are checked
        with [info], while missing ephemeral consumers are recreated from
        their last configuration. The session is single-owner: do not call
        [next] or [next_with_timeout] concurrently on one value. *)

    val create :
      sw:Eio.Switch.t -> Stream.t -> Config.t -> (t, Error.t) result
    (** [create ~sw stream config] creates and owns an ephemeral push
        consumer. If [config] has no delivery subject, a fresh inbox is
        chosen. Durable names are rejected. A five-minute inactive threshold
        and memory storage are supplied when absent. The delivery subscription
        is installed before the consumer is created, so retained messages
        cannot race the initial subscription. The consumer is deleted when
        [close] is called or [sw] releases. *)

    val consumer : t -> consumer
    (** [consumer push] is the current server-side consumer. An ephemeral
        consumer may change after recovery. *)

    val initial_pending : t -> int64
    (** [initial_pending push] is the server-reported pending count from the
        consumer setup that created [push]. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next push] waits for the next delivered message. Messages are not
        acknowledged automatically. Idle-heartbeat frames are consumed and
        flow-control requests are answered with an empty message. Other status
        frames fail the handle instead of being treated as data. A configured
        heartbeat that is not received within two intervals fails with
        [Missing_heartbeat]. During reconnect recovery, heartbeat deadlines
        are suspended until the subscription is replayed and the consumer is
        confirmed or recreated. *)

    val next_with_timeout : timeout:Mtime.Span.t -> t -> (Msg.t, Error.t) result
    (** [next_with_timeout ~timeout push] bounds the wait with an absolute
        caller deadline across reconnect restoration; control frames do not
        extend it. A timeout leaves an open push handle active. A missing
        configured heartbeat fails the handle with [Missing_heartbeat]. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter push ~f] invokes [f] for each message until the handle is closed
        or fails. It does not acknowledge messages and has the same
        single-owner rule as [next]. *)

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
      stream -> (t, Error.t) result
    (** [v ~sw stream] creates a client-managed ephemeral pull consumer. The
        initial delivery policy defaults to [All]. Ordered sessions always use
        [No_ack], memory storage, and a five-minute inactive threshold; they
        request idle heartbeats (five seconds by default) to detect a lost
        consumer. The session owns its pull subscription and recreates the
        ephemeral consumer after a consumer-sequence gap, a missing heartbeat,
        consumer deletion, or a non-replayed transport disconnect. Recreated
        consumers resume at the next stream sequence. *)

    val next : t -> (Msg.t, Error.t) result
    (** [next ordered] returns the next message in consumer order. A call may
        perform consumer deletion and recreation before returning. Stream
        sequence numbers may skip when a filter is used; consumer sequence
        numbers must remain consecutive. *)

    val next_with_timeout : timeout:Mtime.Span.t -> t -> (Msg.t, Error.t) result
    (** [next_with_timeout ~timeout ordered] uses an absolute caller deadline
        across waiting and ordered-consumer recreation. A normal timeout leaves
        the current session open; a timeout after the old consumer has been
        torn down fails the session. *)

    val iter : t -> f:(Msg.t -> unit) -> (unit, Error.t) result
    (** [iter ordered ~f] invokes [f] for each ordered message until the
        session is closed or fails. Messages are not acknowledged. *)

    val close : t -> (unit, Error.t) result
    (** [close ordered] stops the pull session, best-effort deletes its current
        ephemeral consumer, and is idempotent. Explicit closure returns
        [Ordered_closed] from subsequent reads. *)
  end

  val info : ?timeout:Mtime.Span.t -> t -> (Info.t, Error.t) result
  val delete : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
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
