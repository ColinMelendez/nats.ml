(** A direct-style Core NATS connection running under Eio. *)

module Config : sig
  type t

  val v :
    ?core:Nats.Config.t ->
    ?auth:Nats.Auth.t ->
    ?command_capacity:int ->
    ?subscription_capacity:int ->
    ?event_capacity:int ->
    ?read_capacity:int ->
    ?read_chunk_size:int ->
    ?max_reconnect_attempts:int option ->
    ?reconnect_buffer_size:int ->
    ?reconnect_delay:Mtime.Span.t ->
    ?reconnect_max_delay:Mtime.Span.t ->
    ?reconnect_jitter:Mtime.Span.t ->
    ?random:Random.State.t ->
    ?tls:Tls.Config.client ->
    ?tls_required:bool ->
    ?inbox_prefix:string ->
    ?handshake_timeout:Mtime.Span.t ->
    ?request_timeout:Mtime.Span.t ->
    ?flush_timeout:Mtime.Span.t ->
    ?drain_timeout:Mtime.Span.t ->
    unit ->
    (t, Error.t) result
  (** [v] validates connection capacities, timeouts, and reconnect policy.
      [max_reconnect_attempts] counts full candidate passes after a transport
      loss; [None] permits unlimited attempts. The default is [Some 3]. The
      [reconnect_buffer_size] option bounds bytes accepted from new Core
      publishes while reconnecting; its default is 8 MiB and [-1] disables the
      bound. A publish accepted into this buffer is written after [CONNECT] and
      subscription replay succeeds, and can be lost or duplicated at a transport
      failure. A value of [0] selects the default, matching the Go client. The
      first redial is immediate; later attempts wait [reconnect_delay] (default
      one second) and double up to [reconnect_max_delay] (default 30 seconds).
      [reconnect_jitter] adds a bounded random offset to delayed reconnect
      waits; it defaults to zero. [random] supplies the state used for that
      sampling and defaults to a fresh self-initialized state. [tls] supplies
      the client TLS configuration used when the server's initial [INFO]
      requires TLS. Set [tls_required] to force the same upgrade when the server
      does not advertise it. [auth] derives fresh CONNECT credentials from each
      server [INFO], so nonce signers are called again after reconnect. The
      caller must install a [Mirage_crypto_rng] generator before connecting with
      TLS. *)

  val default : t
end

module Event_stream : sig
  type t

  val next : t -> (Event.t, Error.t) result
  (** [next events] returns the next lifecycle or Core event. During reconnect,
      stale Core events from the failed transport are suppressed; the
      replacement [INFO] and [Connected] sequence is emitted with the
      [Reconnected] event. *)
end

module Subscription : sig
  type t
  type delivery = { message : Nats.Message.t; status : Nats.Op.status option }
  type recovery = Detached of int | Attached of int
  type next = Delivery of delivery | Recovery

  val recovery : t -> recovery
  (** [recovery subscription] reports whether a replayable subscription is
      detached from the current transport or attached to it. The initial state
      is [Attached 0]; each successful reconnect replay advances the generation.
  *)

  val await_recovery :
    ?timeout:Mtime.Span.t -> from:recovery -> t -> (recovery, Error.t) result
  (** [await_recovery ?timeout ~from subscription] waits until the
      subscription's recovery state differs from [from], or until it reaches a
      terminal error. A timeout leaves the subscription active. *)

  val next_or_recovery : t -> (next, Error.t) result
  (** [next_or_recovery subscription] waits for the next delivery, recovery
      detachment, or terminal subscription error. Unlike [next], it exposes a
      recovery wakeup to consumers that must restore protocol-level state. *)

  val next_or_recovery_nonblocking : t -> (next, Error.t) result option
  (** [next_or_recovery_nonblocking subscription] consumes one queued delivery,
      recovery wakeup, or terminal error without waiting. *)

  val next_or_recovery_with_timeout :
    timeout:Mtime.Span.t -> t -> (next, Error.t) result
  (** [next_or_recovery_with_timeout ~timeout subscription] waits for one
      delivery, recovery wakeup, or terminal error for at most [timeout]. *)

  val sid : t -> int

  val next : t -> (delivery, Error.t) result
  (** [next] waits until a delivery or a terminal subscription error is
      available. *)

  val next_nonblocking : t -> (delivery, Error.t) result option
  (** [next_nonblocking subscription] consumes one queued delivery without
      waiting. It returns [None] when the queue is empty and
      [Some (Error error)] for a queued terminal subscription error. *)

  val next_with_timeout :
    timeout:Mtime.Span.t -> t -> (delivery, Error.t) result
  (** [next_with_timeout ~timeout subscription] waits at most [timeout] for a
      delivery. A non-positive timeout is rejected with
      [Invalid_timeout "subscription"]. A timeout leaves the subscription active
      and returns [Error.Timeout]; closure, disconnection, and cancellation
      retain the same behavior as [next]. *)

  val iter : t -> f:(delivery -> unit) -> (unit, Error.t) result
  val unsubscribe : t -> (unit, Error.t) result
  val auto_unsubscribe : t -> max_messages:int -> (unit, Error.t) result

  val drain : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
  (** [drain subscription] keeps the subscription drain pending across a
      transient reconnect and reissues its server barrier after the replacement
      subscription is installed. A timeout still completes the local drain with
      [Timeout]. *)
end

type t
type error = Error.t

val now : t -> Mtime.t
(** [now connection] reads the monotonic clock used by the connection for
    protocol deadlines. Use it when calculating deadlines for operations that
    combine several connection primitives. *)

val await_reconnect : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
(** [await_reconnect ?timeout connection] waits for an in-progress transport
    recovery to complete. It returns immediately when [connection] is usable; it
    does not initiate a reconnect or replay any caller operation. *)

val fresh_inbox : t -> Nats.Subject.t
(** [fresh_inbox connection] allocates a fresh reply subject under the
    connection's configured inbox prefix. *)

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.Mono.t ->
  ?config:Config.t ->
  Nats.Endpoint.t list ->
  (t, Error.t) result
(** [connect endpoints] resolves and tries the configured endpoint list in
    order. A successful endpoint is preferred on later reconnect passes; DNS is
    resolved again for every pass. Each [INFO] replaces the discovered candidate
    set while retaining configured seeds; malformed advertisements are ignored.
    The list must be non-empty. A [tls] endpoint requires [Config.tls] and
    performs TLS before the NATS handshake. A successful result means that the
    initial CONNECT has been written and the event stream is active; server
    authorization failures are reported asynchronously as Core server-error
    events. *)

val publish_msg : t -> Nats.Message.t -> (unit, Error.t) result
(** [publish_msg connection message] accepts a new publish during reconnect
    while the bounded reconnect buffer has room. Accepted bytes are flushed
    after the replacement handshake and subscription replay. The call does not
    mean that the server received the message; a full buffer returns
    [Reconnect_buffer_exceeded]. *)

val publish :
  t ->
  ?reply_to:Nats.Subject.t ->
  ?headers:Nats.Header.t ->
  Nats.Subject.t ->
  string ->
  (unit, Error.t) result
(** [publish connection ...] has the same reconnect-buffer contract as
    {!publish_msg}. *)

val subscribe :
  t ->
  ?queue_group:Nats.Queue_group.t ->
  ?replay_on_reconnect:bool ->
  ?pending_messages:int ->
  ?pending_bytes:int ->
  Nats.Subject.Filter.t ->
  (Subscription.t, Error.t) result
(** [subscribe ~replay_on_reconnect:false filter] creates an ephemeral
    subscription, such as a pull-reply inbox, that is terminated with
    [Disconnected] rather than restored after a transport loss. The default is
    [true], preserving ordinary subscription replay. A blocked subscription read
    receives [Disconnected] during a transient transport loss; an in-flight
    drain remains pending and re-establishes its server barrier after reconnect.
    Deliveries already queued or accepted by the server before the drain barrier
    remain available before the terminal marker. [pending_messages] and
    [pending_bytes] optionally constrain queued deliveries; each value must be
    positive or [-1], where [-1] disables that endpoint-specific limit. A
    connection's own bounded queue capacity remains in force. A subscription
    requested while reconnecting is registered immediately in the local replay
    state; its [SUB] is emitted as part of the replacement handshake.
    Unsubscribe and auto-unsubscribe update that local intent without writing a
    separate wire command until a live connection is available. Subscription
    setup is cancellation-safe: if cancellation arrives after the server
    subscription is created but before setup returns, the subscription is
    revoked before cancellation is re-raised. *)

module Request : sig
  type t

  val await : t -> (Nats.Message.t, Error.t) result
  (** [await request] waits for the request reply or its terminal connection
      error. Cancelling the fiber waiting on [await] does not cancel the
      request; use {!cancel} when that is required. *)

  val cancel : t -> (unit, Error.t) result
  (** [cancel request] cancels the request subscription. It is idempotent. *)
end

val request_async :
  ?timeout:Mtime.Span.t -> t -> Nats.Message.t -> (Request.t, Error.t) result
(** [request_async ?timeout connection message] starts a request and returns
    once its private reply subscription is installed. The request remains owned
    by [connection]'s switch until it replies, times out, is cancelled, or the
    connection terminates. A request started during reconnect is represented in
    the replay state and its publish uses the bounded reconnect buffer. A
    request already in flight is not published again after a transport loss; it
    remains pending until its reply, timeout, cancellation, or final connection
    termination. *)

val request :
  ?timeout:Mtime.Span.t ->
  ?headers:Nats.Header.t ->
  t ->
  Nats.Subject.t ->
  string ->
  (Nats.Message.t, Error.t) result

val request_msg :
  ?timeout:Mtime.Span.t ->
  t ->
  Nats.Message.t ->
  (Nats.Message.t, Error.t) result

val request_msg_retry :
  ?timeout:Mtime.Span.t ->
  retry_wait:Mtime.Span.t ->
  retry_attempts:int option ->
  t ->
  Nats.Message.t ->
  (Nats.Message.t, Error.t) result
(** [request_msg_retry ?timeout ~retry_wait ~retry_attempts connection message]
    retries only [No_responders] failures. [retry_attempts] counts retries after
    the initial request; [None] retries without a limit. The wait uses the
    connection's monotonic clock and remains cancellation-safe. A negative
    [retry_attempts] is rejected with [Invalid_retry_attempts]. *)

val flush : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
(** [flush connection] during reconnect queues the protocol [PING] behind the
    replacement handshake and waits for its [PONG]. A flush that was already
    waiting when the transport failed receives [Disconnected]. *)

val drain : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
(** [drain connection] closes the connection and returns
    [Connection_reconnecting] when called during transport recovery, matching
    the Go client's reconnecting-connection behavior. *)

val close : t -> (unit, Error.t) result
val events : t -> Event_stream.t
