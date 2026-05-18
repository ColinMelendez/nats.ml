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
end

module Subscription : sig
  type t
  type delivery = { message : Nats.Message.t; status : Nats.Op.status option }

  val sid : t -> int
  val next : t -> (delivery, Error.t) result
  val iter : t -> f:(delivery -> unit) -> (unit, Error.t) result
  val unsubscribe : t -> (unit, Error.t) result
  val auto_unsubscribe : t -> max_messages:int -> (unit, Error.t) result
  val drain : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
end

type t

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
    performs TLS before the NATS handshake. *)

val publish_msg : t -> Nats.Message.t -> (unit, Error.t) result

val publish :
  t ->
  ?reply_to:Nats.Subject.t ->
  ?headers:Nats.Header.t ->
  Nats.Subject.t ->
  string ->
  (unit, Error.t) result

val subscribe :
  t ->
  ?queue_group:Nats.Queue_group.t ->
  Nats.Subject.Filter.t ->
  (Subscription.t, Error.t) result

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

val flush : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
val drain : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
val close : t -> (unit, Error.t) result
val events : t -> Event_stream.t
