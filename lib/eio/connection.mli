(** A direct-style Core NATS connection running under Eio. *)

module Config : sig
  type t

  val v :
    ?core:Nats.Config.t ->
    ?credentials:Nats.Client.Connect.t ->
    ?command_capacity:int ->
    ?subscription_capacity:int ->
    ?event_capacity:int ->
    ?read_capacity:int ->
    ?read_chunk_size:int ->
    ?flush_timeout:Mtime.Span.t ->
    ?drain_timeout:Mtime.Span.t ->
    unit ->
    (t, Error.t) result

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
  val unsubscribe : t -> (unit, Error.t) result
end

type t

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.Mono.t ->
  ?config:Config.t ->
  Eio.Net.Sockaddr.stream ->
  (t, Error.t) result

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

val flush : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
val drain : ?timeout:Mtime.Span.t -> t -> (unit, Error.t) result
val close : t -> (unit, Error.t) result
val events : t -> Event_stream.t
