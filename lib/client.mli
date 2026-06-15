(** The immutable, I/O-neutral Core NATS client state machine. *)

type t
type phase = Awaiting_info | Awaiting_connect | Connected | Draining | Closed

module Connect : sig
  type t

  val v :
    ?auth_token:string ->
    ?user:string ->
    ?pass:string ->
    ?jwt:string ->
    ?nkey:string ->
    ?signature:string ->
    unit ->
    t
end

type command =
  | Connect of { credentials : Connect.t; tls_required : bool }
  | Publish of Message.t
  | Subscribe of {
      subject : Subject.Filter.t;
      queue_group : Queue_group.t option;
    }
  | Unsubscribe of { sid : int }
  | Auto_unsubscribe of { sid : int; max_messages : int }
  | Drain_subscription of { sid : int }
  | Flush
  | Drain
  | Close

type subscription = {
  sid : int;
  subject : Subject.Filter.t;
  queue_group : Queue_group.t option;
  remaining : int option;
  delivered : int;
}

type delivery = {
  sid : int;
  message : Message.t;
  status : Op.status option;
  header_block : bool;
}
(** A decoded application delivery. [header_block] is [true] when the server
    sent an [HMSG] packet, including an empty [NATS/1.0] header block. *)

type transition = {
  state : t;
  output : string list;
  events : Event.t list;
  deliveries : delivery list;
  subscription_id : int option;
}

val v : Config.t -> t
val phase : t -> phase
val info : t -> Info.t option
val subscriptions : t -> subscription list

val prepare_reconnect : ?preserve_pending_pings:bool -> t -> t
(** [prepare_reconnect state] resets connection negotiation while preserving
    client-assigned subscription ids and their replay intent. By default it
    discards old protocol barriers; [preserve_pending_pings] keeps barriers
    queued during an ongoing reconnect across a failed redial attempt. Use it
    after an unexpected transport loss, before receiving the next server [INFO].
*)

val forget_subscription : t -> int -> t
(** [forget_subscription state sid] removes the local replay intent for [sid]
    without emitting wire output. It is intended for adapter-owned ephemeral
    subscriptions, such as a request inbox whose request has failed. *)

val outgoing : t -> command -> (transition, Error.t) result
(** [outgoing] records subscription intent and flush barriers while the core is
    negotiating a connection. Their wire operations are emitted by the later
    [Connect] transition; publishes still require the adapter's reconnect buffer
    because their payload bytes are not replayed from core state. *)

val incoming :
  ?eod:bool ->
  t ->
  now:Mtime.t ->
  Bytesrw.Bytes.Reader.t ->
  (transition, Error.t) result
(** [incoming] consumes at most one complete operation.

    [Packet.Need_more] is returned as an empty successful transition without
    advancing the reader. The caller must retain that reader and call [incoming]
    again after appending more bytes. Once a packet has been consumed, a framing
    or codec error poisons the stream and is returned as [Error]. *)

val timer : t -> now:Mtime.t -> transition
val next_timeout : t -> Mtime.t option
