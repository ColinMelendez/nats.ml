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
  | Flush
  | Drain
  | Close

type subscription = {
  sid : int;
  subject : Subject.Filter.t;
  queue_group : Queue_group.t option;
  remaining : int option;
}

type delivery = { sid : int; message : Message.t; status : Op.status option }

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

val prepare_reconnect : t -> t
(** [prepare_reconnect state] resets connection negotiation while preserving
    client-assigned subscription ids and their replay intent. Use it after an
    unexpected transport loss, before receiving the next server [INFO]. *)

val outgoing : t -> command -> (transition, Error.t) result

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
