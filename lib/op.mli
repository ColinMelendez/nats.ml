(** The phase-blind abstract syntax of Core NATS wire operations. *)

type status = { code : int; description : string }
(** The optional status line carried by a NATS header block. *)

type t =
  | Info of string
  | Connect of string
  | Pub of Message.t
  | Hpub of { message : Message.t; status : status option }
  | Sub of {
      subject : Subject.Filter.t;
      queue_group : Queue_group.t option;
      sid : int;
    }
  | Unsub of { sid : int; max_messages : int option }
  | Msg of { sid : int; message : Message.t }
  | Hmsg of { sid : int; message : Message.t; status : status option }
  | Ping
  | Pong
  | Ok
  | Server_error of string

val pp : Format.formatter -> t -> unit
