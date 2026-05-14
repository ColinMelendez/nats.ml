(** Immutable application messages carried by Core NATS. *)

type t
(** A message has an ordinary subject, an optional reply subject, immutable
    payload bytes represented as a string, and immutable headers. *)

val v :
  subject:Subject.t -> ?reply_to:Subject.t -> ?headers:Header.t -> string -> t
(** [v ~subject ?reply_to ?headers payload] constructs a message. *)

val subject : t -> Subject.t
val reply_to : t -> Subject.t option
val headers : t -> Header.t
val payload : t -> string
val with_payload : string -> t -> t
val with_headers : Header.t -> t -> t
val pp : Format.formatter -> t -> unit
val equal : t -> t -> bool
