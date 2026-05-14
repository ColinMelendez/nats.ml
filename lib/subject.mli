(** Valid NATS publish subjects and subscription filters. *)

type error =
  | Empty
  | Empty_token of { position : int }
  | Invalid_character of { position : int; character : char }
  | Wildcard_not_allowed of { position : int; token : string }
  | Wildcard_must_be_token of { position : int }
  | Wildcard_not_terminal of { position : int }
      (** Errors produced while validating a subject or filter. *)

val pp_error : Format.formatter -> error -> unit
(** [pp_error ppf error] formats [error]. *)

type t
(** An ordinary subject used for publishing or replying.

    The value is non-empty, has no empty dot-separated tokens, and contains no
    wildcard tokens. *)

val of_string : string -> (t, error) result
(** [of_string s] validates an ordinary publish or reply subject. *)

val literal : string -> t
(** [literal s] is the validated subject [s].

    This is for fixed, programmer-written names. It raises [Invalid_argument]
    when the literal is invalid. External input should use {!of_string}. *)

val to_string : t -> string
val pp : Format.formatter -> t -> unit
val equal : t -> t -> bool

module Filter : sig
  type nonrec error = error

  type t
  (** A subscription filter. It may contain [*] for one token or [>] for the
      remaining tail; [>] is valid only as the final token. *)

  val of_string : string -> (t, error) result
  val literal : string -> t
  val to_string : t -> string
  val pp : Format.formatter -> t -> unit
  val equal : t -> t -> bool
end
