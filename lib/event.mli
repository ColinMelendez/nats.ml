(** Events emitted by the pure client state machine. *)

type notice = Ok | Pong

type t =
  | Info of Info.t
  | Connected
  | Lame_duck_mode
  | Server_error of { message : string }
  | Protocol_notice of notice
  | Flush_completed
  | Draining
  | Closed

val pp_notice : Format.formatter -> notice -> unit
val pp : Format.formatter -> t -> unit
