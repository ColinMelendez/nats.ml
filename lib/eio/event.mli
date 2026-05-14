(** Lifecycle and protocol observations from an Eio connection. *)

type t =
  | Core of Nats.Event.t
  | Disconnected
  | Slow_consumer of Error.slow_consumer

val pp : Format.formatter -> t -> unit
