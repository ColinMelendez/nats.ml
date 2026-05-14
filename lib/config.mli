(** Client-side settings that do not depend on a transport. *)

type error =
  | Empty_name
  | No_responders_without_headers
  | Invalid_ping_interval
  | Invalid_max_pings_without_pong

val pp_error : Format.formatter -> error -> unit

type t
(** The immutable settings used to construct a {!Client.t}. *)

val v :
  ?name:string ->
  ?headers:bool ->
  ?no_echo:bool ->
  ?no_responders:bool ->
  ?ping_interval:Mtime.Span.t option ->
  ?max_pings_without_pong:int ->
  unit ->
  (t, error) result

val default : t
val name : t -> string option
val headers : t -> bool
val no_echo : t -> bool
val no_responders : t -> bool
val ping_interval : t -> Mtime.Span.t option
val max_pings_without_pong : t -> int
val language : t -> string
val version : t -> string
val protocol : t -> int
