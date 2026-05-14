(** Typed server information received in an [INFO] operation. *)

type error =
  | Invalid_json of Jsont.Error.t
  | Invalid_max_payload of int
  | Invalid_protocol of int

val pp_error : Format.formatter -> error -> unit

type t

val of_string : string -> (t, error) result
val server_id : t -> string option
val server_name : t -> string option
val version : t -> string option
val proto : t -> int option
val max_payload : t -> int
val headers : t -> bool
val no_responders : t -> bool
val auth_required : t -> bool
val tls_required : t -> bool
val nonce : t -> string option
val lame_duck_mode : t -> bool
val connect_urls : t -> string list
val pp : Format.formatter -> t -> unit
