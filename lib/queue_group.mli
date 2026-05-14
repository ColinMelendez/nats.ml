(** Queue-group names used by subscriptions. *)

type t
(** A non-empty queue-group name with ordinary subject-token validation and no
    wildcard tokens. *)

type error = Subject.error

val of_string : string -> (t, error) result
val literal : string -> t
val to_string : t -> string
val pp : Format.formatter -> t -> unit
val equal : t -> t -> bool
