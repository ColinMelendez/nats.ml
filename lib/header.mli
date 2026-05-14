(** Immutable, multi-valued NATS message headers. *)

type t
(** Headers preserve insertion order, repeated values, and the original spelling
    of each name. Lookup is case-insensitive. *)

type error =
  | Empty_name
  | Invalid_name_character of { position : int; character : char }
  | Invalid_value_character of { position : int; character : char }

val pp_error : Format.formatter -> error -> unit
val empty : t
val is_empty : t -> bool

val of_list : (string * string) list -> (t, error) result
(** [of_list entries] validates and preserves [entries] in wire order. *)

val add : name:string -> value:string -> t -> (t, error) result
(** [add ~name ~value headers] adds one value after the existing entries. *)

val to_list : t -> (string * string) list
val mem : string -> t -> bool

val find : string -> t -> string option
(** [find name headers] returns the first value for [name] in wire order. *)

val find_all : string -> t -> string list
(** [find_all name headers] returns all values for [name] in wire order. *)

val pp : Format.formatter -> t -> unit
val equal : t -> t -> bool
