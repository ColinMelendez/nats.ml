(** Errors returned by the pure client state machine. *)

type t =
  | Invalid_config of Config.error
  | Invalid_info of Info.error
  | Packet of Packet.error
  | Codec of Codec.error
  | Unexpected_operation of Op.t
  | Info_not_received
  | Already_connected
  | Not_connected
  | Draining
  | Closed
  | Headers_not_negotiated
  | Unknown_subscription of { sid : int }
  | Invalid_subscription_id of int
  | Invalid_max_messages of int
  | Max_payload_exceeded of { size : int; limit : int }

val pp : Format.formatter -> t -> unit
