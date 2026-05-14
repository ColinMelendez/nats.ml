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

let pp ppf = function
  | Invalid_config error ->
      Format.fprintf ppf "invalid config: %a" Config.pp_error error
  | Invalid_info error ->
      Format.fprintf ppf "invalid INFO: %a" Info.pp_error error
  | Packet error -> Format.fprintf ppf "packet: %a" Packet.pp_error error
  | Codec error -> Format.fprintf ppf "codec: %a" Codec.pp_error error
  | Unexpected_operation operation ->
      Format.fprintf ppf "unexpected operation: %a" Op.pp operation
  | Info_not_received -> Format.pp_print_string ppf "server INFO not received"
  | Already_connected ->
      Format.pp_print_string ppf "client is already connected"
  | Not_connected -> Format.pp_print_string ppf "client is not connected"
  | Draining -> Format.pp_print_string ppf "client is draining"
  | Closed -> Format.pp_print_string ppf "client is closed"
  | Headers_not_negotiated ->
      Format.pp_print_string ppf "server headers were not negotiated"
  | Unknown_subscription { sid } ->
      Format.fprintf ppf "unknown subscription id %d" sid
  | Invalid_subscription_id sid ->
      Format.fprintf ppf "invalid subscription id %d" sid
  | Invalid_max_messages count ->
      Format.fprintf ppf "invalid maximum message count %d" count
  | Max_payload_exceeded { size; limit } ->
      Format.fprintf ppf "payload size %d exceeds negotiated limit %d" size
        limit
