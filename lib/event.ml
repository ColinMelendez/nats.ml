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

let pp_notice ppf = function
  | Ok -> Format.pp_print_string ppf "+OK"
  | Pong -> Format.pp_print_string ppf "PONG"

let pp ppf = function
  | Info info -> Format.fprintf ppf "info: %a" Info.pp info
  | Connected -> Format.pp_print_string ppf "connected"
  | Lame_duck_mode -> Format.pp_print_string ppf "lame duck mode"
  | Server_error { message } -> Format.fprintf ppf "server error: %s" message
  | Protocol_notice notice ->
      Format.fprintf ppf "protocol notice: %a" pp_notice notice
  | Flush_completed -> Format.pp_print_string ppf "flush completed"
  | Draining -> Format.pp_print_string ppf "draining"
  | Closed -> Format.pp_print_string ppf "closed"
