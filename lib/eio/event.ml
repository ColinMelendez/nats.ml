type t =
  | Core of Nats.Event.t
  | Disconnected
  | Reconnected
  | Slow_consumer of Error.slow_consumer

let pp ppf = function
  | Core event -> Format.fprintf ppf "core: %a" Nats.Event.pp event
  | Disconnected -> Format.pp_print_string ppf "disconnected"
  | Reconnected -> Format.pp_print_string ppf "reconnected"
  | Slow_consumer kind ->
      Format.fprintf ppf "slow consumer: %a" Error.pp_slow_consumer kind
