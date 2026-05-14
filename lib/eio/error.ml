type slow_consumer = Events | Subscription of { sid : int }

type t =
  | Protocol of Nats.Error.t
  | Invalid_capacity of { name : string; value : int }
  | Command_queue_full of { capacity : int }
  | Invalid_chunk_size of int
  | Invalid_timeout of string
  | Timeout
  | Io of exn
  | Slow_consumer of slow_consumer
  | Disconnected
  | Draining
  | Closed

let pp_slow_consumer ppf = function
  | Events -> Format.pp_print_string ppf "event stream"
  | Subscription { sid } -> Format.fprintf ppf "subscription %d" sid

let pp ppf = function
  | Protocol error -> Format.fprintf ppf "protocol: %a" Nats.Error.pp error
  | Invalid_capacity { name; value } ->
      Format.fprintf ppf "invalid %s capacity %d" name value
  | Command_queue_full { capacity } ->
      Format.fprintf ppf "command queue is full (capacity %d)" capacity
  | Invalid_chunk_size size ->
      Format.fprintf ppf "invalid read chunk size %d" size
  | Invalid_timeout name -> Format.fprintf ppf "invalid %s timeout" name
  | Timeout -> Format.pp_print_string ppf "operation timed out"
  | Io error -> Format.fprintf ppf "I/O error: %s" (Printexc.to_string error)
  | Slow_consumer kind ->
      Format.fprintf ppf "slow consumer: %a" pp_slow_consumer kind
  | Disconnected -> Format.pp_print_string ppf "connection disconnected"
  | Draining -> Format.pp_print_string ppf "connection is draining"
  | Closed -> Format.pp_print_string ppf "connection is closed"
