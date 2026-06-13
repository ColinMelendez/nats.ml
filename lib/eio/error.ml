type slow_consumer = Events | Subscription of { sid : int }

type t =
  | Protocol of Nats.Error.t
  | Auth of Nats.Auth.error
  | Invalid_endpoints
  | Invalid_capacity of { name : string; value : int }
  | Invalid_pending_limit of { name : string; value : int }
  | Command_queue_full of { capacity : int }
  | Invalid_chunk_size of int
  | Invalid_inbox_prefix of Nats.Subject.error
  | Invalid_reconnect_attempts of int
  | Invalid_retry_attempts of int
  | Invalid_reconnect_delay of {
      initial : Mtime.Span.t;
      maximum : Mtime.Span.t;
    }
  | Invalid_reconnect_jitter of Mtime.Span.t
  | Invalid_timeout of string
  | Tls_required
  | Tls_unexpected_input
  | Tls of exn
  | Timeout
  | No_responders
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
  | Auth error ->
      Format.fprintf ppf "authentication: %a" Nats.Auth.pp_error error
  | Invalid_endpoints ->
      Format.pp_print_string ppf "at least one endpoint is required"
  | Invalid_capacity { name; value } ->
      Format.fprintf ppf "invalid %s capacity %d" name value
  | Invalid_pending_limit { name; value } ->
      Format.fprintf ppf "invalid %s pending limit %d" name value
  | Command_queue_full { capacity } ->
      Format.fprintf ppf "command queue is full (capacity %d)" capacity
  | Invalid_chunk_size size ->
      Format.fprintf ppf "invalid read chunk size %d" size
  | Invalid_inbox_prefix error ->
      Format.fprintf ppf "invalid inbox prefix: %a" Nats.Subject.pp_error error
  | Invalid_reconnect_attempts value ->
      Format.fprintf ppf "invalid reconnect attempt limit %d" value
  | Invalid_retry_attempts value ->
      Format.fprintf ppf "invalid request retry attempt limit %d" value
  | Invalid_reconnect_delay { initial; maximum } ->
      Format.fprintf ppf "reconnect delay %a exceeds maximum %a" Mtime.Span.pp
        initial Mtime.Span.pp maximum
  | Invalid_reconnect_jitter value ->
      Format.fprintf ppf "invalid reconnect jitter %a" Mtime.Span.pp value
  | Invalid_timeout name -> Format.fprintf ppf "invalid %s timeout" name
  | Tls_required ->
      Format.pp_print_string ppf
        "TLS is required but no TLS configuration was supplied"
  | Tls_unexpected_input ->
      Format.pp_print_string ppf "unexpected plaintext input before TLS"
  | Tls error -> Format.fprintf ppf "TLS error: %s" (Printexc.to_string error)
  | Timeout -> Format.pp_print_string ppf "operation timed out"
  | No_responders -> Format.pp_print_string ppf "no responders"
  | Io error -> Format.fprintf ppf "I/O error: %s" (Printexc.to_string error)
  | Slow_consumer kind ->
      Format.fprintf ppf "slow consumer: %a" pp_slow_consumer kind
  | Disconnected -> Format.pp_print_string ppf "connection disconnected"
  | Draining -> Format.pp_print_string ppf "connection is draining"
  | Closed -> Format.pp_print_string ppf "connection is closed"
