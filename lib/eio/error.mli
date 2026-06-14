(** Errors reported by the Eio connection facade. *)

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
  | Invalid_reconnect_buffer_size of int
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
  | Connection_reconnecting
  | Timeout
  | No_responders
  | Reconnect_buffer_exceeded of { limit : int }
  | Io of exn
  | Slow_consumer of slow_consumer
  | Disconnected
  | Draining
  | Closed

val pp_slow_consumer : Format.formatter -> slow_consumer -> unit
val pp : Format.formatter -> t -> unit
