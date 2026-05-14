(** Framing for the line-oriented NATS protocol. *)

type limits = {
  max_line_bytes : int;
  max_header_bytes : int;
  max_payload_bytes : int;
  max_packet_bytes : int;
}
(** Limits checked before a packet body is copied. *)

val default_limits : limits

type framing =
  | Line
  | Payload of { bytes : int }
  | Headers of { header_bytes : int; total_bytes : int }

type t
(** A complete packet without its trailing CRLF in [body]. *)

type error =
  | End_of_input
  | Need_more
  | Unexpected_end
  | Invalid_limits
  | Line_too_long of { limit : int }
  | Packet_too_large of { size : int; limit : int }
  | Headers_too_large of { size : int; limit : int }
  | Payload_too_large of { size : int; limit : int }
  | Malformed_line
  | Invalid_length of { keyword : string; value : string }
  | Invalid_lengths of { keyword : string }
  | Invalid_terminator
  | Unknown_operation of string

val pp_error : Format.formatter -> error -> unit

val read :
  ?eod:bool -> ?limits:limits -> Bytesrw.Bytes.Reader.t -> (t, error) result
(** [read reader] consumes one complete packet from [reader].

    If a packet is incomplete, [Need_more] is returned and the reader is not
    advanced. Pass [~eod:true] when the caller knows that no more bytes will
    arrive to turn that condition into [Unexpected_end] (or [End_of_input] for
    an empty reader). The caller must retain and reuse the same reader when more
    bytes arrive. A framing error also leaves the reader unadvanced, but the
    stream is then poisoned and must be closed rather than retried. *)

val line : t -> string
val body : t -> string
val framing : t -> framing
val to_string : t -> string
val pp : Format.formatter -> t -> unit
