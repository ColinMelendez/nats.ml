(** Encoding and decoding of the phase-blind Core NATS operations. *)

type error =
  | Packet of Packet.error
  | Invalid_operation of { keyword : string }
  | Invalid_field_count of { keyword : string }
  | Invalid_subject of Subject.error
  | Invalid_header of Header.error
  | Invalid_header_block
  | Invalid_status
  | Invalid_sid of string
  | Invalid_max_messages of string
  | Invalid_value of { keyword : string }
  | Message_has_headers
  | Invalid_limits

val pp_error : Format.formatter -> error -> unit
val decode : Packet.t -> (Op.t, error) result
val encode : ?limits:Packet.limits -> Op.t -> (string, error) result

val read :
  ?eod:bool ->
  ?limits:Packet.limits ->
  Bytesrw.Bytes.Reader.t ->
  (Op.t, error) result

val write :
  ?limits:Packet.limits ->
  Bytesrw.Bytes.Writer.t ->
  Op.t ->
  (unit, error) result
