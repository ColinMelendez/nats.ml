(** A modern, layered NATS client SDK for OCaml. *)

module Subject = Subject
(** Valid publish subjects and subscription filters. *)

module Queue_group = Queue_group
(** Valid queue group names. *)

module Endpoint = Endpoint
(** Validated Core NATS server endpoints. *)

module Header = Header
(** Immutable, multi-valued message headers. *)

module Message = Message
(** Immutable application messages. *)

module Op = Op
(** The phase-blind Core NATS wire-operation AST. *)

module Packet = Packet
(** Incremental CRLF and payload framing. *)

module Codec = Codec
(** Encoding and decoding of Core NATS wire operations. *)

module Config = Config
(** Transport-independent client settings. *)

module Info = Info
(** Typed server information received in [INFO]. *)

module Auth = Auth
(** Reusable authentication capabilities for Core NATS handshakes. *)

module Error = Error
(** Structured errors from the client state machine. *)

module Event = Event
(** Events emitted by the client state machine. *)

module Client = Client
(** The immutable, I/O-neutral Core NATS state machine. *)
