(** A modern, layered NATS client SDK for OCaml. *)

module Subject = Subject
(** Valid publish subjects and subscription filters. *)

module Queue_group = Queue_group
(** Valid queue group names. *)

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
