(** Validated Core NATS server endpoints. *)

type scheme =
  | Nats
  | Tls
      (** The transport scheme named by an endpoint URL. [Tls] is explicit
          endpoint metadata; the Eio adapter decides how it composes with TLS
          negotiation. *)

type error =
  | Empty
  | Missing_scheme
  | Unsupported_scheme of string
  | Invalid_authority
  | Missing_host
  | Invalid_host of string
  | Unbracketed_ipv6
  | Userinfo_not_supported
  | Invalid_port of string
  | Port_out_of_range of string
  | Invalid_suffix

val pp_error : Format.formatter -> error -> unit

type t
(** An endpoint with a [nats] or [tls] scheme, a non-empty host, and a TCP port
    in the range 1--65535. Endpoint values contain no DNS result or transport
    resource. *)

val of_string : string -> (t, error) result
(** [of_string value] parses a Core NATS endpoint URL. The accepted grammar is
    [nats://host[:port]] or [tls://host[:port]], with port [4222] when omitted.
    IPv6 hosts must use brackets. Userinfo, paths, queries, fragments, and
    WebSocket schemes are rejected. Scheme and host casing are canonicalized. *)

val of_connect_url : ?default_scheme:scheme -> string -> (t, error) result
(** [of_connect_url ?default_scheme value] parses a configured endpoint URL or a
    server advertisement in [host:port] form. Bare advertisements use
    [default_scheme] (the [nats] scheme by default) and default to port [4222]
    when no port is present. An explicit URL scheme always takes precedence. *)

val scheme : t -> scheme
val host : t -> string
val port : t -> int
val to_string : t -> string
val pp : Format.formatter -> t -> unit
val equal : t -> t -> bool
val compare : t -> t -> int

module Pool : sig
  type endpoint = t
  type t

  val v : endpoint list -> t
  (** [v seeds] creates a pool in configured-seed order. Duplicate endpoints are
      removed while preserving first occurrence. *)

  val seeds : t -> endpoint list
  val discovered : t -> endpoint list

  val candidates : t -> endpoint list
  (** [candidates pool] is the current deterministic dial order. *)

  val preferred : t -> endpoint option
  (** [preferred pool] is the most recently connected endpoint, when known. *)

  val update_discovered : t -> endpoint list -> t
  (** [update_discovered pool endpoints] replaces the non-empty advertised set.
      Configured seeds are never reported as discovered. An empty advertisement
      leaves the current set unchanged. The currently preferred discovered
      endpoint remains available when absent from an advertisement until
      another endpoint becomes current and processes a later advertisement. *)

  val connected : t -> endpoint -> t
  (** [connected pool endpoint] makes [endpoint] the first candidate on the next
      dial pass. *)

  val failed : t -> endpoint -> t
  (** [failed pool endpoint] moves a configured or current discovered endpoint
      to the end of the dial order. A preferred endpoint that is no longer in
      either set is removed after it fails. *)
end
