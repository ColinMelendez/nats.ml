(** Reusable Core NATS authentication capabilities. *)

type error = Auth_required | Missing_nonce | Signing of string

val pp_error : Format.formatter -> error -> unit

type signer = nonce:string -> (string, string) result
(** A nonce signer returns the NATS signature or an opaque diagnostic. *)

type t
(** Reusable credentials for deriving a fresh {!Client.Connect.t} per server
    [INFO]. A signer, when present, remains outside {!Client.t}. *)

val none : t
(** [none] sends no authentication fields. *)

val tls : t
(** [tls] sends no authentication fields and permits an authentication-required
    server to authenticate the connection from its TLS client certificate. The
    caller must supply a client certificate through the transport TLS
    configuration. *)

val token : string -> t
(** [token value] authenticates with a bearer token. *)

val user_pass : user:string -> pass:string -> t
(** [user_pass ~user ~pass] authenticates with a username and password. *)

val nkey : nkey:string -> sign:signer -> t
(** [nkey ~nkey ~sign] authenticates with an NKey public key and nonce
    signature. *)

val jwt : jwt:string -> nkey:string -> sign:signer -> t
(** [jwt ~jwt ~nkey ~sign] authenticates with a JWT, NKey public key, and nonce
    signature. *)

val connect : t -> Info.t -> (Client.Connect.t, error) result
(** [connect auth info] derives the low-level CONNECT credentials for [info].

    A nonce signer is called for every handshake, including reconnects. An
    authentication-required server rejects {!none}; NKey and JWT credentials
    reject an [INFO] without a nonce. *)
