(** Privileged NATS system-account administration under Eio.

    The system account is an operator-facing NATS service. It is separate from
    JetStream administration and normally requires explicit permissions on the
    [$SYS] request and event subjects. JSON response bodies remain available as
    [Jsont.json] values because the server adds fields across releases. *)

module Error : sig
  type api = {
    code : int;
    err_code : int option;
    description : string;
    payload : Jsont.json;
  }

  type t =
    | Connection of Nats_eio.Error.t
    | Decode of Jsont.Error.t
    | Encode of Jsont.Error.t
    | Invalid_identifier of { kind : string; value : string }
    | Invalid_client_id of int64
    | Invalid_selector_tag of string
    | Invalid_timeout
    | Invalid_option of { name : string; reason : string }
    | Invalid_target_endpoint of { target : string; endpoint : string }
    | Unexpected_response of string
    | Unexpected_status of { code : int; description : string }
    | Server of api

  val pp : Format.formatter -> t -> unit
end

type t
(** A handle for privileged system-account requests over a Core connection. *)

type system = t

val v : Nats_eio.Connection.t -> t
val connection : t -> Nats_eio.Connection.t

module Target : sig
  type t

  val all : t
  val server : string -> (t, Error.t) result
  val account : string -> (t, Error.t) result
  val pp : Format.formatter -> t -> unit
end

module Selector : sig
  type t

  val empty : t

  val v :
    ?server_name:string ->
    ?cluster:string ->
    ?host:string ->
    ?exact_match:bool ->
    ?tags:string list ->
    ?domain:string ->
    unit ->
    (t, Error.t) result

  val server_name : t -> string option
  val cluster : t -> string option
  val host : t -> string option
  val exact_match : t -> bool
  val tags : t -> string list
  val domain : t -> string option
end

module Monitor : sig
  (** Typed request fields for the server monitor services. The common server,
      cluster, host, tag, and domain filters live in {!Selector}; choose the
      constructor matching the requested endpoint for its additional fields. *)
  module Options : sig
    type t

    type sort =
      | Cid
      | Start
      | Subs
      | Pending
      | Out_msgs
      | In_msgs
      | Out_bytes
      | In_bytes
      | Last
      | Idle
      | Uptime
      | Stop
      | Reason
      | Rtt

    type state = Open | Closed | All

    val empty : t

    val connz :
      ?sort:sort ->
      ?auth:bool ->
      ?subscriptions:bool ->
      ?subscriptions_detail:bool ->
      ?offset:int ->
      ?limit:int ->
      ?cid:int64 ->
      ?mqtt_client:string ->
      ?state:state ->
      ?user:string ->
      ?account:string ->
      ?filter_subject:string ->
      unit ->
      (t, Error.t) result

    val subsz :
      ?offset:int ->
      ?limit:int ->
      ?subscriptions:bool ->
      ?account:string ->
      ?test:string ->
      unit ->
      (t, Error.t) result

    val routez :
      ?subscriptions:bool ->
      ?subscriptions_detail:bool ->
      unit ->
      (t, Error.t) result

    val gatewayz :
      ?name:string ->
      ?accounts:bool ->
      ?account_name:string ->
      ?account_subscriptions:bool ->
      ?account_subscriptions_detail:bool ->
      unit ->
      (t, Error.t) result

    val leafz :
      ?subscriptions:bool -> ?account:string -> unit -> (t, Error.t) result

    val accountz : ?account:string -> unit -> (t, Error.t) result

    val account_statz :
      ?accounts:string list ->
      ?include_unused:bool ->
      unit ->
      (t, Error.t) result

    val jsz :
      ?account:string ->
      ?accounts:bool ->
      ?streams:bool ->
      ?consumer:bool ->
      ?direct_consumer:bool ->
      ?config:bool ->
      ?leader_only:bool ->
      ?offset:int ->
      ?limit:int ->
      ?raft_groups:bool ->
      ?stream_leader_only:bool ->
      unit ->
      (t, Error.t) result

    val healthz :
      ?js_enabled:bool ->
      ?js_enabled_only:bool ->
      ?js_server_only:bool ->
      ?js_meta_only:bool ->
      ?account:string ->
      ?stream:string ->
      ?consumer:string ->
      ?details:bool ->
      unit ->
      (t, Error.t) result

    val profilez :
      ?name:string ->
      ?debug:int ->
      ?duration_ns:int64 ->
      unit ->
      (t, Error.t) result
    (** [duration_ns] is encoded as the nanosecond integer expected by the
        server's Go [time.Duration] field. *)

    val ipqueuesz : ?all:bool -> ?filter:string -> unit -> (t, Error.t) result
    val raftz : ?account:string -> ?group:string -> unit -> (t, Error.t) result
  end

  module Endpoint : sig
    type t =
      | Idz
      | Statz
      | Varz
      | Subsz
      | Connz
      | Routez
      | Gatewayz
      | Leafz
      | Accountz
      | Jsz
      | Healthz
      | Profilez
      | Expvarz
      | Ipqueuesz
      | Raftz
      | Account_info
      | Account_stats
      | Account_connections

    val to_string : t -> string
    val pp : Format.formatter -> t -> unit
  end

  type response

  val endpoint : response -> Endpoint.t
  val payload : response -> Jsont.json
  val server : response -> Jsont.json option
  val data : response -> Jsont.json option
  val error : response -> Error.api option

  val request :
    ?timeout:Mtime.Span.t ->
    ?selector:Selector.t ->
    ?options:Options.t ->
    t ->
    target:Target.t ->
    Endpoint.t ->
    (response list, Error.t) result
  (** [request system ~target endpoint] queries one server, every server, or an
      account's servers. [Target.all] and [Target.account _] collect replies
      until [timeout] expires and may therefore return an empty list. Use the
      endpoint-specific constructors in [Options] for pagination, connection,
      subscription, JetStream, health, profile, and routing filters; those
      fields are merged with the common [selector]. A server-targeted request
      returns [Error.Connection Nats_eio.Error.Timeout] when its one reply does
      not arrive. Responses preserve their full version-dependent JSON body. *)
end

module Control : sig
  val reload :
    ?timeout:Mtime.Span.t ->
    ?selector:Selector.t ->
    t ->
    server:string ->
    (unit, Error.t) result

  val kick :
    ?timeout:Mtime.Span.t ->
    ?selector:Selector.t ->
    t ->
    server:string ->
    client_id:int64 ->
    (unit, Error.t) result

  val client_lame_duck :
    ?timeout:Mtime.Span.t ->
    ?selector:Selector.t ->
    t ->
    server:string ->
    client_id:int64 ->
    (unit, Error.t) result
  (** These operations target a specific server. [client_lame_duck] invokes the
      server's [$SYS.REQ.SERVER.<server-id>.LDM] operation for one client; it
      does not put the whole server into lame-duck mode. *)
end

module Events : sig
  type scope = All | Servers | Accounts | Server of string | Account of string

  type event =
    | Server_stats of { server_id : string; payload : Jsont.json }
    | Server_shutdown of { server_id : string; payload : Jsont.json }
    | Server_lame_duck of { server_id : string; payload : Jsont.json }
    | Server_auth_error of { server_id : string; payload : Jsont.json }
    | Account_connect of { account_id : string; payload : Jsont.json }
    | Account_disconnect of { account_id : string; payload : Jsont.json }
    | Account_leafnode_connect of { account_id : string; payload : Jsont.json }
    | Account_server_connections of {
        account_id : string;
        payload : Jsont.json;
      }
    | Unknown of { subject : string; payload : Jsont.json }

  type t

  val subscribe : ?scope:scope -> system -> (t, Error.t) result
  (** [subscribe ?scope system] subscribes to the selected system-event
      namespaces. [All] covers server and account events, but excludes request
      subjects and private inbox traffic. Events with an unrecognised subject in
      those namespaces—including the account-wide authentication-error
      subject—are returned as [Unknown] with their original JSON payload. *)

  val next : t -> (event, Error.t) result
  val next_with_timeout : timeout:Mtime.Span.t -> t -> (event, Error.t) result
  val close : t -> (unit, Error.t) result
end
