type error =
  | Invalid_json of Jsont.Error.t
  | Invalid_max_payload of int
  | Invalid_protocol of int

let pp_error ppf = function
  | Invalid_json error ->
      Format.fprintf ppf "invalid INFO JSON: %a" Jsont.Error.pp error
  | Invalid_max_payload value ->
      Format.fprintf ppf "invalid INFO max_payload %d" value
  | Invalid_protocol value ->
      Format.fprintf ppf "invalid INFO protocol %d" value

type t = {
  server_id : string option;
  server_name : string option;
  version : string option;
  proto : int option;
  max_payload : int;
  headers : bool;
  no_responders : bool;
  auth_required : bool;
  tls_required : bool;
  nonce : string option;
  lame_duck_mode : bool;
  connect_urls : string list;
}

type json = {
  server_id : string option;
  server_name : string option;
  version : string option;
  proto : int option;
  max_payload : int;
  headers : bool option;
  no_responders : bool option;
  auth_required : bool option;
  tls_required : bool option;
  nonce : string option;
  lame_duck_mode : bool option;
  connect_urls : string list option;
}

let json_codec =
  Jsont.Object.map ~kind:"NATS INFO"
    (fun
      server_id
      server_name
      version
      proto
      max_payload
      headers
      no_responders
      auth_required
      tls_required
      nonce
      lame_duck_mode
      connect_urls
    ->
      {
        server_id;
        server_name;
        version;
        proto;
        max_payload;
        headers;
        no_responders;
        auth_required;
        tls_required;
        nonce;
        lame_duck_mode;
        connect_urls;
      })
  |> Jsont.Object.opt_mem "server_id" Jsont.string
  |> Jsont.Object.opt_mem "server_name" Jsont.string
  |> Jsont.Object.opt_mem "version" Jsont.string
  |> Jsont.Object.opt_mem "proto" Jsont.int
  |> Jsont.Object.mem "max_payload" Jsont.int
  |> Jsont.Object.opt_mem "headers" Jsont.bool
  |> Jsont.Object.opt_mem "no_responders" Jsont.bool
  |> Jsont.Object.opt_mem "auth_required" Jsont.bool
  |> Jsont.Object.opt_mem "tls_required" Jsont.bool
  |> Jsont.Object.opt_mem "nonce" Jsont.string
  |> Jsont.Object.opt_mem "ldm" Jsont.bool
  |> Jsont.Object.opt_mem "connect_urls" (Jsont.list Jsont.string)
  |> Jsont.Object.skip_unknown |> Jsont.Object.finish

let of_string value =
  match Jsont_bytesrw.decode_string' json_codec value with
  | Error error -> Error (Invalid_json error)
  | Ok
      {
        server_id;
        server_name;
        version;
        proto;
        max_payload;
        headers;
        no_responders;
        auth_required;
        tls_required;
        nonce;
        lame_duck_mode;
        connect_urls;
      } -> (
      if max_payload <= 0 then Error (Invalid_max_payload max_payload)
      else
        match proto with
        | Some value when value < 0 -> Error (Invalid_protocol value)
        | _ ->
            Ok
              ({
                 server_id;
                 server_name;
                 version;
                 proto;
                 max_payload;
                 headers = Option.value ~default:false headers;
                 no_responders = Option.value ~default:false no_responders;
                 auth_required = Option.value ~default:false auth_required;
                 tls_required = Option.value ~default:false tls_required;
                 nonce;
                 lame_duck_mode = Option.value ~default:false lame_duck_mode;
                 connect_urls = Option.value ~default:[] connect_urls;
               }
                : t))

let server_id (value : t) = value.server_id
let server_name (value : t) = value.server_name
let version (value : t) = value.version
let proto (value : t) = value.proto
let max_payload (value : t) = value.max_payload
let headers (value : t) = value.headers
let no_responders (value : t) = value.no_responders
let auth_required (value : t) = value.auth_required
let tls_required (value : t) = value.tls_required
let nonce (value : t) = value.nonce
let lame_duck_mode (value : t) = value.lame_duck_mode
let connect_urls (value : t) = value.connect_urls

let pp_option pp_value ppf = function
  | None -> Format.pp_print_string ppf "-"
  | Some value -> pp_value ppf value

let pp ppf (value : t) =
  Format.fprintf ppf "INFO(max_payload=%d, headers=%b, no_responders=%b, "
    value.max_payload value.headers value.no_responders;
  Format.fprintf ppf "server_id=%a, version=%a, connect_urls=%d)"
    (pp_option Format.pp_print_string)
    value.server_id
    (pp_option Format.pp_print_string)
    value.version
    (List.length value.connect_urls)
