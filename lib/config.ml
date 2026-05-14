type error =
  | Empty_name
  | No_responders_without_headers
  | Invalid_ping_interval
  | Invalid_max_pings_without_pong

let pp_error ppf = function
  | Empty_name -> Format.pp_print_string ppf "client name is empty"
  | No_responders_without_headers ->
      Format.pp_print_string ppf "no-responders requires header support"
  | Invalid_ping_interval ->
      Format.pp_print_string ppf "ping interval must be positive"
  | Invalid_max_pings_without_pong ->
      Format.pp_print_string ppf "maximum pings without a PONG must be positive"

type t = {
  name : string option;
  headers : bool;
  no_echo : bool;
  no_responders : bool;
  ping_interval : Mtime.Span.t option;
  max_pings_without_pong : int;
}

let default_ping_interval = Mtime.Span.(30 * s)

let default =
  {
    name = None;
    headers = true;
    no_echo = false;
    no_responders = true;
    ping_interval = Some default_ping_interval;
    max_pings_without_pong = 2;
  }

let v ?name ?(headers = true) ?(no_echo = false) ?(no_responders = true)
    ?(ping_interval = Some default_ping_interval) ?(max_pings_without_pong = 2)
    () =
  match name with
  | Some value when String.equal value "" -> Error Empty_name
  | _ -> (
      match ping_interval with
      | Some value when Mtime.Span.compare value Mtime.Span.zero <= 0 ->
          Error Invalid_ping_interval
      | _ when max_pings_without_pong <= 0 ->
          Error Invalid_max_pings_without_pong
      | _ when no_responders && not headers ->
          Error No_responders_without_headers
      | _ ->
          Ok
            {
              name;
              headers;
              no_echo;
              no_responders;
              ping_interval;
              max_pings_without_pong;
            })

let name value = value.name
let headers value = value.headers
let no_echo value = value.no_echo
let no_responders value = value.no_responders
let ping_interval value = value.ping_interval
let max_pings_without_pong value = value.max_pings_without_pong
let language _ = "ocaml"
let version _ = "0.1.0"
let protocol _ = 1
