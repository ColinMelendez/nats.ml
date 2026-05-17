type scheme = Nats | Tls

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

type t = { scheme : scheme; host : string; port : int }

let ( let* ) value f =
  match value with Error error -> Error error | Ok value -> f value

let pp_error ppf = function
  | Empty -> Format.pp_print_string ppf "endpoint is empty"
  | Missing_scheme -> Format.pp_print_string ppf "endpoint has no scheme"
  | Unsupported_scheme scheme ->
      Format.fprintf ppf "unsupported endpoint scheme %S" scheme
  | Invalid_authority -> Format.pp_print_string ppf "invalid endpoint authority"
  | Missing_host -> Format.pp_print_string ppf "endpoint has no host"
  | Invalid_host host -> Format.fprintf ppf "invalid endpoint host %S" host
  | Unbracketed_ipv6 ->
      Format.pp_print_string ppf "IPv6 endpoint hosts must be bracketed"
  | Userinfo_not_supported ->
      Format.pp_print_string ppf "endpoint userinfo is not supported"
  | Invalid_port port -> Format.fprintf ppf "invalid endpoint port %S" port
  | Port_out_of_range port ->
      Format.fprintf ppf "endpoint port is out of range %S" port
  | Invalid_suffix ->
      Format.pp_print_string ppf
        "endpoint paths, queries, and fragments are not supported"

let scheme_of_string = function
  | "nats" -> Ok Nats
  | "tls" -> Ok Tls
  | scheme -> Error (Unsupported_scheme scheme)

let string_of_scheme = function Nats -> "nats" | Tls -> "tls"

let find_char_from value character start =
  let result = ref None in
  let length = String.length value in
  for position = start to length - 1 do
    match !result with
    | Some _ -> ()
    | None ->
        if Char.equal (String.get value position) character then
          result := Some position
  done;
  !result

let authority_end value start =
  let result = ref (String.length value) in
  for position = start to String.length value - 1 do
    if
      position < !result
      && (Char.equal (String.get value position) '/'
         || Char.equal (String.get value position) '?'
         || Char.equal (String.get value position) '#')
    then result := position
  done;
  !result

let invalid_host_character character =
  let code = Char.code character in
  code <= 32 || Int.equal code 127 || Char.equal character '['
  || Char.equal character ']' || Char.equal character '@'
  || Char.equal character '/' || Char.equal character '?'
  || Char.equal character '#'

let validate_host ~allow_colon host =
  let invalid = ref false in
  for position = 0 to String.length host - 1 do
    match !invalid with
    | true -> ()
    | false ->
        let character = String.get host position in
        if
          invalid_host_character character
          || ((not allow_colon) && Char.equal character ':')
        then invalid := true
  done;
  if String.length host = 0 || !invalid then Error (Invalid_host host)
  else Ok (String.lowercase_ascii host)

let parse_port value =
  if String.length value = 0 then Error (Invalid_port value)
  else
    let parsed = ref 0 in
    let failure = ref None in
    for position = 0 to String.length value - 1 do
      match !failure with
      | Some _ -> ()
      | None ->
          let character = String.get value position in
          if character < '0' || character > '9' then
            failure := Some (Invalid_port value)
          else
            let digit = Char.code character - Char.code '0' in
            if !parsed > (65535 - digit) / 10 then
              failure := Some (Port_out_of_range value)
            else parsed := (!parsed * 10) + digit
    done;
    match !failure with
    | Some error -> Error error
    | None when Int.equal !parsed 0 -> Error (Invalid_port value)
    | None -> Ok !parsed

let parse_authority authority =
  if String.length authority = 0 then Error Missing_host
  else if String.contains authority '@' then Error Userinfo_not_supported
  else if Char.equal (String.get authority 0) '[' then
    match find_char_from authority ']' 1 with
    | None -> Error (Invalid_host authority)
    | Some close ->
        let host = String.sub authority 1 (close - 1) in
        let suffix_start = close + 1 in
        let suffix =
          String.sub authority suffix_start
            (String.length authority - suffix_start)
        in
        let* host = validate_host ~allow_colon:true host in
        let* port =
          if String.length suffix = 0 then Ok 4222
          else if Char.equal (String.get suffix 0) ':' then
            parse_port (String.sub suffix 1 (String.length suffix - 1))
          else Error (Invalid_host authority)
        in
        Ok (host, port)
  else
    let first_colon = find_char_from authority ':' 0 in
    match first_colon with
    | None ->
        let* host = validate_host ~allow_colon:false authority in
        Ok (host, 4222)
    | Some colon ->
        if Option.is_some (find_char_from authority ':' (colon + 1)) then
          Error Unbracketed_ipv6
        else
          let host = String.sub authority 0 colon in
          let port =
            String.sub authority (colon + 1)
              (String.length authority - colon - 1)
          in
          let* host = validate_host ~allow_colon:false host in
          let* port = parse_port port in
          Ok (host, port)

let of_string value =
  let length = String.length value in
  if Int.equal length 0 then Error Empty
  else
    match String.index_opt value ':' with
    | None -> Error Missing_scheme
    | Some separator ->
        let raw_scheme =
          String.sub value 0 separator |> String.lowercase_ascii
        in
        let* scheme = scheme_of_string raw_scheme in
        let authority_start = separator + 1 in
        if
          authority_start + 1 >= length
          || (not (Char.equal (String.get value authority_start) '/'))
          || not (Char.equal (String.get value (authority_start + 1)) '/')
        then Error Invalid_authority
        else
          let host_start = authority_start + 2 in
          let host_stop = authority_end value host_start in
          if not (Int.equal host_stop length) then Error Invalid_suffix
          else
            let authority =
              String.sub value host_start (host_stop - host_start)
            in
            let* host, port = parse_authority authority in
            Ok { scheme; host; port }

let of_connect_url ?(default_scheme = Nats) value =
  let length = String.length value in
  if Int.equal length 0 then Error Empty
  else
    match String.index_opt value ':' with
    | Some separator
      when separator + 2 < length
           && Char.equal (String.get value (separator + 1)) '/'
           && Char.equal (String.get value (separator + 2)) '/' ->
        of_string value
    | _ ->
        let stop = authority_end value 0 in
        if not (Int.equal stop length) then Error Invalid_suffix
        else
          let* host, port = parse_authority value in
          Ok { scheme = default_scheme; host; port }

let scheme (value : t) = value.scheme
let host (value : t) = value.host
let port (value : t) = value.port

let to_string value =
  let host =
    if String.contains value.host ':' then "[" ^ value.host ^ "]"
    else value.host
  in
  Format.asprintf "%s://%s:%d" (string_of_scheme value.scheme) host value.port

let pp ppf value = Format.pp_print_string ppf (to_string value)

let compare_scheme left right =
  match (left, right) with
  | Nats, Nats | Tls, Tls -> 0
  | Nats, Tls -> -1
  | Tls, Nats -> 1

let equal left right =
  Int.equal (compare_scheme left.scheme right.scheme) 0
  && String.equal left.host right.host
  && Int.equal left.port right.port

let compare left right =
  let result = compare_scheme left.scheme right.scheme in
  if not (Int.equal result 0) then result
  else
    let result = String.compare left.host right.host in
    if not (Int.equal result 0) then result
    else Int.compare left.port right.port

module Pool = struct
  type endpoint = t

  type t = {
    seeds : endpoint list;
    discovered : endpoint list;
    order : endpoint list;
    preferred : endpoint option;
  }

  let contains endpoint endpoints =
    List.exists (fun value -> equal endpoint value) endpoints

  let deduplicate endpoints =
    let seen = ref [] in
    let result = ref [] in
    List.iter
      (fun endpoint ->
        if not (contains endpoint !seen) then (
          seen := endpoint :: !seen;
          result := endpoint :: !result))
      endpoints;
    List.rev !result

  let base t = deduplicate (t.seeds @ t.discovered)

  let keep_preferred preferred endpoints =
    match preferred with
    | None -> endpoints
    | Some preferred ->
        preferred
        :: List.filter
             (fun endpoint -> not (equal endpoint preferred))
             endpoints

  let rebuild_order t ~discovered ~preferred =
    let base = deduplicate (t.seeds @ discovered) in
    let existing =
      List.filter (fun endpoint -> contains endpoint base) t.order
    in
    let additions =
      List.filter (fun endpoint -> not (contains endpoint existing)) base
    in
    let order = existing @ additions in
    {
      seeds = t.seeds;
      discovered;
      order = keep_preferred preferred order;
      preferred;
    }

  let v seeds =
    let seeds = deduplicate seeds in
    { seeds; discovered = []; order = seeds; preferred = None }

  let seeds t = t.seeds
  let discovered t = t.discovered
  let candidates t = t.order
  let preferred t = t.preferred

  let update_discovered t discovered =
    rebuild_order t ~discovered:(deduplicate discovered) ~preferred:t.preferred

  let connected t endpoint =
    rebuild_order t ~discovered:t.discovered ~preferred:(Some endpoint)

  let move_to_end endpoint endpoints =
    let present = contains endpoint endpoints in
    let remaining =
      List.filter (fun value -> not (equal endpoint value)) endpoints
    in
    if present then remaining @ [ endpoint ] else remaining

  let failed t endpoint =
    let t = update_discovered t t.discovered in
    let sticky = contains endpoint (base t) in
    let order =
      if sticky then move_to_end endpoint t.order
      else List.filter (fun value -> not (equal endpoint value)) t.order
    in
    let preferred =
      match t.preferred with
      | Some value when equal endpoint value -> None
      | value -> value
    in
    { t with order; preferred }
end
