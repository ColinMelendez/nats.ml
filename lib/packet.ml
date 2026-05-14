type limits = {
  max_line_bytes : int;
  max_header_bytes : int;
  max_payload_bytes : int;
  max_packet_bytes : int;
}

let default_limits =
  {
    max_line_bytes = 4 * 1024;
    max_header_bytes = 64 * 1024;
    max_payload_bytes = 1024 * 1024;
    max_packet_bytes = 2 * 1024 * 1024;
  }

type framing =
  | Line
  | Payload of { bytes : int }
  | Headers of { header_bytes : int; total_bytes : int }

type t = { line : string; body : string; framing : framing }

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

let pp_error ppf = function
  | End_of_input -> Format.pp_print_string ppf "end of input"
  | Need_more -> Format.pp_print_string ppf "more input is required"
  | Unexpected_end -> Format.pp_print_string ppf "unexpected end of input"
  | Invalid_limits -> Format.pp_print_string ppf "invalid packet limits"
  | Line_too_long { limit } ->
      Format.fprintf ppf "control line exceeds %d bytes" limit
  | Packet_too_large { size; limit } ->
      Format.fprintf ppf "packet of %d bytes exceeds %d-byte limit" size limit
  | Headers_too_large { size; limit } ->
      Format.fprintf ppf "header block of %d bytes exceeds %d-byte limit" size
        limit
  | Payload_too_large { size; limit } ->
      Format.fprintf ppf "payload of %d bytes exceeds %d-byte limit" size limit
  | Malformed_line -> Format.pp_print_string ppf "malformed control line"
  | Invalid_length { keyword; value } ->
      Format.fprintf ppf "invalid %s length %S" keyword value
  | Invalid_lengths { keyword } ->
      Format.fprintf ppf "invalid %s length pair" keyword
  | Invalid_terminator ->
      Format.pp_print_string ppf "payload is not terminated by CRLF"
  | Unknown_operation keyword ->
      Format.fprintf ppf "unknown NATS operation %S" keyword

let valid_limits limits =
  limits.max_line_bytes > 0
  && limits.max_header_bytes > 0
  && limits.max_payload_bytes >= 0
  && limits.max_packet_bytes > 0

let find_crlf string =
  let length = String.length string in
  let found = ref None in
  if length >= 2 then
    for position = 0 to length - 2 do
      match !found with
      | Some _ -> ()
      | None ->
          if
            Char.equal (String.get string position) '\r'
            && Char.equal (String.get string (position + 1)) '\n'
          then found := Some position
    done;
  !found

let contains_internal_line_break string =
  let found = ref false in
  let last = String.length string - 1 in
  for position = 0 to last do
    let trailing_carriage_return =
      Int.equal position last && Char.equal (String.get string position) '\r'
    in
    if
      (not trailing_carriage_return)
      &&
      (Char.equal (String.get string position) '\r'
      || Char.equal (String.get string position) '\n')
    then found := true
  done;
  !found

let first_word line =
  match String.index_opt line ' ' with
  | None -> line
  | Some position -> String.sub line 0 position

let words line = String.split_on_char ' ' line
let invalid_length keyword value = Error (Invalid_length { keyword; value })

let parse_nonnegative keyword value =
  let length = String.length value in
  if Int.equal length 0 then invalid_length keyword value
  else
    let parsed = ref 0 in
    let invalid = ref false in
    for position = 0 to length - 1 do
      let code = Char.code (String.get value position) in
      if code < Char.code '0' || code > Char.code '9' then invalid := true
      else if not !invalid then
        let digit = code - Char.code '0' in
        if !parsed > (Stdlib.max_int - digit) / 10 then invalid := true
        else parsed := (!parsed * 10) + digit
    done;
    if !invalid then invalid_length keyword value else Ok !parsed

let add_size keyword left right =
  if left > Stdlib.max_int - right then invalid_length keyword "overflow"
  else Ok (left + right)

let validate_payload keyword size limits =
  if size > limits.max_payload_bytes then
    Error (Payload_too_large { size; limit = limits.max_payload_bytes })
  else Ok ()

let parse_payload_framing keyword value limits =
  match parse_nonnegative keyword value with
  | Error error -> Error error
  | Ok bytes ->
      validate_payload keyword bytes limits
      |> Result.map (fun () -> Payload { bytes })

let classify line limits =
  let keyword = first_word line in
  match keyword with
  | "PUB" -> (
      match words line with
      | [ "PUB"; _subject; payload_length ] ->
          parse_payload_framing keyword payload_length limits
      | [ "PUB"; _subject; _reply_to; payload_length ] ->
          parse_payload_framing keyword payload_length limits
      | _ -> Error Malformed_line)
  | "HPUB" -> (
      match words line with
      | [ "HPUB"; _subject; header_length; total_length ]
      | [ "HPUB"; _subject; _; header_length; total_length ] -> (
          match
            ( parse_nonnegative keyword header_length,
              parse_nonnegative keyword total_length )
          with
          | Ok header_bytes, Ok total_bytes ->
              if header_bytes > total_bytes then
                Error (Invalid_lengths { keyword })
              else if header_bytes > limits.max_header_bytes then
                Error
                  (Headers_too_large
                     { size = header_bytes; limit = limits.max_header_bytes })
              else
                let payload_bytes = total_bytes - header_bytes in
                validate_payload keyword payload_bytes limits
                |> Result.map (fun () -> Headers { header_bytes; total_bytes })
          | Error error, _ | _, Error error -> Error error)
      | _ -> Error Malformed_line)
  | "MSG" -> (
      match words line with
      | [ "MSG"; _subject; _sid; payload_length ]
      | [ "MSG"; _subject; _sid; _; payload_length ] ->
          parse_payload_framing keyword payload_length limits
      | _ -> Error Malformed_line)
  | "HMSG" -> (
      match words line with
      | [ "HMSG"; _subject; _sid; header_length; total_length ]
      | [ "HMSG"; _subject; _sid; _; header_length; total_length ] -> (
          match
            ( parse_nonnegative keyword header_length,
              parse_nonnegative keyword total_length )
          with
          | Ok header_bytes, Ok total_bytes ->
              if header_bytes > total_bytes then
                Error (Invalid_lengths { keyword })
              else if header_bytes > limits.max_header_bytes then
                Error
                  (Headers_too_large
                     { size = header_bytes; limit = limits.max_header_bytes })
              else
                let payload_bytes = total_bytes - header_bytes in
                validate_payload keyword payload_bytes limits
                |> Result.map (fun () -> Headers { header_bytes; total_bytes })
          | Error error, _ | _, Error error -> Error error)
      | _ -> Error Malformed_line)
  | "INFO" | "CONNECT" | "SUB" | "UNSUB" | "PING" | "PONG" | "+OK" | "-ERR" ->
      Ok Line
  | _ -> Error (Unknown_operation keyword)

let packet_size line_length framing =
  match add_size "packet" line_length 2 with
  | Error error -> Error error
  | Ok line_bytes -> (
      match framing with
      | Line -> Ok line_bytes
      | Payload { bytes } -> (
          match add_size "packet" bytes 2 with
          | Error error -> Error error
          | Ok body_bytes -> add_size "packet" line_bytes body_bytes)
      | Headers { total_bytes; _ } -> (
          match add_size "packet" total_bytes 2 with
          | Error error -> Error error
          | Ok body_bytes -> add_size "packet" line_bytes body_bytes))

let body_length = function
  | Line -> 0
  | Payload { bytes } -> bytes
  | Headers { total_bytes; _ } -> total_bytes

let read ?(eod = false) ?(limits = default_limits) reader =
  if not (valid_limits limits) then Error Invalid_limits
  else
    let restore reader string =
      let length = String.length string in
      if length > 0 then
        let bytes = Bytes.of_string string in
        Bytesrw.Bytes.Reader.push_back reader
          (Bytesrw.Bytes.Slice.make bytes ~first:0 ~length)
    in
    let buffer = Buffer.create (Int.min limits.max_packet_bytes 4096) in
    let add_slice slice = Bytesrw.Bytes.Slice.add_to_buffer buffer slice in
    let restore_error error =
      restore reader (Buffer.contents buffer);
      Error error
    in
    let rec read_line () =
      let prefix = Buffer.contents buffer in
      match find_crlf prefix with
      | Some line_end -> Ok (prefix, line_end)
      | None -> (
          if contains_internal_line_break prefix then Error Malformed_line
          else if String.length prefix >= limits.max_line_bytes + 2 then
            Error (Line_too_long { limit = limits.max_line_bytes })
          else
            match Bytesrw.Bytes.Reader.read reader with
            | slice when Bytesrw.Bytes.Slice.is_eod slice ->
                if eod then
                  if String.length prefix = 0 then Error End_of_input
                  else Error Unexpected_end
                else Error Need_more
            | slice ->
                add_slice slice;
                read_line ())
    in
    let read_body size =
      let rec loop () =
        if Buffer.length buffer >= size then Ok ()
        else
          match Bytesrw.Bytes.Reader.read reader with
          | slice when Bytesrw.Bytes.Slice.is_eod slice ->
              if eod then Error Unexpected_end
              else Error Need_more
          | slice ->
              add_slice slice;
              loop ()
      in
      loop ()
    in
    match read_line () with
    | Error error -> restore_error error
    | Ok (complete, line_end) -> (
        if line_end > limits.max_line_bytes then
          restore_error (Line_too_long { limit = limits.max_line_bytes })
        else
          let line = String.sub complete 0 line_end in
          match classify line limits with
          | Error error -> restore_error error
          | Ok framing -> (
              match packet_size line_end framing with
              | Error error -> restore_error error
              | Ok size -> (
                  if size > limits.max_packet_bytes then
                    restore_error
                      (Packet_too_large { size; limit = limits.max_packet_bytes })
                  else
                    match read_body size with
                    | Error error -> restore_error error
                    | Ok () ->
                        let complete = Buffer.contents buffer in
                        let has_body =
                          match framing with Line -> false | _ -> true
                        in
                        let valid_terminator =
                          (not has_body)
                          || Char.equal (String.get complete (size - 2)) '\r'
                             && Char.equal (String.get complete (size - 1)) '\n'
                        in
                        if not valid_terminator then restore_error Invalid_terminator
                        else (
                          if String.length complete > size then
                            restore reader
                              (String.sub complete size
                                 (String.length complete - size));
                          let body_start = line_end + 2 in
                          let body =
                            String.sub complete body_start (body_length framing)
                          in
                          Ok { line; body; framing }))))

let line packet = packet.line
let body packet = packet.body
let framing packet = packet.framing

let to_string packet =
  match packet.framing with
  | Line -> packet.line ^ "\r\n"
  | Payload _ | Headers _ -> packet.line ^ "\r\n" ^ packet.body ^ "\r\n"

let pp ppf packet =
  Format.fprintf ppf "{%s; body=%d bytes}" packet.line
    (String.length packet.body)
