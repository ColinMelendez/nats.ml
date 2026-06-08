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

let pp_error ppf = function
  | Packet error -> Format.fprintf ppf "packet: %a" Packet.pp_error error
  | Invalid_operation { keyword } ->
      Format.fprintf ppf "invalid %s operation" keyword
  | Invalid_field_count { keyword } ->
      Format.fprintf ppf "invalid field count for %s" keyword
  | Invalid_subject error ->
      Format.fprintf ppf "invalid subject: %a" Subject.pp_error error
  | Invalid_header error ->
      Format.fprintf ppf "invalid header: %a" Header.pp_error error
  | Invalid_header_block -> Format.pp_print_string ppf "invalid header block"
  | Invalid_status -> Format.pp_print_string ppf "invalid header status"
  | Invalid_sid value -> Format.fprintf ppf "invalid subscription id %S" value
  | Invalid_max_messages value ->
      Format.fprintf ppf "invalid maximum message count %S" value
  | Invalid_value { keyword } ->
      Format.fprintf ppf "invalid value in %s operation" keyword
  | Message_has_headers ->
      Format.pp_print_string ppf "ordinary operation has headers"
  | Invalid_limits -> Format.pp_print_string ppf "invalid codec limits"

let packet_error error = Error (Packet error)

let keyword line =
  match String.index_opt line ' ' with
  | None -> line
  | Some position -> String.sub line 0 position

let words line = String.split_on_char ' ' line

let rest line keyword =
  let keyword_length = String.length keyword in
  if String.length line = keyword_length then None
  else if
    String.length line > keyword_length
    && Char.equal (String.get line keyword_length) ' '
  then
    Some
      (String.sub line (keyword_length + 1)
         (String.length line - keyword_length - 1))
  else None

let contains_line_break value =
  let found = ref false in
  for position = 0 to String.length value - 1 do
    if
      Char.equal (String.get value position) '\r'
      || Char.equal (String.get value position) '\n'
    then found := true
  done;
  !found

let parse_nonnegative value =
  let length = String.length value in
  if Int.equal length 0 then None
  else
    let parsed = ref 0 in
    let valid = ref true in
    for position = 0 to length - 1 do
      let code = Char.code (String.get value position) in
      if code < Char.code '0' || code > Char.code '9' then valid := false
      else if !valid then
        let digit = code - Char.code '0' in
        if !parsed > (Stdlib.max_int - digit) / 10 then valid := false
        else parsed := (!parsed * 10) + digit
    done;
    if !valid then Some !parsed else None

let parse_sid value =
  match parse_nonnegative value with
  | Some sid when sid > 0 -> Ok sid
  | _ -> Error (Invalid_sid value)

let parse_max_messages value =
  match parse_nonnegative value with
  | Some count -> Ok count
  | None -> Error (Invalid_max_messages value)

let subject value =
  match Subject.of_string value with
  | Ok subject -> Ok subject
  | Error error -> Error (Invalid_subject error)

let filter value =
  match Subject.Filter.of_string value with
  | Ok filter -> Ok filter
  | Error error -> Error (Invalid_subject error)

let queue_group value =
  match Queue_group.of_string value with
  | Ok queue_group -> Ok queue_group
  | Error error -> Error (Invalid_subject error)

let reply_subject = function
  | None -> Ok None
  | Some value -> subject value |> Result.map (fun subject -> Some subject)

let optional_reply_value value =
  if String.equal value "" then None else Some value

let message ~subject_value ~reply_value ~headers payload =
  match subject subject_value with
  | Error error -> Error error
  | Ok subject -> (
      match reply_subject reply_value with
      | Error error -> Error error
      | Ok None -> Ok (Message.v ~subject ~headers payload)
      | Ok (Some reply_to) -> Ok (Message.v ~subject ~reply_to ~headers payload)
      )

let find_crlf_from value start =
  let length = String.length value in
  let found = ref None in
  if start <= length - 2 then
    for position = start to length - 2 do
      match !found with
      | Some _ -> ()
      | None ->
          if
            Char.equal (String.get value position) '\r'
            && Char.equal (String.get value (position + 1)) '\n'
          then found := Some position
    done;
  !found

let starts_with value prefix =
  let prefix_length = String.length prefix in
  String.length value >= prefix_length
  && String.equal (String.sub value 0 prefix_length) prefix

let status first_line =
  if String.equal first_line "NATS/1.0" then Ok None
  else if starts_with first_line "NATS/1.0 " then
    let rest = String.sub first_line 9 (String.length first_line - 9) in
    let code_value, description =
      match String.index_opt rest ' ' with
      | None -> (rest, "")
      | Some separator ->
          ( String.sub rest 0 separator,
            String.sub rest (separator + 1) (String.length rest - separator - 1)
          )
    in
    if Int.equal (String.length code_value) 3 then
      match parse_nonnegative code_value with
      | Some code when code >= 100 && code <= 999 ->
          if contains_line_break description then Error Invalid_status
          else Ok (Some { Op.code; description })
      | _ -> Error Invalid_status
    else Error Invalid_status
  else Error Invalid_status

let parse_header_field line =
  match String.index_opt line ':' with
  | None -> Error Invalid_header_block
  | Some separator ->
      if Int.equal separator 0 then Error Invalid_header_block
      else
        let name = String.sub line 0 separator in
        let value_start =
          if
            separator + 1 < String.length line
            && Char.equal (String.get line (separator + 1)) ' '
          then separator + 2
          else separator + 1
        in
        let value =
          String.sub line value_start (String.length line - value_start)
        in
        Ok (name, value)

let parse_header_block block =
  let length = String.length block in
  if length < 2 || not (String.ends_with ~suffix:"\r\n" block) then
    Error Invalid_header_block
  else
    match find_crlf_from block 0 with
    | None -> Error Invalid_header_block
    | Some first_end -> (
        match status (String.sub block 0 first_end) with
        | Error error -> Error error
        | Ok status -> (
            let fields_end = length - 2 in
            let fields = ref [] in
            let position = ref (first_end + 2) in
            let invalid = ref false in
            while (not !invalid) && !position < fields_end do
              match find_crlf_from block !position with
              | None -> invalid := true
              | Some line_end -> (
                  if line_end > fields_end then invalid := true
                  else
                    let line =
                      String.sub block !position (line_end - !position)
                    in
                    if Int.equal (String.length line) 0 then invalid := true
                    else
                      match parse_header_field line with
                      | Error _ -> invalid := true
                      | Ok field ->
                          fields := field :: !fields;
                          position := line_end + 2)
            done;
            if !invalid || not (Int.equal !position fields_end) then
              Error Invalid_header_block
            else
              match Header.of_list (List.rev !fields) with
              | Ok headers -> Ok (status, headers)
              | Error error -> Error (Invalid_header error)))

let payload_and_headers packet =
  match Packet.framing packet with
  | Packet.Headers { header_bytes; total_bytes } ->
      let body = Packet.body packet in
      let header_block = String.sub body 0 header_bytes in
      let payload = String.sub body header_bytes (total_bytes - header_bytes) in
      parse_header_block header_block
      |> Result.map (fun (status, headers) -> (status, headers, payload))
  | _ -> Error (Invalid_operation { keyword = keyword (Packet.line packet) })

let expect_line packet expected_keyword =
  if
    String.equal (keyword (Packet.line packet)) expected_keyword
    && match Packet.framing packet with Packet.Line -> true | _ -> false
  then Ok ()
  else Error (Invalid_operation { keyword = expected_keyword })

let rec decode packet =
  let line = Packet.line packet in
  let operation = keyword line in
  match operation with
  | "INFO" -> (
      match (expect_line packet operation, rest line operation) with
      | Ok (), Some json -> Ok (Op.Info json)
      | Ok (), None -> Error (Invalid_value { keyword = operation })
      | Error error, _ -> Error error)
  | "CONNECT" -> (
      match (expect_line packet operation, rest line operation) with
      | Ok (), Some json -> Ok (Op.Connect json)
      | Ok (), None -> Error (Invalid_value { keyword = operation })
      | Error error, _ -> Error error)
  | "PING" -> expect_line packet operation |> Result.map (fun () -> Op.Ping)
  | "PONG" -> expect_line packet operation |> Result.map (fun () -> Op.Pong)
  | "+OK" -> expect_line packet operation |> Result.map (fun () -> Op.Ok)
  | "-ERR" -> (
      match (expect_line packet operation, rest line operation) with
      | Ok (), Some message -> Ok (Op.Server_error message)
      | Ok (), None -> Error (Invalid_value { keyword = operation })
      | Error error, _ -> Error error)
  | "SUB" -> decode_sub packet
  | "UNSUB" -> decode_unsub packet
  | "PUB" -> decode_pub packet
  | "HPUB" -> decode_hpub packet
  | "MSG" -> decode_msg packet
  | "HMSG" -> decode_hmsg packet
  | _ -> Error (Invalid_operation { keyword = operation })

and decode_sub packet =
  let operation = "SUB" in
  match expect_line packet operation with
  | Error error -> Error error
  | Ok () -> (
      match words (Packet.line packet) with
      | [ "SUB"; filter_value; sid_value ] -> (
          match (filter filter_value, parse_sid sid_value) with
          | Ok subject, Ok sid ->
              Ok (Op.Sub { subject; queue_group = None; sid })
          | Error error, _ | _, Error error -> Error error)
      | [ "SUB"; filter_value; queue_value; sid_value ] -> (
          match
            (filter filter_value, queue_group queue_value, parse_sid sid_value)
          with
          | Ok subject, Ok queue_group, Ok sid ->
              Ok (Op.Sub { subject; queue_group = Some queue_group; sid })
          | Error error, _, _ | _, Error error, _ | _, _, Error error ->
              Error error)
      | _ -> Error (Invalid_field_count { keyword = operation }))

and decode_unsub packet =
  let operation = "UNSUB" in
  match expect_line packet operation with
  | Error error -> Error error
  | Ok () -> (
      match words (Packet.line packet) with
      | [ "UNSUB"; sid_value ] ->
          parse_sid sid_value
          |> Result.map (fun sid -> Op.Unsub { sid; max_messages = None })
      | [ "UNSUB"; sid_value; max_value ] -> (
          match (parse_sid sid_value, parse_max_messages max_value) with
          | Ok sid, Ok max_messages ->
              Ok (Op.Unsub { sid; max_messages = Some max_messages })
          | Error error, _ | _, Error error -> Error error)
      | _ -> Error (Invalid_field_count { keyword = operation }))

and decode_pub packet =
  let operation = "PUB" in
  match Packet.framing packet with
  | Packet.Payload _ -> (
      match words (Packet.line packet) with
      | [ "PUB"; subject_value; payload_length ] -> (
          match
            message ~subject_value ~reply_value:None ~headers:Header.empty
              (Packet.body packet)
          with
          | Ok message -> (
              match parse_nonnegative payload_length with
              | Some _ -> Ok (Op.Pub message)
              | None -> Error (Invalid_value { keyword = operation }))
          | Error error -> Error error)
      | [ "PUB"; subject_value; reply_value; _payload_length ] ->
          message ~subject_value
            ~reply_value:(optional_reply_value reply_value)
            ~headers:Header.empty (Packet.body packet)
          |> Result.map (fun message -> Op.Pub message)
      | _ -> Error (Invalid_field_count { keyword = operation }))
  | _ -> Error (Invalid_operation { keyword = operation })

and decode_hpub packet =
  let operation = "HPUB" in
  match Packet.framing packet with
  | Packet.Headers _ -> (
      match words (Packet.line packet) with
      | [ "HPUB"; subject_value; _header_length; _total_length ] ->
          decode_hpub_message packet ~subject_value ~reply_value:None
      | [ "HPUB"; subject_value; reply_value; _header_length; _total_length ] ->
          decode_hpub_message packet ~subject_value
            ~reply_value:(optional_reply_value reply_value)
      | _ -> Error (Invalid_field_count { keyword = operation }))
  | _ -> Error (Invalid_operation { keyword = operation })

and decode_hpub_message packet ~subject_value ~reply_value =
  match payload_and_headers packet with
  | Error error -> Error error
  | Ok (status, headers, payload) ->
      message ~subject_value ~reply_value ~headers payload
      |> Result.map (fun message -> Op.Hpub { message; status })

and decode_msg packet =
  let operation = "MSG" in
  match Packet.framing packet with
  | Packet.Payload _ -> (
      match words (Packet.line packet) with
      | [ "MSG"; subject_value; sid_value; _payload_length ] -> (
          match parse_sid sid_value with
          | Error error -> Error error
          | Ok sid ->
              message ~subject_value ~reply_value:None ~headers:Header.empty
                (Packet.body packet)
              |> Result.map (fun message -> Op.Msg { sid; message }))
      | [ "MSG"; subject_value; sid_value; reply_value; _payload_length ] -> (
          match parse_sid sid_value with
          | Error error -> Error error
          | Ok sid ->
              message ~subject_value
                ~reply_value:(optional_reply_value reply_value)
                ~headers:Header.empty (Packet.body packet)
              |> Result.map (fun message -> Op.Msg { sid; message }))
      | _ -> Error (Invalid_field_count { keyword = operation }))
  | _ -> Error (Invalid_operation { keyword = operation })

and decode_hmsg packet =
  let operation = "HMSG" in
  match Packet.framing packet with
  | Packet.Headers _ -> (
      match words (Packet.line packet) with
      | [ "HMSG"; subject_value; sid_value; _header_length; _total_length ] -> (
          match parse_sid sid_value with
          | Error error -> Error error
          | Ok sid ->
              decode_hmsg_message packet ~sid ~subject_value ~reply_value:None)
      | [
       "HMSG";
       subject_value;
       sid_value;
       reply_value;
       _header_length;
       _total_length;
      ] -> (
          match parse_sid sid_value with
          | Error error -> Error error
          | Ok sid ->
              decode_hmsg_message packet ~sid ~subject_value
                ~reply_value:(optional_reply_value reply_value))
      | _ -> Error (Invalid_field_count { keyword = operation }))
  | _ -> Error (Invalid_operation { keyword = operation })

and decode_hmsg_message packet ~sid ~subject_value ~reply_value =
  match payload_and_headers packet with
  | Error error -> Error error
  | Ok (status, headers, payload) ->
      message ~subject_value ~reply_value ~headers payload
      |> Result.map (fun message -> Op.Hmsg { sid; message; status })

type output = { line : string; body : string; framing : Packet.framing }

let header_block status headers =
  let buffer = Buffer.create 64 in
  Buffer.add_string buffer "NATS/1.0";
  (match status with
  | None -> ()
  | Some { Op.code; description } ->
      Buffer.add_char buffer ' ';
      Buffer.add_string buffer (string_of_int code);
      if not (String.equal description "") then (
        Buffer.add_char buffer ' ';
        Buffer.add_string buffer description));
  Buffer.add_string buffer "\r\n";
  List.iter
    (fun (name, value) ->
      Buffer.add_string buffer name;
      Buffer.add_string buffer ": ";
      Buffer.add_string buffer value;
      Buffer.add_string buffer "\r\n")
    (Header.to_list headers);
  Buffer.add_string buffer "\r\n";
  Buffer.contents buffer

let valid_status = function
  | None -> true
  | Some { Op.code; description } ->
      code >= 100 && code <= 999 && not (contains_line_break description)

let reply_suffix reply_to =
  match reply_to with
  | None -> ""
  | Some reply_to -> " " ^ Subject.to_string reply_to

let output_size output =
  let line_size = String.length output.line in
  match output.framing with
  | Packet.Line -> line_size + 2
  | Packet.Payload { bytes } -> line_size + 2 + bytes + 2
  | Packet.Headers { total_bytes; _ } -> line_size + 2 + total_bytes + 2

let check_limits (limits : Packet.limits) output =
  if
    limits.max_line_bytes <= 0
    || limits.max_header_bytes <= 0
    || limits.max_payload_bytes < 0
    || limits.max_packet_bytes <= 0
  then Error Invalid_limits
  else if String.length output.line > limits.max_line_bytes then
    packet_error (Packet.Line_too_long { limit = limits.max_line_bytes })
  else
    let body_length = String.length output.body in
    let payload_length, header_length =
      match output.framing with
      | Packet.Line -> (0, 0)
      | Packet.Payload { bytes } -> (bytes, 0)
      | Packet.Headers { header_bytes; total_bytes } ->
          (total_bytes - header_bytes, header_bytes)
    in
    if header_length > limits.max_header_bytes then
      packet_error
        (Packet.Headers_too_large
           { size = header_length; limit = limits.max_header_bytes })
    else if payload_length > limits.max_payload_bytes then
      packet_error
        (Packet.Payload_too_large
           { size = payload_length; limit = limits.max_payload_bytes })
    else
      let expected_body_length =
        match output.framing with
        | Packet.Line -> 0
        | Packet.Payload { bytes } -> bytes
        | Packet.Headers { total_bytes; _ } -> total_bytes
      in
      if body_length <> expected_body_length then Error Invalid_limits
      else if output_size output > limits.max_packet_bytes then
        packet_error
          (Packet.Packet_too_large
             { size = output_size output; limit = limits.max_packet_bytes })
      else Ok ()

let sid_suffix sid =
  match sid with None -> "" | Some sid -> " " ^ string_of_int sid

let make_payload_output ?sid ~keyword ~subject ~reply_to payload =
  let line =
    keyword ^ " " ^ Subject.to_string subject ^ sid_suffix sid
    ^ reply_suffix reply_to ^ " "
    ^ string_of_int (String.length payload)
  in
  {
    line;
    body = payload;
    framing = Packet.Payload { bytes = String.length payload };
  }

let make_headers_output ?sid ~keyword ~subject ~reply_to ~status headers payload
    =
  let header_block = header_block status headers in
  let header_bytes = String.length header_block in
  let total_bytes = header_bytes + String.length payload in
  let line =
    keyword ^ " " ^ Subject.to_string subject ^ sid_suffix sid
    ^ reply_suffix reply_to ^ " " ^ string_of_int header_bytes ^ " "
    ^ string_of_int total_bytes
  in
  {
    line;
    body = header_block ^ payload;
    framing = Packet.Headers { header_bytes; total_bytes };
  }

let encode_output ?(limits = Packet.default_limits) output =
  match check_limits limits output with
  | Error error -> Error error
  | Ok () -> (
      match output.framing with
      | Packet.Line -> Ok (output.line ^ "\r\n")
      | Packet.Payload _ | Packet.Headers _ ->
          Ok (output.line ^ "\r\n" ^ output.body ^ "\r\n"))

let encode ?(limits = Packet.default_limits) operation =
  let output =
    match operation with
    | Op.Info json ->
        if String.length json = 0 || contains_line_break json then
          Error (Invalid_value { keyword = "INFO" })
        else Ok { line = "INFO " ^ json; body = ""; framing = Packet.Line }
    | Op.Connect json ->
        if String.length json = 0 || contains_line_break json then
          Error (Invalid_value { keyword = "CONNECT" })
        else Ok { line = "CONNECT " ^ json; body = ""; framing = Packet.Line }
    | Op.Pub message ->
        if not (Header.is_empty (Message.headers message)) then
          Error Message_has_headers
        else
          Ok
            (make_payload_output ~keyword:"PUB"
               ~subject:(Message.subject message)
               ~reply_to:(Message.reply_to message) (Message.payload message))
    | Op.Hpub { message; status } ->
        if not (valid_status status) then Error Invalid_status
        else
          Ok
            (make_headers_output ~keyword:"HPUB"
               ~subject:(Message.subject message)
               ~reply_to:(Message.reply_to message) ~status
               (Message.headers message) (Message.payload message))
    | Op.Sub { subject; queue_group; sid } ->
        if sid <= 0 then Error (Invalid_sid (string_of_int sid))
        else
          let queue =
            match queue_group with
            | None -> ""
            | Some queue_group -> " " ^ Queue_group.to_string queue_group
          in
          Ok
            {
              line =
                "SUB "
                ^ Subject.Filter.to_string subject
                ^ queue ^ " " ^ string_of_int sid;
              body = "";
              framing = Packet.Line;
            }
    | Op.Unsub { sid; max_messages } -> (
        if sid <= 0 then Error (Invalid_sid (string_of_int sid))
        else
          match max_messages with
          | Some max_messages when max_messages < 0 ->
              Error (Invalid_max_messages (string_of_int max_messages))
          | None | Some _ ->
              let count =
                match max_messages with
                | None -> ""
                | Some max_messages -> " " ^ string_of_int max_messages
              in
              Ok
                {
                  line = "UNSUB " ^ string_of_int sid ^ count;
                  body = "";
                  framing = Packet.Line;
                })
    | Op.Msg { sid; message } ->
        if sid <= 0 then Error (Invalid_sid (string_of_int sid))
        else if not (Header.is_empty (Message.headers message)) then
          Error Message_has_headers
        else
          Ok
            (make_payload_output ~keyword:"MSG"
               ~subject:(Message.subject message)
               ~reply_to:(Message.reply_to message) ~sid
               (Message.payload message))
    | Op.Hmsg { sid; message; status } ->
        if sid <= 0 then Error (Invalid_sid (string_of_int sid))
        else if not (valid_status status) then Error Invalid_status
        else
          Ok
            (make_headers_output ~keyword:"HMSG"
               ~subject:(Message.subject message)
               ~reply_to:(Message.reply_to message) ~status ~sid
               (Message.headers message) (Message.payload message))
    | Op.Ping -> Ok { line = "PING"; body = ""; framing = Packet.Line }
    | Op.Pong -> Ok { line = "PONG"; body = ""; framing = Packet.Line }
    | Op.Ok -> Ok { line = "+OK"; body = ""; framing = Packet.Line }
    | Op.Server_error message ->
        if String.length message = 0 || contains_line_break message then
          Error (Invalid_value { keyword = "-ERR" })
        else Ok { line = "-ERR " ^ message; body = ""; framing = Packet.Line }
  in
  match output with
  | Error error -> Error error
  | Ok output -> encode_output ~limits output

let read ?eod ?limits reader =
  match Packet.read ?eod ?limits reader with
  | Error error -> Error (Packet error)
  | Ok packet -> decode packet

let write ?limits writer operation =
  match encode ?limits operation with
  | Error error -> Error error
  | Ok bytes ->
      Bytesrw.Bytes.Writer.write_string writer bytes;
      Ok ()
