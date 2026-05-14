type t = string

type error =
  | Empty
  | Empty_token of { position : int }
  | Invalid_character of { position : int; character : char }
  | Wildcard_not_allowed of { position : int; token : string }
  | Wildcard_must_be_token of { position : int }
  | Wildcard_not_terminal of { position : int }

let pp_error ppf = function
  | Empty -> Format.pp_print_string ppf "subject is empty"
  | Empty_token { position } ->
      Format.fprintf ppf "subject has an empty token at position %d" position
  | Invalid_character { position; character } ->
      Format.fprintf ppf "invalid character %C at position %d" character
        position
  | Wildcard_not_allowed { position; token } ->
      Format.fprintf ppf "wildcard token %S is not allowed at position %d" token
        position
  | Wildcard_must_be_token { position } ->
      Format.fprintf ppf "wildcard at position %d must be a complete token"
        position
  | Wildcard_not_terminal { position } ->
      Format.fprintf ppf "tail wildcard at position %d must be the final token"
        position

let is_forbidden_character character =
  let code = Char.code character in
  Int.compare code 32 <= 0 || Int.equal code 127

let validate_token ~filter ~final subject start stop =
  if Int.equal start stop then Error (Empty_token { position = start })
  else
    let failure = ref None in
    for position = start to stop - 1 do
      match !failure with
      | Some _ -> ()
      | None ->
          let character = String.get subject position in
          if is_forbidden_character character then
            failure := Some (Invalid_character { position; character })
          else if Char.equal character '*' || Char.equal character '>' then
            if not filter then
              failure :=
                Some
                  (Wildcard_not_allowed
                     {
                       position;
                       token = String.sub subject start (stop - start);
                     })
            else if not (Int.equal (stop - start) 1) then
              failure := Some (Wildcard_must_be_token { position })
            else if Char.equal character '>' && not final then
              failure := Some (Wildcard_not_terminal { position })
    done;
    match !failure with Some error -> Error error | None -> Ok ()

let validate ~filter subject =
  let length = String.length subject in
  if Int.equal length 0 then Error Empty
  else
    let failure = ref None in
    let token_start = ref 0 in
    for position = 0 to length do
      match !failure with
      | Some _ -> ()
      | None -> (
          let at_end = Int.equal position length in
          let at_separator =
            (not at_end) && Char.equal (String.get subject position) '.'
          in
          if at_end || at_separator then
            match
              validate_token ~filter ~final:at_end subject !token_start position
            with
            | Ok () -> token_start := position + 1
            | Error error -> failure := Some error)
    done;
    match !failure with Some error -> Error error | None -> Ok subject

let of_string subject = validate ~filter:false subject

let literal subject =
  match of_string subject with
  | Ok subject -> subject
  | Error error ->
      invalid_arg (Format.asprintf "invalid NATS subject %a" pp_error error)

let to_string subject = subject
let pp ppf subject = Format.pp_print_string ppf subject
let equal = String.equal

module Filter = struct
  type nonrec error = error
  type t = string

  let of_string filter = validate ~filter:true filter

  let literal filter =
    match of_string filter with
    | Ok filter -> filter
    | Error error ->
        invalid_arg
          (Format.asprintf "invalid NATS subject filter %a" pp_error error)

  let to_string filter = filter
  let pp ppf filter = Format.pp_print_string ppf filter
  let equal = String.equal
end
