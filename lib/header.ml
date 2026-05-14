type entry = { name : string; value : string }
type t = entry list

type error =
  | Empty_name
  | Invalid_name_character of { position : int; character : char }
  | Invalid_value_character of { position : int; character : char }

let pp_error ppf = function
  | Empty_name -> Format.pp_print_string ppf "header name is empty"
  | Invalid_name_character { position; character } ->
      Format.fprintf ppf "invalid header-name character %C at position %d"
        character position
  | Invalid_value_character { position; character } ->
      Format.fprintf ppf "invalid header-value character %C at position %d"
        character position

let invalid_name_character character =
  let code = Char.code character in
  Int.compare code 32 <= 0 || Int.equal code 127 || Char.equal character ':'

let invalid_value_character character =
  let code = Char.code character in
  Int.compare code 32 < 0 || Int.equal code 127

let validate_name name =
  if Int.equal (String.length name) 0 then Error Empty_name
  else
    let failure = ref None in
    for position = 0 to String.length name - 1 do
      match !failure with
      | Some _ -> ()
      | None ->
          let character = String.get name position in
          if invalid_name_character character then
            failure := Some (Invalid_name_character { position; character })
    done;
    match !failure with Some error -> Error error | None -> Ok ()

let validate_value value =
  let failure = ref None in
  for position = 0 to String.length value - 1 do
    match !failure with
    | Some _ -> ()
    | None ->
        let character = String.get value position in
        if invalid_value_character character then
          failure := Some (Invalid_value_character { position; character })
  done;
  match !failure with Some error -> Error error | None -> Ok ()

let validate_entry name value =
  match validate_name name with
  | Error error -> Error error
  | Ok () -> validate_value value

let empty = []
let is_empty headers = match headers with [] -> true | _ -> false

let of_list entries =
  List.fold_left
    (fun result (name, value) ->
      match result with
      | Error _ -> result
      | Ok headers -> (
          match validate_entry name value with
          | Error error -> Error error
          | Ok () -> Ok ({ name; value } :: headers)))
    (Ok []) entries

let add ~name ~value headers =
  match validate_entry name value with
  | Error error -> Error error
  | Ok () -> Ok ({ name; value } :: headers)

let to_list headers =
  List.rev_map (fun { name; value } -> (name, value)) headers

let equal_entry left right =
  String.equal left.name right.name && String.equal left.value right.value

let equal left right =
  let rec loop left right =
    match (left, right) with
    | [], [] -> true
    | left :: left_tail, right :: right_tail ->
        equal_entry left right && loop left_tail right_tail
    | [], _ :: _ | _ :: _, [] -> false
  in
  loop left right

let normalized name = String.lowercase_ascii name

let mem name headers =
  let wanted = normalized name in
  List.exists (fun { name; _ } -> String.equal wanted (normalized name)) headers

let find_all name headers =
  let wanted = normalized name in
  let values = ref [] in
  List.iter
    (fun { name; value } ->
      if String.equal wanted (normalized name) then values := value :: !values)
    headers;
  !values

let find name headers =
  match find_all name headers with [] -> None | value :: _ -> Some value

let pp ppf headers =
  Format.pp_print_string ppf "[";
  let first = ref true in
  List.iter
    (fun (name, value) ->
      if !first then first := false else Format.pp_print_string ppf "; ";
      Format.fprintf ppf "%s: %S" name value)
    (to_list headers);
  Format.pp_print_string ppf "]"
