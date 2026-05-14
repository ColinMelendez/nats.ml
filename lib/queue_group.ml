type t = string
type error = Subject.error

let of_string name =
  match Subject.of_string name with
  | Ok subject -> Ok (Subject.to_string subject)
  | Error error -> Error error

let literal name =
  match of_string name with
  | Ok name -> name
  | Error error ->
      invalid_arg
        (Format.asprintf "invalid NATS queue group %a" Subject.pp_error error)

let to_string name = name
let pp ppf name = Format.pp_print_string ppf name
let equal = String.equal
