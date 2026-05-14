type t = {
  subject : Subject.t;
  reply_to : Subject.t option;
  headers : Header.t;
  payload : string;
}

let v ~subject ?reply_to ?(headers = Header.empty) payload =
  { subject; reply_to; headers; payload }

let subject message = message.subject
let reply_to message = message.reply_to
let headers message = message.headers
let payload message = message.payload
let with_payload payload message = { message with payload }
let with_headers headers message = { message with headers }

let equal left right =
  Subject.equal left.subject right.subject
  && Option.equal Subject.equal left.reply_to right.reply_to
  && Header.equal left.headers right.headers
  && String.equal left.payload right.payload

let pp_reply_to ppf = function
  | None -> Format.pp_print_string ppf "none"
  | Some subject -> Format.fprintf ppf "%S" (Subject.to_string subject)

let pp ppf message =
  Format.fprintf ppf "Message(subject=%S, reply_to=%a, headers=%a, payload=%S)"
    (Subject.to_string message.subject)
    pp_reply_to message.reply_to Header.pp message.headers message.payload
