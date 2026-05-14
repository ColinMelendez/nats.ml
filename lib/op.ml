type status = { code : int; description : string }

type t =
  | Info of string
  | Connect of string
  | Pub of Message.t
  | Hpub of { message : Message.t; status : status option }
  | Sub of {
      subject : Subject.Filter.t;
      queue_group : Queue_group.t option;
      sid : int;
    }
  | Unsub of { sid : int; max_messages : int option }
  | Msg of { sid : int; message : Message.t }
  | Hmsg of { sid : int; message : Message.t; status : status option }
  | Ping
  | Pong
  | Ok
  | Server_error of string

let pp_status ppf { code; description } =
  Format.fprintf ppf "%d %s" code description

let pp ppf = function
  | Info json -> Format.fprintf ppf "INFO %S" json
  | Connect json -> Format.fprintf ppf "CONNECT %S" json
  | Pub message -> Format.fprintf ppf "PUB %a" Message.pp message
  | Hpub { message; status } -> (
      match status with
      | None -> Format.fprintf ppf "HPUB %a" Message.pp message
      | Some status ->
          Format.fprintf ppf "HPUB (%a) %a" pp_status status Message.pp message)
  | Sub { subject; queue_group; sid } -> (
      match queue_group with
      | None -> Format.fprintf ppf "SUB %a sid=%d" Subject.Filter.pp subject sid
      | Some queue_group ->
          Format.fprintf ppf "SUB %a %a sid=%d" Subject.Filter.pp subject
            Queue_group.pp queue_group sid)
  | Unsub { sid; max_messages } -> (
      match max_messages with
      | None -> Format.fprintf ppf "UNSUB sid=%d" sid
      | Some max_messages ->
          Format.fprintf ppf "UNSUB sid=%d max=%d" sid max_messages)
  | Msg { sid; message } ->
      Format.fprintf ppf "MSG sid=%d %a" sid Message.pp message
  | Hmsg { sid; message; status } -> (
      match status with
      | None -> Format.fprintf ppf "HMSG sid=%d %a" sid Message.pp message
      | Some status ->
          Format.fprintf ppf "HMSG sid=%d (%a) %a" sid pp_status status
            Message.pp message)
  | Ping -> Format.pp_print_string ppf "PING"
  | Pong -> Format.pp_print_string ppf "PONG"
  | Ok -> Format.pp_print_string ppf "+OK"
  | Server_error message -> Format.fprintf ppf "-ERR %S" message
