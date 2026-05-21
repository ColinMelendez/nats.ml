open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Error.pp" (fun () ->
         Format.asprintf "%a" Nats.Error.pp error))

let render_transition transition =
  List.iter
    (fun event ->
      ignore
        (no_exception "Event.pp" (fun () ->
             Format.asprintf "%a" Nats.Event.pp event)))
    transition.Nats.Client.events;
  List.iter
    (fun delivery ->
      ignore
        (no_exception "Message.pp" (fun () ->
             Format.asprintf "%a" Nats.Message.pp delivery.Nats.Client.message)))
    transition.Nats.Client.deliveries

let test_incoming input =
  let client = Nats.Client.v Nats.Config.default in
  let reader = Bytesrw.Bytes.Reader.of_string input in
  let result =
    no_exception "Client.incoming" (fun () ->
        Nats.Client.incoming ~eod:true client ~now:Mtime.min_stamp reader)
  in
  match result with
  | Error error -> render_error error
  | Ok transition -> render_transition transition

let suite =
  ( "client",
    [ test_case "incoming transitions are crash-safe" [ bytes ] test_incoming ]
  )
