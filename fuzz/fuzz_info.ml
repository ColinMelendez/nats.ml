open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Info.pp_error" (fun () ->
         Format.asprintf "%a" Nats.Info.pp_error error))

let test_of_string input =
  match no_exception "Info.of_string" (fun () -> Nats.Info.of_string input) with
  | Error error -> render_error error
  | Ok info ->
      ignore
        (no_exception "Info.pp" (fun () ->
             Format.asprintf "%a" Nats.Info.pp info))

let suite =
  ("info", [ test_case "of_string is crash-safe" [ bytes ] test_of_string ])
