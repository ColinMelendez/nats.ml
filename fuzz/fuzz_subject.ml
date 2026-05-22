open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Subject.pp_error" (fun () ->
         Format.asprintf "%a" Nats.Subject.pp_error error))

let test_subject input =
  match Nats.Subject.of_string input with
  | Error error -> render_error error
  | Ok subject -> (
      ignore
        (no_exception "Subject.pp" (fun () ->
             Format.asprintf "%a" Nats.Subject.pp subject));
      let printed = Nats.Subject.to_string subject in
      match Nats.Subject.of_string printed with
      | Error error ->
          failf "valid subject became invalid after printing: %a"
            Nats.Subject.pp_error error
      | Ok subject' ->
          if not (Nats.Subject.equal subject subject') then
            failf "subject changed after roundtrip: %S <> %S" printed
              (Nats.Subject.to_string subject'))

let test_filter input =
  match Nats.Subject.Filter.of_string input with
  | Error error -> render_error error
  | Ok filter -> (
      ignore
        (no_exception "Subject.Filter.pp" (fun () ->
             Format.asprintf "%a" Nats.Subject.Filter.pp filter));
      let printed = Nats.Subject.Filter.to_string filter in
      match Nats.Subject.Filter.of_string printed with
      | Error error ->
          failf "valid filter became invalid after printing: %a"
            Nats.Subject.pp_error error
      | Ok filter' ->
          if not (Nats.Subject.Filter.equal filter filter') then
            failf "filter changed after roundtrip: %S <> %S" printed
              (Nats.Subject.Filter.to_string filter'))

let suite =
  ( "subject",
    [
      test_case "publish subject validation is crash-safe" [ bytes ]
        test_subject;
      test_case "filter validation is crash-safe" [ bytes ] test_filter;
    ] )
