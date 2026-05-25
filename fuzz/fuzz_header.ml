open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Header.pp_error" (fun () ->
         Format.asprintf "%a" Nats.Header.pp_error error))

let test_of_list entries =
  match
    no_exception "Header.of_list" (fun () -> Nats.Header.of_list entries)
  with
  | Error error -> render_error error
  | Ok headers -> (
      ignore
        (no_exception "Header.pp" (fun () ->
             Format.asprintf "%a" Nats.Header.pp headers));
      let printed = Nats.Header.to_list headers in
      match
        no_exception "Header.of_list on printed headers" (fun () ->
            Nats.Header.of_list printed)
      with
      | Error error ->
          failf "valid headers became invalid after roundtrip: %a"
            Nats.Header.pp_error error
      | Ok headers' ->
          if not (Nats.Header.equal headers headers') then
            failf "headers changed after roundtrip")

let suite =
  ( "header",
    [
      test_case "of_list is crash-safe and roundtrips"
        [ list (pair bytes bytes) ]
        test_of_list;
    ] )
