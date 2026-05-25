open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Endpoint.pp_error" (fun () ->
         Format.asprintf "%a" Nats.Endpoint.pp_error error))

let render_endpoint endpoint =
  ignore
    (no_exception "Endpoint.pp" (fun () ->
         Format.asprintf "%a" Nats.Endpoint.pp endpoint));
  let wire =
    no_exception "Endpoint.to_string" (fun () ->
        Nats.Endpoint.to_string endpoint)
  in
  match
    no_exception "Endpoint.of_string on printed endpoint" (fun () ->
        Nats.Endpoint.of_string wire)
  with
  | Error error ->
      failf "printed endpoint became invalid: %a" Nats.Endpoint.pp_error error
  | Ok endpoint' ->
      if not (Nats.Endpoint.equal endpoint endpoint') then
        failf "endpoint changed after roundtrip: %S <> %S" wire
          (Nats.Endpoint.to_string endpoint')

let test_endpoint input =
  match
    no_exception "Endpoint.of_string" (fun () -> Nats.Endpoint.of_string input)
  with
  | Error error -> render_error error
  | Ok endpoint -> render_endpoint endpoint

let test_connect_url input =
  match
    no_exception "Endpoint.of_connect_url" (fun () ->
        Nats.Endpoint.of_connect_url input)
  with
  | Error error -> render_error error
  | Ok endpoint -> render_endpoint endpoint

let suite =
  ( "endpoint",
    [
      test_case "of_string is crash-safe and roundtrips" [ bytes ] test_endpoint;
      test_case "of_connect_url is crash-safe and roundtrips" [ bytes ]
        test_connect_url;
    ] )
