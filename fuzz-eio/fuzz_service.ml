open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Service.Error.pp" (fun () ->
         Format.asprintf "%a" Nats_eio.Service.Error.pp error))

let test_config name version metadata =
  match
    no_exception "Service.Config.v" (fun () ->
        Nats_eio.Service.Config.v ~name ~version ~metadata ())
  with
  | Error error -> render_error error
  | Ok config ->
      ignore
        (no_exception "Service.Config.name" (fun () ->
             Nats_eio.Service.Config.name config));
      ignore
        (no_exception "Service.Config.version" (fun () ->
             Nats_eio.Service.Config.version config));
      ignore
        (no_exception "Service.Config.metadata" (fun () ->
             Nats_eio.Service.Config.metadata config))

let test_endpoint name metadata =
  match
    no_exception "Service.Endpoint.v" (fun () ->
        Nats_eio.Service.Endpoint.v ~name ?metadata (fun _ -> Ok ()))
  with
  | Error error -> render_error error
  | Ok endpoint ->
      ignore
        (no_exception "Service.Endpoint.name" (fun () ->
             Nats_eio.Service.Endpoint.name endpoint));
      ignore
        (no_exception "Service.Endpoint.metadata" (fun () ->
             Nats_eio.Service.Endpoint.metadata endpoint))

let suite =
  ( "service",
    [
      test_case "config validation is crash-safe"
        [ bytes; bytes; list (pair bytes bytes) ]
        test_config;
      test_case "endpoint validation is crash-safe"
        [ bytes; option (list (pair bytes bytes)) ]
        test_endpoint;
    ] )
