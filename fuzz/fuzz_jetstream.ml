open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_config_error error =
  ignore
    (no_exception "JetStream.Error.pp_config" (fun () ->
         Format.asprintf "%a" Nats_eio.Jetstream.Error.pp_config error))

let test_input input =
  let value = input in
  let transform =
    no_exception "Stream.Config.Transform.v" (fun () ->
        Nats_eio.Jetstream.Stream.Config.Transform.v ~destination:value ())
  in
  (match transform with Ok _ -> () | Error error -> render_config_error error);
  let source =
    no_exception "Stream.Config.Source.v" (fun () ->
        Nats_eio.Jetstream.Stream.Config.Source.v ~name:value ())
  in
  match source with Ok _ -> () | Error error -> render_config_error error

let suite =
  ( "jetstream",
    [
      test_case "stream config constructors are crash-safe" [ bytes ] test_input;
    ] )
