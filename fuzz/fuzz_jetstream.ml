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
  (match source with Ok _ -> () | Error error -> render_config_error error);
  let consumer_limits =
    no_exception "Stream.Config.Consumer_limits.v" (fun () ->
        Nats_eio.Jetstream.Stream.Config.Consumer_limits.v
          ~inactive_threshold:(Mtime.Span.of_uint64_ns 100_000_000L)
          ~max_ack_pending:(String.length value) ())
  in
  let config =
    match consumer_limits with
    | Error error -> Error error
    | Ok consumer_limits ->
        no_exception "Stream.Config.v policy controls" (fun () ->
            Nats_eio.Jetstream.Stream.Config.v ~name:"FUZZ"
              ~subjects:[ Nats.Subject.Filter.literal "fuzz.>" ]
              ~discard:Nats_eio.Jetstream.Stream.Config.New
              ~max_msgs_per_subject:1L ~max_consumers:(String.length value)
              ~discard_new_per_subject:true
              ~no_ack:(String.length value mod 2 = 0)
              ~duplicate_window:(Mtime.Span.of_uint64_ns 100_000_000L)
              ~first_sequence:(Int64.of_int (String.length value))
              ~consumer_limits ())
  in
  match config with
  | Ok config ->
      List.iter
        (fun result ->
          match result with
          | Ok _ -> ()
          | Error error -> render_config_error error)
        [
          Nats_eio.Jetstream.Stream.Config.with_max_consumers config None;
          Nats_eio.Jetstream.Stream.Config.with_discard_new_per_subject config
            false;
          Nats_eio.Jetstream.Stream.Config.with_no_ack config false;
          Nats_eio.Jetstream.Stream.Config.with_duplicate_window config None;
          Nats_eio.Jetstream.Stream.Config.with_first_sequence config None;
          Nats_eio.Jetstream.Stream.Config.with_consumer_limits config None;
        ]
  | Error error -> render_config_error error

let suite =
  ( "jetstream",
    [
      test_case "stream config constructors are crash-safe" [ bytes ] test_input;
    ] )
