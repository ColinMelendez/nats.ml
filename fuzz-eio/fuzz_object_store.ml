open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_object_config_error error =
  ignore
    (no_exception "Object_store.Error.pp_config" (fun () ->
         Format.asprintf "%a" Nats_eio.Object_store.Error.pp_config error))

let render_jetstream_config_error error =
  ignore
    (no_exception "Jetstream.Error.pp_config" (fun () ->
         Format.asprintf "%a" Nats_eio.Jetstream.Error.pp_config error))

let valid_replicas replicas =
  Int.compare replicas 1 >= 0 && Int.compare replicas 5 <= 0

let test_object_config replicas metadata =
  match
    no_exception "Object_store.Config.v" (fun () ->
        Nats_eio.Object_store.Config.v ~bucket:"assets" ~replicas ~metadata ())
  with
  | Error error -> (
      if valid_replicas replicas then (
        render_object_config_error error;
        failf "rejected valid replica count %d" replicas)
      else
        match error with
        | Nats_eio.Object_store.Config.Invalid_replicas value
          when Int.equal value replicas ->
            render_object_config_error error
        | _ ->
            render_object_config_error error;
            failf "wrong error for invalid replica count %d" replicas)
  | Ok config ->
      if not (valid_replicas replicas) then
        failf "accepted invalid replica count %d" replicas;
      if
        not (String.equal (Nats_eio.Object_store.Config.bucket config) "assets")
      then fail "configuration changed its bucket name"

let test_placement cluster tags =
  match
    no_exception "Stream.Config.Placement.v" (fun () ->
        Nats_eio.Jetstream.Stream.Config.Placement.v ?cluster ~tags ())
  with
  | Error error -> render_jetstream_config_error error
  | Ok placement ->
      ignore
        (no_exception "Placement.cluster" (fun () ->
             Nats_eio.Jetstream.Stream.Config.Placement.cluster placement));
      ignore
        (no_exception "Placement.tags" (fun () ->
             Nats_eio.Jetstream.Stream.Config.Placement.tags placement))

let test_stream_config replicas =
  let subject = Nats.Subject.Filter.literal "orders.>" in
  match
    no_exception "Stream.Config.v" (fun () ->
        Nats_eio.Jetstream.Stream.Config.v ~name:"ORDERS" ~subjects:[ subject ]
          ~replicas ())
  with
  | Error error -> (
      if valid_replicas replicas then (
        render_jetstream_config_error error;
        failf "rejected valid stream replica count %d" replicas)
      else
        match error with
        | Nats_eio.Jetstream.Error.Invalid_replicas value
          when Int.equal value replicas ->
            render_jetstream_config_error error
        | _ ->
            render_jetstream_config_error error;
            failf "wrong stream error for invalid replica count %d" replicas)
  | Ok config ->
      if
        Int.compare (Nats_eio.Jetstream.Stream.Config.replicas config) 1 < 0
        || Int.compare (Nats_eio.Jetstream.Stream.Config.replicas config) 5 > 0
      then failf "stream config stored invalid replicas for input %d" replicas

let suite =
  ( "object_store",
    [
      test_case "bucket config is crash-safe at replica boundaries"
        [ int; list (pair bytes bytes) ]
        test_object_config;
      test_case "placement validation is crash-safe"
        [ option bytes; list bytes ]
        test_placement;
      test_case "stream config is crash-safe at replica boundaries" [ int ]
        test_stream_config;
    ] )
