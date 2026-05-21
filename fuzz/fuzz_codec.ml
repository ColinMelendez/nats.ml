open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_error error =
  ignore
    (no_exception "Codec.pp_error" (fun () ->
         Format.asprintf "%a" Nats.Codec.pp_error error))

let render_operation operation =
  no_exception "Op.pp" (fun () -> Format.asprintf "%a" Nats.Op.pp operation)

let test_read input =
  let reader = Bytesrw.Bytes.Reader.of_string input in
  let result =
    no_exception "Codec.read" (fun () -> Nats.Codec.read ~eod:true reader)
  in
  match result with
  | Error error -> render_error error
  | Ok operation -> (
      let printed = render_operation operation in
      let encoded =
        match
          no_exception "Codec.encode" (fun () -> Nats.Codec.encode operation)
        with
        | Error error ->
            failf "Codec.encode rejected a decoded operation: %a"
              Nats.Codec.pp_error error
        | Ok wire -> wire
      in
      let reread =
        no_exception "Codec.read on encoded operation" (fun () ->
            Nats.Codec.read ~eod:true (Bytesrw.Bytes.Reader.of_string encoded))
      in
      match reread with
      | Error error ->
          failf "Codec.encode produced unreadable wire: %a" Nats.Codec.pp_error
            error
      | Ok operation' ->
          let printed' = render_operation operation' in
          if not (String.equal printed printed') then
            failf "codec operation changed after roundtrip: %S <> %S" printed
              printed')

let suite =
  ( "codec",
    [
      test_case "read is crash-safe and decoded operations roundtrip" [ bytes ]
        test_read;
    ] )
