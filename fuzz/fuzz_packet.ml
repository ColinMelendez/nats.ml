open Alcobar

let no_exception label f =
  match f () with
  | value -> value
  | exception exception_value ->
      failf "%s raised %s" label (Printexc.to_string exception_value)

let render_packet_error error =
  ignore
    (no_exception "Packet.pp_error" (fun () ->
         Format.asprintf "%a" Nats.Packet.pp_error error))

let test_read input =
  let reader = Bytesrw.Bytes.Reader.of_string input in
  let result =
    no_exception "Packet.read" (fun () -> Nats.Packet.read ~eod:true reader)
  in
  match result with
  | Error error -> render_packet_error error
  | Ok packet -> (
      ignore
        (no_exception "Packet.pp" (fun () ->
             Format.asprintf "%a" Nats.Packet.pp packet));
      let wire =
        no_exception "Packet.to_string" (fun () -> Nats.Packet.to_string packet)
      in
      let reread =
        no_exception "Packet.read on encoded packet" (fun () ->
            Nats.Packet.read ~eod:true (Bytesrw.Bytes.Reader.of_string wire))
      in
      match reread with
      | Error error ->
          failf "Packet.to_string produced unreadable wire: %a"
            Nats.Packet.pp_error error
      | Ok packet' ->
          let wire' =
            no_exception "Packet.to_string on reread packet" (fun () ->
                Nats.Packet.to_string packet')
          in
          if not (String.equal wire wire') then
            failf "packet wire changed after roundtrip: %S <> %S" wire wire')

let suite =
  ( "packet",
    [
      test_case "read is crash-safe and packet wire is stable" [ bytes ]
        test_read;
    ] )
