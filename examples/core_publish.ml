let or_fail pp = function
  | Ok value -> value
  | Error error -> failwith (Format.asprintf "%a" pp error)

let run env =
  Eio.Switch.run @@ fun sw ->
  let endpoint =
    or_fail Nats.Endpoint.pp_error
      (Nats.Endpoint.of_string "nats://127.0.0.1:4222")
  in
  let connection =
    or_fail Nats_eio.Error.pp
      (Nats_eio.Connection.connect ~sw ~net:(Eio.Stdenv.net env)
         ~clock:(Eio.Stdenv.mono_clock env)
         [ endpoint ])
  in
  let subject = Nats.Subject.literal "hello" in
  or_fail Nats_eio.Error.pp
    (Nats_eio.Connection.publish connection subject "world");
  or_fail Nats_eio.Error.pp (Nats_eio.Connection.flush connection);
  or_fail Nats_eio.Error.pp (Nats_eio.Connection.close connection)

let () = Eio_main.run run
