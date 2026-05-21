let () =
  Alcobar.run "nats" [ Fuzz_packet.suite; Fuzz_codec.suite; Fuzz_client.suite ]
