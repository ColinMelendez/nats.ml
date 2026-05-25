let () =
  Alcobar.run "nats"
    [
      Fuzz_packet.suite;
      Fuzz_codec.suite;
      Fuzz_client.suite;
      Fuzz_subject.suite;
      Fuzz_info.suite;
      Fuzz_endpoint.suite;
      Fuzz_header.suite;
    ]
