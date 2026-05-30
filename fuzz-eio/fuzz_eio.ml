let () = Alcobar.run "nats-eio-fuzz" [ Fuzz_object_store.suite; Fuzz_service.suite ]
