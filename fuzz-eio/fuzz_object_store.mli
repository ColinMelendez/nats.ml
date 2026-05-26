(** Fuzz tests for {!Nats_eio.Object_store} and its bucket configuration. *)

val suite : string * Alcobar.test_case list
(** Object Store and JetStream configuration fuzz tests. *)
