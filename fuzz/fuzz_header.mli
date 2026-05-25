(** Fuzz tests for {!Nats.Header}. *)

val suite : string * Alcobar.test_case list
(** Header construction and round-trip fuzz tests. *)
