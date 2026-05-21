(** Fuzz tests for {!Nats.Packet}. *)

val suite : string * Alcobar.test_case list
(** Packet framing fuzz tests. *)
