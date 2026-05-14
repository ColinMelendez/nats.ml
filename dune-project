(lang dune 3.22)
(name nats)

(package
 (name nats)
 (synopsis "A modern NATS client SDK for OCaml")
 (description
  "An I/O-neutral NATS protocol core and an Eio client facade, with Core NATS, JetStream, Key-Value, Object Store, and Services planned as layered capabilities.")
 (license ISC)
 (authors "Colin <cwrmcolin@gmail.com>")
 (maintainers "Colin <cwrmcolin@gmail.com>")
 (depends
  (ocaml (>= 5.2))
  bytesrw
  jsont
  mtime
  (windtrap :with-test)))
