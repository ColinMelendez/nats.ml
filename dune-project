(lang dune 3.22)
(name nats)

(package
 (name nats)
 (synopsis "A modern NATS client SDK for OCaml")
 (description
  "An I/O-neutral NATS protocol core, with Core NATS, JetStream, Key-Value, Object Store, Services, and runtime adapters planned as layered capabilities.")
 (license ISC)
 (authors "Colin <cwrmcolin@gmail.com>")
 (maintainers "Colin <cwrmcolin@gmail.com>")
 (depends
  (ocaml (>= 5.2))
  bytesrw
  jsont
  mtime
  (windtrap :with-test)))

(package
 (name nats-eio)
 (synopsis "Eio connection facade for the NATS client SDK")
 (description
  "A structured-concurrency Eio transport and direct-style facade over the nats protocol core.")
 (license ISC)
 (authors "Colin <cwrmcolin@gmail.com>")
 (maintainers "Colin <cwrmcolin@gmail.com>")
 (depends
  (ocaml (>= 5.2))
  nats
  bytesrw
  eio
  cstruct
  tls-eio
  mtime
  (eio_main :with-test)
  (windtrap :with-test)))
