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
  (ocaml (>= 5.5))
  bytesrw
  jsont
  mtime
  (alcobar :with-test)
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
  (ocaml (>= 5.5))
  nats
  bytesrw
  jsont
  eio
  cstruct
  tls-eio
  mtime
  ptime
  base64
  digestif
  (alcobar :with-test)
  (eio_main :with-test)
  (windtrap :with-test)))

(package
 (name nats-eio-system)
 (synopsis "Privileged NATS system-account administration for Eio")
 (description
  "Server and account monitoring, operational control, and system-event subscriptions over the privileged NATS system account API.")
 (license ISC)
 (authors "Colin <cwrmcolin@gmail.com>")
 (maintainers "Colin <cwrmcolin@gmail.com>")
 (depends
  (ocaml (>= 5.5))
  nats
  nats-eio
  bytesrw
  jsont
  eio
  mtime
  (eio_main :with-test)
  (windtrap :with-test)))

(package
 (name nats-eio-opentelemetry)
 (synopsis "OpenTelemetry observations for the NATS Eio client")
 (description
  "An optional OpenTelemetry bridge for NATS Eio connection statistics and lifecycle events.")
 (license ISC)
 (authors "Colin <cwrmcolin@gmail.com>")
 (maintainers "Colin <cwrmcolin@gmail.com>")
 (depends
  (ocaml (>= 5.5))
  nats-eio
  eio
  mtime
  opentelemetry
  (alcobar :with-test)
  (eio_main :with-test)
  (windtrap :with-test)))
