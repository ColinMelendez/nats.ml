(lang dune 3.22)
(name nats)
(generate_opam_files true)

(package
 (name nats)
 (synopsis "I/O-neutral NATS protocol core for OCaml")
 (description
  "I/O-neutral Core NATS protocol types, codecs, parsing, and state-machine primitives for OCaml.")
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
 (synopsis "Eio NATS client SDK for OCaml")
 (description
  "A structured-concurrency Eio TCP/TLS client with Core NATS, JetStream, Key-Value, Object Store, and Services APIs.")
 (license ISC)
 (authors "Colin <cwrmcolin@gmail.com>")
 (maintainers "Colin <cwrmcolin@gmail.com>")
 (depends
  (ocaml (>= 5.5))
  (nats (= :version))
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
  (nats (= :version))
  (nats-eio (= :version))
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
  (nats-eio (= :version))
  eio
  mtime
  opentelemetry
  (alcobar :with-test)
  (eio_main :with-test)
  (windtrap :with-test)))
