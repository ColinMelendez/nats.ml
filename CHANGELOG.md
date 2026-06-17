# Changelog

## Unreleased

Initial release preparation.

- Provide an I/O-neutral Core NATS protocol library and an Eio TCP/TLS client.
- Cover Core NATS, authentication, discovery, reconnect, drain, statistics,
  and lifecycle observations.
- Cover JetStream publishing, consumption, resource administration,
  Key-Value, Object Store, and Services.
- Provide optional system-account administration and OpenTelemetry packages.
- Validate behavior with pure protocol tests, fuzz/property tests, live NATS
  servers, cluster failure scenarios, and an official Go SDK peer.

The supported server matrix and feature-level compatibility policy are in
[`docs/support.md`](docs/support.md).
