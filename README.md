# NATS for OCaml

This repository is building a modern NATS client SDK for OCaml.

The implementation is organized around an I/O-neutral Core NATS protocol
state machine and runtime adapters. The first adapter will target Eio. The
long-term feature set includes Core NATS, JetStream, Key-Value, Object Store,
and Services; NATS Streaming/STAN is intentionally out of scope.

The research and architecture proposal is in
[`docs/design.md`](docs/design.md). The phased implementation roadmap is in
[`plan.md`](plan.md).

## Development

Enter the development shell and run the build or tests with Dune:

```sh
nix develop
dune build
dune runtest
```

The real-server tests use a smaller, separate shell so integration-only
tooling does not become part of the normal OCaml development environment:

```sh
nix develop .#integration
```

The current pure-core tests are portable. An opt-in Core NATS acceptance run
against a pinned `nats-server` Docker image is available with:

```sh
./scripts/start-colima.sh
./scripts/runtest-server.sh
./scripts/runtest-reconnect.sh
./scripts/runtest-cluster.sh
./scripts/runtest-jetstream-cluster.sh
./scripts/runtest-tls.sh
./scripts/runtest-lameduck.sh
./scripts/runtest-interop.sh
./scripts/runtest-interop-auth-matrix.sh
./scripts/runtest-interop-auth-negative-matrix.sh
./scripts/runtest-interop-service.sh
./scripts/runtest-interop-jetstream.sh
./scripts/runtest-interop-jetstream-matrix.sh
./scripts/runtest-interop-key-value.sh
./scripts/runtest-interop-key-value-matrix.sh
./scripts/runtest-interop-jetstream-reconnect.sh
./scripts/runtest-interop-auth-jetstream-reconnect-matrix.sh
./scripts/runtest-interop-jetstream-cluster.sh
./scripts/runtest-interop-jetstream-cluster-matrix.sh
./scripts/runtest-interop-auth-jetstream-cluster.sh
./scripts/runtest-interop-auth-jetstream-cluster-matrix.sh
NATS_TEST_JS_CLUSTER_FAILURE_MODE=leader ./scripts/runtest-interop-jetstream-cluster.sh
NATS_TEST_INTEROP_JETSTREAM_MODE=push ./scripts/runtest-interop-jetstream.sh
NATS_TEST_INTEROP_JETSTREAM_MODE=ordered ./scripts/runtest-interop-jetstream.sh
NATS_INTEROP_JETSTREAM_MATRIX_SCENARIOS=ordered ./scripts/runtest-interop-jetstream-matrix.sh
NATS_TEST_TLS=1 ./scripts/runtest-interop.sh
./scripts/runtest-interop-matrix.sh
./scripts/runtest-server-matrix.sh
./scripts/runtest-interop-reconnect.sh
NATS_TEST_TLS=1 ./scripts/runtest-interop-reconnect.sh
```

The script requires Docker and is intentionally outside `dune runtest`; Docker
is the one integration dependency supplied by the host rather than Nix. The
Colima helper creates or starts the `default` profile with a 10 GiB disk by
default; set `COLIMA_DISK_GIB` only when a larger disk is actually needed. Set
`NATS_SERVER_IMAGE` to try another server image. Cluster, TLS, and reconnect
scenarios are split into focused runners; the server reconnect runner starts
two single-node servers, kills the active one, checks pending-request failure,
checks Core subscription recovery, and checks Service endpoint and monitoring
recovery through the same failover.
The cluster runner starts a three-node route mesh, connects only to the seed,
checks the advertised client URLs, kills the seed, verifies recovery to a
discovered peer, then kills that active peer and verifies recovery to the last
node with subscription replay.
The JetStream cluster runner starts a separate three-node full route mesh with
file-backed, three-replica JetStream state, verifies durable Push delivery and
acknowledgement before killing the seed, then verifies reconnect, replicated
stream/consumer state, and a second publish/delivery on a surviving node. The
cross-SDK Ordered reconnect runner adds elected stream-leader targeting and
durable seed restart recovery, and checks both the OCaml client and the
official Go peer. Its authenticated companion covers NKey, JWT, NKey-over-TLS,
JWT-over-TLS, and mTLS, with seed failure, elected-leader failure, and durable
seed restart across `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`.
The lame-duck runner starts two fresh server containers, signals each live
server through its container, and checks the dynamic `INFO` flag, typed
`Lame_duck_mode` event, and continued use of each connection.
The TLS runner generates an ephemeral CA and hostname-checked server
certificate, then verifies a real TLS connection. To exercise token
authentication, set `NATS_TEST_TOKEN`; to exercise username/password
authentication, set both `NATS_TEST_USER` and `NATS_TEST_PASS` to non-empty
ephemeral credentials before running the script. The credentials must use only
ASCII letters, digits, underscores, and hyphens; choose exactly one
authentication mode.
The dedicated auth interop matrix generates ephemeral NKey/JWT material with
the Nix-provided `nsc`, generates a short-lived certificate authority and
client certificates with OpenSSL, and checks NKey, JWT, NKey-over-TLS,
JWT-over-TLS, and mTLS against `nats:2.10.22`, `nats:2.12.15`, and
`nats:2.14.5`. Its companion negative matrix checks rejected NKey/JWT
signatures and missing mTLS client certificates from both the OCaml client and
the official Go peer.
The harness then requires anonymous connection rejection as well as successful
authenticated traffic. To enable the JetStream acceptance slice, also set
`NATS_TEST_JETSTREAM=1`; this starts the server with JetStream enabled and
exercises stream management, publish acknowledgements, duplicate message ids,
and cleanup. The server runner also checks request timeout/cancellation
cleanup, auto-unsubscribe limits, bounded subscription slow-consumer behavior,
subscription drain, connection drain, and parent-switch cleanup.
The server runner also executes a Services acceptance executable covering
endpoint and group registration, `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS`
discovery, successful and service-error replies, handler failure isolation,
statistics, service-local drain, and parent-connection usability.
It also checks two live service instances sharing a queue group for one-reply
per-request routing and aggregate worker statistics.
With `NATS_TEST_JETSTREAM=1`, it also runs a separate consumer acceptance
executable covering durable and owned Push consumers, Ordered filtering, and
Ordered consumer deletion/recreation against the live server, followed by a
Key-Value acceptance executable covering bucket status, direct reads, CAS
mutations, history, tombstones, filtered keys, and a live watch, followed by
an Object Store acceptance executable covering chunked content, metadata,
links, listing, deletion, watches, sealing, and cleanup.
The server matrix runner repeats the server, cluster, and lame-duck runners for
`nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5` (nine sequential cases by
default); set `NATS_SERVER_IMAGES` or `NATS_SERVER_MATRIX_SCENARIOS` to select
another bounded version sweep. Token, username/password, and JetStream
variables apply only to its `server` scenario; cluster and lame-duck cases
deliberately clear them and remain anonymous. This is a version sweep of the
existing live-server contracts, not a claim of full NATS conformance or broad
failure injection.
Within each `server` case, lifecycle acceptance runs two fresh slow-consumer
cycles and two fresh subscription/connection drain cycles before the final
parent-switch check. Each lame-duck case also runs two fresh-container cycles.
The JetStream test uses a per-run stream name; set
`NATS_TEST_JETSTREAM_RUN_ID` only when a stable, safe identifier is useful for
debugging. The interoperability runner starts the same pinned server image,
builds an official Go `nats.go` peer through the separate Nix integration
shell, and checks Core pub/sub, repeated headers, bidirectional request/reply,
no-responders, and clean drain/close. It supports the same anonymous, token,
and username/password modes.
The separate JetStream interop runner is a single-server durable-pull slice: an
official Go `nats.go` peer creates a unique stream and durable pull consumers,
then the OCaml client and Go peer exchange, acknowledge, and deduplicate
JetStream messages. It supports the same anonymous, token, username/password,
and server-required TLS modes as the Core runner; set `NATS_SERVER_IMAGE` to
try another pinned release. The JetStream interop matrix runner defaults to
pull and Push in anonymous plaintext/TLS modes across the three pinned
releases; set `NATS_INTEROP_JETSTREAM_MATRIX_MODES` to a comma-separated
subset of `anonymous`, `anonymous-tls`, `token`, `token-tls`, `user-pass`, and
`user-pass-tls` for a broader sweep. The full six-mode sweep has passed for
both pull and Push on each pinned release. Ordered is an opt-in matrix scenario:
set `NATS_INTEROP_JETSTREAM_MATRIX_SCENARIOS=ordered` (or include `ordered` in
the comma-separated scenario list). Its full six-mode sweep also passes on all
three pinned releases. The separate Ordered reconnect cluster runner starts a
three-node file-backed JetStream cluster and an official Go `nats.go` peer.
Its default `seed` mode kills the initial node after a filtered sequence
baseline, then verifies both clients reconnect to a discovered peer, recreate
their ephemeral Ordered sessions, and complete cleanup. Set
`NATS_TEST_JS_CLUSTER_FAILURE_MODE=leader` to record the current
JetStream stream leader, route both clients to the two survivors, kill that
elected leader after the baseline, wait for a replacement, and verify Ordered
progress on both clients. Because Ordered consumers are intentionally
one-replica, a stream-leader failure can either preserve a consumer identity
and continue at consumer sequence 3 or recreate that consumer and resume at
sequence 1 if its consumer leader was also the failed node. The same is true
of seed-node failure: the runner checks the corresponding exact stream,
consumer, and delivery metadata in either case. Set
`NATS_TEST_JS_CLUSTER_FAILURE_MODE=restart` to use run-unique Docker volumes,
wait until both clients have failed over, restart the seed container, and
require all three stream replicas to be current before post-restart delivery.
The companion `./scripts/runtest-interop-jetstream-cluster-matrix.sh` runs the
seed, leader, and restart modes over the three pinned server images by default;
set
`NATS_SERVER_IMAGES` or `NATS_INTEROP_JETSTREAM_CLUSTER_MATRIX_MODES` to select
a bounded subset. Its nine-case anonymous plaintext sweep passes on
`nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`. The authenticated
companion `./scripts/runtest-interop-auth-jetstream-cluster-matrix.sh` runs
the five NKey/JWT/TLS modes over seed, leader, and restart failures: all 45
cross-SDK cases pass across the same three pinned releases. Broader
cluster-failure matrices remain separate work. The base cluster runner also
accepts `NATS_TEST_TOKEN` or paired `NATS_TEST_USER`/`NATS_TEST_PASS` values,
with `NATS_TEST_TLS=1` for their server-required TLS variants; the companion
matrix intentionally focuses on generated NKey/JWT/mTLS material. The optional
`NATS_TEST_TOKEN` or paired
`NATS_TEST_USER`/`NATS_TEST_PASS` values only replace the built-in credentials
for their selected modes; they do not select modes. Set
`NATS_TEST_INTEROP_JETSTREAM_MODE=push` to run the separate durable Push slice:
the Go and OCaml clients bind opposite durable push consumers, exchange one
message each way, and synchronously acknowledge the deliveries. Anonymous
plaintext and TLS Push pass on all three pinned releases; token and
username/password plaintext and TLS Push also pass on all three releases.
Set `NATS_TEST_INTEROP_JETSTREAM_MODE=ordered` to run the cross-SDK Ordered
slice: two independent Ordered sessions consume a filtered stream with
interleaved non-matching messages, verify AckNone configuration and exact
stream/consumer sequences, and coordinate cleanup. The anonymous, token, and
username/password plaintext/TLS Ordered modes pass on all three pinned
releases.
The dedicated Key-Value interop runner uses the official Go `nats.go`
`jetstream.KeyValue` API as the peer. It alternates bucket creation, revisioned
updates, stale CAS validation, tombstones, watches, purge markers, and cleanup
between Go and OCaml. Its matrix covers the same six anonymous/authenticated
plaintext/TLS modes across all three pinned releases; all 18 baseline cases
pass. Cluster/reconnect, NKey/JWT, mTLS, and Object Store cross-SDK coverage
remain separate acceptance work; the dedicated auth matrix covers those
authentication and TLS contracts for Core traffic.
The separate JetStream reconnect runner uses a file-backed stream and durable
Push consumers on one persistent server container, kills and restarts that
container, and verifies both the OCaml and Go Push legs recover and exchange
new messages without recreating their sessions. Its authenticated companion
`./scripts/runtest-interop-auth-jetstream-reconnect-matrix.sh` covers NKey, JWT,
NKey-over-TLS, JWT-over-TLS, and mTLS across all three pinned releases; all 15
restart cases pass. The base runner also accepts token and username/password
credentials, including their server-required TLS variants, for focused
compatibility checks. Ordered reconnect has a separate three-node failover
runner as described above.
The Core interop matrix runner (`./scripts/runtest-interop-matrix.sh`) is a
bounded Core gate: by
default it spans the established `nats:2.10.22` floor, `nats:2.12.15`, and the
current `nats:2.14.5` release, and runs Core traffic, repeated plaintext
reconnect, single-server TLS Core traffic, and repeated TLS reconnect for each
image. The twelve cases run
sequentially; set `NATS_SERVER_IMAGES` to a comma-separated image list or
`NATS_INTEROP_MATRIX_SCENARIOS` to a comma-separated subset of `core`,
`reconnect`, `tls-core`, and `tls-reconnect`. The matrix defaults to anonymous
authentication; set the token or username/password variables described above
to repeat the selected matrix with that authentication mode. It does not
claim full server conformance: broader failure-injection matrices, KV
cluster/reconnect, Object Store cross-SDK behavior, and
the wider Services cross-SDK/version matrix remain separate acceptance work.
The cross-SDK reconnect runner
starts three independent NATS servers, kills the first and then the second
after flushed exchanges, and checks that the Go and OCaml clients recover twice,
replay their subscriptions, and exchange messages after each failover. It
accepts the same anonymous, token, and username/password modes; set
`NATS_SERVER_IMAGE` to select the server image for this scenario. Set
`NATS_TEST_TLS=1` to generate an ephemeral CA and run the same two-failover
scenario through the server-required TLS upgrade; the CA and hostname policy
are supplied to both SDKs.
The Service interop runner starts the official Go `nats.go` `micro` peer beside
the OCaml client and checks bidirectional endpoint requests, response headers,
service-error headers, named INFO/STATS discovery, queue and metadata
declarations, exact endpoint counters, and a request/reply completion barrier.
It supports anonymous, token, username/password, and server-required TLS modes
against the pinned `nats:2.10.22` image. The wider server-version, NKey/JWT,
mTLS, reconnect, and feature-family cross-SDK matrices remain separate work.

The first JetStream layer is available through `Nats_eio.Jetstream`: typed
stream and consumer configuration and management, including consumer metadata,
sampling, push rate limits, replica inheritance, singular and multi-subject
filters, redelivery backoff schedules, typed pause/resume control, and
read-modify-write updates that preserve unknown server fields, direct
stored-message
reads, one-shot fetch, durable publish and message acknowledgements, and
switch-owned Eio pull/push sessions over ordinary Core NATS request/reply. Push
sessions consume idle heartbeats, answer flow-control requests including
stalled-heartbeat replies, and restore replayable delivery subscriptions across
reconnects. Ordered sessions use client-managed ephemeral pull consumers,
validate consecutive consumer sequences, and resume from the next stream
sequence after recovery. `Nats_eio.Object_store` now provides validated
buckets, direct metadata, incremental Bytesrw put/get, digest verification,
metadata updates and links, snapshot/live watches, listing, deletion,
replacement cleanup, sealing, and bucket policy updates for replicas, placement,
compression, and stream metadata. `Nats_eio.Key_value` provides revisioned
values, compare-and-set mutations, finite scans, history, and cancellable
watches, while the dedicated Key-Value interop runner cross-checks revision,
CAS, tombstone, watch, purge, and cleanup behavior against the official Go
`nats.go` API. `Nats_eio.Service` provides typed endpoint workers, queue groups,
`$SRV.*` monitoring and fan-out discovery, request/service-error replies,
statistics, replayable subscriptions, and service-local draining. The
dedicated JetStream cluster slice is intentionally narrower than a full
failure matrix; advanced cluster scenarios and cross-SDK interoperability
coverage remain in the final acceptance phase. The Ordered reconnect cluster
slice passes anonymous plaintext seed-node failure, JetStream-leader failure,
and durable seed restart on all three pinned server releases.

The project uses Dune package management. No compatibility layer for NATS
Streaming is planned.
