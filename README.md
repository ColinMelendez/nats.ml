# NATS for OCaml

This repository is building a modern NATS client SDK for OCaml.

The implementation is organized around an I/O-neutral Core NATS protocol
state machine and runtime adapters. The first adapter will target Eio. The
long-term feature set includes Core NATS, JetStream, Key-Value, Object Store,
and Services; NATS Streaming/STAN is intentionally out of scope.

The research and architecture proposal is in
[`docs/design.md`](docs/design.md). The phased implementation roadmap is in
[`plan.md`](plan.md). API stability records are kept in
[`docs/core-api-review.md`](docs/core-api-review.md),
[`docs/jetstream-api-review.md`](docs/jetstream-api-review.md), and
[`docs/durable-feature-api-review.md`](docs/durable-feature-api-review.md).
The current observability boundary is described in
[`docs/observability.md`](docs/observability.md).
The optional `nats-eio-opentelemetry` package bridges that boundary to
OpenTelemetry without adding an observability dependency to `nats-eio`.

## Capability comparisons

The comparison target is the user-visible capability of the official Go SDK,
not a method-for-method translation of its API. Eio uses direct iteration,
switch-owned lifetimes, and structured results where Go uses callbacks,
channels, and mutable handles.

| Comparison point | Current position |
| --- | --- |
| Core connection conveniences | Core protocol, authentication, TLS, discovery, reconnect, drain, lifecycle events, and race-safe cumulative connection statistics are covered. Go-specific custom dialers, proxy headers, stale-connection tuning, richer introspection, and dynamic callback hooks are not currently exposed. |
| Observability | `Connection.stats` and the bounded lifecycle event stream are dependency-free; the optional `nats-eio-opentelemetry` package exports cumulative metrics and payload-free lifecycle spans with explicit event-stream ownership. |
| JetStream resource administration | Account information plus stream and consumer administration/configuration are covered, including placement, persistence mode, message counters, and the material pinned Go SDK fields. |
| Server-wide administration | Monitoring and selected controls are covered by the optional `nats-eio-system` package: privileged server/account queries, fan-out collection, reload, client kick/LDM, and system events. Claims, resolver, and user-management operations remain separate work. |
| JetStream consumption | Pull, push, ordered, fetch-by-bytes, no-wait fetch, flow control, priority groups, and bounded continuous consumption are covered. `Messages`/`Consume` threshold callbacks are represented by Eio backpressure and result ownership. |
| Key-Value watches | Revision-resumable `Watch` and distinct `Ordered_watch` modes are covered. Ordinary watches retain their weaker recovery contract; ordered watches validate consumer sequence continuity and recover at the next stream revision. |
| Object Store | Streaming data access, links, metadata, watches, sealing, bucket managers/listers, and file helpers are covered; the anonymous replicated cluster slice also covers seed loss, elected-leader loss, and durable restart. |

### JetStream administration versus server administration

`Nats_eio.Jetstream` administers JetStream resources through the `$JS.API.*`
request/reply namespace. It covers account/domain usage, stream and consumer
CRUD, listing, message operations, and configuration projection.

General server-wide administration is a separate capability and package. NATS
exposes privileged system-account services under subjects such as
`$SYS.REQ.SERVER.<server-id>.*` and `$SYS.REQ.ACCOUNT.<account-id>.*`; these
provide monitoring and operational control across servers and accounts. They
require system-account permissions and have version-specific JSON schemas.
They are distinct from JetStream administration and from the ordinary
application client surface. The `nats-eio-system` package provides this
capability without adding a privileged dependency to ordinary `nats-eio`
applications. See the [NATS system-account reference](https://github.com/nats-io/nats.docs/blob/master/running-a-nats-service/nats_admin/jwt.md).

Open a `Nats_eio_system` handle over an existing connection to use typed target
and selector construction, endpoint-specific monitoring options, monitoring
endpoints (`VARZ`, `STATZ`, `CONNZ`, `SUBSZ`, `ACCOUNTZ`, `JSZ`, and the other
server monitor services), reload, client kick/LDM controls, and classified
`$SYS` events. Responses retain their complete `Jsont.json` payload so callers
can consume fields introduced by newer servers. The package intentionally does
not implement operator JWT claims mutation, resolver management, user
information, or alternate transports; those are separate authorization and
product decisions.

### Ordered recovery for Key-Value watches

The current `Key_value.Watch` is a normal JetStream consumer with revision
resumption and keeps that contract deliberately lightweight. Callers that need
the stronger ordered-consumer invariant use the separate
`Key_value.Ordered_watch` module. It emits `Initial_done` after the selected
retained snapshot, then delivers typed bucket entries while preserving the
same filtering, metadata-only, delete-filtering, timeout, and switch-lifetime
options.

`Ordered_watch` is built on the lower-level ordered consumer. It validates
consumer sequence continuity, detects missing heartbeats, consumer deletion,
non-replayed disconnects, and delivery gaps, then recreates its ephemeral
no-ack memory consumer at the next expected stream revision. Filters,
headers-only mode, heartbeat/replay settings, metadata, name prefixes, and
reset limits are retained across generations. `max_reset_attempts` bounds
recovery; the default permits the underlying ordered session's unlimited
attempt policy.

This is a distinct mode rather than an undocumented strengthening of `Watch`:
ordered recovery changes duplicate, loss, and error semantics. The local mock
suite covers retained/live and empty snapshots, metadata-only delivery, and
gap recovery with replay. Live-server, cluster-failure, and Go-peer coverage
remain part of the production-readiness acceptance program.

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
./scripts/runtest-system.sh
./scripts/runtest-system-cluster.sh
./scripts/runtest-system-matrix.sh
./scripts/runtest-reconnect.sh
./scripts/runtest-cluster.sh
./scripts/runtest-jetstream-cluster.sh
./scripts/runtest-tls.sh
./scripts/runtest-lameduck.sh
./scripts/runtest-interop.sh
./scripts/runtest-interop-auth-matrix.sh
./scripts/runtest-interop-auth-negative-matrix.sh
./scripts/runtest-interop-service.sh
./scripts/runtest-interop-service-matrix.sh
./scripts/runtest-interop-service-reconnect.sh
./scripts/runtest-interop-service-failure.sh
./scripts/runtest-interop-service-parent-close.sh
./scripts/runtest-interop-jetstream.sh
./scripts/runtest-interop-jetstream-matrix.sh
./scripts/runtest-interop-key-value.sh
./scripts/runtest-interop-key-value-matrix.sh
./scripts/runtest-interop-object-store.sh
./scripts/runtest-interop-jetstream-reconnect.sh
./scripts/runtest-interop-auth-jetstream-reconnect-matrix.sh
./scripts/runtest-interop-jetstream-cluster.sh
./scripts/runtest-interop-key-value-cluster.sh
./scripts/runtest-interop-object-store-cluster.sh
./scripts/runtest-interop-object-store-cluster-matrix.sh
./scripts/runtest-interop-auth-object-store-cluster.sh
./scripts/runtest-interop-auth-object-store-cluster-matrix.sh
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
NATS_TEST_TLS=1 ./scripts/runtest-interop-service-reconnect.sh
NATS_TEST_TLS=1 ./scripts/runtest-interop-service-failure.sh
NATS_TEST_TLS=1 ./scripts/runtest-interop-service-parent-close.sh
```

Once the integration shell is active, the Core/Services, single-node
JetStream, and cluster interop runners have a five-minute deadline by default.
Set
`NATS_TEST_RUN_TIMEOUT` to a positive number of seconds to adjust it. Set
`NATS_TEST_ARTIFACT_DIR` to a caller-owned directory to preserve logs, Docker
state, the image identity, and run metadata when an interop case
fails. When an acceptance executable is already built, set
`NATS_TEST_ACCEPTANCE_BINARY` to its absolute executable path to bypass only
the Dune build/exec step; the live server and Go-peer checks still run. The
JetStream matrix accepts `pull`, `push`, `ordered`, `kv`, and
`object` scenarios; the default remains the smaller `pull,push` slice.

The system-account cluster runner uses three ephemeral routed containers and
the matrix repeats it over the three pinned NATS releases. These runners
require the selected images to already be cached; they refuse to pull images
so that the documented 10 GiB Colima baseline remains bounded.

The script requires Docker and is intentionally outside `dune runtest`; Docker
is the one integration dependency supplied by the host rather than Nix. The
Colima helper creates or starts the `default` profile with a 10 GiB disk by
default; set `COLIMA_DISK_GIB` only when a larger disk is actually needed. Set
`NATS_SERVER_IMAGE` to try another server image. Cluster, TLS, and reconnect
scenarios are split into focused runners; the server reconnect runner starts
two single-node servers, kills the active one, checks pending-request failure,
checks Core subscription recovery, and checks Service endpoint and monitoring
recovery through the same failover.

For heavier matrices, use the owned test profile wrapper:

```text
./scripts/runtest-with-colima.sh \
  ./scripts/runtest-interop-auth-key-value-cluster-matrix.sh
```

It defaults to a separate `nats-tests` profile with 4 CPUs, 6 GiB of RAM, and
a 12 GiB disk. It restores the Docker context that was active before the run
and stops the profile only when this invocation started it. If the profile is
already present, it uses the stored allocation without resizing it; a running
profile is not stopped. Override `NATS_TEST_COLIMA_PROFILE`,
`NATS_TEST_COLIMA_CPUS`,
`NATS_TEST_COLIMA_MEMORY_GIB`, or `NATS_TEST_COLIMA_DISK_GIB` when the host
requires a different allocation for a new profile. `DOCKER_HOST` must be unset
so the wrapper can isolate Docker access to the selected profile. The wrapper
serializes use of a named profile; choose distinct profile names for concurrent
VM-backed runs. It records the owner PID and reports a possibly stale lock when
that process no longer exists, but never removes a lock automatically; after
an uncatchable termination it reports the exact path for manual inspection.
The lock should be removed only after confirming no test wrapper is running.
The KV matrix pulls its pinned NATS images on demand, while the system-account
runners keep their cached-image-only contract.

The cluster runner starts a three-node route mesh, connects only to the seed,
checks the advertised client URLs, kills the seed, verifies recovery to a
discovered peer, then kills that active peer and verifies recovery to the last
node with subscription replay.
The system-account runner extends that routed check to privileged account
administration, server/account monitoring, system events, reload, and the same
failover path. Its matrix covers username/password, username/password over
TLS, NKey, NKey over TLS, JWT, JWT over TLS, and certificate-mapped mTLS across
`nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5` (21 cases by default). Set
`NATS_SYSTEM_SERVER_IMAGES` or `NATS_SYSTEM_AUTH_MODES` to select a bounded
subset; the runner refuses uncached images. JWT resolver accounts have dynamic
public IDs, so the test derives both the account request target and event
subject from the generated resolver configuration. Certificate-mapped mTLS
uses the explicit `Nats.Auth.tls` handshake mode, which sends an empty
`CONNECT` while allowing the server to authenticate the transport certificate.
Simple token authentication is intentionally not a system-account cell because
the token-only server mode does not select an account-scoped privileged user;
token authentication remains covered by the ordinary Core acceptance runner.
The JetStream cluster runner starts a separate three-node full route mesh with
file-backed, three-replica JetStream state, verifies durable Push delivery and
acknowledgement before killing the seed, then verifies reconnect, replicated
stream/consumer state, and a second publish/delivery on a surviving node. The
cross-SDK Ordered reconnect runner adds elected stream-leader targeting and
durable seed restart recovery, and checks both the OCaml client and the
official Go peer. Its authenticated companion covers NKey, JWT, NKey-over-TLS,
JWT-over-TLS, and mTLS across `nats:2.10.22`, `nats:2.12.15`, and
`nats:2.14.5`. Its `management` mode takes all three persistent nodes out of
service long enough to require JetStream management requests to fail, then
checks recovery. Its `changed-advertised` mode replaces the original seed with
a new client endpoint, requires both clients to observe the new advertisement,
removes the remaining original nodes, and checks a second reconnect through
the replacement. Both modes are Ordered-consumer scenarios because that is
where the endpoint and consumer recovery contract is defined.
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
Set `NATS_TEST_JS_CLUSTER_FAILURE_MODE=node-a`, `node-b`, or `node-c` to make
the selected cluster member the initial endpoint for both clients and then
remove that specific member. These fixed-node modes exercise recovery from
the seed and from each discovered peer; `node-a` is equivalent to the older
`seed` spelling. They deliberately use the non-restart path, so the permanent
loss of one replica does not claim full three-replica quorum.
Set `NATS_TEST_JS_CLUSTER_FAILURE_MODE=multi-node` to use persistent per-node
volumes, lose node-a, wait for both clients to reconnect, lose node-b, and
restart both failed members. The runner does not claim JetStream availability
while two of three replicas are down; it signals recovery only after all three
servers are ready, and the Go peer then requires the replicated stream to be
current before post-recovery delivery. The anonymous Ordered, KV, and Object
Store smoke cases pass on `nats:2.10.22`; the authenticated Ordered
multi-node matrix now passes all 15 cases across the three releases. The
authenticated KV and Object Store multi-node matrices also pass all 15 cases
each across those releases.
The companion `./scripts/runtest-interop-jetstream-cluster-matrix.sh` selects
node-a, node-b, node-c, leader, restart, multi-node, management, and
changed-advertised modes over the three pinned server images by default;
`seed` remains accepted as an alias for node-a. Set
`NATS_SERVER_IMAGES` or `NATS_INTEROP_JETSTREAM_CLUSTER_MATRIX_MODES` to select
a bounded subset. The completed anonymous fixed-node/leader/restart sweep
passes 15 cases across the three releases; management and changed-advertised
add three passing Ordered cases each. The anonymous multi-node mode remains a
`nats:2.10.22` smoke case for each JetStream scenario. The authenticated
companion `./scripts/runtest-interop-auth-jetstream-cluster-matrix.sh` accepts
the same eight failure modes across the five NKey/JWT/TLS modes. Its fixed-node
Ordered sweep passes 45 node-a/node-b/node-c cases across the three releases;
the earlier seed/leader/restart sweep remains a separate 45-case baseline.
The authenticated management and changed-advertised additions pass all 30
mode/release cells (five credential/TLS modes, two scenarios, three releases),
and the authenticated Ordered multi-node slice passes all 15 cases. The base
cluster runner also
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
pass. The Object Store interop runner uses the same official Go peer to
exchange chunked content and metadata, updates, links, listing, tombstones,
and sealing. Its six-mode single-server authentication/TLS matrix passes all
18 cases across `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5`. The dedicated
`./scripts/runtest-interop-key-value-cluster.sh` runner now covers replicated
ordered-watch recovery after seed loss, elected-leader loss, and durable seed
restart in anonymous plaintext mode. Its expanded matrix passes all 15
node-a/node-b/node-c, leader, and restart cases across the three pinned
releases. The anonymous multi-node KV smoke passes on `nats:2.10.22`; the
authenticated KV multi-node matrix adds 15 passing cases across the five
credential/TLS modes and three releases, and the corresponding Object Store
matrix adds another 15.
The authenticated KV companion wrappers
`./scripts/runtest-interop-auth-key-value-cluster.sh` and
`./scripts/runtest-interop-auth-key-value-cluster-matrix.sh` now route the five
generated NKey/JWT/TLS modes through node-a, node-b, node-c, leader, restart,
and multi-node failures. The NKey seed smoke and isolated JWT restart cases
pass. The
full 45-cell seed/leader/restart KV sweep passes across all three pinned
releases under the owned `nats-tests` Colima profile, including the previously
resource-sensitive 2.12.15 JWT restart case. Authenticated fixed-node KV
coverage adds another 45 passing cases across node-a, node-b, and node-c for
the same five modes and three releases; its authenticated multi-node matrix
also passes all 15 cases across those modes and releases. The dedicated
`./scripts/runtest-interop-object-store-cluster.sh`
runner covers replicated Object Store content, metadata, cross-SDK writes and
reads, cleanup, seed loss, elected-leader loss, and durable seed restart in
anonymous plaintext mode. Its expanded matrix passes all 15 node-a/node-b,
node-c, leader, and restart cases across the three pinned releases;
the companion matrix repeats those cases by default and accepts
`NATS_SERVER_IMAGES` or `NATS_INTEROP_OBJECT_STORE_CLUSTER_MODES` for a bounded
sweep. The default matrix also includes the multi-node mode, while
changed-advertisement and management-operation scenarios remain defined by
the Ordered runner rather than this feature-family runner. The
authenticated companion wrappers reuse the
NKey/JWT/mTLS cluster matrix and select the same Object Store scenario; their
five credential/TLS modes across the original seed/leader/restart failures and
three releases have passed all 45 cells (15 per release). Authenticated
fixed-node Object Store coverage adds another 45 passing cases across
node-a, node-b, and node-c for the same five modes and three releases. The
anonymous multi-node Object Store smoke passes on `nats:2.10.22`; its
authenticated multi-node matrix also passes all 15 cases across the five
credential/TLS modes and three releases.
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
cluster/reconnect, and the wider Services cross-SDK/version matrix remain
separate acceptance work.
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
declarations, exact endpoint counters, custom endpoint statistics data in both
directions, and a request/reply completion barrier. It supports anonymous,
token, username/password, NKey, JWT, mTLS, and server-required TLS modes
against the pinned `nats:2.10.22` image. The Service matrix runner repeats
that exchange across `nats:2.10.22`, `nats:2.12.15`, and `nats:2.14.5` in all
eleven plaintext/TLS authentication modes (33 cases by default); set
`NATS_SERVER_IMAGES` or `NATS_INTEROP_SERVICE_MATRIX_MODES` to select a
bounded subset. Set `NATS_INTEROP_SERVICE_MATRIX_SCENARIOS` to
`service-failure` and/or `service-parent-close` to apply the same matrix to
the focused lifecycle cases; both scenarios pass all 33 version/authentication
cells. Cross-SDK reconnect coverage remains separate from that version matrix.
The Service reconnect runner reuses the
three-server failover harness, performs bidirectional endpoint requests before
each kill, waits for both clients to report recovery, and checks endpoint
replay plus INFO/STATS monitoring after each failover. It accepts the same
authentication and TLS controls as the Core reconnect runner. The Service
failure runner uses a bounded pending queue and a controlled Go publisher to
force the OCaml endpoint subscription into a slow-consumer failure, then
checks the failed Service state and parent-connection usability. Explicit
parent-connection closure is covered by a separate runner that waits for an
after-close marker before probing the endpoint, verifies that no stale
responder remains, and checks clean explicit Service stopping.

The first JetStream layer is available through `Nats_eio.Jetstream`: typed
stream and consumer configuration and management, including consumer metadata,
sampling, push rate limits, replica inheritance, singular and multi-subject
filters, redelivery backoff schedules, typed pause/resume control, and
read-modify-write updates that preserve unknown server fields, direct
stored-message reads, one-shot and continuous pull fetch, durable publish and
message acknowledgements, and switch-owned Eio pull/push/ordered sessions over
ordinary Core NATS request/reply. The continuous pull handle is bounded and
switch-owned, with explicit stop and drain controls. Push sessions consume
idle heartbeats, answer flow-control requests including
stalled-heartbeat replies, and restore replayable delivery subscriptions across
reconnects. Ordered sessions use client-managed ephemeral pull consumers,
validate consecutive consumer sequences, and resume from the next stream
sequence after recovery. Both KV and Object Store expose account-wide managers,
name/status listers, and read-modify-write policy projections; Object Store
also provides file transfer helpers and cross-SDK data-plane coverage.
`Nats_eio.Object_store` provides validated buckets, direct metadata,
incremental Bytesrw put/get, digest verification, metadata updates and links,
snapshot/live watches, listing, deletion, replacement cleanup, sealing, and
bucket policy updates for replicas, placement, compression, and stream
metadata. `Nats_eio.Key_value` provides revisioned values, compare-and-set
mutations, finite scans, history, cancellable watches, TTL/marker policy, and
KV stream composition, while the dedicated Key-Value interop runner
cross-checks revision, CAS, tombstone, watch, purge, and cleanup behavior
against the official Go `nats.go` API. `Nats_eio.Service` provides typed
endpoint workers, queue groups,
`$SRV.*` monitoring and fan-out discovery, request/service-error replies,
statistics, replayable subscriptions, and service-local draining. The
dedicated JetStream cluster slice is intentionally narrower than a full
failure matrix; advanced cluster scenarios remain in the final acceptance
phase. The Ordered reconnect cluster slice passes anonymous plaintext seed-node
failure, JetStream-leader failure, and durable seed restart on all three pinned
server releases.

The project uses Dune package management. No compatibility layer for NATS
Streaming is planned.
