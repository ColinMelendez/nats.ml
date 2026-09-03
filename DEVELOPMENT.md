# Development guide

Use this guide to build the project, choose the right test scope, run protocol
benchmarks, and validate a release candidate.

## Prerequisites

Install the following tools:

- [Nix](https://nixos.org/) for the pinned OCaml toolchain and dependencies.
- A Docker-compatible container runtime for live-server tests. The test scripts
  call the `docker` command. Docker, Colima, or a compatible Podman setup can
  provide it.

You don't need a container runtime for local builds or deterministic tests.

## Build and test locally

For most changes, use the default development shell:

```sh
nix develop
dune build
dune runtest
```

Build the generated API documentation when you change a public interface:

```sh
dune build @doc
```

The default shell contains Dune, OCamlFormat, Odoc, and editor tooling.
`dune runtest` runs the deterministic test suite without starting a NATS
server.

## Choose a Nix environment

| Command | Use it for |
| --- | --- |
| `nix develop` | Normal development, formatting, local builds, and API documentation. |
| `nix develop .#integration` | Live-server tests that need Go, OpenSSL, `nsc`, or ShellCheck. |
| `nix develop .#test` | Reproducible tests and benchmarks without editor tooling. |

Most integration scripts enter the integration environment automatically. To
run several integration commands in one session, enter it yourself:

```sh
nix develop .#integration
```

## Run live-server tests

Start your container runtime before running these tests. On macOS, the
repository can start the default Colima profile for you:

```sh
./scripts/start-colima.sh
```

The helper creates a 10 GiB disk by default. Set `COLIMA_DISK_GIB` before
starting a new profile if you need more space.

Choose the narrowest runner that covers your change:

| Area | Command |
| --- | --- |
| Core NATS and Services | `./scripts/runtest-server.sh` |
| Core NATS with JetStream enabled | `NATS_TEST_JETSTREAM=1 ./scripts/runtest-server.sh` |
| TLS | `./scripts/runtest-tls.sh` |
| Reconnect | `./scripts/runtest-reconnect.sh` |
| Core cluster discovery and failover | `./scripts/runtest-cluster.sh` |
| Lame-duck events | `./scripts/runtest-lameduck.sh` |
| System-account administration | `./scripts/runtest-system.sh` |
| System-account cluster behavior | `./scripts/runtest-system-cluster.sh` |
| Core interoperability with the Go SDK | `./scripts/runtest-interop.sh` |
| JetStream interoperability | `./scripts/runtest-interop-jetstream.sh` |
| Key-Value interoperability | `./scripts/runtest-interop-key-value.sh` |
| Object Store interoperability | `./scripts/runtest-interop-object-store.sh` |
| JetStream cluster recovery | `./scripts/runtest-interop-jetstream-cluster.sh` |
| Key-Value cluster recovery | `./scripts/runtest-interop-key-value-cluster.sh` |
| Object Store cluster recovery | `./scripts/runtest-interop-object-store-cluster.sh` |

Set `NATS_SERVER_IMAGE` to run a focused test against a different server
image:

```sh
NATS_SERVER_IMAGE=nats:2.14.6 ./scripts/runtest-server.sh
```

## Run version and authentication matrices

Run a focused test first. Use a matrix when the focused test passes and the
change affects compatibility, authentication, or recovery.

| Coverage | Command |
| --- | --- |
| Core server, cluster, and lame-duck behavior | `./scripts/runtest-server-matrix.sh` |
| Core and reconnect interoperability, with and without TLS | `./scripts/runtest-interop-matrix.sh` |
| NKey, JWT, and mTLS interoperability | `./scripts/runtest-interop-auth-matrix.sh` |
| Authentication rejection | `./scripts/runtest-interop-auth-negative-matrix.sh` |
| Services across authentication modes | `./scripts/runtest-interop-service-matrix.sh` |
| JetStream, Key-Value, and Object Store scenarios | `./scripts/runtest-interop-jetstream-matrix.sh` |
| System-account authentication modes | `./scripts/runtest-system-matrix.sh` |
| JetStream cluster recovery | `./scripts/runtest-interop-jetstream-cluster-matrix.sh` |
| Key-Value cluster recovery | `./scripts/runtest-interop-key-value-cluster-matrix.sh` |
| Object Store cluster recovery | `./scripts/runtest-interop-object-store-cluster-matrix.sh` |

Specialized authenticated cluster matrices are also available:

- `./scripts/runtest-interop-auth-jetstream-reconnect-matrix.sh`
- `./scripts/runtest-interop-auth-jetstream-cluster-matrix.sh`
- `./scripts/runtest-interop-auth-key-value-cluster-matrix.sh`
- `./scripts/runtest-interop-auth-object-store-cluster-matrix.sh`

### Select matrix cases

Matrix selectors are comma-separated. The scripts reject empty entries and
unknown values.

| Variable | Accepted values |
| --- | --- |
| `NATS_SERVER_IMAGES` | Container image names. The default is the supported server set. |
| `NATS_SERVER_MATRIX_SCENARIOS` | `server`, `cluster`, `lameduck` |
| `NATS_INTEROP_MATRIX_SCENARIOS` | `core`, `reconnect`, `tls-core`, `tls-reconnect` |
| `NATS_INTEROP_SERVICE_MATRIX_SCENARIOS` | `service`, `service-failure`, `service-parent-close` |
| `NATS_INTEROP_SERVICE_MATRIX_MODES` | `anonymous`, `token`, `user-pass`, `nkey`, `jwt`, `mtls`, and the TLS variants |
| `NATS_INTEROP_JETSTREAM_MATRIX_SCENARIOS` | `pull`, `push`, `ordered`, `kv`, `object` |
| `NATS_INTEROP_JETSTREAM_MATRIX_MODES` | `anonymous`, `token`, `user-pass`, and their TLS variants |
| `NATS_SYSTEM_AUTH_MODES` | `user-pass`, `nkey`, `jwt`, `mtls`, and the TLS variants |
| `NATS_SYSTEM_SERVER_IMAGES` | Container image names for the system-account matrix. |
| `NATS_INTEROP_JETSTREAM_CLUSTER_MATRIX_MODES` | Cluster failure modes, including management and changed-advertised recovery. |
| `NATS_INTEROP_KEY_VALUE_CLUSTER_MODES` | Cluster failure modes for Key-Value. |
| `NATS_INTEROP_OBJECT_STORE_CLUSTER_MODES` | Cluster failure modes for Object Store. |

For example, run only Ordered JetStream interoperability against one server:

```sh
NATS_SERVER_IMAGES=nats:2.14.6 \
NATS_INTEROP_JETSTREAM_MATRIX_SCENARIOS=ordered \
  ./scripts/runtest-interop-jetstream-matrix.sh
```

### Select a cluster failure

Set `NATS_TEST_JS_CLUSTER_FAILURE_MODE` for a focused JetStream, Key-Value, or
Object Store cluster run.

| Mode | Behavior |
| --- | --- |
| `seed` or `node-a` | Stop the initial node and recover through a discovered peer. |
| `node-b` or `node-c` | Stop a selected peer. |
| `leader` | Stop the elected stream leader. |
| `restart` | Stop and restart the seed. |
| `multi-node` | Exercise recovery across more than one node failure. |
| `management` | Make management requests fail while the cluster is unavailable, then verify recovery. |
| `changed-advertised` | Replace the seed endpoint and reconnect through the new advertisement. |

`management` and `changed-advertised` apply to the JetStream Ordered
consumer runner.

## Configure live tests

### Set authentication

Use one authentication mode at a time.

For token authentication:

```sh
NATS_TEST_TOKEN=test-token ./scripts/runtest-server.sh
```

For username and password authentication:

```sh
NATS_TEST_USER=test-user \
NATS_TEST_PASS=test-pass \
  ./scripts/runtest-server.sh
```

Set both username and password to non-empty values. Credentials must contain
only ASCII letters, digits, underscores, and hyphens. The authentication
matrix scripts generate their own NKey, JWT, certificate, and mTLS material.

### Set timeouts and preserve failures

The Core, single-node JetStream, and JetStream cluster interoperability
runners have a five-minute deadline. Set a positive timeout in seconds when a
slower host needs more time:

```sh
NATS_TEST_RUN_TIMEOUT=600 ./scripts/runtest-interop-jetstream.sh
```

Supported runners can preserve logs, container state, image identity, and run
metadata after a failure:

```sh
artifacts=$(mktemp -d)
NATS_TEST_ARTIFACT_DIR="$artifacts" ./scripts/runtest-server.sh
```

For JetStream cluster work, `NATS_TEST_ACCEPTANCE_BINARY` can name an
already-built acceptance executable. Use an absolute path. This skips only the
Dune build and execution wrapper; the live server and Go peer still run.

Set `NATS_TEST_JETSTREAM_RUN_ID` only when a stable stream-name suffix helps
with debugging.

## Run heavy matrices with Colima

Use the owned test profile wrapper for resource-intensive matrices:

```sh
./scripts/runtest-with-colima.sh \
  ./scripts/runtest-interop-auth-key-value-cluster-matrix.sh
```

The wrapper uses a separate `nats-tests` profile with 4 CPUs, 6 GiB of
memory, and a 12 GiB disk. Override these defaults only when creating a profile:

| Variable | Purpose |
| --- | --- |
| `NATS_TEST_COLIMA_PROFILE` | Profile name. Use distinct names for concurrent runs. |
| `NATS_TEST_COLIMA_CPUS` | CPU allocation. |
| `NATS_TEST_COLIMA_MEMORY_GIB` | Memory allocation in GiB. |
| `NATS_TEST_COLIMA_DISK_GIB` | Disk allocation in GiB. |

Unset `DOCKER_HOST` before using the wrapper so it can isolate Docker access
to the selected profile. The wrapper serializes use of each profile and
reports stale locks for manual inspection. Remove a reported lock only after
you confirm that no test wrapper is running.

System-account matrix runners require their selected images to be cached and
refuse to pull them. Most other matrix runners pull missing pinned images.

## Run protocol benchmarks

The benchmark suite compiles the local library under three native-code
profiles:

| Profile | Compiler options |
| --- | --- |
| `bench_no_opt` | `-Oclassic` |
| `bench_o3` | `-O3` |
| `bench_o3_unbox` | `-O3 -unbox-closures` |

Run the matrix with a new or empty output directory:

```sh
results=$(mktemp -d)
nix develop .#test -c ./scripts/benchmark-optimizer-matrix.sh "$results"
```

The runner requires Dune 3.24.2 and the Flambda-enabled OCaml 5.5.0 compiler.
It waits up to two minutes for a quiet host. Set
`NATS_BENCH_WAIT_QUIET_SECONDS` to adjust the wait.

Loaded-host results are rejected by default. Set
`NATS_BENCH_ALLOW_LOADED=1` only for exploratory work; don't use those
results as regression baselines. The output directory contains raw reports,
JSON verdicts, profile-specific baselines, and a `summary.md` comparison.
Negative percentages in the summary mean faster execution.

See the [benchmark report](docs/benchmark-report.md) for the current reference
run and interpretation.

## Validate a release candidate

Validate a clean checkout of the exact commit you plan to tag. Follow the
[release requirements](docs/support.md#release-evidence)
for the required matrices and host-diverse evidence.

After validation:

1. Record the commands, environment, results, and limitations in
   [the release evidence](docs/release-evidence.md).
2. Update the [support policy](docs/support.md) only when the evidence changes
   a compatibility claim.
3. Tag the exact validated commit.

Keep historical pass counts and run-specific observations in the release
evidence or [implementation plan](docs/plan.md), not in this guide.
