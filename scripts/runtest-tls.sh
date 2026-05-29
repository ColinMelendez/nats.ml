#!/bin/sh
set -eu

script_dir=$(CDPATH=; export CDPATH; cd "$(dirname "$0")" && pwd)
cd "$script_dir/.."

if [ "${NATS_INTEGRATION_SHELL-}" != 1 ]; then
  LC_ALL=C
  export LC_ALL
  exec nix develop .#integration -c env \
    NATS_INTEGRATION_SHELL=1 "$script_dir/runtest-tls.sh" "$@"
fi

image=${NATS_SERVER_IMAGE:-nats:2.10.22}
container=
cert_dir=$(mktemp -d "$script_dir/.nats-tls.XXXXXX")
log=$(mktemp "${TMPDIR:-/tmp}/ocaml-nats-tls-log.XXXXXX")

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT/INT/TERM trap.
cleanup() {
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$cert_dir"
  rm -f "$log"
}

trap cleanup EXIT INT TERM

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" \
  -subj "/CN=ocaml-nats-test-ca" >/dev/null
openssl req -newkey rsa:2048 -nodes \
  -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" \
  >/dev/null
openssl x509 -req -in "$cert_dir/server.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" \
  -CAcreateserial -out "$cert_dir/server.pem" -days 1 -sha256 \
  -copy_extensions copy >/dev/null

container=$(docker run --detach \
  --volume "$cert_dir:/etc/nats/certs:ro" \
  --volume "$script_dir/nats-server-tls.conf:/etc/nats/tls.conf:ro" \
  --publish 127.0.0.1::4222 "$image" --config /etc/nats/tls.conf)

port=
attempt=0
while [ "$attempt" -lt 30 ]; do
  port=$(docker port "$container" 4222/tcp 2>/dev/null | sed -n 's/.*://p' | head -n 1) || port=
  if [ -n "$port" ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ -z "$port" ]; then
  echo "could not determine the published NATS TLS port" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

ready=0
attempt=0
while [ "$attempt" -lt 20 ]; do
  if docker logs "$container" 2>&1 | grep -q "Server is ready"; then
    ready=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "NATS TLS server did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
fi

status=0
if dune build test/server/server_tls.exe >"$log" 2>&1 && \
    NATS_TEST_TLS_SERVER="nats://127.0.0.1:$port" \
    NATS_TEST_TLS_CA="$cert_dir/ca.pem" \
    "$(pwd)/_build/default/test/server/server_tls.exe" >>"$log" 2>&1
then
  cat "$log"
else
  status=$?
  cat "$log" >&2
  docker logs "$container" >&2 || true
fi
exit "$status"
