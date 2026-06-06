#!/bin/sh

# This file is sourced by interop runners after they select auth_mode. It
# assumes script_dir, auth_mode, auth_dir, cert_dir, and tls_enabled are set.
# shellcheck disable=SC2154

prepare_nkey_material() {
  auth_dir=$(mktemp -d "$script_dir/.nats-interop-auth.XXXXXX")
  nsc init --all-dirs "$auth_dir" --dir "$auth_dir/store" --name interop \
    >/dev/null
  (
    cd "$auth_dir/store/interop" || exit
    NKEYS_PATH="$auth_dir" NSC_CWD_ONLY=1 nsc edit account \
      --name interop --js-enable 0 >/dev/null
    NKEYS_PATH="$auth_dir" NSC_CWD_ONLY=1 nsc generate config \
      --mem-resolver --config-file "$auth_dir/nats.conf" >/dev/null
    NKEYS_PATH="$auth_dir" NSC_CWD_ONLY=1 nsc generate creds \
      --account interop --name interop --output-file "$auth_dir/user.creds" \
      >/dev/null
  )
  awk '/BEGIN NATS USER JWT/{getline; print; exit}' "$auth_dir/user.creds" \
    >"$auth_dir/user.jwt"
  awk '/BEGIN USER NKEY SEED/{getline; print; exit}' "$auth_dir/user.creds" \
    >"$auth_dir/user.seed"
  jwt=$(cat "$auth_dir/user.jwt")
  payload=$(printf '%s' "$jwt" | cut -d. -f2 | tr '_-' '/+')
  case $((${#payload} % 4)) in
    2) payload="$payload==" ;;
    3) payload="$payload=" ;;
  esac
  nkey_public=$(printf '%s' "$payload" | openssl base64 -d -A |
    sed -n 's/.*"sub":"\([^"]*\)".*/\1/p')
  if [ -z "$nkey_public" ]; then
    echo "could not extract the generated NKey public key" >&2
    exit 1
  fi
  export NATS_TEST_NKEY_PUBLIC="$nkey_public"
  if [ "$auth_mode" = jwt ]; then
    unset NATS_TEST_NKEY_SEED_FILE
    export NATS_TEST_USER_JWT_FILE="$auth_dir/user.jwt"
    export NATS_TEST_USER_SEED_FILE="$auth_dir/user.seed"
  else
    export NATS_TEST_NKEY_SEED_FILE="$auth_dir/user.seed"
    unset NATS_TEST_USER_JWT_FILE NATS_TEST_USER_SEED_FILE
  fi
  if [ "${negative_mode-}" = credentials ]; then
    nsc generate nkey -u | sed -n '1p' >"$auth_dir/bad.seed"
    if [ ! -s "$auth_dir/bad.seed" ]; then
      echo "could not generate the invalid authentication seed" >&2
      exit 1
    fi
  fi
}

prepare_tls_material() {
  cert_dir=$(mktemp -d "$script_dir/.nats-interop-tls.XXXXXX")
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" \
    -subj "/CN=ocaml-nats-interop-test-ca" >/dev/null
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    >/dev/null
  openssl x509 -req -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" \
    -CAcreateserial -out "$cert_dir/server.pem" -days 1 -sha256 \
    -copy_extensions copy >/dev/null
  export NATS_TEST_TLS_CA="$cert_dir/ca.pem"
  if [ "$auth_mode" = mtls ]; then
    openssl req -newkey rsa:2048 -nodes \
      -keyout "$cert_dir/client-key.pem" -out "$cert_dir/client.csr" \
      -subj "/CN=ocaml-nats-interop-client" \
      -addext "extendedKeyUsage=clientAuth" >/dev/null
    openssl x509 -req -in "$cert_dir/client.csr" \
      -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" \
      -CAcreateserial -out "$cert_dir/client.pem" -days 1 -sha256 \
      -copy_extensions copy >/dev/null
    export NATS_TEST_TLS_CERT="$cert_dir/client.pem"
    export NATS_TEST_TLS_KEY="$cert_dir/client-key.pem"
  else
    unset NATS_TEST_TLS_CERT NATS_TEST_TLS_KEY
  fi
}
