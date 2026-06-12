#!/bin/sh

# This file is sourced by the system-account acceptance runner after it has
# selected auth_mode, run_id, script_dir, and tls_enabled.
# shellcheck disable=SC2154

prepare_system_nkey_material() {
  auth_dir=$(mktemp -d "$script_dir/.nats-system-auth.XXXXXX")
  nsc init --all-dirs "$auth_dir" --dir "$auth_dir/store" --name system \
    >/dev/null
  (
    cd "$auth_dir/store/system" || exit
    NKEYS_PATH="$auth_dir" NSC_CWD_ONLY=1 nsc generate config \
      --mem-resolver --config-file "$auth_dir/nats.conf" \
      --sys-account SYS >/dev/null
    NKEYS_PATH="$auth_dir" NSC_CWD_ONLY=1 nsc describe account \
      --name SYS -J >"$auth_dir/system-account.json"
  )
  creds="$auth_dir/creds/system/SYS/sys.creds"
  if [ ! -f "$creds" ]; then
    echo "nsc did not generate system-account credentials" >&2
    return 1
  fi
  awk '/BEGIN NATS USER JWT/{getline; print; exit}' "$creds" \
    >"$auth_dir/system.jwt"
  awk '/BEGIN USER NKEY SEED/{getline; print; exit}' "$creds" \
    >"$auth_dir/system.seed"
  jwt=$(cat "$auth_dir/system.jwt")
  payload=$(printf '%s' "$jwt" | cut -d. -f2 | tr '_-' '/+')
  case $((${#payload} % 4)) in
    2) payload="$payload==" ;;
    3) payload="$payload=" ;;
  esac
  nkey_public=$(printf '%s' "$payload" | openssl base64 -d -A |
    sed -n 's/.*"sub":"\([^"]*\)".*/\1/p')
  if [ -z "$nkey_public" ]; then
    echo "could not extract the generated system NKey public key" >&2
    return 1
  fi
  system_account=$(sed -n 's/.*"sub": "\([^"]*\)".*/\1/p' \
    "$auth_dir/system-account.json" | head -n 1)
  if [ -z "$system_account" ]; then
    echo "could not extract the generated system account id" >&2
    return 1
  fi
  export NATS_TEST_SYSTEM_NKEY_PUBLIC="$nkey_public"
  export NATS_TEST_SYSTEM_ACCOUNT="$system_account"
  if [ "$auth_mode" = nkey ] || [ "$auth_mode" = nkey-tls ]; then
    export NATS_TEST_SYSTEM_NKEY_SEED_FILE="$auth_dir/system.seed"
    unset NATS_TEST_SYSTEM_USER_JWT_FILE NATS_TEST_SYSTEM_USER_SEED_FILE
  else
    export NATS_TEST_SYSTEM_USER_JWT_FILE="$auth_dir/system.jwt"
    export NATS_TEST_SYSTEM_USER_SEED_FILE="$auth_dir/system.seed"
    unset NATS_TEST_SYSTEM_NKEY_SEED_FILE
  fi
}

prepare_system_tls_material() {
  cert_dir=$(mktemp -d "$script_dir/.nats-system-tls.XXXXXX")
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" \
    -subj "/CN=ocaml-nats-system-test-ca" >/dev/null
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null
  openssl x509 -req -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" \
    -CAcreateserial -out "$cert_dir/server.pem" -days 1 -sha256 \
    -copy_extensions copy >/dev/null
  export NATS_TEST_TLS_CA="$cert_dir/ca.pem"
  if [ "$auth_mode" = mtls ]; then
    openssl req -newkey rsa:2048 -nodes \
      -keyout "$cert_dir/client-key.pem" -out "$cert_dir/client.csr" \
      -subj "/CN=ocaml-nats-system-client" \
      -addext "subjectAltName=email:sys@nats.io" \
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
