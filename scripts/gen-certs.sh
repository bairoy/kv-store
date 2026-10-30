#!/bin/sh
# Create a private CA and one server certificate for a kv-store cluster.
#
#   scripts/gen-certs.sh [output-dir] [extra-hostnames...]
#
# Writes ca.pem, server.pem and server.key (default directory: ./certs).
# The certificate is valid for localhost, 127.0.0.1 and node1..node8, plus any
# extra hostnames given. Every server uses the same certificate; clients and
# servers trust ca.pem.
set -eu

out="${1:-certs}"
[ "$#" -gt 0 ] && shift
mkdir -p "$out"

san="DNS:localhost,IP:127.0.0.1"
for n in 1 2 3 4 5 6 7 8; do san="$san,DNS:node$n"; done
for host in "$@"; do san="$san,DNS:$host"; done

cat > "$out/ca.cnf" <<CNF
[req]
distinguished_name = dn
x509_extensions = ca
prompt = no
[dn]
CN = kv-store-ca
[ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
CNF

cat > "$out/server.ext" <<EXT
subjectAltName = $san
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = serverAuth
EXT

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$out/ca.key"
openssl req -x509 -new -key "$out/ca.key" -days 825 -config "$out/ca.cnf" -out "$out/ca.pem"

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$out/server.key"
openssl req -new -key "$out/server.key" -subj "/CN=kv-store-node" -out "$out/server.csr"
openssl x509 -req -in "$out/server.csr" -CA "$out/ca.pem" -CAkey "$out/ca.key" \
  -CAcreateserial -days 825 -extfile "$out/server.ext" -out "$out/server.pem"

rm -f "$out/server.csr" "$out/ca.cnf" "$out/server.ext" "$out/ca.srl"
chmod 600 "$out/ca.key"
# readable by the unprivileged user inside the container; keep this directory private
chmod 644 "$out/server.key"

echo "wrote $out/ca.pem, $out/server.pem, $out/server.key (keep ca.key private)"
