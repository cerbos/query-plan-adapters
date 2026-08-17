#!/bin/bash
set -euo pipefail

# Every schema here is an adversarial one: the adversarial harness is the only suite that reaches
# a generated client at all. The translator unit test produces where-inputs and never hands one to
# Prisma, so it needs no client and no schema — which is why there is no default `prisma generate`.
generate_v7_clients() {
  echo "Generating Prisma v7 clients..."
  node node_modules/prisma/build/index.js generate --schema=prisma/schema.adversarial.prisma
  # The provider is baked into a generated client, so each executed store leg needs its own —
  # PostgreSQL (cerbos/query-plan-adapters#320) and MySQL (#340). Generation touches no database.
  node node_modules/prisma/build/index.js generate --schema=prisma/schema.adversarial.pg.prisma
  node node_modules/prisma/build/index.js generate --schema=prisma/schema.adversarial.mysql.prisma
}

active_legacy_major=""

restore_client_packages() {
  if [[ -d node_modules/@prisma/client ]]; then
    mv node_modules/@prisma/client "node_modules/@prisma/client-v${active_legacy_major}"
  fi
  if [[ -d node_modules/@prisma/client-v7-temp ]]; then
    mv node_modules/@prisma/client-v7-temp node_modules/@prisma/client
  fi
}

generate_legacy_clients() {
  active_legacy_major="$1"
  echo "Generating Prisma v${active_legacy_major} clients..."
  mv node_modules/@prisma/client node_modules/@prisma/client-v7-temp
  trap restore_client_packages EXIT
  mv "node_modules/@prisma/client-v${active_legacy_major}" node_modules/@prisma/client

  node "node_modules/prisma-v${active_legacy_major}/build/index.js" generate \
    --schema="prisma/schema.adversarial.v${active_legacy_major}.prisma"
  node "node_modules/prisma-v${active_legacy_major}/build/index.js" generate \
    --schema="prisma/schema.adversarial.pg.v${active_legacy_major}.prisma"
  node "node_modules/prisma-v${active_legacy_major}/build/index.js" generate \
    --schema="prisma/schema.adversarial.mysql.v${active_legacy_major}.prisma"

  restore_client_packages
  trap - EXIT
}

case "${1:-all}" in
  5)
    generate_legacy_clients 5
    ;;
  6)
    generate_legacy_clients 6
    ;;
  7)
    generate_v7_clients
    ;;
  all)
    generate_v7_clients
    generate_legacy_clients 6
    generate_legacy_clients 5
    ;;
  *)
    echo "Usage: $0 [5|6|7|all]" >&2
    exit 2
    ;;
esac
