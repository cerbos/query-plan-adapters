#!/bin/bash
set -euo pipefail

# Every schema here is an adversarial one: the adversarial harness is the only suite that reaches
# a generated client at all. The translator unit test produces where-inputs and never hands one to
# Prisma, so it needs no client and no schema — which is why there is no default `prisma generate`.
generate_v7_clients() {
  echo "Generating Prisma v7 clients..."
  node node_modules/prisma/build/index.js generate --schema=prisma/schema.adversarial.prisma
  # The provider is baked into a generated client, so the PostgreSQL adversarial leg
  # (cerbos/query-plan-adapters#320) needs its own. Generation touches no database.
  node node_modules/prisma/build/index.js generate --schema=prisma/schema.adversarial.pg.prisma
}

restore_v7_client_package() {
  if [[ -d node_modules/@prisma/client ]]; then
    mv node_modules/@prisma/client node_modules/@prisma/client-v6
  fi
  if [[ -d node_modules/@prisma/client-v7-temp ]]; then
    mv node_modules/@prisma/client-v7-temp node_modules/@prisma/client
  fi
}

generate_v6_clients() {
  echo "Generating Prisma v6 clients..."
  mv node_modules/@prisma/client node_modules/@prisma/client-v7-temp
  trap restore_v7_client_package EXIT
  mv node_modules/@prisma/client-v6 node_modules/@prisma/client

  node node_modules/prisma-v6/build/index.js generate --schema=prisma/schema.adversarial.v6.prisma
  node node_modules/prisma-v6/build/index.js generate --schema=prisma/schema.adversarial.pg.v6.prisma

  restore_v7_client_package
  trap - EXIT
}

case "${1:-all}" in
  6)
    generate_v6_clients
    ;;
  7)
    generate_v7_clients
    ;;
  all)
    generate_v7_clients
    generate_v6_clients
    ;;
  *)
    echo "Usage: $0 [6|7|all]" >&2
    exit 2
    ;;
esac
