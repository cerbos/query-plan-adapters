#!/bin/bash
set -euo pipefail

generate_v7_clients() {
  echo "Generating Prisma v7 clients..."
  node node_modules/prisma/build/index.js generate
  node node_modules/prisma/build/index.js generate --schema=prisma/schema.adversarial.prisma
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

  node node_modules/prisma-v6/build/index.js generate --schema=prisma/schema.v6.prisma
  node node_modules/prisma-v6/build/index.js generate --schema=prisma/schema.adversarial.v6.prisma

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
