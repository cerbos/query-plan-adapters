#!/bin/bash
set -e

echo "Generating Prisma v7 client..."
node node_modules/prisma/build/index.js generate

echo "Generating Prisma v6 client..."
mv node_modules/@prisma/client node_modules/@prisma/client-v7-temp
mv node_modules/@prisma/client-v6 node_modules/@prisma/client

trap 'mv node_modules/@prisma/client node_modules/@prisma/client-v6; mv node_modules/@prisma/client-v7-temp node_modules/@prisma/client' EXIT

node node_modules/prisma-v6/build/index.js generate --schema=prisma/schema.v6.prisma

trap - EXIT

mv node_modules/@prisma/client node_modules/@prisma/client-v6
mv node_modules/@prisma/client-v7-temp node_modules/@prisma/client

echo "Both clients generated successfully!"
