#!/usr/bin/env bash
#
# The drizzle half of `demo/scripts/run-example.sh drizzle`: pack the adapter, install THAT, build
# the example against it, run it. The PDP is already up and reachable at $CERBOS_HOST — the
# shared runner owns it.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document whose cases and expected results come from demo/cases.json.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"
rm -f cerbos-orm-drizzle-*.tgz

# 1. Build and pack the adapter into the artifact npm would publish.
echo "==> packing @cerbos/orm-drizzle" >&2
(cd "${ADAPTER_DIR}" && npm run build) >&2
TARBALL="$(cd "${ADAPTER_DIR}" && npm pack --silent --pack-destination "${EXAMPLE_DIR}")"
echo "==> packed ${TARBALL}" >&2

# 2. Install the example's own pinned tree from the committed lockfile, then the tarball on top.
#
# The tarball is NOT a package.json dependency and NOT in the lockfile: its integrity hash changes
# on every build, which would make `npm ci` fail on a lockfile that was correct when committed.
# `--no-save --no-package-lock` keeps both files exactly as committed while still resolving the
# adapter — and its peer range against this example's drizzle-orm — the way a consumer's install
# would.
echo "==> npm ci" >&2
npm ci >&2
echo "==> installing the packed adapter" >&2
npm install --no-save --no-package-lock "./${TARBALL}" >&2

# 3. Compile, from cold. This is half of the packaging proof: `moduleResolution: nodenext` reads
#    the adapter's `exports` map, so a broken one fails HERE rather than shipping to a consumer.
#
#    `tsc --build` is incremental, and the state it reuses is keyed on this example's own sources
#    — which do not change when the adapter's packaging does. A warm lib/ and tsconfig.tsbuildinfo
#    therefore let a broken `exports` map compile clean, which is the exact break this step
#    exists to catch. CI is always cold; a developer's tree is not, and it is the tree where
#    someone checks the break by hand.
#
#    There is no schema-generation step to run first, unlike the Prisma example: a Drizzle schema
#    is ordinary TypeScript, and src/schema.ts carries the one CREATE TABLE beside it. The SQLite
#    file itself is scratch state main.ts deletes and recreates on every run.
echo "==> tsc" >&2
rm -rf lib tsconfig.tsbuildinfo
npm run build >&2

# 4. Run. stdout is the JSON document and nothing else.
echo "==> node lib/main.js" >&2
node lib/main.js
