#!/usr/bin/env bash
#
# The prisma half of `demo/scripts/run-example.sh prisma`: pack the adapter, install THAT, build
# the example against it, run it. The PDP is already up and reachable at $CERBOS_HOST — the
# shared runner owns it.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"
rm -f cerbos-orm-prisma-*.tgz

# 1. Build and pack the adapter into the artifact npm would publish.
echo "==> packing @cerbos/orm-prisma" >&2
(cd "${ADAPTER_DIR}" && npm run build) >&2
TARBALL="$(cd "${ADAPTER_DIR}" && npm pack --silent --pack-destination "${EXAMPLE_DIR}")"
echo "==> packed ${TARBALL}" >&2

# 2. Install the example's own pinned tree from the committed lockfile, then the tarball on top.
#
# The tarball is NOT a package.json dependency and NOT in the lockfile: its integrity hash changes
# on every build, which would make `npm ci` fail on a lockfile that was correct when committed.
# `--no-save --no-package-lock` keeps both files exactly as committed while still resolving the
# adapter — and its peer range against this example's @prisma/client — the way a consumer's
# install would.
echo "==> npm ci" >&2
npm ci >&2
echo "==> installing the packed adapter" >&2
npm install --no-save --no-package-lock "./${TARBALL}" >&2

# 3. Generate the Prisma client and create the SQLite file the example seeds into.
#
# The database file is scratch state this example owns and .gitignore excludes, so each run starts
# from nothing by deleting it. That is deliberately not `db push --force-reset`: a reset is a
# destructive operation against whatever database the config happens to name, and this needs no
# such power — the file is ours and it is one `rm` away from clean.
echo "==> prisma generate && db push" >&2
rm -f prisma/demo.db prisma/demo.db-journal
npx prisma generate >&2
npx prisma db push >&2

# 4. Compile. This is half of the packaging proof: `moduleResolution: nodenext` reads the
#    adapter's `exports` map, so a broken one fails HERE rather than shipping to a consumer.
#
#    Discard the incremental state first, because that proof is otherwise standing on a side
#    effect. `tsc --build` decides it is up to date by comparing this project's OWN inputs —
#    `include: ["src"]` — against its outputs. A dependency under node_modules is not an input,
#    so repacking and reinstalling the adapter does not invalidate a warm `lib/`: with the
#    tarball's `exports` map pointing at a missing file, `tsc --build` still exits 0 and the
#    break surfaces later, as a MODULE_NOT_FOUND out of step 5, instead of as the TS2307 this
#    step exists to raise. What saves the run today is that step 3 rewrites `src/generated` on
#    every invocation, and those ARE inputs — so the recheck happens by accident, and only for
#    as long as `prisma generate` keeps touching files. One `rm` makes it happen on purpose.
echo "==> tsc" >&2
rm -rf lib tsconfig.tsbuildinfo
npm run build >&2

# 5. Run. stdout is the JSON document and nothing else.
echo "==> node lib/main.js" >&2
node lib/main.js
