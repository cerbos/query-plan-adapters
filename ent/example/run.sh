#!/usr/bin/env bash
#
# The ent half of `demo/scripts/run-example.sh ent`: resolve the adapter, build this module, run the
# program. The PDP is already up and reachable at $CERBOS_HOST — the shared runner owns it.
#
# The store is SQLite through modernc.org/sqlite, in a file this directory owns, so there is nothing
# to start here.
#
# There is no pack-and-install step here, unlike every other example: the adapter is resolved through
# the `replace` directive in go.mod, so THIS EXAMPLE PROVES USAGE SHAPES, NOT PACKAGING. README.md
# says why, and docs/adr/0002-examples-install-the-packed-artifact.md is where the exception lives.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.
#
# Pre-reqs: a Go toolchain satisfying this module's `go` directive.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT INT TERM

cd "${EXAMPLE_DIR}"

# 1. The invariant this example rests on: it is its OWN module, so nothing under this directory is
#    part of the adapter a consumer downloads.
#
#    The consequence is about the module zip — a directory containing a go.mod is excluded from its
#    parent's zip, so `entgo.io/ent/cmd/ent` and the generator tree behind it, and
#    modernc.org/sqlite, never reach a consumer of github.com/cerbos/query-plan-adapters/ent. That
#    was verified by packing the adapter with golang.org/x/mod/zip — the package cmd/go itself builds
#    zips with — once with this go.mod in place and once with it renamed away; README.md records the
#    experiment and its result.
#
#    What is re-checked on every run is the cause rather than the consequence, because the cause is
#    the half a stray edit could regress and it costs nothing to ask: `go list -m` names the main
#    module of the directory it runs in, reading go.mod alone. Delete this module's go.mod and the
#    command walks up to the adapter instead, so the two agreeing means the boundary is gone.
echo "==> checking the example is a module of its own" >&2
example_module="$(go list -m)"
adapter_module="$(cd "${ADAPTER_DIR}" && go list -m)"
if [[ "${example_module}" == "${adapter_module}" ]]; then
  echo "this directory is part of ${adapter_module} rather than a module of its own — the nested" \
    "go.mod is what keeps this example's generator and driver dependencies out of every consumer's" \
    "build (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
  exit 1
fi

# 2. Resolve dependencies from the committed go.mod and go.sum, and from nothing else.
#
# `-mod=readonly` is this toolchain's `npm ci`: it fails rather than editing go.mod, so a
# dependency this example does not pin cannot be pulled in silently — which is what makes the
# committed go.mod, and therefore the Renovate bump that lands with it, the thing this job gates on.
# `go mod verify` then says the downloaded modules still hash to what go.sum records.
#
# There is no `>=` floor here for a release to slip through, because Go has no spelling for one: a
# require directive names one exact version, so a new ent release cannot enter this build without a
# commit that edits this directory. See README.md, "go.mod is the committed lockfile".
echo "==> go mod download && go mod verify" >&2
export GOFLAGS="-mod=readonly"
go mod download >&2
go mod verify >&2

# What actually got resolved, on stderr where a human reading a CI log will see it. The `=> ../` on
# the adapter line is the replace directive, stated rather than buried: it is why this example is
# not a packaging test.
{
  go version
  go list -m entgo.io/ent
  go list -m github.com/cerbos/query-plan-adapters/ent
} >&2

# 3. Build. The binary goes to a scratch directory rather than into the module, so a stale one
#    cannot be the thing that runs and the working tree stays clean.
echo "==> go build" >&2
go build -o "${BUILD_DIR}/example" . >&2

# 4. Run. stdout is the JSON document and nothing else.
echo "==> ${BUILD_DIR}/example" >&2
"${BUILD_DIR}/example"
