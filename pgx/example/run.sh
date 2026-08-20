#!/usr/bin/env bash
#
# The pgx half of `demo/scripts/run-example.sh pgx`: start the store, resolve the adapter, build this
# module, run the program. The PDP is already up and reachable at $CERBOS_HOST — the shared runner
# owns it.
#
# The PostgreSQL server is this script's job rather than the runner's. demo/docker-compose.yml holds
# the one thing every example genuinely shares, and nearly every store is somebody else's; putting
# them there would grow it into the language switch the split exists to avoid. Starting it here is
# also what keeps `demo/scripts/run-example.sh pgx` a single command on a laptop.
#
# There is no pack-and-install step here, unlike every other example: the adapter is resolved through
# the `replace` directive in go.mod, so THIS EXAMPLE PROVES USAGE SHAPES, NOT PACKAGING. README.md
# says why, and docs/adr/0002-examples-install-the-packed-artifact.md is where the exception lives.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/cases.json.
#
# Pre-reqs: docker, and a Go toolchain satisfying this module's `go` directive.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

# The server the adapter is proved against, read rather than restated: ../adversarial_test.go reads
# the same file, so the PostgreSQL this example composes SQL against is the one the corpus is replayed
# against, and bumping it is one edit. See the comment on postgresImageFile there.
POSTGRES_IMAGE="$(cat "${ADAPTER_DIR}/POSTGRES_IMAGE")"
PG_CONTAINER="cerbos-demo-pgx"
# 15432, not PostgreSQL's default 5432: the adversarial suite starts PostgreSQL containers of its
# own, and a demo server holding the default port would leave one of the two reading the other's
# rows. main.go's DSN is the other half of this number.
PG_PORT=15432
PG_USER=cerbos_demo
PG_PASSWORD=cerbos_demo
PG_DB=cerbos_demo

BUILD_DIR="$(mktemp -d)"

cleanup() {
  local status=$?
  if (( status != 0 )); then
    # Only if there is a container to ask, because the checks below run before it is started and
    # `docker logs` on a missing container prints an error that reads as the cause. `>&2` alone
    # carries both of its streams: it points stdout at stderr, and stderr is already there —
    # PostgreSQL logs to stderr, so discarding that one would discard the log.
    if docker container inspect "${PG_CONTAINER}" >/dev/null 2>&1; then
      echo "==> pgx example failed (exit ${status}): PostgreSQL container logs" >&2
      docker logs "${PG_CONTAINER}" >&2 || true
    else
      echo "==> pgx example failed (exit ${status}) before the store was started" >&2
    fi
  fi
  docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
  rm -rf "${BUILD_DIR}"
}
trap cleanup EXIT INT TERM

cd "${EXAMPLE_DIR}"

# 1. The invariant this example rests on: it is its OWN module, so nothing under this directory is
#    part of the adapter a consumer downloads.
#
#    The consequence is about the module zip — a directory containing a go.mod is excluded from its
#    parent's zip, so this example's code and its version pins are not part of
#    github.com/cerbos/query-plan-adapters/pgx at all. That was verified by packing the adapter with
#    golang.org/x/mod/zip — the package cmd/go itself builds zips with — once with this go.mod in
#    place and once with it renamed away; README.md records the experiment and its result.
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
    "go.mod is what keeps this example out of every consumer's build" \
    "(docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
  exit 1
fi

# 2. The two halves of the store's address agree.
#
#    main.go holds the DSN, because an example takes no arguments and reads nothing but $CERBOS_HOST
#    (demo/README.md), and this script publishes the port — so the number lives in two files with
#    nothing but this check holding them equal. Most drift between them is loud on its own (a refused
#    connection naming the port), but one direction is not merely loud: point main.go at 5432 while
#    this script publishes elsewhere and the program would find whatever PostgreSQL the machine
#    already runs, and its first statement is a DROP TABLE. Cheap to rule out.
echo "==> checking main.go's DSN names port ${PG_PORT}" >&2
if ! grep -q "dsn = \"postgres://${PG_USER}:${PG_PASSWORD}@127.0.0.1:${PG_PORT}/${PG_DB}?" main.go; then
  echo "main.go's DSN does not match the container this script starts" \
    "(postgres://${PG_USER}:…@127.0.0.1:${PG_PORT}/${PG_DB}) — the two halves of that address are in" \
    "two files and this is what holds them equal" >&2
  exit 1
fi

# 3. Start the store. First, so it warms up while the module is resolved and built — the readiness
#    wait below is what actually gates the run, not this line.
echo "==> starting ${POSTGRES_IMAGE} on ${PG_PORT}" >&2
docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${PG_CONTAINER}" -p "${PG_PORT}:5432" \
  -e POSTGRES_USER="${PG_USER}" \
  -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
  -e POSTGRES_DB="${PG_DB}" \
  "${POSTGRES_IMAGE}" >/dev/null

# 4. Resolve dependencies from the committed go.mod and go.sum, and from nothing else.
#
# `-mod=readonly` is this toolchain's `npm ci`: it fails rather than editing go.mod, so a dependency
# this example does not pin cannot be pulled in silently — which is what makes the committed go.mod,
# and therefore the Renovate bump that lands with it, the thing this job gates on. `go mod verify`
# then says the downloaded modules still hash to what go.sum records.
#
# There is no `>=` floor here for a release to slip through, because Go has no spelling for one: a
# require directive names one exact version, so a new pgx release cannot enter this build without a
# commit that edits this directory. See README.md, "go.mod is the committed lockfile".
echo "==> go mod download && go mod verify" >&2
export GOFLAGS="-mod=readonly"
go mod download >&2
go mod verify >&2

# What actually got resolved, on stderr where a human reading a CI log will see it. The `=> ../` on
# the adapter line is the replace directive, stated rather than buried: it is why this example is not
# a packaging test.
{
  go version
  go list -m github.com/jackc/pgx/v5
  go list -m github.com/cerbos/query-plan-adapters/pgx
} >&2

# 5. Build. The binary goes to a scratch directory rather than into the module, so a stale one cannot
#    be the thing that runs and the working tree stays clean.
echo "==> go build" >&2
go build -o "${BUILD_DIR}/example" . >&2

# 6. Wait for the store. Deliberately after the build: the container has been starting underneath
#    steps 4-5, so by here there is usually nothing left to wait for, and a build error reports itself
#    without first blocking on a server it will never use.
#
#    The wait is for the SECOND "ready to accept connections", not the first and not pg_isready alone.
#    The official image starts the server to run its initialisation scripts, stops it, and starts it
#    again — so the first ready line, and a pg_isready that catches it, are both true of a server that
#    is about to shut down. ../adversarial_test.go waits on the same occurrence count for the same
#    reason.
echo "==> waiting for PostgreSQL" >&2
ready=0
for _ in {1..60}; do
  if (( $(docker logs "${PG_CONTAINER}" 2>&1 \
    | grep -c 'database system is ready to accept connections' || true) >= 2 )) \
    && docker exec "${PG_CONTAINER}" pg_isready -q -U "${PG_USER}" -d "${PG_DB}"; then
    ready=1
    break
  fi
  sleep 1
done
(( ready == 1 )) || { echo "PostgreSQL failed to start" >&2; exit 1; }

# 7. Run. stdout is the JSON document and nothing else.
echo "==> ${BUILD_DIR}/example" >&2
"${BUILD_DIR}/example"
