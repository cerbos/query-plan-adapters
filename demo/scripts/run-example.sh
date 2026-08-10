#!/usr/bin/env bash
#
# Run one adapter's example application against the shared demo domain.
#
#   demo/scripts/run-example.sh prisma
#
# The split between this script and the example is deliberate. Everything language-independent
# lives here — PDP lifecycle, output capture, canonicalisation, the diff against
# demo/expected.json. Everything language-specific lives in `<adapter>/example/run.sh`, which
# packs the adapter into a real distributable, installs THAT (never the source directory — see
# docs/adr/0002-examples-install-the-packed-artifact.md), runs the example, and prints one JSON
# document to stdout. Ten languages means ten packaging stories; growing a language switch in here
# instead would put all ten in one file.
#
# The example's contract:
#   - takes no arguments, reads no environment except CERBOS_HOST (set below)
#   - prints exactly one JSON document to stdout: {"adapter": "<name>", "shapes": {...}}
#   - anything it wants to say to a human goes to stderr
#
# Pre-reqs: docker (with compose), jq.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"

RED="\033[0;31m"; GREEN="\033[0;32m"; NC="\033[0m"
fail() { printf "${RED}FAIL${NC} %s\n" "$*" >&2; exit 1; }
ok()   { printf "${GREEN}OK${NC}   %s\n" "$*" >&2; }

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <adapter>" >&2
  exit 2
fi
ADAPTER="$1"

# The adapter roster is `adapters` in conformance/actions.json. Deliberately not a second list:
# an adapter added to one roster but not the other looks consistent from either side.
if ! jq -e --arg a "${ADAPTER}" '.adapters | index($a)' \
  "${REPO_ROOT}/conformance/actions.json" >/dev/null; then
  fail "'${ADAPTER}' is not in conformance/actions.json .adapters"
fi

EXAMPLE_DIR="${REPO_ROOT}/${ADAPTER}/example"
RUNNER="${EXAMPLE_DIR}/run.sh"
[[ -d "${EXAMPLE_DIR}" ]] || fail "${ADAPTER}/example/ does not exist"
[[ -x "${RUNNER}" ]] || fail "${ADAPTER}/example/run.sh does not exist or is not executable"

WORK_DIR="$(mktemp -d)"
COMPOSE=(docker compose -f "${DEMO_DIR}/docker-compose.yml" -p "cerbos-demo-${ADAPTER}")

cleanup() {
  local status=$?
  if (( status != 0 )); then
    # Dump diagnostics BEFORE `compose down` discards the container — this is what CI, and anyone
    # running headless, gets to debug with.
    echo "==> demo example failed (exit ${status}): Cerbos container logs" >&2
    "${COMPOSE[@]}" logs --no-color cerbos >&2 2>/dev/null || true
    if [[ -s "${WORK_DIR}/stdout.json" ]]; then
      echo "==> example stdout was:" >&2
      cat "${WORK_DIR}/stdout.json" >&2
    fi
  fi
  "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT INT TERM

echo "==> starting the demo PDP" >&2
"${COMPOSE[@]}" up -d --wait >&2

echo "==> ${ADAPTER}/example/run.sh" >&2
# stdout is the JSON document and nothing else; the example's own chatter goes to stderr, so it
# stays visible while the document stays machine-readable.
if ! CERBOS_HOST="localhost:13593" "${RUNNER}" >"${WORK_DIR}/stdout.json"; then
  fail "${ADAPTER}/example/run.sh exited non-zero"
fi

if ! jq -e . "${WORK_DIR}/stdout.json" >/dev/null 2>&1; then
  fail "${ADAPTER}/example/run.sh did not print a single JSON document to stdout"
fi

# The example names itself, so a runner that silently ran some other example — a stale build
# directory, a copied run.sh — fails here rather than passing on the shared expectations.
declared="$(jq -r '.adapter // ""' "${WORK_DIR}/stdout.json")"
[[ "${declared}" == "${ADAPTER}" ]] || \
  fail "example declared adapter '${declared}', expected '${ADAPTER}'"

# expected.json carries its documentation inline, under a `description` sibling of each shape's
# `results`. Examples emit the results alone, so the comparison strips the prose from the
# expectation rather than asking ten examples to reproduce it.
jq -S '.shapes | with_entries(.value |= .results)' "${DEMO_DIR}/expected.json" \
  >"${WORK_DIR}/expected.json"
jq -S '.shapes' "${WORK_DIR}/stdout.json" >"${WORK_DIR}/actual.json"

if ! diff -u "${WORK_DIR}/expected.json" "${WORK_DIR}/actual.json" >"${WORK_DIR}/diff" 2>&1; then
  echo "==> ${ADAPTER} diverged from demo/expected.json (- expected, + actual)" >&2
  cat "${WORK_DIR}/diff" >&2
  fail "${ADAPTER} example output does not match demo/expected.json"
fi

shape_count="$(jq -r '.shapes | length' "${WORK_DIR}/stdout.json")"
ok "${ADAPTER} example matched demo/expected.json across ${shape_count} usage shapes"
