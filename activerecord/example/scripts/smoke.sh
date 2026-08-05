#!/usr/bin/env bash
# The end-to-end tests for the example application.
#
# The script starts the PDP and the application in Docker, sends real HTTP requests, and
# compares the identifiers in each response with the expected list. Thus a change that breaks
# the adapter also breaks this script. The same script runs on your computer and in CI.
#
#   ./scripts/smoke.sh
#
# The expected results come from the policies in policies/ and the data in models.rb. If you
# change a policy, you must change the expected results here.
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../../conformance/CERBOS_VERSION)"
export CERBOS_VERSION
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export EXAMPLE_PORT="${EXAMPLE_PORT:-4567}"

BASE="http://localhost:${EXAMPLE_PORT}"
FAILURES=0
CHECKS=0

cleanup() {
  if [[ "${FAILURES}" -gt 0 ]]; then
    echo
    echo "==> The application log:"
    docker compose logs --no-color app || true
    echo
    echo "==> The Cerbos log:"
    docker compose logs --no-color cerbos || true
  fi
  docker compose down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Compares the identifiers in a response with an expected list.
#
#   expect_ids <name> <path> <query> <expected ids, separated by spaces>
expect_ids() {
  local name="$1" path="$2" query="$3" expected="$4"
  CHECKS=$((CHECKS + 1))

  local actual
  actual="$(curl -fsS "${BASE}${path}?${query}" | jq -r '.ids | join(" ")')" || {
    echo "FAIL  ${name}: the request failed"
    FAILURES=$((FAILURES + 1))
    return
  }

  if [[ "${actual}" == "${expected}" ]]; then
    echo "ok    ${name}"
  else
    echo "FAIL  ${name}"
    echo "        expected: [${expected}]"
    echo "        actual:   [${actual}]"
    FAILURES=$((FAILURES + 1))
  fi
}

# Compares the HTTP status and the class of the error with the expected values.
expect_status() {
  local name="$1" path="$2" query="$3" expected_status="$4" expected_error="$5"
  CHECKS=$((CHECKS + 1))

  local body status
  body="$(curl -sS -o /tmp/smoke-body -w '%{http_code}' "${BASE}${path}?${query}")"
  status="${body}"
  local error
  error="$(jq -r '.error // ""' </tmp/smoke-body)"

  if [[ "${status}" == "${expected_status}" && "${error}" == "${expected_error}" ]]; then
    echo "ok    ${name}"
  else
    echo "FAIL  ${name}"
    echo "        expected: ${expected_status} ${expected_error}"
    echo "        actual:   ${status} ${error}"
    FAILURES=$((FAILURES + 1))
  fi
}

echo "==> Cerbos ${CERBOS_VERSION}, Ruby ${RUBY_VERSION}"
docker compose up --build --detach --wait

echo
echo "== photos: view =="
# Ana owns ph-hero and ph-banner. She has no permitted tags, and thus she sees only her own
# photos. ph-globex is in a different tenant.
expect_ids "ana sees her own photos" /photos \
  "action=view&user=ana&tenant=acme" "ph-banner ph-hero"

# Ben owns ph-team and ph-draft. He also sees ph-banner, because it is published.
expect_ids "ben sees his own photos and the published photo" /photos \
  "action=view&user=ben&tenant=acme" "ph-banner ph-draft ph-team"

# The tag internal adds ph-hero for Ben. This tests the correlated EXISTS subquery through
# the join table.
expect_ids "a permitted tag adds a photo" /photos \
  "action=view&user=ben&tenant=acme&tags=internal" "ph-banner ph-draft ph-hero ph-team"

echo
echo "== photos: edit =="
expect_ids "only the owner edits" /photos \
  "action=edit&user=ana&tenant=acme" "ph-banner ph-hero"
expect_ids "ben edits his own photos" /photos \
  "action=edit&user=ben&tenant=acme" "ph-draft ph-team"

echo
echo "== photos: the tenant boundary applies to an unconditional allow =="
# The rule for moderate has no condition, and thus the plan is KIND_ALWAYS_ALLOWED. The
# Cerbos filter selects every row. The tenant boundary of the application is outside that
# filter, and thus it still removes the photos of the other tenant.
expect_ids "an administrator sees only the photos of their tenant" /photos \
  "action=moderate&user=ana&role=admin&tenant=acme" "ph-banner ph-draft ph-hero ph-team"
expect_ids "a different tenant gets different photos" /photos \
  "action=moderate&user=cara&role=admin&tenant=globex" "ph-globex"

echo
echo "== photos: a scalar path with dots =="
# The department is on the user and not on the photo. The adapter reads it with a correlated
# scalar subquery through the belongs_to association.
expect_ids "engineering sees the photos of ana" /photos \
  "action=view-same-department&user=ana&tenant=acme&department=engineering" "ph-banner ph-hero"
expect_ids "sales sees the photos of ben" /photos \
  "action=view-same-department&user=ben&tenant=acme&department=sales" "ph-draft ph-team"

echo
echo "== albums =="
expect_ids "ana owns one album and sees the shared album" /albums \
  "action=view&user=ana&tenant=acme" "al-launch al-team"
# Ben is a collaborator on al-launch. This tests membership through a join table.
expect_ids "a collaborator sees the album" /albums \
  "action=view&user=ben&tenant=acme" "al-launch al-team"
expect_ids "a user without a relation sees only the shared album" /albums \
  "action=view&user=zoe&tenant=acme" "al-team"
expect_ids "only the owner edits an album" /albums \
  "action=edit&user=ana&tenant=acme" "al-launch"

echo
echo "== workspaces: hierarchy =="
# hierarchy(R.attr.scope).descendentOf(hierarchy(P.attr.scope)) becomes a LIKE with an ESCAPE
# clause, because the scope of the principal is a constant and the scope of the row is a
# column.
expect_ids "a scope selects the workspaces below it" /workspaces \
  "action=view&user=zoe&tenant=acme&scope=acme.engineering" "w-platform"
expect_ids "a higher scope selects more workspaces" /workspaces \
  "action=view&user=zoe&tenant=acme&scope=acme" "w-platform w-sales"
expect_ids "the owner sees their workspace outside their scope" /workspaces \
  "action=view&user=ben&tenant=acme&scope=acme.engineering" "w-platform w-sales"

echo
echo "== the adapter is fail-closed =="
# The rule for search-regex uses matches(). No SQL dialect gives the behaviour of RE2. Thus
# the adapter raises an error and the application gives no rows. It does not give a filter
# that is only approximately correct.
expect_status "a regular expression raises an error" /photos \
  "action=search-regex&user=ana&tenant=acme" "422" "Cerbos::ActiveRecord::UnsupportedOperatorError"

echo
if [[ "${FAILURES}" -eq 0 ]]; then
  echo "All ${CHECKS} checks passed."
else
  echo "${FAILURES} of ${CHECKS} checks failed."
  exit 1
fi
