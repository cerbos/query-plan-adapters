#!/usr/bin/env bash
# Edge-case regression smoke test for the cerbos-spring-data example.
#
# Companion to smoke.sh (which owns the pedagogical scenario matrix and the PDP
# audit-log verification). This script is a full-stack regression tripwire: every
# assertion pins a historical adapter bug that was fixed on main and would have
# produced a DIFFERENT row set (or an HTTP 500) before its fix. The scenarios run
# against dedicated fixtures in the isolated "edge" tenant (SeedData.java) and the
# `edge-*` actions in policies/photo.yaml, so smoke.sh's expectations are untouched.
#
# Scenario -> historical bug map (details in the policy file and example README):
#   edge-ieee-eq / edge-ieee-ne  PR #274  algebraic eq/ne add-solve vs IEEE addition
#   edge-nan-ordering            PR #275  Double.compare total order vs IEEE NaN
#   edge-retention               PR #279  timestamp()/now()-duration() threw for every query
#   edge-bracket-title           PR #285  LIKE escaping missed SQL Server's [ class
#   edge-size-huge               PR #286  size() threshold >= 2^31 truncated by (int) cast
#   bulk-unsafe delete           PR #273  delete(Specification) collection-row corruption
#
# Pre-reqs: docker, curl, jq, gradle (8.x), JDK 17+.

set -euo pipefail

cd "$(dirname "$0")/.."

GREEN="\033[0;32m"; RED="\033[0;31m"; NC="\033[0m"
fail() { printf "${RED}FAIL${NC} %s\n" "$*" >&2; exit 1; }
ok()   { printf "${GREEN}OK${NC}   %s\n" "$*"; }

cleanup() {
    local status=$?
    if [[ -n "${APP_PID:-}" ]]; then
        kill "$APP_PID" 2>/dev/null || true
        wait "$APP_PID" 2>/dev/null || true
    fi
    if (( status != 0 )); then
        echo "==> edge-case smoke test failed (exit $status): Cerbos container logs" >&2
        docker compose logs --no-color cerbos >&2 || true
        if [[ -f build/smoke/edge-app.log ]]; then
            echo "==> edge-case smoke test failed (exit $status): last 200 lines of Spring Boot log" >&2
            tail -n 200 build/smoke/edge-app.log >&2 || true
        fi
    fi
    docker compose down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> docker compose up -d"
docker compose up -d

echo "==> waiting for Cerbos health"
for i in {1..30}; do
    if docker compose ps --format json cerbos | grep -q '"Health":"healthy"'; then break; fi
    sleep 1
done

echo "==> gradle bootRun (background)"
mkdir -p build/smoke
gradle bootRun --no-daemon >build/smoke/edge-app.log 2>&1 &
APP_PID=$!

echo "==> waiting for Spring Boot on :8080"
for i in {1..60}; do
    if curl -sS -o /dev/null "http://localhost:8080/" 2>/dev/null; then break; fi
    sleep 1
done
curl -sS -o /dev/null "http://localhost:8080/" || \
    { tail -40 build/smoke/edge-app.log; fail "Spring Boot didn't come up"; }

assert_ids() {
    local label=$1 url=$2 expected=$3
    local got
    got=$(curl -fsS "$url" | jq -r '[.[].id] | sort | join(",")')
    if [[ "$got" == "$expected" ]]; then
        ok "$label  => ${got:-<empty>}"
    else
        fail "$label  expected=${expected:-<empty>}  got=${got:-<empty>}"
    fi
}

EDGE="http://localhost:8080/photos?user=edge-user&tenant=edge"

# Edge fixtures (SeedData.java, tenant "edge"):
#   e1 "[SEC] Quarterly report"  public  score=NULL  createdAt=now-30d
#   e2 "Secret launch plan"     !public  score=NULL  createdAt=now-1h
#   e3 "Precision probe"         public  score=-0.6  createdAt=now-1h
#   e4 "Cold archive shot"      !public  score=2.5   createdAt=now-30d
#   e5 "Retention candidate"     public  score=NULL  createdAt=now-30d
#   e6 "Fresh upload"            public  score=NULL  createdAt=now-1h

# PR #274: `score + 0.7 == 0.1` has NO satisfying double (IEEE addition skips 0.1),
# so check() denies every row. The pre-fix algebraic solve emitted `score = -0.6`
# and returned e3.
assert_ids "edge/ieee-eq"      "$EDGE&action=edge-ieee-eq"      ""

# PR #274 (ne): pre-fix `score != -0.6` wrongly EXCLUDED e3; correct result is every
# non-null score whose IEEE sum differs from 0.1 — e3 AND e4.
assert_ids "edge/ieee-ne"      "$EDGE&action=edge-ieee-ne"      "e3,e4"

# PR #275: `(public ? 1.0 : 0.0/0.0) > 0.5` — NaN ordering is false in CEL/IEEE, so
# only public rows qualify. Pre-fix Double.compare made `NaN > 0.5` true and the
# non-public rows e2 and e4 leaked through.
assert_ids "edge/nan-ordering" "$EDGE&action=edge-nan-ordering" "e1,e3,e5,e6"

# PR #279: retention window `timestamp(createdAt) < now() - duration("24h")` — the
# rows older than 24h. Pre-fix this action was an HTTP 500 on every request.
assert_ids "edge/retention"    "$EDGE&action=edge-retention"    "e1,e4,e5"

# PR #285: startsWith("[SEC]") — the escaped LIKE pattern must literal-match e1 and
# never match the class-trap row e2 ("Secret..." starts with a character in {S,E,C}).
assert_ids "edge/bracket"      "$EDGE&action=edge-bracket-title" "e1"

# PR #286: size(title) > 4294967296 — impossible, so zero rows. Pre-fix the (int)
# cast wrapped the threshold to 0 and every non-empty title matched.
assert_ids "edge/size-huge"    "$EDGE&action=edge-size-huge"    ""

# PR #273: delete(Specification) with a Relation-mapped predicate must be refused by
# the adapter's bulk-delete guard (surfaced by the demo endpoint as HTTP 409) instead
# of silently destroying collection rows while deleting zero photos.
DELETE_BODY=$(mktemp)
DELETE_STATUS=$(curl -sS -X DELETE -o "$DELETE_BODY" -w '%{http_code}' \
    "http://localhost:8080/photos/bulk-unsafe?user=alice&action=comment")
if [[ "$DELETE_STATUS" != "409" ]]; then
    cat "$DELETE_BODY" >&2
    fail "bulk-unsafe delete: expected HTTP 409 from the guard, got HTTP $DELETE_STATUS"
fi
if ! grep -q "SELECT-only" "$DELETE_BODY"; then
    cat "$DELETE_BODY" >&2
    fail "bulk-unsafe delete: 409 body did not contain the guard's SELECT-only message"
fi
rm -f "$DELETE_BODY"
ok "bulk-unsafe/guard  => HTTP 409 with guard message"

# Integrity check: the guard must have fired BEFORE any SQL ran. The photo rows and —
# crucially — the tag/label/grant collection rows (the pre-#273 corruption target)
# must all still produce the same row sets smoke.sh pins.
assert_ids "bulk-unsafe/photos-intact" \
    "http://localhost:8080/photos?user=alice&action=comment" "p1,p2,p5,p6,p7,p8"
assert_ids "bulk-unsafe/grants-intact" \
    "http://localhost:8080/photos?user=alice&action=group-grant&groups=finance" "p2,p7"
assert_ids "bulk-unsafe/labels-intact" \
    "http://localhost:8080/photos?user=alice&action=needs-moderation" "p2,p3"

echo
ok "all edge-case regression assertions passed"
