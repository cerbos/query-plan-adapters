#!/usr/bin/env bash
# End-to-end smoke test for the cerbos-spring-data photo-sharing example.
#
# Brings up the Cerbos PDP via docker compose, starts the Spring Boot app, and
# hits the REST endpoint with a handful of (user, role, action) tuples. Each
# request triggers a real PlanResources call to the PDP container — the audit
# log in `docker compose logs cerbos` then proves what plan the adapter saw.
#
# Pre-reqs: docker, curl, jq, gradle (8.x), JDK 17+.

set -euo pipefail

cd "$(dirname "$0")/.."

GREEN="\033[0;32m"; RED="\033[0;31m"; NC="\033[0m"
fail() { printf "${RED}FAIL${NC} %s\n" "$*" >&2; exit 1; }
ok()   { printf "${GREEN}OK${NC}   %s\n" "$*"; }

cleanup() {
    if [[ -n "${APP_PID:-}" ]]; then
        kill "$APP_PID" 2>/dev/null || true
        wait "$APP_PID" 2>/dev/null || true
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
gradle bootRun --no-daemon >build/smoke/app.log 2>&1 &
APP_PID=$!

echo "==> waiting for Spring Boot on :8080"
for i in {1..60}; do
    if curl -fsS "http://localhost:8080/photos?user=alice" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "http://localhost:8080/photos?user=alice" >/dev/null || \
    { tail -40 build/smoke/app.log; fail "Spring Boot didn't come up"; }

assert_ids() {
    local label=$1 url=$2 expected=$3
    local got
    got=$(curl -fsS "$url" | jq -r '[.[].id] | sort | join(",")')
    if [[ "$got" == "$expected" ]]; then
        ok "$label  => $got"
    else
        fail "$label  expected=$expected  got=$got"
    fi
}

# Seed data (from SeedData.java):
#   p1 alice public  !arch tags=travel,sunset
#   p2 alice private !arch tags=friends,food
#   p3 bob   public   arch tags=wedding
#   p4 bob   private !arch tags=portrait
#   p5 charlie public !arch tags=travel,outdoors,friends
#   p6 alice private  arch tags=legacy
#
# view (user)  : (public AND !archived) OR ownerId == self
# edit (user)  : ownerId == self
# comment(user): (public AND !archived) OR "friends" in tags OR ownerId == self
# any (admin)  : ALWAYS_ALLOWED  =>  all 6

assert_ids "alice/view"        "http://localhost:8080/photos?user=alice&action=view"          "p1,p2,p5,p6"
assert_ids "alice/edit"        "http://localhost:8080/photos?user=alice&action=edit"          "p1,p2,p6"
assert_ids "alice/comment"     "http://localhost:8080/photos?user=alice&action=comment"       "p1,p2,p5,p6"
assert_ids "bob/view"          "http://localhost:8080/photos?user=bob&action=view"            "p1,p3,p4,p5"
assert_ids "bob/edit"          "http://localhost:8080/photos?user=bob&action=edit"            "p3,p4"
assert_ids "charlie/comment"   "http://localhost:8080/photos?user=charlie&action=comment"     "p1,p2,p5"
assert_ids "admin/view"        "http://localhost:8080/photos?user=admin&role=admin&action=view"    "p1,p2,p3,p4,p5,p6"
assert_ids "admin/delete"      "http://localhost:8080/photos?user=admin&role=admin&action=delete"  "p1,p2,p3,p4,p5,p6"

echo
echo "==> PDP decision-log tail (proves PlanResources was hit, not stubbed):"
docker compose logs --tail=20 cerbos | grep -E '"planResources"|callId' || true

echo
ok "all assertions passed"
