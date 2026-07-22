#!/usr/bin/env bash
# End-to-end smoke test for the cerbos-spring-data multi-resource example.
#
# Brings up the Cerbos PDP via docker compose, starts the Spring Boot app, and
# hits three resource endpoints with a matrix of principal, tenant, role, and action tuples. Each
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
    if curl -sS -o /dev/null "http://localhost:8080/" 2>/dev/null; then break; fi
    sleep 1
done
curl -sS -o /dev/null "http://localhost:8080/" || \
    { tail -40 build/smoke/app.log; fail "Spring Boot didn't come up"; }

# Audit records are the independent proof that the HTTP request reached the PDP. The readiness
# probe uses an unmapped route so it does not authorize; existing container logs are baselined.
pdp_plan_records() {
    docker compose logs --no-color --no-log-prefix cerbos 2>/dev/null |
        jq -cR 'fromjson? | select(
            .["log.logger"] == "cerbos.audit" and
            .["log.kind"] == "decision" and
            .planResources != null
        )'
}

pdp_access_records() {
    docker compose logs --no-color --no-log-prefix cerbos 2>/dev/null |
        jq -cR 'fromjson? | select(
            .["log.logger"] == "cerbos.audit" and
            .["log.kind"] == "access" and
            ((.method // "") | endswith("/PlanResources"))
        )'
}

pdp_plan_count() {
    pdp_plan_records | wc -l | tr -d ' '
}

pdp_access_count() {
    pdp_access_records | wc -l | tr -d ' '
}

pdp_records_since() {
    local baseline=$1
    pdp_plan_records | tail -n "+$((baseline + 1))"
}

pdp_access_records_since() {
    local baseline=$1
    pdp_access_records | tail -n "+$((baseline + 1))"
}

PDP_BASELINE=$(pdp_plan_count)
PDP_ACCESS_BASELINE=$(pdp_access_count)

action_from_url() {
    local url=$1 action
    action=${url#*action=}
    if [[ "$action" == "$url" ]]; then
        fail "authorization assertion URL has no action: $url"
    fi
    printf '%s' "${action%%&*}"
}

resource_from_url() {
    local url=$1 path
    path=${url#*://}
    path=${path#*/}
    path=${path%%\?*}
    path=${path%%/*}
    case "$path" in
        photos) printf 'photo' ;;
        albums) printf 'album' ;;
        workspaces) printf 'workspace' ;;
        *) fail "authorization assertion URL has unknown resource path: $url" ;;
    esac
}

verify_pdp_call() {
    local label=$1 expected_action=$2 expected_resource=$3 access_before=$4 decision_before=$5
    local access_target decision_target access_count decision_count
    local access_record decision_record access_id decision_id observed_action observed_resource
    local filter_kind
    access_target=$((access_before + 1))
    decision_target=$((decision_before + 1))

    for _ in {1..50}; do
        access_count=$(pdp_access_count)
        decision_count=$(pdp_plan_count)
        if (( access_count > access_target || decision_count > decision_target )); then
            fail "$label made multiple PDP calls: accesses=$access_before->$access_count" \
                "decisions=$decision_before->$decision_count"
        fi
        if (( access_count == access_target && decision_count == decision_target )); then
            access_record=$(pdp_access_records | tail -1)
            decision_record=$(pdp_plan_records | tail -1)
            access_id=$(jq -r '.callId' <<<"$access_record")
            decision_id=$(jq -r '.callId' <<<"$decision_record")
            observed_action=$(jq -r '.planResources.input.actions | join(",")' \
                <<<"$decision_record")
            observed_resource=$(jq -r '.planResources.input.resource.kind' \
                <<<"$decision_record")
            filter_kind=$(jq -r '.planResources.output.filter.kind // empty' \
                <<<"$decision_record")
            if [[ "$access_id" != "$decision_id" ]]; then
                fail "$label PDP access/decision call IDs differ: $access_id != $decision_id"
            fi
            if [[ "$observed_action" != "$expected_action" ]]; then
                fail "$label PDP action mismatch: expected=$expected_action got=$observed_action"
            fi
            if [[ "$observed_resource" != "$expected_resource" ]]; then
                fail "$label PDP resource mismatch: expected=$expected_resource got=$observed_resource"
            fi
            if [[ -z "$filter_kind" ]]; then
                fail "$label PDP decision had no query-plan filter output"
            fi
            return
        fi
        sleep 0.1
    done
    fail "$label PDP audit timeout: accesses=$access_before->$access_count" \
        "decisions=$decision_before->$decision_count"
}

assert_ids() {
    local label=$1 url=$2 expected=$3
    local got access_before decision_before action resource
    access_before=$(pdp_access_count)
    decision_before=$(pdp_plan_count)
    action=$(action_from_url "$url")
    resource=$(resource_from_url "$url")
    got=$(curl -fsS "$url" | jq -r '[.[].id] | sort | join(",")')
    if [[ "$got" == "$expected" ]]; then
        ok "$label  => $got"
    else
        fail "$label  expected=$expected  got=$got"
    fi
    verify_pdp_call "$label" "$action" "$resource" "$access_before" "$decision_before"
}

assert_page() {
    local label=$1 url=$2 expected_ids=$3 expected_total=$4 expected_pages=$5
    local response got_ids got_total got_pages access_before decision_before action resource
    access_before=$(pdp_access_count)
    decision_before=$(pdp_plan_count)
    action=$(action_from_url "$url")
    resource=$(resource_from_url "$url")
    response=$(curl -fsS "$url")
    got_ids=$(jq -r '[.content[].id] | join(",")' <<<"$response")
    got_total=$(jq -r '.totalElements' <<<"$response")
    got_pages=$(jq -r '.totalPages' <<<"$response")
    if [[ "$got_ids" == "$expected_ids" && "$got_total" == "$expected_total" && \
          "$got_pages" == "$expected_pages" ]]; then
        ok "$label  => ids=$got_ids total=$got_total pages=$got_pages"
    else
        fail "$label  expected=ids:$expected_ids,total:$expected_total,pages:$expected_pages" \
            "got=ids:$got_ids,total:$got_total,pages:$got_pages"
    fi
    verify_pdp_call "$label" "$action" "$resource" "$access_before" "$decision_before"
}

assert_status() {
    local label=$1 url=$2 expected=$3
    local got
    got=$(curl -sS -o /dev/null -w '%{http_code}' "$url")
    if [[ "$got" == "$expected" ]]; then
        ok "$label  => HTTP $got"
    else
        fail "$label  expected=HTTP:$expected got=HTTP:$got"
    fi
}

# Seed data (from SeedData.java):
#   p1 alice   public  !arch location=Lisbon tags=travel,sunset labels=editorial
#   p2 alice   private !arch location=NULL   tags=friends,food   labels=safety,faces
#   p3 bob     public   arch location=Paris   tags=wedding        labels=safety
#   p4 bob     private !arch location=Studio tags=portrait       labels=[]
#   p5 charlie public  !arch location=Alps   tags=travel,...     labels=quality,safety
#   p6 alice   private  arch location=Home   tags=legacy         labels=[]
#   p7 dana    public  !arch location=NULL   tags=[] title contains literal "%"
#   p8 erin    public  !arch location=Tokyo  tags=unicode,... title contains literal "_"
#   p9 globex-user private arch tenant=globex (cross-tenant control row)
#
# Grants deliberately include null subjects, duplicate matches, wrong permissions, and one
# grant whose tenant disagrees with its photo. Group IDs are tenant-qualified in the database.
#
# view (user)  : (public AND !archived) OR ownerId == self
# edit (user)  : ownerId == self
# comment(user): (public AND !archived) OR "friends" in tags OR ownerId == self
# any (admin)  : ALWAYS_ALLOWED  =>  all 8

assert_ids "alice/view"        "http://localhost:8080/photos?user=alice&action=view"          "p1,p2,p5,p6,p7,p8"
assert_ids "alice/edit"        "http://localhost:8080/photos?user=alice&action=edit"          "p1,p2,p6"
assert_ids "alice/comment"     "http://localhost:8080/photos?user=alice&action=comment"       "p1,p2,p5,p6,p7,p8"
assert_ids "bob/view"          "http://localhost:8080/photos?user=bob&action=view"            "p1,p3,p4,p5,p7,p8"
assert_ids "bob/edit"          "http://localhost:8080/photos?user=bob&action=edit"            "p3,p4"
assert_ids "charlie/comment"   "http://localhost:8080/photos?user=charlie&action=comment"     "p1,p2,p5,p7,p8"
assert_ids "admin/view"        "http://localhost:8080/photos?user=admin&role=admin&action=view"    "p1,p2,p3,p4,p5,p6,p7,p8"
assert_ids "admin/delete"      "http://localhost:8080/photos?user=admin&role=admin&action=delete"  "p1,p2,p3,p4,p5,p6,p7,p8"

# The application-owned tenant Specification composes outside every Cerbos result kind.
assert_ids "admin/globex"      "http://localhost:8080/photos?user=admin&role=admin&tenant=globex&action=view" "p9"
assert_ids "alice/view/rating" "http://localhost:8080/photos?user=alice&action=view&minRating=5" "p1,p5"

# Scalar/nested comparisons, NULL handling, principal list attributes, and empty collections.
assert_ids "discover"          "http://localhost:8080/photos?user=alice&action=discover"                 "p1,p3,p5,p7,p8"
assert_ids "located"           "http://localhost:8080/photos?user=alice&action=located"                  "p1,p3,p4,p5,p6,p8"
assert_ids "similar/travel"    "http://localhost:8080/photos?user=alice&action=similar&interests=travel" "p1,p5"
assert_ids "similar/multiple"  "http://localhost:8080/photos?user=alice&action=similar&interests=travel,food" "p1,p2,p5"
assert_ids "similar/empty"     "http://localhost:8080/photos?user=alice&action=similar"                  ""

# Enterprise grants combine direct users, tenant-qualified groups, nullable child fields,
# child-to-parent tenant integrity, duplicate matching rows, and the mandatory tenant fence.
assert_ids "delegated/direct" \
    "http://localhost:8080/photos?user=alice&action=delegated-view" "p3"
assert_ids "delegated/groups" \
    "http://localhost:8080/photos?user=alice&action=delegated-view&groups=finance,engineering" \
    "p2,p3,p5"
assert_ids "delegated/finance" \
    "http://localhost:8080/photos?user=dana&action=delegated-view&groups=finance" "p2"
assert_ids "delegated/mismatched-tenant" \
    "http://localhost:8080/photos?user=dana&action=delegated-view&groups=legal" ""
assert_ids "delegated/globex" \
    "http://localhost:8080/photos?user=alice&tenant=globex&action=delegated-view&groups=finance" \
    "p9"

# Null-only grant members are UNKNOWN, not false. Guarding null explicitly changes negation.
assert_ids "grant/positive-unknown" \
    "http://localhost:8080/photos?user=alice&action=group-grant&groups=finance" "p2,p7"
assert_ids "grant/negated-unknown" \
    "http://localhost:8080/photos?user=alice&action=no-group-grant&groups=finance" "p4,p5,p6"
assert_ids "grant/guarded-negation" \
    "http://localhost:8080/photos?user=alice&action=no-group-grant-safe&groups=finance" \
    "p1,p3,p4,p5,p6,p8"

# Structured @OneToMany label relation: exists, all, size, and exists_one.
assert_ids "needs-moderation"  "http://localhost:8080/photos?user=alice&action=needs-moderation"   "p2,p3"
assert_ids "fully-reviewed"    "http://localhost:8080/photos?user=alice&action=fully-reviewed"     "p1,p3,p4,p6,p7,p8"
assert_ids "unlabelled"        "http://localhost:8080/photos?user=alice&action=unlabelled"          "p4,p6,p7,p8"
assert_ids "exactly-one"       "http://localhost:8080/photos?user=alice&action=exactly-one-reviewed" "p1,p2,p3,p5"

# LIKE wildcards must be escaped as literals by the adapter.
assert_ids "literal-percent"   "http://localhost:8080/photos?user=alice&action=percent-title"      "p7"
assert_ids "literal-underscore" "http://localhost:8080/photos?user=alice&action=underscore-title"  "p8"

# An action with no matching rule becomes KIND_ALWAYS_DENIED.
assert_ids "always-denied"     "http://localhost:8080/photos?user=alice&action=publish" ""

# Relation predicates must also survive Spring Data's paginated content and count queries.
assert_page "moderation/page-0" \
    "http://localhost:8080/photos/page?user=alice&action=needs-moderation&page=0&size=1" \
    "p2" "2" "2"
assert_page "moderation/page-1" \
    "http://localhost:8080/photos/page?user=alice&action=needs-moderation&page=1&size=1" \
    "p3" "2" "2"
assert_page "delegated/page-0" \
    "http://localhost:8080/photos/page?user=alice&action=delegated-view&groups=finance,engineering&page=0&size=2" \
    "p2,p3" "3" "2"
assert_page "delegated/page-1" \
    "http://localhost:8080/photos/page?user=alice&action=delegated-view&groups=finance,engineering&page=1&size=2" \
    "p5" "3" "2"

# Independent persistence and authorization paths for two additional Cerbos resource kinds.
assert_ids "album/alice-view" \
    "http://localhost:8080/albums?user=alice&action=view" "a1,a2"
assert_ids "album/bob-manage" \
    "http://localhost:8080/albums?user=bob&action=manage" "a2"
assert_ids "album/admin-globex" \
    "http://localhost:8080/albums?user=admin&role=admin&tenant=globex&action=view" "a3"
assert_ids "album/always-denied" \
    "http://localhost:8080/albums?user=alice&action=publish" ""

assert_ids "workspace/alice-access" \
    "http://localhost:8080/workspaces?user=alice&action=access" "w1"
assert_ids "workspace/bob-administer" \
    "http://localhost:8080/workspaces?user=bob&action=administer" "w1"
assert_ids "workspace/admin-globex" \
    "http://localhost:8080/workspaces?user=admin&role=admin&tenant=globex&action=access" "w3"
assert_ids "workspace/always-denied" \
    "http://localhost:8080/workspaces?user=alice&action=publish" ""

# Prove every successful HTTP assertion above made exactly one PlanResources call with the
# expected resource/action pair. The full multiset cannot confuse the same action across kinds.
EXPECTED_PLAN_PAIRS=$(printf '%s\n' \
    photo/view photo/view photo/view photo/view photo/view \
    photo/edit photo/edit \
    photo/comment photo/comment \
    photo/delete \
    photo/discover \
    photo/located \
    photo/similar photo/similar photo/similar \
    photo/delegated-view photo/delegated-view photo/delegated-view photo/delegated-view \
    photo/delegated-view photo/delegated-view photo/delegated-view \
    photo/group-grant photo/no-group-grant photo/no-group-grant-safe \
    photo/needs-moderation photo/needs-moderation photo/needs-moderation \
    photo/fully-reviewed \
    photo/unlabelled \
    photo/exactly-one-reviewed \
    photo/percent-title \
    photo/underscore-title \
    photo/publish \
    album/view album/view album/manage album/publish \
    workspace/access workspace/access workspace/administer workspace/publish | sort)
EXPECTED_PLAN_CALLS=$(printf '%s\n' "$EXPECTED_PLAN_PAIRS" | wc -l | tr -d ' ')

OBSERVED_ACCESS_CALLS=$(( $(pdp_access_count) - PDP_ACCESS_BASELINE ))
if (( OBSERVED_ACCESS_CALLS != EXPECTED_PLAN_CALLS )); then
    fail "PDP access-log count mismatch: expected=$EXPECTED_PLAN_CALLS got=$OBSERVED_ACCESS_CALLS"
fi

OBSERVED_PLAN_PAIRS=$(pdp_records_since "$PDP_BASELINE" |
    jq -r '"\(.planResources.input.resource.kind)/\(.planResources.input.actions | join(","))"' |
    sort)
if [[ "$OBSERVED_PLAN_PAIRS" != "$EXPECTED_PLAN_PAIRS" ]]; then
    diff -u <(printf '%s\n' "$EXPECTED_PLAN_PAIRS") \
        <(printf '%s\n' "$OBSERVED_PLAN_PAIRS") >&2 || true
    fail "PDP resource/action multiset did not match the HTTP scenarios"
fi

MISSING_FILTERS=$(pdp_records_since "$PDP_BASELINE" |
    jq -s '[.[] | select(.planResources.output.filter.kind == null)] | length')
if [[ "$MISSING_FILTERS" != "0" ]]; then
    fail "$MISSING_FILTERS PDP decision records had no query-plan filter output"
fi

EXPECTED_RESOURCE_KINDS=$(printf '%s\n' album photo workspace)
OBSERVED_RESOURCE_KINDS=$(pdp_records_since "$PDP_BASELINE" |
    jq -r '.planResources.input.resource.kind' | sort -u)
if [[ "$OBSERVED_RESOURCE_KINDS" != "$EXPECTED_RESOURCE_KINDS" ]]; then
    diff -u <(printf '%s\n' "$EXPECTED_RESOURCE_KINDS") \
        <(printf '%s\n' "$OBSERVED_RESOURCE_KINDS") >&2 || true
    fail "PDP resource-kind set did not include exactly album, photo, and workspace"
fi

ACCESS_CALL_IDS=$(pdp_access_records_since "$PDP_ACCESS_BASELINE" | jq -r '.callId' | sort)
DECISION_CALL_IDS=$(pdp_records_since "$PDP_BASELINE" | jq -r '.callId' | sort)
if [[ "$ACCESS_CALL_IDS" != "$DECISION_CALL_IDS" ]]; then
    diff -u <(printf '%s\n' "$ACCESS_CALL_IDS") \
        <(printf '%s\n' "$DECISION_CALL_IDS") >&2 || true
    fail "PDP PlanResources access and decision records did not share the same call IDs"
fi
ok "HTTP -> service -> PDP  => $EXPECTED_PLAN_CALLS calls across 3 resource kinds"

# Rejected pagination requests stop in the controller and must not call the PDP. A subsequent
# valid sentinel is the audit flush barrier: the complete delta must contain only that sentinel.
PDP_BEFORE_REJECTED=$(pdp_plan_count)
PDP_ACCESS_BEFORE_REJECTED=$(pdp_access_count)
assert_status "pagination/negative-page" \
    "http://localhost:8080/photos/page?user=alice&page=-1&size=1" "400"
assert_status "pagination/oversized" \
    "http://localhost:8080/photos/page?user=alice&page=0&size=101" "400"
assert_status "filter/invalid-rating" \
    "http://localhost:8080/photos?user=alice&minRating=6" "400"
SENTINEL_RESPONSE=$(curl -fsS \
    "http://localhost:8080/photos?user=smoke&action=audit-sentinel" |
    jq -r 'length')
if [[ "$SENTINEL_RESPONSE" != "0" ]]; then
    fail "audit sentinel unexpectedly returned $SENTINEL_RESPONSE photos"
fi
verify_pdp_call "audit-sentinel" "audit-sentinel" "photo" \
    "$PDP_ACCESS_BEFORE_REJECTED" "$PDP_BEFORE_REJECTED"
ok "controller rejection -> no PDP call"

echo
echo "==> Sample verified PDP PlanResources decisions:"
pdp_records_since "$PDP_BASELINE" | tail -3 |
    jq -c '{callId, resource: .planResources.input.resource.kind, action: .planResources.input.actions, filter: .planResources.output.filter.kind}'

echo
ok "all HTTP, database result, and PDP audit assertions passed"
