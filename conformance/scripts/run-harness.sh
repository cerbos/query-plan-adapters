#!/usr/bin/env bash
# Runs an adapter's conformance harness locally, one store leg at a time, the way its workflow does:
# it starts the store the harness needs, runs the harness, and tears the store down again.
#
#   conformance/scripts/run-harness.sh <adapter> [store...]   # default: every store leg CI runs
#   conformance/scripts/run-harness.sh --all                   # every adapter, one after another
#   conformance/scripts/run-harness.sh --list                  # the roster and each adapter's stores
#
# Stores per adapter (the first is the default when CI runs only one):
#   drizzle             sqlite postgres mysql
#   prisma              sqlite-v7 sqlite-v6 postgres-v7 postgres-v6 mysql-v7 mysql-v6
#   mongoose            mongo mongo-next
#   langchain-chromadb  chroma
#   convex              convex
#   sqlalchemy          all                      (one pytest run covers every store)
#   activerecord        sqlite postgres mysql    (ACTIVERECORD_VERSION / RUBY_VERSION pass through)
#   ent, pgx            all                      (the Go harness starts its own containers)
#   elasticsearch-java  elasticsearch elasticsearch-next
#   spring-data         h2 postgres mysql mysql-server-prep   (ADAPTER_TEST_ORM passes through)
#
# Legs run one at a time on purpose: several harnesses at once overload a laptop into timeouts and
# out-of-memory kills that read as failures. Docker is needed for every adapter but SQLite-only
# legs. Without a JDK on PATH the Java adapters run Gradle in eclipse-temurin:21-jdk.
#
# Caches (Gradle, pytest's basetemp) go under QPA_CACHE_DIR, default ~/.cache/query-plan-adapters,
# not /tmp, which is often a small tmpfs.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CACHE_DIR="${QPA_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME}/.cache}/query-plan-adapters}"
# The JDK the Java adapters build in when none is on PATH. A toolchain, not a service the harness
# talks to, so it is pinned here rather than in an <adapter>/*_IMAGE file.
TEMURIN_IMAGE="eclipse-temurin:21-jdk@sha256:3e3c176ffed168beb42c607be9bc1639b466cf00261a0fb04425562c9d0c5c2b"
mkdir -p "${CACHE_DIR}"

roster() {
  find "${REPO_ROOT}" -mindepth 2 -maxdepth 2 -name conformance-ledger.json \
    | xargs -n1 dirname | xargs -n1 basename | sort
}

stores_for() {
  case "$1" in
    drizzle) echo "sqlite postgres mysql" ;;
    prisma) echo "sqlite-v7 sqlite-v6 postgres-v7 postgres-v6 mysql-v7 mysql-v6" ;;
    mongoose) echo "mongo mongo-next" ;;
    langchain-chromadb) echo "chroma" ;;
    convex) echo "convex" ;;
    sqlalchemy | ent | pgx) echo "all" ;;
    activerecord) echo "sqlite postgres mysql" ;;
    elasticsearch-java) echo "elasticsearch elasticsearch-next" ;;
    spring-data) echo "h2 postgres mysql mysql-server-prep" ;;
    *) return 1 ;;
  esac
}

die() { echo "run-harness: $*" >&2; exit 2; }

# A container this script started, removed on exit whatever happens.
CONTAINERS=()
cleanup() {
  for name in "${CONTAINERS[@]}"; do docker rm -f "${name}" >/dev/null 2>&1 || true; done
  CONTAINERS=()
}
trap cleanup EXIT INT TERM

wait_for() { # <description> <command...>: up to 60 tries, a second apart
  local what="$1"; shift
  for _ in $(seq 1 60); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "run-harness: ${what} did not become ready" >&2
  return 1
}

npm_ready() { [[ -d node_modules ]] || npm ci --no-audit --no-fund; }

gradle() { # <adapter> <gradle args...>, with the environment passed through
  local adapter="$1"; shift
  if command -v java >/dev/null 2>&1; then
    ./gradlew "$@" --no-daemon
    return
  fi
  local home="${CACHE_DIR}/gradle-${adapter}"
  mkdir -p "${home}"
  local env_args=()
  for var in ELASTICSEARCH_IMAGE_FILE ADAPTER_TEST_ORM ADAPTER_TEST_DB ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS \
    ADAPTER_TEST_POSTGRES_INITDB_ARGS ADAPTER_TEST_MYSQL_COLLATION; do
    [[ -n "${!var:-}" ]] && env_args+=(-e "${var}=${!var}")
  done
  docker run --rm --network host --user "$(id -u):$(id -g)" \
    --group-add "$(stat -c %g /var/run/docker.sock)" \
    -e HOME=/tmp/home -e GRADLE_USER_HOME=/gradle "${env_args[@]}" \
    -v "${home}:/gradle" -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${REPO_ROOT}:${REPO_ROOT}" -w "${REPO_ROOT}/${adapter}" "${TEMURIN_IMAGE}" \
    sh -c 'mkdir -p /tmp/home && exec ./gradlew "$@"' gradle "$@" --no-daemon
}

run_leg() { # <adapter> <store>
  local adapter="$1" store="$2"
  cd "${REPO_ROOT}/${adapter}" || return 1
  case "${adapter}:${store}" in
    drizzle:sqlite) npm_ready && npm run test:adversarial ;;
    drizzle:postgres | drizzle:mysql) npm_ready && npm run "test:adversarial:${store}" ;;

    prisma:sqlite-v*) npm_ready && npm run "test:adversarial:${store#sqlite-}" ;;
    prisma:postgres-v* | prisma:mysql-v*) npm_ready && npm run "test:adversarial:${store%-v*}:${store##*-}" ;;

    mongoose:mongo | mongoose:mongo-next)
      local file=MONGO_IMAGE
      [[ "${store}" == mongo-next ]] && file=MONGO_NEXT_IMAGE
      npm_ready || return 1
      CONTAINERS+=(qpa-harness-mongo)
      docker run -d --rm --name qpa-harness-mongo -p 27017:27017 "$(cat "${file}")" >/dev/null || return 1
      wait_for MongoDB docker exec qpa-harness-mongo mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' \
        && npm run test:adversarial
      ;;

    langchain-chromadb:chroma)
      npm_ready || return 1
      CONTAINERS+=(qpa-harness-chroma)
      docker run -d --rm --name qpa-harness-chroma -p 8234:8000 "$(cat CHROMA_IMAGE)" >/dev/null || return 1
      wait_for Chroma curl -sf http://127.0.0.1:8234/api/v2/heartbeat \
        && CHROMA_URL=http://127.0.0.1:8234 npm run test:adversarial
      ;;

    convex:convex)
      npm_ready || return 1
      docker compose up -d || return 1
      local status=0
      if wait_for "the Convex backend" curl -sf http://127.0.0.1:3210/version; then
        local key
        key="$(docker compose exec -T backend ./generate_admin_key.sh 2>/dev/null | tail -n 1)"
        CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY="${key}" npx convex deploy -y \
          && CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY="${key}" npx convex codegen \
          && CONVEX_URL=http://127.0.0.1:3210 npm run test:adversarial || status=$?
      else
        status=1
      fi
      docker compose down -v >/dev/null 2>&1
      return "${status}"
      ;;

    sqlalchemy:all)
      local basetemp="${CACHE_DIR}/sqlalchemy-pytest"
      local status=0
      if command -v pdm >/dev/null 2>&1; then
        pdm run pytest tests/test_adversarial_conformance.py --basetemp "${basetemp}" || status=$?
      elif [[ -x .venv/bin/pytest ]]; then
        .venv/bin/pytest tests/test_adversarial_conformance.py --basetemp "${basetemp}" || status=$?
      else
        die "sqlalchemy needs pdm, or a .venv from 'pdm install -G :all'"
      fi
      rm -rf "${basetemp}"
      return "${status}"
      ;;

    activerecord:sqlite | activerecord:postgres | activerecord:mysql)
      ADAPTER_TEST_DB="${store}" ./scripts/test.sh spec/conformance_spec.rb ;;

    ent:all | pgx:all) go test -count=1 -run TestAdversarialConformance -timeout 30m ./... ;;

    elasticsearch-java:elasticsearch)
      ELASTICSEARCH_IMAGE_FILE=ELASTICSEARCH_IMAGE gradle "${adapter}" test --tests '*ElasticsearchAdversarialConformanceTest*' ;;
    elasticsearch-java:elasticsearch-next)
      ELASTICSEARCH_IMAGE_FILE=ELASTICSEARCH_NEXT_IMAGE gradle "${adapter}" test --tests '*ElasticsearchAdversarialConformanceTest*' ;;

    spring-data:h2) gradle "${adapter}" test --tests '*AdversarialConformanceTest' ;;
    spring-data:postgres | spring-data:mysql)
      ADAPTER_TEST_DB="${store}" gradle "${adapter}" test --tests '*AdversarialConformanceTest' ;;
    spring-data:mysql-server-prep)
      ADAPTER_TEST_DB=mysql ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS=true \
        gradle "${adapter}" test --tests '*AdversarialConformanceTest' ;;

    *) die "no recipe for ${adapter} / ${store}" ;;
  esac
}

RESULTS=()
run_adapter() { # <adapter> [store...]
  local adapter="$1"; shift
  local known
  known="$(stores_for "${adapter}")" \
    || die "'${adapter}' is not in this script; the roster is: $(roster | xargs)"
  local stores=("$@")
  [[ "${#stores[@]}" -gt 0 ]] || read -r -a stores <<<"${known}"
  for store in "${stores[@]}"; do
    [[ " ${known} " == *" ${store} "* ]] || die "${adapter} has no store '${store}' (stores: ${known})"
  done
  for store in "${stores[@]}"; do
    echo "==> ${adapter} / ${store}" >&2
    # A subshell, so a leg's cd and variables stay in it; it inherits no trap, so it sets its own.
    if (trap cleanup EXIT INT TERM; run_leg "${adapter}" "${store}"); then
      RESULTS+=("pass  ${adapter} / ${store}")
    else
      RESULTS+=("FAIL  ${adapter} / ${store}")
    fi
    cleanup
  done
}

case "${1:-}" in
  "" | -h | --help) sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
  --list) for adapter in $(roster); do printf '%-20s %s\n' "${adapter}" "$(stores_for "${adapter}" || echo '(not in this script)')"; done; exit 0 ;;
  --all)
    for adapter in $(roster); do
      stores_for "${adapter}" >/dev/null || die "${adapter} has a ledger but no entry in this script: add it to stores_for and run_leg"
    done
    for adapter in $(roster); do run_adapter "${adapter}"; done
    ;;
  *) run_adapter "$@" ;;
esac

echo >&2
printf '%s\n' "${RESULTS[@]}" >&2
[[ " ${RESULTS[*]} " != *"FAIL "* ]]
