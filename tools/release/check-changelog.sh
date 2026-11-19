#!/usr/bin/env bash
# Usage: check-changelog.sh <base> [head]
# Fails when a change to an adapter's shipped code between <base> and <head> leaves that adapter's CHANGELOG.md untouched.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
base="${1:?usage: check-changelog.sh <base> [head]}"
head="${2:-HEAD}"

shipped() {
  case "$1" in
    */example/* | *.test.ts | */src/corpus.ts | */src/test-setup.* | */src/generated/* | *_test.go | */testdata/*) return 1 ;;
    prisma/src/* | prisma/generator/* | mongoose/src/* | drizzle/src/* | convex/src/* | langchain-chromadb/src/*) return 0 ;;
    ent/*.go | pgx/*.go) return 0 ;;
    sqlalchemy/src/*) return 0 ;;
    activerecord/lib/* | sequel/lib/* | mongodb-ruby/lib/*) return 0 ;;
    efcore/src/*) return 0 ;;
    jpa/src/main/* | spring-data/src/main/* | exposed/src/main/* | elasticsearch-java/src/main/*) return 0 ;;
  esac
  return 1
}

changed="$(git diff --name-only "$(git merge-base "$base" "$head")" "$head")"
missing=()
for adapter in $(while read -r f; do shipped "$f" && echo "${f%%/*}"; done <<<"$changed" | sort -u); do
  grep -qxF "$adapter/CHANGELOG.md" <<<"$changed" || missing+=("$adapter")
done

if ((${#missing[@]})); then
  for adapter in "${missing[@]}"; do
    echo "::error file=$adapter/CHANGELOG.md::$adapter's shipped code changed, so add a line under '## Unreleased' in $adapter/CHANGELOG.md, or label the pull request no-changelog if a caller cannot observe the change."
  done
  exit 1
fi
echo "changelogs cover every adapter whose shipped code changed"
