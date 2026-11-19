#!/usr/bin/env bash
# Usage: check-changelog.sh <base> [head]
# Fails when a change to an adapter's source between <base> and <head> leaves that adapter's CHANGELOG.md untouched.
# An adapter is a top-level directory holding a conformance-ledger.json; its source is any non-test source file in it.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
base="${1:?usage: check-changelog.sh <base> [head]}"
head="${2:-HEAD}"

source_ext='\.(ts|tsx|js|cjs|mjs|go|py|rb|kt|java|cs)$'
not_source='(^|/)(example|test|tests|spec|testdata|scripts)/|_test\.go$|_spec\.rb$|\.test\.ts$|/src/corpus\.ts$|/src/test-setup\.|/src/generated/|(^|/)jest\.[^/]*$|\.config\.[cm]?[jt]s$|/internal/conformance[^/]*/|^convex/convex/'

changed="$(git diff --name-only "$(git merge-base "$base" "$head")" "$head")"
adapters=()
for ledger in */conformance-ledger.json; do adapters+=("${ledger%%/*}"); done

failed=0
for adapter in "${adapters[@]}"; do
  if [[ ! -f "$adapter/CHANGELOG.md" ]]; then
    echo "::error file=$adapter/conformance-ledger.json::$adapter has no CHANGELOG.md: add one that opens with '## Unreleased' (docs/ci.md, \"Cutting a release\")."
    failed=1
    continue
  fi
  code="$(grep "^$adapter/" <<<"$changed" | grep -E "$source_ext" | grep -vE "$not_source" || true)"
  [[ -n "$code" ]] || continue
  grep -qxF "$adapter/CHANGELOG.md" <<<"$changed" && continue
  echo "::error file=$adapter/CHANGELOG.md::$adapter's source changed ($(head -n 1 <<<"$code")), so add a line under '## Unreleased' in $adapter/CHANGELOG.md, or label the pull request no-changelog if a caller cannot observe the change."
  failed=1
done

((failed == 0)) || exit 1
echo "changelogs cover every adapter whose source changed"
