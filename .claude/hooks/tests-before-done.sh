#!/usr/bin/env bash
set -uo pipefail
command -v jq >/dev/null || { echo '{"systemMessage": "tests-before-done.sh needs jq on PATH: tests were not run."}'; exit 0; }
input="$(cat)"
session="$(jq -r '.session_id' <<<"$input")"
transcript="$(jq -r '.transcript_path // ""' <<<"$input")"
root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
cd "$root" || exit 0

[ -f "$transcript" ] && grep -qE '"name":"(Edit|Write|MultiEdit|NotebookEdit)"' "$transcript" || exit 0

base=""
for ref in upstream/main origin/main main; do
  base="$(git merge-base HEAD "$ref" 2>/dev/null)" && break
done
[ -n "$base" ] || exit 0

changed="$( { git diff --name-only "$base"; git ls-files --others --exclude-standard; } | grep -vE '\.md$' | sort -u)"
[ -n "$changed" ] || exit 0

state="$(git rev-parse --git-common-dir)/claude-tests"
mkdir -p "$state"
fingerprint="$( { git diff "$base"; git ls-files --others --exclude-standard -z | xargs -0 shasum 2>/dev/null; } | shasum | cut -d' ' -f1)"
[ -f "$state/pass-$fingerprint" ] && exit 0

attempts_file="$state/attempts-$session"
attempts="$(cat "$attempts_file" 2>/dev/null || echo 0)"
if [ "$attempts" -ge 3 ]; then
  rm -f "$attempts_file"
  jq -n '{systemMessage: "Tests are still failing after 3 attempts. Do not report this work as done."}'
  exit 0
fi

failed=""
log="$(mktemp)"
if grep -q '^conformance/' <<<"$changed"; then
  conformance/scripts/validate-corpus.sh >>"$log" 2>&1 || failed="validate-corpus"
fi
for dir in $(cut -d/ -f1 <<<"$changed" | sort -u); do
  [ -f "$dir/conformance-ledger.json" ] || continue
  leg="$(conformance/scripts/run-harness.sh --list | awk -v a="$dir" '$1 == a {print $2}')"
  conformance/scripts/run-harness.sh "$dir" "$leg" >>"$log" 2>&1 || failed="$failed $dir($leg)"
done

if [ -n "$failed" ]; then
  echo $((attempts + 1)) >"$attempts_file"
  { echo "Not done: failed:$failed. Fix the code, or tell the user why it fails. Do not weaken a test."; tail -n 60 "$log"; } >&2
  exit 2
fi
rm -f "$attempts_file"
touch "$state/pass-$fingerprint"
exit 0
