#!/usr/bin/env bash
set -uo pipefail
command -v jq >/dev/null || { echo "Blocked: secret-scan.sh needs jq on PATH." >&2; exit 2; }
input="$(cat)"
tool="$(jq -r '.tool_name' <<<"$input")"
cwd="$(jq -r '.cwd // "."' <<<"$input")"

text="$(jq -r '
  .tool_input
  | [.content?, .new_string?, .new_source?, .command?, (.edits[]?.new_string?)]
  | map(select(type == "string")) | join("\n")
' <<<"$input")"

patterns='AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,}|xox[abprs]-[A-Za-z0-9-]{10,}|sk-ant-[A-Za-z0-9_-]{20,}|sk-[A-Za-z0-9]{32,}|-----BEGIN ([A-Z]+ )?PRIVATE KEY-----|convex-self-hosted\|[0-9a-f]{20,}|npm_[A-Za-z0-9]{36}|pypi-[A-Za-z0-9_-]{40,}'

block() { echo "Blocked: $1 Do not put secrets in files or commands. Read them from the environment." >&2; exit 2; }

if grep -qE -- "$patterns" <<<"$text"; then
  block "the $tool input matches a secret pattern."
fi

if [ "$tool" = "Bash" ] && grep -qE '(^|[;&|[:space:]])git[[:space:]]+(commit|push)([[:space:]]|$)' <<<"$text"; then
  if { git -C "$cwd" diff HEAD; git -C "$cwd" diff "@{upstream}" HEAD; } 2>/dev/null | grep -E '^\+' | grep -qE -- "$patterns"; then
    block "the diff being committed or pushed matches a secret pattern."
  fi
fi
exit 0
