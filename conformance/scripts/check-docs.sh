#!/usr/bin/env bash
# Documentation invariants: two kinds of drift the adapter suites cannot see.
#
#   1. `conformance/README.md` carries an accurate table of contents. It is the longest document
#      in the repository and is read by section, not front to back; a stale TOC sends a reader
#      (human or agent) to the wrong anchor, which is worse than no TOC at all.
#
#   2. Prose spanning the adapter roster says "every adapter", so it stays true when the roster
#      changes. The rule and its rationale live in CLAUDE.md, "Working with Adapters"; this script
#      is only the enforcement.
#
# Usage:
#   check-docs.sh              run both checks (exit 1 on failure)
#   check-docs.sh --print-toc  print the table of contents conformance/README.md should carry

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
README="${REPO_ROOT}/conformance/README.md"

cd "${REPO_ROOT}"

# Emit the TOC entry list for conformance/README.md.
#
# Headings inside fenced code blocks are bash comments, not headings, so the fence state is
# tracked. Anchors follow GitHub's slug rules: strip inline code and emphasis markers, lowercase,
# drop everything that is not alphanumeric/space/hyphen, then spaces to hyphens.
generate_toc() {
  awk '
    /^```/ { fence = !fence; next }
    fence  { next }
    /^#{2,4}[[:space:]]/ {
      depth = index($0, " ") - 1
      title = substr($0, depth + 2)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", title)

      # The TOC does not list itself.
      if (title == "Contents") next

      slug = tolower(title)
      gsub(/`/, "", slug)
      gsub(/[*_]/, "", slug)
      gsub(/[^a-z0-9 -]/, "", slug)
      gsub(/ /, "-", slug)

      indent = ""
      for (i = 2; i < depth; i++) indent = indent "  "
      print indent "- [" title "](#" slug ")"
    }
  ' "${README}"
}

if [[ "${1:-}" == "--print-toc" ]]; then
  generate_toc
  exit 0
fi

status=0

# ---------------------------------------------------------------------------
# 1. conformance/README.md table of contents is in sync
# ---------------------------------------------------------------------------

expected_toc="$(generate_toc)"

# Duplicate headings would need GitHub's `-1`, `-2` anchor suffixes, which generate_toc does not
# emit. Rather than guess wrong, say so.
if duplicates="$(printf '%s\n' "${expected_toc}" | sed 's/.*(#//; s/)$//' | sort | uniq -d)" \
  && [[ -n "${duplicates}" ]]; then
  echo "conformance/README.md has headings that slug to the same anchor:"
  printf '  %s\n' ${duplicates}
  echo "Rename one, or teach check-docs.sh GitHub's -1/-2 anchor suffixes."
  status=1
fi

# The TOC is everything between the "## Contents" heading and the next "## " heading, minus the
# explanatory preamble (kept as the non-list lines).
actual_toc="$(awk '
  /^## Contents[[:space:]]*$/ { inside = 1; next }
  inside && /^## /            { inside = 0 }
  inside && /^ *- \[/         { print }
' "${README}")"

if [[ -z "${actual_toc}" ]]; then
  echo "conformance/README.md has no '## Contents' section."
  echo "Add one; generate it with: conformance/scripts/check-docs.sh --print-toc"
  status=1
elif [[ "${actual_toc}" != "${expected_toc}" ]]; then
  echo "conformance/README.md table of contents is out of date."
  echo "Regenerate it with: conformance/scripts/check-docs.sh --print-toc"
  echo
  diff <(printf '%s\n' "${actual_toc}") <(printf '%s\n' "${expected_toc}") \
    | sed 's/^/  /' || true
  status=1
fi

# ---------------------------------------------------------------------------
# 2. Prose spanning the roster says "every adapter"
# ---------------------------------------------------------------------------
#
# Narrow by design, and limited to spelled-out number WORDS: a number word directly qualifying a
# roster noun, plus the "all ten" / "the other nine" / "ten-way" idioms.
#
# Digits are the escape hatch. A genuine count of something that is not the roster — corpus
# actions, seed rows, the elements in `manyTeams`, fail-closed shapes — reads as a measurement
# when written "11 of the compared actions", and the check leaves it alone. Prose that spells a
# number out is almost always counting adapters.
#
# The scan covers every tracked file, not just Markdown: the same sentence appears in test-file
# doc comments, JSON `description` fields and shell headers, and an agent reads those too.
roster_nouns='adapters?|harnesses|harness|examples?|stores?|workflows?|languages|copies'
stale_count="(\\b(nine|ten|eleven|twelve)[[:space:]]+(${roster_nouns})\\b)"
stale_count+="|(\\ball (nine|ten|eleven|twelve)\\b)"
stale_count+="|(\\bthe other (nine|ten|eleven|twelve)\\b)"
stale_count+="|(\\b(nine|ten|eleven|twelve)-way\\b)"

# Every tracked file. Worktrees, node_modules and build output are not tracked, so `git ls-files`
# excludes them without a filter. Two exclusions:
#
#   - Lockfiles are generated, nobody reads them for meaning, and they are large enough to
#     dominate the scan.
#   - This script, which spells the idioms out above in order to define them. A linter matching
#     its own pattern definition is the usual reason linters skip themselves.
mapfile -t tracked < <(
  git ls-files \
    | grep -vE '(^|/)(package-lock\.json|pdm\.lock|go\.sum|gradle\.lockfile)$' \
    | grep -vxF 'conformance/scripts/check-docs.sh'
)

hits="$(grep -nEH "${stale_count}" "${tracked[@]}" 2>/dev/null || true)"

if [[ -n "${hits}" ]]; then
  echo "These lines count the adapter roster in prose:"
  echo
  printf '%s\n' "${hits}" | sed 's/^/  /'
  echo
  echo "Rewrite each as 'every adapter' / 'every harness' / 'every example', so the sentence"
  echo "stays true when the roster changes. A count of something else belongs in digits."
  status=1
fi

if [[ "${status}" -eq 0 ]]; then
  echo "check-docs.sh: TOC in sync, no roster counts restated in prose"
fi

exit "${status}"
