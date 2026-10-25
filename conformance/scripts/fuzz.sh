#!/usr/bin/env bash
# Runs one fuzz round locally, the way .github/workflows/fuzz.yaml does: record generated cases
# against both pinned PDPs, replay them through each adapter's harness, then write the report
# (conformance/README.md, "Fuzzing").
#
#   conformance/scripts/fuzz.sh                          # seed from the date, 500 cases, every adapter
#   conformance/scripts/fuzz.sh -s 7 -n 200 drizzle prisma
#   conformance/scripts/fuzz.sh -o ~/fuzz/s7 -s 7        # keep the run somewhere other than a temp dir
#
# Each adapter replays every store leg run-harness.sh knows for it, one at a time. The report is
# <out>/report.md; the run fails when the report finds an adapter failure. Needs Docker.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

seed="$(date -u +%Y%m%d)"
count=500
out=""
while getopts "s:n:o:h" opt; do
  case "${opt}" in
    s) seed="${OPTARG}" ;;
    n) count="${OPTARG}" ;;
    o) out="${OPTARG}" ;;
    *) sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

[[ -n "${out}" ]] || out="$(mktemp -d "${TMPDIR:-/tmp}/qpa-fuzz-${seed}-XXXX")"
mkdir -p "${out}"
out="$(cd "${out}" && pwd)"

go -C "${CONFORMANCE_DIR}/generator" run . fuzz -seed "${seed}" -count "${count}" -out "${out}" || exit 1

adapters=("$@")
[[ "${#adapters[@]}" -gt 0 ]] || read -r -a adapters <<<"$("${SCRIPT_DIR}/run-harness.sh" --list | awk '{print $1}' | xargs)"
silent=()
for adapter in "${adapters[@]}"; do
  # A failing case fails the harness; the report, not the exit status, says what failed. A harness
  # that wrote no verdicts (it did not build, or its store did not start) is absent from the
  # report, so it is caught here, as the workflow's replay job does.
  QPA_FUZZ_DIR="${out}" "${SCRIPT_DIR}/run-harness.sh" "${adapter}" || true
  compgen -G "${out}/results/${adapter}-*.json" >/dev/null || silent+=("${adapter}")
done

go -C "${CONFORMANCE_DIR}/generator" run . fuzz-report -dir "${out}" >/dev/null
status=$?
echo "==> ${out}/report.md" >&2
if [[ "${#silent[@]}" -gt 0 ]]; then
  echo "fuzz: no results from ${silent[*]}; their harnesses replayed nothing" >&2
  status=1
fi
exit "${status}"
