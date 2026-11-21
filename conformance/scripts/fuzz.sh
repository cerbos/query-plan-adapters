#!/usr/bin/env bash
# Runs one fuzz round locally, the way .github/workflows/fuzz.yaml does: record generated cases
# against both pinned PDPs, replay them through each adapter's harness, then write the report
# (conformance/README.md, "Fuzzing").
#
#   conformance/scripts/fuzz.sh                          # seed from the date, 500 cases, every adapter
#   conformance/scripts/fuzz.sh -s 7 -n 200 drizzle prisma
#   conformance/scripts/fuzz.sh -o ~/fuzz/s7 -s 7        # keep the run somewhere other than a temp dir
#   conformance/scripts/fuzz.sh -s 7 -r 40               # add 40 generated rows to the dataset
#   conformance/scripts/fuzz.sh -s 7 -e 80               # add up to 80 rows on the edges of the run's literals
#   conformance/scripts/fuzz.sh -d -s 7                  # every leg on its default store configuration
#
# Each adapter replays every store leg run-harness.sh knows for it, one at a time. A PostgreSQL or
# MySQL leg runs on a store variant the seed picks for it (conformance/README.md, "Store
# variants"); -d keeps every leg on its default. The report is <out>/report.md; the run fails when
# the report finds an adapter failure. Needs Docker.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

seed="$(date -u +%Y%m%d)"
count=500
rows=0
edges=0
out=""
variants=1
while getopts "s:n:r:e:o:dh" opt; do
  case "${opt}" in
    s) seed="${OPTARG}" ;;
    n) count="${OPTARG}" ;;
    r) rows="${OPTARG}" ;;
    e) edges="${OPTARG}" ;;
    o) out="${OPTARG}" ;;
    d) variants=0 ;;
    *) sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

[[ -n "${out}" ]] || out="$(mktemp -d "${TMPDIR:-/tmp}/qpa-fuzz-${seed}-XXXX")"
mkdir -p "${out}"
out="$(cd "${out}" && pwd)"

go -C "${CONFORMANCE_DIR}/generator" run . fuzz -seed "${seed}" -count "${count}" -rows "${rows}" -edges "${edges}" -out "${out}" || exit 1

adapters=("$@")
[[ "${#adapters[@]}" -gt 0 ]] || read -r -a adapters <<<"$("${SCRIPT_DIR}/run-harness.sh" --list | awk '{print $1}' | xargs)"
silent=()
for adapter in "${adapters[@]}"; do
  # A failing case fails the harness; the report, not the exit status, says what failed. A harness
  # that wrote no verdicts (it did not build, or its store did not start) is absent from the
  # report, so it is caught here, as the workflow's replay job does.
  if [[ "${variants}" -eq 0 ]]; then
    QPA_FUZZ_DIR="${out}" "${SCRIPT_DIR}/run-harness.sh" "${adapter}" || true
  else
    for store in $("${SCRIPT_DIR}/run-harness.sh" --list | awk -v a="${adapter}" '$1 == a { $1 = ""; print }'); do
      QPA_FUZZ_DIR="${out}" \
        QPA_STORE_VARIANT="$("${SCRIPT_DIR}/run-harness.sh" --pick-variant "${seed}" "${adapter}" "${store}")" \
        "${SCRIPT_DIR}/run-harness.sh" "${adapter}" "${store}" || true
    done
  fi
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
