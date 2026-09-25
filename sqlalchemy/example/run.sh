#!/usr/bin/env bash
#
# Called by `demo/scripts/run-example.sh sqlalchemy`: build the adapter, install the built
# wheel here, and run the example. The PDP is already up at $CERBOS_HOST.
# stdout is exactly one JSON document; everything else goes to stderr.
#
# Pre-reqs: pdm, and a Python matching this example's `requires-python`.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
DIST_DIR="${ADAPTER_DIR}/dist"

cd "${EXAMPLE_DIR}"

# 1. Build a wheel and an sdist. An editable install would use ../src and skip packaging.
#    See docs/adr/0002-examples-install-the-packed-artifact.md. `pdm build` cleans the dest dir.
echo "==> pdm build (cerbos-sqlalchemy)" >&2
pdm build --project "${ADAPTER_DIR}" --dest "${DIST_DIR}" >&2

wheels=("${DIST_DIR}"/*.whl)
sdists=("${DIST_DIR}"/*.tar.gz)
if (( ${#wheels[@]} != 1 )) || [[ ! -f "${wheels[0]}" ]]; then
  echo "expected exactly one wheel in ${DIST_DIR}, found: ${wheels[*]}" >&2
  exit 1
fi
if (( ${#sdists[@]} != 1 )) || [[ ! -f "${sdists[0]}" ]]; then
  echo "expected exactly one sdist in ${DIST_DIR}, found: ${sdists[*]}" >&2
  exit 1
fi
WHEEL="${wheels[0]}"
SDIST="${sdists[0]}"
echo "==> built $(basename "${WHEEL}") and $(basename "${SDIST}")" >&2

# 2. Check the example is not inside the artifacts. This relies on a pdm-backend default
#    that `[tool.pdm] includes` could silently change.
#    Listings go into variables, not pipes: with pipefail, an early `grep -q` match
#    would SIGPIPE the producer and fail. zipfile is used because `unzip` may be missing.
refuse_example() {
  local artifact="$1" entries="$2"
  if grep -qE '(^|/)example/' <<<"${entries}"; then
    echo "$(basename "${artifact}") ships the example — the example must not be inside the" \
      "artifact it exercises (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
    exit 1
  fi
}
refuse_example "${WHEEL}" \
  "$(python3 -c 'import sys, zipfile; print(chr(10).join(zipfile.ZipFile(sys.argv[1]).namelist()))' "${WHEEL}")"
refuse_example "${SDIST}" "$(tar tzf "${SDIST}")"

# 3. Install the pinned tree. `--check --frozen-lockfile` fails if pdm.lock has drifted.
echo "==> pdm install" >&2
pdm install --check --frozen-lockfile >&2

# PDM venvs have no pip. ensurepip also works on a venv left by an earlier run,
# unlike PDM's `venv.with_pip`, which applies only at creation.
pdm run python -m ensurepip --upgrade >&2

# 4. Install the wheel over the pinned tree, and `pip check` its Requires-Dist against it.
#    The wheel is not in the lockfile because it changes every build.
#    Uninstall first: every local build has the same scm version, so pip would otherwise
#    keep the previous run's adapter.
echo "==> pip install $(basename "${WHEEL}")" >&2
pdm run python -m pip uninstall --yes --quiet --disable-pip-version-check cerbos-sqlalchemy >&2
pdm run python -m pip install --quiet --disable-pip-version-check "${WHEEL}" >&2
pdm run python -m pip check >&2

# 5. Check the import did not resolve to ../src, e.g. via PYTHONPATH or an editable install.
echo "==> checking cerbos_sqlalchemy did not resolve to ../src" >&2
pdm run python - "${ADAPTER_DIR}" <<'PY' >&2
import pathlib
import sys

import cerbos_sqlalchemy

module = pathlib.Path(cerbos_sqlalchemy.__file__).resolve()
if module.is_relative_to(pathlib.Path(sys.argv[1]).resolve() / "src"):
    raise SystemExit(
        f"cerbos_sqlalchemy resolved to {module}, inside the adapter's own source tree — "
        "the example must execute the built distribution "
        "(docs/adr/0002-examples-install-the-packed-artifact.md)"
    )
print(f"cerbos_sqlalchemy {cerbos_sqlalchemy.__version__} from {module.parent}")
PY

# 6. Run.
echo "==> python main.py" >&2
pdm run python main.py
