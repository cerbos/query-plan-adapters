#!/usr/bin/env bash
#
# The sqlalchemy half of `demo/scripts/run-example.sh sqlalchemy`: build the adapter into the
# distributions PyPI would receive, install THAT into this directory's own environment, run the
# program. The PDP is already up and reachable at $CERBOS_HOST — the shared runner owns it.
#
# The store is SQLite, in a file this directory owns, so there is nothing to start here.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.
#
# Pre-reqs: pdm, and a Python matching this example's `requires-python`.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
DIST_DIR="${ADAPTER_DIR}/dist"

cd "${EXAMPLE_DIR}"

# 1. Build the adapter into a wheel and an sdist.
#
# This is the Python form of "pack the adapter into a real distributable and install THAT"
# (docs/adr/0002-examples-install-the-packed-artifact.md). An editable or path install would
# resolve `cerbos_sqlalchemy` straight out of ../src, which is what both of this adapter's test
# suites already do — so the thing this example exists to execute, the distribution PyPI would
# serve, would go untouched.
#
# Both artifacts, not just the wheel: `pdm build` builds the wheel FROM the sdist, so an sdist
# that omits a module produces a wheel that omits it too, and that is a real class of Python
# packaging bug. The dest directory is cleaned by `pdm build` itself, so a stale wheel from an
# earlier run cannot be the one installed below.
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

# 2. The example must stay OUT of the artifacts it exercises. The TypeScript adapters get that
#    from their `files` allowlist and Go from nested-module exclusion; ADR 0002 asks for Python
#    and Java to check it deliberately, because neither has an equivalent.
#
#    It holds today because pdm-pep517 includes the package directory and `tests/` and nothing
#    else — a default, which is exactly the kind of fact that stops being true without anyone
#    noticing. `[tool.pdm] includes` in ../pyproject.toml would change it silently.
#
#    Listed with python3's zipfile rather than `unzip`, which is not on every image. Both
#    listings are taken into variables first: under `set -o pipefail`, a `grep -q` that matches
#    early closes the pipe and the producer dies of SIGPIPE, so a piped form would report failure
#    in exactly the case this exists to catch.
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

# 3. Install this example's own pinned tree from the committed lockfile.
#
# `--check --frozen-lockfile` is `npm ci`: it fails if pdm.lock has drifted from pyproject.toml
# rather than quietly re-resolving, which is what makes the committed lockfile — and therefore
# the Renovate bump that lands with it — the thing this job actually gates on.
echo "==> pdm install" >&2
pdm install --check --frozen-lockfile >&2

# Step 4 installs the wheel with pip, and PDM builds its virtualenv without one. `ensurepip` is
# stdlib and idempotent, so it works both on the venv PDM has just created and on one an earlier
# run left behind — PDM's own `venv.with_pip` setting would only have applied at creation time,
# and would silently do nothing on the second of those.
pdm run python -m ensurepip --upgrade >&2

# 4. Install the wheel on top, resolving its dependency metadata against that pinned tree.
#
# The wheel is NOT a dependency in pyproject.toml and NOT in the lockfile: it is rebuilt on every
# run, so a lockfile naming it would be wrong the moment the adapter changed. Installing it here
# leaves both files exactly as committed while still resolving `Requires-Dist` — the adapter's
# `sqlalchemy>=1.4` and `cerbos>=0.10.4` — against the versions this example pins, which is the
# Python analogue of a peer range. pip leaves an already-satisfied requirement alone, so the
# pinned tree survives; `pip check` then says so rather than leaving it assumed.
#
# UNINSTALLED FIRST, and that line is load-bearing. pip treats an installed distribution of the
# same version as satisfying the requirement even when the requirement is a local wheel path, and
# the adapter's version comes from an scm tag that a build of this working tree does not move —
# so every rebuild produces the same version and pip would skip it, leaving the PREVIOUS run's
# adapter installed. CI is always cold and would never see it; the tree where someone checks a
# packaging break by hand is not, and there it turns the break into a green run. `-y` on a
# distribution that is not installed warns and succeeds, which is what makes this safe first time.
echo "==> pip install $(basename "${WHEEL}")" >&2
pdm run python -m pip uninstall --yes --quiet --disable-pip-version-check cerbos-sqlalchemy >&2
pdm run python -m pip install --quiet --disable-pip-version-check "${WHEEL}" >&2
pdm run python -m pip check >&2

# 5. Assert the import the example is about to make does NOT resolve to the adapter's source.
#
#    Unlike a compiled language, Python will import a source tree that merely happens to be on
#    sys.path: `pdm install -e`, a stray PYTHONPATH, or running from the adapter directory would
#    each leave this example passing while proving nothing about packaging at all, which is the
#    whole reason it exists. Stated as "not the source tree" rather than "inside site-packages"
#    because the first is the hazard and the second is an install layout.
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

# 6. Run. stdout is the JSON document and nothing else.
echo "==> python main.py" >&2
pdm run python main.py
