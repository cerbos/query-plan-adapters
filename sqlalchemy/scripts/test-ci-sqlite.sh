#!/usr/bin/env bash
# Runs the suites that need no Docker store (every SQLite leg of the conformance harness, and the
# unit suites) against the SQLite CI links: Ubuntu 24.04's libsqlite3, 3.45.
#
#   sqlalchemy/scripts/test-ci-sqlite.sh [pytest args...]
#
# A developer's Python usually bundles a newer SQLite than CI's, and the two differ in ways a
# translation can trip over: before 3.46 the parser stack is fixed, so a deeply nested expression
# fails with "parser stack overflow" on CI and nowhere else. The dependencies come from pdm.lock,
# as CI's do; a fresh resolve picks newer releases that disagree with CI on other cases.
#
# Needs Docker. The checkout is mounted read-only and copied, so nothing is written to it.
set -euo pipefail

SQLALCHEMY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${SQLALCHEMY_DIR}/.." && pwd)"
# Ubuntu 24.04 is what ubuntu-latest runs; its libsqlite3 is the SQLite the workflow's Python uses.
UBUNTU_IMAGE="ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55"

docker run --rm -v "${REPO_ROOT}:/repo:ro" -e PYTHONDONTWRITEBYTECODE=1 \
  -e PDM_BUILD_SCM_VERSION=0.0.0 "${UBUNTU_IMAGE}" bash -c '
    set -euo pipefail
    apt-get -qq update >/dev/null
    apt-get -qq install -y python3-venv >/dev/null
    python3 -m venv /pdm && /pdm/bin/pip -q install pdm >/dev/null
    cp -r /repo/sqlalchemy /work && cp -r /repo/conformance /conformance
    cd /work && rm -rf .venv
    /pdm/bin/pdm sync -q -G test -G testcontainers
    .venv/bin/python -c "import sqlite3, sqlalchemy; print(\"SQLite\", sqlite3.sqlite_version, \"SQLAlchemy\", sqlalchemy.__version__)"
    .venv/bin/pytest -p no:cacheprovider tests -k "not postgresql and not mysql" "$@"
  ' test-ci-sqlite "$@"
