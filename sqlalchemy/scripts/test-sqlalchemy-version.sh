#!/usr/bin/env bash

# Install and test one supported SQLAlchemy line without changing the committed lockfile.
# This mirrors .github/workflows/sqlalchemy_pr.yaml so adapterctl's environment label selects
# the version it claims rather than inheriting whichever version PDM installed previously.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <1.4|2.x>" >&2
  exit 2
fi

version="$1"
temporary_directory=""

cleanup() {
  if [[ -n "${temporary_directory}" && -d "${temporary_directory}" ]]; then
    rm -rf "${temporary_directory}"
  fi
}
trap cleanup EXIT

case "${version}" in
  1.4)
    temporary_directory="$(mktemp -d)"
    constraint_file="${temporary_directory}/sqlalchemy-constraint.txt"
    lock_file="${temporary_directory}/pdm.lock"
    printf '%s\n' 'sqlalchemy<2' >"${constraint_file}"
    pdm install -G testcontainers \
      --override "${constraint_file}" \
      --lockfile "${lock_file}"
    expected_prefix="1.4"
    ;;
  2.x)
    pdm install -G testcontainers --lockfile pdm.lock
    expected_prefix="2."
    ;;
  *)
    echo "Unknown SQLAlchemy version '${version}' (expected 1.4 or 2.x)" >&2
    exit 2
    ;;
esac

pdm run python -c \
  'import sys, sqlalchemy; expected = sys.argv[1]; actual = sqlalchemy.__version__; actual.startswith(expected) or sys.exit(f"expected {expected}*, got {actual}"); print(f"SQLAlchemy {actual}")' \
  "${expected_prefix}"

pdm run test
