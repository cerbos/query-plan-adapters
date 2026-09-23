#!/usr/bin/env bash
# Rewrites golden/expectations.json from what the translator emits today. Review the diff.
# CI never runs this, so a translator change that moves a filter fails there.
#
# The file records the ActiveRecord major it was generated under, and this refuses to run
# under another. Use the default ACTIVERECORD_VERSION.
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

cleanup() { docker compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker compose build tests
# `--no-deps`: the translator unit test reads wire fixtures and needs no PDP.
docker compose run --rm --no-deps -e GOLDEN_UPDATE=1 tests \
  bundle exec rspec spec/translator_spec.rb
