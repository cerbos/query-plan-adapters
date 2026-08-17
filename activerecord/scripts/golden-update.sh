#!/usr/bin/env bash
# Rewrites golden/expectations.json from what the translator emits today.
#
# A deliberate act, and the diff is the review. CI NEVER runs this: a translator change that
# moves an emitted filter has to fail there whatever anyone ran locally.
#
# The asset declares the ActiveRecord major it was generated under, and the writer refuses to
# run under any other one — otherwise a toolchain swap arrives looking like a translation
# change. Run it under the default, which is the major the asset declares.
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

cleanup() { docker compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker compose build tests
# `--no-deps`: the translator unit test replays conformance/wire-fixtures/ and starts no PDP.
docker compose run --rm --no-deps -e GOLDEN_UPDATE=1 tests \
  bundle exec rspec spec/translator_spec.rb
