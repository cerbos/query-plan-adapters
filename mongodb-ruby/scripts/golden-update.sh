#!/usr/bin/env bash
# Rewrites golden/expectations.json from what the translator emits today.
#
# A deliberate act, and the diff is the review. CI NEVER runs this: a translator change that
# moves an emitted filter has to fail there whatever anyone ran locally.
#
# Offline: the translator unit test replays conformance/wire-fixtures/, so no PDP and no MongoDB
# are started.
set -euo pipefail

cd "$(dirname "$0")/.."

bundle check >/dev/null 2>&1 || bundle install --quiet
GOLDEN_UPDATE=1 exec env -u CERBOS_HOST -u MONGODB_URI bundle exec rspec spec/translator_spec.rb
