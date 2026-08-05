#!/usr/bin/env bash
# The repository mount masks the Gemfile.lock baked into the image, and a lockfile left in
# the working tree by a different Ruby/ActiveRecord combination would otherwise make
# `bundle exec` fail with a missing-gem error. Re-resolve only when that has happened.
set -euo pipefail

bundle check >/dev/null 2>&1 || bundle install --quiet

exec "$@"
