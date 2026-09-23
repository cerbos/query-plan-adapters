#!/usr/bin/env bash
# The repo mount hides the image's Gemfile.lock. A local lockfile from another Ruby or
# ActiveRecord version can reference missing gems, so reinstall only when that happens.
set -euo pipefail

bundle check >/dev/null 2>&1 || bundle install --quiet

exec "$@"
