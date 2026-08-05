#!/usr/bin/env bash
# The mount of the repository hides the Gemfile.lock in the image. A lockfile in the working
# tree from a different combination of Ruby and ActiveRecord makes `bundle exec` fail, because
# a gem is missing. This script installs the gems again, but only after that condition.
set -euo pipefail

bundle check >/dev/null 2>&1 || bundle install --quiet

exec "$@"
