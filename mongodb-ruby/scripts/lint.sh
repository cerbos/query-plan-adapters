#!/usr/bin/env bash
# Runs standardrb over the adapter, its suites and its example.
set -euo pipefail

cd "$(dirname "$0")/.."

bundle check >/dev/null 2>&1 || bundle install --quiet
bundle exec standardrb "$@"
