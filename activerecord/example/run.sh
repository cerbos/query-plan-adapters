#!/usr/bin/env bash
# Demo-domain example for the ActiveRecord adapter.
# Called by demo/scripts/run-example.sh, which starts the PDP and sets CERBOS_HOST.
# stdout is one JSON document only; all logs go to stderr.
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"

# 1. Build the gem and use that, not the source (ADR 0002). This tests the gemspec's `files`,
#    required_ruby_version and dependency range, which the harnesses never touch.
echo "==> gem build cerbos-activerecord.gemspec" >&2
GEM_FILE="$(cd "${ADAPTER_DIR}" && gem build cerbos-activerecord.gemspec --output "${EXAMPLE_DIR}/cerbos-activerecord.gem" >&2 && echo "${EXAMPLE_DIR}/cerbos-activerecord.gem")"

# 2. Make sure example/ is not inside the gem. A broad `spec.files` glob would include it.
echo "==> checking the gem does not carry the example" >&2
if tar -xOf "${GEM_FILE}" data.tar.gz | tar -tzf - | grep -q '^example/'; then
  echo "cerbos-activerecord.gem contains example/ — the example must not ship inside the" \
    "adapter (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
  exit 1
fi

# 3. Unpack the gem into vendor/ for the Gemfile to use. A lib/ file missing from the gem
#    then fails at `require` here. Not `gem install`: Bundler can't see gems outside the bundle.
UNPACKED="${EXAMPLE_DIR}/vendor/cerbos-activerecord"
echo "==> gem unpack into vendor/" >&2
rm -rf "${EXAMPLE_DIR}/vendor"
mkdir -p "${EXAMPLE_DIR}/vendor"
gem unpack "${GEM_FILE}" --target="${EXAMPLE_DIR}/vendor" >&2
gem unpack --spec "${GEM_FILE}" --target="${EXAMPLE_DIR}/vendor" >&2
mv "${EXAMPLE_DIR}"/vendor/cerbos-activerecord-*.gemspec \
  "${UNPACKED}/cerbos-activerecord.gemspec"

echo "==> bundle install" >&2
export BUNDLE_PATH="${EXAMPLE_DIR}/.bundle-path"
bundle install >&2

# 4. Run. app.rb sends its own logs to stderr.
echo "==> ruby app.rb" >&2
DEMO_DIR="${REPO_ROOT}/demo" exec bundle exec ruby app.rb
