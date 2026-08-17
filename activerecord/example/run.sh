#!/usr/bin/env bash
# The demo-domain example for the ActiveRecord adapter.
#
# Invoked by demo/scripts/run-example.sh, which starts the pinned PDP over demo/policies/ and
# sets CERBOS_HOST. Everything language-specific lives here; the shared runner grows no Ruby
# branch.
#
# stdout carries exactly one JSON document and nothing else. Everything a human might read goes
# to stderr.
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"

# 1. Build the adapter into a real distributable and install THAT, not the source directory
#    (docs/adr/0002-examples-install-the-packed-artifact.md). Installing the gem is what
#    executes the gemspec's `files` allowlist, its required_ruby_version and its dependency
#    range — none of which any harness touches, because every harness loads lib/ directly.
echo "==> gem build cerbos-activerecord.gemspec" >&2
GEM_FILE="$(cd "${ADAPTER_DIR}" && gem build cerbos-activerecord.gemspec --output "${EXAMPLE_DIR}/cerbos-activerecord.gem" >&2 && echo "${EXAMPLE_DIR}/cerbos-activerecord.gem")"

# 2. The example must not ship inside the artifact it exercises. TypeScript adapters get this
#    from their `files` allowlist; RubyGems needs it checked deliberately, because a careless
#    `spec.files = Dir["**/*"]` would sweep example/ into the published gem.
echo "==> checking the gem does not carry the example" >&2
if tar -xOf "${GEM_FILE}" data.tar.gz | tar -tzf - | grep -q '^example/'; then
  echo "cerbos-activerecord.gem contains example/ — the example must not ship inside the" \
    "adapter (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
  exit 1
fi

# 3. Install the artifact's CONTENTS, not this directory. `gem unpack` extracts exactly the
#    files the gemspec's `files` allowlist put in the gem — nothing more — and the Gemfile
#    resolves the adapter from there, so a lib/ file missing from the allowlist fails at
#    `require` here rather than for the first consumer who installs it.
#
#    Bundler needs the gem on its load path to `require` it under `bundle exec`, which is why
#    this is an unpack-and-resolve rather than a bare `gem install`: an installed gem outside
#    the bundle is invisible to it.
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

# 4. Run. stdout is the JSON document; `app.rb` moves its own chatter to stderr.
echo "==> ruby app.rb" >&2
DEMO_DIR="${REPO_ROOT}/demo" exec bundle exec ruby app.rb
