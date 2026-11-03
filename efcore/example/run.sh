#!/usr/bin/env bash
#
# Called by `demo/scripts/run-example.sh efcore`, which starts the PDP and sets CERBOS_HOST.
# Packs the adapter, restores the demo-domain program against that .nupkg and runs it on in-memory
# SQLite.
#
# stdout carries exactly one JSON document, diffed against demo/expected.json. Everything else
# goes to stderr.
#
# Pre-reqs: .NET 10 SDK.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

# 1. Pack the adapter under a version no feed has, so restore can only resolve this .nupkg and never
#    a stale copy from the global packages folder (docs/adr/0002-examples-install-the-packed-artifact.md).
VERSION="0.0.0-example.$(date +%s)"
rm -rf "${EXAMPLE_DIR}/packages"
echo "==> dotnet pack Cerbos.QueryPlan.EntityFrameworkCore ${VERSION}" >&2
dotnet pack "${ADAPTER_DIR}/src/Cerbos.QueryPlan.EntityFrameworkCore" -c Release -p:Version="${VERSION}" \
  -o "${EXAMPLE_DIR}/packages" >&2

# 2. The example must not ship inside the package (ADR 0002).
NUPKG="${EXAMPLE_DIR}/packages/Cerbos.QueryPlan.EntityFrameworkCore.${VERSION}.nupkg"
entries="$(unzip -Z1 "${NUPKG}")"
if grep -qi 'example' <<<"${entries}"; then
  echo "$(basename "${NUPKG}") contains example files — the example must not ship inside the" \
    "adapter (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
  exit 1
fi

# 3. Build, then run the built program so restore and build output stay off stdout.
echo "==> dotnet build example" >&2
dotnet build "${EXAMPLE_DIR}/Example.csproj" -c Release -p:AdapterVersion="${VERSION}" \
  -o "${EXAMPLE_DIR}/bin/run" >&2

echo "==> dotnet bin/run/Example.dll" >&2
DEMO_DIR="${REPO_ROOT}/demo" exec dotnet "${EXAMPLE_DIR}/bin/run/Example.dll"
