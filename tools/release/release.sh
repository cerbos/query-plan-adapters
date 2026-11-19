#!/usr/bin/env bash
# Usage:
#   release.sh bump <adapter> <version>   write the version to the manifest and move CHANGELOG.md's Unreleased entries under it
#   release.sh notes <adapter> <version>  print that version's CHANGELOG.md section
#   release.sh tag <adapter> <version>    tag HEAD and push that tag alone to $REMOTE (default origin)
# <adapter> is the adapter's directory name. See docs/ci.md, "Cutting a release".
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# directory|tag prefix|manifest|version pattern: "npm", or sed -E with the version in group 2. No manifest: the tag is the version.
table() {
  cat <<'EOF'
prisma|@cerbos/orm-prisma@v|package.json|npm
mongoose|@cerbos/orm-mongoose@v|package.json|npm
drizzle|@cerbos/orm-drizzle@v|package.json|npm
convex|@cerbos/orm-convex@v|package.json|npm
langchain-chromadb|@cerbos/langchain-chromadb@v|package.json|npm
ent|ent/v||
pgx|pgx/v||
sqlalchemy|sqla/v||
activerecord|activerecord/v|lib/cerbos/active_record/version.rb|(VERSION = ")([^"]+)
sequel|sequel/v|lib/cerbos/sequel/version.rb|(VERSION = ")([^"]+)
mongodb-ruby|mongodb-ruby/v|lib/cerbos/mongodb/version.rb|(VERSION = ")([^"]+)
efcore|efcore/v|src/Cerbos.QueryPlan.EntityFrameworkCore/Cerbos.QueryPlan.EntityFrameworkCore.csproj|(<Version>)([^<]+)
jpa|jpa/v|build.gradle.kts|(^version = ")([^"]+)
spring-data|spring-data/v|build.gradle.kts|(^version = ")([^"]+)
exposed|exposed/v|build.gradle.kts|(^version = ")([^"]+)
elasticsearch-java|elasticsearch-java/v|build.gradle.kts|(^version = ")([^"]+)
EOF
}

die() { echo "$*" >&2; exit 1; }

load() {
  local row
  row="$(table | grep "^$1|")" || die "unknown adapter $1"
  IFS='|' read -r dir prefix manifest pattern <<<"$row"
  version="$2"
  [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "version must be X.Y.Z[-pre], without a v: $version"
}

section() {
  awk -v want="$2" '
    /^## / { if (on) exit; h = $0; sub(/^## \[?v?/, "", h); sub(/[] ].*$/, "", h); on = (h == want); next }
    on' "$1/CHANGELOG.md" | sed -e '/./,$!d' | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'
}

manifest_version() {
  if [[ "$pattern" == npm ]]; then
    node -p "require('./$dir/package.json').version"
  elif [[ -n "$manifest" ]]; then
    sed -nE "s/.*${pattern}.*/\2/p" "$dir/$manifest" | head -n 1
  else
    echo "$version"
  fi
}

bump() {
  load "$@"
  [[ -f "$dir/CHANGELOG.md" ]] || die "$dir/CHANGELOG.md is missing"
  [[ -n "$(section "$dir" Unreleased)" ]] || die "$dir/CHANGELOG.md has nothing under Unreleased"
  [[ -z "$(section "$dir" "$version")" ]] || die "$dir/CHANGELOG.md already has v$version"
  if [[ "$pattern" == npm ]]; then
    (cd "$dir" && npm version "$version" --no-git-tag-version --allow-same-version >/dev/null)
  elif [[ -n "$manifest" ]]; then
    sed -E -i.bak "s/${pattern}/\1${version}/" "$dir/$manifest" && rm "$dir/$manifest.bak"
  fi
  V="$version" perl -0pi -e 's/^## \[?Unreleased\]?\n/## Unreleased\n\n## v$ENV{V}\n/m' "$dir/CHANGELOG.md"
  echo "$dir: v$version"
}

notes() {
  load "$@"
  section "$dir" "$version"
}

tag() {
  load "$@"
  [[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"
  [[ "$(manifest_version)" == "$version" ]] || die "$dir is at $(manifest_version), not $version"
  [[ -n "$(section "$dir" "$version")" ]] || die "$dir/CHANGELOG.md has no v$version section"
  git tag "${prefix}${version}"
  git push "${REMOTE:-origin}" "refs/tags/${prefix}${version}"
}

case "${1:-}" in
  bump | notes | tag) [[ $# -eq 3 ]] || die "usage: release.sh $1 <adapter> <version>"; "$@" ;;
  *) die "usage: release.sh bump|notes|tag <adapter> <version>" ;;
esac
