#!/usr/bin/env bash
# Usage:
#   release.sh prepare [<adapter>=<major|minor|patch|version> ...]  bump each on a branch off $REMOTE/main and open the release PR; no arguments lists unreleased adapters
#   release.sh bump <adapter> <major|minor|patch|version>  write the version to the manifest and move CHANGELOG.md's Unreleased entries under it
#   release.sh notes <adapter> <version>                   print that version's CHANGELOG.md section
#   release.sh tag                                         tag HEAD with every adapter's newest CHANGELOG.md version not yet on $REMOTE
# $REMOTE defaults to the remote whose URL is cerbos/query-plan-adapters. <adapter> is the adapter's directory name. See docs/ci.md, "Cutting a release".
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
duckdb|duckdb/v||
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

repository=cerbos/query-plan-adapters

remote_name() {
  if [[ -n "${REMOTE:-}" ]]; then echo "$REMOTE"; return; fi
  local name url
  while read -r name url _; do
    if [[ "$url" =~ [:/]${repository}(\.git)?/?$ ]]; then echo "$name"; return; fi
  done < <(git remote -v)
  die "no remote points at $repository; set REMOTE"
}

load() {
  local row
  row="$(table | grep "^$1|")" || die "unknown adapter $1"
  IFS='|' read -r dir prefix manifest pattern <<<"$row"
  version="${2:-}"
}

check_version() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "version must be X.Y.Z[-pre], without a v: $1"
}

current() {
  local tagged
  tagged="$(git tag --list "${prefix}*" --sort=-v:refname | head -n 1)"
  if [[ -n "$tagged" ]]; then echo "${tagged#"$prefix"}"; else manifest_version; fi
}

increment() {
  local major minor patch
  IFS=. read -r major minor patch <<<"${1%%-*}"
  case "$2" in
    major) echo "$((major + 1)).0.0" ;;
    minor) echo "${major}.$((minor + 1)).0" ;;
    patch) echo "${major}.${minor}.$((patch + 1))" ;;
  esac
}

newest() {
  awk '/^## v[0-9]/ { sub(/^## v/, ""); sub(/ .*$/, ""); print; exit }' "$dir/CHANGELOG.md"
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
  case "$version" in
    major | minor | patch) version="$(increment "$(current)" "$version")" ;;
  esac
  check_version "$version"
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

display_name() {
  if [[ "$pattern" == npm ]]; then node -p "require('./$dir/package.json').name"; else echo "$dir"; fi
}

prepare() {
  local remote spec adapter bump from rows="" sections="" names=()
  if (($# == 0)); then
    for adapter in $(table | cut -d'|' -f1); do
      load "$adapter"
      if [[ -f "$dir/CHANGELOG.md" && -n "$(section "$dir" Unreleased)" ]]; then echo "$dir (at $(current))"; fi
    done
    return 0
  fi
  [[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"
  remote="$(remote_name)"
  for spec in "$@"; do
    [[ "$spec" == *=* ]] || die "expected <adapter>=<major|minor|patch|version>: $spec"
    load "${spec%%=*}"
    [[ -f "$dir/CHANGELOG.md" && -n "$(section "$dir" Unreleased)" ]] || die "$dir/CHANGELOG.md has nothing under Unreleased"
  done
  command -v gh >/dev/null || die "prepare needs gh"

  local branch="release-$(date +%Y%m%d-%H%M%S)"
  git fetch --quiet "$remote" main
  git switch --quiet --no-track -c "$branch" "$remote/main"
  for spec in "$@"; do
    adapter="${spec%%=*}" bump="${spec#*=}"
    load "$adapter"
    from="$(current)"
    bump "$adapter" "$bump" >/dev/null
    names+=("${dir} ${version}")
    rows+="| \`$(display_name)\` | ${from} | ${version} |"$'\n'
    sections+=$'\n'"## \`$(display_name)\` ${version}"$'\n\n'"$(section "$dir" "$version")"$'\n'
  done

  local title="chore: release ${names[0]}" i
  for ((i = 1; i < ${#names[@]}; i++)); do
    if ((i == ${#names[@]} - 1)); then title+=" and ${names[i]}"; else title+=", ${names[i]}"; fi
  done
  local body
  body="$(printf 'Moves each adapter'"'"'s `Unreleased` changelog entries under its new version. Once this is merged, check out the merge commit and run `tools/release/release.sh tag`.\n\n| Package | From | To |\n| --- | --- | --- |\n%s%s' "$rows" "$sections")"

  git add -A
  git commit --quiet --signoff -m "$title"
  git push --quiet --set-upstream "$remote" "$branch"
  gh pr create --repo "$repository" --base main --head "$branch" --title "$title" --body "$body"
  git switch --quiet -
}

notes() {
  load "$@"
  check_version "$version"
  section "$dir" "$version"
}

tag() {
  [[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"
  local remote remote_tags tags=() adapter
  remote="$(remote_name)"
  remote_tags="$(git ls-remote --refs --tags "$remote" | sed 's#.*refs/tags/##')"
  for adapter in $(table | cut -d'|' -f1); do
    load "$adapter"
    [[ -f "$dir/CHANGELOG.md" ]] || continue
    version="$(newest)"
    [[ -n "$version" ]] || continue
    grep -qxF "${prefix}${version}" <<<"$remote_tags" && continue
    [[ "$(manifest_version)" == "$version" ]] || die "$dir: CHANGELOG.md is at v$version but the manifest at $(manifest_version)"
    tags+=("${prefix}${version}")
  done
  ((${#tags[@]})) || die "every adapter's newest CHANGELOG.md version is already tagged on $remote"
  printf 'tagging %s with:\n' "$(git log -1 --format='%h %s')"
  printf '  %s\n' "${tags[@]}"
  read -r -p "proceed? (y/N) " answer
  [[ "$answer" == [yY] ]] || die "cancelled"
  local t i batch
  for t in "${tags[@]}"; do git tag "$t"; done
  for ((i = 0; i < ${#tags[@]}; i += 3)); do
    batch=("${tags[@]:i:3}")
    git push --atomic "$remote" "${batch[@]/#/refs/tags/}"
  done
}

main() {
  case "${1:-}" in
    prepare) shift; prepare "$@" ;;
    bump | notes) [[ $# -eq 3 ]] || die "usage: release.sh $1 <adapter> <version>"; "$@" ;;
    tag) [[ $# -eq 1 ]] || die "usage: release.sh tag"; tag ;;
    *) die "usage: release.sh prepare [<adapter>=<bump> ...], release.sh bump|notes <adapter> <version>, or release.sh tag" ;;
  esac
}

main "$@"; exit
