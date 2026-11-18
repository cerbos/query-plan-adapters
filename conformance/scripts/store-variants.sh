# Store configuration variants, sourced by run-harness.sh (conformance/README.md, "Store variants").
#
# A variant is one non-default server setting, or the previous major, of PostgreSQL or MySQL. It is
# applied under the image names the harnesses already start (every */POSTGRES_IMAGE and
# */MYSQL_IMAGE), by tagging a derived local image as that name for one leg and tagging the
# original back afterwards, so no harness needs to know variants exist. A harness that pins the
# same setting itself (a collation-server flag, an initdb locale) keeps its pin.

STORE_VARIANTS_PG=(
  pg-previous
  pg-timezone-east
  pg-timezone-west
  pg-extra-float-digits
  pg-datestyle
  pg-legacy-strings
)

STORE_VARIANTS_MYSQL=(
  mysql-previous
  mysql-timezone
  mysql-high-not-precedence
  mysql-no-backslash-escapes
  mysql-ansi
  mysql-lax
  mysql-collation-general
)

# MySQL 8's default, which a variant adds to rather than replaces unless it says otherwise.
MYSQL_DEFAULT_SQL_MODE="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

variant_engine() { # <variant>: pg or mysql
  case "$1" in pg-*) echo pg ;; mysql-*) echo mysql ;; *) return 1 ;; esac
}

variant_known() {
  local v
  for v in "${STORE_VARIANTS_PG[@]}" "${STORE_VARIANTS_MYSQL[@]}"; do [[ "${v}" == "$1" ]] && return 0; done
  return 1
}

leg_engines() { # <adapter> <store>: the engines a leg starts, of those variants exist for
  case "$1:$2" in
    drizzle:postgres | prisma:postgres-v* | activerecord:postgres | sequel:postgres \
      | jpa:postgres | exposed:postgres | efcore:postgres | pgx:all) echo pg ;;
    drizzle:mysql | prisma:mysql-v* | activerecord:mysql | sequel:mysql \
      | jpa:mysql | jpa:mysql-server-prep | exposed:mysql | exposed:mysql-server-prep) echo mysql ;;
    sqlalchemy:all | ent:all) echo "pg mysql" ;;
    *) echo "" ;;
  esac
}

leg_variants() { # <adapter> <store>
  local engine
  for engine in $(leg_engines "$1" "$2"); do
    case "${engine}" in
      pg) printf '%s\n' "${STORE_VARIANTS_PG[@]}" ;;
      mysql) printf '%s\n' "${STORE_VARIANTS_MYSQL[@]}" ;;
    esac
  done
}

pick_variant() { # <seed> <adapter> <store>: one of the leg's variants, or "default" if it has none
  local seed="$1" adapter="$2" store="$3" variants n sum
  mapfile -t variants < <(leg_variants "${adapter}" "${store}")
  n="${#variants[@]}"
  if [[ "${n}" -eq 0 ]]; then echo default; return; fi
  sum="$(printf '%s %s %s' "${seed}" "${adapter}" "${store}" | cksum | cut -d' ' -f1)"
  echo "${variants[$((sum % n))]}"
}

variant_images() { # <engine>: the image names harnesses start for it
  local file
  case "$1" in pg) file=POSTGRES_IMAGE ;; mysql) file=MYSQL_IMAGE ;; esac
  cat "${REPO_ROOT}"/*/"${file}" | sort -u
}

previous_major() { # <image>: the same image one major back
  case "$1" in
    postgres:[0-9]*)
      local tag="${1#postgres:}" major
      major="${tag%%[!0-9]*}"
      echo "postgres:$((major - 1))${tag#"${major}"}"
      ;;
    mysql:8.4*) echo "mysql:8.0" ;;
    *) return 1 ;;
  esac
}

variant_dockerfile() { # <variant> <base image>
  local v="$1" base="$2"
  case "${v}" in
    pg-previous | mysql-previous) printf 'FROM %s\n' "${base}" ;;
    pg-timezone-east) pg_settings "${base}" "timezone = 'Pacific/Chatham'" ;;
    pg-timezone-west) pg_settings "${base}" "timezone = 'America/St_Johns'" ;;
    pg-extra-float-digits) pg_settings "${base}" "extra_float_digits = 0" ;;
    pg-datestyle) pg_settings "${base}" "datestyle = 'SQL, DMY'" "intervalstyle = 'postgres_verbose'" ;;
    pg-legacy-strings) pg_settings "${base}" "standard_conforming_strings = off" ;;
    mysql-timezone) mysql_settings "${base}" "default-time-zone = '-09:30'" ;;
    mysql-high-not-precedence) mysql_settings "${base}" "sql-mode = '${MYSQL_DEFAULT_SQL_MODE},HIGH_NOT_PRECEDENCE'" ;;
    mysql-no-backslash-escapes) mysql_settings "${base}" "sql-mode = '${MYSQL_DEFAULT_SQL_MODE},NO_BACKSLASH_ESCAPES'" ;;
    mysql-ansi) mysql_settings "${base}" "sql-mode = 'ANSI,STRICT_TRANS_TABLES'" ;;
    mysql-lax) mysql_settings "${base}" "sql-mode = ''" ;;
    mysql-collation-general) mysql_settings "${base}" "character-set-server = utf8mb4" "collation-server = utf8mb4_general_ci" ;;
    *) return 1 ;;
  esac
}

# ALTER SYSTEM from an init script, so the setting survives a harness that replaces the command.
pg_settings() {
  local base="$1"; shift
  local sql="" s
  for s in "$@"; do sql+="ALTER SYSTEM SET ${s};\\n"; done
  printf 'FROM %s\nRUN printf "%s" > /docker-entrypoint-initdb.d/zz-qpa-variant.sql\n' "${base}" "${sql}"
}

# A conf.d file, which mysqld flags a harness passes on its command line still override.
mysql_settings() {
  local base="$1"; shift
  local conf="[mysqld]\\n" s
  for s in "$@"; do conf+="${s}\\n"; done
  printf 'FROM %s\nRUN printf "%s" > /etc/mysql/conf.d/zz-qpa-variant.cnf\n' "${base}" "${conf}"
}

# Tags every image a variant has replaced back to its original. Also run before a leg, so a run
# that was killed before its own restore does not leave a variant behind.
restore_store_images() {
  local engine image saved
  for engine in pg mysql; do
    while read -r image; do
      saved="$(docker image inspect -f '{{index .Config.Labels "qpa.original"}}' "${image}" 2>/dev/null)" || continue
      [[ -n "${saved}" && "${saved}" != "<no value>" ]] || continue
      docker tag "${saved}" "${image}" && docker rmi "${saved}" >/dev/null \
        && echo "store variant: restored ${image}" >&2
    done < <(variant_images "${engine}")
  done
}

apply_store_variant() { # <variant>
  local v="$1" engine image saved base
  engine="$(variant_engine "${v}")" || return 1
  restore_store_images
  while read -r image; do
    docker image inspect "${image}" >/dev/null 2>&1 || docker pull -q "${image}" >/dev/null || return 1
    # The original keeps a name of its own while the variant holds its tag: an image left without
    # one can be removed, and BuildKit can only start FROM a name.
    saved="qpa-store-original/${image//[:\/]/-}:latest"
    docker tag "${image}" "${saved}" || return 1
    base="${saved}"
    if [[ "${v}" == *-previous ]]; then
      base="$(previous_major "${image}")" || return 1
      docker image inspect "${base}" >/dev/null 2>&1 || docker pull -q "${base}" >/dev/null || return 1
    fi
    variant_dockerfile "${v}" "${base}" \
      | docker build -q --label "qpa.original=${saved}" --label "qpa.store-variant=${v}" -t "${image}" - >/dev/null \
      || return 1
    echo "store variant: ${image} runs ${v}" >&2
  done < <(variant_images "${engine}")
}

# Adds "variant" to the results files this leg wrote for the variant's engine, so the fuzz report
# names the configuration a verdict came from.
label_fuzz_results() { # <variant> <marker file>
  local v="$1" marker="$2" engine pattern
  engine="$(variant_engine "${v}")" || return 0
  case "${engine}" in pg) pattern=postgres ;; mysql) pattern=mysql ;; esac
  find "${QPA_FUZZ_DIR}/results" -maxdepth 1 -name '*.json' -newer "${marker}" -print0 2>/dev/null \
    | xargs -0 -r python3 -c '
import json, sys
variant, pattern, paths = sys.argv[1], sys.argv[2], sys.argv[3:]
for path in paths:
    with open(path) as f:
        leg = json.load(f)
    if pattern in leg.get("store", ""):
        leg["variant"] = variant
        with open(path, "w") as f:
            json.dump(leg, f, indent=2)
            f.write("\n")
' "${v}" "${pattern}"
}
