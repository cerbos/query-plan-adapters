# frozen_string_literal: true

# The database the suites run on, chosen by ADAPTER_TEST_DB: `sqlite` (the default, in memory),
# `postgres` or `mysql`. The latter two are the services scripts/test.sh starts from
# docker-compose.yaml; only the conformance suite runs on them in CI.
module Database
  STORES = %w[sqlite postgres mysql].freeze
  STORE = ENV.fetch("ADAPTER_TEST_DB", "sqlite")
  unless STORES.include?(STORE)
    raise ArgumentError, "ADAPTER_TEST_DB must be one of #{STORES.join(", ")}, not #{STORE.inspect}"
  end

  # Byte-exact and NO PAD, as CEL compares strings. MySQL's default collation makes `=`
  # case-insensitive, and utf8mb4_0900_as_cs still ignores a soft hyphen (#474).
  MYSQL_COLLATION = "utf8mb4_0900_bin"

  module_function

  def establish!
    return if @established
    @established = true

    case STORE
    when "sqlite" then establish_sqlite!
    when "postgres"
      ActiveRecord::Base.establish_connection(
        adapter: "postgresql", host: "postgres", username: "postgres", password: "conformance",
        database: "conformance"
      )
    when "mysql"
      # The server creates every column in MYSQL_COLLATION (docker-compose.yaml). The session
      # needs it too, since a literal takes the connection's collation, not the column's; the
      # mysql2 adapter sets it with SET NAMES.
      ActiveRecord::Base.establish_connection(
        adapter: "mysql2", host: "mysql", username: "root", password: "conformance",
        database: "conformance", encoding: "utf8mb4", collation: MYSQL_COLLATION
      )
    end
  end

  def establish_sqlite!
    ActiveRecord::Base.establish_connection(
      adapter: "sqlite3",
      database: ":memory:",
      # One connection, so the PRAGMA below applies to every query.
      pool: 1
    )
    # CEL string matching is case-sensitive; SQLite's LIKE is not by default. Without this,
    # `contains("a_b")` would also match `xA_by` and the collation seeds would pass by accident.
    ActiveRecord::Base.connection.execute("PRAGMA case_sensitive_like = ON")
  end
end
