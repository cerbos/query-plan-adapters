# frozen_string_literal: true

# The database the suites run on, chosen by ADAPTER_TEST_DB: `sqlite` (the default, in memory),
# `postgres`, `mysql` or `mariadb`. The others are the services scripts/test.sh starts from
# docker-compose.yaml; only the conformance suite runs on them in CI.
module Database
  STORES = %w[sqlite postgres mysql mariadb].freeze
  STORE = ENV.fetch("ADAPTER_TEST_DB", "sqlite")
  unless STORES.include?(STORE)
    raise ArgumentError, "ADAPTER_TEST_DB must be one of #{STORES.join(", ")}, not #{STORE.inspect}"
  end

  # MySQL's default, which folds case and accents (#474): the adapter compares strings byte for
  # byte itself.
  MYSQL_COLLATION = "utf8mb4_0900_ai_ci"
  MARIADB_COLLATION = "utf8mb4_uca1400_ai_ci"

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
      # The server's columns and this session both run on MYSQL_COLLATION.
      ActiveRecord::Base.establish_connection(
        adapter: "mysql2", host: "mysql", username: "root", password: "conformance",
        database: "conformance", encoding: "utf8mb4", collation: MYSQL_COLLATION
      )
    when "mariadb"
      ActiveRecord::Base.establish_connection(
        adapter: "mysql2", host: "mariadb", username: "root", password: "conformance",
        database: "conformance", encoding: "utf8mb4", collation: MARIADB_COLLATION
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
