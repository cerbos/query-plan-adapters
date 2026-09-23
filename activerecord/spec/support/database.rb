# frozen_string_literal: true

# One SQLite database in memory for the two suites.
module Database
  module_function

  def establish!
    return if @established
    @established = true

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
