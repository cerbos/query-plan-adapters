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
      # Only one connection. Thus the PRAGMA below applies to each query of the suite.
      pool: 1
    )
    # CEL compares strings with attention to the case of the letters. The LIKE operator of
    # SQLite does not do this with its default configuration. Without this PRAGMA, the test
    # `contains("a_b")` would also find `xA_by`. Then the rows in the corpus for the collation
    # would agree for an incorrect reason.
    ActiveRecord::Base.connection.execute("PRAGMA case_sensitive_like = ON")
  end
end
