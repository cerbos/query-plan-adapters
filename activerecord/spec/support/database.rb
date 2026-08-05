# frozen_string_literal: true

# One in-memory SQLite database shared by both suites.
module Database
  module_function

  def establish!
    return if @established
    @established = true

    ActiveRecord::Base.establish_connection(
      adapter: "sqlite3",
      database: ":memory:",
      # A single connection, so the PRAGMA below applies to every query the suite runs.
      pool: 1
    )
    # CEL string matching is case-sensitive; SQLite's LIKE is not, by default. Without this
    # the `contains("a_b")` probe would match `xA_by`, and the corpus's collation witnesses
    # would pass for the wrong reason.
    ActiveRecord::Base.connection.execute("PRAGMA case_sensitive_like = ON")
  end
end
