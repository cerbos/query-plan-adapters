# frozen_string_literal: true

# The one database every suite in this process uses. It exists when this file is loaded,
# because a Sequel::Model class reads the schema of its table when it is defined.
#
# The store is chosen with ADAPTER_TEST_DB, exactly as the drizzle and prisma harnesses choose
# theirs: `sqlite` (the default, in memory), `postgres` or `mysql`. An unknown value fails
# rather than falling back, because a typo that quietly ran SQLite would report a store as
# covered that nothing executed.
#
# Only the conformance harness runs on the real servers. Collation, LIKE escaping, cast targets
# and literal typing are translator behaviour, so a store the harness does not execute is a
# store the adapter does not cover. The contract suite asserts SQLite's rendering and refuses to
# run anywhere else (see Database.require_sqlite!).
module Database
  STORES = %w[sqlite postgres mysql].freeze

  STORE = ENV.fetch("ADAPTER_TEST_DB", "sqlite")
  unless STORES.include?(STORE)
    raise "ADAPTER_TEST_DB=#{STORE.inspect} is not one of #{STORES.join(", ")}"
  end

  # The connection string of the real server. scripts/test.sh sets it for the compose service
  # it starts; a local run against a server of your own sets it by hand.
  def self.url
    ENV.fetch("DATABASE_URL") {
      raise "ADAPTER_TEST_DB=#{STORE} needs DATABASE_URL — run it through scripts/test.sh"
    }
  end

  # MySQL's default collation makes `=` itself case- and accent-insensitive, and CEL's string
  # equality is byte-exact. That is a store misconfiguration and not an adapter limitation, so
  # the leg pins a binary collation on the tables (see conformance_store.rb) AND on the
  # connection: the literals the adapter writes — the 'true' and 'false' of string() over a
  # boolean — compare in the collation of the connection, not of a column. The README states the
  # same requirement for a consumer.
  MYSQL_COLLATION = "utf8mb4_0900_bin"

  DB =
    case STORE
    when "sqlite"
      # Only one connection. Thus the PRAGMA below applies to each query of the suite, and the
      # tables in memory are the same tables for every query.
      Sequel.sqlite(max_connections: 1).tap do |db|
        # CEL compares strings with attention to the case of the letters. The LIKE operator of
        # SQLite does not do this with its default configuration. Without this PRAGMA, the test
        # `contains("a_b")` would also find `xA_by`. Then the rows in the corpus for the
        # collation would agree for an incorrect reason.
        db.run("PRAGMA case_sensitive_like = ON")
      end
    when "postgres"
      Sequel.connect(url, max_connections: 1)
    when "mysql"
      # `collation_connection` and not `SET NAMES utf8mb4 COLLATE ...`: with the latter, the
      # trilogy driver (2.13, against MySQL 8.4) crashes the Ruby process with a segfault on the
      # next query. The connection already speaks utf8mb4, so only the collation needs setting.
      Sequel.connect(url, max_connections: 1,
        connect_sqls: ["SET collation_connection = #{MYSQL_COLLATION}"])
    end

  # The column type a timestamp needs to keep the corpus's microseconds. SQLite stores the text
  # Sequel writes and PostgreSQL's `timestamp` keeps six digits, but MySQL's `datetime` keeps
  # none unless it is asked for them.
  TIMESTAMP_TYPE = (STORE == "mysql") ? "datetime(6)" : DateTime

  # The options every table in the harness is created with.
  TABLE_OPTIONS = (STORE == "mysql") ? {charset: "utf8mb4", collate: MYSQL_COLLATION} : {}

  # Every instant in the corpus is UTC, and so is every timestamp literal the adapter binds. The
  # database timezone is what Sequel converts a Time into before it writes it. Left at the
  # default, the local zone of the machine would reach both sides — the stored rows and the
  # literals — and they would agree only because they were wrong in the same way.
  Sequel.default_timezone = :utc

  # The contract suite reads SQLite's quoting in its patterns. On another store they would fail for a reason that says
  # nothing about the adapter, so they refuse to start instead.
  def self.require_sqlite!(suite)
    return if STORE == "sqlite"

    raise "#{suite} records and asserts SQLite's rendering and runs on SQLite only; " \
          "ADAPTER_TEST_DB=#{STORE} is for spec/conformance_spec.rb"
  end
end
