# frozen_string_literal: true

# The domain stress harness (../../README.md, "The harness contract"). Not part of any CI
# suite. It loads the adapter, and its Gemfile, from ../../../../activerecord. Run from this
# directory:
#
#   DOMAINS_DB=sqlite ruby harness.rb [domain ...]
#   DOMAINS_DB=postgres DOMAINS_PORT=35432 ruby harness.rb
#   DOMAINS_DB=mysql DOMAINS_PORT=33306 ruby harness.rb
#   DOMAINS_DB=mariadb DOMAINS_PORT=33306 ruby harness.rb

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../../../activerecord/Gemfile", __dir__)
require "bundler/setup"
require "active_record"
require "cerbos"
require "json"
require "time"
require "fileutils"
require "cerbos/active_record"
require_relative "../../../../activerecord/spec/support/stub_pdp"

module ArDomains
  ROOT = File.expand_path("../..", __dir__)
  RESULTS = File.join(ROOT, "results", "activerecord")
  ALL = %w[healthcare fintech logistics education legal retail hr media iot government].freeze
  STORE = ENV.fetch("DOMAINS_DB", "sqlite")
  HOST = ENV.fetch("DOMAINS_HOST", "127.0.0.1")
  PORT = ENV["DOMAINS_PORT"]&.to_i
  PASSWORD = ENV.fetch("DOMAINS_PASSWORD", "conformance")
  MYSQL_COLLATION = "utf8mb4_0900_ai_ci"
  # Government keys agencies `DSS` and `dss` apart, which a case-folding collation cannot hold.
  MYSQL_DATABASE_COLLATION = Hash.new(MYSQL_COLLATION).merge("government" => "utf8mb4_0900_as_cs").freeze
  MARIADB_COLLATION = "utf8mb4_uca1400_ai_ci"
  MARIADB_DATABASE_COLLATION = Hash.new(MARIADB_COLLATION).merge("government" => "utf8mb4_uca1400_as_cs").freeze

  SQLITE_TYPES = {
    "string" => "varchar", "int" => "integer", "bigint" => "bigint", "float" => "float",
    "decimal" => "decimal(18,2)", "bool" => "boolean", "timestamp" => "datetime(6)",
    "json" => "json", "string[]" => "json"
  }.freeze

  module_function

  def connect(domain)
    ActiveRecord::Base.connection_handler.clear_all_connections!
    case STORE
    when "sqlite"
      ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:", pool: 1)
      ActiveRecord::Base.connection.execute("PRAGMA case_sensitive_like = ON")
    when "postgres"
      admin = {adapter: "postgresql", host: HOST, port: PORT, username: "postgres", password: PASSWORD}
      ActiveRecord::Base.establish_connection(admin.merge(database: "postgres"))
      ActiveRecord::Base.connection.execute("DROP DATABASE IF EXISTS dom_#{domain}")
      ActiveRecord::Base.connection.execute("CREATE DATABASE dom_#{domain}")
      ActiveRecord::Base.establish_connection(admin.merge(database: "dom_#{domain}"))
    when "mysql", "mariadb"
      mariadb = STORE == "mariadb"
      admin = {adapter: "mysql2", host: HOST, port: PORT, username: "root", password: PASSWORD,
               encoding: "utf8mb4", collation: mariadb ? MARIADB_COLLATION : MYSQL_COLLATION}
      collation = (mariadb ? MARIADB_DATABASE_COLLATION : MYSQL_DATABASE_COLLATION)[domain]
      ActiveRecord::Base.establish_connection(admin)
      ActiveRecord::Base.connection.execute("DROP DATABASE IF EXISTS dom_#{domain}")
      ActiveRecord::Base.connection.execute("CREATE DATABASE dom_#{domain} CHARACTER SET utf8mb4 COLLATE #{collation}")
      ActiveRecord::Base.establish_connection(admin.merge(database: "dom_#{domain}"))
    else raise "DOMAINS_DB must be sqlite, postgres, mysql or mariadb"
    end
  end

  # PostgreSQL and MySQL take the kit's shared SQL. On SQLite ActiveRecord keeps datetimes in a
  # text format of its own, so the rows go in through ActiveRecord from rows.json.
  def load(domain)
    if STORE == "sqlite"
      load_rows(JSON.parse(File.read(File.join(ROOT, domain, "generated", "rows.json"))))
    else
      sql = File.read(File.join(ROOT, domain, "generated", "sql", "#{(STORE == "mariadb") ? "mysql" : STORE}.sql"))
      sql.lines.reject { |line| line.start_with?("--") }.join.split(/;\s*$/).map(&:strip).reject(&:empty?)
        .each { |statement| ActiveRecord::Base.connection.execute(statement) }
    end
  end

  def load_rows(rows)
    connection = ActiveRecord::Base.connection
    rows.fetch("tables").each do |table|
      name = table.fetch("name")
      columns = table.fetch("columns").map { |column|
        "#{connection.quote_column_name(column.fetch("name"))} #{SQLITE_TYPES.fetch(column.fetch("type"))}" \
          "#{" NOT NULL" unless column.fetch("nullable")}"
      }
      key = Array(table.fetch("primaryKey")).map { |c| connection.quote_column_name(c) }.join(", ")
      connection.execute("CREATE TABLE #{connection.quote_table_name(name)} (#{columns.join(", ")}, PRIMARY KEY (#{key}))")
      json = table.fetch("columns").select { |c| %w[json string[]].include?(c.fetch("type")) }.map { |c| c.fetch("name") }
      model = Class.new(ActiveRecord::Base) { self.table_name = name }
      rows = table.fetch("rows").map { |row| json.empty? ? row : row.merge(json.to_h { |c| [c, JSON.generate(row[c])] }) }
      json.each { |c| model.attribute c, :string }
      model.insert_all!(rows) unless rows.empty?
    end
  end

  def goldens(domain)
    Dir[File.join(ROOT, domain, "golden", "*", "*", "*.json")].sort.map { |path| JSON.parse(File.read(path)) }
  end

  def run_domain(domain)
    connect(domain)
    load(domain)
    require_relative "mappings/#{domain}"
    mapping = const_get(domain.capitalize)
    goldens(domain).map { |golden| verdict(golden, mapping) }
  end

  def verdict(golden, mapping)
    allowed = golden.fetch("allowed").sort
    base = {"domain" => golden.fetch("domain"), "pdp" => golden.fetch("pdp"),
            "principal" => golden.fetch("principalName"), "action" => golden.fetch("action")}
    plan = StubPdp.plan_resources(golden.fetch("plan"), resource_kind: golden.fetch("resourceKind"))
    begin
      relation = Cerbos::ActiveRecord.query_plan_to_relation(
        plan: plan, model: mapping::MODEL, attributes: mapping::ATTRIBUTES,
        null_attribute_representation: mapping::NULLS,
        **(mapping.const_defined?(:OVERRIDES) ? {operator_overrides: mapping::OVERRIDES} : {})
      )
      raw = relation.pluck(mapping::MODEL.primary_key).map(&:to_s)
    rescue Cerbos::ActiveRecord::Error => e
      return base.merge("verdict" => "refused", "returned" => [], "allowed" => allowed, "detail" => "#{e.class}: #{e.message}")
    rescue => e
      return base.merge("verdict" => "error", "returned" => [], "allowed" => allowed, "detail" => "#{e.class}: #{e.message}"[0, 600])
    end
    returned = raw.uniq.sort
    detail = ""
    verdict =
      if (returned - allowed).any? then "over-grant"
      elsif returned.length < allowed.length then "under-grant"
      elsif raw.length != returned.length
        detail = "duplicate rows returned"
        "error"
      else "pass"
      end
    detail = relation.to_sql if verdict != "pass" && detail.empty?
    base.merge("verdict" => verdict, "returned" => returned, "allowed" => allowed, "detail" => detail)
  end

  def run(domains)
    results = domains.flat_map { |domain| run_domain(domain) }
    FileUtils.mkdir_p(RESULTS)
    path = File.join(RESULTS, "#{STORE}.json")
    if domains != ALL && File.exist?(path)
      kept = JSON.parse(File.read(path)).fetch("results").reject { |r| domains.include?(r.fetch("domain")) }
      results = kept + results
    end
    results.sort_by! { |r| [ALL.index(r.fetch("domain")), r.fetch("pdp"), r.fetch("principal"), r.fetch("action")] }
    File.write(path, "#{JSON.pretty_generate("adapter" => "activerecord", "store" => STORE, "results" => results)}\n")
    tally = results.group_by { |r| r.fetch("verdict") }.transform_values(&:count)
    puts "#{STORE}: #{tally}"
    results.reject { |r| r.fetch("verdict") == "pass" }.each do |r|
      puts "  #{r.fetch("verdict")} #{r.fetch("domain")} #{r.fetch("pdp")} #{r.fetch("principal")}/#{r.fetch("action")} " \
           "returned=#{r.fetch("returned")} allowed=#{r.fetch("allowed")} #{r.fetch("detail")[0, 300]}"
    end
  end
end

ActiveRecord.default_timezone = :utc
ArDomains.run(ARGV.empty? ? ArDomains::ALL : ARGV)
