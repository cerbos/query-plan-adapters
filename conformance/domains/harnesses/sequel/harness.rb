# frozen_string_literal: true

# The domain stress harness (../../README.md, "The harness contract"). Not part of any CI
# suite. It loads the adapter, and its Gemfile, from ../../../../sequel. Run from this
# directory:
#
#   DOMAINS_DB=sqlite ruby harness.rb [domain ...]
#   DOMAINS_DB=postgres DOMAINS_PORT=35432 ruby harness.rb
#   DOMAINS_DB=mysql DOMAINS_PORT=33306 ruby harness.rb

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../../../sequel/Gemfile", __dir__)
require "bundler/setup"
require "sequel"
require "cerbos"
require "json"
require "time"
require "fileutils"
require "cerbos/sequel"
require_relative "../../../../sequel/spec/support/stub_pdp"

Sequel.default_timezone = :utc

module SequelDomains
  ROOT = File.expand_path("../..", __dir__)
  RESULTS = File.join(ROOT, "results", "sequel")
  ALL = %w[healthcare fintech logistics education legal retail hr media iot government].freeze
  STORE = ENV.fetch("DOMAINS_DB", "sqlite")
  HOST = ENV.fetch("DOMAINS_HOST", "127.0.0.1")
  PORT = ENV["DOMAINS_PORT"]
  PASSWORD = ENV.fetch("DOMAINS_PASSWORD", "conformance")
  MYSQL_COLLATION = "utf8mb4_0900_ai_ci"
  # Government keys agencies `DSS` and `dss` apart, which a case-folding collation cannot hold.
  MYSQL_DATABASE_COLLATION = Hash.new(MYSQL_COLLATION).merge("government" => "utf8mb4_0900_as_cs").freeze

  SQLITE_TYPES = {
    "string" => String, "int" => Integer, "bigint" => :Bignum, "float" => Float,
    "decimal" => [BigDecimal, {size: [18, 2]}], "bool" => TrueClass, "timestamp" => DateTime,
    "json" => String, "string[]" => String
  }.freeze

  class << self
    attr_reader :db
  end

  module_function

  def connect(domain)
    @db&.disconnect
    @db =
      case STORE
      when "sqlite"
        Sequel.sqlite(max_connections: 1).tap { |db| db.run("PRAGMA case_sensitive_like = ON") }
      when "postgres"
        url = "postgres://postgres:#{PASSWORD}@#{HOST}:#{PORT}"
        Sequel.connect("#{url}/postgres", max_connections: 1) do |admin|
          admin.run("DROP DATABASE IF EXISTS dom_#{domain}")
          admin.run("CREATE DATABASE dom_#{domain}")
        end
        Sequel.connect("#{url}/dom_#{domain}", max_connections: 1)
      when "mysql"
        url = "trilogy://root:#{PASSWORD}@#{HOST}:#{PORT}"
        options = {max_connections: 1, fractional_seconds: true,
                   connect_sqls: ["SET collation_connection = #{MYSQL_COLLATION}"]}
        Sequel.connect("#{url}/mysql", **options) do |admin|
          admin.run("DROP DATABASE IF EXISTS dom_#{domain}")
          admin.run("CREATE DATABASE dom_#{domain} CHARACTER SET utf8mb4 COLLATE #{MYSQL_DATABASE_COLLATION[domain]}")
        end
        Sequel.connect("#{url}/dom_#{domain}", **options)
      else raise "DOMAINS_DB must be sqlite, postgres or mysql"
      end
  end

  # PostgreSQL and MySQL take the kit's shared SQL. On SQLite Sequel keeps datetimes as text in
  # a format of its own, so the rows go in through Sequel from rows.json.
  def load(domain)
    if STORE == "sqlite"
      load_rows(JSON.parse(File.read(File.join(ROOT, domain, "generated", "rows.json"))))
    else
      sql = File.read(File.join(ROOT, domain, "generated", "sql", "#{STORE}.sql"))
      sql.lines.reject { |line| line.start_with?("--") }.join.split(/;\s*$/).map(&:strip).reject(&:empty?)
        .each { |statement| db.run(statement) }
    end
  end

  def load_rows(rows)
    rows.fetch("tables").each do |table|
      columns = table.fetch("columns")
      db.create_table(table.fetch("name").to_sym) do
        columns.each do |col|
          type, options = SQLITE_TYPES.fetch(col.fetch("type"))
          column col.fetch("name").to_sym, type, **(options || {}), null: col.fetch("nullable")
        end
        primary_key Array(table.fetch("primaryKey")).map(&:to_sym)
      end
      typed = table.fetch("rows").map { |row|
        columns.to_h { |column|
          name = column.fetch("name")
          value = row[name]
          value =
            case column.fetch("type")
            when "timestamp" then value && Time.iso8601(value).utc
            when "json", "string[]" then JSON.generate(value)
            when "decimal" then value && BigDecimal(value.to_s)
            else value
            end
          [name.to_sym, value]
        }
      }
      db[table.fetch("name").to_sym].multi_insert(typed)
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
      dataset = Cerbos::Sequel.query_plan_to_dataset(
        plan: plan, model: mapping::MODEL, attributes: mapping::ATTRIBUTES,
        null_attribute_representation: mapping::NULLS
      )
      raw = dataset.select_map(mapping::MODEL.primary_key).map(&:to_s)
    rescue Cerbos::Sequel::Error => e
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
    detail = dataset.sql if verdict != "pass" && detail.empty?
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
    File.write(path, "#{JSON.pretty_generate("adapter" => "sequel", "store" => STORE, "results" => results)}\n")
    tally = results.group_by { |r| r.fetch("verdict") }.transform_values(&:count)
    puts "#{STORE}: #{tally}"
    results.reject { |r| r.fetch("verdict") == "pass" }.each do |r|
      puts "  #{r.fetch("verdict")} #{r.fetch("domain")} #{r.fetch("pdp")} #{r.fetch("principal")}/#{r.fetch("action")} " \
           "returned=#{r.fetch("returned")} allowed=#{r.fetch("allowed")} #{r.fetch("detail")[0, 300]}"
    end
  end
end

SequelDomains.run(ARGV.empty? ? SequelDomains::ALL : ARGV)
