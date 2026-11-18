# frozen_string_literal: true

# The domain stress harness (../domains/README.md, "The harness contract"). Not part of any CI
# suite. Run from the adapter directory against a MongoDB of your own:
#
#   DOMAINS_MONGODB_URI=mongodb://127.0.0.1:37017 DOMAINS_STORE=mongo-7.0 bundle exec ruby domains/harness.rb [domain ...]

require "bundler/setup"
require "mongo"
require "cerbos"
require "json"
require "time"
require "fileutils"
require "cerbos/mongodb"
require_relative "../spec/support/stub_pdp"

Mongo::Logger.logger.level = Logger::FATAL

module MongoDomains
  ROOT = File.expand_path("../../domains", __dir__)
  RESULTS = File.join(ROOT, "results", "mongodb-ruby")
  ALL = %w[healthcare fintech logistics education legal retail hr media iot government].freeze
  URI = ENV.fetch("DOMAINS_MONGODB_URI", "mongodb://127.0.0.1:37017")
  STORE = ENV.fetch("DOMAINS_STORE", "mongo")
  TIMESTAMP = /\A\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z\z/

  module_function

  # Each resource of resources.json as one document keyed by its id, its attributes at the top
  # level. Timestamps are stored as BSON dates, as an application would store them.
  def document(resource)
    {"_id" => resource.fetch("id")}.merge(typed(resource.fetch("attr")))
  end

  def typed(value)
    case value
    when Hash then value.transform_values { |v| typed(v) }
    when Array then value.map { |v| typed(v) }
    when TIMESTAMP then Time.iso8601(value).utc
    else value
    end
  end

  def collection(domain)
    @client&.close
    @client = Mongo::Client.new("#{URI}/dom_#{domain}?directConnection=true", server_selection_timeout: 10)
    resources = JSON.parse(File.read(File.join(ROOT, domain, "generated", "resources.json")))
    collection = @client[resources.fetch("kind")]
    collection.drop
    collection.insert_many(resources.fetch("resources").map { |resource| document(resource) })
    collection
  end

  def goldens(domain)
    Dir[File.join(ROOT, domain, "golden", "*", "*", "*.json")].sort.map { |path| JSON.parse(File.read(path)) }
  end

  def run_domain(domain)
    coll = collection(domain)
    require_relative "mappings/#{domain}"
    mapping = const_get(domain.capitalize)
    goldens(domain).map { |golden| verdict(golden, mapping, coll) }
  end

  def verdict(golden, mapping, coll)
    allowed = golden.fetch("allowed").sort
    base = {"domain" => golden.fetch("domain"), "pdp" => golden.fetch("pdp"),
            "principal" => golden.fetch("principalName"), "action" => golden.fetch("action")}
    plan = StubPdp.plan_resources(golden.fetch("plan"), resource_kind: golden.fetch("resourceKind"))
    begin
      result = Cerbos::MongoDB.query_plan_to_filter(
        plan: plan, mapper: mapping::MAPPER, null_attribute_representation: mapping::NULLS
      )
      raw = coll.find(result.filter, projection: {_id: 1}).map { |doc| doc.fetch("_id") }
    rescue Cerbos::MongoDB::MapperError => e
      return base.merge("verdict" => "error", "returned" => [], "allowed" => allowed, "detail" => "#{e.class}: #{e.message}")
    rescue Cerbos::MongoDB::Error => e
      return base.merge("verdict" => "refused", "returned" => [], "allowed" => allowed, "detail" => "#{e.class}: #{e.message}")
    rescue => e
      return base.merge("verdict" => "error", "returned" => [], "allowed" => allowed, "detail" => "#{e.class}: #{e.message}"[0, 600])
    end
    returned = raw.uniq.sort
    verdict =
      if (returned - allowed).any? then "over-grant"
      elsif returned.length < allowed.length then "under-grant"
      else "pass"
      end
    base.merge("verdict" => verdict, "returned" => returned, "allowed" => allowed,
      "detail" => (verdict == "pass") ? "" : result.filter.inspect)
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
    File.write(path, "#{JSON.pretty_generate("adapter" => "mongodb-ruby", "store" => STORE, "results" => results)}\n")
    tally = results.group_by { |r| r.fetch("verdict") }.transform_values(&:count)
    puts "#{STORE}: #{tally}"
    results.reject { |r| r.fetch("verdict") == "pass" }.each do |r|
      puts "  #{r.fetch("verdict")} #{r.fetch("domain")} #{r.fetch("pdp")} #{r.fetch("principal")}/#{r.fetch("action")} " \
           "returned=#{r.fetch("returned")} allowed=#{r.fetch("allowed")} #{r.fetch("detail")[0, 400]}"
    end
  ensure
    @client&.close
  end
end

MongoDomains.run(ARGV.empty? ? MongoDomains::ALL : ARGV)
