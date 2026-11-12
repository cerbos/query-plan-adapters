# frozen_string_literal: true

require "fileutils"
require "time"

# Reads the shared conformance corpus: the dataset, the recorded goldens, and this adapter's
# ledger. See conformance/README.md, "The harness contract".
#
# Duplicated across adapters on purpose (ADR 0007). Do not extract or share it.
module ConformanceCorpus
  DIR = File.expand_path("../../../conformance", __dir__)
  LEDGER_FILE = File.expand_path("../../conformance-ledger.json", __dir__)

  # A seed requiring a store capability the ledger lacks on this store is a resource it cannot
  # hold: it is not loaded, and its id is struck from every golden's allowed set.
  STORE = ENV.fetch("ADAPTER_TEST_DB", "sqlite")
  LACKS = JSON.parse(File.read(LEDGER_FILE)).fetch("lacks", {})
    .select { |_, entry| entry.fetch("stores", [STORE]).include?(STORE) }.keys.freeze
  ALL_SEEDS = JSON.parse(File.read(File.join(DIR, "seeds.json"))).fetch("seeds")
  UNHELD = ALL_SEEDS.select { |seed| seed.fetch("requires", []).intersect?(LACKS) }
    .map { |seed| seed.fetch("id") }.freeze
  SEEDS = ALL_SEEDS.reject { |seed| UNHELD.include?(seed.fetch("id")) }.freeze
  SEEDS_BY_ID = SEEDS.to_h { |seed| [seed.fetch("id"), seed] }.freeze
  DERIVED = JSON.parse(File.read(File.join(DIR, "derived-fields.json"))).fetch("derived").freeze

  PDP_TAGS = JSON.parse(File.read(File.join(DIR, "pdp-versions.json")))
    .values_at("current", "previous").map { |pdp| pdp.fetch("tag") }.freeze

  LEDGER = JSON.parse(File.read(LEDGER_FILE)).fetch("cases").freeze

  # The planner folds `now() - duration("24h")` into a literal, which the goldens record as
  # this placeholder. The real instant goes back in, at the nanosecond precision the planner
  # emits (fixed digits, so a run never lands on a whole microsecond by chance).
  NOW_PLACEHOLDER = "__NOW_MINUS_24H__"
  NOW_MINUS_24H = Time.at(Time.now.to_i - 86_400, 123_456_789, :nsec).utc.iso8601(9).freeze

  # The directory a fuzz run was recorded into (conformance/README.md, "Fuzzing"). When it is set,
  # `goldens` reads the generated cases from it instead of the corpus, and the harness judges each
  # case with `fuzz_verdict` instead of the ledger: a refusal is always acceptable, since the
  # ledger has no entry for a shape nobody has written down yet.
  FUZZ_DIR = ENV.fetch("QPA_FUZZ_DIR", "").then { |dir| dir.empty? ? nil : dir }.freeze

  module_function

  # The attributes of every resource check() saw, as dotted paths under `request.resource.attr`.
  # A list or scalar ends a path; an object is walked into (`parent.inner.aBool`, `obj.inner`).
  def resource_attribute_paths
    JSON.parse(File.read(File.join(DIR, "resources.json"))).fetch("resources")
      .flat_map { |resource| attribute_paths(resource.fetch("attr"), "request.resource.attr") }
      .uniq.sort
  end

  def attribute_paths(value, prefix)
    return [prefix] unless value.is_a?(Hash)

    value.flat_map { |key, child| attribute_paths(child, "#{prefix}.#{key}") }
  end

  # @return [Array<Hash>] every golden file recorded against one PDP, sorted by case id.
  # Raises when the tag has no golden files, so a missing directory cannot pass as zero cases.
  def goldens(tag)
    root = FUZZ_DIR ? File.join(FUZZ_DIR, tag) : File.join(DIR, "golden", tag)
    paths = Dir[File.join(root, "**", "*.json")].sort
    raise "no golden files for PDP #{tag} under #{root}" if paths.empty?

    paths.map { |path|
      golden = JSON.parse(File.read(path).gsub(NOW_PLACEHOLDER, NOW_MINUS_24H))
      golden.merge("allowed" => golden.fetch("allowed") - UNHELD)
    }
  end

  # The golden's plan as the Cerbos Ruby SDK returns it: sent through Cerbos::Client#plan_resources
  # against the stub PDP (spec/support/stub_pdp.rb), which answers with the recorded plan. This is
  # the form an application passes to the adapter, not the JSON hash on disk.
  def sdk_plan(golden)
    StubPdp.plan_resources(golden.fetch("plan"), resource_kind: golden.fetch("request").fetch("resourceKind"))
  end

  # The ledger entry for a case under one PDP, or nil. An entry scoped with `pdp` applies only
  # to the tags it lists.
  def ledger_entry(id, tag)
    entry = LEDGER[id]
    entry if entry && (entry["pdp"].nil? || entry["pdp"].include?(tag))
  end

  # Judges one fuzz case: the block translates and runs it, returning the ids it selected. A
  # Cerbos::Sequel::Error is the adapter refusing, as an `unsupported` ledger entry expects.
  def fuzz_verdict(golden, expected)
    base = {"pdp" => golden.fetch("pdp"), "id" => golden.fetch("id")}
    begin
      returned = yield
    rescue Cerbos::Sequel::Error
      return base.merge("verdict" => "refused")
    rescue => e
      return base.merge("verdict" => "error", "detail" => "#{e.class}: #{e.message}")
    end
    return base.merge("verdict" => "match") if returned == expected

    verdict =
      if (returned - expected).any? then "over-grant"
      elsif (expected - returned).any? then "under-grant"
      else "error"
      end
    out = base.merge("verdict" => verdict, "expected" => expected, "returned" => returned)
    (verdict == "error") ? out.merge("detail" => "the right rows, but some of them repeated") : out
  end

  # The verdicts the conformance spec gathered, by store leg, written once the suite ends.
  def fuzz_results = (@fuzz_results ||= Hash.new { |hash, store| hash[store] = [] })

  # Writes <FUZZ_DIR>/results/sequel-<store>.json for each leg, which `fuzz-report` reads.
  def write_fuzz_results
    return unless FUZZ_DIR

    dir = File.join(FUZZ_DIR, "results")
    FileUtils.mkdir_p(dir)
    fuzz_results.each do |store, results|
      sorted = results.sort_by { |r| [r.fetch("id"), r.fetch("pdp")] }
      File.write(File.join(dir, "sequel-#{store}.json"),
        "#{JSON.pretty_generate("adapter" => "sequel", "store" => store, "results" => sorted)}\n")
    end
  end

  def skipped?(golden, tag)
    divergence = golden["plannerDivergence"]
    !divergence.nil? && (divergence["pdp"].nil? || divergence["pdp"].include?(tag))
  end

  # --- the dataset: seeds.json plus derived-fields.json ----------------------------------------

  def derived(seed, field) = DERIVED.fetch(seed.fetch("id")).fetch(field)

  # `parentSeedId` names the seed that forms this row's `parent`; that seed's own parent forms
  # `parent.inner` (ADR 0005).
  def parent_seed_of(seed)
    parent_id = seed&.fetch("parentSeedId")
    parent_id && SEEDS_BY_ID.fetch(parent_id)
  end
end

RSpec.configure do |config|
  config.after(:suite) { ConformanceCorpus.write_fuzz_results }
end
