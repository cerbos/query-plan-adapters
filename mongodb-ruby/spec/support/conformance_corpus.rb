# frozen_string_literal: true

require "time"

# Reads the shared conformance corpus: the dataset, the recorded goldens, and this adapter's
# ledger. See conformance/README.md, "The harness contract".
#
# Duplicated across adapters on purpose (ADR 0007). Do not extract or share it.
module ConformanceCorpus
  DIR = File.expand_path("../../../conformance", __dir__)
  LEDGER_FILE = File.expand_path("../../conformance-ledger.json", __dir__)

  def self.read_json(path) = JSON.parse(File.read(path, encoding: "UTF-8"))

  SEEDS = read_json(File.join(DIR, "seeds.json")).fetch("seeds").freeze
  SEEDS_BY_ID = SEEDS.to_h { |seed| [seed.fetch("id"), seed] }.freeze
  DERIVED = read_json(File.join(DIR, "derived-fields.json")).fetch("derived").freeze

  PDP_TAGS = read_json(File.join(DIR, "pdp-versions.json"))
    .values_at("current", "previous").map { |pdp| pdp.fetch("tag") }.freeze

  LEDGER = read_json(LEDGER_FILE).fetch("cases").freeze

  # The planner folds `now() - duration("24h")` into a literal, which the goldens record as
  # this placeholder. The real instant goes back in at the nanosecond precision the planner
  # emits, and that is load-bearing here: a BSON date holds milliseconds, so this adapter refuses
  # the real instant, and a tidy millisecond substitute would translate in the harness while the
  # same case refuses in production.
  NOW_PLACEHOLDER = "__NOW_MINUS_24H__"
  NOW_MINUS_24H = Time.at(Time.now.to_i - 86_400, 123_456_789, :nsec).utc.iso8601(9).freeze

  module_function

  # The attributes of every resource check() saw, as dotted paths under `request.resource.attr`.
  # A list or scalar ends a path; an object is walked into (`parent.inner.aBool`, `obj.inner`).
  def resource_attribute_paths
    read_json(File.join(DIR, "resources.json")).fetch("resources")
      .flat_map { |resource| attribute_paths(resource.fetch("attr"), "request.resource.attr") }
      .uniq.sort
  end

  def attribute_paths(value, prefix)
    return [prefix] unless value.is_a?(Hash)

    value.flat_map { |key, child| attribute_paths(child, "#{prefix}.#{key}") }
  end

  def read_golden(path) = JSON.parse(File.read(path, encoding: "UTF-8").gsub(NOW_PLACEHOLDER, NOW_MINUS_24H))

  # @return [Array<Hash>] every golden file recorded against one PDP, sorted by case id.
  # Raises when the tag has no golden files, so a missing directory cannot pass as zero cases.
  def goldens(tag)
    paths = Dir[File.join(DIR, "golden", tag, "**", "*.json")].sort
    raise "no golden files for PDP #{tag} under #{File.join(DIR, "golden", tag)}" if paths.empty?

    paths.map { |path| read_golden(path) }
  end

  # One golden by case id, for the unit suites: a plan the planner really produced, rather than
  # one typed by hand.
  def golden(id, tag = PDP_TAGS.first) = read_golden(File.join(DIR, "golden", tag, "#{id}.json"))

  # The ledger entry for a case under one PDP, or nil. An entry scoped with `pdp` applies only
  # to the tags it lists.
  def ledger_entry(id, tag)
    entry = LEDGER[id]
    entry if entry && (entry["pdp"].nil? || entry["pdp"].include?(tag))
  end

  # A plannerDivergence is a PDP bug no adapter can pass. The generator records it only in the
  # golden of a tag it applies to.
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
