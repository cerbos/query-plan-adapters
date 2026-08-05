# frozen_string_literal: true

# Loads the repo-level shared corpus (cerbos/query-plan-adapters#263) and the deterministic
# derived fields documented in conformance/README.md.
#
# These derivations are part of the shared contract, not adapter-specific fixtures: the same
# rows and the same attributes drive both sides of the differential comparison, so getting
# one wrong makes the oracle agree with the adapter for the wrong reason.
module ConformanceCorpus
  DIR = File.expand_path("../../../conformance", __dir__)

  SEEDS_FILE = JSON.parse(File.read(File.join(DIR, "seeds.json"))).freeze
  ACTIONS_FILE = JSON.parse(File.read(File.join(DIR, "actions.json"))).freeze
  CERBOS_VERSION = File.read(File.join(DIR, "CERBOS_VERSION")).strip.freeze

  SEEDS = SEEDS_FILE.fetch("seeds").freeze
  RESOURCE_KIND = SEEDS_FILE.fetch("resourceKind").freeze
  PRINCIPAL = SEEDS_FILE.fetch("principal").freeze

  # This adapter's key in the manifest is its directory name.
  ADAPTER = "activerecord"

  UNSUPPORTED = ACTIONS_FILE
    .fetch("adapterUnsupported", {})
    .fetch(ADAPTER, [])
    .to_h { |entry| [entry.fetch("action"), entry.fetch("reason")] }
    .freeze

  SUPPORTED_EXPECTED = ACTIONS_FILE
    .fetch("adapterSupportedExpected", {})
    .fetch(ADAPTER, [])
    .map { |entry| entry.fetch("action") }
    .freeze

  EXPECTED_UNSUPPORTED = ACTIONS_FILE
    .fetch("expectedUnsupported")
    .map { |entry| entry.fetch("action") }
    .freeze

  SKIPPED = ACTIONS_FILE
    .fetch("knownDivergences", [])
    .select { |entry| entry.fetch("adapters").include?(ADAPTER) }
    .map { |entry| entry.fetch("action") }
    .freeze

  # The classification is derived from the manifest at runtime, never copied into the
  # harness — a new corpus action then reaches this adapter automatically.
  ORACLE_ACTIONS = (
    (ACTIONS_FILE.fetch("conformance") - UNSUPPORTED.keys) + SUPPORTED_EXPECTED - SKIPPED
  ).freeze

  THROWING_ACTIONS = (UNSUPPORTED.keys + EXPECTED_UNSUPPORTED - SUPPORTED_EXPECTED).uniq.sort.freeze

  module_function

  def created_by(seed)
    (seed.fetch("aNumber") >= 2) ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z"
  end

  def a_double(seed)
    case seed.fetch("id")
    when "a1" then -0.6
    when "a2" then 0.25
    when "a3" then nil
    else seed.fetch("aNumber") + 0.3
    end
  end

  CREATED_AT = {
    "a1" => "2020-03-15T10:30:00Z",
    "a2" => "2037-01-01T00:00:00Z",
    "a3" => nil,
    "a4" => "2024-06-01T00:00:00Z",
    "a5" => "2020-03-15T10:30:00.123456Z"
  }.freeze

  def created_at(seed)
    id = seed.fetch("id")
    return CREATED_AT.fetch(id) if CREATED_AT.key?(id)

    (seed.fetch("aNumber") >= 2) ? "2036-06-06T06:06:06Z" : "2021-05-05T05:05:05Z"
  end

  SCOPES = {
    "a1" => "dept",
    "a2" => "dept.eng",
    "a3" => "dept.eng.platform",
    "a4" => "dept.eng.platform.obs",
    "a5" => "dept.engineering",
    "a6" => "dept.sales",
    "a7" => nil,
    "a8" => "",
    "a9" => "50%",
    "b1" => "50%:a_b:x",
    "b2" => "50x:a_b:y",
    "b3" => "50%:aXb:y",
    "b4" => "50%:a_b",
    "b5" => "dept.eng.platform2",
    "b6" => "50%.a_b",
    "c1" => "Dept.Eng",
    "c2" => "dept.eng.",
    "d1" => "[env]:prod:eu",
    "d2" => "e:prod:eu"
  }.freeze

  def scope(seed)
    SCOPES[seed.fetch("id")]
  end

  LABELS = {
    "a1" => ["gold", "silver"],
    "a6" => [nil, "silver"],
    "a8" => ["silver"],
    "c1" => ["Gold"]
  }.freeze

  def labels(seed)
    LABELS.fetch(seed.fetch("id"), [])
  end
end
