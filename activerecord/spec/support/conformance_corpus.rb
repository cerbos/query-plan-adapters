# frozen_string_literal: true

require "json"

# Reads the shared corpus of the repository (cerbos/query-plan-adapters#263).
#
# Nothing here is data only for this adapter. The same rows and the same attributes go to both
# sides of the differential comparison. Thus a value that this file invents, or a corpus field
# that it drops, makes the oracle agree with the adapter for an incorrect reason and no test
# downstream can see it. Two guards hold that shut:
#
# * the derived fields come from conformance/derived-fields.json. They are not calculated here.
# * the seed keys, the tag keys and the derived-field names are asserted against the JSON. A
#   key that this harness does not read is an error, and so is a key that it reads and the
#   corpus no longer carries.
module ConformanceCorpus
  DIR = File.expand_path("../../../conformance", __dir__)

  SEEDS_FILE = JSON.parse(File.read(File.join(DIR, "seeds.json"))).freeze
  CATALOG_FILE = JSON.parse(File.read(File.join(DIR, "catalog.json"))).freeze
  CHECK_RESOURCES_FILE = JSON.parse(File.read(File.join(DIR, "check-resources.json"))).freeze
  DERIVED_FILE = JSON.parse(File.read(File.join(DIR, "derived-fields.json"))).freeze
  CERBOS_VERSION = File.read(File.join(DIR, "CERBOS_VERSION")).strip.freeze

  SEEDS = SEEDS_FILE.fetch("seeds").freeze
  RESOURCE_KIND = SEEDS_FILE.fetch("resourceKind").freeze
  # Verbatim. A projection here — an allowlist of principal attributes, for example — would
  # drop the attribute that a new action discriminates on from the plan AND from the oracle,
  # and the action would then pass without testing anything.
  PRINCIPAL = CHECK_RESOURCES_FILE.fetch("principal").freeze
  CHECK_RESOURCES = CHECK_RESOURCES_FILE.fetch("resources").freeze

  # The key of this adapter in the manifest is the name of its directory.
  ADAPTER = "activerecord"
  MANIFEST_FILE = JSON.parse(
    File.read(File.expand_path("../../../#{ADAPTER}/adapterctl.json", __dir__))
  ).freeze

  # --- corpus coverage guards ---------------------------------------------------------------

  SEED_KEYS = %w[
    id aBool aString aNumber aOptionalString tags subCategoryNames parentSeedId
  ].freeze
  # Corpus prose that no harness reads: the one documented exclusion from SEED_KEYS.
  SEED_NOTE_KEY = "note"
  # The one array of nested objects that a seed carries. A key added inside an element
  # disappears from both sides of the differential as quietly as a key at the top level.
  TAG_KEYS = %w[id name].freeze
  DERIVED_KEYS = %w[createdBy aDouble createdAt scope labels].freeze

  # The corpus principal is guarded the same way and for the same reason. It feeds the PLAN
  # under test AND the check() oracle, so an attribute dropped on the way in vanishes from both
  # sides at once: the plan folds to ALWAYS_DENIED and the oracle, built from the same
  # principal, agrees with it. `id` and `roles` are deliberately in scope one level above the
  # attributes, the same two-level shape SEED_KEYS and TAG_KEYS use for a row and its tags
  # (cerbos/query-plan-adapters#399).
  PRINCIPAL_KEYS = %w[id roles attr].freeze
  PRINCIPAL_ATTR_KEYS = %w[allowedTags context fewTeams manyTeams].freeze

  module_function

  def assert_keys!(label, got, want, optional = [])
    unexpected = got - want - optional
    unless unexpected.empty?
      raise "#{label} carries #{unexpected.inspect}, which this harness does not read. An " \
            "unread corpus field disappears from the stored row and from the check() oracle " \
            "at the same time, so the differential still agrees and the field tests nothing."
    end

    missing = want - got
    raise "#{label} is missing #{missing.inspect}, which this harness reads." unless missing.empty?
  end

  # SEEDS holds the parsed JSON rows without a change, so `keys` reports the key set of the
  # corpus. Keep it that way. A reader that rebuilt each row field by field could only report
  # the keys that this file already names, and the assertion would then prove nothing.
  SEEDS.each_with_index do |seed, index|
    assert_keys!("seeds.json seeds[#{index}]", seed.keys, SEED_KEYS, [SEED_NOTE_KEY])
    seed.fetch("tags").each_with_index do |tag, tag_index|
      assert_keys!("seeds.json seeds[#{index}].tags[#{tag_index}]", tag.keys, TAG_KEYS)
    end
  end

  # PRINCIPAL holds the parsed JSON object and goes to the SDK without a change, so `keys`
  # reports the key set of the corpus on both levels.
  assert_keys!("seeds.json principal", PRINCIPAL.keys, PRINCIPAL_KEYS)
  assert_keys!("seeds.json principal.attr", PRINCIPAL.fetch("attr").keys, PRINCIPAL_ATTR_KEYS)

  # The two value shapes those attributes take. A reshaped attribute — a list flattened to a
  # string, a string wrapped in a list — reaches the plan and the oracle at the same time, so
  # the differential agrees and the action proves nothing.
  PRINCIPAL.fetch("attr").each do |name, value|
    next if value.is_a?(String)
    next if value.is_a?(Array) && value.all? { |item| item.is_a?(String) }

    raise "seeds.json principal.attr.#{name} is neither a string nor a list of strings, the " \
          "only two shapes this harness reads"
  end

  assert_keys!("derived-fields.json fields", DERIVED_FILE.fetch("fields"), DERIVED_KEYS)

  DERIVED = DERIVED_FILE.fetch("derived").freeze
  if DERIVED.keys.sort != SEEDS.map { |seed| seed.fetch("id") }.sort
    raise "derived-fields.json must carry exactly one entry for each seed id"
  end
  DERIVED.each do |id, entry|
    assert_keys!("derived-fields.json derived[#{id.inspect}]", entry.keys, DERIVED_KEYS)
  end

  # --- v1 control plane ---------------------------------------------------------------------

  unless [CATALOG_FILE, CHECK_RESOURCES_FILE, MANIFEST_FILE].all? {
    |file| file.fetch("schemaVersion") == 1
  }
    raise "control-plane files must use schemaVersion 1"
  end
  raise "adapterctl.json names the wrong adapter" unless MANIFEST_FILE.fetch("adapter") == ADAPTER

  CATALOG = CATALOG_FILE.fetch("actions").freeze
  ALL_CATALOG_ACTIONS = CATALOG.map { |entry| entry.fetch("name") }.freeze
  SELECTED_ACTION = ENV.fetch("ADAPTERCTL_ACTION", "").strip.freeze

  def effective_outcomes(catalog_actions, outcomes, selected_action)
    if catalog_actions.uniq.size != catalog_actions.size
      raise "catalog action names must be unique"
    end
    if !selected_action.empty? && !catalog_actions.include?(selected_action)
      raise "ADAPTERCTL_ACTION names unknown catalog action #{selected_action.inspect}"
    end
    if selected_action.empty?
      unless outcomes.keys.sort == catalog_actions.sort
        raise "adapterctl outcomes must cover the catalog exactly"
      end
      return outcomes
    end

    selected_outcome = outcomes[selected_action]
    return outcomes unless selected_outcome.nil? || selected_outcome.fetch("status") == "unassessed"

    outcomes.merge(selected_action => {"status" => "matched"})
  end

  OUTCOMES = effective_outcomes(
    ALL_CATALOG_ACTIONS,
    MANIFEST_FILE.fetch("outcomes"),
    SELECTED_ACTION
  ).freeze

  def selected?(action)
    SELECTED_ACTION.empty? || SELECTED_ACTION == action
  end

  MANIFEST_ACTIONS = ALL_CATALOG_ACTIONS.select { |action| selected?(action) }.freeze
  ORACLE_EXPECTATIONS = CATALOG
    .select { |entry| selected?(entry.fetch("name")) }
    .to_h { |entry| [entry.fetch("name"), entry.fetch("oracleExpectation")] }
    .freeze

  UPSTREAM_BLOCKED = MANIFEST_ACTIONS.select {
    |action| OUTCOMES.fetch(action).fetch("status") == "upstream-blocked"
  }.freeze

  ORACLE_ACTIONS = MANIFEST_ACTIONS.select {
    |action| OUTCOMES.fetch(action).fetch("status") == "matched"
  }.freeze

  REPRESENTATION_DEPENDENT_REJECTIONS = MANIFEST_ACTIONS
    .select { |action|
      action == "null-eq-missing" && OUTCOMES.fetch(action).fetch("status") == "rejected"
    }
    .freeze

  def require_message(label, message)
    if message.nil? || message.empty?
      raise "adapterctl.json pins no throw message for #{label}: the throw suite would then " \
            "accept a failure for any reason at all"
    end
    message
  end

  THROWING_ACTIONS = MANIFEST_ACTIONS.filter_map { |action|
    outcome = OUTCOMES.fetch(action)
    status = outcome.fetch("status")
    case status
    when "rejected"
      reason = outcome.fetch("reason", "")
      raise "adapterctl.json rejected outcome #{action.inspect} has no reason" if reason.empty?
      next if action == "null-eq-missing"
      [action, require_message("outcomes.#{action}", outcome["message"])]
    when "matched", "upstream-blocked"
      nil
    when "unassessed"
      raise "adapterctl.json outcome #{action.inspect} is unassessed"
    else
      raise "adapterctl.json outcome #{action.inspect} has unknown status #{status.inspect}"
    end
  }.freeze

  REPRESENTATION_DEPENDENT_THROWS = REPRESENTATION_DEPENDENT_REJECTIONS.map { |action|
    outcome = OUTCOMES.fetch(action)
    reason = outcome.fetch("reason", "")
    raise "adapterctl.json rejected outcome #{action.inspect} has no reason" if reason.empty?
    [action, require_message("outcomes.#{action}", outcome["message"])]
  }.freeze

  # --- the golden wire fixtures (ADR 0006) ---------------------------------------------------
  #
  # One recorded PlanResources response per corpus action, captured against the pinned PDP. The
  # translator unit test reads its plans from here, so it needs no PDP and no policy — and it
  # pins planner SHAPE rather than whatever this adapter happens to ask for.

  WIRE_FIXTURES_DIR = File.join(DIR, "wire-fixtures")

  # regenerate-wire-fixtures.sh rewrites the folded `now() - duration("24h")` literal to this
  # placeholder, because the real one moves on every replan and every fixture would then differ.
  # A reader has to put an instant back, and the PRECISION of the one it chooses is load-bearing
  # here: the planner emits nanoseconds, and that is exactly what ts-window and ts-vf are
  # classified `rejected` for. Substituting a microsecond instant would make both
  # translate and read as a misclassification.
  NOW_PLACEHOLDER = "__NOW_MINUS_24H__"
  PLANNED_AT = "2026-08-11T09:13:39.123456789Z"

  def wire_fixture_actions
    Dir[File.join(WIRE_FIXTURES_DIR, "*.json")]
      .map { |path| File.basename(path, ".json") }
      .sort
  end

  # @return [Hash] the recorded response, ready to hand to the adapter as-is: it carries the
  #   plan under `filter`, which is the protobuf shape Plan.normalise already reads.
  def wire_fixture(action)
    substitute_planned_at(JSON.parse(File.read(File.join(WIRE_FIXTURES_DIR, "#{action}.json"))))
  end

  def substitute_planned_at(node)
    case node
    when Hash
      if node.dig("expression", "operands", 0, "value") == NOW_PLACEHOLDER
        node.merge("expression" => node.fetch("expression").merge(
          "operands" => [{"value" => PLANNED_AT}] + node.dig("expression", "operands").drop(1)
        ))
      else
        node.transform_values { |child| substitute_planned_at(child) }
      end
    when Array then node.map { |child| substitute_planned_at(child) }
    else node
    end
  end

  # --- derived fields, read and never calculated ---------------------------------------------

  def derived(seed)
    DERIVED.fetch(seed.fetch("id"))
  end

  def created_by(seed) = derived(seed).fetch("createdBy")

  def a_double(seed) = derived(seed).fetch("aDouble")

  def created_at(seed) = derived(seed).fetch("createdAt")

  def scope(seed) = derived(seed).fetch("scope")

  def labels(seed) = derived(seed).fetch("labels")

  # --- the real to-one relation (conformance/README.md, ADR 0005) -----------------------------
  #
  # `parentSeedId` is the one seed key that resolves against another ROW. It names the seed
  # whose four scalars this row's `parent` carries, and that seed's own `parentSeedId` names
  # the ones `parent.inner` carries. The chain stops at two levels.
  #
  # A resource owns a FRESH parent row, and does not point at the row of the named seed. Thus
  # no two resources use one parent, and a filter that gave the parent in place of the child
  # cannot agree with the oracle by accident.
  SEEDS_BY_ID = SEEDS.to_h { |seed| [seed.fetch("id"), seed] }.freeze

  def parent_seed_of(seed)
    return nil if seed.nil?

    parent_id = seed.fetch("parentSeedId")
    return nil if parent_id.nil?

    SEEDS_BY_ID.fetch(parent_id) do
      raise "seeds.json: #{seed.fetch("id").inspect} names parent #{parent_id.inspect}, " \
            "which is not a seed id"
    end
  end

  # The four scalars of one hop as check() attributes. A NULL column is a MISSING attribute
  # one hop out, exactly as it is on the root row.
  def relation_attr(seed)
    attr = {
      "aBool" => seed.fetch("aBool"),
      "aString" => seed.fetch("aString"),
      "aNumber" => seed.fetch("aNumber")
    }
    optional = seed.fetch("aOptionalString")
    attr["aOptionalString"] = optional unless optional.nil?
    attr
  end
end
