# frozen_string_literal: true

# Reads the shared conformance corpus (#263).
#
# The same data feeds both the adapter and the oracle, so an invented value or a dropped field
# would make them agree silently. Two guards:
#
# * derived fields are read from conformance/derived-fields.json, never computed here.
# * key sets are asserted against the JSON: an unread key is an error, and so is a missing one.
module ConformanceCorpus
  DIR = File.expand_path("../../../conformance", __dir__)

  SEEDS_FILE = JSON.parse(File.read(File.join(DIR, "seeds.json"))).freeze
  ACTIONS_FILE = JSON.parse(File.read(File.join(DIR, "actions.json"))).freeze
  DERIVED_FILE = JSON.parse(File.read(File.join(DIR, "derived-fields.json"))).freeze
  CERBOS_VERSION = File.read(File.join(DIR, "CERBOS_VERSION")).strip.freeze

  SEEDS = SEEDS_FILE.fetch("seeds").freeze
  RESOURCE_KIND = SEEDS_FILE.fetch("resourceKind").freeze
  # Verbatim. An allowlist would drop a new attribute from both plan and oracle, and the action
  # would pass without testing anything.
  PRINCIPAL = SEEDS_FILE.fetch("principal").freeze

  # This adapter's key in actions.json (its directory name).
  ADAPTER = "activerecord"

  # --- corpus coverage guards ---------------------------------------------------------------

  SEED_KEYS = %w[
    id aBool aString aNumber aOptionalString aNumberList aBoolList tags subCategoryNames
    parentSeedId
  ].freeze
  # Prose no harness reads; the only exclusion from SEED_KEYS.
  SEED_NOTE_KEY = "note"
  # Tags are the only nested objects in a seed, so their keys are guarded too.
  TAG_KEYS = %w[id name].freeze
  DERIVED_KEYS = %w[createdBy aDouble createdAt scope labels updatedAt].freeze

  # The principal feeds both the plan and the oracle, so its keys are guarded the same way,
  # on two levels: the principal itself and its `attr` (#399).
  PRINCIPAL_KEYS = %w[id roles attr].freeze
  PRINCIPAL_ATTR_KEYS = %w[allowedTags context fewTeams manyTeams zero emptyTeams manyStructs nullableStructs missingStructs].freeze

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

  # SEEDS is the raw parsed JSON, so `keys` is the corpus's real key set. Keep it raw: a
  # rebuilt row would only report keys this file already knows.
  SEEDS.each_with_index do |seed, index|
    assert_keys!("seeds.json seeds[#{index}]", seed.keys, SEED_KEYS, [SEED_NOTE_KEY])
    seed.fetch("tags").each_with_index do |tag, tag_index|
      assert_keys!("seeds.json seeds[#{index}].tags[#{tag_index}]", tag.keys, TAG_KEYS)
    end

    # Elements go into a typed column. A wrong-typed element would be coerced in SQLite while
    # check() saw the original. Null elements are allowed and stored.
    {"aNumberList" => [Numeric], "aBoolList" => [TrueClass, FalseClass]}.each do |key, types|
      list = seed.fetch(key)
      raise "seeds.json seeds[#{index}].#{key} must be a list" unless list.is_a?(Array)

      stray = list.reject { |element| element.nil? || types.any? { |type| element.is_a?(type) } }
      next if stray.empty?

      raise "seeds.json seeds[#{index}].#{key} carries #{stray.inspect}, which its typed " \
            "element column cannot hold"
    end
  end

  # PRINCIPAL is the raw parsed JSON, so `keys` is the corpus's real key set.
  assert_keys!("seeds.json principal", PRINCIPAL.keys, PRINCIPAL_KEYS)
  assert_keys!("seeds.json principal.attr", PRINCIPAL.fetch("attr").keys, PRINCIPAL_ATTR_KEYS)

  # Check each attribute's value shape. A reshaped value would reach plan and oracle alike and
  # go unnoticed.
  PRINCIPAL.fetch("attr").each do |name, value|
    if name == "zero"
      raise "principal zero must be numeric" unless value.is_a?(Numeric)
      next
    end
    if %w[manyStructs nullableStructs missingStructs].include?(name)
      raise "principal #{name} must be a list" unless value.is_a?(Array)
      value.each do |item|
        raise "principal #{name} element must be an object" unless item.is_a?(Hash)
        assert_keys!("principal #{name} element", item.keys, (name == "missingStructs") ? [] : ["name"])
        if name == "manyStructs"
          raise "principal #{name} name must be a string" unless item.fetch("name").is_a?(String)
        elsif name == "nullableStructs"
          raise "principal #{name} name must be null" unless item.fetch("name").nil?
        end
      end
      next
    end
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

  # --- classification, read from actions.json -----------------------------------------------
  #
  # Each group is read by name. An unnamed group would vanish from every count and test
  # silently (the projection trap in conformance/README.md).

  UNSUPPORTED = ACTIONS_FILE
    .fetch("adapterUnsupported", {})
    .fetch(ADAPTER, [])
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

  # `== null` probes on attributes whose NULLs the oracle omits. Translated with the `omitted`
  # representation and asserted to throw. Their oracle is empty by construction.
  NULL_REPRESENTATION_OMITTED = ACTIONS_FILE
    .fetch("nullRepresentationOmitted", [])
    .map { |entry| entry.fetch("action") }
    .freeze

  # Actions whose oracle is empty or total by construction: action => "empty" | "total".
  # Every other compared action must have neither, since a degenerate oracle cannot fail
  # (conformance/README.md, "The degeneracy guard").
  DEGENERATE_ORACLE_SHAPES = %w[empty total].freeze
  DEGENERATE_ORACLES = ACTIONS_FILE
    .fetch("degenerateOracles")
    .to_h { |entry|
      action = entry.fetch("action")
      oracle = entry.fetch("oracle")
      unless DEGENERATE_ORACLE_SHAPES.include?(oracle)
        raise "actions.json degenerateOracles.#{action} declares oracle #{oracle.inspect}, " \
              "which is neither \"empty\" nor \"total\""
      end
      [action, oracle]
    }
    .freeze
  if DEGENERATE_ORACLES.size != ACTIONS_FILE.fetch("degenerateOracles").size
    raise "actions.json degenerateOracles lists an action more than once"
  end

  SKIPPED = ACTIONS_FILE
    .fetch("knownDivergences", [])
    .select { |entry| entry.fetch("adapters").include?(ADAPTER) }
    .map { |entry| entry.fetch("action") }
    .freeze

  ORACLE_ACTIONS = (
    (ACTIONS_FILE.fetch("conformance") - UNSUPPORTED.map { |entry| entry.fetch("action") }) +
      SUPPORTED_EXPECTED - SKIPPED
  ).freeze

  # Every classified action, for any adapter, so the size tripwire and the "classified exactly
  # once" test see divergences registered only by other adapters.
  MANIFEST_ACTIONS = (
    ACTIONS_FILE.fetch("conformance") +
      EXPECTED_UNSUPPORTED +
      NULL_REPRESENTATION_OMITTED +
      SUPPORTED_EXPECTED +
      ACTIONS_FILE.fetch("knownDivergences", []).map { |entry| entry.fetch("action") }
  ).uniq.freeze

  # The message a throwing action's error must contain. Without it, any error (a mapping typo,
  # a transport error) would pass as the classified limitation (#326).
  def require_message(label, message)
    if message.nil? || message.empty?
      raise "actions.json pins no throw message for #{label}: the throw suite would then " \
            "accept a failure for any reason at all"
    end

    message
  end

  # [action, message] for every shape this adapter must refuse.
  THROWING_ACTIONS = (
    UNSUPPORTED.map { |entry|
      action = entry.fetch("action")
      [action, require_message("adapterUnsupported.#{ADAPTER}.#{action}", entry["message"])]
    } +
    ACTIONS_FILE.fetch("expectedUnsupported")
      .reject { |entry| SUPPORTED_EXPECTED.include?(entry.fetch("action")) }
      .map { |entry|
        action = entry.fetch("action")
        [action, require_message(
          "expectedUnsupported.#{action}.messages.#{ADAPTER}", entry["messages"]&.[](ADAPTER)
        )]
      }
  ).freeze

  # The same, for actions every adapter must refuse under the `omitted` representation.
  NULL_OMITTED_THROWS = ACTIONS_FILE
    .fetch("nullRepresentationOmitted", [])
    .map { |entry|
      action = entry.fetch("action")
      [action, require_message(
        "nullRepresentationOmitted.#{action}.messages.#{ADAPTER}", entry["messages"]&.[](ADAPTER)
      )]
    }
    .freeze

  # --- the golden wire fixtures (ADR 0006) ---------------------------------------------------
  #
  # One recorded PlanResources response per corpus action, from the pinned PDP. The translator
  # unit test reads its plans from here, so it needs no PDP.

  WIRE_FIXTURES_DIR = File.join(DIR, "wire-fixtures")

  # The fixtures replace the folded `now() - duration("24h")` with a placeholder so replans do
  # not churn. We put back an instant with nanosecond precision, as the planner emits:
  # ts-window and ts-vf are refused for exactly that, and a microsecond value would let them
  # translate.
  NOW_PLACEHOLDER = "__NOW_MINUS_24H__"
  PLANNED_AT = "2026-08-11T09:13:39.123456789Z"

  def wire_fixture_actions
    Dir[File.join(WIRE_FIXTURES_DIR, "*.json")]
      .map { |path| File.basename(path, ".json") }
      .sort
  end

  # @return [Hash] the recorded response; the plan is under `filter`, which Plan.normalise reads.
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
  def updated_at(seed) = derived(seed).fetch("updatedAt")

  def scope(seed) = derived(seed).fetch("scope")

  def labels(seed) = derived(seed).fetch("labels")

  # --- the real to-one relation (conformance/README.md, ADR 0005) -----------------------------
  #
  # `parentSeedId` names the seed whose scalars `parent` copies; that seed's own `parentSeedId`
  # gives `parent.inner`. Two levels at most.
  #
  # Each resource gets its own new parent row, so a filter that confused parent and child
  # cannot match the oracle by accident.
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

  # One hop's scalars as check() attributes. A NULL column is omitted, as on the root row.
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
