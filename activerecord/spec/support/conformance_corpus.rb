# frozen_string_literal: true

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
  ACTIONS_FILE = JSON.parse(File.read(File.join(DIR, "actions.json"))).freeze
  DERIVED_FILE = JSON.parse(File.read(File.join(DIR, "derived-fields.json"))).freeze
  CERBOS_VERSION = File.read(File.join(DIR, "CERBOS_VERSION")).strip.freeze

  SEEDS = SEEDS_FILE.fetch("seeds").freeze
  RESOURCE_KIND = SEEDS_FILE.fetch("resourceKind").freeze
  # Verbatim. A projection here — an allowlist of principal attributes, for example — would
  # drop the attribute that a new action discriminates on from the plan AND from the oracle,
  # and the action would then pass without testing anything.
  PRINCIPAL = SEEDS_FILE.fetch("principal").freeze

  # The key of this adapter in the manifest is the name of its directory.
  ADAPTER = "activerecord"

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

  assert_keys!("derived-fields.json fields", DERIVED_FILE.fetch("fields"), DERIVED_KEYS)

  DERIVED = DERIVED_FILE.fetch("derived").freeze
  if DERIVED.keys.sort != SEEDS.map { |seed| seed.fetch("id") }.sort
    raise "derived-fields.json must carry exactly one entry for each seed id"
  end
  DERIVED.each do |id, entry|
    assert_keys!("derived-fields.json derived[#{id.inspect}]", entry.keys, DERIVED_KEYS)
  end

  # --- classification, read from the manifest at run time ------------------------------------
  #
  # Each group is read by name. A group that this file did not name would disappear from every
  # count and from every test at the same time, and the run would then pass without a word.
  # That is the projection trap in conformance/README.md.

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

  # Actions that probe `== null` against an attribute whose NULL columns the oracle OMITS.
  # The harness translates these with the null representation of the adapter set to omitted and
  # asserts the rejection. Their oracle is empty by construction, so they must not join the
  # degeneracy guard.
  NULL_REPRESENTATION_OMITTED = ACTIONS_FILE
    .fetch("nullRepresentationOmitted", [])
    .map { |entry| entry.fetch("action") }
    .freeze

  SKIPPED = ACTIONS_FILE
    .fetch("knownDivergences", [])
    .select { |entry| entry.fetch("adapters").include?(ADAPTER) }
    .map { |entry| entry.fetch("action") }
    .freeze

  ORACLE_ACTIONS = (
    (ACTIONS_FILE.fetch("conformance") - UNSUPPORTED.map { |entry| entry.fetch("action") }) +
      SUPPORTED_EXPECTED - SKIPPED
  ).freeze

  # Every action that the corpus classifies, whichever adapter it belongs to. A divergence that
  # only another adapter registered must still arrive here, so the size tripwire and the
  # "classified exactly once" test bring it up for triage instead of letting it disappear.
  MANIFEST_ACTIONS = (
    ACTIONS_FILE.fetch("conformance") +
      EXPECTED_UNSUPPORTED +
      NULL_REPRESENTATION_OMITTED +
      SUPPORTED_EXPECTED +
      ACTIONS_FILE.fetch("knownDivergences", []).map { |entry| entry.fetch("action") }
  ).uniq.freeze

  # The message that the error of a throwing action must contain.
  #
  # Without it, "it threw" is satisfied as happily by a typo in the attribute map, by an
  # unrelated validation or by a transport error as by the limitation that the classification
  # names — and the classification then rests on a failure that never reached its own mechanism
  # (cerbos/query-plan-adapters#326).
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

  # The same, for the group that every adapter must refuse under the `omitted` representation.
  NULL_OMITTED_THROWS = ACTIONS_FILE
    .fetch("nullRepresentationOmitted", [])
    .map { |entry|
      action = entry.fetch("action")
      [action, require_message(
        "nullRepresentationOmitted.#{action}.messages.#{ADAPTER}", entry["messages"]&.[](ADAPTER)
      )]
    }
    .freeze

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
