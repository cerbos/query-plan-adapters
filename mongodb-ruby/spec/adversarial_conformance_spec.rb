# frozen_string_literal: true

require "cerbos"
require "mongo"
require "cerbos/mongodb/mongoid"

# The adversarial differential conformance harness (cerbos/query-plan-adapters#263).
#
# For each action in the shared conformance/ corpus it plans against a real Cerbos PDP (pinned
# by conformance/CERBOS_VERSION and conformance/CERBOS_IMAGE_DIGEST, loading
# conformance/policies/), translates the plan through this adapter's public interface, runs the
# filter against the corpus documents in a real MongoDB, and compares the ids with an oracle: the
# same PDP's check() decision for every document, sent the same attributes the document holds.
# Nobody writes an expected id set. See conformance/README.md for the oracle recipe, the NULL
# conventions and the degeneracy guard.
#
# The stored document shape is spec/support/adversarial_store.rb and the mapper is
# spec/support/corpus_mapper.rb, which the translator unit test replays the wire fixtures with.
Mongo::Logger.logger.level = Logger::FATAL

RSpec.describe "adversarial conformance" do
  before(:all) { AdversarialStore.establish! }

  MAPPER = CorpusMapper::MAPPER
  ALL_IDS = ConformanceCorpus::SEEDS.map { |seed| seed.fetch("id") }.sort.freeze

  def translate(plan, null_attribute_representation: :explicit)
    Cerbos::MongoDB.query_plan_to_filter(
      plan: plan, mapper: MAPPER, null_attribute_representation: null_attribute_representation
    )
  end

  def adapter_filtered_ids(action, null_attribute_representation: :explicit)
    result = translate(AdversarialOracle.plan(action), null_attribute_representation: null_attribute_representation)
    # ALWAYS_DENIED still runs its match-nothing filter: the query is the thing under test.
    AdversarialStore.ids(result.filter)
  end

  # Whether any operand anywhere in the SDK's plan is a literal null, or a list holding one.
  def plan_carries_null?(node)
    case node
    when Cerbos::Output::PlanResources::Expression
      node.operands.any? { |operand| plan_carries_null?(operand) }
    when Cerbos::Output::PlanResources::Expression::Value
      node.value.nil? || (node.value.is_a?(Array) && node.value.any?(&:nil?))
    else
      false
    end
  end

  def expect_non_degenerate_oracle(action)
    ids = AdversarialOracle.allowed_ids(action)
    expect(ids).not_to be_empty, "#{action}: oracle allowed nothing"
    expect(ids.size).to be < ALL_IDS.size, "#{action}: oracle allowed every seed"
  end

  # --- the degeneracy guard (conformance/README.md, "The degeneracy guard") ------------------
  #
  # One or more actions for each hostile group this adapter ORACLE-COMPARES. Each is asserted to
  # be in the oracle set, so moving an action into adapterUnsupported fails here instead of
  # quietly emptying the guard. Derived from this adapter's own oracle set, not copied: a list
  # built for another adapter names shapes this one never compares (#324).
  #
  # w1-size-zero-chain, w1-not-size-chain, w1-size-frac-chain, double-huge-lt and the two
  # string-cast actions are absent from both lists: their oracles are empty by CONSTRUCTION.
  DEGENERACY_GUARD_ACTIONS = (%w[
    pv-in pv-in-unrolled vf-le like-percent all-on-empty pv-exists pv-all null-eq null-ne
  ] +
    # The explicit-null convention against a non-null operand (#308). A stored null is already
    # a value to MongoDB. The fifth sibling, null-value-pv-not-exists, is a liveness probe: the
    # negated macro the value-list fold produces is what is refused, not the null convention.
    %w[null-value-ne-const null-value-not-eq-const null-value-not-in-const null-value-f2f] +
    # The absent to-one parent (#309/#315/#316/#333/#334).
    %w[w1-all-chain w1-size-nonneg-chain w1-not-in-chain w1-not-hasint-chain w1-size-frac-le-chain] +
    # The cr-div group throws (#311); the computed-relation group is guarded by the
    # fractional-size shape this adapter translates.
    %w[cr-size-frac-ge] +
    # The real to-one relation (#375): the negated hop, the null comparison, two-level depth,
    # the root conjunction and the disjunction, whose failure direction is an under-grant.
    %w[rel-not-bool-hop rel-ne-null-hop rel-bool-hop2 rel-hop-and-root rel-hop2-or-exists] +
    # Case sensitivity in string matching, and the primary key as a filterable attribute (#376),
    # including the concatenation that used to send $add a string.
    %w[cs-contains id-eq-const id-f2f-ne id-concat cast-string-bool] +
    # Root position and bare operand forms (#388), and the hazard classes the corpus missed
    # (#387): positional reads of scalar lists, the value-first hasIntersection, the
    # below-cliff unroll.
    %w[not-lt root-bare-bool or-eq-exists not-and index-scalar-list index-scalar-list-not-eq
      index-scalar-list-null vf-hasint pv-exists-unrolled] +
    %w[string-size-gt0 in-map-keys double-huge-gt] +
    # #414: every newly discriminating shape guards its observed execution side.
    %w[hasint-map-null hasint-map-null-vf hasint-map-vf hasint-null-vf in-numbers lambda-in-literal
      not-concat-unsolvable-ne not-hasint-empty-chain pv-shadow regex-dot regex-unanchored
      root-not-bool size-ge-one wildcard-contains wildcard-endswith projection-exists-eq
      projection-exists-not-eq rel-not-eq-hop rel-not-contains-hop rel-not-hierarchy-hop] +
    # #396: error-bearing branches retain a non-empty oracle under their enclosing expression.
    %w[cast-not-string-null cast-not-timestamp index-not-oob regex-eq-true regex-final-newline] +
    # Number and boolean list elements: $arrayElemAt keeps each element's BSON type, so `true`
    # is not `1` and a null element is a value under negation.
    %w[index-number-list index-number-list-not-eq index-bool-list index-bool-list-not-eq
      index-bool-list-vs-number index-number-list-vs-bool] +
    # Two conditionals compared with each other. The driver sends $cond against $cond to the
    # server untouched and MongoDB evaluates it correctly; it is Mongoose's $expr caster, not the
    # server, that makes the mongoose adapter refuse this shape.
    %w[p-ternary-vs-ternary]).freeze

  # Shapes this adapter REFUSES, kept as PDP/policy liveness probes for groups the guard above
  # cannot cover. Each is asserted NOT to be in the oracle set, so a shape the adapter learns to
  # translate must move up into the guard proper.
  LIVENESS_ONLY_PROBES = %w[
    not-nan-order-string not-ternary-parent hier-empty-delim matches-alt w1-not-exists-chain
    null-value-pv-not-exists w1-ternary-chain-cond cr-div-neg-zero cast-int-double concat-f2f
    not-contains map-eq-list
    div-by-division eq-list except-eq except-size hier-overlaps-list-prefix in-var-var-omitted
    in-var-var-omitted-neg lambda-in-literal-neg lambda-ternary ne-list not-concat-unsolvable
    not-nan-ord-le pv-exists-one pv-filter pv-map pv-except pv-not-all pv-not-exists pv-structs
    regex-alternation regex-brace regex-case regex-digit regex-grouped regex-optional-operators
    regex-posix regex-repetition temporal-raw-eq
    cast-not-double cast-not-int cast-not-string-missing index-fractional index-negative
    regex-lookahead
  ].freeze

  describe "corpus" do
    # Corpus additions must update both the classification and the degeneracy tripwires.
    it "pins the corpus size and how it divides here" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(288)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(11)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
      expect(ConformanceCorpus::MANIFEST_ACTIONS.size).to eq(301)
      expect(ConformanceCorpus::UNSUPPORTED.size).to eq(89)
      expect(ConformanceCorpus::SUPPORTED_EXPECTED.size).to eq(4)
      expect(ConformanceCorpus::ORACLE_ACTIONS.size).to eq(203)
      expect(ConformanceCorpus::THROWING_ACTIONS.size).to eq(96)
      # Each new hostile group needs a representative here.
      expect(DEGENERACY_GUARD_ACTIONS.size).to eq(DEGENERACY_GUARD_ACTIONS.uniq.size)
      expect(DEGENERACY_GUARD_ACTIONS.size).to eq(73)
      expect(LIVENESS_ONLY_PROBES.size).to eq(LIVENESS_ONLY_PROBES.uniq.size)
    end

    # Adding a throwing action without a pinned message must fail the run rather than turn the
    # throw suite back into a bare "it threw" (cerbos/query-plan-adapters#326).
    it "refuses a throwing action that pins no message" do
      expect { ConformanceCorpus.require_message("synthetic", nil) }.to raise_error(/pins no throw message/)
      expect { ConformanceCorpus.require_message("synthetic", "") }.to raise_error(/pins no throw message/)
    end

    it "gives every action exactly one outcome" do
      oracle = ConformanceCorpus::ORACLE_ACTIONS
      throwing = ConformanceCorpus::THROWING_ACTIONS.map(&:first)
      null_omitted = ConformanceCorpus::NULL_REPRESENTATION_OMITTED
      skipped = ConformanceCorpus::SKIPPED

      misclassified = ConformanceCorpus::MANIFEST_ACTIONS.reject { |action|
        [oracle, throwing, null_omitted, skipped].count { |group| group.include?(action) } == 1
      }
      expect(misclassified).to be_empty
    end

    it "produces a non-degenerate oracle for the shapes it compares" do
      DEGENERACY_GUARD_ACTIONS.each do |action|
        expect(ConformanceCorpus::ORACLE_ACTIONS).to include(action),
          "#{action}: in the degeneracy guard but this adapter does not compare it"
        expect_non_degenerate_oracle(action)
      end
    end

    it "produces a non-degenerate oracle for the shapes it refuses" do
      LIVENESS_ONLY_PROBES.each do |action|
        expect(ConformanceCorpus::ORACLE_ACTIONS).not_to include(action),
          "#{action}: this adapter now translates it, so move it into the guard proper"
        expect_non_degenerate_oracle(action)
      end
    end

    # These shapes intentionally have empty or total oracles: type errors, unequal runtime
    # types, or empty-list identities. The planner kind is pinned too, so dropping their inputs
    # cannot silently turn a conditional error probe into a folded plan.
    {
      "except-root" => [:KIND_CONDITIONAL, false], "pv-empty-exists" => [:KIND_ALWAYS_DENIED, false],
      "pv-empty-not-exists" => [:KIND_ALWAYS_ALLOWED, true], "pv-empty-all" => [:KIND_ALWAYS_ALLOWED, true],
      "pv-empty-not-all" => [:KIND_ALWAYS_DENIED, false], "pv-structs-null" => [:KIND_CONDITIONAL, false],
      "pv-structs-missing" => [:KIND_ALWAYS_DENIED, false], "type-string-number" => [:KIND_CONDITIONAL, false],
      "type-number-string" => [:KIND_CONDITIONAL, false], "type-columns" => [:KIND_CONDITIONAL, false],
      "type-size-bool" => [:KIND_CONDITIONAL, false], "type-size-number" => [:KIND_CONDITIONAL, false],
      "type-hierarchy-number" => [:KIND_CONDITIONAL, false], "type-number-contains" => [:KIND_CONDITIONAL, false],
      "type-needle-contains" => [:KIND_CONDITIONAL, false], "type-number-startswith" => [:KIND_CONDITIONAL, false],
      "type-needle-startswith" => [:KIND_CONDITIONAL, false], "type-number-endswith" => [:KIND_CONDITIONAL, false],
      "type-needle-endswith" => [:KIND_CONDITIONAL, false], "eq-map" => [:KIND_CONDITIONAL, false],
      "ne-map" => [:KIND_CONDITIONAL, true], "eq-map-null" => [:KIND_CONDITIONAL, false],
      "in-nested-list" => [:KIND_CONDITIONAL, false], "in-list-element" => [:KIND_CONDITIONAL, false],
      "hasint-map-element" => [:KIND_CONDITIONAL, false]
    }.each do |action, (kind, total)|
      it "#{action} keeps its intentional #{total ? "total" : "empty"} oracle and planner shape" do
        expect(AdversarialOracle.plan(action).kind).to eq(kind)
        expect(AdversarialOracle.allowed_ids(action)).to eq(total ? ALL_IDS : [])
      end
    end

    # The seeder for the to-one chain, pinned directly (ADR 0005). The two hops are read back
    # out of the stored documents rather than counted: a count cannot tell the corpus's values
    # from the root's, which is exactly the flat-alias failure this relation exists to expose.
    it "seeds the to-one chain that the corpus describes" do
      with_parent = ConformanceCorpus::SEEDS.select { |seed| ConformanceCorpus.parent_seed_of(seed) }
      with_inner = ConformanceCorpus::SEEDS.select { |seed|
        ConformanceCorpus.parent_seed_of(ConformanceCorpus.parent_seed_of(seed))
      }
      expect(with_parent).not_to be_empty
      expect(with_inner).not_to be_empty
      expect(with_parent.size).to be < ConformanceCorpus::SEEDS.size

      stored = AdversarialStore.collection.find({}).to_h { |doc|
        [doc.fetch("resourceId"), [doc.dig("parent", "aString"), doc.dig("parent", "inner", "aString")]]
      }
      expected = ConformanceCorpus::SEEDS.to_h { |seed|
        parent = ConformanceCorpus.parent_seed_of(seed)
        inner = ConformanceCorpus.parent_seed_of(parent)
        [seed.fetch("id"), [parent&.fetch("aString"), inner&.fetch("aString")]]
      }
      expect(stored).to eq(expected)
    end

    # The positional actions read an element's BSON type. Read the lists back and compare them
    # with the corpus element for element, so a seeder that turned a null element into false or
    # 0 cannot leave the filter reading a list check() was never sent.
    it "seeds the number and boolean lists verbatim, null elements included" do
      stored = AdversarialStore.collection.find({}).to_h { |doc|
        [doc.fetch("resourceId"), [doc.fetch("aNumberList"), doc.fetch("aBoolList")]]
      }
      expected = ConformanceCorpus::SEEDS.to_h { |seed| [seed.fetch("id"), [seed.fetch("aNumberList"), seed.fetch("aBoolList")]] }
      expect(stored).to eq(expected)
      expect(expected.values.map(&:first).flatten).to include(nil)
      expect(expected.values.map(&:last).flatten).to include(nil)
    end

    # #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
    # refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION, so it
    # belongs to neither guard list, and a bare "it raises" would not say whether refusing it is
    # REQUIRED. This is that argument: the other conjunct is `R.attr.aBool`, which the adapter
    # certainly expresses, and an adapter that dropped the half it could not translate would
    # return every document that conjunct selects — all of which the PDP denies here.
    it "must refuse filter-as-conjunct, because the conjunct it can express over-grants" do
      expect(AdversarialOracle.allowed_ids("filter-as-conjunct")).to be_empty

      surviving_half = adapter_filtered_ids("root-bare-bool")
      expect(surviving_half).not_to be_empty
      expect(surviving_half.size).to be < ALL_IDS.size

      message = ConformanceCorpus::THROWING_ACTIONS.find { |(action, _)| action == "filter-as-conjunct" }.last
      expect { adapter_filtered_ids("filter-as-conjunct") }
        .to raise_error(Cerbos::MongoDB::Error, /#{Regexp.escape(message)}/)
    end
  end

  describe "matches the check() oracle" do
    ConformanceCorpus::ORACLE_ACTIONS.each do |action|
      it action do
        expect(adapter_filtered_ids(action)).to eq(AdversarialOracle.allowed_ids(action))
      end
    end
  end

  # The same corpus through Mongoid, on typed models (spec/support/adversarial_mongoid.rb), via
  # Cerbos::MongoDB::Mongoid.criteria. A bare `Model.where(filter)` is not safe — Mongoid converts
  # constants to each field's declared type — and the cast probes below are the proof.
  describe "through Mongoid" do
    before(:all) { AdversarialMongoid.configure!(ENV.fetch("MONGODB_URI")) }

    def mongoid_ids(criteria) = criteria.pluck(:resourceId).sort

    ConformanceCorpus::ORACLE_ACTIONS.each do |action|
      it "#{action} matches the check() oracle" do
        result = translate(AdversarialOracle.plan(action))
        criteria = Cerbos::MongoDB::Mongoid.criteria(AdversarialMongoid::Resource, result)
        expect(mongoid_ids(criteria)).to eq(AdversarialOracle.allowed_ids(action))
      end
    end

    # No policy in the corpus compares these fields with a constant of another type WITHOUT a
    # value_type declaration, so the driver's answer is the reference: the harness above proves
    # it, and MongoDB's type-strict comparisons are CEL's heterogeneous equality. The helper
    # agrees with it on every probe. A bare where returns other documents, or cannot encode the
    # query, on at least one — the over-grant the helper exists to prevent — and
    # spec/mongoid_spec.rb shows its selector differs on every one.
    it "keeps Mongoid's type conversions out of the cast probes" do
      disagreeing = AdversarialMongoid::CAST_PROBES.select do |name, condition|
        filter = Cerbos::MongoDB.query_plan_to_filter(
          plan: {"kind" => "KIND_CONDITIONAL", "condition" => condition}, mapper: AdversarialMongoid::UNTYPED_MAPPER
        ).filter
        driver = AdversarialStore.ids(filter)
        expect(mongoid_ids(AdversarialMongoid::Resource.where(Cerbos::MongoDB::Mongoid.raw(filter)))).to eq(driver), name

        bare = begin
          mongoid_ids(AdversarialMongoid::Resource.where(filter))
        rescue => e
          e.class
        end
        bare != driver
      end
      expect(disagreeing.keys).to include("aBool == 1", "aNumber < \"3\"", "aDouble > -1e19")
    end
  end

  describe "fails loudly" do
    # The plan is fetched OUTSIDE the assertion, so a PDP failure fails the test rather than
    # passing it, and no query runs: the refusal must come from the TRANSLATION, so a server
    # aborting a wrongly emitted filter cannot masquerade as the adapter refusing to emit it.
    # The MESSAGE is asserted, not only the error (cerbos/query-plan-adapters#326).
    ConformanceCorpus::THROWING_ACTIONS.each do |(action, message)|
      it "#{action} fails during translation with the message the corpus pins" do
        plan = AdversarialOracle.plan(action)
        expect(plan.kind).to eq(:KIND_CONDITIONAL)

        expect { translate(plan) }.to raise_error(Cerbos::MongoDB::Error, /#{Regexp.escape(message)}/)
      end
    end
  end

  # `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the corpus
  # default: a NULL field sends NO attribute, so check() denies every document (#302).
  #
  # This adapter expresses that PER ATTRIBUTE: `nullable: true` on a mapper entry declares "a
  # stored null is a missing Cerbos attribute", and the resulting guards make `eq(field, null)`
  # contradictory — the empty set the oracle demands. `owner` maps to the SAME field without
  # `nullable`, so `null-eq` still returns its explicit-null documents. Both are asserted: the
  # pair is what proves the mapper flag, not the corpus, is doing the discriminating.
  describe "null attribute representation" do
    ConformanceCorpus::NULL_OMITTED_THROWS.each do |(action, message)|
      it "#{action} already aligns via the nullable mapper flag, and is refused under :omitted" do
        expect(AdversarialOracle.allowed_ids(action)).to be_empty
        expect(adapter_filtered_ids(action)).to be_empty

        # The same field WITHOUT `nullable` keeps the explicit-null translation, so the empty
        # result above is the flag talking, not a filter that matches nothing everywhere.
        null_eq = AdversarialOracle.allowed_ids("null-eq")
        expect(null_eq).not_to be_empty
        expect(adapter_filtered_ids("null-eq")).to eq(null_eq)

        # The global option is the fail-closed backstop for callers who omit attributes without
        # declaring `nullable` on every affected mapper entry.
        expect { adapter_filtered_ids(action, null_attribute_representation: :omitted) }
          .to raise_error(Cerbos::MongoDB::Error, /#{Regexp.escape(message)}/)
      end
    end

    # The completeness guard. The refusal must key off the null OPERAND, not off a list of
    # operators: `hasIntersection(tagNames, ["public", null])` carries one in its value list, and
    # an allowlist of eq/ne/in would miss it. Enumerating the corpus rather than naming shapes
    # means a newly added action carrying a null constant is covered automatically.
    it "refuses every null-carrying action under :omitted; an indexed null element stays a value" do
      message = ConformanceCorpus::NULL_OMITTED_THROWS.first.last
      null_carrying = ConformanceCorpus::MANIFEST_ACTIONS.sort.select { |action|
        plan = AdversarialOracle.plan(action)
        plan.conditional? && plan_carries_null?(plan.condition)
      }

      # Guard the guard: if the walk stopped finding null operands the loop below is vacuous.
      expect(null_carrying).to include("null-eq-missing", "in-null-elem-hasint")

      not_refused = null_carrying.reject do |action|
        # The representation option describes absent fields, not null list elements. An indexed
        # element keeps its explicit null value, and the bounds guard excludes missing positions.
        if action == "index-scalar-list-null"
          expect(adapter_filtered_ids(action, null_attribute_representation: :omitted))
            .to eq(AdversarialOracle.allowed_ids(action))
          next true
        end

        adapter_filtered_ids(action, null_attribute_representation: :omitted)
        false
      rescue Cerbos::MongoDB::Error => e
        # The refusal must be the null-operand check talking, not an incidental failure.
        e.message.include?(message)
      end
      expect(not_refused).to be_empty
    end
  end

  describe "known divergences" do
    # The planner folds has() on a missing attribute to ALWAYS_ALLOWED while check() denies the
    # documents that lack it. Excluded from the oracle run, and pinned here so the exclusion is
    # visible and fails the day the pinned PDP changes.
    it "p-has is an upstream planner over-grant, not an adapter bug" do
      plan = AdversarialOracle.plan("p-has")
      oracle = AdversarialOracle.allowed_ids("p-has")

      expect(ConformanceCorpus::SKIPPED).to include("p-has")
      expect(plan.kind).to eq(:KIND_ALWAYS_ALLOWED)
      expect(oracle).not_to be_empty
      expect(oracle.size).to be < ALL_IDS.size
      expect(adapter_filtered_ids("p-has")).to eq(ALL_IDS)
    end
  end

  # The corpus pins two count spellings over the chain — `size(...) == 0` and
  # `!(size(...) > 0)` — but the guard has to be a property of the chain rather than of the two
  # spellings that happen to be pinned. These synthesise the remaining threshold/polarity
  # combinations onto the same seeded collection (cerbos/query-plan-adapters#316).
  it "every count threshold over the chain inherits the absent-parent guard" do
    chain = {"variable" => "request.resource.attr.mainCategory.subCategories"}
    size = {"expression" => {"operator" => "size", "operands" => [chain]}}
    compare = ->(operator, threshold) { {"expression" => {"operator" => operator, "operands" => [size, {"value" => threshold}]}} }
    negate = ->(condition) { {"expression" => {"operator" => "not", "operands" => [condition]}} }
    filtered = lambda { |condition|
      result = translate({"kind" => "KIND_CONDITIONAL", "condition" => condition})
      expect(result).to be_conditional
      AdversarialStore.ids(result.filter)
    }

    # Every seed that HAS a mainCategory holds exactly one subCategory, and the rest are CEL
    # missing-path errors — so each of these is empty unless the guard leaks.
    {
      "size(chain) == 0" => compare.call("eq", 0),
      "size(chain) <= 0" => compare.call("le", 0),
      "size(chain) >= 2" => compare.call("ge", 2),
      "!(size(chain) > 0)" => negate.call(compare.call("gt", 0)),
      "!(size(chain) >= 1)" => negate.call(compare.call("ge", 1)),
      "!(size(chain) < 2)" => negate.call(compare.call("lt", 2))
    }.each do |shape, condition|
      expect([shape, filtered.call(condition)]).to eq([shape, []])
    end

    # The mirror image, so the loop above cannot pass by denying everything.
    with_parent = AdversarialOracle.allowed_ids("w1-size-nonneg-chain")
    expect(with_parent).not_to be_empty
    expect(with_parent.size).to be < ALL_IDS.size
    expect(filtered.call(compare.call("ge", 0))).to eq(with_parent)
    expect(filtered.call(compare.call("lt", 2))).to eq(with_parent)
  end

  # The README's mapping-hazard contract rests on ONE structural fact: this adapter builds no
  # subquery. A relation is a path inside the same document, so the filter and the application
  # read the same document and the subquery hazards cannot arise. The day it reaches a second
  # collection they all arrive at once, silently (cerbos/query-plan-adapters#323).
  it "emits no $lookup and reaches no second collection" do
    forbidden = /\$lookup|\$graphLookup|\$unionWith|\.aggregate\b/

    # The claim is about the adapter, not about the corpus's mapper, so every published file is
    # read — minus comments, where prose about the guard is not a violation of it.
    sources = Dir[File.expand_path("../lib/**/*.rb", __dir__)].sort
    expect(sources.map { |path| File.basename(path) }).to include("mongodb.rb", "translator.rb")
    offending = sources.flat_map { |path|
      File.readlines(path, encoding: "UTF-8").each_with_index.filter_map { |line, index|
        code = line.sub(/#.*$/, "")
        "#{File.basename(path)}:#{index + 1}" if code.match?(forbidden)
      }
    }
    expect(offending).to be_empty

    # And the emitted filters, so an operator assembled from fragments cannot slip past the scan.
    ConformanceCorpus::ORACLE_ACTIONS.each do |action|
      expect([action, JSON.generate(translate(AdversarialOracle.plan(action)).filter)])
        .to match([action, satisfy { |json| !json.match?(forbidden) }])
    end
  end
end
