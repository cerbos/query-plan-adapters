# frozen_string_literal: true

# The adversarial conformance harness (#263).
#
# For each corpus action: plan it on a real PDP (conformance/policies/), translate the plan,
# run it on the seeded SQLite rows, and compare the ids with check() on each row.
# See conformance/README.md for details.

RSpec.describe "adversarial conformance" do
  # Memoized: the schema is built once.
  before { AdversarialModels.establish! }

  # Shared with the translator unit test (spec/support/corpus_attributes.rb).
  ATTRIBUTES = CorpusAttributes::ATTRIBUTES
  UNDECLARED_ATTRIBUTES = CorpusAttributes::UNDECLARED

  def adapter_filtered_ids(action)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: AdversarialOracle.plan(action),
      model: AdvResource,
      attributes: ATTRIBUTES
    ).pluck(:id).sort
  end

  # True if the SDK plan has a null constant, alone or in a list. Mirrors the adapter's walk.
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
    expect(ids.size).to be < ConformanceCorpus::SEEDS.size,
      "#{action}: oracle allowed every seed"
  end

  # --- the degeneracy guard (conformance/README.md, "The degeneracy guard") ------------------
  #
  # An oracle that allows all or no rows proves nothing (a dead PDP looks like a pass), so
  # every compared oracle must be neither. The only exemptions are in `degenerateOracles` in
  # conformance/actions.json, and those must match their declared shape exactly.
  def expect_oracle_shape(action, ids)
    all_ids = ConformanceCorpus::SEEDS.map { |seed| seed.fetch("id") }.sort
    declared = ConformanceCorpus::DEGENERATE_ORACLES[action]
    why = "the differential cannot fail against a degenerate oracle; see degenerateOracles in " \
          "conformance/actions.json"
    case declared
    when "empty"
      expect(ids).to eq([]), "#{action}: degenerateOracles declares an empty oracle, but it " \
        "allowed #{ids.inspect}; #{why}"
    when "total"
      expect(ids).to eq(all_ids), "#{action}: degenerateOracles declares a total oracle, but " \
        "it allowed #{ids.inspect}; #{why}"
    else
      expect(ids).not_to be_empty, "#{action}: oracle allowed nothing — #{why}"
      expect(ids.size).to be < all_ids.size, "#{action}: oracle allowed every seed — #{why}"
    end
  end

  # Refused shapes whose group has no compared action here. Their oracle is still checked, to
  # prove the PDP and policy are live. Each must not be compared; once the adapter translates
  # one, remove it and the sweep takes over.
  #
  # cr-div-other-column and cr-div-then-add cover division sub-shapes the compared cr-div-*
  # actions miss (cr-div-then-add-ne repeats the second).
  #
  # Every index-*-list shape is listed separately: indexed-list support could arrive for some
  # and not others.
  #
  # The rest are either families refused whole here (regex-*, except-*, index-*) or refused
  # variants of a compared family (cast-not-int beside the compared cast-not-string-*).
  LIVENESS_ONLY_PROBES = %w[
    regex-final-newline regex-eq-true regex-lookahead
    index-negative index-fractional index-not-oob
    cast-not-int cast-not-timestamp cast-not-double
    cr-div-other-column cr-div-then-add index-scalar-list map-eq-list
    index-scalar-list-not-eq index-scalar-list-null
    index-number-list index-number-list-not-eq index-bool-list index-bool-list-not-eq
    index-bool-list-vs-number index-number-list-vs-bool
    hier-empty-delim matches-alt
    regex-digit regex-case regex-posix
    regex-unanchored regex-dot regex-alternation
    regex-grouped regex-brace regex-repetition
    regex-optional-operators except-size except-eq
    pv-structs pv-filter pv-map pv-except
    div-by-division temporal-raw-eq eq-list
    ne-list
  ].freeze

  describe "corpus" do
    # Corpus additions must update both the classification and degeneracy tripwires.
    it "pins the corpus size" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(311)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(11)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
      expect(ConformanceCorpus::MANIFEST_ACTIONS.size).to eq(324)
      # Refusals must retain their pinned messages.
      expect(ConformanceCorpus::THROWING_ACTIONS.size).to eq(72)
      # An action joins the corpus allowlist of degenerate oracles only deliberately.
      expect(ConformanceCorpus::DEGENERATE_ORACLES.size).to eq(41)
    end

    # A throwing action without a pinned message must fail the run (#326).
    it "refuses a throwing action that pins no message" do
      expect { ConformanceCorpus.require_message("synthetic", nil) }
        .to raise_error(/pins no throw message/)
      expect { ConformanceCorpus.require_message("synthetic", "") }
        .to raise_error(/pins no throw message/)
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

    # Checks every degenerateOracles entry, including ones this adapter does not compare.
    it "gives every degenerateOracles entry exactly the oracle it declares" do
      ConformanceCorpus::DEGENERATE_ORACLES.each_key do |action|
        expect(ConformanceCorpus::MANIFEST_ACTIONS).to include(action),
          "#{action}: in degenerateOracles but not classified by the corpus"
        expect_oracle_shape(action, AdversarialOracle.allowed_ids(action))
      end
    end

    # Reads the to-one chain back through the joins and compares values, not counts: a count
    # cannot spot a row holding the root row's values instead of the parent's (ADR 0005).
    it "seeds the to-one chain that the corpus describes" do
      with_parent = ConformanceCorpus::SEEDS.select { |s| ConformanceCorpus.parent_seed_of(s) }
      with_inner = ConformanceCorpus::SEEDS.select { |s|
        ConformanceCorpus.parent_seed_of(ConformanceCorpus.parent_seed_of(s))
      }

      # All three depths must be present.
      expect(with_parent).not_to be_empty
      expect(with_inner).not_to be_empty
      expect(with_parent.size).to be < ConformanceCorpus::SEEDS.size

      stored = AdvResource
        .left_joins(parent: :inner)
        .pluck(
          :id,
          AdvParent.arel_table[:a_string],
          AdvInner.arel_table[:a_string]
        )
        .to_h { |id, parent, inner| [id, [parent, inner]] }

      expected = ConformanceCorpus::SEEDS.to_h { |seed|
        parent = ConformanceCorpus.parent_seed_of(seed)
        inner = ConformanceCorpus.parent_seed_of(parent)
        [seed.fetch("id"), [parent&.fetch("aString"), inner&.fetch("aString")]]
      }

      expect(stored).to eq(expected)
    end

    # No translated action reads these lists yet (all are refused at `index`), so this is the
    # only check on the stored rows, nulls and order included.
    it "seeds the number and boolean lists in corpus order, null elements included" do
      {
        "aNumberList" => AdvNumberListElement, "aBoolList" => AdvBoolListElement
      }.each do |key, model|
        stored = Hash.new { |hash, id| hash[id] = [] }
        model.order(:resource_id, :position).pluck(:resource_id, :value).each do |id, value|
          stored[id] << value
        end

        expected = ConformanceCorpus::SEEDS.to_h { |seed| [seed.fetch("id"), seed.fetch(key)] }
        expect(expected.values.flatten).to include(nil), "#{key}: no null element to prove"
        expect(expected.keys.to_h { |id| [id, stored[id]] }).to eq(expected), key
        expect(stored.keys - expected.keys).to be_empty, key
      end
    end

    # #387. `filter-as-conjunct` is `aBool && <filter()>`. CEL denies every row (a list is
    # not a boolean). Dropping the filter() half would leave `root-bare-bool`, which returns
    # rows, so the refusal is required.
    it "must refuse filter-as-conjunct, because the conjunct it can express over-grants" do
      expect(AdversarialOracle.allowed_ids("filter-as-conjunct")).to be_empty

      surviving_half = adapter_filtered_ids("root-bare-bool")
      expect(surviving_half).not_to be_empty
      expect(surviving_half.size).to be < ConformanceCorpus::SEEDS.size

      message = ConformanceCorpus::THROWING_ACTIONS
        .find { |(action, _)| action == "filter-as-conjunct" }
        .last
      expect { adapter_filtered_ids("filter-as-conjunct") }
        .to raise_error(Cerbos::ActiveRecord::Error, /#{Regexp.escape(message)}/)
    end

    it "produces a non-degenerate oracle for the shapes it refuses" do
      LIVENESS_ONLY_PROBES.each do |action|
        expect(ConformanceCorpus::ORACLE_ACTIONS).not_to include(action),
          "#{action}: this adapter now translates it, so the sweep guards it; drop the probe"
        expect_non_degenerate_oracle(action)
      end
    end
  end

  describe "matches the check() oracle" do
    ConformanceCorpus::ORACLE_ACTIONS.each do |action|
      it action do
        oracle = AdversarialOracle.allowed_ids(action)
        expect_oracle_shape(action, oracle)
        expect(adapter_filtered_ids(action)).to eq(oracle)
      end
    end
  end

  describe "fails loudly" do
    # These must throw with the pinned message, so an unrelated error cannot pass (#326).
    # The plan is fetched outside the `expect`, so a down PDP fails. No query runs, so a
    # database error cannot pass as a refusal.
    ConformanceCorpus::THROWING_ACTIONS.each do |(action, message)|
      it "#{action} fails during the translation with the message the corpus pins" do
        plan = AdversarialOracle.plan(action)
        expect(plan.kind).to eq(:KIND_CONDITIONAL)

        expect {
          Cerbos::ActiveRecord.query_plan_to_relation(
            plan: plan, model: AdvResource, attributes: ATTRIBUTES
          )
        }.to raise_error(Cerbos::ActiveRecord::Error, /#{Regexp.escape(message)}/)
      end
    end
  end

  # `null-eq` (explicit null) and `null-eq-missing` (omitted) give the same plan but different
  # oracles, so the caller must say which convention it uses.
  describe "null attribute representation" do
    ConformanceCorpus::NULL_OMITTED_THROWS.each do |(action, message)|
      it "#{action} is refused when the representation is omitted" do
        expect {
          Cerbos::ActiveRecord.query_plan_to_relation(
            plan: AdversarialOracle.plan(action),
            model: AdvResource,
            attributes: ATTRIBUTES,
            null_attribute_representation: :omitted
          ).pluck(:id)
        }.to raise_error(Cerbos::ActiveRecord::Error, /#{Regexp.escape(message)}/)
      end

      # Why the refusal is needed: the default translation returns rows the PDP denies.
      it "#{action} would give the rows the PDP denies under the default representation" do
        expect(AdversarialOracle.allowed_ids(action)).to be_empty

        # Checked as a property, not a hand-written id list.
        over_granted = adapter_filtered_ids(action)
        expect(over_granted).not_to be_empty
        expect(over_granted.size).to be < ConformanceCorpus::SEEDS.size
      end
    end

    # A per-attribute declaration beats the per-call option, so one policy can mix both (#308).
    it "lets the declaration of an attribute override the convention of the call" do
      relation = Cerbos::ActiveRecord.query_plan_to_relation(
        plan: AdversarialOracle.plan("null-eq"),
        model: AdvResource,
        attributes: ATTRIBUTES,
        null_attribute_representation: :omitted
      )
      expect(relation.pluck(:id).sort).to eq(AdversarialOracle.allowed_ids("null-eq"))

      # Without the declaration the same call is refused.
      expect {
        Cerbos::ActiveRecord.query_plan_to_relation(
          plan: AdversarialOracle.plan("null-eq"),
          model: AdvResource,
          attributes: UNDECLARED_ATTRIBUTES,
          null_attribute_representation: :omitted
        )
      }.to raise_error(Cerbos::ActiveRecord::Error, /null constant/)
    end

    # #302. The refusal must key on the null operand, not on a list of operators:
    # `hasIntersection(tagNames, ["public", null])` would slip past an eq/ne/in allowlist.
    # Scans every corpus action, so new ones are covered automatically.
    #
    # Uses the UNDECLARED map, since the per-call option only reaches undeclared attributes.
    it "refuses every action in the corpus that carries a null constant" do
      message = ConformanceCorpus::NULL_OMITTED_THROWS.first.last

      null_carrying = ConformanceCorpus::MANIFEST_ACTIONS.sort.select { |action|
        plan = AdversarialOracle.plan(action)
        plan.conditional? && plan_carries_null?(plan.condition)
      }

      # Make sure the walk still finds nulls.
      expect(null_carrying).to include("null-eq-missing")
      expect(null_carrying).to include("in-null-elem-hasint")

      not_refused = null_carrying.reject do |action|
        Cerbos::ActiveRecord.query_plan_to_relation(
          plan: AdversarialOracle.plan(action),
          model: AdvResource,
          attributes: UNDECLARED_ATTRIBUTES,
          null_attribute_representation: :omitted
        )
        false
      rescue Cerbos::ActiveRecord::Error => e
        # Must be the null-operand refusal, not some other error.
        e.message.include?(message)
      end

      expect(not_refused).to be_empty
    end
  end

  describe "known divergences" do
    # Planner bug: check() denies rows missing aOptionalString, but the planner folds has() to
    # ALWAYS_ALLOWED. Fails once the pinned PDP changes this; then p-has can be compared again.
    it "p-has is an upstream planner over-grant, not an adapter bug" do
      plan = AdversarialOracle.plan("p-has")
      oracle = AdversarialOracle.allowed_ids("p-has")
      all_ids = ConformanceCorpus::SEEDS.map { |seed| seed.fetch("id") }.sort

      expect(plan.kind).to eq(:KIND_ALWAYS_ALLOWED)
      expect(oracle).not_to be_empty
      expect(oracle.size).to be < all_ids.size
      expect(adapter_filtered_ids("p-has")).to eq(all_ids)
    end
  end
end
