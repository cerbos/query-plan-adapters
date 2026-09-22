# frozen_string_literal: true

# The adversarial differential conformance harness (cerbos/query-plan-adapters#263).
#
# This harness does four steps for each action in the shared conformance/ corpus. First, it
# makes a plan with a real Cerbos PDP. The version of that PDP is in
# conformance/CERBOS_VERSION, and it loads the policies in conformance/policies/. Second, it
# translates the plan with the public interface of this adapter. Third, it runs the query
# against the SQLite rows from the corpus. Fourth, it compares the set of ids with an oracle.
#
# To make the oracle, the harness calls the check interface of the same PDP for each row. The
# attributes in that call are the same as the data in the row.
#
# No person calculates the expected results. If the filter of this adapter does not agree with
# the evaluation of Cerbos for one row, the test shows the difference. Refer to
# conformance/README.md for the oracle procedure, the NULL conventions and the degeneracy
# guard.
#
# This file contains only the configuration for ActiveRecord: the schema is in
# spec/support/adversarial_models.rb, and the attribute map is below.

RSpec.describe "adversarial conformance" do
  before(:all) { AdversarialModels.establish! }

  # The attribute map lives in spec/support/corpus_attributes.rb, so the translator unit test
  # replays the wire fixtures through the SAME mapping this harness is classified against.
  ATTRIBUTES = CorpusAttributes::ATTRIBUTES
  UNDECLARED_ATTRIBUTES = CorpusAttributes::UNDECLARED

  def adapter_filtered_ids(action)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: AdversarialOracle.plan(action),
      model: AdvResource,
      attributes: ATTRIBUTES
    ).pluck(:id).sort
  end

  # Walks the plan of the SDK and looks for a null constant, in a value or inside a list of
  # values. This mirrors the walk of the adapter, so the test below reads the same operands that
  # the adapter reads.
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
  # If the oracle gave the same result for every row, the differential comparison would agree
  # and prove nothing: a PDP that denies everything, or a policy that did not load, looks
  # exactly like a passing adapter. So every oracle-compared action is swept: before its ids are
  # compared, its oracle is asserted non-empty and short of every seed.
  #
  # The only exemptions are the corpus-level allowlist `degenerateOracles` in
  # conformance/actions.json — actions whose oracle is empty or total BY CONSTRUCTION, for every
  # adapter — and there the sweep asserts the declared shape exactly instead. No exemption lives
  # in this file: an action that newly degenerates is a corpus question, not a local one.
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

  # Shapes that this adapter REFUSES, kept because their group has no compared member here and
  # a non-degenerate oracle still proves that the PDP and the policy are live. Each one is
  # asserted NOT to be in the oracle set, so a shape that the adapter later learns to translate
  # leaves this list for the sweep rather than stays a weaker probe.
  #
  # Two come from the arithmetic edge probes. The sweep reaches that group through
  # cr-div-zero-ne and cr-div-neg-zero, but each of those divides by a column BY ITSELF or by a
  # constant. Two sub-shapes are left with nothing compared: a denominator that is a DIFFERENT
  # column, and arithmetic composed ON a division. cr-div-then-add-ne is the second sub-shape
  # again, so one action speaks for it.
  #
  # Positional scalar-list access probes equality, negation and explicit-null elements, over
  # the string list `tagNames` and over the number and boolean lists, where two cross-type
  # probes compare a boolean element with 1 and a number element with true. All of them are
  # refused at `index`, and each is listed rather than one sibling speaking for the rest: a
  # positional lowering is exactly the change that would start translating some and not
  # others. A map() projection compared to a literal list is also refused. Each stays a probe
  # until the adapter learns to translate it.
  #
  # An empty hierarchy delimiter is refused before the prefix LIKE is built, and a regex with a
  # top-level alternation is a matches(), which this adapter never translates.
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
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(288)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(11)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
      expect(ConformanceCorpus::MANIFEST_ACTIONS.size).to eq(301)
      # Refusals must retain their pinned messages.
      expect(ConformanceCorpus::THROWING_ACTIONS.size).to eq(72)
      # An action joins the corpus allowlist of degenerate oracles only deliberately.
      expect(ConformanceCorpus::DEGENERATE_ORACLES.size).to eq(41)
    end

    # Adding a throwing action without a pinned message must fail the run and must not turn the
    # throw suite quietly back into a bare "it threw" (cerbos/query-plan-adapters#326).
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

    # The corpus allowlist of degenerate oracles, pinned in full whether or not this adapter
    # compares the action. The sweep in "matches the check() oracle" reaches only the compared
    # ones; this reaches the rest, so an entry that stopped being degenerate — or never was —
    # fails here instead of exempting nothing in silence.
    it "gives every degenerateOracles entry exactly the oracle it declares" do
      ConformanceCorpus::DEGENERATE_ORACLES.each_key do |action|
        expect(ConformanceCorpus::MANIFEST_ACTIONS).to include(action),
          "#{action}: in degenerateOracles but not classified by the corpus"
        expect_oracle_shape(action, AdversarialOracle.allowed_ids(action))
      end
    end

    # The seeder for the to-one chain, pinned directly (ADR 0005).
    #
    # The two hops are read back THROUGH the joins and compared with the corpus, and the rows
    # are not counted. A count cannot tell an inner row that carries the values of the corpus
    # from one that carries the columns of the root row, and that is the failure of a flat
    # column alias which this relation exists to make visible.
    it "seeds the to-one chain that the corpus describes" do
      with_parent = ConformanceCorpus::SEEDS.select { |s| ConformanceCorpus.parent_seed_of(s) }
      with_inner = ConformanceCorpus::SEEDS.select { |s|
        ConformanceCorpus.parent_seed_of(ConformanceCorpus.parent_seed_of(s))
      }

      # All three depths must be present, or the chain proves less than it appears to.
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

    # The seeder for the two scalar lists, read back in position order and compared with the
    # corpus. Nothing this adapter translates reads them yet — every action on them is refused
    # at `index` — so this is the only test that sees the stored rows, and a dropped null
    # element or a lost position would otherwise wait for the first action that compares them.
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

    # #387. `filter-as-conjunct` puts a filter() ONE LEVEL BELOW the root, where the guard that
    # refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — CEL
    # gives a list where the conjunction needs a boolean, so the PDP denies every seed — which
    # is why degenerateOracles lists it, and why the sweep cannot speak for it.
    #
    # A bare "it raises" would then say nothing about whether refusing it is REQUIRED. This is
    # that argument. The other conjunct is `R.attr.aBool`, which the adapter certainly can
    # express and which `root-bare-bool` spells on its own; an adapter that quietly dropped the
    # conjunct it could not translate would emit exactly that filter and return every row it
    # selects — all of which the PDP denies for this action.
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

    # The same anti-vacuity assertion for the groups where this adapter compares nothing.
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
    # An error is necessary, during the translation or during the query. A filter that is
    # incorrect but makes no error is the only result that we cannot accept, because it gives
    # rows that the PDP denies.
    #
    # The MESSAGE is asserted and not only the error. A bare "it threw" is satisfied by a typo
    # in the attribute map, by an unrelated validation or by a transport error, and the
    # classification would then rest on a failure that never reached the mechanism its reason
    # names (cerbos/query-plan-adapters#326).
    #
    # Two things stay OUTSIDE the assertion for the same reason. The plan comes from the PDP
    # first, so a PDP that is down fails the test and does not pass it. And no query runs: the
    # refusal must come from the TRANSLATION, so that a database which rejects a filter the
    # adapter should never have made cannot look like the adapter refusing to make it.
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

  # The two conventions for a NULL column look the same on the wire. The planner sends the same
  # `eq(attr, null)` node for `null-eq`, where the oracle sends an explicit null, and for
  # `null-eq-missing`, where the oracle omits the attribute. But their oracles do not agree.
  # Thus the caller must tell the adapter which convention it uses.
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

      # The reason the rejection is necessary. A SQL NULL is a stored value, so the default
      # translation gives exactly the rows that the PDP denies. This test holds that difference
      # so the test above cannot pass because of an unrelated error.
      it "#{action} would give the rows the PDP denies under the default representation" do
        expect(AdversarialOracle.allowed_ids(action)).to be_empty

        # The over-grant as a PROPERTY, not a written-down id set. Which rows come back is the
        # oracle's answer, and conformance/README.md forbids a harness stating one: a literal
        # list here would have to be rewritten whenever the seeds move, and it would be the one
        # place in this file a person, rather than the PDP, decided what is correct.
        over_granted = adapter_filtered_ids(action)
        expect(over_granted).not_to be_empty
        expect(over_granted.size).to be < ConformanceCorpus::SEEDS.size
      end
    end

    # The declaration of an attribute wins over the convention of the call. This is the whole
    # point of #308: one policy suite can correctly mix the two, so the option of the call is
    # a fallback and not a switch over the whole plan.
    it "lets the declaration of an attribute override the convention of the call" do
      relation = Cerbos::ActiveRecord.query_plan_to_relation(
        plan: AdversarialOracle.plan("null-eq"),
        model: AdvResource,
        attributes: ATTRIBUTES,
        null_attribute_representation: :omitted
      )
      expect(relation.pluck(:id).sort).to eq(AdversarialOracle.allowed_ids("null-eq"))

      # And without the declaration the same call is refused, so the test above passes
      # because of the declaration and not because the check stopped working.
      expect {
        Cerbos::ActiveRecord.query_plan_to_relation(
          plan: AdversarialOracle.plan("null-eq"),
          model: AdvResource,
          attributes: UNDECLARED_ATTRIBUTES,
          null_attribute_representation: :omitted
        )
      }.to raise_error(Cerbos::ActiveRecord::Error, /null constant/)
    end

    # The completeness guard for #302. The refusal must come from the null OPERAND and not from
    # a list of operators: `hasIntersection(tagNames, ["public", null])` carries a null in its
    # list of values, and an allowlist of eq/ne/in would miss it without a word. This test reads
    # every action in the corpus instead of naming shapes, so a new action that carries a null
    # comes here by itself.
    #
    # It translates with the UNDECLARED map. The convention of the call only reaches an
    # attribute that declares nothing, so a declared attribute would leave this loop with
    # nothing to prove about the fallback (cerbos/query-plan-adapters#308).
    it "refuses every action in the corpus that carries a null constant" do
      message = ConformanceCorpus::NULL_OMITTED_THROWS.first.last

      null_carrying = ConformanceCorpus::MANIFEST_ACTIONS.sort.select { |action|
        plan = AdversarialOracle.plan(action)
        plan.conditional? && plan_carries_null?(plan.condition)
      }

      # Guard the guard. If the walk stopped finding null operands, the loop below is vacuous.
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
        # The refusal must be the null-operand check speaking. A typo in the attribute map or
        # an unrelated validation counting as the required refusal is the quiet pass that
        # conformance/README.md warns about.
        e.message.include?(message)
      end

      expect(not_refused).to be_empty
    end
  end

  describe "known divergences" do
    # This test holds the current behaviour of has() in the planner until the correction comes
    # from the Cerbos project. The check interface denies the rows in which aOptionalString is
    # missing. But the planner changes the same condition into ALWAYS_ALLOWED. The adapter must
    # translate that plan correctly. This test keeps the one permitted difference visible. It
    # fails if the pinned image changes. Then p-has can go back into the differential run.
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
