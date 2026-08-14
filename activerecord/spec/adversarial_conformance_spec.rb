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
  # One action for each group of hostile shapes in policies/adversarial.yaml that this adapter
  # COMPARES with the oracle. Each one is asserted to be in the oracle set, so moving an action
  # into adapterUnsupported fails this list instead of emptying it without a word.
  #
  # The list belongs to this adapter. Do not copy it from another harness: this adapter compares
  # 178 of the 187 conformance actions, and a list built for an adapter that compares fewer would
  # leave most of the groups here with no guard at all (cerbos/query-plan-adapters#324).
  #
  # Each entry has an oracle that is not empty and not every seed. Some actions cannot join
  # either list, because their oracle is degenerate BY CONSTRUCTION and no adapter can change
  # that: in-empty, p-double-frac, arith-add-eq-frac, nan-ord-le, size-huge-gt,
  # w1-size-zero-chain and w1-not-size-chain each allow nothing, and size-huge-lt allows every
  # seed. Their groups are guarded by a sibling here.
  #
  # Two more are degenerate for a reason that makes a bare "it threw" worthless, so each carries
  # its own anti-vacuity test below instead: filter-as-conjunct, and the null-eq-missing probe
  # under the `omitted` representation. Both pin WHY the refusal is required, not merely that
  # one happens.
  DEGENERACY_GUARD_ACTIONS = (%w[
    vf-le
    in-single
    like-percent
    cs-eq
    unicode-eq
    double-threshold
    all-on-empty
    outer-attr-depth2
    triple-negation
    optional-ne
    field-to-field
    size-threshold
    ternary-nested
    f2f-contains
    arith-add
    p-deep-nest
    n-not-all-null
    cr-contains
    cr-div-zero-ne
    cr-div-neg-zero
    cr-size-frac-ge
    nan-ord-ternary
    hier-ancestor-ff
    hier-meta-like
    ts-eq
    null-eq
    null-ne
    in-null-elem-mixed
    in-var-var
    macro-depth3-all
    pv-exists
    pv-all
    w2-outer-relation
    w1-all-chain
    w1-not-exists-chain
    w1-size-nonneg-chain
    w1-not-in-chain
    w1-not-hasint-chain
    null-value-ne-const
    null-value-not-eq-const
    null-value-not-in-const
    null-value-f2f
    null-value-pv-not-exists
    cs-contains
    rel-eq-hop
    rel-bool-hop2
    rel-ne-null-hop
    rel-hop-and-root
    id-eq-const
    id-f2f
    id-concat
    concat-f2f
    cast-string-double
    hier-list-id
  ] +
    # Root position and bare operand forms (#388): one for each hazard — the negation over a
    # bare ordering, where every other negated ordering in the corpus wraps a size() or a
    # ternary; the bare boolean at the ROOT of the condition; and the collection subquery
    # DISJOINED with a scalar predicate rather than conjoined with one, where an adapter that
    # builds its EXISTS as a join loses the rows the other arm allows.
    %w[not-lt root-bare-bool or-eq-exists] +
    # Hazard classes the corpus missed (#387): the De Morgan branch over a conjunction; the
    # negated LIKE against a COLUMN needle, where a definite-FALSE null guard would leak every
    # NULL-needle row through the NOT; the value-first hasIntersection, whose operands reach
    # the wire the other way round; and the BELOW-cliff unroll of a principal collection, the
    # shape a principal holding three teams produces.
    %w[not-and not-contains vf-hasint pv-exists-unrolled] +
    # CEL `%`, which is integer-only and so arrives under an int() cast. This adapter lowers
    # both, which is why the entry is here rather than among the probes below: ent, pgx and
    # spring-data all refuse the shape at the cast.
    %w[arith-mod]).freeze

  # Shapes that this adapter REFUSES, kept because their group has no compared member here and
  # a non-degenerate oracle still proves that the PDP and the policy are live. Each one is
  # asserted NOT to be in the oracle set, so a shape that the adapter later learns to translate
  # must move up into the list above rather than stay a weaker probe.
  #
  # Two come from the arithmetic edge probes. The guard proper reaches that group through
  # cr-div-zero-ne and cr-div-neg-zero, but each of those divides by a column BY ITSELF or by a
  # constant. Two sub-shapes are left with nothing compared: a denominator that is a DIFFERENT
  # column, and arithmetic composed ON a division. cr-div-then-add-ne is the second sub-shape
  # again, so one action speaks for it.
  #
  # The other two are positional access into a scalar list and a map() projection compared to a
  # literal list. Each is the only member of its group this adapter refuses, so each stays a
  # probe until the adapter learns to translate it.
  LIVENESS_ONLY_PROBES = %w[
    cr-div-other-column cr-div-then-add index-scalar-list map-eq-list
  ].freeze

  describe "corpus" do
    # This test is a control and not a formality. A new action in the corpus must not go past
    # this adapter without a test. Increase these numbers only when you know why
    # conformance/actions.json is larger.
    it "pins the corpus size" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(187)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(10)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
      expect(ConformanceCorpus::MANIFEST_ACTIONS.size).to eq(199)
      # Every one of these carries a pinned message, so a throwing action that appears or
      # disappears must be triaged here and cannot join the suite quietly.
      expect(ConformanceCorpus::THROWING_ACTIONS.size).to eq(18)
      # The guard has one entry for each group of hostile shapes. A new group arrives with a
      # new action, which the count above already stops. This number makes the second half of
      # that decision explicit: name a representative for the new group here.
      expect(DEGENERACY_GUARD_ACTIONS.size).to eq(62)
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

    # This test protects the other tests. If the oracle gave the same result for all the rows,
    # each comparison below would agree but would prove nothing. A PDP that denies all the rows
    # is one cause. A policy that does not load is another cause. For these actions, the set of
    # permitted rows must not be empty, and it must not contain all the rows.
    it "produces a non-degenerate oracle" do
      DEGENERACY_GUARD_ACTIONS.each do |action|
        expect(ConformanceCorpus::ORACLE_ACTIONS).to include(action),
          "#{action}: in the degeneracy guard but this adapter does not compare it"
        expect_non_degenerate_oracle(action)
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

    # #387. `filter-as-conjunct` puts a filter() ONE LEVEL BELOW the root, where the guard that
    # refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — CEL
    # gives a list where the conjunction needs a boolean, so the PDP denies every seed — which
    # is why it belongs to neither guard list above: both assert a non-empty, non-total oracle.
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
          "#{action}: this adapter now translates it, so move it into the guard proper"
        expect_non_degenerate_oracle(action)
      end
    end
  end

  describe "matches the check() oracle" do
    ConformanceCorpus::ORACLE_ACTIONS.each do |action|
      it action do
        expect(adapter_filtered_ids(action)).to eq(AdversarialOracle.allowed_ids(action))
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
