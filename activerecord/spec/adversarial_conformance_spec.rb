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
  def require_selected(action)
    selected = ConformanceCorpus::SELECTED_ACTION
    skip "another action was selected by ADAPTERCTL_ACTION" if !selected.empty? && selected != action
  end

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

  def expect_catalog_oracle(action, expectation)
    ids = AdversarialOracle.allowed_ids(action)
    case expectation.fetch("kind")
    when "proper-subset"
      expect(ids).not_to be_empty, "#{action}: oracle allowed nothing"
      expect(ids.size).to be < ConformanceCorpus::CHECK_RESOURCES.size,
        "#{action}: oracle allowed every resource"
    when "empty"
      expect(ids).to be_empty, "#{action}: catalog declares an empty oracle"
    when "total"
      expect(ids.size).to eq(ConformanceCorpus::CHECK_RESOURCES.size),
        "#{action}: catalog declares a total oracle"
    else
      raise "#{action}: unknown oracle expectation #{expectation.fetch("kind").inspect}"
    end
  end

  describe "corpus" do
    it "provisionally matches a selected missing or unassessed action" do
      [nil, {"status" => "unassessed"}].each do |recorded|
        outcomes = recorded.nil? ? {} : {"new-action" => recorded}

        expect(ConformanceCorpus.effective_outcomes(%w[new-action], outcomes, "new-action"))
          .to eq("new-action" => {"status" => "matched"})
      end
    end

    it "preserves an assessed selected action" do
      assessed = {
        "status" => "rejected",
        "reason" => "unsupported",
        "message" => "cannot translate"
      }

      expect(ConformanceCorpus.effective_outcomes(
        %w[known-action], {"known-action" => assessed}, "known-action"
      )).to eq("known-action" => assessed)
    end

    it "keeps exact outcome enforcement for an unscoped run" do
      expect {
        ConformanceCorpus.effective_outcomes(%w[new-action], {}, "")
      }.to raise_error(/outcomes must cover the catalog exactly/)
    end

    it "accounts for the catalog and canonical resources dynamically" do
      expect(ConformanceCorpus::OUTCOMES.keys.sort)
        .to eq(ConformanceCorpus::ALL_CATALOG_ACTIONS.sort)
      expect(ConformanceCorpus::SEEDS.size).to eq(ConformanceCorpus::CHECK_RESOURCES.size)
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
      null_omitted = ConformanceCorpus::REPRESENTATION_DEPENDENT_REJECTIONS
      upstream_blocked = ConformanceCorpus::UPSTREAM_BLOCKED

      misclassified = ConformanceCorpus::MANIFEST_ACTIONS.reject { |action|
        [oracle, throwing, null_omitted, upstream_blocked].count { |group| group.include?(action) } == 1
      }

      expect(misclassified).to be_empty
    end

    it "satisfies every catalog oracle expectation" do
      ConformanceCorpus::ORACLE_EXPECTATIONS.each do |action, expectation|
        expect_catalog_oracle(action, expectation)
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
      require_selected("filter-as-conjunct")
      skip "another action was selected" unless ConformanceCorpus.selected?("filter-as-conjunct")
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
    ConformanceCorpus::REPRESENTATION_DEPENDENT_THROWS.each do |(action, message)|
      it "#{action} is refused when the representation is omitted" do
        require_selected(action)
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
        require_selected(action)
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
      require_selected("null-eq")
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
      require_selected("null-eq-missing")
      message = ConformanceCorpus::REPRESENTATION_DEPENDENT_THROWS.first.last

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

  describe "upstream-blocked outcomes" do
    # This test holds the current behaviour of has() in the planner until the correction comes
    # from the Cerbos project. The check interface denies the rows in which aOptionalString is
    # missing. But the planner changes the same condition into ALWAYS_ALLOWED. The adapter must
    # translate that plan correctly. This test keeps the one permitted difference visible. It
    # fails if the pinned image changes. Then p-has can go back into the differential run.
    it "p-has is an upstream planner over-grant, not an adapter bug" do
      require_selected("p-has")
      skip "another action was selected" unless ConformanceCorpus.selected?("p-has")
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
