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

  def self.field(path) = Cerbos::ActiveRecord.field(path)

  def self.relation(*args, **kwargs) = Cerbos::ActiveRecord.relation(*args, **kwargs)

  ATTRIBUTES = {
    "request.resource.attr.aBool" => field("a_bool"),
    "request.resource.attr.aString" => field("a_string"),
    "request.resource.attr.aNumber" => field("a_number"),
    "request.resource.attr.aDouble" => field("a_double"),
    "request.resource.attr.aOptionalString" => field("a_optional_string"),
    "request.resource.attr.createdBy" => field("created_by"),
    "request.resource.attr.scope" => field("scope"),
    "request.resource.attr.createdAt" => field("created_at"),
    # `owner` uses the same column as aOptionalString. But the corpus sends `owner` as an
    # explicit null, and it does not remove the attribute. The membership tests find this
    # difference.
    "request.resource.attr.owner" => field("a_optional_string"),
    # obj.inner is not a true nested column. It uses the same column as aString. The
    # spring-data, prisma and sqlalchemy harnesses use the same substitute for the p-struct
    # test.
    "request.resource.attr.obj.inner" => field("a_string"),

    "request.resource.attr.tags" => relation(
      :tags, fields: {"id" => field("tag_id"), "name" => field("name")}
    ),
    # The scalar values of tags[].name. A NULL name stays in the list as a null element.
    "request.resource.attr.tagNames" => relation(:tags, member_field: "name"),

    "request.resource.attr.categories" => relation(:categories, fields: {
      "subCategories" => relation(:sub_categories, fields: {
        "name" => field("name"),
        "labels" => relation(:labels, fields: {"name" => field("name")})
      })
    }),

    # The same two hops, but from the root and written as a chain: `mainCategory` is one
    # relation, and `subCategories`/`subNames` are nested relations in its fields. Each seed
    # holds at most one category, so the check side sees `mainCategory` as ONE object, and 16
    # of the 20 seeds have no category and thus no attribute at all.
    #
    # The nesting is deliberate. A flat `has_many :through` under the full name with dots gives
    # the same joins, but it does not say which hop is the parent. Then an absent parent and a
    # parent with no children look the same, and `all`, `!exists` and every count over the
    # chain give back the 16 rows that the PDP denies (w1-*-chain, #309/#315/#316).
    "request.resource.attr.mainCategory" => relation(:categories, fields: {
      "subCategories" => relation(:sub_categories, fields: {"name" => field("name")}),
      "subNames" => relation(:sub_categories, member_field: "name")
    })
  }.freeze

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
  # A sample of the actions that this adapter COMPARES with the oracle, one for each group of
  # hostile shapes that it can express. Each one is asserted to be in the oracle set, so moving
  # an action into adapterUnsupported fails this list instead of emptying it without a word.
  #
  # The list belongs to this adapter and is not a copy of another harness. An entry naming a
  # shape that this adapter refuses would guard nothing at all
  # (cerbos/query-plan-adapters#324).
  DEGENERACY_GUARD_ACTIONS = %w[
    vf-le
    like-percent
    all-on-empty
    pv-exists
    pv-all
    null-eq
    null-ne
    w1-all-chain
    w1-not-exists-chain
    w1-size-nonneg-chain
    w1-not-in-chain
    w1-not-hasint-chain
    cr-div-neg-zero
  ].freeze

  # Shapes that this adapter REFUSES, kept because their group has no compared member here and
  # a non-degenerate oracle still proves that the PDP and the policy are live. Each one is
  # asserted NOT to be in the oracle set, so a shape that the adapter later learns to translate
  # must move up into the list above rather than stay a weaker probe.
  #
  # Column division against a second column is the whole group for this adapter: cr-div-neg-zero
  # above is a CONSTANT denominator, so nothing in the guard proper reaches a row-dependent one.
  LIVENESS_ONLY_PROBES = %w[cr-div-other-column].freeze

  describe "corpus" do
    # This test is a control and not a formality. A new action in the corpus must not go past
    # this adapter without a test. Increase these numbers only when you know why
    # conformance/actions.json is larger.
    it "pins the corpus size" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(133)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(8)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
      expect(ConformanceCorpus::MANIFEST_ACTIONS.size).to eq(143)
      # Every one of these carries a pinned message, so a throwing action that appears or
      # disappears must be triaged here and cannot join the suite quietly.
      expect(ConformanceCorpus::THROWING_ACTIONS.size).to eq(13)
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
        expect(adapter_filtered_ids(action)).to eq(%w[a2 a4 a8 c2 e1])
      end
    end

    # The completeness guard for #302. The refusal must come from the null OPERAND and not from
    # a list of operators: `hasIntersection(tagNames, ["public", null])` carries a null in its
    # list of values, and an allowlist of eq/ne/in would miss it without a word. This test reads
    # every action in the corpus instead of naming shapes, so a new action that carries a null
    # comes here by itself.
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
          attributes: ATTRIBUTES,
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
