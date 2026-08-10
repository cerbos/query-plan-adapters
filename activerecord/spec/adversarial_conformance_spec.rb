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

  def self.field(path, **kwargs) = Cerbos::ActiveRecord.field(path, **kwargs)

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
    # `owner` and `coOwner` alias the columns that `aOptionalString` and `scope` also map,
    # under the OTHER null convention: the oracle sends a real null attribute for them and
    # does not remove it. The declaration here is what makes the equality family definite for
    # these two attributes and leaves every other mapping alone
    # (cerbos/query-plan-adapters#308).
    "request.resource.attr.owner" => field("a_optional_string", null_representation: :explicit),
    # The explicit-null alias of the `scope` column, and the second half of `null-value-f2f`.
    # `scope` itself is omitted when NULL, so the corpus holds the same column under both
    # conventions and the field-to-field test has two explicit nulls to compare. It is NOT a
    # second alias of `a_optional_string`: a column compared with itself is TRUE for all 20
    # seeds, and the degeneracy guard refuses a total oracle.
    "request.resource.attr.coOwner" => field("scope", null_representation: :explicit),
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

  # The same map with every per-attribute declaration removed, so a test can make the
  # convention of the call reach every attribute. It is DERIVED from the map above and is not
  # written out a second time: a new attribute cannot arrive in one and not the other.
  UNDECLARED_ATTRIBUTES = ATTRIBUTES.transform_values { |mapping|
    mapping.is_a?(Cerbos::ActiveRecord::AttributeMapping::Field) ? field(mapping.path) : mapping
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
  # One action for each group of hostile shapes in policies/adversarial.yaml that this adapter
  # COMPARES with the oracle. Each one is asserted to be in the oracle set, so moving an action
  # into adapterUnsupported fails this list instead of emptying it without a word.
  #
  # The list belongs to this adapter. Do not copy it from another harness: this adapter compares
  # 136 of the 141 actions, and a list built for an adapter that compares fewer would leave most
  # of the groups here with no guard at all (cerbos/query-plan-adapters#324).
  #
  # Each entry has an oracle that is not empty and not all 20 seeds. Eight actions cannot join
  # the list, because their oracle is degenerate BY CONSTRUCTION and no adapter can change that:
  # in-empty, p-double-frac, arith-add-eq-frac, nan-ord-le, size-huge-gt, w1-size-zero-chain and
  # w1-not-size-chain each allow nothing, and size-huge-lt allows every seed. Their groups are
  # guarded by a sibling below.
  DEGENERACY_GUARD_ACTIONS = %w[
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
  ].freeze

  # Shapes that this adapter REFUSES, kept because their group has no compared member here and
  # a non-degenerate oracle still proves that the PDP and the policy are live. Each one is
  # asserted NOT to be in the oracle set, so a shape that the adapter later learns to translate
  # must move up into the list above rather than stay a weaker probe.
  #
  # Both come from the arithmetic edge probes. The guard proper reaches that group through
  # cr-div-zero-ne and cr-div-neg-zero, but each of those divides by a column BY ITSELF or by a
  # constant. Two sub-shapes are left with nothing compared: a denominator that is a DIFFERENT
  # column, and arithmetic composed ON a division. cr-div-then-add-ne is the second sub-shape
  # again, so one action speaks for it.
  LIVENESS_ONLY_PROBES = %w[cr-div-other-column cr-div-then-add].freeze

  describe "corpus" do
    # This test is a control and not a formality. A new action in the corpus must not go past
    # this adapter without a test. Increase these numbers only when you know why
    # conformance/actions.json is larger.
    it "pins the corpus size" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(141)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(9)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
      expect(ConformanceCorpus::MANIFEST_ACTIONS.size).to eq(152)
      # Every one of these carries a pinned message, so a throwing action that appears or
      # disappears must be triaged here and cannot join the suite quietly.
      expect(ConformanceCorpus::THROWING_ACTIONS.size).to eq(14)
      # The guard has one entry for each group of hostile shapes. A new group arrives with a
      # new action, which the count above already stops. This number makes the second half of
      # that decision explicit: name a representative for the new group here.
      expect(DEGENERACY_GUARD_ACTIONS.size).to eq(43)
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
