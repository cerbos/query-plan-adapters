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

    # The same chain with two hops, but from the root, through a has_many :through.
    "request.resource.attr.mainCategory.subCategories" => relation(
      :sub_categories, fields: {"name" => field("name")}
    ),
    "request.resource.attr.mainCategory.subNames" => relation(
      :sub_categories, member_field: "name"
    )
  }.freeze

  def adapter_filtered_ids(action)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: AdversarialOracle.plan(action),
      model: AdvResource,
      attributes: ATTRIBUTES
    ).pluck(:id).sort
  end

  describe "corpus" do
    # This test is a control and not a formality. A new action in the corpus must not go past
    # this adapter without a test. Increase these numbers only when you know why
    # conformance/actions.json is larger.
    it "pins the corpus size" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(122)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(3)
      expect(ConformanceCorpus::NULL_REPRESENTATION_OMITTED.size).to eq(1)
    end

    it "classifies every action exactly once" do
      overlap = ConformanceCorpus::ORACLE_ACTIONS & ConformanceCorpus::THROWING_ACTIONS
      expect(overlap).to be_empty
    end

    # This test protects the other tests. If the oracle gave the same result for all the rows,
    # each comparison below would agree but would prove nothing. A PDP that denies all the rows
    # is one cause. A policy that does not load is another cause. For these actions, the set of
    # permitted rows must not be empty, and it must not contain all the rows.
    it "produces a non-degenerate oracle" do
      %w[vf-le like-percent all-on-empty].each do |action|
        ids = AdversarialOracle.allowed_ids(action)
        expect(ids).not_to be_empty, "#{action}: oracle allowed nothing"
        expect(ids.size).to be < ConformanceCorpus::SEEDS.size,
          "#{action}: oracle allowed every seed"
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
    ConformanceCorpus::THROWING_ACTIONS.each do |action|
      it action do
        expect { adapter_filtered_ids(action) }.to raise_error(StandardError)
      end
    end
  end

  # The two conventions for a NULL column look the same on the wire. The planner sends the same
  # `eq(attr, null)` node for `null-eq`, where the oracle sends an explicit null, and for
  # `null-eq-missing`, where the oracle omits the attribute. But their oracles do not agree.
  # Thus the caller must tell the adapter which convention it uses.
  describe "null attribute representation" do
    ConformanceCorpus::NULL_REPRESENTATION_OMITTED.each do |action|
      it "#{action} is refused when the representation is omitted" do
        expect {
          Cerbos::ActiveRecord.query_plan_to_relation(
            plan: AdversarialOracle.plan(action),
            model: AdvResource,
            attributes: ATTRIBUTES,
            null_attribute_representation: :omitted
          ).pluck(:id)
        }.to raise_error(Cerbos::ActiveRecord::Error, /omitted/)
      end

      # The reason the rejection is necessary. A SQL NULL is a stored value, so the default
      # translation gives exactly the rows that the PDP denies. This test holds that difference
      # so the test above cannot pass because of an unrelated error.
      it "#{action} would give the rows the PDP denies under the default representation" do
        expect(AdversarialOracle.allowed_ids(action)).to be_empty
        expect(adapter_filtered_ids(action)).to eq(%w[a2 a4 a8 c2 e1])
      end
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
