# frozen_string_literal: true

# Adversarial differential conformance harness (cerbos/query-plan-adapters#263).
#
# Every action in the shared repo-level conformance/ corpus is planned against a REAL Cerbos
# PDP pinned to conformance/CERBOS_VERSION and loaded with conformance/policies/, translated
# through this adapter's public API, and executed against seeded SQLite rows — then the
# filtered id set is compared with an oracle computed by calling the check API for each seed
# row with attributes mirroring that row exactly.
#
# No hand-computed expectations: if this adapter's filter semantics diverge from Cerbos's own
# evaluation for any row, the mismatch surfaces mechanically. See conformance/README.md for
# the oracle recipe, the NULL conventions and the degeneracy guard.
#
# This file owns only the ActiveRecord-specific configuration: the schema (spec/support/
# adversarial_models.rb) and the attribute mapping below.

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
    # `owner` aliases the same column as aOptionalString, but the corpus sends it as an
    # EXPLICIT null rather than omitting it — that is what the membership probes discriminate.
    "request.resource.attr.owner" => field("a_optional_string"),
    # obj.inner is not a real nested column; it mirrors aString, the same stand-in the
    # spring-data, prisma and sqlalchemy reference harnesses use for the p-struct probe.
    "request.resource.attr.obj.inner" => field("a_string"),

    "request.resource.attr.tags" => relation(
      :tags, fields: {"id" => field("tag_id"), "name" => field("name")}
    ),
    # The scalar projection of tags[].name, with NULL names retained as null elements.
    "request.resource.attr.tagNames" => relation(:tags, member_field: "name"),

    "request.resource.attr.categories" => relation(:categories, fields: {
      "subCategories" => relation(:sub_categories, fields: {
        "name" => field("name"),
        "labels" => relation(:labels, fields: {"name" => field("name")})
      })
    }),

    # The same two-hop chain flattened from the root, through a has_many :through.
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
    # A tripwire, not a formality: a new corpus action must not slip past this adapter
    # unnoticed. Bump these deliberately when conformance/actions.json grows.
    it "pins the corpus size" do
      expect(ConformanceCorpus::ACTIONS_FILE.fetch("conformance").size).to eq(114)
      expect(ConformanceCorpus::EXPECTED_UNSUPPORTED.size).to eq(3)
    end

    it "classifies every action exactly once" do
      overlap = ConformanceCorpus::ORACLE_ACTIONS & ConformanceCorpus::THROWING_ACTIONS
      expect(overlap).to be_empty
    end

    # Guard the guard. Every comparison below could pass vacuously if the oracle itself were
    # trivial — a PDP that denied every row, or a policy that failed to load. These actions
    # must produce a non-empty, non-total allowed set.
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
    # A loud failure — at translation or at execution — is required. A silently-wrong filter
    # is the only unacceptable outcome, because it returns rows the PDP denies.
    ConformanceCorpus::THROWING_ACTIONS.each do |action|
      it action do
        expect { adapter_filtered_ids(action) }.to raise_error(StandardError)
      end
    end
  end

  describe "known divergences" do
    # Pin the planner's has() fold until the upstream fix lands: the check API denies rows
    # where aOptionalString is missing, while the planner folds the same condition to
    # ALWAYS_ALLOWED. The adapter must translate that plan faithfully. This keeps the one
    # intentional oracle divergence visible, and fails when the pinned image changes so
    # p-has can move back into the differential run.
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
