# frozen_string_literal: true

require "yaml"

# The shared policy suite for all the adapters in this repository
# (/policies/resource.yaml).
#
# This file reads the actions from the policy file. Thus a new action in that file cannot stay
# without a test here. For each action, the suite makes a plan with a real PDP, translates the
# plan, runs the query against the rows, and compares the result with an oracle. To make the
# oracle, it calls check() for each row. This is the same differential technique as the
# adversarial harness. No person calculates the expected results.

RSpec.describe "shared policy suite" do
  before(:all) do
    SharedModels.establish!
  end

  POLICY_PATH = File.expand_path("../../policies/resource.yaml", __dir__)

  POLICY_ACTIONS = YAML.load_file(POLICY_PATH)
    .fetch("resourcePolicy").fetch("rules")
    .flat_map { |rule| rule.fetch("actions") }
    .uniq
    .sort
    .freeze

  def self.field(path) = Cerbos::ActiveRecord.field(path)

  def self.relation(*args, **kwargs) = Cerbos::ActiveRecord.relation(*args, **kwargs)

  SHARED_ATTRIBUTES = {
    "request.resource.id" => field("id"),
    "request.resource.attr.id" => field("id"),
    "request.resource.attr.aString" => field("a_string"),
    "request.resource.attr.aNumber" => field("a_number"),
    "request.resource.attr.aBool" => field("a_bool"),
    "request.resource.attr.aOptionalString" => field("a_optional_string"),
    "request.resource.attr.scope" => field("scope"),
    "request.resource.attr.createdBy" => field("creator_id"),

    # These scalar paths with dots go through belongs_to chains. The adapter makes correlated
    # scalar subqueries for them and does not make joins. Thus they cannot increase the number
    # of rows in the result.
    "request.resource.attr.nested.aString" => field("nested.a_string"),
    "request.resource.attr.nested.aNumber" => field("nested.a_number"),
    "request.resource.attr.nested.aBool" => field("nested.a_bool"),
    "request.resource.attr.nested.aOptionalString" => field("nested.a_optional_string"),
    "request.resource.attr.nested.nextlevel.aString" => field("nested.nextlevel.a_string"),
    "request.resource.attr.nested.nextlevel.aBool" => field("nested.nextlevel.a_bool"),

    "request.resource.attr.ownedBy" => relation(:owned_by, member_field: "id"),
    "request.resource.attr.tags" => relation(
      :tags, member_field: "name", fields: {"id" => field("id"), "name" => field("name")}
    ),
    "request.resource.attr.categories" => relation(:categories, fields: {
      "name" => field("name"),
      "subCategories" => relation(:sub_categories, fields: {
        "name" => field("name"),
        "labels" => relation(:labels, fields: {"name" => field("name")})
      })
    }),
    # The same chain, but from the root. The policy uses the shape
    # `categories.subCategories.map(...)` directly.
    "request.resource.attr.categories.subCategories" => relation(
      :sub_categories, fields: {"name" => field("name")}
    )
  }.freeze

  # SQL cannot show these shapes correctly. Each one must raise an error and must not make a
  # filter. These entries are a result of a run of the suite. They are not an input to it.
  UNSUPPORTED = {
    "matches-regex" => "RE2 regex semantics are not portable to SQL LIKE or a dialect regex",
    "index-list" => "list indexing has no positional equivalent over an unordered relation",
    "map-compared" => "comparing a projected collection to an ordered list needs list equality",
    "filter" => "filter() evaluates to a list, not to a condition",
    "kitchensink" => "embeds a bare filter() as a conjunct"
  }.freeze

  # check() cannot evaluate the expressions of these actions with one shape of attributes.
  # Thus no oracle is available for a comparison. The plans are still correct, and each adapter
  # translates them. For this reason, the tests below compare the rows from the query directly.
  #
  # * The `categories.subCategories` shapes read a field from a LIST, and CEL refuses that.
  # * The `tags` shapes use R.attr.tags as a list of simple names. But the exists and all
  #   shapes use the same attribute as a list of objects. No single set of check() attributes
  #   can satisfy both. A relation mapping can satisfy both, because it has a member_field and
  #   a fields map together.
  EXECUTED_RESULT_ONLY = {
    "has-intersection-nested" => %w[507f1f77bcf86cd799439011 resource4 resource5],
    "map-deeply-nested" => %w[507f1f77bcf86cd799439011 resource4 resource5],
    # "public" in R.attr.tags
    "has-tag" => %w[507f1f77bcf86cd799439011 resource4 resource5],
    # !("private" in R.attr.tags) — resource6 is the only one tagged private
    "has-no-tag" => %w[507f1f77bcf86cd799439011 resource2 resource3 resource4 resource5],
    # hasIntersection(R.attr.tags, ["public", "draft"])
    "has-intersection-direct" => %w[507f1f77bcf86cd799439011 resource2 resource4 resource5],
    # none of: createdBy == P.id, "public" in R.attr.tags
    "relation-multiple-none" => %w[resource2 resource6]
  }.freeze

  ORACLE_ACTIONS = (POLICY_ACTIONS - UNSUPPORTED.keys - EXECUTED_RESULT_ONLY.keys).freeze

  def filtered_ids(action)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: SharedOracle.plan(action),
      model: SharedResource,
      attributes: SHARED_ATTRIBUTES
    ).pluck(:id).sort
  end

  it "covers every action in the shared policy" do
    expect(POLICY_ACTIONS.size).to eq(104)
    expect(ORACLE_ACTIONS.size + UNSUPPORTED.size + EXECUTED_RESULT_ONLY.size)
      .to eq(POLICY_ACTIONS.size)
  end

  it "produces a non-degenerate oracle" do
    # This test protects the other tests. If the PDP denied all the rows, because a policy did
    # not load or the connection was bad, each comparison below would agree but would prove
    # nothing.
    %w[equal gt in exists contains].each do |action|
      ids = SharedOracle.allowed_ids(action)
      expect(ids).not_to be_empty, "#{action}: oracle allowed nothing"
      expect(ids.size).to be < SharedModels::FIXTURES.size, "#{action}: oracle allowed everything"
    end
  end

  describe "matches the check() oracle" do
    ORACLE_ACTIONS.each do |action|
      it action do
        expect(filtered_ids(action)).to eq(SharedOracle.allowed_ids(action))
      end
    end
  end

  describe "translates shapes that check() cannot evaluate" do
    EXECUTED_RESULT_ONLY.each do |action, expected|
      it action do
        expect(filtered_ids(action)).to eq(expected.sort)
      end
    end
  end

  describe "fails loudly" do
    UNSUPPORTED.each do |action, reason|
      it "#{action} (#{reason})" do
        expect { filtered_ids(action) }.to raise_error(Cerbos::ActiveRecord::Error)
      end
    end
  end

  describe "plan kinds" do
    it "maps an unconditional allow to the full relation" do
      expect(filtered_ids("always-allow")).to eq(SharedModels::FIXTURES.map { |f| f[:id] }.sort)
    end

    it "maps an unconditional deny to an empty relation" do
      relation = Cerbos::ActiveRecord.query_plan_to_relation(
        plan: SharedOracle.plan("always-deny"), model: SharedResource, attributes: SHARED_ATTRIBUTES
      )
      expect(relation).to be_empty
      expect(relation).to be_a(ActiveRecord::Relation)
    end

    it "returns a composable relation" do
      relation = Cerbos::ActiveRecord.query_plan_to_relation(
        plan: SharedOracle.plan("gt"), model: SharedResource, attributes: SHARED_ATTRIBUTES
      )
      expect(relation.order(a_number: :desc).limit(2).pluck(:a_number)).to eq([5, 4])
    end
  end
end
