# frozen_string_literal: true

require "yaml"

# The shared policy suite every adapter in this repository is exercised against
# (/policies/resource.yaml).
#
# Actions are discovered from the policy file itself, so an action added there cannot
# silently go untested here. Each one is planned against a real PDP, translated, executed
# against seeded rows, and compared with a per-row check() oracle — the same differential
# technique the adversarial harness uses, rather than hand-written expectations.

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

    # Dotted scalar paths resolved through belongs_to chains, as correlated scalar
    # subqueries rather than joins, so they cannot multiply the result set.
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
    # The same chain flattened from the root, for the `categories.subCategories.map(...)`
    # shapes the policy addresses directly.
    "request.resource.attr.categories.subCategories" => relation(
      :sub_categories, fields: {"name" => field("name")}
    )
  }.freeze

  # Shapes SQL cannot express faithfully. Each must raise, never emit a filter — the entries
  # here are an output of running the suite, not an input to it.
  UNSUPPORTED = {
    "matches-regex" => "RE2 regex semantics are not portable to SQL LIKE or a dialect regex",
    "index-list" => "list indexing has no positional equivalent over an unordered relation",
    "map-compared" => "comparing a projected collection to an ordered list needs list equality",
    "filter" => "filter() evaluates to a list, not to a condition",
    "kitchensink" => "embeds a bare filter() as a conjunct"
  }.freeze

  # Actions whose policy expression cannot be evaluated by check() against any single
  # attribute shape, so no oracle exists to compare against. The plans are still well formed
  # and every adapter translates them, so the executed row set is asserted directly.
  #
  # * The `categories.subCategories` shapes read a field off a LIST, which CEL rejects.
  # * The `tags` shapes address R.attr.tags as a list of bare names, while the exists/all
  #   shapes address the same attribute as a list of objects. No single check() payload
  #   satisfies both; a relation mapping does, because member_field and fields coexist.
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
    expect(POLICY_ACTIONS.size).to eq(103)
    expect(ORACLE_ACTIONS.size + UNSUPPORTED.size + EXECUTED_RESULT_ONLY.size)
      .to eq(POLICY_ACTIONS.size)
  end

  it "produces a non-degenerate oracle" do
    # Guard the guard: if the PDP denied everything (a policy that failed to load, a broken
    # connection) every comparison below would pass while proving nothing.
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
