# frozen_string_literal: true

require "bson"
require "cerbos/mongodb/mongoid"

# Cerbos::MongoDB::Mongoid, offline. Mongoid builds a criteria's selector without a connection,
# so the property that makes the helper safe — the selector IS the adapter's filter, with none
# of Mongoid's type conversions applied — is asserted for every corpus action here, with no PDP
# and no server. Whether those filters return the documents the PDP allows is the adversarial
# harness's question, which it asks through the same helper against a real MongoDB.
RSpec.describe "Cerbos::MongoDB::Mongoid" do
  before(:all) { AdversarialMongoid.configure!("mongodb://127.0.0.1:1/offline") }

  MODEL = AdversarialMongoid::Resource
  FIXTURES = (ConformanceCorpus.wire_fixture_actions - ConformanceCorpus::THROWING_ACTIONS.map(&:first)).freeze

  def result_for(action)
    Cerbos::MongoDB.query_plan_to_filter(plan: ConformanceCorpus.wire_fixture(action), mapper: CorpusMapper::MAPPER)
  end

  # Compared as BSON bytes, not with ==, which calls 3 and 3.0 equal: a conversion from Float to
  # Integer is one of the things Mongoid does, and it is what makes -1e19 unencodable.
  def bytes(document) = BSON::Document.new(document).to_bson.to_s

  # Mongoid drops a clause of $and, $or or $nor that repeats an earlier one (Selector#evolve_multi
  # ends in `uniq`). That is the one change it makes, and it cannot change which documents match,
  # so the comparison applies it to the expected side rather than exempting any action.
  def without_repeated_clauses(node)
    case node
    when Hash
      node.to_h { |key, value|
        clauses = value.map { |clause| without_repeated_clauses(clause) } if %w[$and $or $nor].include?(key)
        [key, clauses ? clauses.uniq : without_repeated_clauses(value)]
      }
    when Array then node.map { |child| without_repeated_clauses(child) }
    else node
    end
  end

  it "hands every corpus filter to MongoDB verbatim, bar repeated clauses" do
    conditional = FIXTURES.map { |action| [action, result_for(action)] }.select { |(_, result)| result.conditional? }
    expect(conditional.size).to be > 150

    conditional.each do |(action, result)|
      selector = Cerbos::MongoDB::Mongoid.criteria(MODEL, result).selector
      expect(bytes(selector)).to eq(bytes(without_repeated_clauses(result.filter))), action
    end

    # Anti-vacuity for the one allowance: the corpus does repeat clauses, and nothing else moved.
    repeated = conditional.reject { |(_, result)| without_repeated_clauses(result.filter) == result.filter }
    expect(repeated.map(&:first)).to eq(%w[pv-shadow rel-ne-null-hop])
  end

  # Anti-vacuity: the same filters through a bare `where` are NOT verbatim, so the assertion
  # above is the helper talking rather than Mongoid leaving these filters alone anyway.
  it "is needed: a bare where converts constants in the cast probes" do
    AdversarialMongoid::CAST_PROBES.each do |name, condition|
      filter = Cerbos::MongoDB.query_plan_to_filter(
        plan: {"kind" => "KIND_CONDITIONAL", "condition" => condition}, mapper: AdversarialMongoid::UNTYPED_MAPPER
      ).filter
      expect(bytes(MODEL.where(Cerbos::MongoDB::Mongoid.raw(filter)).selector)).to eq(bytes(filter)), name
      converted = begin
        bytes(MODEL.where(filter).selector) != bytes(filter)
      rescue => e
        # Mongoid cannot even build these: an integral Float beyond 64 bits becomes an Integer
        # BSON cannot hold, and a string compared with a Time field fails inside its evolver.
        expect(e).to be_a(RangeError).or be_a(NoMethodError)
        true
      end
      expect(converted).to be(true), "#{name}: a bare where no longer converts it, so the helper is untested here"
    end
  end

  it "maps the unconditional kinds onto the scope, and composes with the caller's criteria" do
    allowed = Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_ALWAYS_ALLOWED"})
    denied = Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_ALWAYS_DENIED"})
    scope = MODEL.where(createdBy: "alice")

    expect(Cerbos::MongoDB::Mongoid.criteria(scope, allowed).selector).to eq(scope.selector)
    expect(Cerbos::MongoDB::Mongoid.criteria(MODEL, allowed).selector).to eq({})
    expect(Cerbos::MongoDB::Mongoid.criteria(scope, denied).selector).to eq(scope.selector)
    expect(Cerbos::MongoDB::Mongoid.criteria(scope, denied)).to be_empty_and_chainable

    composed = Cerbos::MongoDB::Mongoid.criteria(scope, result_for("vf-le")).where(aBool: true)
    expect(composed.selector).to include("createdBy" => "alice", "aBool" => true, "aNumber" => {"$gte" => 3})
  end
end
