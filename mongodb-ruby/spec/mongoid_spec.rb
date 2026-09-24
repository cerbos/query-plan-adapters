# frozen_string_literal: true

require "bson"
require "cerbos/mongodb/mongoid"

# Cerbos::MongoDB::Mongoid, offline. Mongoid builds a criteria's selector without a connection,
# so the property that makes the helper safe — the selector IS the adapter's filter, with none
# of Mongoid's type conversions applied — is asserted for every recorded plan here, with no
# server. Whether those filters return the documents the PDP allows is the conformance
# harness's question, which it asks through the same helper against a real MongoDB.
RSpec.describe "Cerbos::MongoDB::Mongoid" do
  before(:all) { ConformanceMongoid.configure!("mongodb://127.0.0.1:1/offline") }

  MODEL = ConformanceMongoid::Resource
  CURRENT = ConformanceCorpus::PDP_TAGS.first
  # Every case the harness translates under the current PDP: the ledger's refusals have no filter.
  CASES = ConformanceCorpus.goldens(CURRENT)
    .reject { |golden| ConformanceCorpus.ledger_entry(golden.fetch("id"), CURRENT)&.fetch("status") == "unsupported" }
    .to_h { |golden| [golden.fetch("id"), golden.fetch("plan")] }.freeze

  def result_for(id)
    plan = CASES.fetch(id) { ConformanceCorpus.golden(id).fetch("plan") }
    Cerbos::MongoDB.query_plan_to_filter(plan: plan, mapper: CorpusMapper::MAPPER)
  end

  # Compared as BSON bytes, not with ==, which calls 3 and 3.0 equal: a conversion from Float to
  # Integer is one of the things Mongoid does, and it is what makes -1e19 unencodable.
  def bytes(document) = BSON::Document.new(document).to_bson.to_s

  # Mongoid drops a clause of $and, $or or $nor that repeats an earlier one (Selector#evolve_multi
  # ends in `uniq`). That is the one change it makes, and it cannot change which documents match,
  # so the comparison applies it to the expected side rather than exempting any case.
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
    conditional = CASES.keys.map { |id| [id, result_for(id)] }.select { |(_, result)| result.conditional? }
    expect(conditional).not_to be_empty

    conditional.each do |(id, result)|
      selector = Cerbos::MongoDB::Mongoid.criteria(MODEL, result).selector
      expect(bytes(selector)).to eq(bytes(without_repeated_clauses(result.filter))), id
    end

    # Anti-vacuity for the one allowance: the corpus does repeat clauses somewhere, so the
    # normalisation above is exercised rather than a no-op.
    repeated = conditional.reject { |(_, result)| without_repeated_clauses(result.filter) == result.filter }
    expect(repeated).not_to be_empty
  end

  # Anti-vacuity: the same filters through a bare `where` are NOT verbatim, so the assertion
  # above is the helper talking rather than Mongoid leaving these filters alone anyway.
  it "is needed: a bare where converts constants in the cast probes" do
    ConformanceMongoid::CAST_PROBES.each do |name, condition|
      filter = Cerbos::MongoDB.query_plan_to_filter(
        plan: {"kind" => "KIND_CONDITIONAL", "condition" => condition}, mapper: ConformanceMongoid::UNTYPED_MAPPER
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

    composed = Cerbos::MongoDB::Mongoid.criteria(scope, result_for("comparison/less-or-equal/value-first")).where(aBool: true)
    expect(composed.selector).to include("createdBy" => "alice", "aBool" => true, "aNumber" => {"$gte" => 3})
  end
end
