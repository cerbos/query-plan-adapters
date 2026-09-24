# frozen_string_literal: true

require "mongo"
require "cerbos/mongodb/mongoid"

# The conformance harness. It implements conformance/README.md, "The harness contract":
#
# 1. Store the dataset (spec/support/conformance_store.rb) and map it (spec/support/corpus_mapper.rb).
# 2. For each PDP and each recorded golden file, translate the plan, run the query, and
#    compare the ids with the ones check() allowed. conformance-ledger.json lists the
#    exceptions: `unsupported` must raise the adapter's refusal error, and `divergent` must
#    still give a wrong answer.
# 3. Fail if the ledger names a case that has no golden file.
#
# Every case runs twice: through the official driver, and through Mongoid on typed models
# (spec/support/conformance_mongoid.rb) via Cerbos::MongoDB::Mongoid.criteria. Mongoid converts a
# query constant to its field's declared type, which is the one way the same filter could return
# other documents there.
#
# Needs no PDP: the plans and decisions are recorded. It needs a MongoDB at MONGODB_URI, which
# scripts/test.sh starts from MONGO_IMAGE (or MONGO_NEXT_IMAGE).
Mongo::Logger.logger.level = Logger::FATAL

RSpec.describe "conformance" do
  before(:all) do
    ConformanceStore.establish!
    ConformanceMongoid.configure!(ENV.fetch("MONGODB_URI"))
  end

  MAPPER = CorpusMapper::MAPPER

  def translate(plan) = Cerbos::MongoDB.query_plan_to_filter(plan: plan, mapper: MAPPER)

  # ALWAYS_DENIED still runs its match-nothing filter: the query is the thing under test.
  LEGS = {
    "driver" => ->(result) { ConformanceStore.ids(result.filter) },
    "mongoid" => ->(result) { Cerbos::MongoDB::Mongoid.criteria(ConformanceMongoid::Resource, result).pluck(:resourceId).sort }
  }.freeze

  # A mapping mistake raises MapperError, which is a bug in the harness rather than a limitation
  # of the adapter, so it must never satisfy an `unsupported` entry.
  def expect_refusal(plan)
    expect { translate(plan) }.to raise_error(Cerbos::MongoDB::Error) { |error|
      expect(error).not_to be_a(Cerbos::MongoDB::MapperError)
    }
  end

  # An `unsupported` entry passes on a refusal, and an attribute the map forgot would raise one
  # too. So every attribute check() saw must reach a mapping, exactly or through a mapped
  # ancestor (a relation such as `mainCategory`).
  it "maps every attribute of every resource" do
    mapped = MAPPER.keys
    unmapped = ConformanceCorpus.resource_attribute_paths.reject { |path|
      mapped.any? { |key| path == key || path.start_with?("#{key}.") }
    }
    expect(unmapped).to be_empty
  end

  # The seeder for the to-one chain, read back out of the stored documents (ADR 0005). A count
  # cannot tell the parent's values from the root's, which is the flat-alias failure this
  # relation exists to expose.
  it "stores the to-one chain the corpus describes" do
    stored = ConformanceStore.collection.find({}).to_h { |doc|
      [doc.fetch("resourceId"), [doc.dig("parent", "aString"), doc.dig("parent", "inner", "aString")]]
    }
    expected = ConformanceCorpus::SEEDS.to_h { |seed|
      parent = ConformanceCorpus.parent_seed_of(seed)
      inner = ConformanceCorpus.parent_seed_of(parent)
      [seed.fetch("id"), [parent&.fetch("aString"), inner&.fetch("aString")]]
    }
    expect(stored).to eq(expected)
  end

  # The indexed cases read an element's BSON type, so a seeder that turned a null element into
  # false or 0 would leave the filter reading a list check() was never sent.
  it "stores the number and boolean lists verbatim, null elements included" do
    stored = ConformanceStore.collection.find({}).to_h { |doc|
      [doc.fetch("resourceId"), [doc.fetch("aNumberList"), doc.fetch("aBoolList")]]
    }
    expected = ConformanceCorpus::SEEDS.to_h { |seed| [seed.fetch("id"), [seed.fetch("aNumberList"), seed.fetch("aBoolList")]] }
    expect(stored).to eq(expected)
  end

  ConformanceCorpus::PDP_TAGS.each do |tag|
    describe "PDP #{tag}" do
      it "has a golden file for every case in the ledger" do
        recorded = ConformanceCorpus.goldens(tag).map { |golden| golden.fetch("id") }
        expect(ConformanceCorpus::LEDGER.keys - recorded).to be_empty
      end

      ConformanceCorpus.goldens(tag).each do |golden|
        id = golden.fetch("id")
        plan = golden.fetch("plan")
        allowed = golden.fetch("allowed").sort

        LEGS.each do |leg, ids|
          it "#{leg} #{golden.fetch("tier")}: #{id}" do
            skip "planner divergence" if ConformanceCorpus.skipped?(golden, tag)

            case ConformanceCorpus.ledger_entry(id, tag)&.fetch("status")
            when nil then expect(ids.call(translate(plan))).to eq(allowed)
            when "unsupported" then expect_refusal(plan)
            when "divergent" then expect(ids.call(translate(plan))).not_to eq(allowed)
            else raise "#{id}: unknown ledger status"
            end
          end
        end
      end
    end
  end
end
