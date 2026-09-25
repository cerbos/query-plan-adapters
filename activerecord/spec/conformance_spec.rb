# frozen_string_literal: true

# The conformance harness. It implements conformance/README.md, "The harness contract":
#
# 1. Store the dataset (spec/support/conformance_store.rb) and map it (corpus_attributes.rb).
# 2. For each PDP and each recorded golden file, translate the plan, run the query, and
#    compare the ids with the ones check() allowed. conformance-ledger.json lists the
#    exceptions: `unsupported` must raise the adapter's refusal error, and `divergent` must
#    still give a wrong answer.
# 3. Fail if the ledger names a case that has no golden file.
#
# Needs no PDP: the plans and decisions are recorded. The store is SQLite in memory, or
# PostgreSQL or MySQL (ADAPTER_TEST_DB, spec/support/database.rb).

RSpec.describe "conformance" do
  before { ConformanceStore.establish! }

  def ids(plan)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: plan, model: AdvResource, attributes: CorpusAttributes::ATTRIBUTES
    ).pluck(:id).sort
  end

  # An `unsupported` entry passes on any Cerbos::ActiveRecord::Error, and an attribute the map
  # forgot raises one too. So every attribute check() saw must reach a mapping, exactly or
  # through a mapped ancestor (a relation such as `mainCategory`), or a forgotten mapping would
  # pass as a refusal.
  it "maps every attribute of every resource" do
    mapped = CorpusAttributes::ATTRIBUTES.keys
    unmapped = ConformanceCorpus.resource_attribute_paths.reject { |path|
      mapped.any? { |key| path == key || path.start_with?("#{key}.") }
    }
    expect(unmapped).to be_empty
  end

  ConformanceCorpus::PDP_TAGS.each do |tag|
    describe "PDP #{tag}" do
      it "has a golden file for every case in the ledger" do
        recorded = ConformanceCorpus.goldens(tag).map { |golden| golden.fetch("id") }
        expect(ConformanceCorpus::LEDGER.keys - recorded).to be_empty
      end

      ConformanceCorpus.goldens(tag).each do |golden|
        it "#{golden.fetch("tier")}: #{golden.fetch("id")}" do
          skip "planner divergence" if ConformanceCorpus.skipped?(golden, tag)

          plan = golden.fetch("plan")
          allowed = golden.fetch("allowed").sort

          case ConformanceCorpus.ledger_entry(golden.fetch("id"), tag)&.fetch("status")
          when nil then expect(ids(plan)).to eq(allowed)
          when "unsupported" then expect { ids(plan) }.to raise_error(Cerbos::ActiveRecord::Error)
          when "divergent" then expect(ids(plan)).not_to eq(allowed)
          else raise "#{golden.fetch("id")}: unknown ledger status"
          end
        end
      end
    end
  end
end
