# frozen_string_literal: true

require "bson"

# The translator unit test (ADR 0006).
#
# Reads its plans from conformance/wire-fixtures/ and asserts the filter this adapter emits
# against golden/expectations.json. Needs NO PDP, no policy and no MongoDB: the fixtures are
# recorded PlanResources responses, and nothing here opens a connection.
#
# It proves the adapter still emits what it emitted yesterday for a planner shape pinned
# independently of it. It says nothing about whether that filter returns the documents the PDP
# allows — that is spec/adversarial_conformance_spec.rb, and only the corpus asks the same question
# of every other adapter. So a new SHAPE belongs in conformance/policies/adversarial.yaml, never
# here.
RSpec.describe "translator" do
  MAPPER = CorpusMapper::MAPPER
  THROWING_ACTIONS = ConformanceCorpus::THROWING_ACTIONS
  THROWING = THROWING_ACTIONS.map(&:first).freeze

  # `nullRepresentationOmitted` is NOT in that list. Under the default representation the
  # `nullable` mapper flag translates `null-eq-missing` into a filter, so it carries a golden
  # entry like any other action; its refusal belongs to the flipped option and is asserted below.
  RECORDED_ACTIONS = (ConformanceCorpus.wire_fixture_actions - THROWING).freeze

  def self.fixture_kind(action)
    JSON.parse(File.read(File.join(ConformanceCorpus::WIRE_FIXTURES_DIR, "#{action}.json"), encoding: "UTF-8"))
      .fetch("filter").fetch("kind")
  end

  def self.translate(action, **options)
    Cerbos::MongoDB.query_plan_to_filter(plan: ConformanceCorpus.wire_fixture(action), mapper: MAPPER, **options)
  end

  # The recorded document for one action: the plan kind the planner folded to and, for a
  # conditional plan, the filter. An unconditional plan carries no `filter` key at all — its
  # filter is one of two constants, asserted below — so the two cases cannot be confused.
  def self.expectation_for(action)
    result = translate(action)
    entry = {"kind" => result.kind}
    entry["filter"] = GoldenExpectations.encode(result.filter) if result.conditional?
    # Through JSON and back, so the comparison sees exactly the types the asset can hold.
    JSON.parse(JSON.generate(entry))
  end

  def expectation_for(action) = self.class.expectation_for(action)

  def emitted(action) = self.class.translate(action).filter

  # Regeneration is a deliberate act and CI never performs it. A throwing action gets no entry:
  # its message is corpus data, pinned in actions.json and asserted below, and skipping it is also
  # what keeps regeneration from papering over a misclassification.
  if ENV["GOLDEN_UPDATE"] == "1"
    GoldenExpectations.write(RECORDED_ACTIONS.to_h { |action| [action, expectation_for(action)] })
  end

  RECORDED = GoldenExpectations.read.freeze

  # Every node of a filter, with the chain of keys that leads to it.
  def self.walk(node, path = [], &block)
    yield node, path
    case node
    when Hash then node.each { |key, child| walk(child, path + [key], &block) }
    when Array then node.each_with_index { |child, index| walk(child, path + [index], &block) }
    end
  end

  def walk(node, &) = self.class.walk(node, &)

  CONDITIONAL_ACTIONS = RECORDED_ACTIONS.select { |action| fixture_kind(action) == "KIND_CONDITIONAL" }.freeze

  describe "the golden expectations" do
    # ADR 0006: every wire fixture accounted for exactly once.
    it "accounts for every wire fixture exactly once" do
      classified = (RECORDED.keys + THROWING).sort
      expect(classified).to eq(ConformanceCorpus.wire_fixture_actions)
      expect(classified).to eq(classified.uniq)
      expect(RECORDED.keys).to eq(RECORDED.keys.sort)
    end

    # Update these tripwires only after replaying new actions against the oracle.
    it "pins how the corpus divides here" do
      conditional, unconditional = RECORDED.values.partition { |entry| entry.fetch("kind") == "KIND_CONDITIONAL" }
      expect({
        "conditional" => conditional.size, "unconditional" => unconditional.size, "throwing" => THROWING_ACTIONS.size
      }).to eq({"conditional" => 198, "unconditional" => 7, "throwing" => 96})
    end

    # The unconditional folds are the planner's, not this adapter's.
    it "names the planner folds the corpus declares" do
      unconditional = RECORDED.reject { |_, entry| entry.fetch("kind") == "KIND_CONDITIONAL" }
      expect(unconditional.keys).to eq(%w[in-empty p-has pv-empty-all pv-empty-exists pv-empty-not-all pv-empty-not-exists pv-structs-missing])
      expect(unconditional.values).to all(satisfy { |entry| !entry.key?("filter") })
      expect(unconditional.fetch("p-has").fetch("kind")).to eq("KIND_ALWAYS_ALLOWED")
      expect(ConformanceCorpus::SKIPPED).to include("p-has")
    end

    it "declares the adapter and a regeneration command that exists, and no generator" do
      contents = JSON.parse(File.read(GoldenExpectations::FILE, encoding: "UTF-8"))
      expect(contents.keys).to eq(%w[adapter regenerate expectations])
      expect(contents.fetch("adapter")).to eq("mongodb-ruby")

      command = contents.fetch("regenerate")
      expect(command).to eq(GoldenExpectations::REGENERATE)
      script = File.expand_path("../#{command.delete_prefix("./")}", __dir__)
      expect(File.executable?(script)).to be(true), "#{command} is not an executable file"
    end

    it "refuses a file that declares another adapter" do
      allow(File).to receive(:read).and_call_original
      allow(File).to receive(:read).with(GoldenExpectations::FILE, encoding: "UTF-8").and_return(
        JSON.generate({"adapter" => "mongoose", "expectations" => {}})
      )
      expect { GoldenExpectations.read }.to raise_error(/declares adapter "mongoose"/)
    end

    # What keeps "no generator" true: a library type reaching the filter would be written as
    # whatever its #to_json prints, and the file would then depend on that library's version.
    it "refuses any value that is not plain data" do
      expect { GoldenExpectations.encode({"_id" => BSON::ObjectId.new}) }.to raise_error(/BSON::ObjectId/)
      expect { GoldenExpectations.encode({"a" => {"$regex" => /x/}}) }.to raise_error(/Regexp/)
      expect { GoldenExpectations.encode({a: 1}) }.to raise_error(/non-String key/)
      expect { GoldenExpectations.encode({"a" => Float::NAN}) }.to raise_error(/JSON cannot hold/)
    end
  end

  describe "emits the golden expectation" do
    RECORDED_ACTIONS.each do |action|
      it action do
        expect(expectation_for(action)).to eq(RECORDED.fetch(action))
      end
    end
  end

  # --- rules over the WHOLE corpus -----------------------------------------------------------
  #
  # What survives an unread regeneration: a regenerated file happily records a wrong filter, but
  # a rule stated over every action does not, and it holds for an action nobody has added yet.
  # Each carries its own anti-vacuity assertion.
  describe "what every emitted filter is" do
    it "is a query document the BSON encoder accepts" do
      CONDITIONAL_ACTIONS.each do |action|
        expect { BSON::Document.new(emitted(action)).to_bson }.not_to raise_error, action
      end
    end

    it "gives unconditional plans a constant, safe-to-run filter" do
      folds = RECORDED_ACTIONS - CONDITIONAL_ACTIONS
      expect(folds).not_to be_empty
      folds.each do |action|
        result = self.class.translate(action)
        expected = result.always_allowed? ? {} : {"$expr" => false}
        expect(result.filter).to eq(expected), action
      end
    end

    # MongoDB accepts $expr only at the top level of a query document — not under a field and
    # not inside $elemMatch ("$expr can only be applied to the top-level document"). A filter
    # breaking that aborts the query rather than returning the wrong documents, but a query that
    # aborts is a refusal the adapter should have made at translation.
    it "puts $expr only where MongoDB accepts it" do
      seen = 0
      CONDITIONAL_ACTIONS.each do |action|
        walk(emitted(action)) do |_, path|
          next unless path.last == "$expr"

          seen += 1
          logical = path[0..-2].reject { |key| key.is_a?(Integer) }
          expect(logical - %w[$and $or $nor]).to be_empty, "#{action}: $expr under #{path.inspect}"
        end
      end
      expect(seen).to be > 0, "no action emitted $expr, so this rule guarded nothing"
    end

    # PCRE2's `$` also matches before a final newline, where RE2's does not, so a pattern that
    # ends in an unescaped `$` admits "value\n". Every end anchor this adapter emits is `\z`.
    it "never ends a regular expression with a bare $" do
      patterns = []
      CONDITIONAL_ACTIONS.each do |action|
        walk(emitted(action)) do |node, path|
          patterns << [action, node] if node.is_a?(String) && %w[$regex regex].include?(path.last)
        end
      end
      expect(patterns).not_to be_empty
      expect(patterns.select { |(_, pattern)| pattern.match?(/(?<!\\)\$\z/) }).to be_empty
      expect(patterns.map(&:last)).to include(a_string_ending_with("\\z"))
    end

    # Server-side JavaScript and a second collection are both ways out of the document the
    # application serialised; this adapter needs neither and emits neither.
    it "uses no server-side JavaScript and reads no second collection" do
      forbidden = %w[$where $function $accumulator $lookup $graphLookup $unionWith]
      operators = []
      CONDITIONAL_ACTIONS.each do |action|
        walk(emitted(action)) { |_, path| operators << path.last if path.last.is_a?(String) && path.last.start_with?("$") }
      end
      expect(operators).to include("$elemMatch", "$expr", "$nor")
      expect(operators & forbidden).to be_empty
    end

    # A timestamp reaching MongoDB as a STRING would compare lexically against a stored date and
    # silently disagree with the PDP, so the actions binding a BSON date are named exactly.
    it "binds a date in exactly the actions the corpus timestamps" do
      dated = CONDITIONAL_ACTIONS.select { |action|
        found = false
        walk(emitted(action)) { |node, _| found ||= node.is_a?(Time) }
        found
      }
      expect(dated.sort).to eq(TIMESTAMPED)
    end

    TIMESTAMPED = RECORDED.select { |_, entry| JSON.generate(entry).include?('{"$date":') }.keys.sort.freeze

    it "records a date only as a millisecond instant in CEL's range" do
      expect(TIMESTAMPED).not_to be_empty
      TIMESTAMPED.each do |action|
        walk(RECORDED.fetch(action)) do |node, _|
          next unless node.is_a?(Hash) && node.key?("$date")

          expect(node.fetch("$date")).to match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\z/), action
        end
      end
    end
  end

  # --- the shapes that must throw ------------------------------------------------------------
  #
  # Against the message the corpus pins, so a mapper typo or an unrelated validation cannot
  # stand in for the limitation the classification declares.
  describe "refuses" do
    THROWING_ACTIONS.each do |(action, message)|
      it "#{action} with the message the corpus pins" do
        expect { self.class.translate(action) }
          .to raise_error(Cerbos::MongoDB::Error, /#{Regexp.escape(message)}/)
      end
    end

    # Where in the walk each refusal is raised, and how often. No corpus action can state this:
    # it is a question about the adapter, not about which documents come back. A change that
    # moves refusals from one site to another — or silently starts translating a family — fails
    # here even when every pinned message still matches.
    it "raises every refusal from a known site, in these proportions" do
      sites = THROWING.map do |action|
        self.class.translate(action)
        raise "#{action} did not raise"
      rescue Cerbos::MongoDB::Error => e
        frame = e.backtrace.find { |line| line.include?("/lib/cerbos/mongodb/") }
        File.basename(frame[/[^:]+/])
      end
      expect(sites.tally.sort.to_h).to eq(REFUSAL_SITES)
    end

    REFUSAL_SITES = {
      "aggregation.rb" => 35, "hierarchy.rb" => 4, "regex.rb" => 10, "translator.rb" => 47
    }.freeze
  end

  # The `nullRepresentationOmitted` group: refused under the flipped option. Both halves are
  # asserted, because the rejection alone would pass if the adapter raised for another reason.
  describe "the omitted null representation" do
    ConformanceCorpus::NULL_OMITTED_THROWS.each do |(action, message)|
      it "#{action} is refused under :omitted" do
        expect { self.class.translate(action, null_attribute_representation: :omitted) }
          .to raise_error(Cerbos::MongoDB::Error, /#{Regexp.escape(message)}/)
      end

      it "#{action} is a null-selecting comparison under the default" do
        filter = RECORDED.fetch(action).fetch("filter")
        comparisons = []
        walk(filter) { |node, _| comparisons << node if node.is_a?(Hash) && node.key?("$eq") && node["$eq"].nil? }
        expect(comparisons).not_to be_empty
      end
    end
  end
end
