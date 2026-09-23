# frozen_string_literal: true

# The translator unit test (ADR 0006, #377).
#
# Replays conformance/wire-fixtures/ and compares the emitted SQL with golden/expectations.json.
# Needs no PDP and no database server (SQLite in memory).
#
# It only proves the output has not changed. Whether the filter returns the right rows is
# spec/adversarial_conformance_spec.rb's job, so new shapes go in the corpus, not here.

# At load time, not in a `before` hook: golden regeneration below runs while this file loads,
# and rendering SQL needs a connection.
AdversarialModels.establish!

RSpec.describe "translator" do
  # Every emitted statement starts with this, so recording only the WHERE clause loses nothing.
  # Asserted below.
  PREAMBLE = %(SELECT "adversarial_resources".* FROM "adversarial_resources")

  THROWING_ACTIONS = ConformanceCorpus::THROWING_ACTIONS
  THROWING = THROWING_ACTIONS.map(&:first).freeze

  # `nullRepresentationOmitted` actions are not in THROWING: by default they translate to
  # IS NULL and get a golden entry. Their refusal under `omitted` is tested separately below.
  RECORDED_ACTIONS = (ConformanceCorpus.wire_fixture_actions - THROWING).freeze

  # Shapes ActiveRecord 7.1 renders differently from 8.0. Asserted both ways, so a shape that
  # stops diverging fails and must be removed.
  RENDERING_DIFFERS_ON_ACTIVERECORD_71 = %w[].freeze

  def self.emitted_sql(action)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: ConformanceCorpus.wire_fixture(action),
      model: AdvResource,
      attributes: CorpusAttributes::ATTRIBUTES
    ).to_sql
  end

  def emitted_sql(action) = self.class.emitted_sql(action)

  # The plan kind, plus the WHERE clause for a conditional plan. Unconditional plans have no
  # `where` key at all, not an empty string.
  def self.expectation_for(action)
    sql = emitted_sql(action)
    kind = JSON.parse(File.read(
      File.join(ConformanceCorpus::WIRE_FIXTURES_DIR, "#{action}.json")
    )).fetch("filter").fetch("kind")

    entry = {"kind" => kind}
    entry["where"] = sql.delete_prefix("#{PREAMBLE} WHERE ") if sql.start_with?("#{PREAMBLE} WHERE ")
    entry
  end

  # CI never regenerates, so a changed filter fails there and the diff gets reviewed.
  # Throwing actions get no entry; their messages are pinned in actions.json.
  if ENV["GOLDEN_UPDATE"] == "1"
    GoldenExpectations.write(RECORDED_ACTIONS.to_h { |action| [action, expectation_for(action)] })
  end

  RECORDED = GoldenExpectations.read.freeze

  describe "the golden expectations" do
    it "accounts for every wire fixture exactly once" do
      classified = (RECORDED.keys + THROWING).sort

      # Every fixture has a golden entry or a pinned throw.
      expect(classified).to eq(ConformanceCorpus.wire_fixture_actions)
      # And not both.
      expect(classified).to eq(classified.uniq)
      # Sorted, so diffs are easy to read.
      expect(RECORDED.keys).to eq(RECORDED.keys.sort)
    end

    # Update these tripwires only after replaying new actions against the oracle.
    it "pins how the corpus divides here" do
      conditional, unconditional = RECORDED.values.partition { |entry|
        entry.fetch("kind") == "KIND_CONDITIONAL"
      }

      expect({
        "conditional" => conditional.size,
        "unconditional" => unconditional.size,
        "throwing" => THROWING_ACTIONS.size
      }).to eq({"conditional" => 245, "unconditional" => 7, "throwing" => 72})
    end

    # Unconditional plans come from the planner, not this adapter. p-has is a declared
    # upstream divergence.
    it "names the planner folds the corpus declares" do
      unconditional = RECORDED.select { |_, entry| entry.fetch("kind") != "KIND_CONDITIONAL" }
      expect(unconditional.keys).to eq(%w[in-empty p-has pv-empty-all pv-empty-exists pv-empty-not-all pv-empty-not-exists pv-structs-missing])
      expect(unconditional.fetch("in-empty").fetch("kind")).to eq("KIND_ALWAYS_DENIED")
      expect(unconditional.fetch("p-has").fetch("kind")).to eq("KIND_ALWAYS_ALLOWED")
      expect(ConformanceCorpus::SKIPPED).to include("p-has")
    end

    it "declares the adapter, the generator and a command that exists" do
      contents = JSON.parse(File.read(GoldenExpectations::FILE))
      expect(contents.fetch("adapter")).to eq("activerecord")
      expect(contents.fetch("activerecord")).to eq(GoldenExpectations::GOLDEN_ACTIVERECORD_MAJOR)

      command = contents.fetch("regenerate")
      expect(command).to eq(GoldenExpectations::REGENERATE)
      script = File.expand_path("../#{command.delete_prefix("./")}", __dir__)
      expect(File.executable?(script)).to be(true), "#{command} is not an executable file"
    end

    # So a toolchain change cannot pass as a translation change.
    it "refuses to regenerate under a different ActiveRecord major" do
      allow(GoldenExpectations).to receive(:installed_activerecord_major).and_return("0.0")
      expect { GoldenExpectations.write({}) }
        .to raise_error(/is generated under ActiveRecord/)
    end

    it "refuses a file that declares another adapter" do
      allow(File).to receive(:read).with(GoldenExpectations::FILE).and_return(
        JSON.generate({"adapter" => "sqlalchemy", "expectations" => {}})
      )
      expect { GoldenExpectations.read }.to raise_error(/declares adapter "sqlalchemy"/)
    end
  end

  describe "emits the golden expectation" do
    RECORDED_ACTIONS.each do |action|
      it action do
        emitted = self.class.expectation_for(action)
        recorded = RECORDED.fetch(action)

        if GoldenExpectations.installed_activerecord_major !=
            GoldenExpectations::GOLDEN_ACTIVERECORD_MAJOR &&
            RENDERING_DIFFERS_ON_ACTIVERECORD_71.include?(action)
          expect(emitted).not_to eq(recorded)
          next
        end

        expect(emitted).to eq(recorded)
      end
    end

    # On the other leg, every action not in the list must still render identically.
    it "diverges on exactly the shapes the list names" do
      # Every name in the list must be a recorded action.
      expect(RENDERING_DIFFERS_ON_ACTIVERECORD_71 - RECORDED_ACTIONS).to be_empty

      # Empty on purpose: 7.1 and 8.0 render every recorded shape identically today.
      expect(RENDERING_DIFFERS_ON_ACTIVERECORD_71).to be_empty

      next if GoldenExpectations.installed_activerecord_major ==
        GoldenExpectations::GOLDEN_ACTIVERECORD_MAJOR

      diverging = RECORDED_ACTIONS.reject { |action|
        self.class.expectation_for(action) == RECORDED.fetch(action)
      }
      expect(diverging.sort).to eq(RENDERING_DIFFERS_ON_ACTIVERECORD_71.sort)
    end
  end

  # --- rules over the whole corpus -----------------------------------------------------------
  #
  # These still catch bugs if someone regenerates without reading the diff, and they cover
  # future actions too. Each also checks it is not vacuous.
  describe "what the emitted statement contains" do
    it "opens every statement with the same preamble" do
      RECORDED_ACTIONS.each do |action|
        expect(emitted_sql(action)).to start_with(PREAMBLE), action
      end
    end

    it "reassembles every recorded clause into the statement it came from" do
      # Only on the 8.0 leg; the other leg is covered by the divergence list.
      skip "asset was generated under another ActiveRecord" if
        GoldenExpectations.installed_activerecord_major !=
          GoldenExpectations::GOLDEN_ACTIVERECORD_MAJOR

      RECORDED_ACTIONS.each do |action|
        entry = RECORDED.fetch(action)
        expected = entry.key?("where") ? "#{PREAMBLE} WHERE #{entry.fetch("where")}" : PREAMBLE
        expect(emitted_sql(action)).to eq(expected), action
      end
    end

    # Without ESCAPE, a needle with %, _ or \ matches rows the PDP denies.
    it "gives every LIKE an ESCAPE clause" do
      with_like = 0
      RECORDED_ACTIONS.each do |action|
        sql = emitted_sql(action)
        likes = sql.scan(" LIKE ").size
        next if likes.zero?

        with_like += 1
        expect(sql.scan(" ESCAPE ").size).to eq(likes), action
      end

      expect(with_like).to be > 0, "no action emitted a LIKE, so this rule guarded nothing"
    end

    # A mapping typo gives an unknown table, and SQLite only complains when the query runs,
    # which this test never does.
    it "names only tables the schema declares" do
      # From the live connection, not a hand-written list.
      known = ::ActiveRecord::Base.connection.tables
      seen = []
      RECORDED_ACTIONS.each do |action|
        emitted_sql(action).scan(/"(adversarial_[a-z_]+)"/).flatten.uniq.each do |table|
          seen << table
          expect(known).to include(table), "#{action} names #{table}"
        end
      end

      expect(seen.uniq.size).to be > 1, "only one table was ever named, so this rule is vacuous"
    end

    # A second FROM on the resource table means a subquery lost its correlation.
    it "never joins the resource table to itself" do
      resource_scans = ->(sql) { sql.scan('FROM "adversarial_resources"').size }

      RECORDED_ACTIONS.each do |action|
        expect(resource_scans.call(emitted_sql(action))).to eq(1), action
      end

      # The detector does count 2 when the table appears twice.
      uncorrelated = %(#{PREAMBLE} WHERE EXISTS (SELECT 1 FROM "adversarial_resources"))
      expect(resource_scans.call(uncorrelated)).to eq(2)
    end

    # Pinned exactly: a timestamp sent as a plain string would compare as text and could
    # disagree with the PDP.
    it "binds a temporal literal in exactly the actions the corpus timestamps" do
      dated = RECORDED_ACTIONS.select { |action|
        emitted_sql(action).match?(/'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/)
      }
      expect(dated.sort).to eq(%w[ts-eq ts-eq-offset ts-ne])
    end
  end

  # --- the shapes that must throw ------------------------------------------------------------
  describe "refuses" do
    THROWING_ACTIONS.each do |(action, message)|
      it "#{action} with the message the corpus pins" do
        expect {
          Cerbos::ActiveRecord.query_plan_to_relation(
            plan: ConformanceCorpus.wire_fixture(action),
            model: AdvResource,
            attributes: CorpusAttributes::ATTRIBUTES
          )
        }.to raise_error(Cerbos::ActiveRecord::Error, /#{Regexp.escape(message)}/)
      end
    end
  end

  # `nullRepresentationOmitted` actions throw only under the `omitted` option. Both halves are
  # tested, so a throw for some other reason cannot pass.
  describe "the omitted null representation" do
    ConformanceCorpus::NULL_OMITTED_THROWS.each do |(action, message)|
      it "#{action} is refused" do
        expect {
          Cerbos::ActiveRecord.query_plan_to_relation(
            plan: ConformanceCorpus.wire_fixture(action),
            model: AdvResource,
            attributes: CorpusAttributes::UNDECLARED,
            null_attribute_representation: :omitted
          )
        }.to raise_error(Cerbos::ActiveRecord::Error, /#{Regexp.escape(message)}/)
      end

      # Why the refusal matters: by default the same plan becomes IS NULL, which the harness
      # shows returns rows the PDP denies.
      it "#{action} would otherwise emit an IS NULL filter" do
        expect(RECORDED.fetch(action).fetch("where")).to include("IS NULL")
      end
    end
  end
end
