# frozen_string_literal: true

# The translator unit test (ADR 0006, cerbos/query-plan-adapters#377).
#
# Reads its plans from conformance/wire-fixtures/ and asserts the filter this adapter emits
# against golden/expectations.json. Needs NO PDP, no policy and no database server: the fixtures
# are recorded responses and the models are SQLite in memory.
#
# What it proves, and what it does not. It proves the adapter still emits what it emitted
# yesterday for a planner shape that is pinned independently of it. It says nothing about
# whether that filter returns the rows the PDP allows — that is
# spec/adversarial_conformance_spec.rb, and only the corpus asks the same question of every
# other adapter. So a new SHAPE belongs in conformance/policies/adversarial.yaml, never here.
#
# The recorded value is the emitted relation RENDERED as SQL, which makes ActiveRecord's own
# renderer an input to the bytes; see spec/support/golden_expectations.rb for the header key
# that declares it and the divergence list below for the other CI leg.

# At load, not in a `before` hook: the golden regeneration below runs while this file is being
# read, and rendering a relation needs a connection to quote against. Offline either way — the
# schema is SQLite in memory and the rows come from conformance/seeds.json.
AdversarialModels.establish!

RSpec.describe "translator" do
  # The statement every emitted relation opens with. Recording only the WHERE clause is lossless
  # exactly because this is constant, and that is asserted below rather than assumed.
  PREAMBLE = %(SELECT "adversarial_resources".* FROM "adversarial_resources")

  THROWING_ACTIONS = ConformanceCorpus::THROWING_ACTIONS
  THROWING = THROWING_ACTIONS.map(&:first).freeze

  # `nullRepresentationOmitted` is NOT in that list. Under the default representation this
  # adapter translates `null-eq-missing` into an IS NULL filter, so it carries a golden entry
  # like any other action; its refusal is a property of the flipped option and is asserted on
  # its own below.
  RECORDED_ACTIONS = (ConformanceCorpus.wire_fixture_actions - THROWING).freeze

  # Shapes ActiveRecord 7.1 renders differently from the 8.0 the asset is generated under.
  # Asserted rather than skipped, in BOTH directions: a shape that stops diverging has to fail
  # here so this list shrinks deliberately instead of rotting into a permanent exemption.
  RENDERING_DIFFERS_ON_ACTIVERECORD_71 = %w[].freeze

  def self.emitted_sql(action)
    Cerbos::ActiveRecord.query_plan_to_relation(
      plan: ConformanceCorpus.wire_fixture(action),
      model: AdvResource,
      attributes: CorpusAttributes::ATTRIBUTES
    ).to_sql
  end

  def emitted_sql(action) = self.class.emitted_sql(action)

  # The recorded document for one action: the plan kind the planner folded to, and the WHERE
  # clause for a conditional plan. An unconditional plan carries no `where` at all, so the two
  # cases cannot be confused by an empty string.
  def self.expectation_for(action)
    sql = emitted_sql(action)
    kind = JSON.parse(File.read(
      File.join(ConformanceCorpus::WIRE_FIXTURES_DIR, "#{action}.json")
    )).fetch("filter").fetch("kind")

    entry = {"kind" => kind}
    entry["where"] = sql.delete_prefix("#{PREAMBLE} WHERE ") if sql.start_with?("#{PREAMBLE} WHERE ")
    entry
  end

  # Regeneration is a deliberate act and CI never performs it, so a translator change that moves
  # an emitted filter fails there whatever anyone ran locally, and the diff is the review.
  #
  # A throwing action gets no entry: its message is corpus data, pinned in actions.json and
  # asserted below. Skipping it here is also what keeps regeneration from papering over a
  # misclassification — an action that starts throwing loses its entry rather than gaining a
  # recorded error string.
  if ENV["GOLDEN_UPDATE"] == "1"
    GoldenExpectations.write(RECORDED_ACTIONS.to_h { |action| [action, expectation_for(action)] })
  end

  RECORDED = GoldenExpectations.read.freeze

  describe "the golden expectations" do
    # ADR 0006 requires every wire fixture to be accounted for exactly once.
    it "accounts for every wire fixture exactly once" do
      classified = (RECORDED.keys + THROWING).sort

      # Total, so a fixture with neither a golden entry nor a pinned throw is a failure rather
      # than silence.
      expect(classified).to eq(ConformanceCorpus.wire_fixture_actions)
      # Disjoint, so an action carrying BOTH would satisfy the union above while asserting two
      # contradictory things.
      expect(classified).to eq(classified.uniq)
      # The asset is written sorted, so a translator change reads as the list of shapes it moved.
      expect(RECORDED.keys).to eq(RECORDED.keys.sort)
    end

    # Tripwires. Bump them deliberately: a count that moves unnoticed is how a shape gets
    # dropped from an asset nobody reads end to end.
    it "pins how the corpus divides here" do
      conditional, unconditional = RECORDED.values.partition { |entry|
        entry.fetch("kind") == "KIND_CONDITIONAL"
      }

      expect({
        "conditional" => conditional.size,
        "unconditional" => unconditional.size,
        "throwing" => THROWING_ACTIONS.size
      }).to eq({"conditional" => 179, "unconditional" => 2, "throwing" => 18})
    end

    # The two unconditional folds are the planner's, not this adapter's, and each is pinned
    # elsewhere: in-empty is a conformance action the harness compares, and p-has is the one
    # declared upstream divergence.
    it "names the planner folds the corpus declares" do
      unconditional = RECORDED.select { |_, entry| entry.fetch("kind") != "KIND_CONDITIONAL" }
      expect(unconditional.keys).to eq(%w[in-empty p-has])
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

    # Rule 2 of "When the generator is an input": regeneration refuses under any other major,
    # so `golden:update` cannot present a toolchain swap as a translation change.
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

    # The other half of rule 3. On the leg the asset was NOT generated under, every action
    # outside the list above must still render byte-identically — otherwise the list is stale in
    # the other direction and that leg is proving nothing about the emitted filter.
    it "diverges on exactly the shapes the list names" do
      # A name in the list that is not a recorded action can never fire, on either leg. Checked
      # here because this leg is the one that runs on every push.
      expect(RENDERING_DIFFERS_ON_ACTIVERECORD_71 - RECORDED_ACTIONS).to be_empty

      # The list is EMPTY, and that is a claim about ActiveRecord rather than an unfilled
      # placeholder: 7.1 and 8.0 render every recorded shape byte-identically. Pinned here so a
      # future release that changes one has to be triaged into the list deliberately, and proved
      # on the 7.1 leg by the comparison below.
      expect(RENDERING_DIFFERS_ON_ACTIVERECORD_71).to be_empty

      next if GoldenExpectations.installed_activerecord_major ==
        GoldenExpectations::GOLDEN_ACTIVERECORD_MAJOR

      diverging = RECORDED_ACTIONS.reject { |action|
        self.class.expectation_for(action) == RECORDED.fetch(action)
      }
      expect(diverging.sort).to eq(RENDERING_DIFFERS_ON_ACTIVERECORD_71.sort)
    end
  end

  # --- rules over the WHOLE corpus -----------------------------------------------------------
  #
  # These are what survives an unread regeneration. A regenerated file happily records a filter
  # that collapses a NULL to FALSE; a rule stated over every action does not, and it holds for a
  # corpus action nobody has added yet. Each carries its own anti-vacuity assertion.
  describe "what the emitted statement contains" do
    it "opens every statement with the same preamble" do
      RECORDED_ACTIONS.each do |action|
        expect(emitted_sql(action)).to start_with(PREAMBLE), action
      end
    end

    # The inverse: the recorded WHERE clause reassembles into the exact statement emitted, so
    # recording only the clause loses nothing.
    it "reassembles every recorded clause into the statement it came from" do
      # Only on the leg the asset was generated under. Elsewhere the recorded bytes belong to
      # another renderer, and the divergence list above is what speaks for that leg.
      skip "asset was generated under another ActiveRecord" if
        GoldenExpectations.installed_activerecord_major !=
          GoldenExpectations::GOLDEN_ACTIVERECORD_MAJOR

      RECORDED_ACTIONS.each do |action|
        entry = RECORDED.fetch(action)
        expected = entry.key?("where") ? "#{PREAMBLE} WHERE #{entry.fetch("where")}" : PREAMBLE
        expect(emitted_sql(action)).to eq(expected), action
      end
    end

    # Every LIKE this adapter emits declares its own ESCAPE character. A needle holding %, _ or
    # \ is a corpus shape (like-percent, like-underscore, like-backslash), and a LIKE without an
    # ESCAPE clause returns rows the PDP denies.
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

    # Every qualified identifier names a table this harness declared. A typo in the attribute
    # map produces a column that does not exist, and SQLite only complains when the statement
    # runs — which a unit test never does.
    it "names only tables the schema declares" do
      # Read back from the live connection rather than restated: a list written here could only
      # ever agree with itself.
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

    # The resource table appears in exactly one FROM position. A second one is a self-join, and
    # a correlated subquery that lost its correlation reads as exactly that.
    it "never joins the resource table to itself" do
      RECORDED_ACTIONS.each do |action|
        expect(emitted_sql(action).scan('FROM "adversarial_resources"').size).to eq(1), action
      end

      # Anti-vacuity: the detector reads 2 on a statement that really does name it twice.
      uncorrelated = %(#{PREAMBLE} WHERE EXISTS (SELECT 1 FROM "adversarial_resources"))
      expect(uncorrelated.scan('FROM "adversarial_resources"').size).to eq(2)
    end

    # The actions that bind a temporal literal, named exactly. A timestamp reaching SQL as a
    # STRING would compare lexically and silently disagree with the PDP on any other format, so
    # the set is pinned rather than left to whatever the renderer does today.
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

    it "refuses a throwing action that pins no message" do
      expect { ConformanceCorpus.require_message("synthetic", nil) }
        .to raise_error(/pins no throw message/)
    end
  end

  # The `nullRepresentationOmitted` group. Its refusal is a property of the flipped option, so
  # it is asserted here rather than folded into the list above — and BOTH halves are asserted,
  # because the rejection alone would pass vacuously if the adapter raised for another reason.
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

      # What makes the refusal necessary: under the default representation the same plan
      # translates into an IS NULL filter, and the harness proves those are the rows the PDP
      # denies. Without this half, "it raised" says nothing.
      it "#{action} would otherwise emit an IS NULL filter" do
        expect(RECORDED.fetch(action).fetch("where")).to include("IS NULL")
      end
    end
  end
end
