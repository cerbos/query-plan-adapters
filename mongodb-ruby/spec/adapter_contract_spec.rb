# frozen_string_literal: true

require "bson"

# The caller-supplied contract: what an application passes in, which the corpus structurally
# cannot vary. The conformance harness replays every case through ONE mapper, so a value_parser,
# a callable mapper, an unmapped reference, the mapper's own validation, the per-call null
# representation and the plan shapes the adapter accepts have no case spelling (CLAUDE.md, "What
# a translator unit test may pin", kinds 1 and 2). So does the boundary between a refusal and a
# mapping mistake, which the harness's `unsupported` assertion rests on. Offline: nothing opens a
# connection.
RSpec.describe "adapter contract" do
  def plan(condition) = {"kind" => "KIND_CONDITIONAL", "condition" => condition}

  def var(name) = {"variable" => name}

  def val(value) = {"value" => value}

  def expr(operator, *operands) = {"expression" => {"operator" => operator, "operands" => operands}}

  def filter(condition, mapper = {}, **options)
    Cerbos::MongoDB.query_plan_to_filter(plan: plan(condition), mapper: mapper, **options).filter
  end

  describe "the result" do
    it "reports each plan kind and a filter that is safe to run as-is" do
      allowed = Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_ALWAYS_ALLOWED"})
      denied = Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_ALWAYS_DENIED"})
      conditional = Cerbos::MongoDB.query_plan_to_filter(
        plan: plan(var("request.resource.attr.aBool")), mapper: {"request.resource.attr.aBool" => {field: "aBool"}}
      )

      expect([allowed.kind, allowed.always_allowed?, allowed.filter]).to eq(["KIND_ALWAYS_ALLOWED", true, {}])
      expect([denied.kind, denied.always_denied?, denied.filter]).to eq(["KIND_ALWAYS_DENIED", true, {"$expr" => false}])
      expect([conditional.kind, conditional.conditional?]).to eq(["KIND_CONDITIONAL", true])
      expect(conditional.filter).to eq({"aBool" => {"$eq" => true}})
    end
  end

  describe "the refusal type" do
    # The harness passes an `unsupported` ledger entry on any Cerbos::MongoDB::Error except a
    # MapperError. These pin both sides of that line.
    it "refuses a shape it cannot express with an Error that is not a MapperError" do
      golden = ConformanceCorpus.golden("regex/matches/lookahead-from-principal")
      expect { Cerbos::MongoDB.query_plan_to_filter(plan: golden.fetch("plan"), mapper: CorpusMapper::MAPPER) }
        .to raise_error(Cerbos::MongoDB::UnsupportedError) { |error| expect(error).not_to be_a(Cerbos::MongoDB::MapperError) }
    end

    it "reports a mapping mistake as a MapperError, not as a refusal" do
      golden = ConformanceCorpus.golden("string/equals/case-sensitive")
      expect { Cerbos::MongoDB.query_plan_to_filter(plan: golden.fetch("plan")) }
        .to raise_error(Cerbos::MongoDB::MapperError)
    end

    it "does not let a caller mutate the unconditional filters for the next call" do
      expect(Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_ALWAYS_ALLOWED"}).filter).to be_frozen
      expect(Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_ALWAYS_DENIED"}).filter).to be_frozen
    end
  end

  # The README's mapping-hazard contract rests on ONE structural fact: this adapter builds no
  # subquery. A relation is a path inside the same document, so the filter and the application read
  # the same document. No corpus case can state that (a case asks which documents come back), so it
  # is pinned against the source, which is total over mapper shapes where a walk of emitted filters
  # is not (cerbos/query-plan-adapters#323).
  it "emits no $lookup and reaches no second collection" do
    forbidden = /\$lookup|\$graphLookup|\$unionWith|\.aggregate\b/
    sources = Dir[File.expand_path("../lib/**/*.rb", __dir__)].sort
    # Guard the guard: a scan that found no files, or lost the entry point, would pass vacuously.
    expect(sources.map { |path| File.basename(path) }).to include("mongodb.rb", "translator.rb")
    # Prose about the guard is not a violation of it, so comments come off first.
    offending = sources.flat_map { |path|
      File.readlines(path, encoding: "UTF-8").each_with_index.filter_map { |line, index|
        "#{File.basename(path)}:#{index + 1}" if line.sub(/#.*$/, "").match?(forbidden)
      }
    }
    expect(offending).to be_empty
  end

  describe "the plans it reads" do
    let(:condition) { expr("eq", var("request.resource.attr.aString"), val("x")) }
    let(:expected) { {"aString" => {"$eq" => "x"}} }
    let(:mapper) { {"request.resource.attr.aString" => {field: "aString"}} }

    it "reads a PlanResources response with the plan under `filter`, and one without" do
      expect(Cerbos::MongoDB.query_plan_to_filter(plan: {"filter" => plan(condition)}, mapper: mapper).filter).to eq(expected)
      expect(Cerbos::MongoDB.query_plan_to_filter(plan: plan(condition), mapper: mapper).filter).to eq(expected)
    end

    it "reads symbol keys and the SDK's output objects" do
      symbolic = {kind: :KIND_CONDITIONAL, condition: {expression: {operator: "eq", operands: [{variable: "request.resource.attr.aString"}, {value: "x"}]}}}
      expect(Cerbos::MongoDB.query_plan_to_filter(plan: symbolic, mapper: mapper).filter).to eq(expected)

      sdk_plan = Struct.new(:kind, :condition).new(
        :KIND_CONDITIONAL,
        Struct.new(:operator, :operands).new("eq", [Struct.new(:name).new("request.resource.attr.aString"), Struct.new(:value).new("x")])
      )
      expect(Cerbos::MongoDB.query_plan_to_filter(plan: sdk_plan, mapper: mapper).filter).to eq(expected)
    end

    it "refuses a plan it cannot read" do
      expect { Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_NONSENSE"}) }
        .to raise_error(Cerbos::MongoDB::InvalidPlanError, /Unrecognised query plan kind/)
      expect { Cerbos::MongoDB.query_plan_to_filter(plan: {"kind" => "KIND_CONDITIONAL"}) }
        .to raise_error(Cerbos::MongoDB::InvalidPlanError, /has no condition/)
      expect { Cerbos::MongoDB.query_plan_to_filter(plan: 42) }
        .to raise_error(Cerbos::MongoDB::InvalidPlanError, /Cannot read a query plan/)
      expect { filter({"nonsense" => 1}) }.to raise_error(Cerbos::MongoDB::InvalidPlanError, /Unrecognised query plan operand/)
      expect { filter(expr("frobnicate", val(1))) }.to raise_error(Cerbos::MongoDB::UnsupportedError, "Unsupported operator: frobnicate")
    end

    # The gRPC SDK delivers every plan number as a Float (protobuf's Value holds a double), and
    # the JSON wire delivers an integral one as an Integer. Both have to reach the same answer.
    it "treats an integral Float index as the integer it is" do
      mapper = {"request.resource.attr.list" => {field: "list"}}
      from_json = filter(expr("eq", expr("index", var("request.resource.attr.list"), val(1)), val("x")), mapper)
      from_grpc = filter(expr("eq", expr("index", var("request.resource.attr.list"), val(1.0)), val("x")), mapper)
      expect(from_grpc).to eq(from_json)
      expect(JSON.generate(from_grpc)).to include('"$arrayElemAt":["$list",1]')
    end

    # BSON has no integer wider than 64 bits, and the planner's number was a double all along.
    it "keeps an integral literal beyond int64 as the double it was on the wire" do
      mapper = {"request.resource.attr.aDouble" => {field: "aDouble"}}
      emitted = filter(expr("gt", var("request.resource.attr.aDouble"), val(-10_000_000_000_000_000_000)), mapper)
      bound = emitted.fetch("aDouble").fetch("$gt")
      expect(bound).to be_a(Float)
      expect { BSON::Document.new(emitted).to_bson }.not_to raise_error
    end
  end

  describe "the mapper" do
    it "maps a field" do
      mapper = {"request.resource.attr.title" => {field: "doc.title"}}
      expect(filter(expr("eq", var("request.resource.attr.title"), val("a")), mapper)).to eq({"doc.title" => {"$eq" => "a"}})
    end

    # Before cerbos/query-plan-adapters#492 an unmapped reference was used verbatim as a document
    # path, and `$ne` or `$nor` over a path no document stores matched every document.
    describe "an unmapped reference" do
      let(:negated) { expr("ne", var("request.resource.attr.status"), val("x")) }
      let(:message) { /No mapper entry for request.resource.attr.status: an unmapped reference is not used verbatim/ }

      it "is refused when the Hash has no entry for it" do
        expect { filter(negated, {"request.resource.attr.title" => {field: "title"}}) }
          .to raise_error(Cerbos::MongoDB::MapperError, message)
      end

      it "is refused when a callable returns no entry for it" do
        expect { filter(negated, ->(_) {}) }.to raise_error(Cerbos::MongoDB::MapperError, message)
      end

      it "is refused under the default mapper" do
        expect { filter(negated) }.to raise_error(Cerbos::MongoDB::MapperError, message)
      end

      it "is refused as the collection a macro ranges over" do
        macro = expr("exists", var("request.resource.attr.status"),
          expr("lambda", expr("eq", var("t.name"), val("a")), var("t")))
        expect { filter(macro, {"request.resource.attr.title" => {field: "title"}}) }
          .to raise_error(Cerbos::MongoDB::MapperError, message)
      end

      # The opt-in, for a caller whose documents really are shaped like the plan path.
      it "keeps the plan path when an entry naming no field declares it" do
        expect(filter(negated, {"request.resource.attr.status" => {}}))
          .to eq({"request.resource.attr.status" => {"$ne" => "x"}})
      end
    end

    it "accepts a callable in place of a Hash, with string or symbol keys" do
      callable = ->(reference) { {"field" => reference.delete_prefix("request.resource.attr.")} }
      expect(filter(expr("eq", var("request.resource.attr.title"), val("a")), callable)).to eq({"title" => {"$eq" => "a"}})
    end

    # A misspelt `nullable` silently ignored would drop a guard, which is an over-grant.
    it "refuses an unknown or ill-typed mapper key instead of ignoring it" do
      condition = expr("eq", var("request.resource.attr.title"), val("a"))
      expect { filter(condition, {"request.resource.attr.title" => {feild: "t"}}) }
        .to raise_error(Cerbos::MongoDB::MapperError, /unknown keys \[:feild\]/)
      expect { filter(condition, {"request.resource.attr.title" => {nullable: "yes"}}) }
        .to raise_error(Cerbos::MongoDB::MapperError, /nullable must be true or false/)
      expect { filter(condition, {"request.resource.attr.title" => {value_type: :uuid}}) }
        .to raise_error(Cerbos::MongoDB::MapperError, /value_type must be one of/)
      expect { filter(condition, {"request.resource.attr.title" => {relation: {name: "t", type: :several}}}) }
        .to raise_error(Cerbos::MongoDB::MapperError, /relation type must be :one or :many/)
      expect { filter(condition, ->(_) { {nulable: true} }) }
        .to raise_error(Cerbos::MongoDB::MapperError, /unknown keys \[:nulable\]/)
      expect { filter(condition, 42) }.to raise_error(Cerbos::MongoDB::MapperError, /must be a Hash or respond to #call/)
    end

    # The application's key is usually an ObjectId, and the plan carries it as a string.
    it "applies a value_parser to each constant compared with the field" do
      id = BSON::ObjectId.new
      mapper = {"request.resource.id" => {field: "_id", value_parser: ->(value) { BSON::ObjectId.from_string(value) }}}

      expect(filter(expr("eq", var("request.resource.id"), val(id.to_s)), mapper)).to eq({"_id" => {"$eq" => id}})
      expect(filter(expr("in", var("request.resource.id"), val([id.to_s])), mapper)).to eq({"_id" => {"$in" => [id]}})
      # And to the value-first spelling, which is mirrored rather than read positionally.
      expect(filter(expr("eq", val(id.to_s), var("request.resource.id")), mapper)).to eq({"_id" => {"$eq" => id}})
    end

    it "gives a relation's element fields their own value_parser" do
      mapper = {"request.resource.attr.owner" => {
        relation: {name: "owner", type: :one, fields: {"id" => {field: "_id", value_parser: ->(value) { "parsed:#{value}" }}}}
      }}
      expect(filter(expr("eq", var("request.resource.attr.owner.id"), val("7")), mapper)).to eq({"owner._id" => {"$eq" => "parsed:7"}})
    end

    # A declared scalar type settles an equality against a constant of another type without
    # reaching the server: CEL's heterogeneous equality is false (and its negation true).
    it "folds an equality against a constant of another declared type" do
      mapper = {"request.resource.attr.n" => {field: "n", value_type: :number}}
      expect(filter(expr("eq", var("request.resource.attr.n"), val("5")), mapper)).to eq({"$expr" => {"$eq" => [false, true]}})
      expect(filter(expr("ne", var("request.resource.attr.n"), val("5")), mapper)).to eq({"$expr" => {"$eq" => [true, true]}})
      expect(filter(expr("eq", var("request.resource.attr.n"), val(5)), mapper)).to eq({"n" => {"$eq" => 5}})
    end
  end

  describe "the null attribute representation" do
    let(:null_eq) { expr("eq", var("request.resource.attr.x"), val(nil)) }

    let(:mapper) { {"request.resource.attr.x" => {field: "x"}} }

    it "matches null under :explicit and refuses it under :omitted" do
      expect(filter(null_eq, mapper)).to eq({"$and" => [{"x" => {"$exists" => true}}, {"x" => {"$eq" => nil}}]})
      expect { filter(null_eq, mapper, null_attribute_representation: :omitted) }
        .to raise_error(Cerbos::MongoDB::UnsupportedError, /missing-attribute error/)
    end

    # A null inside a list constructor is a NULL value only under the explicit convention.
    it "refuses a null inside a list literal under :omitted" do
      condition = expr("in", var("request.resource.attr.x"), expr("list", val("a"), val(nil)))
      expect { filter(condition, null_attribute_representation: :omitted) }
        .to raise_error(Cerbos::MongoDB::UnsupportedError, /a null literal in a collection or struct constructor/)
    end

    it "refuses a representation it does not know" do
      expect { filter(null_eq, null_attribute_representation: "omitted") }
        .to raise_error(ArgumentError, /must be :explicit or :omitted/)
    end

    # `nullable: true` states per field what the call option states for every undeclared one.
    it "lets a nullable field keep a stored null out of every comparison" do
      mapper = {"request.resource.attr.x" => {field: "x", nullable: true}}
      expect(filter(expr("ne", var("request.resource.attr.x"), val("a")), mapper))
        .to eq({"$and" => [{"x" => {"$ne" => nil}}, {"x" => {"$ne" => "a"}}]})
      # A negation CEL is certain to evaluate keeps the guard outside its $nor, so a stored null
      # is denied under both polarities.
      expect(filter(expr("not", expr("eq", var("request.resource.attr.x"), val("a"))), mapper))
        .to eq({"$and" => [{"x" => {"$ne" => nil}}, {"$nor" => [{"$and" => [{"x" => {"$ne" => nil}}, {"x" => {"$eq" => "a"}}]}]}]})
    end
  end

  # Under :omitted a NULL field sends no attribute, so CEL denies every comparison against it,
  # while MongoDB's $ne and $nor match a document the path is absent from or null in. An entry
  # that declares no `nullable` therefore takes the call-level convention as its default, exactly
  # as if it declared `nullable: true` (#493). A caller-supplied argument no corpus case can vary:
  # the harness runs one mapping, under :explicit.
  describe "omitted: an undeclared entry is nullable" do
    def corpus_filter(id, mapper = CorpusMapper::MAPPER, **options)
      Cerbos::MongoDB.query_plan_to_filter(plan: ConformanceCorpus.golden(id).fetch("plan"), mapper: mapper, **options).filter
    end

    def omitted(id, mapper = CorpusMapper::MAPPER) = corpus_filter(id, mapper, null_attribute_representation: :omitted)

    def top_level_conjuncts(filter)
      filter.key?("$and") ? filter["$and"].flat_map { |clause| top_level_conjuncts(clause) } : [filter]
    end

    # The corpus mapping declares the scalars it seeds as missing (aString, aNumber, aBool)
    # nullable, so each test strips the declaration from the field it reads, leaving the entry for
    # the call-level default to decide. `owner` maps the nullable aOptionalString field without
    # declaring it, as a caller on the explicit convention would.
    def undeclared(field)
      CorpusMapper::MAPPER.to_h { |key, config| [key, (config[:field] == field) ? config.except(:nullable) : config] }
    end

    # Every entry, and every relation field beneath it, declaring `nullable`.
    def declaring_nullable(config, nullable)
      declared = config.merge(nullable: nullable)
      relation = config[:relation]
      return declared unless relation&.key?(:fields)

      declared.merge(relation: relation.merge(fields: relation[:fields].transform_values { |field| declaring_nullable(field, nullable) }))
    end

    def guard(field) = JSON.generate({field => {"$ne" => nil}})

    [
      ["string/equals/case-sensitive", "aString"],
      ["comparison/not-equals/value-first", "aString"],
      ["null/not-equals/explicit-null-against-literal", "aOptionalString"]
    ].each do |(id, field)|
      it "#{id} requires #{field} to be present and non-null, outside any negation" do
        mapper = undeclared(field)
        expect(top_level_conjuncts(corpus_filter(id, mapper))).not_to include({field => {"$ne" => nil}})
        expect(top_level_conjuncts(omitted(id, mapper))).to include({field => {"$ne" => nil}})
      end
    end

    # A `not` over an undeclared entry is translated as one over a declared-nullable entry: the
    # guard sits outside the $nor, which would otherwise readmit the missing documents. A negated
    # `&&` is pushed down to its leaves first, so each leaf carries its own guard.
    [
      ["null/equals/negated-explicit-null-against-literal", "aOptionalString"],
      ["null/in/negated-explicit-null-in-literal-list", "aOptionalString"],
      ["logic/not/over-and", "aString"]
    ].each do |(id, field)|
      it "#{id} guards #{field} outside the negation" do
        mapper = undeclared(field)
        expect(JSON.generate(corpus_filter(id, mapper))).not_to include(guard(field))
        expect(JSON.generate(omitted(id, mapper))).to include(guard(field))
      end
    end

    it "a callable mapper takes the default too" do
      mapper = undeclared("aString")
      callable = ->(key) { mapper[key] }
      %w[string/equals/case-sensitive logic/not/over-and].each do |id|
        expect(omitted(id, callable)).to eq(omitted(id, mapper))
      end
    end

    # `tagNames` projects the `name` field of each `tags` element, which declares no `nullable`.
    it "a relation's element field takes the default too" do
      id = "collection/exists/scalar-list-equals"
      expect(JSON.generate(corpus_filter(id))).not_to include(guard("name"))
      expect(JSON.generate(omitted(id))).to include(guard("name"))
    end

    # `nullable: false` is the per-entry opt-out: it asserts the field is always stored.
    it "declaring nullable: false keeps the explicit translation" do
      mapper = CorpusMapper::MAPPER.transform_values { |config| declaring_nullable(config, false) }
      %w[string/equals/case-sensitive comparison/not-equals/value-first logic/not/over-and].each do |id|
        expect(omitted(id, mapper)).to eq(corpus_filter(id, mapper))
      end
    end

    it "leaves the :explicit translation of an undeclared entry as it was" do
      mapper = {"request.resource.attr.x" => {field: "x"}}
      expect(filter(expr("ne", var("request.resource.attr.x"), val("a")), mapper)).to eq({"x" => {"$ne" => "a"}})
      expect(filter(expr("ne", var("request.resource.attr.x"), val("a")), mapper, null_attribute_representation: :omitted))
        .to eq({"$and" => [{"x" => {"$ne" => nil}}, {"x" => {"$ne" => "a"}}]})
    end
  end
end
