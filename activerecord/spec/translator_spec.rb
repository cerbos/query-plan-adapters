# frozen_string_literal: true

# Translation-level tests: the public API surface, the plan shapes accepted, the generated
# SQL, and — most importantly — the shapes the adapter must refuse rather than guess at.
#
# These need no PDP; the differential suites cover semantics against a real one.

RSpec.describe Cerbos::ActiveRecord do
  before(:all) do
    SharedModels.establish!
    EdgeCaseModels.establish!
  end

  def field(path) = described_class.field(path)

  def relation(*args, **kwargs) = described_class.relation(*args, **kwargs)

  def conditional(condition)
    {"kind" => "KIND_CONDITIONAL", "condition" => condition}
  end

  def expression(operator, *operands)
    {"expression" => {"operator" => operator, "operands" => operands}}
  end

  def variable(name) = {"variable" => name}

  def value(constant) = {"value" => constant}

  ATTRS = {
    "request.resource.attr.aString" => Cerbos::ActiveRecord.field("a_string"),
    "request.resource.attr.aNumber" => Cerbos::ActiveRecord.field("a_number"),
    "request.resource.attr.aBool" => Cerbos::ActiveRecord.field("a_bool"),
    "request.resource.attr.tags" => Cerbos::ActiveRecord.relation(
      :tags, member_field: "name", fields: {"name" => Cerbos::ActiveRecord.field("name")}
    )
  }.freeze

  def translate(plan, model: SharedResource, attributes: ATTRS, **options)
    described_class.query_plan_to_relation(
      plan: plan, model: model, attributes: attributes, **options
    )
  end

  describe "plan kinds" do
    it "returns every row for an unconditional allow" do
      expect(translate({"kind" => "KIND_ALWAYS_ALLOWED"}).count).to eq(SharedModels::FIXTURES.size)
    end

    it "returns no rows for an unconditional deny" do
      expect(translate({"kind" => "KIND_ALWAYS_DENIED"})).to be_empty
    end

    it "rejects an unrecognised kind" do
      expect { translate({"kind" => "KIND_SOMETHING_ELSE"}) }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /Unrecognised query plan kind/)
    end

    it "rejects a conditional plan with no condition" do
      expect { translate({"kind" => "KIND_CONDITIONAL"}) }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /no condition/)
    end
  end

  describe "accepted plan shapes" do
    let(:condition) do
      expression("eq", variable("request.resource.attr.aString"), value("string"))
    end

    it "accepts a protobuf-style response wrapping the plan in filter" do
      expect(translate({"filter" => conditional(condition)}).pluck(:id))
        .to contain_exactly("507f1f77bcf86cd799439011", "resource3")
    end

    it "accepts symbol keys" do
      plan = {kind: "KIND_CONDITIONAL", condition: {
        expression: {operator: "eq",
                     operands: [{variable: "request.resource.attr.aString"}, {value: "string"}]}
      }}
      expect(translate(plan).count).to eq(2)
    end

    # The official Ruby SDK (https://github.com/cerbos/cerbos-sdk-ruby) is the primary source
    # of plans, so its output types are asserted directly rather than through a stand-in. The
    # differential suites already drive real client responses end to end; this pins the
    # contract as a named test, so an SDK shape change fails here rather than somewhere deep
    # in a harness.
    def sdk_plan(kind, condition)
      Cerbos::Output::PlanResources.new(
        request_id: "test", kind: kind, condition: condition,
        validation_errors: [], metadata: nil
      )
    end

    it "accepts a Cerbos::Output::PlanResources from the official Ruby SDK" do
      plan = sdk_plan(
        :KIND_CONDITIONAL,
        Cerbos::Output::PlanResources::Expression.new(operator: "eq", operands: [
          Cerbos::Output::PlanResources::Expression::Variable.new(name: "request.resource.attr.aString"),
          Cerbos::Output::PlanResources::Expression::Value.new(value: "string")
        ])
      )

      expect(plan).to be_conditional
      expect(translate(plan).pluck(:id))
        .to contain_exactly("507f1f77bcf86cd799439011", "resource3")
    end

    it "maps the SDK's unconditional kinds onto whole and empty relations" do
      expect(translate(sdk_plan(:KIND_ALWAYS_ALLOWED, nil)).count)
        .to eq(SharedModels::FIXTURES.size)
      expect(translate(sdk_plan(:KIND_ALWAYS_DENIED, nil))).to be_empty
    end

    it "resolves a nested SDK lambda over a relation" do
      plan = sdk_plan(
        :KIND_CONDITIONAL,
        Cerbos::Output::PlanResources::Expression.new(operator: "exists", operands: [
          Cerbos::Output::PlanResources::Expression::Variable.new(name: "request.resource.attr.tags"),
          Cerbos::Output::PlanResources::Expression.new(operator: "lambda", operands: [
            Cerbos::Output::PlanResources::Expression.new(operator: "eq", operands: [
              Cerbos::Output::PlanResources::Expression::Variable.new(name: "t.name"),
              Cerbos::Output::PlanResources::Expression::Value.new(value: "public")
            ]),
            Cerbos::Output::PlanResources::Expression::Variable.new(name: "t")
          ])
        ])
      )

      expect(translate(plan).pluck(:id))
        .to contain_exactly("507f1f77bcf86cd799439011", "resource4", "resource5")
    end

    it "accepts any object exposing kind and condition, for non-SDK clients" do
      expression_node = Struct.new(:operator, :operands)
      variable_node = Struct.new(:name)
      value_node = Struct.new(:value)
      plan = Struct.new(:kind, :condition).new(
        :KIND_CONDITIONAL,
        expression_node.new("eq", [
          variable_node.new("request.resource.attr.aString"), value_node.new("string")
        ])
      )
      expect(translate(plan).count).to eq(2)
    end

    it "rejects something that is not a plan" do
      expect { translate("nope") }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /Cannot read a query plan/)
    end
  end

  describe "operand order" do
    # The planner preserves policy source order, so `1 < R.attr.aNumber` arrives value-first.
    # Adapters that assumed a column always comes first, and swapped operands to restore that,
    # inverted the comparison (cerbos/query-plan-adapters#257).
    it "keeps a value-first comparison in source order" do
      sql = translate(conditional(
        expression("lt", value(1), variable("request.resource.attr.aNumber"))
      )).to_sql

      expect(sql).to include(%(1 < "shared_resources"."a_number"))
      expect(translate(conditional(
        expression("lt", value(1), variable("request.resource.attr.aNumber"))
      )).pluck(:a_number).sort).to eq([2, 3, 4, 5])
    end
  end

  describe "LIKE escaping" do
    it "escapes wildcards in a literal needle and declares an ESCAPE character" do
      sql = translate(conditional(
        expression("startsWith", variable("request.resource.attr.aString"), value("100%_x"))
      )).to_sql

      expect(sql).to include("ESCAPE")
      expect(sql).to include('100\%\_x%')
    end

    it "escapes a backslash needle rather than passing it through as the escape character" do
      sql = translate(conditional(
        expression("endsWith", variable("request.resource.attr.aString"), value("\\"))
      )).to_sql

      expect(sql).to include("%\\\\")
    end

    it "escapes a column-valued needle at query time" do
      sql = translate(conditional(
        expression("contains",
          variable("request.resource.attr.aString"),
          variable("request.resource.attr.aString"))
      )).to_sql

      expect(sql.scan("REPLACE").size).to eq(4)
    end
  end

  describe "unmapped attributes" do
    it "raises rather than guessing a column" do
      expect {
        translate(conditional(
          expression("eq", variable("request.resource.attr.notMapped"), value(1))
        ))
      }.to raise_error(Cerbos::ActiveRecord::UnmappedAttributeError, /notMapped/)
    end

    it "raises for a member field the relation does not declare" do
      expect {
        translate(conditional(expression("exists",
          variable("request.resource.attr.tags"),
          expression("lambda", expression("eq", variable("t.colour"), value("red")), variable("t")))))
      }.to raise_error(Cerbos::ActiveRecord::UnmappedAttributeError, /colour/)
    end

    it "raises when a collection is used where a scalar is required" do
      expect {
        translate(conditional(
          expression("eq", variable("request.resource.attr.tags"), value("x"))
        ))
      }.to raise_error(Cerbos::ActiveRecord::UnmappedAttributeError, /relation/)
    end

    it "raises when a macro is given something that is not a collection" do
      expect {
        translate(conditional(expression("exists",
          variable("request.resource.attr.aString"),
          expression("lambda", value(true), variable("t")))))
      }.to raise_error(Cerbos::ActiveRecord::UnmappedAttributeError, /exists needs a collection/)
    end
  end

  describe "association shapes it refuses to guess at" do
    it "raises for a polymorphic belongs_to" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq", variable("a"), value("x"))),
          model: EdgeComment,
          attributes: {"a" => field("commentable.name")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /polymorphic/)
    end

    it "raises for a scoped association" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeDocument,
          attributes: {"c" => relation(:approved_comments, member_field: "body")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /carries a scope/)
    end

    it "raises for a collection in a dotted scalar path" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq", variable("a"), value("x"))),
          model: EdgeDocument,
          attributes: {"a" => field("comments.body")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /collection association/)
    end

    it "raises for an association that does not exist" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeDocument,
          attributes: {"c" => relation(:missing_things)}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /no association/)
    end

    it "discriminates on the type column for an `as:` association" do
      # Without the type condition the subquery would match comments belonging to a
      # different owner class that happens to share an id.
      sql = described_class.query_plan_to_relation(
        plan: conditional(expression("in", value("hello"), variable("c"))),
        model: EdgeDocument,
        attributes: {"c" => relation(:comments, member_field: "body")}
      ).to_sql

      expect(sql).to include("commentable_type")
      expect(sql).to include("EdgeDocument")
    end
  end

  describe "unsupported operators" do
    it "raises for an operator it does not implement" do
      expect {
        translate(conditional(
          expression("matches", variable("request.resource.attr.aString"), value("^s"))
        ))
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /Unsupported operator: matches/)
    end

    it "raises for a collection used as a condition" do
      expect {
        translate(conditional(expression("filter",
          variable("request.resource.attr.tags"),
          expression("lambda",
            expression("eq", variable("t.name"), value("public")), variable("t")))))
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /not to a boolean/)
    end

    it "raises for a sub-microsecond timestamp literal" do
      # The planner emits nanoseconds for now(); binding that through ActiveRecord would
      # silently truncate it and move the instant the policy compares against.
      expect { Cerbos::ActiveRecord::Timestamps.parse("2026-08-04T08:55:39.185020547Z") }
        .to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /sub-microsecond/)
    end

    it "accepts trailing zeroes beyond microsecond precision" do
      expect(Cerbos::ActiveRecord::Timestamps.parse("2024-06-01T00:00:00.123456000Z"))
        .to eq(Time.utc(2024, 6, 1, 0, 0, 0, 123456))
    end

    it "raises for timestamp() over a column holding a formatted string" do
      expect {
        translate(conditional(expression("lt",
          expression("timestamp", variable("request.resource.attr.aString")),
          expression("timestamp", value("2025-01-01T00:00:00Z")))))
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /must map to a datetime column/)
    end

    it "rejects an invalid timestamp literal" do
      expect { Cerbos::ActiveRecord::Timestamps.parse("2025-13-01") }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /Invalid RFC-3339/)
    end
  end

  describe "operator overrides" do
    it "takes precedence over the default translation" do
      relation = translate(
        conditional(expression("matches", variable("request.resource.attr.aString"), value("^str"))),
        operator_overrides: {
          "matches" => ->(column, _pattern) { Arel::Nodes::Equality.new(column, Arel::Nodes.build_quoted("string")) }
        }
      )
      expect(relation.count).to eq(2)
    end

    it "refuses to override a structural operator" do
      expect {
        translate(conditional(expression("eq", value(1), value(1))),
          operator_overrides: {"exists" => ->(*) {}})
      }.to raise_error(ArgumentError, /cannot be overridden/)
    end
  end

  describe "mapping helpers" do
    it "requires a path" do
      expect { described_class.field(nil) }.to raise_error(ArgumentError, /path is required/)
    end

    it "requires an association" do
      expect { described_class.relation(nil) }.to raise_error(ArgumentError, /association is required/)
    end

    it "rejects a nested field that is not a mapping" do
      expect { described_class.relation(:tags, fields: {"name" => "name"}) }
        .to raise_error(ArgumentError, /must be a field or relation mapping/)
    end
  end

  describe "generated SQL" do
    it "aliases each correlated subquery so nesting cannot self-correlate" do
      sql = translate(conditional(expression("exists",
        variable("request.resource.attr.tags"),
        expression("lambda",
          expression("eq", variable("t.name"), value("public")), variable("t"))))).to_sql

      expect(sql).to match(/EXISTS \(SELECT 1 FROM/)
      expect(sql).to include("cerbos_shared_resource_tags_1")
      expect(sql).to include("cerbos_shared_tags_2")
    end

    it "resolves a dotted path as a correlated scalar subquery, not a join" do
      sql = described_class.query_plan_to_relation(
        plan: conditional(expression("eq", variable("a"), value("Ada"))),
        model: EdgeDocument,
        attributes: {"a" => field("author.name")}
      ).to_sql

      expect(sql).to include("(SELECT")
      expect(sql).not_to include("JOIN")
    end
  end
end
