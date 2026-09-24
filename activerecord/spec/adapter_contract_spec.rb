# frozen_string_literal: true

# Tests for what the corpus cannot ask. Read CLAUDE.md, "What a translator unit test may pin",
# before adding here. Three kinds:
#
# * Caller-supplied arguments (kind 2, permanent): operator overrides, mapper forms, the
#   per-call null representation, and ActiveRecord model shapes (through, scoped, STI,
#   polymorphic, composite keys). The corpus uses one mapping, so it cannot vary these.
# * Plans the planner never emits: empty `and`, wrong operand count, unknown kind. The adapter
#   accepts plans from any source, so these must fail closed.
# * Cast and division refusals the corpus also covers, kept here to pin which operand type
#   raises. Not a substitute for the corpus cases.
#
# Anything a conformance golden already decides does not belong here. New shapes go in the
# corpus.
#
# No PDP or database server needed (SQLite in memory).

RSpec.describe Cerbos::ActiveRecord do
  # Memoized: the schema is built once.
  before do
    ConformanceStore.establish!
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
    "request.resource.attr.aString" => described_class.field("a_string"),
    "request.resource.attr.aNumber" => described_class.field("a_number"),
    "request.resource.attr.aBool" => described_class.field("a_bool"),
    "request.resource.attr.tags" => described_class.relation(
      :tags, member_field: "name", fields: {"name" => described_class.field("name")}
    )
  }.freeze

  def translate(plan, model: AdvResource, attributes: ATTRS, **options)
    described_class.query_plan_to_relation(
      plan: plan, model: model, attributes: attributes, **options
    )
  end

  describe "plan kinds" do
    it "returns every row for an unconditional allow" do
      expect(translate({"kind" => "KIND_ALWAYS_ALLOWED"}).count).to eq(ConformanceCorpus::SEEDS.size)
    end

    it "returns no rows for an unconditional deny" do
      expect(translate({"kind" => "KIND_ALWAYS_DENIED"})).to be_empty
    end

    it "rejects an unrecognised kind" do
      expect { translate({"kind" => "KIND_SOMETHING_ELSE"}) }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /Unrecognised query plan kind/)
    end

    # Reading only the expected positions would drop the extra operand and widen the filter.
    it "rejects an operator that carries the wrong number of operands" do
      expect {
        translate(conditional(expression("eq",
          variable("request.resource.attr.aString"), value("string"), value("extra"))))
      }.to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /eq takes 2 operands/)

      expect {
        translate(conditional(expression("gt",
          expression("size", variable("request.resource.attr.aString"), value("extra")),
          value(1))))
      }.to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /size takes 1 operands/)
    end

    # An empty `and` is TRUE and would allow every row.
    it "rejects an and or an or with no operands" do
      %w[and or].each do |operator|
        expect { translate(conditional(expression(operator))) }
          .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /has no operands/)
      end
    end

    it "rejects a conditional plan with no condition" do
      expect { translate({"kind" => "KIND_CONDITIONAL"}) }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /no condition/)
    end
  end

  # Every plan format must give the same answer. The protobuf and duck-typed formats are not
  # used anywhere else. Each case compares with the Hash form, not a hand-written id list.
  describe "accepted plan shapes" do
    let(:condition) do
      expression("eq", variable("request.resource.attr.aString"), value("one"))
    end

    let(:reference_ids) { translate(conditional(condition)).pluck(:id).sort }

    # An empty or total reference would make every comparison below pass.
    it "has a reference answer that is neither empty nor total" do
      expect(reference_ids).not_to be_empty
      expect(reference_ids.size).to be < ConformanceCorpus::SEEDS.size
    end

    it "accepts a protobuf-style response wrapping the plan in filter" do
      expect(translate({"filter" => conditional(condition)}).pluck(:id).sort)
        .to eq(reference_ids)
    end

    it "accepts symbol keys" do
      plan = {kind: "KIND_CONDITIONAL", condition: {
        expression: {operator: "eq",
                     operands: [{variable: "request.resource.attr.aString"}, {value: "one"}]}
      }}
      expect(translate(plan).pluck(:id).sort).to eq(reference_ids)
    end

    # Uses the real Ruby SDK types, so an SDK change fails here with a clear name.
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
          Cerbos::Output::PlanResources::Expression::Value.new(value: "one")
        ])
      )

      expect(plan).to be_conditional
      expect(translate(plan).pluck(:id).sort).to eq(reference_ids)
    end

    it "maps the SDK's unconditional kinds onto whole and empty relations" do
      expect(translate(sdk_plan(:KIND_ALWAYS_ALLOWED, nil)).count)
        .to eq(ConformanceCorpus::SEEDS.size)
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

      hash_form = conditional(expression("exists",
        variable("request.resource.attr.tags"),
        expression("lambda",
          expression("eq", variable("t.name"), value("public")),
          variable("t"))))
      lambda_ids = translate(hash_form).pluck(:id).sort
      expect(lambda_ids).not_to be_empty
      expect(lambda_ids.size).to be < ConformanceCorpus::SEEDS.size

      expect(translate(plan).pluck(:id).sort).to eq(lambda_ids)
    end

    it "accepts any object exposing kind and condition, for non-SDK clients" do
      expression_node = Struct.new(:operator, :operands)
      variable_node = Struct.new(:name)
      value_node = Struct.new(:value)
      plan = Struct.new(:kind, :condition).new(
        :KIND_CONDITIONAL,
        expression_node.new("eq", [
          variable_node.new("request.resource.attr.aString"), value_node.new("one")
        ])
      )
      expect(translate(plan).pluck(:id).sort).to eq(reference_ids)
    end

    it "rejects something that is not a plan" do
      expect { translate("nope") }
        .to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /Cannot read a query plan/)
    end
  end

  describe "casts that SQL cannot make the way CEL does" do
    it "raises for int() over a string column" do
      # CEL errors on '1junk' and denies; SQLite casts it to 1 and would keep the row.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("gt",
            expression("int", variable("s")), value(0))),
          model: EdgeDocument, attributes: {"s" => field("title")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError,
        /int\(\) applied to a :string column/)
    end

    # Each operand type has its own message, so it is clear which one refused (#326).
    it "raises for int() over a double column, naming the rounding difference" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq",
            expression("int", variable("d")), value(0))),
          model: EdgeDocument, attributes: {"d" => field("score")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError,
        /int\(\) applied to a double column is not portable/)
    end

    it "raises for double() over a string column" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("gt",
            expression("double", variable("s")), value(0.5))),
          model: EdgeDocument, attributes: {"s" => field("title")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /double\(\) needs a numeric column/)
    end

    it "accepts int() over an integer column, where the cast has nothing to do" do
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("gt", expression("int", variable("n")), value(0))),
        model: EdgeDocument, attributes: {"n" => field("n")}
      )
      expect(relation.order(:id).pluck(:title)).to eq(%w[two])
    end
  end

  describe "membership between two columns under each NULL convention" do
    let(:plan) do
      conditional(expression("in", variable("a"), expression("list", variable("b"))))
    end

    let(:mapping) { {"a" => field("title"), "b" => field("n")} }

    it "treats two explicit nulls as equal" do
      sql = described_class.query_plan_to_relation(
        plan: plan, model: EdgeDocument, attributes: mapping
      ).to_sql
      expect(sql).to match(/IS NULL AND .*IS NULL/)
    end

    it "does not treat two omitted attributes as equal" do
      # Two missing attributes make CEL error and deny. Plain SQL equality gives UNKNOWN,
      # which also denies.
      sql = described_class.query_plan_to_relation(
        plan: plan, model: EdgeDocument, attributes: mapping,
        null_attribute_representation: :omitted
      ).to_sql
      expect(sql).not_to match(/IS NULL AND .*IS NULL/)
    end
  end

  describe "membership with a column inside the list" do
    # `null in [R.attr.x]` is true when the column is null. `NULL IN (x)` would always be
    # UNKNOWN.
    it "translates a null needle against a list holding a column" do
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("in", value(nil),
          expression("list", variable("s")))),
        model: EdgeDocument,
        attributes: {"s" => field("title")}
      )
      expect(relation.to_sql).to include('"title" IS NULL')
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

  # For `R.attr.parent.children`, a missing parent is a missing path and CEL denies. A naive
  # subquery cannot tell that from a parent with no children, so `all`, `!exists` and counts
  # would return denied rows (#309). The nested `fields:` mapping marks the parent hop.
  # The corpus covers this with the `relation/<operator>/*to-one-chain` cases over
  # `mainCategory`; these tests check each polarity.
  describe "a collection reached through a parent hop" do
    CHAIN_ATTRIBUTES = {
      "request.resource.attr.tag" => described_class.relation(:tags, fields: {
        "labels" => described_class.relation(
          :labels, fields: {"name" => described_class.field("name")}
        ),
        "labelNames" => described_class.relation(:labels, member_field: "name"),
        "name" => described_class.field("name")
      })
    }.freeze

    def chain_titles(condition)
      Cerbos::ActiveRecord.query_plan_to_relation(
        plan: conditional(condition), model: EdgeDocument, attributes: CHAIN_ATTRIBUTES
      ).order(:id).pluck(:title)
    end

    def label_lambda(operator)
      expression(operator,
        variable("request.resource.attr.tag.labels"),
        expression("lambda",
          expression("eq", variable("l.name"), value("urgent")), variable("l")))
    end

    it "keeps the row without a parent out of a positive existential" do
      expect(chain_titles(label_lambda("exists"))).to eq(%w[zero])
    end

    it "keeps the row without a parent out of a negated existential" do
      # "two" has a parent with no match, so it is in. "negative" has no parent, so it is out.
      expect(chain_titles(expression("not", label_lambda("exists"))))
        .to eq(%w[two])
    end

    # Only the parentless row is removed. "two" has an empty label list, and `all` over it is
    # TRUE in CEL too.
    it "keeps the row without a parent out of a universal, and keeps the childless one in" do
      expect(chain_titles(label_lambda("all"))).to eq(%w[zero two])
    end

    it "keeps the row without a parent out of every count threshold" do
      size = expression("size", variable("request.resource.attr.tag.labels"))
      expect(chain_titles(expression("ge", size, value(0)))).to eq(%w[zero two])
      expect(chain_titles(expression("eq", size, value(0)))).to eq(%w[two])
      expect(chain_titles(expression("not", expression("gt", size, value(0)))))
        .to eq(%w[two])
    end

    it "keeps the row without a parent out of a negated membership" do
      membership = expression("in",
        value("urgent"), variable("request.resource.attr.tag.labelNames"))
      expect(chain_titles(membership)).to eq(%w[zero])
      expect(chain_titles(expression("not", membership))).to eq(%w[two])
    end

    it "keeps the row without a parent out of a negated hasIntersection" do
      intersection = expression("hasIntersection",
        variable("request.resource.attr.tag.labelNames"),
        expression("list", value("urgent")))
      expect(chain_titles(intersection)).to eq(%w[zero])
      expect(chain_titles(expression("not", intersection))).to eq(%w[two])
    end

    # A directly mapped relation has no parent hop, so an empty collection stays empty.
    it "leaves a direct relation with the vacuous truth of an empty collection" do
      titles = described_class.query_plan_to_relation(
        plan: conditional(expression("all",
          variable("request.resource.attr.tags"),
          expression("lambda",
            expression("eq", variable("t.name"), value("chained")), variable("t")))),
        model: EdgeDocument,
        attributes: {
          "request.resource.attr.tags" => relation(:tags, fields: {"name" => field("name")})
        }
      ).order(:id).pluck(:title)

      # "negative" has no tags, so `all` is TRUE, as in CEL.
      expect(titles).to eq(%w[zero negative])
    end

    it "raises when a step of the path names a scalar field" do
      expect {
        chain_titles(expression("eq", variable("request.resource.attr.tag.name.x"), value("y")))
      }.to raise_error(Cerbos::ActiveRecord::UnmappedAttributeError, /scalar field/)
    end

    it "raises when a step of the path names nothing" do
      expect {
        chain_titles(expression("eq", variable("request.resource.attr.tag.missing"), value("y")))
      }.to raise_error(Cerbos::ActiveRecord::UnmappedAttributeError, /maps it to nothing/)
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

    # In each case the association returns fewer rows than a plain subquery would find, so
    # the filter could allow rows Cerbos denies.
    it "raises for a scope on the association, including a through chain" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeDocument,
          attributes: {"c" => relation(:visible_tags, member_field: "name")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /carries a scope/)
    end

    it "raises for a default scope on the target model" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeDocument,
          attributes: {"c" => relation(:softs, member_field: "name")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /default scope/)
    end

    it "raises for a has_one mapped as a collection" do
      # The database does not enforce one row for a has_one; a subquery would see them all.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeDocument,
          attributes: {"c" => relation(:profile, member_field: "name")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /not a collection/)
    end

    it "raises for an association that points at a subclass in a single-table hierarchy" do
      # The association filters on the type column; without it the subquery would find
      # base-class rows too.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeDocument,
          attributes: {"c" => relation(:special_kinds, member_field: "name")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /single-table hierarchy/)
    end

    it "accepts an association that points at the base class of a hierarchy" do
      # No type condition here, so the subquery already agrees.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("in", value("x"), variable("c"))),
          model: EdgeDocument,
          attributes: {"c" => relation(:kinds, member_field: "name")}
        ).to_sql
      }.not_to raise_error
    end

    it "raises for an association that joins on more than one column" do
      # The keys are an array. Without the guard they became one quoted column name and the
      # query failed with "no such column".
      reflection = EdgeCpkParent.reflect_on_association(:kids)
      skip "this ActiveRecord does not give composite keys" unless reflection.foreign_key.is_a?(Array)

      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("exists", variable("c"),
            expression("lambda", value(true), variable("x")))),
          model: EdgeCpkParent,
          attributes: {"c" => relation(:kids, member_field: "name")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedAssociationError, /more than one column/)
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
      # Otherwise it would also find comments of another owner class with the same id.
      sql = described_class.query_plan_to_relation(
        plan: conditional(expression("in", value("hello"), variable("c"))),
        model: EdgeDocument,
        attributes: {"c" => relation(:comments, member_field: "body")}
      ).to_sql

      expect(sql).to include("commentable_type")
      expect(sql).to include("EdgeDocument")
    end
  end

  # CEL division by zero gives NaN or Infinity, not an error. Turning it into NULL is wrong
  # for `!=`: `NaN != 1.0` is TRUE in CEL but `NULL != 1.0` is UNKNOWN in SQL.
  describe "division by a row-dependent denominator" do
    def divide_compare(operator, constant)
      described_class.query_plan_to_relation(
        plan: conditional(expression(operator,
          expression("div", variable("n"), variable("n")), value(constant))),
        model: EdgeDocument,
        attributes: {"n" => field("n")}
      ).order(:id).pluck(:title)
    end

    it "keeps the NaN row for a not-equal comparison" do
      # 0/0 is NaN, which is not equal to anything, so the zero row is allowed.
      expect(divide_compare("ne", 1.0)).to eq(%w[zero])
    end

    it "removes the NaN row from an ordered comparison" do
      # NaN is not ordered, so the comparison is false for the zero row.
      expect(divide_compare("gt", 0.5)).to eq(%w[two negative])
    end

    it "removes the NaN row from an equality" do
      expect(divide_compare("eq", 1.0)).to eq(%w[two negative])
    end

    it "keeps the NaN row under a negated equality" do
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("not",
          expression("eq", expression("div", variable("n"), variable("n")), value(1.0)))),
        model: EdgeDocument,
        attributes: {"n" => field("n")}
      )
      expect(relation.order(:id).pluck(:title)).to eq(%w[zero])
    end

    it "resolves an Infinity from a constant zero denominator" do
      # 2/0 is +Infinity, and -3/0 is -Infinity.
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("gt",
          expression("div", variable("n"), value(0.0)), value(0.0))),
        model: EdgeDocument,
        attributes: {"n" => field("n")}
      )
      expect(relation.order(:id).pluck(:title)).to eq(%w[two])
    end

    it "keeps the sign of a negative zero denominator" do
      # Zero keeps its sign: 2.0 / -0.0 is -Infinity and -3.0 / -0.0 is +Infinity.
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("gt",
          expression("div", variable("n"), value(-0.0)), value(0.0))),
        model: EdgeDocument,
        attributes: {"n" => field("n")}
      )
      expect(relation.order(:id).pluck(:title)).to eq(%w[negative])
    end

    it "refuses a division by a column that may be zero" do
      # SQL cannot read the sign of a zero column, so the Infinity's sign is unknown. Only
      # x/x is safe: a zero there is always 0/0, which is NaN.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("gt",
            expression("div", variable("n"), variable("author")), value(0.0))),
          model: EdgeDocument,
          attributes: {"n" => field("n"), "author" => field("author_id")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /sign of the Infinity/)
    end

    it "raises for more arithmetic on a value that may not be finite" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("gt",
            expression("add", expression("div", variable("n"), variable("n")), value(1.0)),
            value(0.0))),
          model: EdgeDocument,
          attributes: {"n" => field("n")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /NaN or Infinity/)
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
      # now() has nanoseconds. ActiveRecord would truncate them and change the instant.
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
          "matches" => ->(column, _pattern) { Arel::Nodes::Equality.new(column, Arel::Nodes.build_quoted("one")) }
        }
      )
      # `matches` has no default and would raise, so these rows come from the override.
      expect(relation.pluck(:a_string)).to eq(["one"])
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

    it "rejects a null representation it does not know" do
      expect { described_class.field("title", null_representation: :sometimes) }
        .to raise_error(ArgumentError, /must be :explicit or :omitted/)
    end
  end

  # A per-attribute null convention overrides the per-call option (#308, ADR 0004). With
  # `:explicit`, CEL sees a null value, so `eq`, `ne` and `in` need a definite answer where
  # SQL would give UNKNOWN.
  describe "a declared null convention" do
    let(:declared) do
      {
        "e" => described_class.field("title", null_representation: :explicit),
        "f" => described_class.field("n", null_representation: :explicit),
        "u" => field("author_id")
      }
    end

    def declared_sql(condition)
      described_class.query_plan_to_relation(
        plan: conditional(condition), model: EdgeDocument, attributes: declared
      ).to_sql
    end

    it "preserves CEL scalar types under explicit null conventions" do
      numeric_text = EdgeDocument.create!(title: "0", n: 0)
      nulls = EdgeDocument.create!(title: nil, n: nil)
      begin
        {"eq" => [], "ne" => [numeric_text.id, nulls.id]}.each do |operator, expected|
          query = described_class.query_plan_to_relation(
            plan: conditional(expression(operator, variable("e"), value(0))),
            model: EdgeDocument, attributes: declared
          )
          expect(query.where(id: [numeric_text.id, nulls.id]).order(:id).pluck(:id)).to eq(expected)
        end
        query = described_class.query_plan_to_relation(
          plan: conditional(expression("eq", variable("e"), variable("f"))),
          model: EdgeDocument, attributes: declared
        )
        expect(query.where(id: [numeric_text.id, nulls.id]).pluck(:id)).to eq([nulls.id])
      ensure
        numeric_text.destroy!
        nulls.destroy!
      end
    end

    it "guards a declared column in an equality against a constant" do
      sql = declared_sql(expression("eq", variable("e"), value("x")))
      expect(sql).to include('"title" IS NOT NULL')
    end

    it "guards a declared column under a negated equality" do
      # `null != "x"` is TRUE in CEL but UNKNOWN in SQL, so the guard goes inside the NOT.
      sql = declared_sql(expression("ne", variable("e"), value("x")))
      expect(sql).to match(/NOT.*"title" IS NOT NULL/m)
    end

    it "guards a declared column in a membership against constants" do
      sql = declared_sql(
        expression("in", variable("e"), expression("list", value("x"), value("y")))
      )
      expect(sql).to include('"title" IS NOT NULL')
      expect(sql).to include("IN (")
    end

    it "leaves a membership alone when the list already carries a null" do
      # A null element adds an `IS NULL` branch, which is already definite.
      sql = declared_sql(
        expression("in", variable("e"), expression("list", value("x"), value(nil)))
      )
      expect(sql).not_to include('"title" IS NOT NULL')
    end

    it "matches two nulls when both columns declare the convention" do
      sql = declared_sql(expression("eq", variable("e"), variable("f")))
      expect(sql).to match(/"title" IS NULL AND .*"n" IS NULL/)
    end

    it "refuses a comparison between two columns under mixed conventions" do
      expect { declared_sql(expression("ne", variable("e"), variable("u"))) }
        .to raise_error(
          Cerbos::ActiveRecord::UnsupportedOperatorError,
          /between two columns under mixed null conventions/
        )
    end

    it "leaves the order operators alone" do
      # CEL errors on a null here and denies either way, as UNKNOWN does. No guard needed.
      sql = declared_sql(expression("lt", variable("e"), value("x")))
      expect(sql).not_to include("IS NOT NULL")
    end

    it "keeps an operator override rather than restructuring around it" do
      # Adding the guard would silently replace the caller's translation.
      sql = described_class.query_plan_to_relation(
        plan: conditional(expression("eq", variable("e"), value("x"))),
        model: EdgeDocument,
        attributes: declared,
        operator_overrides: {
          "eq" => ->(left, right) { Arel::Nodes::NotEqual.new(left, Arel::Nodes.build_quoted(right)) }
        }
      ).to_sql
      expect(sql).to include('"title" != ')
      expect(sql).not_to include("IS NOT NULL")
    end

    it "does not apply the convention of the call to an attribute that declares nothing" do
      # `u` declares nothing, so the call's `:omitted` applies and the null is refused.
      # Declared attributes are unaffected.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq", variable("u"), value(nil))),
          model: EdgeDocument, attributes: declared,
          null_attribute_representation: :omitted
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /null constant/)

      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq", variable("e"), value(nil))),
          model: EdgeDocument, attributes: declared,
          null_attribute_representation: :omitted
        )
      }.not_to raise_error
    end
  end

  describe "generated SQL" do
    it "aliases each correlated subquery so nesting cannot self-correlate" do
      sql = translate(conditional(expression("exists",
        variable("request.resource.attr.tags"),
        expression("lambda",
          expression("eq", variable("t.name"), value("public")), variable("t"))))).to_sql

      expect(sql).to include("EXISTS (SELECT 1 FROM")
      # The subquery uses an alias, so a nested macro has a name to correlate to.
      expect(sql).to match(/"adversarial_tags" "cerbos_adversarial_tags_\d+"/)
      expect(sql).not_to include('FROM "adversarial_tags" WHERE')
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

  # The per-call `null_attribute_representation: :omitted` (#302, #308). The corpus declares
  # its conventions per attribute, so only this suite can vary the call's. Runs over the plans
  # recorded against the current PDP, so new cases are covered automatically.
  describe "the omitted null representation of the call" do
    let(:goldens) do
      ConformanceCorpus.goldens(ConformanceCorpus::PDP_TAGS.first).to_h { |golden| [golden.fetch("id"), golden] }
    end

    def omitted_call(plan, attributes)
      described_class.query_plan_to_relation(
        plan: plan, model: AdvResource, attributes: attributes,
        null_attribute_representation: :omitted
      )
    end

    def carries_null?(node)
      case node
      when Hash
        if node.key?("value")
          value = node.fetch("value")
          value.nil? || (value.is_a?(Array) && value.any?(&:nil?))
        else
          node.values.any? { |child| carries_null?(child) }
        end
      when Array then node.any? { |child| carries_null?(child) }
      else false
      end
    end

    # The refusal keys on the null operand, not on a list of operators:
    # `hasIntersection(tagNames, ["public", null])` would slip past an eq/ne/in allowlist.
    it "refuses every recorded plan that carries a null constant" do
      null_carrying = goldens.values.select { |golden| carries_null?(golden.fetch("plan")) }
      # The walk still finds nulls, in an equality and inside a list.
      expect(null_carrying.map { |golden| golden.fetch("id") }).to include(
        "null/equals/null-literal-on-missing-attribute",
        "null/has-intersection/literal-list-with-null-element"
      )

      not_refused = null_carrying.reject do |golden|
        omitted_call(golden.fetch("plan"), CorpusAttributes::UNDECLARED)
        false
      rescue Cerbos::ActiveRecord::UnsupportedOperatorError => e
        e.message.include?("null constant")
      end
      expect(not_refused.map { |golden| golden.fetch("id") }).to be_empty
    end

    # A per-attribute declaration beats the per-call option, so one policy can mix both.
    it "lets the declaration of an attribute override the convention of the call" do
      golden = goldens.fetch("null/equals/null-literal")
      plan = golden.fetch("plan")

      expect(omitted_call(plan, CorpusAttributes::ATTRIBUTES).pluck(:id).sort)
        .to eq(golden.fetch("allowed").sort)
      expect { omitted_call(plan, CorpusAttributes::UNDECLARED) }
        .to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /null constant/)
    end
  end
end
