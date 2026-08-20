# frozen_string_literal: true

# What the adapter can be asked WITHOUT a corpus action, and nothing that has one.
#
# Read CLAUDE.md, "What a translator unit test may pin", before adding to this file. Most of it
# is kind 2 — **a caller-supplied argument the corpus structurally cannot vary**, and permanent.
# adapterctl.json classifies each action against ONE attribute map per adapter, so an
# operator override, a second mapper form, a per-call `null_attribute_representation` and the
# association shapes this adapter refuses to guess at have no corpus spelling at all: the corpus
# asks what a POLICY produces, not what a caller passes. The association block is the largest of
# them and the reason this file is worth its length — `has_many :through`, a scoped association,
# single-table inheritance, a polymorphic `belongs_to` and a composite primary key are
# ActiveRecord MODEL shapes, and no policy can describe one.
#
# Two other kinds are here, and they are not the same thing:
#
# * **Shapes CEL cannot reach.** A plan whose `and` carries no operands, an operator with the
#   wrong operand count, an unrecognised plan kind. The planner never emits one, so the corpus
#   is built from real plans and cannot carry them — but this adapter accepts a plan from any
#   source, and a plan that lost or gained an operand must not widen the filter. Permanent.
# * **Refusals the corpus also carries, kept for the MECHANISM rather than the shape.** The cast
#   and division blocks below drive their refusals through purpose-built columns
#   (`EdgeDocument`), so each message names the operand type that raised it; the corpus proves
#   the same shapes end to end against its own mapping. These are not a substitute for those
#   actions and must never become one.
#
# What is NOT here, deliberately: anything the golden asset already pins byte-for-byte. Operand
# order and LIKE escaping used to live here and were removed once
# golden/expectations.json recorded the emitted SQL — `vf-le` and `like-percent` pin those exact
# clauses, and spec/translator_spec.rb states the ESCAPE rule over the whole corpus. A shape
# belongs in conformance/policies/adversarial.yaml, never here.
#
# Needs no PDP and no database server: the models are SQLite in memory.

RSpec.describe Cerbos::ActiveRecord do
  before(:all) do
    AdversarialModels.establish!
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

    # An `and` with no operands would give TRUE, and thus the filter would permit every row.
    # The planner does not make that shape, but a plan that lost its operands on the way here
    # must not become "permit everything".
    # A plan that carries an extra operand is malformed. If the adapter read only the positions
    # it expected, the extra operand would disappear and the filter would be wider than the
    # condition. A malformed plan must fail closed.
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

  # The four transports a plan can arrive over, held to ONE answer.
  #
  # No corpus action can ask this: the harness only ever hands the adapter an SDK object and the
  # translator unit test only ever hands it a parsed wire fixture, so the REST/protobuf shape and
  # the duck-typed one are executed nowhere else. Every case below compares against the Hash
  # decoding of the same condition rather than a written-down id set — a hand-written set is the
  # thing conformance/ exists to abolish, and it would also have to be rewritten whenever the
  # seeds move.
  describe "accepted plan shapes" do
    let(:condition) do
      expression("eq", variable("request.resource.attr.aString"), value("one"))
    end

    # The reference answer, and its own anti-vacuity guard: if this became empty or total, every
    # comparison below would agree and prove nothing.
    let(:reference_ids) do
      ids = translate(conditional(condition)).pluck(:id).sort
      expect(ids).not_to be_empty
      expect(ids.size).to be < ConformanceCorpus::SEEDS.size
      ids
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

    # The official Ruby SDK (https://github.com/cerbos/cerbos-sdk-ruby) is the usual source of
    # plans. Thus these tests use its output types directly and do not use a substitute. The
    # differential suites already use real responses from the client. This test holds the
    # contract with a name. Thus a change in the SDK makes this test fail, and it does not make
    # an unclear failure in a harness.
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
      # CEL reads a whole string or makes an error, and Cerbos then denies the row. SQLite
      # reads the digits at the front, so CAST('1junk' AS INTEGER) is 1 and the filter would
      # keep a row that the PDP denies.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("gt",
            expression("int", variable("s")), value(0))),
          model: EdgeDocument, attributes: {"s" => field("title")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError,
        /int\(\) applied to a :string column/)
    end

    # The two operands fail for two different reasons, so each message names only its own. A
    # message that named both would not show which mechanism stopped the translation, which is
    # what the corpus pins these messages for (cerbos/query-plan-adapters#326).
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
      # A NULL column sends no attribute under this convention. Two NULL columns are then two
      # MISSING attributes, and CEL raises rather than finding them equal, so the PDP denies
      # the row. Plain equality gives UNKNOWN and keeps the row out.
      sql = described_class.query_plan_to_relation(
        plan: plan, model: EdgeDocument, attributes: mapping,
        null_attribute_representation: :omitted
      ).to_sql
      expect(sql).not_to match(/IS NULL AND .*IS NULL/)
    end
  end

  describe "membership with a column inside the list" do
    # `null in [R.attr.x]` is true when the column is null. An earlier version built
    # `NULL IN (x)`, which is always UNKNOWN, and Arel could not even render it.
    it "translates a null needle against a list holding a column" do
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("in", value(nil),
          expression("list", variable("s")))),
        model: EdgeDocument,
        attributes: {"s" => field("title")}
      )
      expect(relation.to_sql).to match(/"title" IS NULL/)
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

  # A path such as `R.attr.parent.children` reaches a collection THROUGH a to-one parent. CEL
  # cannot read a field from a list, so an absent parent is a missing path and Cerbos denies the
  # row. A subquery from the resource row sees the same empty result for an absent parent and
  # for a parent with no children, and thus `all`, `!exists` and every count over the chain
  # would give back the rows that the PDP denies (cerbos/query-plan-adapters#309/#315/#316).
  #
  # The caller writes the chain as a nested `fields:` mapping, and that nesting is what tells
  # the adapter which hops are the parent. The corpus proves the behaviour end to end with the
  # w1-*-chain actions; these tests hold each polarity on a small model.
  describe "a collection reached through a parent hop" do
    CHAIN_ATTRIBUTES = {
      "request.resource.attr.tag" => Cerbos::ActiveRecord.relation(:tags, fields: {
        "labels" => Cerbos::ActiveRecord.relation(
          :labels, fields: {"name" => Cerbos::ActiveRecord.field("name")}
        ),
        "labelNames" => Cerbos::ActiveRecord.relation(:labels, member_field: "name"),
        "name" => Cerbos::ActiveRecord.field("name")
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
      # "two" has the parent and no matching label, so it belongs in the result. "negative" has
      # no tag at all, and it must stay out under this polarity as well.
      expect(chain_titles(expression("not", label_lambda("exists"))))
        .to eq(%w[two])
    end

    # The guard removes only the row with NO parent. "two" has the parent and an empty label
    # list, which is a real empty collection, and `all` over it is vacuously TRUE in CEL too.
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

    # A relation that the caller mapped directly keeps the meaning of an empty collection. Only
    # a chain has a parent hop to require.
    it "leaves a direct relation with the vacuous truth of an empty collection" do
      titles = Cerbos::ActiveRecord.query_plan_to_relation(
        plan: conditional(expression("all",
          variable("request.resource.attr.tags"),
          expression("lambda",
            expression("eq", variable("t.name"), value("chained")), variable("t")))),
        model: EdgeDocument,
        attributes: {
          "request.resource.attr.tags" => relation(:tags, fields: {"name" => field("name")})
        }
      ).order(:id).pluck(:title)

      # "negative" has no tags, so `all` over the empty collection is TRUE, as it is in CEL.
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

    # Each of these was found by an adversarial review. In every case the association gives
    # fewer rows than the table holds, so the attributes that Cerbos evaluates and the rows
    # that a plain subquery finds do not agree, and the filter selected a row that the
    # decision did not.
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
      # ActiveRecord does not make the database enforce that a has_one has only one row, so
      # the association gives one row while a subquery would examine every row.
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
      # Such an association also filters on the inheritance column. Without that condition the
      # subquery would find the rows of the base class, which the association never gives.
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
      # ActiveRecord adds no type condition there, so the subquery already agrees.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("in", value("x"), variable("c"))),
          model: EdgeDocument,
          attributes: {"c" => relation(:kinds, member_field: "name")}
        ).to_sql
      }.not_to raise_error
    end

    it "raises for an association that joins on more than one column" do
      # ActiveRecord gives an array for the keys of such an association. Before the guard the
      # array reached table[...] and became one quoted name, and the query then failed with
      # "no such column" instead of the adapter refusing the mapping.
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
      # Without the condition on the type column, the subquery would also find the comments of
      # a different owner class that has the same id.
      sql = described_class.query_plan_to_relation(
        plan: conditional(expression("in", value("hello"), variable("c"))),
        model: EdgeDocument,
        attributes: {"c" => relation(:comments, member_field: "body")}
      ).to_sql

      expect(sql).to include("commentable_type")
      expect(sql).to include("EdgeDocument")
    end
  end

  # A division by zero is not an error in CEL: arithmetic on attributes uses doubles, and
  # IEEE-754 gives NaN or an Infinity. An earlier version of this adapter made the result NULL
  # with NULLIF. That is correct for an ordered comparison, but not for `!=`: `NaN != 1.0` is
  # TRUE in CEL, while `NULL != 1.0` is UNKNOWN in SQL. Thus the filter removed a row that the
  # PDP permits. A live PDP confirmed the difference before this test was written.
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
      # 0/0 is NaN, and NaN is not equal to any value, so CEL permits the zero row.
      expect(divide_compare("ne", 1.0)).to eq(%w[zero])
    end

    it "removes the NaN row from an ordered comparison" do
      # NaN has no order against any value, so the comparison is false for the zero row.
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
      # IEEE-754 keeps the sign of a zero, so 2.0 / -0.0 is -Infinity and -3.0 / -0.0 is
      # +Infinity. A PDP confirmed this before the fix: the adapter returned the positive row
      # where Cerbos allows only the negative one.
      relation = described_class.query_plan_to_relation(
        plan: conditional(expression("gt",
          expression("div", variable("n"), value(-0.0)), value(0.0))),
        model: EdgeDocument,
        attributes: {"n" => field("n")}
      )
      expect(relation.order(:id).pluck(:title)).to eq(%w[negative])
    end

    it "refuses a division by a column that may be zero" do
      # The sign of a zero column cannot be read in SQL, so the sign of the Infinity is
      # unknown. Only a division of a value by itself stays safe, because then a zero
      # denominator means a zero numerator, which gives NaN.
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
      # The planner makes nanoseconds for now(). ActiveRecord would remove the last digits of
      # that value, and thus it would change the instant in the comparison.
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
      # The override answered, not the default translation: `matches` has no default here and
      # would otherwise raise, and the rows are the ones its equality selects.
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

  # The convention of a NULL column belongs to the attribute, and the option of the call is
  # only its fallback (cerbos/query-plan-adapters#308, ADR 0004). A declaration of `:explicit`
  # makes `eq`, `ne` and `in` definite, because CEL holds a null VALUE under that convention
  # while SQL answers UNKNOWN and an UNKNOWN keeps the row out under BOTH polarities.
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

    it "guards a declared column in an equality against a constant" do
      sql = declared_sql(expression("eq", variable("e"), value("x")))
      expect(sql).to match(/"title" IS NOT NULL/)
    end

    it "guards a declared column under a negated equality" do
      # `null != "x"` is TRUE in CEL. Plain `NOT ("title" = 'x')` stays UNKNOWN and loses the
      # row, so the guard has to sit INSIDE the negation.
      sql = declared_sql(expression("ne", variable("e"), value("x")))
      expect(sql).to match(/NOT.*"title" IS NOT NULL/m)
    end

    it "guards a declared column in a membership against constants" do
      sql = declared_sql(
        expression("in", variable("e"), expression("list", value("x"), value("y")))
      )
      expect(sql).to match(/"title" IS NOT NULL/)
      expect(sql).to match(/IN \(/)
    end

    it "leaves a membership alone when the list already carries a null" do
      # A null element already forces the `IS NULL` branch, which is definite by itself.
      sql = declared_sql(
        expression("in", variable("e"), expression("list", value("x"), value(nil)))
      )
      expect(sql).not_to match(/"title" IS NOT NULL/)
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
      # A null receiver raises a no-overload error in CEL, which denies under both polarities.
      # UNKNOWN already does that, so a guard here would change the meaning.
      sql = declared_sql(expression("lt", variable("e"), value("x")))
      expect(sql).not_to match(/IS NOT NULL/)
    end

    it "keeps an operator override rather than restructuring around it" do
      # `eq` and `ne` RESTRUCTURE the comparison, so to add the guard would discard the
      # translation of the caller in silence.
      sql = described_class.query_plan_to_relation(
        plan: conditional(expression("eq", variable("e"), value("x"))),
        model: EdgeDocument,
        attributes: declared,
        operator_overrides: {
          "eq" => ->(left, right) { Arel::Nodes::NotEqual.new(left, Arel::Nodes.build_quoted(right)) }
        }
      ).to_sql
      expect(sql).to match(/"title" != /)
      expect(sql).not_to match(/IS NOT NULL/)
    end

    it "does not apply the convention of the call to an attribute that declares nothing" do
      # `u` declares nothing, so the `:omitted` of the call reaches it and the null constant
      # is refused. The declared attributes above are unaffected.
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

      expect(sql).to match(/EXISTS \(SELECT 1 FROM/)
      # The subquery reads an ALIAS of the association table and never the table itself, so a
      # macro nested inside it has a name to correlate to.
      expect(sql).to match(/"adversarial_tags" "cerbos_adversarial_tags_\d+"/)
      expect(sql).not_to match(/FROM "adversarial_tags" WHERE/)
    end

    # The shared corpus never nests a macro over the SAME association inside itself. Without
    # a new alias for each scope, the inner subquery would correlate to its own row and not to
    # the element of the outer scope. Then the condition would be true for each row that has
    # one tag, and thus it would permit rows that Cerbos denies.
    it "correlates a macro nested over the same association to the outer element" do
      inner = expression("exists", variable("request.resource.attr.tags"),
        expression("lambda",
          expression("ne", variable("u.name"), variable("t.name")), variable("u")))

      sql = translate(conditional(expression("exists",
        variable("request.resource.attr.tags"),
        expression("lambda", inner, variable("t"))))).to_sql

      # Two different aliases, and the inner subquery compares against the OUTER one. Matched
      # structurally rather than by name: the numbering is an implementation detail, and
      # spelling it here would fail on an unrelated change to how scopes are counted.
      comparison = sql[/"(cerbos_adversarial_tags_\d+)"\."name" != "(cerbos_adversarial_tags_\d+)"\."name"/, 0]
      expect(comparison).not_to be_nil
      inner_alias, outer_alias = comparison.scan(/cerbos_adversarial_tags_\d+/)
      expect(inner_alias).not_to eq(outer_alias)

      # And the outer alias is the one the enclosing EXISTS introduced, so the inner subquery
      # correlates to the outer ELEMENT rather than to its own row. Without that, the condition
      # is true for every row holding one tag and returns rows the PDP denies.
      expect(sql).to match(/EXISTS \(SELECT 1 FROM "adversarial_tags" "#{outer_alias}".*#{inner_alias}/m)
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
