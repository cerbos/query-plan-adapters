# frozen_string_literal: true

# Tests for what the corpus cannot ask. Read CODING_STANDARDS.md, "What a translator unit test may
# pin", before adding here. Three kinds:
#
# * Caller-supplied arguments (kind 2, permanent): operator overrides, mapper forms, the
#   per-call null representation, and ActiveRecord model shapes (through, scoped, STI,
#   polymorphic, composite keys). The corpus uses one mapping, so it cannot vary these.
# * Plans the planner never emits: empty `and`, wrong operand count, unknown kind. The adapter
#   accepts plans from any source, so these must fail closed.
# * Corpus gaps (kind 3, temporary): policy-reachable shapes the corpus does not carry yet,
#   tracked by #668. Each is deleted when its case lands.
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

  # Under `:explicit`, the convention most of these vary from. The adapter's own default is
  # pinned under "the default null representation".
  def translate(plan, model: AdvResource, attributes: ATTRS, null_attribute_representation: :explicit, **options)
    described_class.query_plan_to_relation(
      plan: plan, model: model, attributes: attributes,
      null_attribute_representation: null_attribute_representation, **options
    )
  end

  describe "the default null representation" do
    # The default is the convention whose wrong guess is a refusal rather than an over-grant
    # (docs/adr/0011-denial-fails-closed-in-the-result.md).
    it "is :omitted, so a null constant is refused" do
      plan = conditional(expression("in", variable("request.resource.attr.aString"), value(["a", nil])))
      expect {
        described_class.query_plan_to_relation(plan: plan, model: AdvResource, attributes: ATTRS)
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /the default is :omitted/)
      expect(translate(plan)).to be_a(ActiveRecord::Relation)
    end
  end

  describe "plan kinds" do
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

  # Every plan format must give the same answer. The protobuf, symbol-keyed and duck-typed
  # formats are not used anywhere else; the conformance harness covers the SDK's types. Each case
  # compares with the Hash form, not a hand-written id list.
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

  # The corpus maps one double column, so it cannot ask about a decimal one. The string-column
  # refusals are the corpus's `cast/*/malformed-string` cases.
  describe "casts that SQL cannot make the way CEL does" do
    it "raises for int() over a decimal column, naming the nearest-double difference" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq",
            expression("int", variable("d")), value(0))),
          model: EdgeDocument, attributes: {"d" => field("amount")}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError,
        /int\(\) applied to a :decimal column/)
    end
  end

  # A column inside the list compares as `==` between the two columns does, each under its own
  # declared convention (#574). The corpus maps one convention per attribute, so only this
  # suite can mix them or vary the call's default.
  describe "membership against a column member under declared conventions" do
    let(:rows) do
      [[nil, nil], [1, nil], [nil, 1], [1, 1], [1, 2], [2, nil]].map do |author_id, n|
        EdgeDocument.create!(author_id: author_id, n: n)
      end
    end

    let(:in_column) { expression("in", variable("a"), expression("list", variable("b"))) }
    let(:in_column_or_two) do
      expression("in", variable("a"), expression("list", variable("b"), value(2)))
    end

    after { rows.each(&:destroy!) }

    def mapping(a, b)
      {
        "a" => described_class.field("author_id", null_representation: a),
        "b" => described_class.field("n", null_representation: b)
      }
    end

    def member_ids(condition, attributes, call = :explicit)
      described_class.query_plan_to_relation(
        plan: conditional(condition), model: EdgeDocument, attributes: attributes,
        null_attribute_representation: call
      ).where(id: rows.map(&:id)).order(:id).pluck(:id)
    end

    def rows_at(*indexes) = indexes.map { |index| rows[index].id }

    %i[explicit omitted].each do |call|
      context "when the call's convention is #{call}" do
        it "treats two explicit nulls as equal, and an explicit null beside a value as unequal" do
          explicit = mapping(:explicit, :explicit)
          expect(member_ids(in_column, explicit, call)).to eq(rows_at(0, 3))
          expect(member_ids(expression("not", in_column), explicit, call)).to eq(rows_at(1, 2, 4, 5))
          expect(member_ids(in_column_or_two, explicit, call)).to eq(rows_at(0, 3, 5))
          expect(member_ids(expression("not", in_column_or_two), explicit, call))
            .to eq(rows_at(1, 2, 4))
        end

        it "denies a row where either omitted column is NULL, under any nesting" do
          omitted = mapping(:omitted, :omitted)
          expect(member_ids(in_column, omitted, call)).to eq(rows_at(3))
          expect(member_ids(expression("not", in_column), omitted, call)).to eq(rows_at(4))
          # CEL builds the list first, so a missing member errors even when 2 would match.
          expect(member_ids(in_column_or_two, omitted, call)).to eq(rows_at(3))
          expect(member_ids(expression("not", in_column_or_two), omitted, call)).to eq(rows_at(4))
        end

        it "leaves a NULL computed member UNKNOWN, since CEL errors computing it" do
          # `null + 1` is an error in CEL, never a null that `null in [...]` could match.
          in_sum = expression("in", variable("a"),
            expression("list", expression("add", variable("b"), value(1))))
          explicit = mapping(:explicit, :explicit)
          expect(member_ids(in_sum, explicit, call)).to be_empty
          expect(member_ids(expression("not", in_sum), explicit, call)).to eq(rows_at(2, 3, 4))
        end

        it "answers each side of mixed conventions as its own convention says, as == does" do
          explicit_needle = mapping(:explicit, :omitted)
          expect(member_ids(in_column, explicit_needle, call)).to eq(rows_at(3))
          expect(member_ids(expression("not", in_column), explicit_needle, call)).to eq(rows_at(2, 4))
          expect(member_ids(in_column_or_two, explicit_needle, call)).to eq(rows_at(3))
          expect(member_ids(expression("not", in_column_or_two), explicit_needle, call))
            .to eq(rows_at(2, 4))

          explicit_member = mapping(:omitted, :explicit)
          expect(member_ids(in_column, explicit_member, call)).to eq(rows_at(3))
          expect(member_ids(expression("not", in_column), explicit_member, call)).to eq(rows_at(1, 4, 5))
          expect(member_ids(in_column_or_two, explicit_member, call)).to eq(rows_at(3, 5))
          expect(member_ids(expression("not", in_column_or_two), explicit_member, call))
            .to eq(rows_at(1, 4))
        end
      end
    end

    # An undeclared column takes the call's convention in `==`, `!=` and `in` alike (#592).
    {
      explicit: {
        "eq" => [0, 3], "not_eq" => [1, 2, 4, 5], "ne_const" => [0, 2, 5], "not_eq_const" => [0, 2, 5]
      },
      omitted: {"eq" => [3], "not_eq" => [4], "ne_const" => [5], "not_eq_const" => [5]}
    }.each do |call, expected|
      it "gives undeclared columns the call's #{call} convention in ==, != and in alike" do
        undeclared = mapping(nil, nil)
        equal = expression("eq", variable("a"), variable("b"))
        conditions = {
          "eq" => [equal, in_column],
          "not_eq" => [expression("not", equal), expression("ne", variable("a"), variable("b")),
            expression("not", in_column)],
          "ne_const" => [expression("ne", variable("a"), value(1))],
          "not_eq_const" => [expression("not", expression("eq", variable("a"), value(1))),
            expression("not", expression("in", variable("a"), expression("list", value(1))))]
        }
        conditions.each do |name, shapes|
          shapes.each do |condition|
            expect(member_ids(condition, undeclared, call)).to eq(rows_at(*expected.fetch(name))), "#{name}: #{condition}"
          end
        end
      end
    end

    it "answers an undeclared column beside a declared one under the call's convention" do
      equal = expression("eq", variable("a"), variable("b"))
      {[nil, :omitted] => :explicit, [:omitted, nil] => :explicit, [nil, :explicit] => :omitted}
        .each do |(a, b), call|
          declared = mapping(a || call, b || call)
          [equal, expression("not", equal), in_column, expression("not", in_column)].each do |condition|
            expect(member_ids(condition, mapping(a, b), call)).to eq(member_ids(condition, declared, call))
          end
        end
    end
  end

  # `value in R.attr.<relation>` compares the value under its own declared convention, against a
  # stored member that is a null value when NULL (#591). The corpus maps `owner` as `:explicit`
  # and asserts its goldens only under the call's default, so only this suite can check the
  # rows returned under the other.
  describe "membership in a relation under the value's declared convention" do
    let(:rows) do
      [[nil, [nil]], [nil, ["x"]], ["x", [nil, "x"]], ["x", [nil]], [nil, []]].map do |title, names|
        EdgeDocument.create!(title: title).tap do |document|
          names.each { |name| EdgeTag.create!(name: name, document_id: document.id) }
        end
      end
    end

    let(:in_tags) { expression("in", variable("v"), variable("tags")) }

    after do
      EdgeTag.where(document_id: rows.map(&:id)).delete_all
      rows.each(&:destroy!)
    end

    def relation_ids(condition, value_convention, call)
      described_class.query_plan_to_relation(
        plan: conditional(condition), model: EdgeDocument,
        attributes: {
          "v" => described_class.field("title", null_representation: value_convention),
          "tags" => relation(:tags, member_field: "name")
        },
        null_attribute_representation: call
      ).where(id: rows.map(&:id)).order(:id).pluck(:id)
    end

    def rows_at(*indexes) = indexes.map { |index| rows[index].id }

    %i[explicit omitted].each do |call|
      context "when the call's convention is #{call}" do
        it "matches an explicit null value against a null member" do
          expect(relation_ids(in_tags, :explicit, call)).to eq(rows_at(0, 2))
          expect(relation_ids(expression("not", in_tags), :explicit, call)).to eq(rows_at(1, 3, 4))
        end

        it "denies a row whose omitted value is NULL, under either polarity" do
          expect(relation_ids(in_tags, :omitted, call)).to eq(rows_at(2))
          expect(relation_ids(expression("not", in_tags), :omitted, call)).to eq(rows_at(3))
        end
      end
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
        attributes: {"s" => field("title")},
        null_attribute_representation: :explicit
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

  # The rows a parent hop returns under each operator and polarity are the corpus's
  # `relation/<operator>/*to-one-chain` cases over `mainCategory` (#309). What is left here is a
  # path through the nested `fields:` mapping that the mapping cannot resolve.
  describe "a collection reached through a parent hop" do
    CHAIN_ATTRIBUTES = {
      "request.resource.attr.tag" => described_class.relation(:tags, fields: {
        "name" => described_class.field("name")
      })
    }.freeze

    def chain_titles(condition)
      Cerbos::ActiveRecord.query_plan_to_relation(
        plan: conditional(condition), model: EdgeDocument, attributes: CHAIN_ATTRIBUTES
      ).order(:id).pluck(:title)
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
  # The NaN of `x / x`, the signed-zero divisors and the refused column divisor are the corpus's
  # `arithmetic/divide/*` cases.
  describe "division by a row-dependent denominator" do
    # NaN is carried through arithmetic; an Infinity beside a column is not, since the column may
    # hold the opposite Infinity.
    it "raises for an Infinity carried into arithmetic beside a column" do
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("gt",
            expression("add", expression("div", variable("n"), value(0.0)), variable("n")),
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
          expression("eq", expression("noSuchOperator", variable("request.resource.attr.aString")), value("x"))
        ))
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /Unsupported operator: noSuchOperator/)
    end

    # The planner rejects an invalid literal at compile time, so only a hand-built plan carries one.
    it "rejects an invalid timestamp literal" do
      expect {
        translate(conditional(expression("lt",
          expression("timestamp", variable("request.resource.attr.createdAt")),
          expression("timestamp", value("2025-13-01")))),
          attributes: ATTRS.merge("request.resource.attr.createdAt" => field("created_at")))
      }.to raise_error(Cerbos::ActiveRecord::InvalidPlanError, /Invalid RFC-3339/)
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
      # The default would return strings starting with "str", so these rows come from the override.
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
  # SQL would give UNKNOWN. The guards a declared `:explicit` attribute gets in `eq`, `ne` and
  # `in` are the corpus's `null/*` cases over `owner`.
  describe "a declared null convention" do
    let(:declared) do
      {
        "e" => described_class.field("title", null_representation: :explicit),
        "f" => described_class.field("n", null_representation: :explicit),
        "u" => field("author_id")
      }
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

    # The corpus fixes each attribute's convention, so only a declaration can pair `:explicit`
    # with `:omitted` in an `eq`. The explicit NULL is a null value, definite wherever the other
    # side is present; the omitted NULL is a missing attribute, UNKNOWN under any polarity.
    it "compares two columns under mixed conventions as each side's convention says" do
      # `n` declares `:explicit`; `author_id` takes the call's `:omitted`. Both are integers, so
      # the comparison is not a cross-type one.
      null_n = EdgeDocument.create!(n: nil, author_id: 5)
      null_author = EdgeDocument.create!(n: 5, author_id: nil)
      # The explicit IS NOT NULL alone would make `eq` FALSE here, and `ne` TRUE.
      both_null = EdgeDocument.create!(n: nil, author_id: nil)
      ids = [null_n.id, null_author.id, both_null.id]
      allowed = ->(condition) {
        described_class.query_plan_to_relation(
          plan: conditional(condition), model: EdgeDocument, attributes: declared,
          null_attribute_representation: :omitted
        ).where(id: ids).pluck(:id)
      }
      begin
        equality = expression("eq", variable("f"), variable("u"))
        expect(allowed.call(equality)).to be_empty
        expect(allowed.call(expression("not", equality))).to eq([null_n.id])
        expect(allowed.call(expression("ne", variable("f"), variable("u")))).to eq([null_n.id])
      ensure
        [null_n, null_author, both_null].each(&:destroy!)
      end
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

    it "keeps refusing a null constant under :omitted when an operator override owns eq" do
      # The override would receive the null and could select the NULL rows the PDP denies.
      expect {
        described_class.query_plan_to_relation(
          plan: conditional(expression("eq", variable("u"), value(nil))),
          model: EdgeDocument, attributes: declared,
          null_attribute_representation: :omitted,
          operator_overrides: {"eq" => ->(left, right) { Arel::Nodes::Equality.new(left, right) }}
        )
      }.to raise_error(Cerbos::ActiveRecord::UnsupportedOperatorError, /null constant/)
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

    # True if every null in the plan is a scalar operand of `eq` or `ne` beside a field
    # attribute: the one shape {Translator} renders as UNKNOWN-when-NULL (#551).
    def only_null_equalities?(node, attributes)
      case node
      when Hash
        expression = node["expression"]
        if expression && %w[eq ne].include?(expression.fetch("operator"))
          operands = expression.fetch("operands")
          nulls = operands.select { |operand| operand.key?("value") && operand.fetch("value").nil? }
          variable = operands.find { |operand| operand.key?("variable") }
          return true if nulls.length == 1 && operands.length == 2 && variable &&
            attributes[variable.fetch("variable")].is_a?(Cerbos::ActiveRecord::AttributeMapping::Field)
        end
        return false if node.key?("value") && carries_null?(node)

        node.values.all? { |child| only_null_equalities?(child, attributes) }
      when Array then node.all? { |child| only_null_equalities?(child, attributes) }
      else true
      end
    end

    # The refusal keys on the null operand, not on a list of operators:
    # `hasIntersection(tagNames, ["public", null])` would slip past an eq/ne/in allowlist.
    # The one exception is `eq`/`ne` against a field attribute, rendered UNKNOWN-when-NULL.
    it "refuses every recorded plan that carries a null constant, except eq/ne against a field" do
      null_carrying = goldens.values.select { |golden| carries_null?(golden.fetch("plan")) }
      # The walk still finds nulls, in an equality and inside a list.
      expect(null_carrying.map { |golden| golden.fetch("id") }).to include(
        "null/equals/null-literal-on-missing-attribute",
        "null/has-intersection/literal-list-with-null-element"
      )

      translated, refused = null_carrying.partition do |golden|
        omitted_call(golden.fetch("plan"), CorpusAttributes::UNDECLARED).to_sql
        true
      rescue Cerbos::ActiveRecord::UnsupportedOperatorError => e
        raise unless e.message.include?("null constant")

        false
      end

      translated_ids = translated.map { |golden| golden.fetch("id") }
      expect(translated_ids).to include("null/equals/null-literal-on-missing-attribute")
      expect(refused.map { |golden| golden.fetch("id") })
        .to include("null/has-intersection/literal-list-with-null-element")
      expect(translated.reject { |golden|
        only_null_equalities?(golden.fetch("plan"), CorpusAttributes::UNDECLARED)
      }.map { |golden| golden.fetch("id") }).to be_empty
    end

    # A per-attribute declaration beats the per-call option, so one policy can mix both.
    it "lets the declaration of an attribute override the convention of the call" do
      golden = goldens.fetch("null/equals/null-literal")
      plan = golden.fetch("plan")

      expect(omitted_call(plan, CorpusAttributes::ATTRIBUTES).pluck(:id).sort)
        .to eq(golden.fetch("allowed").sort)
      # Undeclared, `owner` takes the call's `:omitted`: a NULL owner is a missing attribute,
      # and a present one is never null, so `== null` allows nothing.
      expect(omitted_call(plan, CorpusAttributes::UNDECLARED).pluck(:id)).to be_empty
    end
  end

  # A plan the planner never emits: it folds `x in` a map literal, a dyn() map or a principal map
  # to its keys. The adapter accepts plans from any source, and answering FALSE would grant the
  # negation.
  describe "a list or map literal" do
    it "tests the keys of a map value in a membership" do
      plan = conditional(expression("in", variable("request.resource.attr.aString"), value({"one" => 1})))

      expect(translate(plan).pluck(:a_string).uniq).to eq(["one"])
      expect(translate(conditional(expression("not", plan["condition"]))).where(a_string: "one")).to be_empty
    end
  end
end
