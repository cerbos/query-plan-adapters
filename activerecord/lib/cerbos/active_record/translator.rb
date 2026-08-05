# frozen_string_literal: true

require "arel"

require_relative "arel_support"
require_relative "attribute_mapping"
require_relative "dialect"
require_relative "errors"
require_relative "plan"
require_relative "relations"
require_relative "string_matching"
require_relative "timestamps"
require_relative "values"

module Cerbos
  module ActiveRecord
    # Goes through a query plan after a normalise operation, and makes the equivalent Arel
    # predicate.
    #
    # Two rules control almost all the decisions in this class:
    #
    # * *The order on the wire is the order in the source.* The planner keeps the order of the
    #   operands in the policy. Thus <tt>1 < R.attr.x</tt> comes as
    #   <tt>lt(value(1), variable(x))</tt>. This adapter makes the comparison in the same
    #   order (+1 < x+), and that SQL is already correct. Some adapters made a different
    #   assumption: that a column is always first. They moved the operands to get that order,
    #   and thus they turned the directional comparisons around
    #   (cerbos/query-plan-adapters#257).
    #
    # * *An error is not a false.* CEL denies a resource if the evaluation of its condition
    #   makes an error. A missing attribute is one cause. An element without a field is
    #   another. The UNKNOWN value of SQL has the same behaviour: a predicate does not select
    #   it, and the negation of that predicate does not select it. This translation keeps
    #   UNKNOWN and does not change it into a boolean. For this reason, the collection macros
    #   become CASE expressions and not only EXISTS subqueries.
    class Translator
      # The adapter must not resolve the operands of these operators before the operator runs.
      # Each of these operators does one of two things: it connects an iterator variable to a
      # scope, or it must keep UNKNOWN through a branch.
      STRUCTURAL_OPERATORS = %w[and or not if lambda exists all exists_one filter map].freeze

      COMPARISONS = %w[eq ne lt gt le ge].freeze

      ARITHMETIC = {"add" => "+", "sub" => "-", "mult" => "*", "mod" => "%"}.freeze

      STRING_MATCHES = {
        "contains" => {prefix: true, suffix: true},
        "startsWith" => {prefix: false, suffix: true},
        "endsWith" => {prefix: true, suffix: false}
      }.freeze

      NULL_REPRESENTATIONS = %i[explicit omitted].freeze

      def initialize(model:, attributes:, operator_overrides: {}, null_attribute_representation: :explicit)
        @model = model
        @attributes = attributes.transform_keys(&:to_s)
        @operator_overrides = operator_overrides.transform_keys(&:to_s)
        @null_attribute_representation = null_attribute_representation.to_sym
        @dialect = Dialect.for(model)
        @matcher = StringMatcher.new(@dialect)

        unless NULL_REPRESENTATIONS.include?(@null_attribute_representation)
          raise ArgumentError,
            "null_attribute_representation must be :explicit or :omitted, got " \
            "#{null_attribute_representation.inspect}"
        end

        @operator_overrides.each_key do |operator|
          if STRUCTURAL_OPERATORS.include?(operator)
            raise ArgumentError,
              "Operator #{operator.inspect} cannot be overridden: it binds scopes or " \
              "branches, so its operands are not resolved before it runs"
          end
        end
      end

      attr_reader :model, :attributes, :operator_overrides, :dialect, :matcher,
        :null_attribute_representation

      # @param plan [Object] anything {Plan.normalise} accepts
      # @return [ActiveRecord::Relation]
      def translate(plan)
        normalised = Plan.normalise(plan)
        return model.none if normalised.always_denied?
        return model.all if normalised.always_allowed?

        assert_no_null_operands(normalised.condition) if null_attribute_representation == :omitted

        @aliaser = Relations::Aliaser.new
        # The keys are object identities. Each column that the adapter resolves is a new Arel
        # node, and that same node goes through the translation without a change. Thus
        # identity is the correct comparison here.
        @column_types = {}.compare_by_identity
        environment = Environment.new(translator: self, bindings: {})
        model.where(predicate(normalised.condition, environment))
      end

      # @api private
      attr_reader :aliaser

      # @api private
      def register_column_type(node, owner_model, column_name)
        type = owner_model.columns_hash[column_name.to_s]&.type
        @column_types[node] = type if type
        node
      end

      # @api private
      def column_type(node)
        @column_types[node]
      end

      # @api private
      def root_table
        model.arel_table
      end

      # Resolves an operand to a value. The value is an Arel node, a Ruby constant, or one of
      # the intermediate {Values} that the operator around it uses.
      #
      # @api private
      def evaluate(node, environment)
        case node
        when Plan::Value then node.value
        when Plan::Variable then environment.resolve(node.name)
        when Plan::Expression then evaluate_expression(node, environment)
        else raise InvalidPlanError, "Unrecognised query plan node: #{node.inspect}"
        end
      end

      private

      # Refuses each null constant in the plan under the `omitted` representation.
      #
      # With that convention a NULL column sends no attribute. Thus CEL raises a
      # missing-attribute error and the PDP denies the row, but `IS NULL` would give exactly
      # those rows (cerbos/query-plan-adapters#302).
      #
      # The scan examines the OPERANDS and not a list of operators. A null constant can reach
      # a predicate that selects NULL through more shapes than `eq` and `ne`: `in` and
      # `hasIntersection` carry one in a list. A list of operators would also need a change
      # for each new operator.
      #
      # The scan is wider than the shapes that give too many rows. `ne(x, null)` is correct
      # by itself, but this adapter puts a negation around a predicate and does not push it
      # into the leaf. Thus a leaf cannot know that a `not` above it will make `IS NOT NULL`
      # into a predicate that selects NULL again. To refuse each null constant is correct for
      # all the shapes; a smaller rule needs the adapter to count the negations.
      def assert_no_null_operands(node)
        case node
        when Plan::Value
          if node.value.nil? || (node.value.is_a?(Array) && node.value.any?(&:nil?))
            raise UnsupportedOperatorError,
              "Cannot translate a null constant with null_attribute_representation: :omitted. " \
              "A NULL column then sends no attribute, so Cerbos evaluates the comparison as a " \
              "missing-attribute error and denies the row, but a filter that selects NULL " \
              "would return that row. Send a NULL column as an explicit null and use " \
              ":explicit, or keep this shape out of the policy."
          end
        when Plan::Expression
          node.operands.each { |operand| assert_no_null_operands(operand) }
        end
      end

      def evaluate_expression(node, environment)
        operator = node.operator
        operands = node.operands

        case operator
        when "and", "or" then combine(operator, operands, environment)
        when "not" then negate(operands, environment)
        when "if" then ternary(operands, environment)
        when "exists", "all", "exists_one", "filter", "map" then macro(operator, operands, environment)
        when "lambda"
          raise InvalidPlanError, "lambda outside a collection macro"
        else
          apply(operator, operands.map { |o| evaluate(o, environment) })
        end
      end

      def predicate(node, environment)
        as_predicate(evaluate(node, environment))
      end

      def as_predicate(value)
        if collection?(value)
          raise UnsupportedOperatorError,
            "#{describe(value)} cannot be used as a condition: CEL collection expressions " \
            "such as filter() and map() evaluate to a list, not to a boolean"
        end

        # A boolean column alone is a correct CEL condition. But the `where` method of
        # ActiveRecord refuses a column reference alone, and PostgreSQL needs a boolean
        # expression and not a value. A comparison with TRUE has the same result, and this is
        # also true for NULL.
        return ArelSupport.comparison("eq", value, true) if column_type(value) == :boolean

        ArelSupport.to_predicate(value)
      end

      # An `and` with no operands would give TRUE, and thus the filter would permit every row.
      # The Cerbos planner does not make that shape, but this adapter accepts a plan from any
      # source. A plan that lost its operands — an incomplete JSON body, a bad conversion —
      # must not become "permit everything". Thus an empty operand list is an error.
      def combine(operator, operands, environment)
        if operands.empty?
          raise InvalidPlanError, "#{operator} has no operands"
        end

        predicates = operands.map { |operand| predicate(operand, environment) }
        (operator == "and") ? ArelSupport.and_node(predicates) : ArelSupport.or_node(predicates)
      end

      def negate(operands, environment)
        unless operands.length == 1
          raise InvalidPlanError, "not takes exactly one operand, got #{operands.length}"
        end
        ArelSupport.not_node(predicate(operands.first, environment))
      end

      # --- ternary --------------------------------------------------------------------

      # +if(condition, then, else)+.
      #
      # The CASE that this method makes has no ELSE clause. This is necessary. If the
      # condition is UNKNOWN, because of a NULL column or a missing attribute, CEL makes an
      # error and denies the row. A CASE without a WHEN clause that agrees gives NULL. Thus
      # the row stays out of the result, and it also stays out when a NOT operator is around
      # the CASE. An +ELSE+ clause would put those rows into the else branch.
      def ternary(operands, environment)
        unless operands.length == 3
          raise InvalidPlanError, "if takes exactly three operands, got #{operands.length}"
        end

        condition = predicate(operands[0], environment)
        then_value = evaluate(operands[1], environment)
        else_value = evaluate(operands[2], environment)

        # An arm with a value that is not finite must not go to the database. Thus the
        # translator keeps the ternary, and the comparison around it calculates each branch.
        if deferred_value?(then_value) || deferred_value?(else_value)
          return Values::ConditionalValue.new(
            condition: condition, then_value: then_value, else_value: else_value
          )
        end

        ArelSupport.case_node(
          [[condition, then_value], [ArelSupport.not_node(condition), else_value]]
        )
      end

      def deferred_value?(value)
        value.is_a?(Values::IEEEConstant) || value.is_a?(Values::ConditionalValue)
      end

      # --- collection macros ----------------------------------------------------------

      def macro(operator, operands, environment)
        unless operands.length == 2
          raise InvalidPlanError, "#{operator} takes a collection and a lambda"
        end

        collection = evaluate(operands[0], environment)
        if collection.is_a?(Array)
          return value_list_macro(operator, collection, operands[1], environment)
        end

        scope = require_collection(operator, collection)
        body_node, iterator = lambda_parts(operands[1])
        inner = environment.bind(iterator, scope)

        case operator
        when "map"
          Values::MappedCollection.new(scope: scope, projection: evaluate(body_node, inner))
        when "filter"
          Values::FilteredCollection.new(scope: scope, body: predicate(body_node, inner))
        else
          quantifier(operator, scope, predicate(body_node, inner))
        end
      end

      # The three CEL quantifiers are different in one important way. Each one has different
      # behaviour for an element whose body made an error. Thus each one gets its own guard
      # for that error, and they do not share one guard:
      #
      # * +exists+ ignores the errors if one element gives true;
      # * +all+ ignores the errors if one element gives false;
      # * +exists_one+ never ignores them, because it must count all the elements.
      def quantifier(operator, scope, body)
        error_witness = scope.exists(ArelSupport.is_null(body))

        case operator
        when "exists"
          ArelSupport.case_node(
            [[scope.exists(body), true], [error_witness, nil]], else_value: false
          )
        when "all"
          ArelSupport.case_node(
            [[scope.exists(ArelSupport.not_node(body)), false], [error_witness, nil]],
            else_value: true
          )
        when "exists_one"
          ArelSupport.case_node(
            [[error_witness, nil]],
            else_value: ArelSupport.comparison("eq", scope.count(body), 1)
          )
        else
          raise UnsupportedOperatorError, "Unsupported collection macro: #{operator}"
        end
      end

      # A macro over a list of constants. The planner sends the list itself when the collection
      # is a principal attribute, because it knows those values when it makes the plan.
      #
      # The elements are known here, so the translator evaluates the body one time for each
      # element and joins the results. SQL gives the correct answer without more work: OR and
      # AND obey the same three-valued logic as the CEL quantifiers. OR is TRUE if one element
      # is true, UNKNOWN if no element is true and one is unknown, and FALSE if all are false.
      # That is exactly `exists`. AND is the same for `all`.
      def value_list_macro(operator, values, lambda_node, environment)
        body_node, iterator = lambda_parts(lambda_node)
        bodies = values.map { |value| predicate(body_node, environment.bind(iterator, value)) }

        case operator
        when "exists" then ArelSupport.or_node(bodies)
        when "all" then ArelSupport.and_node(bodies)
        when "exists_one" then exactly_one_of(bodies)
        else
          # `filter` and `map` give a list, and the operator that uses it — `size` or
          # `hasIntersection` — would need a second list-valued form. No corpus shape needs it,
          # so the adapter refuses instead of keeping code that nothing proves.
          raise UnsupportedOperatorError,
            "#{operator} over a list of constants is not supported: only exists, all and " \
            "exists_one have a translation for that shape"
        end
      end

      # `exists_one` never ignores an element that made an error, so the guard for the error
      # comes first. After that it is an exact count of the elements that are true.
      def exactly_one_of(bodies)
        matches = bodies
          .map { |body| ArelSupport.case_node([[body, 1]], else_value: 0) }
          .reduce { |left, right| ArelSupport.infix("+", left, right) }

        ArelSupport.case_node(
          [[ArelSupport.or_node(bodies.map { |body| ArelSupport.is_null(body) }), nil]],
          else_value: ArelSupport.comparison("eq", matches, 1)
        )
      end

      def lambda_parts(node)
        unless node.is_a?(Plan::Expression) && node.operator == "lambda" && node.operands.length == 2
          raise InvalidPlanError, "Expected a lambda operand, got #{node.inspect}"
        end

        body, iterator = node.operands
        unless iterator.is_a?(Plan::Variable)
          raise InvalidPlanError, "Lambda iterator must be a variable, got #{iterator.inspect}"
        end

        [body, iterator.name]
      end

      def require_collection(operator, value)
        return value.scope if value.is_a?(Values::Collection)

        raise UnmappedAttributeError,
          "#{operator} needs a collection, but its operand resolved to #{describe(value)}; " \
          "map that attribute with Cerbos::ActiveRecord.relation"
      end

      # --- eagerly-resolved operators -------------------------------------------------

      # How many operands each operator takes. A plan that carries more is malformed, and this
      # adapter accepts a plan from any source. If it read only the positions it expected, an
      # extra operand would disappear and the filter would be wider than the condition.
      ARITY = {
        "eq" => 2, "ne" => 2, "lt" => 2, "gt" => 2, "le" => 2, "ge" => 2,
        "in" => 2, "add" => 2, "sub" => 2, "mult" => 2, "div" => 2, "mod" => 2,
        "contains" => 2, "startsWith" => 2, "endsWith" => 2, "hasIntersection" => 2,
        "ancestorOf" => 2, "descendentOf" => 2, "overlaps" => 2,
        "size" => 1, "string" => 1, "double" => 1, "int" => 1, "timestamp" => 1,
        "hierarchy" => 1..2
      }.freeze

      def apply(operator, values)
        assert_arity(operator, values)

        override = operator_overrides[operator]
        return override.call(*values) if override

        case operator
        when *COMPARISONS then compare(operator, values.fetch(0), values.fetch(1))
        when "in" then membership(values.fetch(0), values.fetch(1))
        when "div" then divide(values.fetch(0), values.fetch(1))
        when *ARITHMETIC.keys then arithmetic(operator, values.fetch(0), values.fetch(1))
        when *STRING_MATCHES.keys then string_match(operator, values)
        when "size" then size(values.fetch(0))
        when "hasIntersection" then has_intersection(values.fetch(0), values.fetch(1))
        when "timestamp" then timestamp(values.fetch(0))
        when "string" then cast(values.fetch(0), "TEXT", "VARCHAR")
        when "double" then cast(values.fetch(0), dialect.double_type, dialect.double_type)
        when "int" then cast(values.fetch(0), "INTEGER", "SIGNED")
        when "list" then values
        when "hierarchy" then hierarchy(values)
        when "ancestorOf" then ancestor_of(values.fetch(0), values.fetch(1))
        when "descendentOf" then ancestor_of(values.fetch(1), values.fetch(0))
        when "overlaps" then overlaps(values.fetch(0), values.fetch(1))
        else
          raise UnsupportedOperatorError,
            "Unsupported operator: #{operator}. Supply an operator override if the database " \
            "can express it faithfully."
        end
      end

      def assert_arity(operator, values)
        expected = ARITY[operator]
        return if expected.nil?
        return if expected.is_a?(Range) ? expected.cover?(values.length) : expected == values.length

        raise InvalidPlanError,
          "#{operator} takes #{expected} operands, but the plan gives #{values.length}"
      end

      # --- comparison -----------------------------------------------------------------

      def compare(operator, left, right)
        # This is a ternary that the translator kept. It compares each arm and then makes the
        # branches again. This CASE also has no ELSE clause. Thus an UNKNOWN condition stays
        # UNKNOWN.
        if left.is_a?(Values::ConditionalValue)
          return ArelSupport.case_node([
            [left.condition, compare(operator, left.then_value, right)],
            [ArelSupport.not_node(left.condition), compare(operator, left.else_value, right)]
          ])
        end
        if right.is_a?(Values::ConditionalValue)
          return ArelSupport.case_node([
            [right.condition, compare(operator, left, right.then_value)],
            [ArelSupport.not_node(right.condition), compare(operator, left, right.else_value)]
          ])
        end

        if left.is_a?(Values::IEEEConstant) || right.is_a?(Values::IEEEConstant)
          return compare_non_finite(operator, left, right)
        end

        reject_collection(operator, left)
        reject_collection(operator, right)

        # Both sides are constants. The translator calculates the result here. It does not
        # make SQL that is always true or always false.
        if constant?(left) && constant?(right)
          return fold_comparison(operator, left, right)
        end

        # `= NULL` is never true. In CEL, `null == x` is a usual equality. Thus the adapter
        # puts the other side on the left, and Arel makes IS NULL or IS NOT NULL.
        if left.nil? && %w[eq ne].include?(operator)
          return ArelSupport.comparison(operator, right, nil)
        end

        ArelSupport.comparison(operator, left, right)
      end

      # CEL obeys IEEE-754. NaN is not equal to any value, and it is not equal to itself. It
      # also has no order against any value. PostgreSQL is different, because it puts NaN
      # above all the other doubles. Thus the translator calculates these comparisons here
      # and does not put them into the SQL of the dialect.
      def compare_non_finite(operator, left, right)
        left_value = left.is_a?(Values::IEEEConstant) ? left.value : left
        right_value = right.is_a?(Values::IEEEConstant) ? right.value : right

        nan_side = [left_value, right_value].find { |v| v.is_a?(Float) && v.nan? }
        if nan_side
          other = left_value.equal?(nan_side) ? right_value : left_value
          result = (operator == "ne")

          return result if other.is_a?(Numeric)
          if ArelSupport.arel_node?(other)
            # The translator calculates the comparison for each value that is present. But a
            # missing attribute stays an error. It does not become the true result of `ne`.
            return ArelSupport.case_node([[ArelSupport.is_null(other), nil]], else_value: result)
          end

          raise UnsupportedOperatorError,
            "NaN can only be compared with a number or a column, got #{describe(other)}"
        end

        unless left_value.is_a?(Numeric) && right_value.is_a?(Numeric)
          raise UnsupportedOperatorError,
            "Infinity can only be compared with a number, got " \
            "#{describe(left_value)} and #{describe(right_value)}"
        end

        fold_comparison(operator, left_value, right_value)
      end

      def fold_comparison(operator, left, right)
        case operator
        when "eq" then left == right
        when "ne" then left != right
        when "lt" then left < right
        when "gt" then left > right
        when "le" then left <= right
        when "ge" then left >= right
        end
      end

      # --- membership -----------------------------------------------------------------

      def membership(left, right)
        return relation_membership(right.scope, left) if right.is_a?(Values::Collection)
        return relation_membership(left.scope, right) if left.is_a?(Values::Collection)

        scalar_membership(left, right)
      end

      # +value in R.attr.<relation>+. If the relation of a row is empty, the row stays out of
      # the result. This agrees with the CEL deny for a missing attribute.
      def relation_membership(scope, value)
        member = scope.member_column

        condition =
          if value.nil?
            ArelSupport.comparison("eq", member, nil)
          elsif ArelSupport.arel_node?(value)
            # Two explicit nulls are equal in CEL, but the result in SQL is UNKNOWN. Thus the
            # adapter writes that condition.
            ArelSupport.or_node([
              ArelSupport.comparison("eq", member, value),
              ArelSupport.and_node([
                ArelSupport.comparison("eq", member, nil),
                ArelSupport.comparison("eq", value, nil)
              ])
            ])
          else
            ArelSupport.comparison("eq", member, value)
          end

        scope.exists(condition)
      end

      def scalar_membership(needle, values)
        members = values.is_a?(Array) ? values : [values]
        return false if members.empty?

        # The usual shape: a column against a list of constants. An IN clause reads better than
        # a chain of equality tests.
        if ArelSupport.arel_node?(needle) && members.none? { |member| ArelSupport.arel_node?(member) }
          present = members.reject(&:nil?)

          predicates = []
          unless present.empty?
            predicates << Arel::Nodes::In.new(
              ArelSupport.quote(needle), present.map { |value| ArelSupport.quote(value) }
            )
          end
          # A null element makes the membership test true for an attribute that is null. The
          # attribute must be null and not only missing.
          predicates << ArelSupport.comparison("eq", needle, nil) if present.length != members.length

          return predicates.empty? ? false : ArelSupport.or_node(predicates)
        end

        # A list that holds a column, or a needle that is a constant, needs one comparison for
        # each element. `null in [R.attr.x]` is the example: it is true when the column is null.
        ArelSupport.or_node(members.map { |member| member_equality(needle, member) })
      end

      # CEL equality for one element of a membership test. Two nulls are equal in CEL, but the
      # result of that comparison in SQL is UNKNOWN, so the adapter writes it out.
      def member_equality(needle, member)
        needle_is_node = ArelSupport.arel_node?(needle)
        member_is_node = ArelSupport.arel_node?(member)

        return ArelSupport.comparison("eq", member, nil) if needle.nil? && member_is_node
        return ArelSupport.comparison("eq", needle, nil) if member.nil? && needle_is_node

        if needle_is_node && member_is_node
          return ArelSupport.or_node([
            ArelSupport.comparison("eq", needle, member),
            ArelSupport.and_node([
              ArelSupport.comparison("eq", needle, nil),
              ArelSupport.comparison("eq", member, nil)
            ])
          ])
        end

        return needle.nil? == member.nil? if needle.nil? || member.nil?

        ArelSupport.to_predicate(compare("eq", needle, member))
      end

      # --- arithmetic -----------------------------------------------------------------

      def arithmetic(operator, left, right)
        reject_collection(operator, left)
        reject_collection(operator, right)
        # A division that can give a value which is not finite stays as branches until a
        # comparison calculates it. More arithmetic on those branches has no SQL equivalent,
        # so the adapter raises instead of making an incorrect filter.
        reject_deferred(operator, left)
        reject_deferred(operator, right)

        # CEL uses `+` for strings and for numbers. SQL does not. On SQLite and MySQL,
        # `'a' + 'b'` is an addition of numbers, and it changes both sides into 0. Thus string
        # operands need the concatenation operation of the dialect.
        if operator == "add" && (string_valued?(left) || string_valued?(right))
          return left + right if left.is_a?(::String) && right.is_a?(::String)

          return dialect.concat(left, right)
        end

        if left.is_a?(Numeric) && right.is_a?(Numeric)
          return left.public_send(ARITHMETIC.fetch(operator), right)
        end

        ArelSupport.infix(ARITHMETIC.fetch(operator), left, right)
      end

      def string_valued?(value)
        return true if value.is_a?(::String)

        %i[string text].include?(column_type(value))
      end

      # Cerbos sends each number as a double, and CEL arithmetic on attributes uses doubles.
      # Thus the division must also use doubles. If it did not, SQLite and PostgreSQL would do
      # an integer division and change +5 / 2+ into +2+.
      def divide(numerator, denominator)
        reject_collection("div", numerator)
        reject_collection("div", denominator)
        reject_deferred("div", numerator)
        reject_deferred("div", denominator)

        if numerator.is_a?(Numeric) && denominator.is_a?(Numeric)
          return divide_constants(numerator.to_f, denominator.to_f)
        end

        # A constant denominator that is not zero can never divide by zero. Thus a plain
        # division is exact, and it keeps the SQL small.
        if denominator.is_a?(Numeric) && !denominator.to_f.zero?
          return ArelSupport.infix("/", as_double(numerator), denominator.to_f)
        end

        divide_with_zero_denominator(numerator, denominator)
      end

      # IEEE-754 keeps the sign of a zero. `2.0 / -0.0` is -Infinity, not +Infinity, because the
      # sign of the result is the sign of the numerator against the sign of the denominator.
      def zero_sign(denominator)
        return 1.0 unless denominator.is_a?(Numeric)

        (1.0 / denominator.to_f).negative? ? -1.0 : 1.0
      end

      # A division by zero is not an error in CEL, because CEL arithmetic on attributes uses
      # doubles. IEEE-754 gives NaN for 0/0, +Infinity for a positive numerator, and -Infinity
      # for a negative one. SQL cannot hold those three values, and NULL is not equal to any of
      # them: `NaN != 1.0` is TRUE in CEL, but `NULL != 1.0` is UNKNOWN in SQL, and thus a
      # NULL would remove a row that the PDP permits.
      #
      # The translator keeps the three cases as branches. The comparison around the division
      # then calculates each branch, in the same way as any other constant that is not finite.
      def divide_with_zero_denominator(numerator, denominator)
        # The denominator is a constant zero, so the sign of that zero is known.
        return zero_denominator_value(numerator, zero_sign(denominator)) if denominator.is_a?(Numeric)

        # The denominator is row-dependent. SQL cannot tell -0.0 from 0.0 — both satisfy
        # `= 0` and no portable function reads the sign bit — so the sign of an Infinity is
        # unknowable here. The one shape that stays safe is a division of a value by itself:
        # the denominator can only be zero when the numerator is zero too, which gives NaN,
        # and NaN has no sign question.
        unless numerator == denominator
          raise UnsupportedOperatorError,
            "Cannot divide by a column that may be zero: IEEE-754 keeps the sign of a zero, " \
            "SQL cannot tell -0.0 from 0.0, and thus the sign of the Infinity is unknown. " \
            "Divide by a constant, or keep this shape out of the policy."
        end

        Values::ConditionalValue.new(
          condition: ArelSupport.comparison("eq", as_double(denominator), 0.0),
          then_value: Values::IEEEConstant.new(value: Float::NAN),
          else_value: ArelSupport.infix("/", as_double(numerator), as_double(denominator))
        )
      end

      def zero_denominator_value(numerator, sign)
        if numerator.is_a?(Numeric)
          return Values::IEEEConstant.new(value: numerator.to_f / (sign * 0.0))
        end

        Values::ConditionalValue.new(
          condition: ArelSupport.comparison("eq", as_double(numerator), 0.0),
          then_value: Values::IEEEConstant.new(value: Float::NAN),
          else_value: Values::ConditionalValue.new(
            condition: ArelSupport.comparison("gt", as_double(numerator), 0.0),
            then_value: Values::IEEEConstant.new(value: sign * Float::INFINITY),
            else_value: Values::IEEEConstant.new(value: -sign * Float::INFINITY)
          )
        )
      end

      def divide_constants(numerator, denominator)
        return Values::IEEEConstant.new(value: numerator / denominator) if denominator.zero?

        numerator / denominator
      end

      def as_double(value)
        return value.to_f if value.is_a?(Numeric)

        cast(value, dialect.double_type, dialect.double_type)
      end

      def cast(value, type, mysql_type)
        return value if value.nil?

        Arel::Nodes::NamedFunction.new(
          "CAST",
          [Arel::Nodes::As.new(ArelSupport.quote(value), Arel.sql(dialect.mysql? ? mysql_type : type))]
        )
      end

      # --- strings --------------------------------------------------------------------

      def string_match(operator, values)
        receiver = values.fetch(0)
        needle = values.fetch(1)
        reject_collection(operator, receiver)
        reject_collection(operator, needle)

        matcher.match(receiver, needle, **STRING_MATCHES.fetch(operator))
      end

      def size(target)
        case target
        when Values::Collection
          # size() counts the elements and does not evaluate them. Thus it also counts a
          # member column that is NULL. No error is possible, and no guard is necessary.
          target.scope.count
        when Values::FilteredCollection
          # filter() is different from exists(). It never ignores an element that made an
          # error. Thus one body with an UNKNOWN result makes the full count unknown.
          ArelSupport.case_node(
            [[target.scope.exists(ArelSupport.is_null(target.body)), nil]],
            else_value: target.scope.count(target.body)
          )
        when ::String
          target.length
        else
          dialect.char_length(target)
        end
      end

      def has_intersection(left, right)
        # hasIntersection gives the same result if the operands change sides. The planner
        # keeps the order of the source. Thus the list of literals can come on each side.
        left, right = right, left if left.is_a?(Array) && !right.is_a?(Array)
        values = right.is_a?(Array) ? right : [right]

        case left
        when Values::Collection
          left.scope.exists(scalar_membership(left.scope.member_column, values))
        when Values::MappedCollection
          # map() makes an error for each element that makes an error, and it ignores no
          # errors. Thus the guard for the error must come before the test for a true
          # element.
          ArelSupport.case_node(
            [
              [left.scope.exists(ArelSupport.is_null(left.projection)), nil],
              [left.scope.exists(scalar_membership(left.projection, values)), true]
            ],
            else_value: false
          )
        else
          raise UnmappedAttributeError,
            "hasIntersection needs a collection, got #{describe(left)}"
        end
      end

      TEMPORAL_COLUMN_TYPES = %i[datetime timestamp timestamptz time date].freeze

      def timestamp(value)
        return value if value.is_a?(::Time)
        return Timestamps.parse(value) if value.is_a?(::String)

        unless ArelSupport.arel_node?(value)
          raise UnsupportedOperatorError,
            "timestamp() needs an RFC-3339 literal or a temporal column, got #{describe(value)}"
        end

        type = column_type(value)
        return value if TEMPORAL_COLUMN_TYPES.include?(type)

        # A comparison between a string column and a Time compares two different text
        # formats. ActiveRecord makes `2025-01-01 00:00:00`, but an RFC-3339 column holds
        # `2025-01-01T00:00:00Z`. Thus the order of the results comes from the text and not
        # from the instants. The adapter refuses this shape and does not make the SQL.
        raise UnsupportedOperatorError,
          "timestamp() applied to a #{type.inspect} column: this adapter compares instants " \
          "using the database's own temporal type, so the attribute must map to a datetime " \
          "column rather than to a column holding a formatted timestamp string"
      end

      # --- hierarchy ------------------------------------------------------------------

      def hierarchy(values)
        value = values.fetch(0)
        delimiter = values.fetch(1, nil) || "."

        unless delimiter.is_a?(::String) && !delimiter.empty?
          raise InvalidPlanError, "hierarchy() delimiter must be a non-empty string"
        end

        if value.is_a?(Array)
          return Values::Hierarchy.new(value: nil, segments: value, delimiter: delimiter)
        end

        Values::Hierarchy.new(value: value, segments: nil, delimiter: delimiter)
      end

      def matching_hierarchies(left, right)
        unless left.is_a?(Values::Hierarchy) && right.is_a?(Values::Hierarchy)
          raise UnsupportedOperatorError, "Hierarchy operators need hierarchy() operands"
        end
        # A hierarchy from a list is already in segments. Thus its delimiter has no meaning,
        # and a comparison with a path that has a different delimiter is correct.
        if left.segments.nil? && right.segments.nil? && left.delimiter != right.delimiter
          raise UnsupportedOperatorError,
            "Hierarchy operands use different delimiters: " \
            "#{left.delimiter.inspect} and #{right.delimiter.inspect}"
        end

        [left, right]
      end

      # Gives the segments of a hierarchy, if the adapter can know them during the
      # translation. A column is a string with a delimiter until the query runs. Thus a column
      # has no segments here.
      def segments_of(hierarchy)
        return hierarchy.segments if hierarchy.segments
        return hierarchy.value.split(hierarchy.delimiter, -1) if hierarchy.value.is_a?(::String)

        nil
      end

      # Compares the segments one by one. The adapter uses this when one side or the other
      # side came from a list. It must be possible to get the segments of both sides. SQL
      # cannot divide a column into segments.
      def require_segments(hierarchy)
        segments = segments_of(hierarchy)
        return segments if segments

        raise UnsupportedOperatorError,
          "A hierarchy built from a list can only be compared against another hierarchy " \
          "whose segments are known when the query is built; this one is a column"
      end

      def segment_wise?(left, right)
        !left.segments.nil? || !right.segments.nil?
      end

      def hierarchy_equal(left, right)
        unless segment_wise?(left, right)
          return compare("eq", left.value, right.value)
        end

        above = require_segments(left)
        below = require_segments(right)
        return false if above.length != below.length

        ArelSupport.and_node(
          above.each_with_index.map { |segment, index| as_predicate(compare("eq", segment, below[index])) }
        )
      end

      def ancestor_of(ancestor_node, descendent_node)
        ancestor, descendent = matching_hierarchies(ancestor_node, descendent_node)
        delimiter = ancestor.delimiter
        above = ancestor.value
        below = descendent.value

        if segment_wise?(ancestor, descendent)
          above_segments = require_segments(ancestor)
          below_segments = require_segments(descendent)
          # An ancestor is a shorter path, and each of its segments agrees with the segment in
          # the same position in the other path.
          return false if above_segments.length >= below_segments.length

          return ArelSupport.and_node(
            above_segments.each_with_index.map do |segment, index|
              as_predicate(compare("eq", segment, below_segments[index]))
            end
          )
        end

        if above.is_a?(::String) && below.is_a?(::String)
          return below.start_with?(above + delimiter)
        end

        if below.is_a?(::String)
          # A descendant that is a constant has a known and limited set of ancestors. The
          # adapter compares them exactly. A LIKE operation would need an escape character for
          # its metacharacters.
          parts = below.split(delimiter, -1)
          prefixes = (1...parts.length).map { |i| parts[0, i].join(delimiter) }
          return scalar_membership(above, prefixes)
        end

        if above.is_a?(::String)
          return matcher.match(below, above + delimiter, prefix: false, suffix: true)
        end

        raise UnsupportedOperatorError,
          "Hierarchy comparison between two columns is not supported: the descendant test " \
          "needs a literal prefix to match against"
      end

      def overlaps(left_node, right_node)
        left, right = matching_hierarchies(left_node, right_node)

        ArelSupport.or_node([
          as_predicate(hierarchy_equal(left, right)),
          as_predicate(ancestor_of(left, right)),
          as_predicate(ancestor_of(right, left))
        ])
      end

      # --- helpers --------------------------------------------------------------------

      def constant?(value)
        !ArelSupport.arel_node?(value) && !collection?(value) && !value.nil?
      end

      def collection?(value)
        value.is_a?(Values::Collection) ||
          value.is_a?(Values::FilteredCollection) ||
          value.is_a?(Values::MappedCollection)
      end

      def reject_deferred(operator, value)
        return unless deferred_value?(value)

        raise UnsupportedOperatorError,
          "#{operator} cannot take an operand that may be NaN or Infinity: only a comparison " \
          "can resolve those values without binding them into SQL"
      end

      def reject_collection(operator, value)
        return unless collection?(value)

        raise UnmappedAttributeError,
          "#{operator} cannot take a collection operand (#{describe(value)})"
      end

      def describe(value)
        case value
        when Values::Collection then "a relation"
        when Values::FilteredCollection then "a filtered relation"
        when Values::MappedCollection then "a projected relation"
        when Values::Hierarchy then "a hierarchy"
        else "#{value.inspect} (#{value.class})"
        end
      end

      # Resolves the plan variables with the attribute map from the caller. It also resolves
      # the iterator variables that the collection macros around them connected to a scope.
      class Environment
        def initialize(translator:, bindings:)
          @translator = translator
          @bindings = bindings
        end

        attr_reader :translator, :bindings

        def bind(name, scope)
          self.class.new(translator: translator, bindings: bindings.merge(name => scope))
        end

        def resolve(name)
          mapping = translator.attributes[name]
          return resolve_mapping(mapping, translator.model, translator.root_table) if mapping

          head, rest = name.split(".", 2)
          unless bindings.key?(head)
            raise UnmappedAttributeError,
              "No mapping for attribute #{name.inspect}. Add it to the attributes hash " \
              "passed to Cerbos::ActiveRecord.query_plan_to_relation."
          end

          scope = bindings[head]

          # A macro over a list of constants binds the iterator to an element of that list.
          # An element is a value and has no fields, so a reference with a dot is an error.
          unless scope.is_a?(Relations::Scope)
            return scope if rest.nil?

            raise UnmappedAttributeError,
              "#{name.inspect} reads the field #{rest.inspect} from #{head.inspect}, but " \
              "#{head.inspect} is an element of a list of constants and has no fields"
          end

          return element(scope) if rest.nil?

          member = scope.mapping&.fields&.[](rest)
          unless member
            raise UnmappedAttributeError,
              "Relation #{scope.mapping&.association.inspect} has no mapping for member " \
              "field #{rest.inspect} (referenced as #{name.inspect})"
          end

          resolve_mapping(member, scope.model, scope.table)
        end

        private

        def element(scope)
          scope.mapping&.member_field ? scope.member_column : Values::Collection.new(scope: scope)
        end

        def resolve_mapping(mapping, owner_model, owner_table)
          case mapping
          when AttributeMapping::Field then resolve_field(mapping, owner_model, owner_table)
          when AttributeMapping::Relation
            Values::Collection.new(
              scope: Relations.build(
                owner_model: owner_model,
                owner_table: owner_table,
                mapping: mapping,
                aliaser: translator.aliaser
              )
            )
          else
            raise UnmappedAttributeError, "Unrecognised attribute mapping: #{mapping.inspect}"
          end
        end

        def resolve_field(mapping, owner_model, owner_table)
          segments = mapping.segments
          if segments.length == 1
            return translator.register_column_type(
              owner_table[segments.first], owner_model, segments.first
            )
          end

          *associations, column = segments
          scope = Relations.build_path(
            owner_model: owner_model,
            owner_table: owner_table,
            association_names: associations,
            aliaser: translator.aliaser
          )
          translator.register_column_type(scope.scalar(column), scope.model, column)
        end
      end
    end
  end
end
