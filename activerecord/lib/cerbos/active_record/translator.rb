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

require_relative "translator/arithmetic"
require_relative "translator/casts"
require_relative "translator/collections"
require_relative "translator/comparisons"
require_relative "translator/environment"
require_relative "translator/hierarchies"
require_relative "translator/membership"
require_relative "translator/null_conventions"
require_relative "translator/strings"

module Cerbos
  module ActiveRecord
    # Walks a normalised query plan and builds the equivalent Arel predicate.
    #
    # Two rules drive most decisions:
    #
    # * **Operand order is source order.** `1 < R.attr.x` arrives as
    #   `lt(value(1), variable(x))` and becomes `1 < x`. Never swap operands to put the column
    #   first; that flipped comparisons in other adapters (cerbos/query-plan-adapters#257).
    #
    # * **An error is not false.** CEL denies when evaluation errors (missing attribute,
    #   missing field). SQL UNKNOWN behaves the same: excluded by a predicate and by its
    #   negation. So UNKNOWN is kept, not coerced to a boolean, which is why collection macros
    #   become CASE expressions, not just EXISTS.
    #
    # This file holds the plan walk and the operator table. Each operator family is a module
    # in `translator/`.
    #
    # @see Cerbos::ActiveRecord.query_plan_to_relation
    class Translator
      # Operators whose operands must not be resolved first: they bind an iterator variable or
      # must carry UNKNOWN through a branch.
      STRUCTURAL_OPERATORS = %w[and or not if lambda exists all exists_one filter map].freeze

      # The comparison operators.
      COMPARISONS = %w[eq ne lt gt le ge].freeze

      # The arithmetic operators, except `div`, and their SQL operators.
      ARITHMETIC = {"add" => "+", "sub" => "-", "mult" => "*", "mod" => "%"}.freeze

      # String operators that become `LIKE`, and where each adds a wildcard.
      STRING_MATCHES = {
        "contains" => {prefix: true, suffix: true},
        "startsWith" => {prefix: false, suffix: true},
        "endsWith" => {prefix: true, suffix: false}
      }.freeze

      # The operators whose result CEL holds as a boolean, so `string()` over one spells
      # `"true"`/`"false"` rather than whatever the database renders a predicate as. `if` is
      # boolean only when an arm is: see {#ternary}.
      BOOLEAN_OPERATORS = (
        %w[and or not exists all exists_one in hasIntersection ancestorOf descendentOf overlaps] +
        COMPARISONS + STRING_MATCHES.keys
      ).freeze

      # The values that `null_attribute_representation` accepts. See
      # {AttributeMapping::NULL_REPRESENTATIONS}.
      NULL_REPRESENTATIONS = AttributeMapping::NULL_REPRESENTATIONS

      # Operators CEL evaluates to a definite boolean over null, so the only ones an
      # attribute's declared convention affects. All others use the call's convention.
      EQUALITY_FAMILY = %w[eq ne in].freeze

      # The ActiveRecord column types that hold a CEL string.
      STRING_COLUMN_TYPES = %i[string text].freeze
      # The ActiveRecord column types that hold a whole number.
      INTEGER_COLUMN_TYPES = %i[integer bigint].freeze
      # The ActiveRecord column types that hold a CEL number.
      NUMERIC_COLUMN_TYPES = %i[integer bigint float decimal].freeze
      # The ActiveRecord column types that hold a CEL number exactly, not as a double.
      EXACT_NUMERIC_COLUMN_TYPES = %i[integer bigint decimal].freeze
      # CEL's int range. A wider whole number in a plan can only be a double.
      INT64_RANGE = (-(2**63))..(2**63 - 1)
      # The ActiveRecord column types that hold an instant.
      TEMPORAL_COLUMN_TYPES = %i[datetime timestamp timestamptz time date].freeze

      include Arithmetic
      include Casts
      include Collections
      include Comparisons
      include Hierarchies
      include Membership
      include NullConventions
      include Strings

      # An operator whose operands are resolved first, so an override can replace it.
      # `translation` runs on the translator with the resolved operands.
      #
      # @attr arity [Integer, Range<Integer>, nil] the operand count (`nil` for any).
      # @attr translation [Proc] the translation of the resolved operands.
      Operator = Struct.new(:arity, :translation)

      # Every non-structural operator. To add one, add an entry here and its method.
      #
      # Arity is checked first: silently dropping an extra operand from a malformed plan could
      # widen the filter.
      OPERATORS = {
        **COMPARISONS.to_h { |name|
          [name, Operator.new(2, ->(left, right) { compare(name, left, right) })]
        },
        **ARITHMETIC.keys.to_h { |name|
          [name, Operator.new(2, ->(left, right) { arithmetic(name, left, right) })]
        },
        **STRING_MATCHES.keys.to_h { |name|
          [name, Operator.new(2, ->(receiver, needle) { string_match(name, receiver, needle) })]
        },
        "div" => Operator.new(2, ->(numerator, denominator) { divide(numerator, denominator) }),
        "in" => Operator.new(2, ->(needle, haystack) { membership(needle, haystack) }),
        "hasIntersection" => Operator.new(2, ->(left, right) { has_intersection(left, right) }),
        "size" => Operator.new(1, ->(target) { size(target) }),
        "timestamp" => Operator.new(1, ->(value) { timestamp(value) }),
        "string" => Operator.new(1, ->(value) { cast_to_string(value) }),
        "double" => Operator.new(1, ->(value) { cast_to_double(value) }),
        "int" => Operator.new(1, ->(value) { cast_to_int(value) }),
        "list" => Operator.new(nil, ->(*values) { values }),
        "hierarchy" => Operator.new(1..2, ->(value, delimiter = nil) { hierarchy(value, delimiter) }),
        "ancestorOf" => Operator.new(2, ->(ancestor, descendent) { ancestor_of(ancestor, descendent) }),
        "descendentOf" => Operator.new(2, ->(descendent, ancestor) { ancestor_of(ancestor, descendent) }),
        "overlaps" => Operator.new(2, ->(left, right) { overlaps(left, right) })
      }.freeze

      # Operand counts from OPERATORS. Kept for compatibility with code that used it before.
      ARITY = OPERATORS.filter_map { |name, operator| [name, operator.arity] if operator.arity }.to_h.freeze

      # Create a translator.
      #
      # @param model [Class] the model to filter.
      # @param attributes [Hash{String, Symbol => AttributeMapping::Field, AttributeMapping::Relation}]
      #   the attribute map.
      # @param operator_overrides [Hash{String, Symbol => #call}] the operator overrides.
      # @param null_attribute_representation [Symbol, String] the fallback NULL convention.
      #
      # @raise [ArgumentError] when `null_attribute_representation` is not in
      #   {NULL_REPRESENTATIONS}, or an override names one of {STRUCTURAL_OPERATORS}.
      #
      # @see Cerbos::ActiveRecord.query_plan_to_relation
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

      # @!attribute [r] model
      #   @return [Class] the model to filter.

      # @!attribute [r] attributes
      #   @return [Hash{String => AttributeMapping::Field, AttributeMapping::Relation}] the
      #     attribute map.

      # @!attribute [r] operator_overrides
      #   @return [Hash{String => #call}] the operator overrides.

      # @!attribute [r] null_attribute_representation
      #   @return [:explicit, :omitted] the fallback NULL convention.

      # @!attribute [r] dialect
      #   @private

      # @!attribute [r] matcher
      #   @private

      # Translate a query plan into a relation over {#model}.
      #
      # @param plan [Object] the query plan, in any shape
      #   {Cerbos::ActiveRecord.query_plan_to_relation} accepts.
      #
      # @return [ActiveRecord::Relation] a filtered relation.
      # @return [ActiveRecord::Relation] `model.none` if the plan always denies.
      # @return [ActiveRecord::Relation] `model.all` if the plan always allows.
      #
      # @raise [Error] when the adapter cannot translate the plan correctly.
      def translate(plan)
        normalised = Plan.normalise(plan)
        return model.none if normalised.always_denied?
        return model.all if normalised.always_allowed?

        # Always check: an attribute can declare `:omitted` even when the call is `:explicit`.
        assert_no_null_operands(normalised.condition)

        @aliaser = Relations::Aliaser.new
        # Keyed by identity: each resolved column is a fresh Arel node passed through unchanged.
        @column_types = {}.compare_by_identity
        @timestamp_operands = {}.compare_by_identity
        @cel_types = {}.compare_by_identity
        @null_representations = {}.compare_by_identity
        @omitted_attributes = {}.compare_by_identity
        environment = Environment.new(translator: self, bindings: {})
        model.where(predicate(normalised.condition, environment))
      end

      # @private
      attr_reader :aliaser

      # @private
      def register_column_type(node, owner_model, column_name)
        type = owner_model.columns_hash[column_name.to_s]&.type
        @column_types[node] = type if type
        node
      end

      # @private
      def column_type(node)
        @column_types[node]
      end

      # Records an attribute's declared NULL convention against its resolved Arel node, since
      # operators only see resolved values.
      #
      # @private
      def register_null_representation(node, representation)
        @null_representations[node] = representation if representation
        node
      end

      # Records the column a top-level field attribute resolved to when its convention, declared
      # or inherited from the call, is `:omitted`. Only such a column may meet a null constant in
      # `eq` or `ne`; {#assert_no_null_operands} refuses every other null operand under `:omitted`.
      #
      # @private
      def register_attribute_field(node, mapping)
        if mapping.is_a?(AttributeMapping::Field) &&
            (mapping.null_representation || null_attribute_representation) == :omitted
          @omitted_attributes[node] = true
        end
        node
      end

      # True if the node is a top-level field attribute under `:omitted`. See
      # {#register_attribute_field}.
      #
      # @private
      def omitted_attribute?(node)
        @omitted_attributes.key?(node)
      end

      # True if the node's attribute declares `:explicit`. Only then does CEL see a null value,
      # so only then is a definite comparison needed.
      #
      # @private
      def explicit_null?(node)
        @null_representations[node] == :explicit
      end

      # @private
      def root_table
        model.arel_table
      end

      # Resolves an operand to an Arel node, a Ruby constant, or an intermediate {Values} value.
      #
      # @private
      def evaluate(node, environment)
        case node
        when Plan::Value then constant(node.value)
        when Plan::Variable then environment.resolve(node.name)
        when Plan::Expression then evaluate_expression(node, environment)
        else raise InvalidPlanError, "Unrecognised query plan node: #{node.inspect}"
        end
      end

      private

      # A plan number reaches Ruby through JSON, which decodes a whole double such as -1e19 as
      # an Integer. Beyond int64 it can only be a double, and ActiveRecord refuses to bind such
      # an Integer on PostgreSQL (IntegerOutOf64BitRange), so hold it as the Float it is.
      def constant(value)
        case value
        when Integer then INT64_RANGE.cover?(value) ? value : value.to_f
        when Array then value.map { |element| constant(element) }
        else value
        end
      end

      def evaluate_expression(node, environment)
        result = evaluate_operator(node.operator, node.operands, environment)
        BOOLEAN_OPERATORS.include?(node.operator) ? record_boolean(result) : result
      end

      def evaluate_operator(operator, operands, environment)
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

        reject_double_text("a condition", value)

        # A bare boolean column is valid CEL, but `where` rejects a bare column and PostgreSQL
        # wants a boolean expression. `= TRUE` gives the same result, NULL included.
        return ArelSupport.comparison("eq", value, true) if column_type(value) == :boolean

        ArelSupport.to_predicate(value)
      end

      # --- structural operators -------------------------------------------------------

      # An empty `and` would be TRUE and allow every row. The planner never emits it, but a
      # truncated or mangled plan might, so it is an error.
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

      # `if(condition, then, else)`.
      #
      # No ELSE on purpose. An UNKNOWN condition (NULL column, missing attribute) is a CEL
      # error and a deny. With no matching WHEN the CASE is NULL, so the row is excluded even
      # under NOT. An ELSE would let it in.
      def ternary(operands, environment)
        unless operands.length == 3
          raise InvalidPlanError, "if takes exactly three operands, got #{operands.length}"
        end

        condition = predicate(operands[0], environment)
        then_value = evaluate(operands[1], environment)
        else_value = evaluate(operands[2], environment)

        reject_double_text("if", then_value)
        reject_double_text("if", else_value)

        # A non-finite arm must not reach SQL, so defer to the enclosing comparison.
        if deferred_value?(then_value) || deferred_value?(else_value)
          return Values::ConditionalValue.new(
            condition: condition, then_value: then_value, else_value: else_value
          )
        end

        result = branches(condition, then_value, else_value)
        (boolean_arm?(then_value) || boolean_arm?(else_value)) ? record_boolean(result) : result
      end

      def boolean_arm?(value)
        value == true || value == false || boolean_value?(value)
      end

      # Records that CEL holds a node as a boolean. See {Casts#boolean_value?}.
      def record_boolean(value)
        ArelSupport.arel_node?(value) ? record_cel_type(value, :bool) : value
      end

      # `CASE WHEN c THEN a WHEN NOT c THEN b END`. No ELSE, so UNKNOWN gives NULL. See
      # {#ternary}.
      def branches(condition, then_value, else_value)
        ArelSupport.case_node(
          [[condition, then_value], [ArelSupport.not_node(condition), else_value]]
        )
      end

      # --- operators with resolved operands -------------------------------------------

      def apply(operator, values)
        assert_arity(operator, values)
        assert_uniform_null_conventions(operator, values)

        override = operator_overrides[operator]
        # Only the built-in eq and ne can resolve string() of a double.
        values.each { |value| reject_double_text(operator, value) } if override || !%w[eq ne].include?(operator)
        plain = override ? override.call(*values) : dispatch(operator, values)

        with_null_conventions(operator, values, plain, overridden: !override.nil?)
      end

      def dispatch(operator, values)
        translation = OPERATORS[operator]&.translation
        unless translation
          raise UnsupportedOperatorError,
            "Unsupported operator: #{operator}. Supply an operator override if the database " \
            "can express it faithfully."
        end

        instance_exec(*values, &translation)
      end

      def assert_arity(operator, values)
        expected = OPERATORS[operator]&.arity
        return if expected.nil?
        return if expected.is_a?(Range) ? expected.cover?(values.length) : expected == values.length

        raise InvalidPlanError,
          "#{operator} takes #{expected} operands, but the plan gives #{values.length}"
      end

      # --- helpers --------------------------------------------------------------------

      def record_column_type(node, type)
        @column_types[node] = type
        node
      end

      def constant?(value)
        !ArelSupport.arel_node?(value) && !collection?(value) && !value.nil?
      end

      def collection?(value)
        value.is_a?(Values::Collection) ||
          value.is_a?(Values::FilteredCollection) ||
          value.is_a?(Values::MappedCollection)
      end

      # A value that may be NaN or Infinity, kept out of SQL until a comparison resolves it.
      # See {Values::IEEEConstant}.
      def deferred_value?(value)
        value.is_a?(Values::IEEEConstant) || value.is_a?(Values::ConditionalValue)
      end

      # Refuses collections, then possibly non-finite values, for operators that need scalars.
      def require_scalars(operator, *values)
        values.each { |value| reject_collection(operator, value) }
        values.each { |value| reject_deferred(operator, value) }
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
        when Values::DoubleText then "string() of a double"
        else "#{value.inspect} (#{value.class})"
        end
      end
    end
  end
end
