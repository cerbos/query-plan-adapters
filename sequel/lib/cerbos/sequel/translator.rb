# frozen_string_literal: true

require_relative "sql_support"
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
  module Sequel
    # Goes through a query plan after a normalise operation, and makes the equivalent Sequel
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
    #
    # This file holds the walk over the plan and the table of operators. Each family of
    # operators is a module in +translator/+, included below.
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

      # The operators whose result CEL holds as a boolean, so `string()` over one spells
      # `"true"`/`"false"` rather than whatever the database renders a predicate as. `if` is
      # boolean only when an arm is: see {#ternary}.
      BOOLEAN_OPERATORS = (
        %w[and or not exists all exists_one in hasIntersection ancestorOf descendentOf overlaps] +
        COMPARISONS + STRING_MATCHES.keys
      ).freeze

      NULL_REPRESENTATIONS = AttributeMapping::NULL_REPRESENTATIONS

      # The operators that CEL evaluates to a definite boolean over a null value, and thus the
      # only ones that the declared convention of an attribute can settle. Everything else — a
      # collection macro, `hasIntersection`, a string match — keeps the convention of the call,
      # because the declaration says nothing about the meaning of its null there.
      EQUALITY_FAMILY = %w[eq ne in].freeze

      # The column types as +Model.db_schema+ reports them. Sequel folds the spellings of each
      # dialect into these symbols: +bigint+ is +:integer+, +text+ and +varchar+ are +:string+,
      # +double precision+ and +real+ are +:float+.
      STRING_COLUMN_TYPES = %i[string].freeze
      INTEGER_COLUMN_TYPES = %i[integer].freeze
      NUMERIC_COLUMN_TYPES = %i[integer float decimal].freeze
      # The column types that hold a CEL number exactly, not as a double.
      EXACT_NUMERIC_COLUMN_TYPES = %i[integer decimal].freeze
      # CEL's int range. A wider whole number in a plan can only be a double.
      INT64_RANGE = (-(2**63))..(2**63 - 1)
      TEMPORAL_COLUMN_TYPES = %i[datetime time date].freeze

      include Arithmetic
      include Casts
      include Collections
      include Comparisons
      include Hierarchies
      include Membership
      include NullConventions
      include Strings

      # An operator that the adapter resolves the operands of before it runs, and that an
      # operator override can therefore replace. +arity+ is how many operands it takes (+nil+
      # for any number). +translation+ gets the resolved operands and runs on the translator.
      Operator = Struct.new(:arity, :translation)

      # Every operator that is not structural. Adding one is one entry here, plus the method
      # that it calls.
      #
      # The arity is checked before the translation runs. A plan that carries more operands
      # than the operator takes is malformed, and this adapter accepts a plan from any source.
      # If it read only the positions it expected, an extra operand would disappear and the
      # filter would be wider than the condition.
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
        "set-field" => Operator.new(2, ->(key, value) { map_entry(key, value) }),
        "struct" => Operator.new(nil, ->(*entries) { map_literal(entries) }),
        "hierarchy" => Operator.new(1..2, ->(value, delimiter = nil) { hierarchy(value, delimiter) }),
        "ancestorOf" => Operator.new(2, ->(ancestor, descendent) { ancestor_of(ancestor, descendent) }),
        "descendentOf" => Operator.new(2, ->(descendent, ancestor) { ancestor_of(ancestor, descendent) }),
        "overlaps" => Operator.new(2, ->(left, right) { overlaps(left, right) })
      }.freeze

      # Operand counts, derived from OPERATORS. Kept because the constant was reachable before
      # OPERATORS replaced it.
      ARITY = OPERATORS.filter_map { |name, operator| [name, operator.arity] if operator.arity }.to_h.freeze

      def initialize(model:, attributes:, operator_overrides: {}, null_attribute_representation: :explicit)
        @dataset, @model = resolve_model(model)
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

      attr_reader :model, :dataset, :attributes, :operator_overrides, :dialect, :matcher,
        :null_attribute_representation

      # @param plan [Object] anything {Plan.normalise} accepts
      # @return [::Sequel::Dataset]
      def translate(plan)
        normalised = Plan.normalise(plan)
        # `where(false)` and not an empty array: the result stays a dataset that a caller can
        # compose with, count and page through, exactly as a conditional plan does.
        return dataset.where(false) if normalised.always_denied?
        return dataset if normalised.always_allowed?

        # Always, and not only under `:omitted`. The option of the call is now the fallback:
        # an attribute can declare `:omitted` while the call declares `:explicit`.
        assert_no_null_operands(normalised.condition)

        @aliaser = Relations::Aliaser.new
        # The keys are object identities. Each column that the adapter resolves is a new Sequel
        # expression, and that same object goes through the translation without a change. Thus
        # identity is the correct comparison here.
        @column_types = {}.compare_by_identity
        @timestamp_operands = {}.compare_by_identity
        @cel_types = {}.compare_by_identity
        @null_representations = {}.compare_by_identity
        @omitted_attributes = {}.compare_by_identity
        environment = Environment.new(translator: self, bindings: {})
        dataset.where(predicate(normalised.condition, environment))
      end

      # @api private
      attr_reader :aliaser

      # @api private
      def register_column_type(node, owner_model, column_name)
        column = owner_model&.db_schema&.[](column_name.to_sym)
        type = column && (column[:type] || fractional_temporal_type(column[:db_type]))
        @column_types[node] = type if type
        node
      end

      # Sequel from 5.32 until 5.91 types a fractional-second `datetime(6)` column as nil,
      # although it types `datetime` and `timestamp(6)` as +:datetime+ (jeremyevans/sequel#2293).
      # An untyped column is refused by `timestamp()`, so a MySQL schema that keeps microseconds
      # would lose every timestamp comparison; read the precision-qualified spelling here.
      def fractional_temporal_type(db_type)
        :datetime if db_type.to_s.match?(/\Adatetime\(\d+\)\z/i)
      end

      # @api private
      def column_type(node)
        @column_types[node]
      end

      # Records the convention that the mapping of an attribute declares, against the identity
      # of the Sequel expression that the attribute resolved to. The declaration arrives with the
      # mapping, but the operators see only resolved values, so the node carries it across.
      #
      # @api private
      def register_null_representation(node, representation)
        @null_representations[node] = representation if representation
        node
      end

      # Records the column a top-level field attribute resolved to when its convention, declared
      # or inherited from the call, is `:omitted`. Only such a column may meet a null constant in
      # `eq` or `ne`; {#assert_no_null_operands} refuses every other null operand under `:omitted`.
      #
      # @api private
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
      # @api private
      def omitted_attribute?(node)
        @omitted_attributes.key?(node)
      end

      # True if this node came from an attribute that the caller declares it sends as an
      # explicit null. An attribute that declares `:omitted`, and one that declares nothing,
      # are both false: only `:explicit` puts a null VALUE into CEL, and thus only `:explicit`
      # needs a comparison that is definite.
      #
      # @api private
      def explicit_null?(node)
        @null_representations[node] == :explicit
      end

      # @api private
      def root_table
        ::Sequel[model.table_name]
      end

      # A field of a constant map bound to a macro's iterator, as `t.name` reads it. A key the
      # map does not hold, or a field read from a value that is not a map, is a CEL error on
      # every row: UNKNOWN.
      #
      # @api private
      def constant_field(map, path)
        path.split(".").reduce(map) do |value, key|
          return cel_type_error unless value.is_a?(Hash) && value.key?(key)

          value[key]
        end
      end

      # Resolves an operand to a value. The value is a Sequel expression, a Ruby constant, or one of
      # the intermediate {Values} that the operator around it uses.
      #
      # @api private
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
      # an Integer. Beyond int64 it can only be a double, so hold it as the Float it is.
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
        # A constant list, map or scalar where a boolean belongs is a CEL error on every row, and
        # none of them may reach `where`, which reads a Hash or an Array as conditions of its own.
        return cel_type_error if value.is_a?(Hash) || value.is_a?(Array) || value.is_a?(Numeric) ||
          (value.is_a?(::String) && !SqlSupport.sql_node?(value))

        # A collection where a boolean belongs — `filter()`, `map()` or a mapped association as
        # a condition, a conjunct or a negation's operand — is a list (or, for a to-one chain, a
        # map) to CEL. Its logical operators and a rule's condition take only a boolean, so it
        # is a no-such-overload error on every row, decided by the type and not by the
        # elements. UNKNOWN is that error in SQL: `NOT` keeps it, `AND FALSE` and `OR TRUE`
        # absorb it exactly as CEL's `&&` and `||` absorb an error.
        return cel_type_error if collection?(value)

        reject_double_text("a condition", value)

        # A boolean column alone is a correct CEL condition. But SQLite and MySQL hold a boolean
        # as an integer, and a CASE or a NOT over a bare column then reads a number and not a
        # truth value. A comparison with TRUE has the same result, and this is also true for
        # NULL.
        return SqlSupport.comparison("eq", value, true) if column_type(value) == :boolean

        SqlSupport.to_predicate(value)
      end

      # --- structural operators -------------------------------------------------------

      # An `and` with no operands would give TRUE, and thus the filter would permit every row.
      # The Cerbos planner does not make that shape, but this adapter accepts a plan from any
      # source. A plan that lost its operands — an incomplete JSON body, a bad conversion —
      # must not become "permit everything". Thus an empty operand list is an error.
      def combine(operator, operands, environment)
        if operands.empty?
          raise InvalidPlanError, "#{operator} has no operands"
        end

        predicates = operands.map { |operand| predicate(operand, environment) }
        (operator == "and") ? SqlSupport.and_node(predicates) : SqlSupport.or_node(predicates)
      end

      def negate(operands, environment)
        unless operands.length == 1
          raise InvalidPlanError, "not takes exactly one operand, got #{operands.length}"
        end
        SqlSupport.not_node(predicate(operands.first, environment))
      end

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

        reject_double_text("if", then_value)
        reject_double_text("if", else_value)

        # An arm with a value that is not finite must not go to the database. Thus the
        # translator keeps the ternary, and the comparison around it calculates each branch.
        if deferred_value?(then_value) || deferred_value?(else_value)
          return Values::ConditionalValue.new(
            condition: condition, then_value: then_value, else_value: else_value
          )
        end

        result = branches(condition, then_value, else_value)
        return record_boolean(result) if boolean_arm?(then_value) || boolean_arm?(else_value)

        record_cel_type(result, branch_cel_type(then_value, else_value))
      end

      def boolean_arm?(value)
        value == true || value == false || boolean_value?(value)
      end

      # Records that CEL holds a node as a boolean. See {Casts#boolean_value?}.
      def record_boolean(value)
        SqlSupport.sql_node?(value) ? record_cel_type(value, :bool) : value
      end

      # CEL gives both arms of a ternary one type, so an arm whose type is certain fixes the
      # other's. A whole constant on its own is not certain: the plan ships `1000000` and
      # `1000000.0` as the same number, and string() spells them "1000000" and "1e+06".
      def branch_cel_type(*arms)
        return :int if arms.any? { |arm| cel_type(arm) == :int }
        return :double if arms.any? { |arm| arm.is_a?(Float) && arm.finite? && arm != arm.truncate }
        return :ambiguous_number if arms.any? { |arm| ambiguous_number?(arm) }

        nil
      end

      def ambiguous_number?(value)
        return true if cel_type(value) == :ambiguous_number
        return false unless value.is_a?(Numeric) && value.finite? && value == value.truncate

        value.to_i.to_s != cel_double_spelling(value.to_f)
      end

      # +CASE WHEN c THEN a WHEN NOT c THEN b END+, with no ELSE clause, so an UNKNOWN
      # condition gives NULL and not the else branch. See {#ternary}.
      def branches(condition, then_value, else_value)
        SqlSupport.case_node(
          [[condition, then_value], [SqlSupport.not_node(condition), else_value]]
        )
      end

      # --- operators with resolved operands -------------------------------------------

      def apply(operator, values)
        assert_arity(operator, values)

        override = operator_overrides[operator]
        reject_map_literals(operator, values, override)
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

      # A +Sequel::Model+ class, or a dataset of one. The associations and the column types are
      # read from the model, so a dataset without a model has nothing to resolve a mapping
      # against.
      def resolve_model(model)
        if model.is_a?(::Sequel::Dataset)
          owner = model.respond_to?(:model) ? model.model : nil
          return [model, owner] if owner.is_a?(Class) && owner < ::Sequel::Model
        elsif model.is_a?(Class) && model < ::Sequel::Model
          return [model.dataset, model]
        end

        raise ArgumentError,
          "model must be a Sequel::Model subclass or a dataset of one, got #{model.inspect}"
      end

      # A map literal's entry. Only constants: a map holding a column would need its equality
      # built element by element, and no shape asks for that.
      def map_entry(key, value)
        unless key.is_a?(::String) && deep_constant?(value)
          raise UnsupportedOperatorError,
            "A map literal is translated only with string keys and constant values, got " \
            "#{describe(key)} => #{describe(value)}"
        end

        Values::MapEntry.new(key: key, value: value)
      end

      def map_literal(entries)
        unless entries.all?(Values::MapEntry)
          raise InvalidPlanError, "struct takes set-field operands, got #{entries.map { |e| describe(e) }.join(", ")}"
        end

        entries.to_h { |entry| [entry.key, entry.value] }
      end

      # The operators that compare a map literal by CEL equality without binding it into SQL.
      MAP_OPERATORS = %w[eq ne in hasIntersection list struct].freeze

      def reject_map_literals(operator, values, override)
        return unless values.any? { |value| holds_map?(value) }
        return if override.nil? && MAP_OPERATORS.include?(operator)

        raise UnsupportedOperatorError,
          "#{operator} cannot take a map literal#{" under an operator override" if override}: " \
          "only eq, ne, in and hasIntersection compare one, by CEL equality"
      end

      def holds_map?(value)
        case value
        when Hash, Values::MapEntry then true
        when Array then value.any? { |element| holds_map?(element) }
        else false
        end
      end

      # A constant all the way down: a scalar, a null, or a list or map of those.
      def deep_constant?(value)
        case value
        when nil then true
        when Array then value.all? { |element| deep_constant?(element) }
        when Hash then value.values.all? { |element| deep_constant?(element) }
        else constant?(value)
        end
      end

      def record_column_type(node, type)
        @column_types[node] = type
        node
      end

      def constant?(value)
        !SqlSupport.sql_node?(value) && !collection?(value) && !value.nil?
      end

      def collection?(value)
        value.is_a?(Values::Collection) ||
          value.is_a?(Values::FilteredCollection) ||
          value.is_a?(Values::MappedCollection)
      end

      # A value that may be NaN or an Infinity. It stays out of SQL until a comparison
      # calculates it. See {Values::IEEEConstant}.
      def deferred_value?(value)
        value.is_a?(Values::IEEEConstant) || value.is_a?(Values::ConditionalValue)
      end

      # Refuses a collection, then a value that may not be finite, in any of +values+: the
      # operands of an operator that needs plain scalars.
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
        when Values::Collection then "an association"
        when Values::FilteredCollection then "a filtered association"
        when Values::MappedCollection then "a projected association"
        when Values::Hierarchy then "a hierarchy"
        when Values::DoubleText then "string() of a double"
        else "#{value.inspect} (#{value.class})"
        end
      end
    end
  end
end
