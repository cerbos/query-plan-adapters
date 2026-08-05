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
    # Walks a normalised query plan and builds the equivalent Arel predicate.
    #
    # Two rules shape almost every decision here:
    #
    # * *Wire order is source order.* The planner preserves the order operands appear in the
    #   policy, so <tt>1 < R.attr.x</tt> arrives as <tt>lt(value(1), variable(x))</tt>. This
    #   adapter emits comparisons in that same order (+1 < x+), which is already correct SQL;
    #   adapters that instead assumed a column always comes first, and swapped operands to
    #   restore that, inverted directional comparisons (cerbos/query-plan-adapters#257).
    #
    # * *An error is not a false.* CEL denies a resource when evaluating its condition raises
    #   — a missing attribute, an element whose field is absent. SQL's UNKNOWN behaves the
    #   same way: it is excluded by a predicate *and* by that predicate's negation. The
    #   translation preserves UNKNOWN rather than collapsing it, which is why collection
    #   macros are CASE expressions rather than bare EXISTS.
    class Translator
      # Operators whose operands must NOT be resolved before the operator runs: they either
      # bind an iterator variable, or must preserve UNKNOWN across a branch.
      STRUCTURAL_OPERATORS = %w[and or not if lambda exists all exists_one filter map].freeze

      COMPARISONS = %w[eq ne lt gt le ge].freeze

      ARITHMETIC = {"add" => "+", "sub" => "-", "mult" => "*", "mod" => "%"}.freeze

      STRING_MATCHES = {
        "contains" => {prefix: true, suffix: true},
        "startsWith" => {prefix: false, suffix: true},
        "endsWith" => {prefix: true, suffix: false}
      }.freeze

      def initialize(model:, attributes:, operator_overrides: {})
        @model = model
        @attributes = attributes.transform_keys(&:to_s)
        @operator_overrides = operator_overrides.transform_keys(&:to_s)
        @dialect = Dialect.for(model)
        @matcher = StringMatcher.new(@dialect)

        @operator_overrides.each_key do |operator|
          if STRUCTURAL_OPERATORS.include?(operator)
            raise ArgumentError,
              "Operator #{operator.inspect} cannot be overridden: it binds scopes or " \
              "branches, so its operands are not resolved before it runs"
          end
        end
      end

      attr_reader :model, :attributes, :operator_overrides, :dialect, :matcher

      # @param plan [Object] anything {Plan.normalise} accepts
      # @return [ActiveRecord::Relation]
      def translate(plan)
        normalised = Plan.normalise(plan)
        return model.none if normalised.always_denied?
        return model.all if normalised.always_allowed?

        @aliaser = Relations::Aliaser.new
        # Identity-keyed: every resolved column is a freshly built Arel node that flows
        # through the translation unchanged, so identity is exactly the right notion here.
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

      # Resolve an operand to a value: an Arel node, a Ruby constant, or one of the
      # intermediate {Values} the surrounding operator consumes.
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

      def evaluate_expression(node, environment)
        operator = node.operator
        operands = node.operands

        case operator
        when "and" then ArelSupport.and_node(operands.map { |o| predicate(o, environment) })
        when "or" then ArelSupport.or_node(operands.map { |o| predicate(o, environment) })
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

        # A bare boolean column is a valid CEL condition, but ActiveRecord's `where` rejects a
        # naked column reference, and PostgreSQL needs a boolean expression rather than a
        # value. Comparing against TRUE is equivalent, including for NULL.
        return ArelSupport.comparison("eq", value, true) if column_type(value) == :boolean

        ArelSupport.to_predicate(value)
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
      # The generated CASE deliberately has no ELSE. When the condition is UNKNOWN — a NULL
      # column, an absent attribute — CEL raises, which is a deny; a CASE with no matching
      # WHEN evaluates to NULL, so the row stays excluded under both polarities of an
      # enclosing NOT. An +ELSE+ would leak those rows into the else-branch instead.
      def ternary(operands, environment)
        unless operands.length == 3
          raise InvalidPlanError, "if takes exactly three operands, got #{operands.length}"
        end

        condition = predicate(operands[0], environment)
        then_value = evaluate(operands[1], environment)
        else_value = evaluate(operands[2], environment)

        # A non-finite arm must not reach the database, so keep the ternary symbolic and let
        # the enclosing comparison fold each branch arithmetically instead.
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

      # CEL's three quantifiers differ precisely in how they treat an element whose body
      # errored, so each gets its own error guard rather than a shared one:
      #
      # * +exists+ absorbs errors behind a true witness;
      # * +all+ absorbs them behind a false witness;
      # * +exists_one+ never absorbs them — it has to count every element.
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

      def apply(operator, values)
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

      # --- comparison -----------------------------------------------------------------

      def compare(operator, left, right)
        # A ternary held back for folding: compare each arm, then re-branch. The CASE again
        # has no ELSE, so an UNKNOWN condition stays UNKNOWN.
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

        # Both sides constant: fold rather than emitting a tautology.
        if constant?(left) && constant?(right)
          return fold_comparison(operator, left, right)
        end

        # `= NULL` is never true. CEL's `null == x` is an ordinary equality, so put the
        # comparable side on the left and let Arel emit IS [NOT] NULL.
        if left.nil? && %w[eq ne].include?(operator)
          return ArelSupport.comparison(operator, right, nil)
        end

        ArelSupport.comparison(operator, left, right)
      end

      # CEL follows IEEE-754: NaN is unequal to everything (including itself) and unordered
      # against everything. PostgreSQL does not — it sorts NaN above every other double — so
      # these comparisons are folded here instead of being bound into dialect SQL.
      def compare_non_finite(operator, left, right)
        left_value = left.is_a?(Values::IEEEConstant) ? left.value : left
        right_value = right.is_a?(Values::IEEEConstant) ? right.value : right

        nan_side = [left_value, right_value].find { |v| v.is_a?(Float) && v.nan? }
        if nan_side
          other = left_value.equal?(nan_side) ? right_value : left_value
          result = (operator == "ne")

          return result if other.is_a?(Numeric)
          if ArelSupport.arel_node?(other)
            # Fold the comparison for every present value, but keep a missing attribute an
            # error rather than turning it into `ne`'s true.
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

      # +value in R.attr.<relation>+. A row whose relation is empty is simply excluded,
      # matching CEL's missing-attribute deny.
      def relation_membership(scope, value)
        member = scope.member_column

        condition =
          if value.nil?
            ArelSupport.comparison("eq", member, nil)
          elsif ArelSupport.arel_node?(value)
            # Two explicit nulls are equal in CEL but UNKNOWN in SQL, so spell that case out.
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

      def scalar_membership(column, values)
        members = values.is_a?(Array) ? values : [values]
        present = members.reject(&:nil?)

        predicates = []
        unless present.empty?
          predicates << Arel::Nodes::In.new(
            ArelSupport.quote(column), present.map { |v| ArelSupport.quote(v) }
          )
        end
        # A null element makes membership true for a null (not merely missing) attribute.
        predicates << ArelSupport.comparison("eq", column, nil) if present.length != members.length

        predicates.empty? ? false : ArelSupport.or_node(predicates)
      end

      # --- arithmetic -----------------------------------------------------------------

      def arithmetic(operator, left, right)
        reject_collection(operator, left)
        reject_collection(operator, right)

        # CEL overloads `+` for strings. SQL does not: `'a' + 'b'` is a numeric addition
        # that coerces both sides to 0 on SQLite and MySQL, so string operands need the
        # dialect's concatenation instead.
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

      # Cerbos transports every number as a double and CEL attribute arithmetic is
      # double-typed, so division must be too: SQLite and PostgreSQL would otherwise apply
      # integer division and truncate +5 / 2+ to +2+.
      def divide(numerator, denominator)
        reject_collection("div", numerator)
        reject_collection("div", denominator)

        if numerator.is_a?(Numeric) && denominator.is_a?(Numeric)
          return divide_constants(numerator.to_f, denominator.to_f)
        end

        # Dialects disagree on division by zero (NULL versus a raised error). NULL filters
        # identically to CEL's NaN for every comparison the planner emits here, and it keeps
        # a database exception from aborting the whole query.
        ArelSupport.infix(
          "/",
          as_double(numerator),
          ArelSupport.function("NULLIF", [as_double(denominator), 0.0])
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
          # size() counts elements without evaluating them, so a NULL member column still
          # counts — there is nothing to error on and no guard is needed.
          target.scope.count
        when Values::FilteredCollection
          # filter(), unlike exists(), never absorbs an erroring element: one UNKNOWN body
          # poisons the whole count.
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
        # hasIntersection is symmetric, and the planner preserves source order, so the
        # literal list may arrive on either side.
        left, right = right, left if left.is_a?(Array) && !right.is_a?(Array)
        values = right.is_a?(Array) ? right : [right]

        case left
        when Values::Collection
          left.scope.exists(scalar_membership(left.scope.member_column, values))
        when Values::MappedCollection
          # map() errors on any erroring element, with no absorption, so the error guard has
          # to come before the true-witness check.
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

        # Comparing a string column against a bound Time compares two *different* textual
        # formats — ActiveRecord renders `2025-01-01 00:00:00`, an RFC-3339 column holds
        # `2025-01-01T00:00:00Z` — so the ordering it produces is lexicographic accident,
        # not an instant comparison. Refuse rather than emit it.
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
        # A list-built hierarchy is already split, so its delimiter carries no meaning and
        # comparing it against a differently-delimited path is well defined.
        if left.segments.nil? && right.segments.nil? && left.delimiter != right.delimiter
          raise UnsupportedOperatorError,
            "Hierarchy operands use different delimiters: " \
            "#{left.delimiter.inspect} and #{right.delimiter.inspect}"
        end

        [left, right]
      end

      # The segments of a hierarchy, when they can be known at translation time. A column is
      # an opaque delimited string until the query runs, so it has none.
      def segments_of(hierarchy)
        return hierarchy.segments if hierarchy.segments
        return hierarchy.value.split(hierarchy.delimiter, -1) if hierarchy.value.is_a?(::String)

        nil
      end

      # Segment-wise comparison, used whenever either side came from a list. Both sides must
      # be segmentable; a column cannot be split into segments in SQL.
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
          # A strict ancestor is a strictly shorter path that agrees on every segment it has.
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
          # A constant descendant has a known, finite set of ancestors: match them exactly
          # rather than with a LIKE whose metacharacters would have to be escaped.
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

      # Resolves plan variables against the caller's mapping, plus whatever iterator
      # variables the enclosing collection macros have bound.
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
          scope = bindings[head]
          unless scope
            raise UnmappedAttributeError,
              "No mapping for attribute #{name.inspect}. Add it to the attributes hash " \
              "passed to Cerbos::ActiveRecord.query_plan_to_relation."
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
