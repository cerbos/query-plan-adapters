# frozen_string_literal: true

require_relative "aggregation"
require_relative "errors"
require_relative "guards"
require_relative "hierarchy"
require_relative "mapper"
require_relative "operands"
require_relative "plan"
require_relative "regex"

module Cerbos
  module MongoDB
    # Translates a CONDITIONAL plan's condition into a MongoDB query filter.
    #
    # One instance per call. +scope+ records whether the walk is inside a collection macro's
    # lambda, where only the iteration variable can be expressed per element.
    class Translator
      include Operands

      Scope = Struct.new(:variable) do
        def collection? = !variable.nil?
      end
      ROOT = Scope.new(nil).freeze

      NULL_REPRESENTATIONS = %i[explicit omitted].freeze

      # Operators whose second operand is a lambda that binds an iteration variable.
      LAMBDA_BINDING = %w[exists exists_one all filter map except].freeze

      # `value OP field` is `field MIRROR(OP) value`.
      MIRRORED = {"eq" => "eq", "ne" => "ne", "lt" => "gt", "le" => "ge", "gt" => "lt", "ge" => "le"}.freeze

      def initialize(mapper:, null_representation:)
        unless NULL_REPRESENTATIONS.include?(null_representation)
          raise ArgumentError, "null_attribute_representation must be :explicit or :omitted, got #{null_representation.inspect}"
        end

        @mapper = mapper
        @null_representation = null_representation
      end

      def translate(condition)
        reject_null_constructor(condition)
        build(condition, @mapper, ROOT)
      end

      private

      # --- the null representation (#302) ---------------------------------------------------

      # Guards every site that would emit a null-selecting predicate from a `null` operand.
      #
      # Under :omitted a NULL field carries no attribute, so CEL raises a missing-attribute
      # error and check() denies; matching null would return exactly the documents the PDP
      # refuses. The rejection is deliberately wider than the over-granting shapes: negation is
      # applied by wrapping the built filter, so a leaf cannot tell whether an enclosing `not`
      # will flip a not-null predicate back into a null-selecting one.
      def assert_null_translatable(context)
        return unless @null_representation == :omitted

        raise UnsupportedError,
          "Cannot translate #{context} under null_attribute_representation :omitted: a NULL field " \
          "sends no attribute, so Cerbos evaluates the comparison as a missing-attribute error " \
          "(deny) while a null-selecting filter would return those documents. Send NULL fields " \
          "as explicit nulls and use :explicit, or keep this shape out of the policy."
      end

      # A null inside a list/struct literal is a NULL value only under the explicit representation.
      def reject_null_constructor(operand, in_constructor = false)
        if value?(operand)
          assert_null_translatable("a null literal in a collection or struct constructor") if in_constructor && carries_null?(operand.value)
        elsif expression?(operand)
          nested = in_constructor || %w[list struct set-field].include?(operand.operator)
          operand.operands.each { |child| reject_null_constructor(child, nested) }
        end
      end

      # Inside a collection predicate, only the iteration variable can be expressed per element.
      def assert_scoped(reference, scope)
        return unless scope.collection?
        return if reference == scope.variable || reference.start_with?("#{scope.variable}.")

        raise UnsupportedError, "Outer reference #{reference} inside a collection predicate is unsupported"
      end

      # --- boolean position -----------------------------------------------------------------

      def build(expression, mapper, scope)
        return bare_variable(expression.name, mapper, scope) if variable?(expression)
        raise InvalidPlanError, "Invalid Cerbos expression structure" unless expression?(expression)

        operator = expression.operator
        operands = expression.operands

        # A literal value list arrives as a macro's collection operand when the planner could not
        # unroll it over a known collection (more than 10 elements). None of the relation
        # requirements below can be satisfied by a literal, so it is folded first.
        if LAMBDA_BINDING.include?(operator) && operands.length == 2 && value?(operands[0])
          return fold_literal_collection(operator, operands[0], operands[1]) { |body| build(body, mapper, scope) }
        end

        case operator
        when "and" then {"$and" => operands.map { |op| build(op, mapper, scope) }}
        when "or" then {"$or" => operands.map { |op| build(op, mapper, scope) }}
        when "not" then translate_not(operands, mapper, scope)
        when *Aggregation::COMPARISONS.keys then translate_comparison(operator, operands, mapper, scope)
        when "in" then translate_in(operands, mapper, scope)
        when "matches" then translate_matches(operands, mapper, scope)
        when "contains", "startsWith", "endsWith" then translate_string_predicate(expression, mapper, scope)
        when "hasIntersection" then translate_has_intersection(operands, mapper, scope)
        when "exists", "all" then translate_quantifier(operator, operands, mapper, scope)
        when "exists_one"
          raise UnsupportedError, "exists_one requires exact match cardinality and is unsupported"
        when "filter"
          # filter() yields a list. In boolean position there is no meaning to pick: `filter(...)`
          # is not `size(filter(...)) > 0` (cerbos/query-plan-adapters#313).
          raise UnsupportedError,
            "filter() returns a list, not a boolean, so it cannot be a condition on its own; " \
            "only size(filter(...)) has a boolean meaning"
        when "map"
          raise UnsupportedError,
            "map() returns a list, not a boolean, so it cannot be a condition on its own; " \
            "only hasIntersection(map(...), [...]) gives the projection a boolean meaning"
        when "lambda" then translate_lambda(operands, mapper, scope)
        when "if"
          raise UnsupportedError, "if aggregation expressions inside collection predicates are unsupported" if scope.collection?

          Guards.with_evaluation({"$expr" => Aggregation.build_expression(expression, mapper)}, operands, mapper)
        when "ancestorOf", "descendentOf", "overlaps" then Hierarchy.build(operator, operands, mapper)
        else
          raise UnsupportedError, "Unsupported operator: #{operator}"
        end
      end

      # Every leaf resolves its field in the active collection scope before emission.
      def leaf(mapper, scope, name, comparison, nullable:, require_exists:)
        assert_scoped(name, scope)
        resolved = mapper.resolve_field(name)
        many = resolved.relation&.type == :many
        filter = Guards.guarded_field_filter(many ? resolved.path.drop(1) : resolved.path, comparison, nullable, require_exists)
        many ? {resolved.relation.name => {"$elemMatch" => filter}} : filter
      end

      # A field compared with a constant: the field is nullable-guarded, and a null constant (or
      # a null inside a constant list) requires the field to exist and is refused under :omitted.
      def value_comparison(mapper, scope, name, comparison, constant, null_context)
        require_exists = carries_null?(constant)
        assert_null_translatable(null_context) if require_exists
        leaf(mapper, scope, name, comparison, nullable: mapper.nullable?(name), require_exists: require_exists)
      end

      def bare_variable(name, mapper, scope)
        assert_scoped(name, scope)
        resolved = mapper.resolve_field(name)
        # A to-MANY relation in boolean position is a collection, and a collection has no truth
        # value. A to-ONE relation flattens to a single dotted scalar path, so it reads exactly
        # like a plain field; its absent-hop requirement is applied by the enclosing operator
        # (cerbos/query-plan-adapters#375).
        raise UnsupportedError, "Bare collection variables are unsupported" if resolved.relation && resolved.relation.type != :one

        Guards.guarded_field_filter(resolved.path, {"$eq" => true}, mapper.nullable?(name))
      end

      def translate_not(operands, mapper, scope)
        operand = operand_at(operands, 0, "not operator requires at least one operand")
        if variable_names(operand).any? { |name| mapper.nullable?(name) } ||
            (expression?(operand) && %w[exists exists_one all].include?(operand.operator))
          raise UnsupportedError, "not over nullable fields or collection macros cannot preserve Cerbos error semantics"
        end

        # with_evaluation ANDs its conjuncts OUTSIDE this $nor, which is where the absent-parent
        # requirement has to sit: inside, the negation would flip it with the predicate (#315).
        Guards.with_evaluation({"$nor" => [build(operand, mapper, scope)]}, [operand], mapper)
      end

      def translate_comparison(operator, operands, mapper, scope)
        left = operand_at(operands, 0, "#{operator} operator requires a left operand")
        right = operand_at(operands, 1, "#{operator} operator requires a right operand")
        both = [left, right]

        if variable?(left) && variable?(right) && both.any? { |op| mapper.value_type(op.name) == :date_time }
          raise UnsupportedError,
            "Bare temporal field comparison cannot preserve CEL string equality: stored Dates " \
            "discard the original lexical spelling; compare timestamp(...) values instead"
        end
        if (variable?(left) || variable?(right)) && both.any? { |op| value?(op) && op.value.is_a?(Array) }
          raise UnsupportedError,
            "Whole-list comparison is not supported: a relation mapping exposes scalar element " \
            "fields, not an ordered list value"
        end

        # Either operand an expression, or two fields: compare inside $expr.
        if expression?(left) || expression?(right) || (variable?(left) && variable?(right))
          if scope.collection?
            raise UnsupportedError, "#{operator} aggregation expressions inside collection predicates are unsupported"
          end

          expr = {Aggregation::COMPARISONS.fetch(operator) => [Aggregation.build(left, mapper), Aggregation.build(right, mapper)]}
          return Guards.with_evaluation({"$expr" => expr}, both, mapper)
        end

        variable = both.find { |op| variable?(op) }
        value = both.find { |op| value?(op) }
        raise UnsupportedError, "#{operator} requires a field/value pair or aggregation operands" unless variable && value

        assert_scoped(variable.name, scope)
        effective = variable.equal?(left) ? operator : MIRRORED.fetch(operator)
        # A constant of a different scalar type than the declared field never equals it.
        config = mapper.resolve_config(variable.name)
        if %w[eq ne].include?(effective) && config&.value_type && config.value_type != :date_time &&
            !value.value.nil? && !matches_type?(value.value, config.value_type) && config.value_parser.nil?
          return Guards.with_nullable({"$expr" => {"$eq" => [effective == "ne", true]}}, [variable], mapper)
        end

        value_comparison(
          mapper, scope, variable.name,
          {Aggregation::COMPARISONS.fetch(effective) => mapper.apply_value_parser(variable.name, value.value)},
          value.value, "`#{effective}` against a null operand"
        )
      end

      def translate_in(operands, mapper, scope)
        left = operand_at(operands, 0, "in requires a left operand")
        right = operand_at(operands, 1, "in requires a right operand")

        if variable?(left) && value?(right)
          raise UnsupportedError, "in with a field on the left requires an array value" unless right.value.is_a?(Array)

          return value_comparison(
            mapper, scope, left.name,
            {"$in" => right.value.map { |element| mapper.apply_value_parser(left.name, element) }},
            right.value, "a null element in an `in` list"
          )
        end
        if value?(left) && left.value.is_a?(Array)
          raise UnsupportedError,
            "List-element membership is not supported: a scalar relation mapping cannot compare a list value with one element"
        end
        if value?(left) && variable?(right)
          return value_comparison(
            mapper, scope, right.name,
            {"$eq" => mapper.apply_value_parser(right.name, left.value)},
            left.value, "a null needle in a mapped-collection `in`"
          )
        end

        raise UnsupportedError, "in supports only field-in-value-list or value-in-mapped-collection shapes"
      end

      def translate_matches(operands, mapper, scope)
        field = operand_at(operands, 0, "matches operator requires a field operand")
        pattern = operand_at(operands, 1, "matches operator requires a regex pattern value")
        unless variable?(field) && value?(pattern) && string?(pattern.value)
          raise UnsupportedError, "matches operator requires a string regex pattern"
        end

        leaf(mapper, scope, field.name, {"$regex" => Regex.normalise_re2(pattern.value)}, nullable: false, require_exists: false)
      end

      # contains/startsWith/endsWith: a $regex on a string field, $expr otherwise.
      def translate_string_predicate(expression, mapper, scope)
        operator = expression.operator
        receiver = operand_at(expression.operands, 0, "#{operator} operator requires a receiver")
        needle = operand_at(expression.operands, 1, "#{operator} operator requires a needle")
        unless variable?(receiver) && value?(needle) && string?(needle.value) &&
            [nil, :string].include?(mapper.value_type(receiver.name))
          if scope.collection?
            raise UnsupportedError, "#{operator} aggregation expressions inside collection predicates are unsupported"
          end

          return Guards.with_evaluation({"$expr" => Aggregation.build_expression(expression, mapper)}, [expression], mapper)
        end

        escaped = Regex.escape(needle.value)
        # PCRE2's `$` also matches before a final newline, so "tail\n" would satisfy
        # endsWith("tail") while CEL says false. `\z` is the absolute end of the subject. `^`
        # needs no counterpart: without multiline it only matches at the start.
        pattern = case operator
        when "contains" then escaped
        when "startsWith" then "^#{escaped}"
        else "#{escaped}\\z"
        end
        leaf(mapper, scope, receiver.name, {"$regex" => pattern}, nullable: mapper.nullable?(receiver.name), require_exists: false)
      end

      def translate_has_intersection(operands, mapper, scope)
        # A null element lowers to a null-matching disjunct exactly as it does for `in`.
        if operands.any? { |op| value?(op) && carries_null?(op.value) }
          assert_null_translatable("a null element in hasIntersection")
        end
        raise InvalidPlanError, "hasIntersection requires exactly two operands" unless operands.length == 2

        first, second = operands
        # hasIntersection is commutative and the planner preserves source order, so the constant
        # list arrives FIRST when the policy spells it first. The two operands are not
        # interchangeable in the emitted filter, so normalise to collection-first (#387).
        value_first = value?(first) && first.value.is_a?(Array)
        collection, values = value_first ? [second, first] : [first, second]

        return translate_map_intersection(collection, values, mapper) if expression_with?(collection, "map")
        raise UnsupportedError, "Invalid operands for hasIntersection" unless variable?(collection) && value?(values)
        raise UnsupportedError, "hasIntersection requires an array value" unless values.value.is_a?(Array)

        leaf(mapper, scope, collection.name, {"$in" => values.value}, nullable: false, require_exists: values.value.include?(nil))
      end

      # hasIntersection(collection.map(e, e.field), [values]): some element's field is in the list.
      def translate_map_intersection(map, values_operand, mapper)
        collection = operand_at(map.operands, 0, "Expected a variable in map expression")
        lambda = operand_at(map.operands, 1, "Expected a lambda in map expression")
        raise UnsupportedError, "Expected a variable in map expression" unless variable?(collection)
        raise UnsupportedError, "Second operand of map must be a lambda expression" unless expression_with?(lambda, "lambda")

        projection = operand_at(lambda.operands, 0, "Map lambda requires a projection operand")
        variable = operand_at(lambda.operands, 1, "Map lambda requires a variable operand")
        raise UnsupportedError, "Invalid map expression structure" unless variable?(variable)
        unless value?(values_operand) && values_operand.value.is_a?(Array)
          raise UnsupportedError, "hasIntersection requires an array value"
        end

        values = values_operand.value
        relation = mapper.resolve_field(collection.name).relation
        raise UnsupportedError, "map operator requires a relation mapping" if relation.nil?
        raise UnsupportedError, "map operator requires a collection relation" unless relation.type == :many
        raise UnsupportedError, "Map projection must be a variable reference" unless variable?(projection)

        scoped = mapper.scoped(collection.name, variable.name)
        element_path = scoped.resolve_field(projection.name).path
        matching = {relation.name => {"$elemMatch" => Guards.guarded_field_filter(element_path, {"$in" => values}, false, values.include?(nil))}}
        return matching unless scoped.nullable?(projection.name)

        # A nullable projection is a missing attribute on any element that stores null, which
        # makes the whole CEL `map` raise: no element may be null.
        {"$and" => [
          {relation.name => {"$not" => {"$elemMatch" => Guards.field_filter(element_path, {"$eq" => nil})}}},
          matching
        ]}
      end

      # exists and all over a mapped to-many relation, as $elemMatch over its elements.
      def translate_quantifier(operator, operands, mapper, scope)
        raise InvalidPlanError, "#{operator} requires exactly two operands" unless operands.length == 2

        collection, lambda = operands
        raise UnsupportedError, "Invalid operands for collection operation" unless variable?(collection) && expression?(lambda)
        raise UnsupportedError, "Second operand must be a lambda expression" unless lambda.operator == "lambda"

        condition = operand_at(lambda.operands, 0, "Lambda operand requires a condition")
        variable = operand_at(lambda.operands, 1, "Lambda operand requires a variable")
        raise UnsupportedError, "Lambda variable must have a name" unless variable?(variable)

        relation = mapper.resolve_field(collection.name).relation
        raise UnsupportedError, "#{operator} operator requires a relation mapping" if relation.nil?
        raise UnsupportedError, "#{operator} operator requires a collection relation" unless relation.type == :many

        element = build(condition, mapper.scoped(collection.name, variable.name), Scope.new(variable.name))
        return {relation.name => {"$elemMatch" => element}} if operator == "exists"

        # "No element fails the condition", on a field that must be an array.
        {relation.name => {"$type" => "array", "$not" => {"$elemMatch" => {"$nor" => [element]}}}}
      end

      def translate_lambda(operands, mapper, scope)
        condition = operand_at(operands, 0, "lambda operator requires a condition operand")
        variable = operand_at(operands, 1, "lambda operator requires a variable operand")
        raise UnsupportedError, "Lambda variable must have a name" unless variable?(variable)

        prefix = "#{variable.name}."
        stripping = Mapper.new(->(key) { {field: key.sub(prefix, "")} })
        # A standalone lambda preserves whether its caller entered a collection.
        build(condition, stripping, scope.collection? ? Scope.new(variable.name) : scope)
      end

      # --- a collection macro over a literal list -------------------------------------------

      # The planner emits a literal collection operand when a known-value collection (typically
      # a folded principal attribute) has more than 10 elements — at 10 or fewer it unrolls
      # exists/all into an or/and chain itself. Apply the same fold here, so the filter does not
      # depend on which side of that threshold the collection lands.
      def fold_literal_collection(operator, collection, lambda)
        unless %w[exists all].include?(operator)
          raise UnsupportedError,
            "#{operator} over a literal collection value is not supported. " \
            "Only exists() and all() can be folded into a flat filter."
        end

        elements = collection.value
        raise UnsupportedError, "#{operator} over a literal collection requires a list value" unless elements.is_a?(Array)
        raise UnsupportedError, "Second operand of #{operator} must be a lambda expression" unless expression_with?(lambda, "lambda")
        unless lambda.operands.length == 2
          raise UnsupportedError, "#{operator} over a literal collection supports single-variable lambdas only"
        end

        body = lambda.operands[0]
        variable = lambda.operands[1]
        raise UnsupportedError, "Lambda variable must have a name" unless variable?(variable)
        # CEL identity over an empty collection; MongoDB rejects an empty $or/$and.
        return {"$expr" => operator == "all"} if elements.empty?

        filters = elements.map { |element| yield(substitute(body, variable.name, element)) }
        (operator == "exists") ? {"$or" => filters} : {"$and" => filters}
      end

      # Substitutes a lambda variable with a concrete element. A nested macro that rebinds the
      # same name shadows it, so substitution only descends into its collection operand.
      def substitute(operand, name, element)
        if variable?(operand)
          return Plan::Value.new(element) if operand.name == name
          return operand unless operand.name.start_with?("#{name}.")

          current = element
          operand.name[(name.length + 1)..].split(".").each do |segment|
            unless current.is_a?(Hash) && current.key?(segment)
              raise UnsupportedError, "Cannot resolve \"#{operand.name}\": collection element has no field \"#{segment}\""
            end

            current = current[segment]
          end
          return Plan::Value.new(current)
        end
        return operand unless expression?(operand)

        if LAMBDA_BINDING.include?(operand.operator) && operand.operands.length == 2
          nested_collection, nested_lambda = operand.operands
          if expression_with?(nested_lambda, "lambda") && variable?(nested_lambda.operands[1]) &&
              nested_lambda.operands[1].name == name
            return Plan::Expression.new(operand.operator, [substitute(nested_collection, name, element), nested_lambda])
          end
        end
        Plan::Expression.new(operand.operator, operand.operands.map { |child| substitute(child, name, element) })
      end
    end
  end
end
