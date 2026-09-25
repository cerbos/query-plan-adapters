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
      LAMBDA_BINDING = Guards::LAMBDA_BINDING

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
        build(fold_constants(condition), @mapper, ROOT)
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

      # A list or map constructor whose every element is a constant, as the constant it builds:
      # `list` becomes an Array and `struct` a Hash. A map with a non-string or repeated key is
      # left as it is (CEL raises on a repeated key, and a document has string keys only), and so
      # is anything that reads a variable.
      def fold_constants(operand)
        return operand unless expression?(operand)

        operands = operand.operands.map { |child| fold_constants(child) }
        case operand.operator
        when "list"
          return Plan::Value.new(operands.map(&:value)) if operands.all? { |child| value?(child) }
        when "struct"
          pairs = operands.map { |field|
            next nil unless expression_with?(field, "set-field") && field.operands.length == 2

            key, value = field.operands
            next nil unless value?(key) && string?(key.value) && value?(value)

            [key.value, value.value]
          }
          if pairs.none?(&:nil?) && pairs.map(&:first).uniq.length == pairs.length
            return Plan::Value.new(pairs.to_h)
          end
        end
        Plan::Expression.new(operand.operator, operands)
      end

      # Whether a constant is a list or a map, which a scalar field never equals.
      def composite?(value) = value.is_a?(Array) || value.is_a?(Hash)

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

        # De Morgan, pushed down to the leaves. CEL's `&&`/`||` let a decided operand absorb an
        # error in the other, so `!(!aBool && parent.x == "one")` is TRUE on a parentless document
        # whose aBool is true. Guarding the whole negation for every field it reads denies that
        # document; negating each operand instead gives every leaf its own guard. The rewrite is
        # exact under CEL's error semantics: `!(a && b)` and `!a || !b` agree on every
        # combination of true, false and error.
        if expression_with?(operand, "and") || expression_with?(operand, "or")
          dual = (operand.operator == "and") ? "$or" : "$and"
          return {dual => operand.operands.map { |child| translate_not([child], mapper, scope) }}
        end
        # `!!x` is `x`, errors included, and eliminating the pair keeps a leaf that answers an
        # error as `false` from being flipped twice by nested $nors.
        if expression_with?(operand, "not")
          return build(operand_at(operand.operands, 0, "not operator requires at least one operand"), mapper, scope)
        end
        # An ordering between a field and a constant negates to its complement, so it keeps the
        # positive leaf's semantics: MongoDB's $lt compares only values of the constant's own
        # BSON type, and a constant CEL cannot order against the field answers false either way.
        # A $nor over the ordering would instead be TRUE for a null, or a value of another type,
        # where CEL raises an error and denies.
        complement = complemented_ordering(operand)
        return build(complement, mapper, scope) if complement

        if (expression?(operand) && %w[exists exists_one all].include?(operand.operator)) ||
            !nullable_guard_exact?(operand, mapper, scope)
          raise UnsupportedError, "not over nullable fields or collection macros cannot preserve Cerbos error semantics"
        end

        # with_evaluation ANDs its conjuncts OUTSIDE this $nor, which is where the absent-parent
        # requirement has to sit: inside, the negation would flip it with the predicate (#315). A
        # null or absent list is the same kind of error CEL denies, so its array requirement sits
        # there too.
        guarded = Guards.with_evaluation({"$nor" => [build(operand, mapper, scope)]}, [operand], mapper)
        list_shape = Guards.list_shape_guard(operand, mapper, !scope.collection?)
        list_shape ? {"$and" => [list_shape, guarded]} : guarded
      end

      # `!(a < b)` is `a >= b` whenever CEL can order `a` and `b` at all.
      COMPLEMENTED_ORDERING = {"lt" => "ge", "le" => "gt", "gt" => "le", "ge" => "lt"}.freeze

      # The complement of an ordering between one field and one constant; nil for anything else.
      def complemented_ordering(operand)
        return nil unless expression?(operand)

        complement = COMPLEMENTED_ORDERING[operand.operator]
        return nil if complement.nil? || operand.operands.length != 2

        left, right = operand.operands
        field_and_constant = (variable?(left) && value?(right)) || (value?(left) && variable?(right))
        return nil unless field_and_constant

        Plan::Expression.new(complement, operand.operands)
      end

      # Whether guarding every nullable field the negated +operand+ reads is exact: it is when
      # CEL is certain to read each of them, so a null one is an error CEL cannot avoid.
      def nullable_guard_exact?(operand, mapper, scope)
        nullable = variable_names(operand).select { |name| mapper.nullable?(name) }
        return true if nullable.empty?

        !scope.collection? &&
          nullable.all? { |name| always_reads?(operand, name) } &&
          nullable.all? { |name| mapper.resolve_field(name).relation&.type != :many }
      end

      # Whether evaluating +operand+ always reads the variable +name+. `&&` and `||` absorb an
      # erroring operand when another decides the result, a ternary reads only the branch its
      # condition selects, and a lambda body is never evaluated over an empty collection: each of
      # those reads +name+ unconditionally only if every path through it does. Every other
      # operator is strict in its operands.
      def always_reads?(operand, name)
        return operand.name == name if variable?(operand)
        return false unless expression?(operand)

        first, *rest = operand.operands
        case operand.operator
        when "and", "or"
          operand.operands.all? { |child| always_reads?(child, name) }
        when "if"
          (!first.nil? && always_reads?(first, name)) || (!rest.empty? && rest.all? { |child| always_reads?(child, name) })
        else
          return !first.nil? && always_reads?(first, name) if LAMBDA_BINDING.include?(operand.operator)

          operand.operands.any? { |child| always_reads?(child, name) }
        end
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
        composite = both.find { |op| value?(op) && composite?(op.value) }
        other = composite && both.find { |op| !op.equal?(composite) }
        if composite && %w[eq ne].include?(operator) && !value?(other)
          return translate_composite_equality(operator, other, composite.value, mapper, scope)
        end
        if composite && expression?(other)
          raise UnsupportedError, "An ordering against a list or map constant inside an expression is unsupported"
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
        # CEL has no ordering between types, so `aNumber < "5"` is an error that denies. A
        # negation reaches here as the complemented ordering (translate_not), so this
        # match-nothing leaf holds under both polarities.
        if !%w[eq ne].include?(effective) && !can_order?(variable.name, value.value, mapper)
          return leaf(mapper, scope, variable.name, {"$in" => []}, nullable: false, require_exists: false)
        end
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

      # `==`/`!=` between a field or a map() projection and a list or map constant.
      #
      # A field that declares a scalar value_type never equals a list or a map, so the answer is
      # settled without a query, as for any constant of another type. Otherwise the list is
      # compared whole inside $expr, where $eq compares two arrays element by element and in
      # order, as CEL's list equality does: numbers by value across int and double, strings byte
      # by byte, null equal to null, and a boolean never equal to a number. Only a list of such
      # scalars is compared this way: MongoDB's NaN equals NaN where CEL's does not, and it
      # compares embedded documents field by field in stored order where CEL's map equality
      # ignores order.
      def translate_composite_equality(operator, other, constant, mapper, scope)
        if scope.collection?
          raise UnsupportedError, "Whole-value comparison inside a collection predicate needs $expr, which MongoDB accepts only at the top level"
        end
        if variable?(other)
          config = mapper.resolve_config(other.name)
          if config&.value_type && config.value_parser.nil?
            return Guards.with_nullable({"$expr" => {"$eq" => [operator == "ne", true]}}, [other], mapper)
          end
        end
        unless constant.is_a?(Array) && constant.all? { |element| comparable_scalar?(element) }
          raise UnsupportedError,
            "Whole-value comparison with a map, or with a list holding a list, a map or NaN, is " \
            "unsupported: MongoDB compares embedded documents in stored field order and NaN equal to NaN"
        end
        list, guard = list_expression(other, mapper)
        compared = {"$expr" => {Aggregation::COMPARISONS.fetch(operator) => [list, {"$literal" => constant}]}}
        filter = Guards.with_evaluation(compared, [other], mapper)
        guard ? {"$and" => [guard, filter]} : filter
      end

      # A scalar CEL and MongoDB's $eq agree on: a string, a boolean, null, or a number other
      # than NaN.
      def comparable_scalar?(value)
        value.nil? || string?(value) || boolean?(value) || (number?(value) && !(value.is_a?(Float) && value.nan?))
      end

      # The list +operand+ stands for, as an aggregation expression, and the filter requiring it
      # to be stored as an array (nil for a plain field, whose non-array value simply compares
      # unequal): a plain field as itself, and a to-many relation's projection or a map() over
      # one as a $map over its elements. $map reads a missing element field as null.
      #
      # @return [Array(Object, Hash or nil)]
      def list_expression(operand, mapper)
        if variable?(operand)
          resolved = mapper.resolve_field(operand.name)
          return ["$#{resolved.path.join(".")}", nil] if resolved.relation.nil?

          relation = mapper.lookup(operand.name)&.relation
          unless relation&.type == :many && relation.field && relation.requires_parent.nil?
            raise UnsupportedError, "Whole-list comparison needs a plain field or a to-many relation's projected field"
          end

          return projected_list(relation, relation.field, mapper)
        end
        unless expression_with?(operand, "map") && operand.operands.length == 2
          raise UnsupportedError, "Whole-list comparison needs a field, a relation projection or a map() over a relation"
        end

        collection, lambda = operand.operands
        unless variable?(collection) && expression_with?(lambda, "lambda") && variable?(lambda.operands[0]) && variable?(lambda.operands[1])
          raise UnsupportedError, "map() in a whole-list comparison must project one field of each element"
        end

        relation = mapper.lookup(collection.name)&.relation
        raise UnsupportedError, "map() in a whole-list comparison needs a to-many relation" unless relation&.type == :many && relation.requires_parent.nil?

        projection = lambda.operands[0].name
        element = lambda.operands[1].name
        field = if projection == element
          relation.field
        elsif projection.start_with?("#{element}.")
          projection[(element.length + 1)..]
        end
        raise UnsupportedError, "map() in a whole-list comparison must project one field of each element" if field.nil?

        projected_list(relation, field, mapper)
      end

      def projected_list(relation, field, mapper)
        config = relation.fields[field]
        nullable = (config.nil? || config.nullable.nil?) ? mapper.nullable_default : config.nullable
        if nullable
          raise UnsupportedError,
            "Whole-list comparison over a nullable element field: a null element is a missing attribute, which CEL raises on"
        end

        path = config&.field || field
        [{"$map" => {"input" => "$#{relation.name}", "in" => "$$this.#{path}"}}, {relation.name => {"$type" => "array"}}]
      end

      # False when CEL cannot order +reference+ against +constant+: the constant is not a number,
      # a string or a boolean (ordering against null, a list or a map is always an error), or it
      # is a scalar of another type than the one +reference+ declares.
      def can_order?(reference, constant, mapper)
        return false unless number?(constant) || string?(constant) || boolean?(constant)

        declared = mapper.value_type(reference)
        return true if declared.nil? || declared == :date_time

        matches_type?(constant, declared)
      end

      def translate_in(operands, mapper, scope)
        left = operand_at(operands, 0, "in requires a left operand")
        right = operand_at(operands, 1, "in requires a right operand")

        if variable?(left) && value?(right)
          raise UnsupportedError, "in with a field on the left requires an array value" unless right.value.is_a?(Array)

          elements = right.value
          if elements.any? { |element| composite?(element) }
            # A field that declares a scalar value_type never equals a list or a map element.
            # Undeclared, a query $in against an array field would also match the field's own
            # elements, which CEL does not.
            config = mapper.resolve_config(left.name)
            unless config&.value_type && config.value_parser.nil?
              raise UnsupportedError, "A list or map element in an `in` list needs the field's scalar value_type declared"
            end

            elements = elements.reject { |element| composite?(element) }
          end

          return value_comparison(
            mapper, scope, left.name,
            {"$in" => elements.map { |element| mapper.apply_value_parser(left.name, element) }},
            elements, "a null element in an `in` list"
          )
        end
        if value?(left) && composite?(left.value)
          return translate_composite_membership(left.value, right, mapper, scope)
        end
        if value?(left) && variable?(right)
          if mapper.relation_of(right.name)&.type == :one
            raise UnsupportedError,
              "`in` over a to-one relation tests the related object's keys in CEL, which are the " \
              "fields its subdocument carries, and the adapter has no filter for a subdocument's field names"
          end

          return value_comparison(
            mapper, scope, right.name,
            {"$eq" => mapper.apply_value_parser(right.name, left.value)},
            left.value, "a null needle in a mapped-collection `in`"
          )
        end

        raise UnsupportedError, "in supports only field-in-value-list or value-in-mapped-collection shapes"
      end

      # `[..] in list`: a list needle, compared whole with each element inside $expr (see
      # translate_composite_equality for when that agrees with CEL). The aggregation $in raises on
      # a non-array, so the list is required to be an array first: a null or absent list is an
      # error to CEL too.
      def translate_composite_membership(needle, haystack, mapper, scope)
        unless needle.is_a?(Array) && needle.all? { |element| comparable_scalar?(element) }
          raise UnsupportedError,
            "Membership of a map, or of a list holding a list, a map or NaN, is unsupported: MongoDB " \
            "compares embedded documents in stored field order and NaN equal to NaN"
        end
        if scope.collection?
          raise UnsupportedError, "List-element membership inside a collection predicate needs $expr, which MongoDB accepts only at the top level"
        end
        unless variable?(haystack)
          raise UnsupportedError, "List-element membership needs a field or a to-many relation's projected field"
        end

        list, guard = list_expression(haystack, mapper)
        guard ||= {mapper.resolve_field(haystack.name).path.join(".") => {"$type" => "array"}}
        {"$and" => [guard, Guards.with_evaluation({"$expr" => {"$in" => [{"$literal" => needle}, list]}}, [haystack], mapper)]}
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
        if values.value.any? { |element| composite?(element) }
          raise UnsupportedError, "hasIntersection with a list or map element is unsupported: a query $in reads it against the element's own contents"
        end

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
        raise to_one_macro_refusal("map") unless relation.type == :many
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
        raise to_one_macro_refusal(operator) unless relation.type == :many

        element = build(condition, mapper.scoped(collection.name, variable.name), Scope.new(variable.name))
        return {relation.name => {"$elemMatch" => element}} if operator == "exists"

        # "No element fails the condition", on a field that must be an array.
        {relation.name => {"$type" => "array", "$not" => {"$elemMatch" => {"$nor" => [element]}}}}
      end

      # A macro over a reference mapped as a to-one relation. To CEL that attribute is a map, and a
      # macro over a map ranges over its KEYS; the adapter translates a macro only as an element
      # match over a collection relation's subdocuments.
      def to_one_macro_refusal(operator)
        UnsupportedError.new(
          "#{operator}() over a to-one relation ranges over the related object's keys in CEL, and " \
          "a MongoDB filter has no form that iterates a subdocument's field names"
        )
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
