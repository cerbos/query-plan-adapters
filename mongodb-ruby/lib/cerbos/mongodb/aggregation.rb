# frozen_string_literal: true

require_relative "errors"
require_relative "operands"
require_relative "regex"
require_relative "timestamp"

module Cerbos
  module MongoDB
    # Aggregation-expression lowering, for everything that has to be evaluated inside +$expr+:
    # arithmetic, conversions, ternaries, +size+, positional reads, and comparisons between two
    # fields.
    #
    # Each operator may also declare a GUARD: the filter that keeps out every document on which
    # the expression cannot be evaluated — where CEL raises and +check()+ denies, while the
    # pipeline would carry on with a null or a wrongly typed value. The enclosing filter walks
    # its operands for guards and ANDs each one alongside itself (see Guards.with_evaluation).
    module Aggregation
      include Operands
      extend Operands

      COMPARISONS = {
        "eq" => "$eq", "ne" => "$ne", "lt" => "$lt", "le" => "$lte", "gt" => "$gt", "ge" => "$gte"
      }.freeze

      VARIADIC = COMPARISONS.merge(
        "and" => "$and", "or" => "$or", "sub" => "$subtract", "mult" => "$multiply", "mod" => "$mod"
      ).freeze

      # Guarded by "the expression is not null": each evaluates to null exactly where CEL raises.
      NOT_NULL_GUARDED = %w[string double int size contains startsWith endsWith].freeze

      module_function

      # A plan operand as an aggregation expression: a variable becomes a +$field.path+, a
      # value becomes itself, an expression recurses.
      def build(operand, mapper)
        return "$#{mapper.resolve_field(operand.name).path.join(".")}" if variable?(operand)
        return operand.value if value?(operand)
        return build_expression(operand, mapper) if expression?(operand)

        raise InvalidPlanError, "Invalid operand structure"
      end

      def build_expression(expression, mapper)
        operator = expression.operator
        operands = expression.operands
        return {VARIADIC.fetch(operator) => operands.map { |op| build(op, mapper) }} if VARIADIC.key?(operator)

        case operator
        when "add" then build_add(operands, mapper)
        when "div" then build_div(operands, mapper)
        when "not" then {"$not" => [build(operand_at(operands, 0, "not operator requires an operand"), mapper)]}
        when "string" then build_string(operands, mapper)
        when "double", "int" then refuse_numeric_conversion(operator)
        when "if" then build_if(operands, mapper)
        when "index"
          collection, index = constant_index(operands)
          # $arrayElemAt keeps each element's BSON type, so `true` is never `1` here and a null
          # element stays a null value, as it is to CEL.
          {"$arrayElemAt" => [build(collection, mapper), index]}
        when "get-field" then build_get_field(operands, mapper)
        when "size" then build_size(operands, mapper)
        when "matches" then build_matches(operands, mapper)
        when "contains" then build_string_predicate(expression, mapper) { |index, _, _| {"$gte" => [index, 0]} }
        when "startsWith" then build_string_predicate(expression, mapper) { |index, _, _| {"$eq" => [index, 0]} }
        when "endsWith" then build_string_predicate(expression, mapper) { |_, receiver, needle| ends_with(receiver, needle) }
        when "timestamp" then build_timestamp(operands, mapper)
        else
          raise UnsupportedError, "Unsupported operator inside aggregation expression: #{operator}"
        end
      end

      # The evaluation guards for every guarded expression below +operand+, outermost first.
      def evaluation_guards(operand, mapper)
        return [] unless expression?(operand)

        guard = guard_for(operand, mapper)
        nested = operand.operands.flat_map { |child| evaluation_guards(child, mapper) }
        guard ? [guard] + nested : nested
      end

      def guard_for(expression, mapper)
        operator = expression.operator
        operands = expression.operands
        case operator
        when *NOT_NULL_GUARDED then not_null_guard(expression, mapper)
        when "timestamp"
          # A literal is validated at translation time; only a field can fail per document.
          (operands[0] && !value?(operands[0])) ? not_null_guard(expression, mapper) : nil
        when "index"
          # An out-of-range index is an error to CEL, not a missing value.
          collection_operand, index = constant_index(operands)
          collection = build(collection_operand, mapper)
          {"$expr" => {"$cond" => {
            "if" => {"$isArray" => collection},
            "then" => {"$gt" => [{"$size" => collection}, index]},
            "else" => false
          }}}
        when "matches"
          input = operand_at(operands, 0, "matches operator requires an input operand")
          {"$expr" => {"$eq" => [{"$type" => build(input, mapper)}, "string"]}}
        end
      end

      def not_null_guard(expression, mapper)
        {"$expr" => {"$ne" => [build_expression(expression, mapper), nil]}}
      end

      # CEL overloads `+` on strings and MongoDB does not: `$add` takes numeric and date types
      # only, and the server aborts the whole query rather than returning no documents.
      # `$concat` is the string spelling. A CONSTANT settles which overload it is — CEL has no
      # mixed-type `+`, so one string operand means every operand is a string. Between two field
      # paths there is no constant and the plan carries no field types, so neither spelling can
      # be chosen and the shape is refused.
      def build_add(operands, mapper)
        built = -> { operands.map { |op| build(op, mapper) } }
        return {"$concat" => built.call} if operands.any? { |op| value?(op) && string?(op.value) }

        if operands.all? { |op| variable?(op) }
          raise UnsupportedError,
            "Cannot tell numeric addition from string concatenation in '+' between two fields: " \
            "CEL overloads '+' on strings and the query plan carries no field types, so neither " \
            "$add nor $concat can be chosen"
        end
        {"$add" => built.call}
      end

      def build_div(operands, mapper)
        denominator = operands[1]
        unless denominator && value?(denominator) && number?(denominator.value) && !denominator.value.zero?
          raise UnsupportedError, "div operator requires a non-zero constant denominator"
        end

        {"$divide" => operands.map { |op| build(op, mapper) }}
      end

      def build_string(operands, mapper)
        input = build(operand_at(operands, 0, "string conversion requires an operand"), mapper)
        {"$cond" => {
          "if" => {"$in" => [{"$type" => input}, %w[string bool int long double decimal]]},
          "then" => {"$convert" => {"input" => input, "to" => "string", "onError" => nil, "onNull" => nil}},
          "else" => nil
        }}
      end

      # CEL's int()/double() are not $convert. CEL reads a WHOLE string or raises, and an error
      # DENIES; $convert parses a leading numeric prefix, so "100%_done" becomes 100 and the
      # filter returns documents the PDP denies. The numeric direction is no safer: CEL truncates
      # toward zero while $convert to "long" rounds. Nothing in the plan says what type the field
      # holds, so no conversion is faithful for every document.
      def refuse_numeric_conversion(operator)
        raise UnsupportedError,
          "'#{operator}()' cannot be translated: $convert parses a numeric prefix where CEL " \
          "requires the whole string and raises otherwise, and rounds where CEL truncates toward zero"
      end

      def build_if(operands, mapper)
        condition, then_operand, else_operand = operands
        raise InvalidPlanError, "if operator requires three operands" unless condition && then_operand && else_operand

        {"$cond" => {
          "if" => build(condition, mapper),
          "then" => build(then_operand, mapper),
          "else" => build(else_operand, mapper)
        }}
      end

      def build_get_field(operands, mapper)
        input, field = operands
        raise InvalidPlanError, "get-field requires an input and a field name" unless input && field && variable?(field)

        {"$getField" => {"field" => field.name, "input" => build(input, mapper)}}
      end

      def build_size(operands, mapper)
        operand = operand_at(operands, 0, "size operator requires an operand")
        inner = build(operand, mapper)
        # $size for an array, $strLenCP for a string, and null — an error to CEL — otherwise.
        size = {"$cond" => [
          {"$isArray" => inner},
          {"$size" => inner},
          {"$cond" => [{"$eq" => [{"$type" => inner}, "string"]}, {"$strLenCP" => inner}, nil]}
        ]}
        parent = variable?(operand) ? mapper.relation_of(operand.name)&.requires_parent : nil
        return size if parent.nil?

        # An absent to-one parent counts as UNKNOWN, not 0. null loses against every number in
        # BSON order, so both `== 0` and `>= 0` exclude the document (#309).
        {"$cond" => [{"$gt" => [{"$size" => {"$ifNull" => ["$#{parent}", []]}}, 0]}, size, nil]}
      end

      def build_matches(operands, mapper)
        input, pattern = operands
        unless input && pattern && value?(pattern) && string?(pattern.value)
          raise InvalidPlanError, "matches operator requires two operands"
        end

        {"$regexMatch" => {"input" => build(input, mapper), "regex" => Regex.normalise_re2(pattern.value)}}
      end

      # contains/startsWith/endsWith over two strings; null (an error to CEL) otherwise.
      def build_string_predicate(expression, mapper)
        receiver_operand, needle_operand = expression.operands
        raise InvalidPlanError, "#{expression.operator} requires two operands" unless receiver_operand && needle_operand

        receiver = build(receiver_operand, mapper)
        needle = build(needle_operand, mapper)
        {"$cond" => [
          {"$and" => [
            {"$eq" => [{"$type" => receiver}, "string"]},
            {"$eq" => [{"$type" => needle}, "string"]}
          ]},
          yield({"$indexOfCP" => [receiver, needle]}, receiver, needle),
          nil
        ]}
      end

      def ends_with(receiver, needle)
        receiver_length = {"$strLenCP" => receiver}
        needle_length = {"$strLenCP" => needle}
        {"$cond" => {
          "if" => {"$gte" => [receiver_length, needle_length]},
          "then" => {"$eq" => [
            {"$substrCP" => [receiver, {"$subtract" => [receiver_length, needle_length]}, needle_length]},
            needle
          ]},
          "else" => false
        }}
      end

      def build_timestamp(operands, mapper)
        operand = operand_at(operands, 0, "timestamp operator requires an operand")
        if value?(operand)
          instant = Timestamp.parse(operand.value)
          if instant.nil?
            raise InvalidPlanError,
              "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range"
          end

          return instant
        end
        timestamp_conversion(build(operand, mapper))
      end

      # A field as a CEL timestamp: a stored date as-is, an RFC 3339 string converted, anything
      # else — including an instant outside CEL's range — null.
      def timestamp_conversion(input)
        converted = {"$cond" => {
          "if" => {"$eq" => [{"$type" => input}, "date"]},
          "then" => input,
          "else" => {"$cond" => {
            "if" => {"$cond" => {
              "if" => {"$eq" => [{"$type" => input}, "string"]},
              "then" => {"$regexMatch" => {"input" => input, "regex" => Timestamp::RFC3339_MONGO}},
              "else" => false
            }},
            "then" => {"$convert" => {"input" => input, "to" => "date", "onError" => nil, "onNull" => nil}},
            "else" => nil
          }}
        }}
        {"$let" => {
          "vars" => {"converted" => converted},
          "in" => {"$cond" => {
            "if" => {"$and" => [
              {"$ne" => ["$$converted", nil]},
              {"$gte" => ["$$converted", Timestamp::MIN]},
              {"$lte" => ["$$converted", Timestamp::MAX]}
            ]},
            "then" => "$$converted",
            "else" => nil
          }}
        }}
      end

      # @return [Array(Plan node, Integer)]
      def constant_index(operands)
        collection, index = operands
        raise InvalidPlanError, "index operator requires two operands" unless collection && index
        unless value?(index) && integral?(index.value) && index.value >= 0
          raise UnsupportedError, "index operator requires a non-negative integer constant"
        end

        [collection, index.value.to_i]
      end
    end
  end
end
