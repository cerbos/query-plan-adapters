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

      VARIADIC = {"and" => "$and", "or" => "$or"}.freeze

      # CEL's double arithmetic, spelled as MongoDB's.
      ARITHMETIC = {"add" => "$add", "sub" => "$subtract", "mult" => "$multiply", "div" => "$divide"}.freeze

      # Guarded by "the expression is not null": each evaluates to null exactly where CEL raises.
      NOT_NULL_GUARDED = %w[string double int size contains startsWith endsWith add sub mult div].freeze

      module_function

      # A plan operand as an aggregation expression: a variable becomes a +$field.path+, a
      # value becomes a constant (see #constant), an expression recurses.
      def build(operand, mapper)
        return "$#{mapper.resolve_field(operand.name).path.join(".")}" if variable?(operand)
        return constant(operand.value) if value?(operand)
        return build_expression(operand, mapper) if expression?(operand)

        raise InvalidPlanError, "Invalid operand structure"
      end

      # A plan constant as an aggregation expression. Inside $expr a string that starts with `$`
      # is a field path (and `$$` a variable), and an array or a document is evaluated element by
      # element, so `R.attr.a + "q" == "$b"` would compare with the document's own `b` field and
      # return documents the PDP denies. $literal keeps each such constant the value it is.
      def constant(value)
        if value.is_a?(Hash) || (value.is_a?(Array) && value.any? { |element| element.is_a?(Hash) || element.is_a?(Array) })
          raise UnsupportedError,
            "A map constant inside an expression is unsupported: MongoDB compares embedded documents in stored field order, CEL's maps ignore it"
        end
        return {"$literal" => value} if value.is_a?(Array) || (value.is_a?(String) && value.start_with?("$"))

        value
      end

      def build_expression(expression, mapper)
        operator = expression.operator
        operands = expression.operands
        return {VARIADIC.fetch(operator) => operands.map { |op| build(op, mapper) }} if VARIADIC.key?(operator)
        return compare(operator, *operands.map { |op| build(op, mapper) }) if COMPARISONS.key?(operator)

        case operator
        when "add" then build_add(operands, mapper)
        when "sub", "mult", "div" then build_arithmetic(operator, operands, mapper)
        when "mod" then build_mod(operands, mapper)
        when "not" then {"$not" => [build(operand_at(operands, 0, "not operator requires an operand"), mapper)]}
        when "string" then build_string(operands, mapper)
        when "double", "int" then refuse_numeric_conversion(operator)
        when "if" then build_if(operands, mapper)
        when "index"
          collection, index = constant_index(operands)
          # A negative or fractional position is an error on every document; its guard (below)
          # keeps every document out, so the value the expression stands for is never read.
          # $arrayElemAt would instead count a negative position from the end.
          return nil if index.nil?

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
          # CEL raises for a negative or fractional list position whatever the document holds.
          return {"$expr" => false} if index.nil?

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
        if operands.any? { |op| value?(op) && string?(op.value) }
          # $concat raises on anything but a string, where CEL has no overload: null instead.
          return with_operands(operands, mapper) { |values|
            {"$cond" => [{"$and" => values.map { |value| {"$eq" => [{"$type" => value}, "string"]} }}, {"$concat" => values}, nil]}
          }
        end

        if operands.all? { |op| variable?(op) }
          raise UnsupportedError,
            "Cannot tell numeric addition from string concatenation in '+' between two fields: " \
            "CEL overloads '+' on strings and the query plan carries no field types, so neither " \
            "$add nor $concat can be chosen"
        end
        build_arithmetic("add", operands, mapper)
      end

      # A CEL comparison inside $expr. MongoDB orders NaN below every number and equal to itself,
      # where every CEL comparison with NaN is false but `!=`, which is true.
      def compare(operator, left, right)
        {"$let" => {
          "vars" => {"left" => left, "right" => right},
          "in" => {"$cond" => [
            {"$or" => [{"$eq" => ["$$left", Float::NAN]}, {"$eq" => ["$$right", Float::NAN]}]},
            operator == "ne",
            {COMPARISONS.fetch(operator) => ["$$left", "$$right"]}
          ]}
        }}
      end

      # Binds each operand to a variable (+$$operand0+, ...) so an expression can read it more
      # than once without building it twice.
      def with_operands(operands, mapper)
        names = operands.each_index.map { |index| "operand#{index}" }
        {"$let" => {
          "vars" => names.zip(operands.map { |op| build(op, mapper) }).to_h,
          "in" => yield(names.map { |name| "$$#{name}" })
        }}
      end

      # CEL arithmetic over doubles: every attribute number reaches CEL as a double, and a plan
      # literal is read as the double it was spelled as (an int literal beside an attribute is a
      # planner divergence the corpus declares). MongoDB keeps an int an int, so int 0 times -1 is
      # 0 where CEL's double is -0.0, and 1 over that is +Infinity where CEL's is -Infinity; so
      # every operand is converted with $toDouble first. A non-number operand is an error to CEL
      # and null here (MongoDB would abort the query instead), which the expression's not-null
      # guard keeps out.
      #
      # Division is IEEE 754's, which $divide is except by zero, where it aborts the query: x / 0
      # is NaN when x is NaN or zero, and otherwise an infinity whose sign is x's, flipped by a
      # negative zero divisor ($toString spells -0.0 "-0").
      #
      # size() is CEL's one int, and int division truncates, so a division over it is refused.
      def build_arithmetic(operator, operands, mapper)
        if operator == "div" && operands.any? { |op| integer_valued?(op) }
          raise UnsupportedError, "Integer division truncates in CEL, and $divide does not: a division over size() is unsupported"
        end

        with_operands(operands, mapper) { |values|
          doubles = values.map { |value| {"$toDouble" => value} }
          result = if operator == "div"
            numerator, denominator = doubles
            divisor = operands[1]
            if value?(divisor) && number?(divisor.value)
              divisor.value.zero? ? divide_by_zero(numerator, denominator, divisor.value) : {"$divide" => doubles}
            else
              {"$cond" => [{"$eq" => [denominator, 0]}, divide_by_zero(numerator, denominator), {"$divide" => doubles}]}
            end
          else
            {ARITHMETIC.fetch(operator) => doubles}
          end
          {"$cond" => [{"$and" => values.map { |value| {"$isNumber" => value} }}, result, nil]}
        }
      end

      # A constant denominator's sign is settled here rather than read back from the server:
      # Mongoid's leg does not keep the sign of a -0.0 literal.
      def divide_by_zero(numerator, denominator, constant = nil)
        negative_zero = if constant.nil?
          {"$eq" => [{"$toString" => denominator}, "-0"]}
        else
          (1.0 / constant.to_f).negative?
        end
        {"$cond" => [
          {"$or" => [{"$eq" => [numerator, Float::NAN]}, {"$eq" => [numerator, 0]}]},
          Float::NAN,
          {"$cond" => [{"$eq" => [{"$lt" => [numerator, 0]}, negative_zero]}, Float::INFINITY, -Float::INFINITY]}
        ]}
      end

      # Whether +operand+ evaluates to a CEL int: size(), or arithmetic over one.
      def integer_valued?(operand)
        return false unless expression?(operand)
        return true if %w[size mod].include?(operand.operator)

        ARITHMETIC.key?(operand.operator) && operand.operands.any? { |child| integer_valued?(child) }
      end

      # CEL's `%` is integer-only: it has no double overload, and every number a resource
      # attribute carries reaches CEL as a double, so `R.attr.x % 2` is a no-such-overload error
      # that denies the document under either polarity, where $mod computes a floating remainder.
      # The one int this pipeline produces is size() (int() is refused), and a zero divisor is an
      # error that $mod turns into an aborted query, so the divisor must be a non-zero integer
      # constant.
      def build_mod(operands, mapper)
        dividend, divisor = operands
        unless dividend && expression?(dividend) && dividend.operator == "size"
          raise UnsupportedError,
            "mod operator requires an integer dividend: CEL's % has no double overload and every " \
            "attribute number reaches CEL as a double, so the modulo is an error on every " \
            "document where $mod would compute a floating remainder"
        end
        unless divisor && value?(divisor) && divisor.value.is_a?(Numeric) && integral?(divisor.value) && !divisor.value.zero?
          raise UnsupportedError,
            "mod operator requires a non-zero integer constant divisor: $mod aborts the whole " \
            "query on a zero divisor instead of denying that document"
        end

        {"$mod" => operands.map { |op| build(op, mapper) }}
      end

      def build_string(operands, mapper)
        operand = operand_at(operands, 0, "string conversion requires an operand")
        if renders_untyped_integral_constant?(operand)
          raise UnsupportedError,
            "string() over an integral constant whose int or double type the plan does not carry: " \
            "CEL renders the int 1000000 as \"1000000\" and the double as \"1e+06\", and the plan " \
            "ships both as the same bare number"
        end

        input = build(operand, mapper)
        {"$switch" => {
          "branches" => [
            {"case" => {"$in" => [{"$type" => input}, %w[int long double decimal]]},
             "then" => cel_double_to_string({"$toDouble" => input})},
            {"case" => {"$in" => [{"$type" => input}, %w[string bool]]}, "then" => {"$toString" => input}}
          ],
          "default" => nil
        }}
      end

      # Whether string() over the operand could render a numeric constant, bare or as a ternary
      # branch, whose int or double type the plan dropped: the plan ships 1000000 and 1000000.0
      # alike. Only an integral magnitude of 1e6 or more renders differently, since Go's shortest
      # %g switches a double to an exponent there ("1e+06") while an int stays plain decimal.
      def renders_untyped_integral_constant?(operand)
        if value?(operand)
          return number?(operand.value) && integral?(operand.value) && operand.value.abs >= 1_000_000
        end
        return false unless expression_with?(operand, "if")

        operand.operands.drop(1).any? { |branch| renders_untyped_integral_constant?(branch) }
      end

      # CEL's string() of a double: every attribute number reaches CEL as a double, which cel-go
      # prints as Go's shortest %g (strconv.FormatFloat(d, 'g', -1, 64)). $toString agrees with
      # it (shortest digits, e+XX/e-XX exponents, -0, NaN) except in two places: it keeps fixed
      # notation up to an exponent of 15 where Go switches at 6 ("1000000" for "1e+06"), and it
      # spells the infinities "Infinity" where Go spells them "+Inf" and "-Inf". Both are
      # rewritten here; for 1e6 <= |d| < 1e16 the fixed form holds exactly the shortest digits,
      # so moving the point is enough.
      def cel_double_to_string(double)
        exponent_form = {"$let" => {
          "vars" => {"fixed" => {"$toString" => {"$abs" => "$$d"}}},
          "in" => {"$let" => {
            "vars" => {
              "point" => {"$indexOfCP" => ["$$fixed", "."]},
              "digits" => {"$rtrim" => {
                "input" => {"$replaceAll" => {"input" => "$$fixed", "find" => ".", "replacement" => ""}},
                "chars" => "0"
              }}
            },
            "in" => {"$let" => {
              "vars" => {"exponent" => {"$subtract" => [
                {"$cond" => [{"$eq" => ["$$point", -1]}, {"$strLenCP" => "$$fixed"}, "$$point"]}, 1
              ]}},
              "in" => {"$concat" => [
                {"$cond" => [{"$lt" => ["$$d", 0]}, "-", ""]},
                {"$substrCP" => ["$$digits", 0, 1]},
                {"$cond" => [
                  {"$gt" => [{"$strLenCP" => "$$digits"}, 1]},
                  {"$concat" => [".", {"$substrCP" => ["$$digits", 1, {"$strLenCP" => "$$digits"}]}]},
                  ""
                ]},
                "e+",
                {"$cond" => [{"$lt" => ["$$exponent", 10]}, "0", ""]},
                {"$toString" => "$$exponent"}
              ]}
            }}
          }}
        }}
        {"$let" => {
          "vars" => {"d" => double},
          "in" => {"$switch" => {
            "branches" => [
              {"case" => {"$eq" => ["$$d", Float::INFINITY]}, "then" => "+Inf"},
              {"case" => {"$eq" => ["$$d", -Float::INFINITY]}, "then" => "-Inf"},
              {"case" => {"$and" => [{"$gte" => [{"$abs" => "$$d"}, 1e6]}, {"$lt" => [{"$abs" => "$$d"}, 1e16]}]},
               "then" => exponent_form}
            ],
            "default" => {"$toString" => "$$d"}
          }}
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

        # Reached through the stored parent array, `$parent.children` is one array PER parent
        # element, so $size of it counts parents, not children. The chain's list is every child
        # of the (one) parent: flatten before counting.
        children = {"$size" => {"$reduce" => {
          "input" => {"$ifNull" => [inner, []]},
          "initialValue" => [],
          "in" => {"$concatArrays" => ["$$value", {"$cond" => [{"$isArray" => "$$this"}, "$$this", []]}]}
        }}}
        # An absent to-one parent counts as UNKNOWN, not 0. null loses against every number in
        # BSON order, so both `== 0` and `>= 0` exclude the document (#309).
        {"$cond" => [{"$gt" => [{"$size" => {"$ifNull" => ["$#{parent}", []]}}, 0]}, children, nil]}
      end

      def build_matches(operands, mapper)
        input, pattern = operands
        unless input && pattern && value?(pattern) && string?(pattern.value)
          raise InvalidPlanError, "matches operator requires two operands"
        end

        {"$regexMatch" => {"input" => build(input, mapper), "regex" => constant(Regex.normalise_re2(pattern.value))}}
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

      # The position of an index expression: a non-negative Integer, or nil for a numeric
      # constant CEL can never index with (negative, or fractional: cel-go accepts a double
      # position only when it is integral), which raises on every document.
      #
      # @return [Array(Plan node, Integer or nil)]
      def constant_index(operands)
        collection, index = operands
        raise InvalidPlanError, "index operator requires two operands" unless collection && index
        unless value?(index) && number?(index.value)
          raise UnsupportedError, "index operator requires a numeric constant position"
        end

        position = index.value
        return [collection, nil] if !integral?(position) || position.negative?

        [collection, position.to_i]
      end
    end
  end
end
