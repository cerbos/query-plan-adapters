# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    # Intermediate values passed between translator steps. Not Arel nodes.
    #
    # @private
    module Values
      # A non-finite CEL double: NaN, +Infinity or -Infinity.
      #
      # Kept out of the database: PostgreSQL sorts NaN above every other double, while Cerbos
      # 0.55 treats unordered comparisons as false (IEEE). Keeping them in Ruby preserves that
      # result under negation.
      IEEEConstant = Struct.new(:value)

      # A ternary held until the enclosing comparison, so a non-finite arm can be evaluated in
      # Ruby instead of going into SQL.
      ConditionalValue = Struct.new(:condition, :then_value, :else_value)

      # A `hierarchy()` path: either a value (string constant or column) with a delimiter, or
      # a list of segments with no delimiter, as `hierarchy(["a", R.id])` makes.
      Hierarchy = Struct.new(:value, :segments, :delimiter)

      # `string()` of a value CEL holds as a double, held until `eq` or `ne` compares it with
      # a literal. See {Translator::Casts#compare_double_text}.
      DoubleText = Struct.new(:value)

      # A resolved collection: the correlated subquery scope from a relation mapping. Used by
      # macros and membership tests.
      Collection = Struct.new(:scope)

      # `filter(collection, lambda)`, held until `size()` consumes it.
      FilteredCollection = Struct.new(:scope, :body)

      # `map(collection, lambda)`, held until `hasIntersection()` consumes it.
      MappedCollection = Struct.new(:scope, :projection)
    end
  end
end
