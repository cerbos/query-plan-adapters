# frozen_string_literal: true

module Cerbos
  module Sequel
    # The intermediate values that the translator moves between its steps. These values are
    # not Sequel expressions.
    module Values
      # A CEL double that is not finite: NaN, +Infinity or -Infinity.
      #
      # These values must not go into the database. PostgreSQL puts NaN above all the other
      # doubles in an ordered comparison. Cerbos 0.55 uses IEEE false for unordered comparisons.
      # The adapter keeps these as Ruby values so comparisons preserve that result under
      # negation without depending on the database's non-finite number semantics.
      IEEEConstant = Struct.new(:value)

      # A ternary that the translator keeps until it finds the comparison around it. If one
      # arm of the ternary is a constant that is not finite, the translator can calculate that
      # arm. Thus it does not put the value into the SQL of the dialect.
      ConditionalValue = Struct.new(:condition, :then_value, :else_value)

      # +hierarchy(value, delimiter)+ is a path. The adapter keeps the path in one of two
      # forms: a value with a delimiter (a string constant or a column), or a list of
      # segments. +hierarchy(["a", R.id])+ makes the list form. A list has no delimiter,
      # because its segments are already separate.
      Hierarchy = Struct.new(:value, :segments, :delimiter)

      # `filter()` over a list of constants, or a list of constants `except()` another list: the
      # constant elements, and for each one whether it stays (a predicate, or a Ruby boolean
      # where the translator could decide). Held until `size()` counts it.
      ConstantList = Struct.new(:elements, :keeps)

      # `map()` over a list of constants: one projection per element. Held until `in` looks a
      # needle up in it.
      ConstantProjection = Struct.new(:projections)

      # One `set-field` of a map literal: a key and its constant value, held until the
      # `struct` around it builds the Hash.
      MapEntry = Struct.new(:key, :value)

      # `string()` of a value CEL holds as a double, held until `eq` or `ne` compares it with
      # a literal. See {Translator::Casts#compare_double_text}.
      DoubleText = Struct.new(:value)

      # A collection after the adapter resolves it. It holds the correlated subquery scope
      # that the association mapping of an attribute made. A macro or a membership test uses it.
      Collection = Struct.new(:scope)

      # +filter(collection, lambda)+. The translator keeps it until +size()+ uses it.
      FilteredCollection = Struct.new(:scope, :body)

      # +map(collection, lambda)+. The translator keeps it until +hasIntersection()+ uses it.
      MappedCollection = Struct.new(:scope, :projection)
    end
  end
end
