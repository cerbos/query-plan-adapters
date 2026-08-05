# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    # The intermediate values that the translator moves between its steps. These values are
    # not Arel nodes.
    module Values
      # A CEL double that is not finite: NaN, +Infinity or -Infinity.
      #
      # These values must not go into the database. PostgreSQL puts NaN above all the other
      # doubles in an ordered comparison. IEEE-754 and CEL do the opposite. The adapter keeps
      # these values as Ruby values. Thus it can calculate each comparison that contains one,
      # and the result is the same for all the dialects.
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

      # A collection after the adapter resolves it. It holds the correlated subquery scope
      # that the relation mapping of an attribute made. A macro or a membership test uses it.
      Collection = Struct.new(:scope)

      # +filter(collection, lambda)+. The translator keeps it until +size()+ uses it.
      FilteredCollection = Struct.new(:scope, :body)

      # +map(collection, lambda)+. The translator keeps it until +hasIntersection()+ uses it.
      MappedCollection = Struct.new(:scope, :projection)
    end
  end
end
