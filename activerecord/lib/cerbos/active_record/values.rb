# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    # Intermediate values the translator passes around that are not (yet) Arel nodes.
    module Values
      # A non-finite CEL double (NaN, ±Infinity).
      #
      # These must never reach the database: PostgreSQL orders NaN as greater than every
      # other double, which is the opposite of IEEE-754 (and of CEL). Holding them as Ruby
      # values lets every comparison involving one be folded dialect-independently.
      IEEEConstant = Struct.new(:value)

      # A ternary held back until its surrounding comparison, so an arm that folded to a
      # non-finite constant can still be resolved arithmetically instead of being bound into
      # dialect SQL.
      ConditionalValue = Struct.new(:condition, :then_value, :else_value)

      # +hierarchy(value, delimiter)+ — a path, held either as a delimited value (a string
      # constant or a column) or as an explicit list of segments, which is what
      # +hierarchy(["a", R.id])+ produces. A list-built hierarchy carries no delimiter
      # meaning: its segments are already split.
      Hierarchy = Struct.new(:value, :segments, :delimiter)

      # A resolved collection: the correlated subquery scope an attribute's relation mapping
      # produced, ready for a macro or a membership test to consume.
      Collection = Struct.new(:scope)

      # +filter(collection, lambda)+, deferred until +size()+ consumes it.
      FilteredCollection = Struct.new(:scope, :body)

      # +map(collection, lambda)+, deferred until +hasIntersection()+ consumes it.
      MappedCollection = Struct.new(:scope, :projection)
    end
  end
end
