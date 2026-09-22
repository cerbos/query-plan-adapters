# frozen_string_literal: true

require "mongoid"

require_relative "../mongodb"

module Cerbos
  module MongoDB
    # Applies a translated plan to a Mongoid model as an ordinary, chainable +Mongoid::Criteria+.
    #
    #   require "cerbos/mongodb/mongoid"
    #
    #   result = Cerbos::MongoDB.query_plan_to_filter(plan: plan, mapper: mapper)
    #   Cerbos::MongoDB::Mongoid.criteria(Document, result).where(archived: false).order(title: 1)
    #
    # Do NOT pass the filter to +Model.where+ yourself. Mongoid converts every constant in a query
    # to the declared type of the field it is compared with, before the query is sent, and CEL
    # never does: on a +Boolean+ field +R.attr.flag == 1+ becomes +flag == true+, and on an
    # +Integer+ field +R.attr.n < "3"+ becomes +n < 3+ — both return documents the PDP denies. An
    # integral +Float+ beyond 64 bits becomes an Integer BSON cannot encode.
    #
    # This helper wraps each top-level field's condition in +Mongoid::RawValue+, Mongoid's own
    # opt-out from that conversion, and recurses only through +$and+, +$or+ and +$nor+, which is
    # the path Mongoid's selector takes. The criteria's selector is then the adapter's filter
    # verbatim; the suites assert that for every corpus action.
    #
    # Field aliases still apply: Mongoid stores a query on +id+ as +_id+, so map a plan variable
    # to the stored field name (+"_id"+), which is what the adapter expects anyway.
    module Mongoid
      LOGICAL = %w[$and $or $nor].freeze

      module_function

      # @param scope [Class, ::Mongoid::Criteria] a Mongoid model, or a criteria to narrow
      # @param result [Result] from {MongoDB.query_plan_to_filter}
      # @return [::Mongoid::Criteria] +scope.none+ for an unconditional deny, +scope+ unchanged
      #   for an unconditional allow, and +scope+ narrowed by the filter otherwise
      def criteria(scope, result)
        scope = scope.criteria unless scope.is_a?(::Mongoid::Criteria)
        return scope.none if result.always_denied?
        return scope if result.always_allowed?

        scope.where(raw(result.filter))
      end

      # The filter with every top-level condition wrapped in +Mongoid::RawValue+.
      def raw(filter)
        filter.to_h do |key, value|
          if LOGICAL.include?(key)
            [key, value.map { |clause| raw(clause) }]
          else
            [key, ::Mongoid::RawValue(value)]
          end
        end
      end
    end
  end
end
