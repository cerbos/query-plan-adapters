# frozen_string_literal: true

require "active_record"

# Namespace shared with the `cerbos` gem, the official Cerbos Ruby SDK.
module Cerbos
  # Turns a Cerbos `PlanResources` response into an `ActiveRecord::Relation`, so the database
  # applies the policy's rules.
  #
  # Start with {.query_plan_to_relation}. The result is a normal relation: add scopes, order,
  # pagination or eager loading as usual.
  #
  # If a plan cannot be translated exactly, the adapter raises an {Error}. It never returns an
  # approximate filter, because that could return rows the PDP denies.
  module ActiveRecord
    # Translate a query plan into a relation of the rows it allows.
    #
    # @param plan [Object] a `Cerbos::Output::PlanResources` from the Ruby SDK
    #   (https://github.com/cerbos/cerbos-sdk-ruby), a parsed `PlanResources` JSON response, or
    #   any object with `kind` and `condition` in those shapes.
    # @param model [Class] the `ActiveRecord::Base` subclass to filter.
    # @param attributes [Hash{String => AttributeMapping::Field, AttributeMapping::Relation}]
    #   plan variable name => model mapping. Build values with {Cerbos::ActiveRecord.field} and
    #   {Cerbos::ActiveRecord.relation}.
    # @param operator_overrides [Hash{String => #call}] operator name => callable that gets the
    #   resolved operands. Use it for shapes your database can express but portable SQL cannot,
    #   such as JSON containment or full-text search.
    # @param null_attribute_representation [Symbol] how the caller sends a NULL column to
    #   Cerbos, for attributes that declare nothing themselves.
    #   `:explicit` (default): the attribute is sent with a null value, so `R.attr.x == null`
    #   matches and `IS NULL` agrees with the PDP.
    #   `:omitted`: the attribute is not sent. CEL errors and the PDP denies, but `IS NULL` would
    #   match, so `==` and `!=` against null are UNKNOWN for a NULL column, and the adapter
    #   refuses any other null constant in the plan (cerbos/query-plan-adapters#551).
    #
    #   Set it per attribute with `null_representation:` on {Cerbos::ActiveRecord.field}; this
    #   value is the fallback. Per-attribute lets one column be mapped twice under different
    #   conventions. Under `:explicit`, `eq`, `ne` and `in` are translated so they never yield
    #   SQL UNKNOWN. Order and string operators are unchanged: CEL errors on a null receiver,
    #   which denies just like UNKNOWN (cerbos/query-plan-adapters#308).
    #
    # @return [ActiveRecord::Relation] a filtered relation.
    # @return [ActiveRecord::Relation] `model.none` if the plan always denies.
    # @return [ActiveRecord::Relation] `model.all` if the plan always allows.
    #
    # @raise [Error] when the adapter cannot translate the plan correctly.
    # @raise [ArgumentError] when `null_attribute_representation` is invalid, or an override
    #   names a structural operator.
    #
    # @example
    #   plan = cerbos.plan_resources(principal: principal, resource: {kind: "document"}, action: "view")
    #
    #   documents = Cerbos::ActiveRecord.query_plan_to_relation(
    #     plan: plan,
    #     model: Document,
    #     attributes: {
    #       "request.resource.attr.ownerId" => Cerbos::ActiveRecord.field("owner_id"),
    #       "request.resource.attr.status"  => Cerbos::ActiveRecord.field("status"),
    #       "request.resource.attr.tags"    => Cerbos::ActiveRecord.relation(
    #         :tags, member_field: "name", fields: {"name" => Cerbos::ActiveRecord.field("name")}
    #       )
    #     }
    #   )
    #
    #   documents.order(:created_at).limit(20)
    #
    # @see https://github.com/cerbos/cerbos-sdk-ruby Cerbos Ruby SDK
    def self.query_plan_to_relation(
      plan:, model:, attributes:,
      operator_overrides: {}, null_attribute_representation: :explicit
    )
      Translator.new(
        model: model,
        attributes: attributes,
        operator_overrides: operator_overrides,
        null_attribute_representation: null_attribute_representation
      ).translate(plan)
    end
  end
end

require_relative "active_record/attribute_mapping"
require_relative "active_record/errors"
require_relative "active_record/plan"
require_relative "active_record/translator"
require_relative "active_record/version"
