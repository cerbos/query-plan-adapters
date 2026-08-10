# frozen_string_literal: true

require "active_record"

require_relative "active_record/attribute_mapping"
require_relative "active_record/errors"
require_relative "active_record/plan"
require_relative "active_record/translator"
require_relative "active_record/version"

module Cerbos
  # Changes a Cerbos +PlanResources+ response into an +ActiveRecord::Relation+. Thus the
  # database applies the authorization rules from the Cerbos policies, and the application
  # code does not.
  #
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
  # The result is a usual relation. Thus you can add scopes, an order, pagination and eager
  # loading to it.
  #
  # If the adapter cannot translate a shape of plan correctly, it raises an
  # {ActiveRecord::Error}. It does not give a filter that is only approximately correct,
  # because such a filter gives rows that the PDP denies.
  module ActiveRecord
    # @param plan [Object] a +Cerbos::Output::PlanResources+ from the official Ruby SDK
    #   (https://github.com/cerbos/cerbos-sdk-ruby), the JSON of a +PlanResources+ response
    #   after a parse, or an object that has +kind+ and +condition+ in those shapes
    # @param model [Class] the subclass of +ActiveRecord::Base+ to filter
    # @param attributes [Hash{String => AttributeMapping::Field, AttributeMapping::Relation}]
    #   the plan variable name and its mapping to the model. Make the values with
    #   {Cerbos::ActiveRecord.field} and {Cerbos::ActiveRecord.relation}.
    # @param operator_overrides [Hash{String => #call}] the operator name and a callable
    #   object. The adapter gives the operands to that object after it resolves them. Use this
    #   for a shape that your database can translate correctly but portable SQL cannot. A JSON
    #   containment operator and a full-text index are two examples.
    # @param null_attribute_representation [Symbol] how the caller sends a NULL column to
    #   Cerbos, for each attribute that declares nothing of its own. With +:explicit+, the
    #   default, a NULL column sends an attribute whose value is null, and thus
    #   <tt>R.attr.x == null</tt> is true for that row and +IS NULL+ agrees with the PDP. With
    #   +:omitted+, a NULL column sends no attribute at all. CEL then raises a
    #   missing-attribute error and the PDP denies the row, but +IS NULL+ would give that row.
    #   Thus the adapter refuses each null constant in the plan under +:omitted+.
    #
    #   Declare the convention PER ATTRIBUTE with the +null_representation:+ argument of
    #   {Cerbos::ActiveRecord.field}, and this value is then the fallback. One policy suite can
    #   correctly mix the two: the same column can be mapped twice and sent as an explicit null
    #   under one attribute name and omitted under another, which one option of the call cannot
    #   express. An attribute that declares +:explicit+ makes the equality family (+eq+, +ne+,
    #   +in+) translate so that it can never be SQL UNKNOWN, because CEL holds a null VALUE
    #   under that convention and UNKNOWN keeps the row out under both polarities. The order
    #   and string operators do not change: a null receiver raises a no-overload error in CEL,
    #   which denies exactly as UNKNOWN does (cerbos/query-plan-adapters#308).
    # @return [ActiveRecord::Relation] +model.none+ if the plan always denies, +model.all+ if
    #   the plan always allows, and a filtered relation for all the other plans
    # @raise [Error] if the adapter cannot translate the plan correctly
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
