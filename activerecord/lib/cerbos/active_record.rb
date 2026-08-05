# frozen_string_literal: true

require "active_record"

require_relative "active_record/attribute_mapping"
require_relative "active_record/errors"
require_relative "active_record/plan"
require_relative "active_record/translator"
require_relative "active_record/version"

module Cerbos
  # Translates a Cerbos +PlanResources+ response into an +ActiveRecord::Relation+, so
  # authorization rules written as Cerbos policies are enforced by the database instead of
  # by application code.
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
  # The result is an ordinary relation, so it composes with scopes, ordering, pagination and
  # eager loading.
  #
  # A plan shape this adapter cannot express faithfully raises an {ActiveRecord::Error}
  # rather than returning a best-effort filter — a wrong filter would return rows the PDP
  # denies.
  module ActiveRecord
    # @param plan [Object] a +Cerbos::Output::PlanResources+ (from the +cerbos+ gem), the
    #   decoded JSON of a +PlanResources+ response, or any object exposing +kind+ and
    #   +condition+ in those shapes
    # @param model [Class] the +ActiveRecord::Base+ subclass being filtered
    # @param attributes [Hash{String => AttributeMapping::Field, AttributeMapping::Relation}]
    #   plan variable name → mapping onto the model. Build values with
    #   {Cerbos::ActiveRecord.field} and {Cerbos::ActiveRecord.relation}
    # @param operator_overrides [Hash{String => #call}] operator name → callable receiving
    #   the resolved operands, for shapes a particular schema can express better than the
    #   default translation (a JSON containment operator, a full-text index, ...)
    # @return [ActiveRecord::Relation] +model.none+ when the plan always denies,
    #   +model.all+ when it always allows, and a filtered relation otherwise
    # @raise [Error] when the plan cannot be translated faithfully
    def self.query_plan_to_relation(plan:, model:, attributes:, operator_overrides: {})
      Translator.new(
        model: model,
        attributes: attributes,
        operator_overrides: operator_overrides
      ).translate(plan)
    end
  end
end
