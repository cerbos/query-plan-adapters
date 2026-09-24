# frozen_string_literal: true

require "sequel"

require_relative "sequel/attribute_mapping"
require_relative "sequel/errors"
require_relative "sequel/plan"
require_relative "sequel/translator"
require_relative "sequel/version"

module Cerbos
  # Changes a Cerbos +PlanResources+ response into a filtered +Sequel::Dataset+. Thus the
  # database applies the authorization rules from the Cerbos policies, and the application
  # code does not.
  #
  #   plan = cerbos.plan_resources(principal: principal, resource: {kind: "document"}, action: "view")
  #
  #   documents = Cerbos::Sequel.query_plan_to_dataset(
  #     plan: plan,
  #     model: Document,
  #     attributes: {
  #       "request.resource.attr.ownerId" => Cerbos::Sequel.field("owner_id"),
  #       "request.resource.attr.status"  => Cerbos::Sequel.field("status"),
  #       "request.resource.attr.tags"    => Cerbos::Sequel.association(
  #         :tags, member_field: "name", fields: {"name" => Cerbos::Sequel.field("name")}
  #       )
  #     }
  #   )
  #
  #   documents.where(archived: false).order(:created_at).limit(20).all
  #
  # The result is a usual dataset of the model. Thus you can add filters, an order, pagination
  # and eager loading to it.
  #
  # If the adapter cannot translate a shape of plan correctly, it raises a
  # {Sequel::Error}. It does not give a filter that is only approximately correct, because
  # such a filter gives rows that the PDP denies.
  #
  # Inside this namespace, +Sequel+ is this adapter. The library itself is +::Sequel+.
  module Sequel
    # @param plan [Object] a +Cerbos::Output::PlanResources+ from the official Ruby SDK
    #   (https://github.com/cerbos/cerbos-sdk-ruby), the JSON of a +PlanResources+ response
    #   after a parse, or an object that has +kind+ and +condition+ in those shapes
    # @param model [Class, ::Sequel::Dataset] the +Sequel::Model+ to filter, or a dataset of
    #   such a model — +Document.where(archived: false)+ — that the filter is added to. The
    #   associations and the column types are read from the model.
    # @param attributes [Hash{String => AttributeMapping::Field, AttributeMapping::Association}]
    #   the plan variable name and its mapping to the model. Make the values with
    #   {Cerbos::Sequel.field} and {Cerbos::Sequel.association}.
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
    #   {Cerbos::Sequel.field}, and this value is then the fallback
    #   (cerbos/query-plan-adapters#308).
    # @return [::Sequel::Dataset] a dataset that selects no row if the plan always denies, the
    #   unfiltered dataset if the plan always allows, and a filtered dataset for all the other
    #   plans
    # @raise [Error] if the adapter cannot translate the plan correctly
    def self.query_plan_to_dataset(
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
