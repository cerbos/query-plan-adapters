# frozen_string_literal: true

require_relative "mongodb/errors"
require_relative "mongodb/mapper"
require_relative "mongodb/plan"
require_relative "mongodb/translator"
require_relative "mongodb/version"

module Cerbos
  # Turns a Cerbos +PlanResources+ response into a MongoDB query filter, so the authorization
  # rules in your Cerbos policies are enforced by MongoDB rather than by application code.
  #
  #   plan = cerbos.plan_resources(principal: principal, resource: {kind: "document"}, action: "view")
  #
  #   result = Cerbos::MongoDB.query_plan_to_filter(
  #     plan: plan,
  #     mapper: {
  #       "request.resource.attr.ownerId" => {field: "owner_id"},
  #       "request.resource.attr.tags" => {relation: {name: "tags", type: :many, field: "name"}}
  #     }
  #   )
  #
  #   documents = client[:documents].find(result.filter) unless result.always_denied?
  #
  # The filter is a plain Hash of BSON-serialisable values with String keys, so it composes with
  # anything that takes a query document: the official +mongo+ driver's +find+, +count_documents+
  # and a +$match+ stage. Combine it with your own predicate under +$and+.
  #
  # A shape the adapter cannot express with CEL's semantics raises {MongoDB::Error}. It never
  # returns a filter that is only approximately right, because such a filter returns documents
  # the PDP denies.
  module MongoDB
    # The translated plan.
    #
    # +filter+ is the query document to apply: for an unconditional allow it is +{}+ (match
    # every document), and for an unconditional deny it is +{"$expr" => false}+ (match none), so
    # the filter is always safe to hand to +find+ as-is. Check {#always_denied?} to skip the
    # query altogether.
    Result = Struct.new(:kind, :filter) do
      def always_allowed? = kind == Plan::ALWAYS_ALLOWED

      def always_denied? = kind == Plan::ALWAYS_DENIED

      def conditional? = kind == Plan::CONDITIONAL
    end

    MATCH_ALL = {}.freeze
    MATCH_NONE = {"$expr" => false}.freeze

    # @param plan [Object] a +Cerbos::Output::PlanResources+ from the official Ruby SDK, the
    #   parsed JSON of a +PlanResources+ response, or an object with +kind+ and +condition+
    # @param mapper [Hash{String => Hash}, #call] how plan variables map to document paths; see
    #   {Mapper}. Omit it only when documents use the plan's own attribute paths verbatim.
    # @param null_attribute_representation [Symbol] how the application sends a NULL field to
    #   +check()+. With +:explicit+ (the default) it sends an explicit null, so
    #   <tt>R.attr.x == null</tt> is true for that document and matching null agrees with the
    #   PDP. With +:omitted+ it sends no attribute at all; CEL then raises a missing-attribute
    #   error and the PDP denies, so every null comparison operand is refused rather than
    #   translated (cerbos/query-plan-adapters#302). Declare +nullable: true+ on a mapper entry
    #   to state per field that a stored null is a missing attribute.
    # @return [Result]
    # @raise [Error] if the plan cannot be translated faithfully
    def self.query_plan_to_filter(plan:, mapper: {}, null_attribute_representation: :explicit)
      translator = Translator.new(mapper: Mapper.wrap(mapper), null_representation: null_attribute_representation)
      normalised = Plan.normalise(plan)
      case normalised.kind
      when Plan::ALWAYS_ALLOWED then Result.new(Plan::ALWAYS_ALLOWED, MATCH_ALL)
      when Plan::ALWAYS_DENIED then Result.new(Plan::ALWAYS_DENIED, MATCH_NONE)
      else Result.new(Plan::CONDITIONAL, translator.translate(normalised.condition))
      end
    end
  end
end
