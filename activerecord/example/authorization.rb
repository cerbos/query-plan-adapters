# frozen_string_literal: true

require "cerbos"
require "cerbos/active_record"

require_relative "models"

# The connection between the Cerbos policies and the ActiveRecord models.
#
# This file contains no copy of the rules in policies/. It contains only the attribute maps.
# Each map says where the adapter finds the data for one attribute in the policy.
module Authorization
  F = Cerbos::ActiveRecord.method(:field)
  R = Cerbos::ActiveRecord.method(:relation)

  # One entry for each resource kind: the model to filter, and the attribute map.
  KINDS = {
    "photo" => {
      model: Photo,
      attributes: {
        "request.resource.attr.ownerId" => F.call("owner_id"),
        "request.resource.attr.published" => F.call("published"),
        "request.resource.attr.title" => F.call("title"),
        # A path with dots. The department is on the user and not on the photo. The adapter
        # makes a correlated scalar subquery through the belongs_to association. Thus the
        # number of rows in the result does not increase.
        "request.resource.attr.ownerDepartment" => F.call("owner.department"),
        # A many-to-many association through photo_tags. `member_field` gives the column that
        # replaces a tag when the policy uses the tag as a simple value.
        "request.resource.attr.tags" => R.call(
          :tags, member_field: "name", fields: {"name" => F.call("name")}
        )
      }
    },

    "album" => {
      model: Album,
      attributes: {
        "request.resource.attr.ownerId" => F.call("owner_id"),
        "request.resource.attr.shared" => F.call("shared"),
        # `P.id in R.attr.collaboratorIds` becomes a correlated EXISTS subquery through the
        # join table.
        "request.resource.attr.collaboratorIds" => R.call(:collaborators, member_field: "id")
      }
    },

    "workspace" => {
      model: Workspace,
      attributes: {
        "request.resource.attr.ownerId" => F.call("owner_id"),
        "request.resource.attr.scope" => F.call("scope")
      }
    }
  }.freeze

  module_function

  # CERBOS_HOST is required, and has no default on purpose. The default gRPC ports are the ones
  # every adapter's `cerbos run` test sidecar binds, so a local default does not fail when the
  # variable is unset — it silently plans against whichever PDP happens to hold that port,
  # loaded with some other suite's policies. See demo/README.md.
  def client
    host = ENV.fetch("CERBOS_HOST") do
      raise "CERBOS_HOST is not set. Start the example with scripts/run.sh or scripts/smoke.sh, " \
        "which set it to the PDP in docker-compose.yaml."
    end
    @client ||= Cerbos::Client.new(host, tls: false)
  end

  # Makes the relation for one principal, one resource kind and one action.
  #
  # The tenant boundary is the one condition that this application owns. It is outside the
  # Cerbos filter for an important reason. A rule without a condition gives
  # KIND_ALWAYS_ALLOWED, and then the Cerbos filter selects every row. The boundary must still
  # apply to those rows. Thus the application adds it after the translation.
  def relation_for(kind:, action:, principal:)
    config = KINDS.fetch(kind) { raise ArgumentError, "Unknown resource kind: #{kind}" }

    plan = client.plan_resources(
      principal: principal,
      resource: {kind: kind},
      action: action
    )

    relation = Cerbos::ActiveRecord.query_plan_to_relation(
      plan: plan,
      model: config.fetch(:model),
      attributes: config.fetch(:attributes)
    )

    [plan, relation.where(tenant: principal.fetch(:attr).fetch("tenant"))]
  end
end
