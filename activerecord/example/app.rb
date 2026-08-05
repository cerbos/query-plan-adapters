# frozen_string_literal: true

require "json"
require "sinatra/base"

require_relative "authorization"
require_relative "models"

# A small HTTP application that shows the adapter in operation.
#
# Each request that needs authorization does the same four steps:
#
#   1. make a query plan for the principal, the resource kind and the action;
#   2. change that plan into an ActiveRecord::Relation with the adapter;
#   3. add the tenant boundary of the application;
#   4. run the query.
#
# The response contains the SQL. Thus you can see the result of the translation.
#
# WARNING — this is a demonstration of identity only. Do not copy this pattern. The endpoints
# read `user`, `role`, `tenant`, `department`, `scope` and `tags` from the query string,
# because the smoke tests must change the principal from curl. In a true application, you must
# make the principal from your authentication layer: a token that you verified, a session, or
# a client certificate. A caller can write anything in a query string, and thus a query string
# is not an identity. Here `?role=admin` gets the rule for an administrator, and `?tenant=...`
# crosses the tenant boundary.
class ExampleApp < Sinatra::Base
  set :host_authorization, permitted_hosts: []

  # Reads one query parameter as a string.
  #
  # Rack reads `tenant[]=acme&tenant[]=globex` as an array. Such an array goes into
  # `where(tenant: [...])`, which becomes `tenant IN ('acme', 'globex')` and thus opens the
  # tenant boundary to more than one tenant. A parameter that selects rows must be one value.
  def scalar_param(name, default)
    value = params.fetch(name, default)
    return value if value.is_a?(String)

    halt 400, json(
      error: "InvalidParameter",
      message: "The parameter #{name.inspect} must be one value, but it was a #{value.class}."
    )
  end

  # The application makes the principal one time for each request.
  def principal
    {
      id: scalar_param("user", "ana"),
      roles: scalar_param("role", "user").split(","),
      attr: {
        "tenant" => scalar_param("tenant", "acme"),
        "department" => scalar_param("department", "engineering"),
        "scope" => scalar_param("scope", "acme"),
        "allowedTags" => scalar_param("tags", "").split(",").reject(&:empty?)
      }
    }
  end

  def authorized(kind)
    action = params.fetch("action", "view")
    plan, relation = Authorization.relation_for(kind: kind, action: action, principal: principal)

    json(
      kind: kind,
      action: action,
      principal: principal,
      planKind: plan.kind,
      ids: relation.order(:id).pluck(:id),
      sql: relation.to_sql
    )
  rescue Cerbos::ActiveRecord::Error => e
    # The adapter is fail-closed. If it cannot translate a shape of plan correctly, it raises
    # an error, and this application gives no rows. It does not give a filter that is only
    # approximately correct.
    status 422
    json(error: e.class.name, message: e.message)
  end

  def json(payload)
    content_type :json
    JSON.pretty_generate(payload)
  end

  get "/healthz" do
    json(status: "ok")
  end

  get "/photos" do
    authorized("photo")
  end

  get "/albums" do
    authorized("album")
  end

  get "/workspaces" do
    authorized("workspace")
  end

  get "/" do
    json(
      endpoints: ["/photos", "/albums", "/workspaces", "/healthz"],
      parameters: %w[action user role tenant department scope tags],
      example: "/photos?action=view&user=ben&tenant=acme&tags=public"
    )
  end
end

Store.setup!

# The application must listen on all the interfaces. Docker forwards the port from the host,
# and the default address of Sinatra accepts only the connections from inside the container.
ExampleApp.set :bind, "0.0.0.0"
ExampleApp.set :port, Integer(ENV.fetch("PORT", "4567"))
ExampleApp.run!
