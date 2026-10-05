# frozen_string_literal: true

require "securerandom"

# An in-process gRPC server that answers CerbosService.PlanResources with a recorded plan, so
# the conformance harness can call the real Cerbos Ruby SDK (Cerbos::Client#plan_resources) and
# hand the adapter what the SDK returns: kind as a Symbol, the condition as
# Cerbos::Output::PlanResources::Expression / Value / Variable, and every number as a Float
# (Google::Protobuf::Value holds a double). Not a PDP: it evaluates nothing.
#
# Each call registers its response under a fresh token and sends that token as the request's
# action, so calls never collide. The server starts on first use, on an ephemeral port on the
# loopback interface, and stops at exit.
module StubPdp
  SERVICE = Cerbos::Protobuf::Cerbos::Svc::V1::CerbosService
  RESPONSE = Cerbos::Protobuf::Cerbos::Response::V1::PlanResourcesResponse

  @responses = {}
  @mutex = Mutex.new

  # Answers PlanResources with the response registered under the request's action.
  class Handler < SERVICE::Service
    def initialize(lookup)
      super()
      @lookup = lookup
    end

    def plan_resources(request, _call)
      response = @lookup.call(request.action)
      raise GRPC::NotFound, "no plan registered for action #{request.action.inspect}" unless response

      response
    end
  end

  module_function

  # @param plan [Hash] a `PlanResourcesFilter` as recorded in a golden file
  # @param resource_kind [String]
  # @return [Cerbos::Output::PlanResources] what the SDK returns for that plan
  def plan_resources(plan, resource_kind:)
    token = "stub-#{SecureRandom.uuid}"
    register(token, plan, resource_kind)
    begin
      client.plan_resources(
        principal: {id: "stub", roles: ["USER"]},
        resource: {kind: resource_kind},
        action: token
      )
    ensure
      @mutex.synchronize { @responses.delete(token) }
    end
  end

  def register(token, plan, resource_kind)
    json = JSON.generate(
      "requestId" => token,
      "action" => token,
      "resourceKind" => resource_kind,
      "policyVersion" => "default",
      "filter" => plan
    )
    response = RESPONSE.decode_json(json, ignore_unknown_fields: true)
    @mutex.synchronize { @responses[token] = response }
  end

  def client
    @mutex.synchronize do
      @client ||= Cerbos::Client.new("127.0.0.1:#{start_server}", tls: false)
    end
  end

  # Called with the mutex held.
  def start_server
    server = GRPC::RpcServer.new(pool_size: 4)
    port = server.add_http2_port("127.0.0.1:0", :this_port_is_insecure)
    server.handle(Handler.new(->(action) { @mutex.synchronize { @responses[action] } }))
    Thread.new { server.run }
    raise "the stub PDP did not start" unless server.wait_till_running(10)

    at_exit { server.stop }
    port
  end
end
