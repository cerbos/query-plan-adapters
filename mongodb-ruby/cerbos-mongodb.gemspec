# frozen_string_literal: true

require_relative "lib/cerbos/mongodb/version"

Gem::Specification.new do |spec|
  spec.name = "cerbos-mongodb"
  spec.version = Cerbos::MongoDB::VERSION
  spec.authors = ["Cerbos"]
  spec.email = ["info@cerbos.dev"]

  spec.summary = "Translate Cerbos query plans into MongoDB query filters (WORK IN PROGRESS)"
  spec.description = <<~DESC.tr("\n", " ").strip
    A WORK-IN-PROGRESS PROTOTYPE: unreleased, not used in production, and free to change its
    interface without a deprecation. Do not use it to enforce access control in a live system
    yet. Converts a Cerbos PlanResources response into a MongoDB query filter for the official
    Ruby driver, so authorization rules expressed as Cerbos policies are enforced in the
    database rather than in application code. Shapes the adapter cannot express faithfully
    raise instead of emitting a filter.
  DESC
  spec.homepage = "https://github.com/cerbos/query-plan-adapters"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}/tree/main/mongodb-ruby"
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/mongodb-ruby/README.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  # No runtime dependency. The filter is a Hash of Strings, numbers, booleans, nil, Arrays and
  # Time — values every BSON encoder takes — so the adapter works with whichever version of the
  # `mongo` driver (or `bson` gem) the application already uses.
end
