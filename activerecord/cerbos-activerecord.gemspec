# frozen_string_literal: true

require_relative "lib/cerbos/active_record/version"

Gem::Specification.new do |spec|
  spec.name = "cerbos-activerecord"
  spec.version = Cerbos::ActiveRecord::VERSION
  spec.authors = ["Cerbos"]
  spec.email = ["info@cerbos.dev"]

  spec.summary = "Translate Cerbos query plans into ActiveRecord relations (WORK IN PROGRESS)"
  spec.description = <<~DESC.tr("\n", " ").strip
    A WORK-IN-PROGRESS PROTOTYPE: unreleased, not used in production, and free to change its
    interface without a deprecation. Do not use it to enforce access control in a live system
    yet. Converts a Cerbos PlanResources response into an ActiveRecord::Relation, so
    authorization rules expressed as Cerbos policies are enforced in the database rather than in
    application code. Shapes the adapter cannot express faithfully raise instead of emitting a
    filter.
  DESC
  spec.homepage = "https://github.com/cerbos/query-plan-adapters"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}/tree/main/activerecord"
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/activerecord/README.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  # 7.1 and not 7.0: the low CI leg is 7.1, and 7.0 is not merely untested here — it predates
  # `query_constraints:`, which the composite-key association in spec/support/edge_case_models.rb
  # needs. A range wider than the legs that run is a claim nothing checks.
  spec.add_dependency "activerecord", ">= 7.1", "< 9.0"
end
