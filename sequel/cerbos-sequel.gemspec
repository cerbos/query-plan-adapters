# frozen_string_literal: true

require_relative "lib/cerbos/sequel/version"

Gem::Specification.new do |spec|
  spec.name = "cerbos-sequel"
  spec.version = Cerbos::Sequel::VERSION
  spec.authors = ["Cerbos"]
  spec.email = ["info@cerbos.dev"]

  spec.summary = "Translate Cerbos query plans into Sequel datasets (WORK IN PROGRESS)"
  spec.description = <<~DESC.tr("\n", " ").strip
    A WORK-IN-PROGRESS PROTOTYPE: unreleased, not used in production, and free to change its
    interface without a deprecation. Do not use it to enforce access control in a live system
    yet. Converts a Cerbos PlanResources response into a filtered Sequel::Dataset, so
    authorization rules expressed as Cerbos policies are enforced in the database rather than in
    application code. Shapes the adapter cannot express faithfully raise instead of emitting a
    filter.
  DESC
  spec.homepage = "https://github.com/cerbos/query-plan-adapters"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.3"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}/tree/main/sequel"
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/sequel/README.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  # 5.69 is the first release with the trilogy adapter, which the conformance harness reaches
  # MySQL through. It is the floor CI tests, on every store, so a wider range would be a claim
  # nothing checks.
  spec.add_dependency "sequel", ">= 5.69", "< 6.0"
end
