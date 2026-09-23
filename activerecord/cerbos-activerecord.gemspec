# frozen_string_literal: true

require_relative "lib/cerbos/active_record/version"

Gem::Specification.new do |spec|
  spec.name = "cerbos-activerecord"
  spec.version = Cerbos::ActiveRecord::VERSION
  spec.summary = "Translate Cerbos query plans into ActiveRecord relations (WORK IN PROGRESS)"
  spec.description = <<~DESC.tr("\n", " ").strip
    A WORK-IN-PROGRESS PROTOTYPE: unreleased, not used in production, and free to change its
    interface without a deprecation. Do not use it to enforce access control in a live system
    yet. Converts a Cerbos PlanResources response into an ActiveRecord::Relation, so
    authorization rules expressed as Cerbos policies are enforced in the database rather than in
    application code. Shapes the adapter cannot express faithfully raise instead of emitting a
    filter.
  DESC
  spec.authors = ["Cerbos"]
  spec.email = ["help@cerbos.dev"]
  spec.license = "Apache-2.0"

  spec.homepage = "https://github.com/cerbos/query-plan-adapters"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/activerecord/CHANGELOG.md"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}/tree/main/activerecord"

  spec.metadata["rubygems_mfa_required"] = "true"

  spec.require_paths = ["lib"]
  spec.files = Dir[
    "lib/**/*.rb",
    ".yardopts",
    "cerbos-activerecord.gemspec",
    "CHANGELOG.md",
    "LICENSE.txt",
    "README.md"
  ]

  spec.required_ruby_version = ">= 3.3"
  # Not 7.0: CI's lowest leg is 7.1, and 7.0 lacks `query_constraints:`, which the
  # composite-key association in spec/support/edge_case_models.rb needs.
  spec.add_dependency "activerecord", ">= 7.1", "< 9.0"
end
