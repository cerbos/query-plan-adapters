# frozen_string_literal: true

require "cerbos"
require "fileutils"
require "json"
require "sequel"

require "cerbos/sequel"

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end
  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.disable_monkey_patching!
  config.order = :defined
end

# In dependency order and not alphabetically: the models read their tables when they are
# defined, so the database must exist first, and the seeder reads the corpus.
%w[
  database conformance_corpus conformance_store corpus_attributes edge_case_models
].each { |file| require_relative "support/#{file}" }
