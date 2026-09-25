# frozen_string_literal: true

require "json"
require "logger"
require "time"

require "cerbos/mongodb"

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end
  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.disable_monkey_patching!
  config.order = :defined
end

Dir[File.expand_path("support/**/*.rb", __dir__)].sort.each { |file| require file }
