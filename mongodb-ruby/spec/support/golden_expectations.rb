# frozen_string_literal: true

require "fileutils"

# The reader and the writer for golden/expectations.json — the filter this adapter is pinned to
# emit for each corpus action (conformance/README.md, "Golden expectations").
#
# Duplicated across adapters ON PURPOSE (ADR 0007): do not extract it into conformance/, do not
# import another adapter's copy, and do not add a drift check between them.
#
# The recorded value is the translator's return value itself, and needs no renderer: a filter is
# a Hash of Strings, numbers, booleans, nil, Arrays and Hashes — already JSON — plus Time, the one
# BSON value JSON has no spelling for, which is written as {"$date" => "<ISO 8601, milliseconds>"},
# the relaxed Extended JSON form. That encoding is this file's, not a library's, so the asset
# declares no generator: nothing but the plan and the translator can move its bytes. The suite
# asserts that no other type ever reaches it.
module GoldenExpectations
  FILE = File.expand_path("../../golden/expectations.json", __dir__)

  ADAPTER = "mongodb-ruby"
  REGENERATE = "./scripts/golden-update.sh"

  # Commentary. Never compared, and carried across a regeneration.
  NOTE_KEY = "note"

  module_function

  # The emitted filter as the JSON the asset holds. Refuses any value outside the closed set of
  # types above, so a library type — an ObjectId, a BSON::Regexp — cannot reach the file as
  # whatever its #to_json happens to print.
  def encode(value, path = "filter")
    case value
    when Hash
      value.each_key { |key| raise "#{path} has a non-String key #{key.inspect}" unless key.is_a?(String) }
      value.to_h { |key, child| [key, encode(child, "#{path}.#{key}")] }
    when Array then value.each_with_index.map { |child, index| encode(child, "#{path}[#{index}]") }
    when Time then {"$date" => value.utc.iso8601(3)}
    when String, Integer, true, false, nil then value
    when Float
      raise "#{path} is #{value}, which JSON cannot hold" unless value.finite?

      value
    else
      raise "#{path} holds a #{value.class}, which is not a value this adapter emits"
    end
  end

  # @return [Hash{String => Hash}] action => the recorded expectation, `note` removed
  def read
    contents = JSON.parse(File.read(FILE, encoding: "UTF-8"))
    if contents["adapter"] != ADAPTER
      raise "#{FILE} declares adapter #{contents["adapter"].inspect}, not #{ADAPTER.inspect}. The " \
            "file is a flat map of corpus action names, so a copy taken from another adapter parses " \
            "cleanly and would be compared against the wrong translator."
    end

    contents.fetch("expectations").transform_values { |entry| entry.except(NOTE_KEY) }
  end

  def notes
    return {} unless File.exist?(FILE)

    JSON.parse(File.read(FILE, encoding: "UTF-8"))
      .fetch("expectations", {})
      .filter_map { |action, entry| [action, entry[NOTE_KEY]] if entry[NOTE_KEY] }
      .to_h
  rescue JSON::ParserError
    {}
  end

  # @param expectations [Hash{String => Hash}] action => the expectation to record
  def write(expectations)
    carried = notes
    body = expectations.keys.sort.to_h { |action|
      entry = {}
      entry[NOTE_KEY] = carried[action] if carried.key?(action)
      [action, entry.merge(expectations.fetch(action))]
    }

    FileUtils.mkdir_p(File.dirname(FILE))
    File.write(FILE, JSON.pretty_generate({
      "adapter" => ADAPTER,
      "regenerate" => REGENERATE,
      "expectations" => body
    }) + "\n")
  end
end
