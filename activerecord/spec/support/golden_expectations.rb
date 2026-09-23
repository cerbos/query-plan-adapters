# frozen_string_literal: true

# Reads and writes golden/expectations.json: the filter this adapter emits for each corpus
# action (conformance/README.md, "Golden expectations").
#
# Duplicated across adapters on purpose (ADR 0007). Do not extract or share it.
module GoldenExpectations
  FILE = File.expand_path("../../golden/expectations.json", __dir__)

  ADAPTER = "activerecord"
  REGENERATE = "./scripts/golden-update.sh"

  # The ActiveRecord version the file was generated under. The file records the relation
  # rendered as SQL, and ActiveRecord does the rendering, so its version changes the output.
  # The other CI leg asserts a pinned divergence list instead (spec/translator_spec.rb).
  GOLDEN_ACTIVERECORD_MAJOR = "8.0"

  # Commentary. Never compared, and carried across a regeneration.
  NOTE_KEY = "note"

  module_function

  def installed_activerecord_major
    ::ActiveRecord::VERSION::STRING.split(".").first(2).join(".")
  end

  # @return [Hash{String => Hash}] action => the recorded expectation, `note` removed
  def read
    contents = JSON.parse(File.read(FILE))

    if contents["adapter"] != ADAPTER
      raise "#{FILE} declares adapter #{contents["adapter"].inspect}, not #{ADAPTER.inspect}. " \
            "The file is a flat map of corpus action names, so a copy taken from another " \
            "adapter parses cleanly and would be compared against the wrong translator."
    end

    if contents[ADAPTER] != GOLDEN_ACTIVERECORD_MAJOR
      raise "#{FILE} declares ActiveRecord #{contents[ADAPTER].inspect}, not " \
            "#{GOLDEN_ACTIVERECORD_MAJOR.inspect}."
    end

    contents.fetch("expectations").transform_values { |entry| entry.except(NOTE_KEY) }
  end

  # Notes from the file about to be overwritten. The header is not checked: it may be older.
  def notes
    return {} unless File.exist?(FILE)

    JSON.parse(File.read(FILE))
      .fetch("expectations", {})
      .filter_map { |action, entry| [action, entry[NOTE_KEY]] if entry[NOTE_KEY] }
      .to_h
  rescue JSON::ParserError
    {}
  end

  # @param expectations [Hash{String => Hash}] action => the expectation to record
  def write(expectations)
    installed = installed_activerecord_major
    if installed != GOLDEN_ACTIVERECORD_MAJOR
      raise "#{FILE} is generated under ActiveRecord #{GOLDEN_ACTIVERECORD_MAJOR}, and " \
            "#{::ActiveRecord::VERSION::STRING} is installed. Regenerating here would rewrite " \
            "every entry the two render differently and present a toolchain swap as a " \
            "translation change."
    end

    carried = notes
    body = expectations.keys.sort.to_h { |action|
      entry = {}
      entry[NOTE_KEY] = carried[action] if carried.key?(action)
      [action, entry.merge(expectations.fetch(action))]
    }

    FileUtils.mkdir_p(File.dirname(FILE))
    File.write(FILE, JSON.pretty_generate({
      "adapter" => ADAPTER,
      ADAPTER => GOLDEN_ACTIVERECORD_MAJOR,
      "regenerate" => REGENERATE,
      "expectations" => body
    }) + "\n")
  end
end
