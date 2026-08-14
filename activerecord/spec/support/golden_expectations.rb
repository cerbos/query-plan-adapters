# frozen_string_literal: true

# The reader and the writer for golden/expectations.json — the filter this adapter is pinned to
# emit for each corpus action (conformance/README.md, "Golden expectations").
#
# Like spec/support/conformance_corpus.rb this code is duplicated across adapters ON PURPOSE
# (ADR 0007). Do not extract it into conformance/, do not import another adapter's copy, and do
# not add a drift check between them. What the adapters share is the DATA — the wire fixtures,
# the seeds, the classification ledger. The golden expectations are not shared data at all: they
# are this adapter's own output, and they live here rather than under conformance/ for that
# reason.
module GoldenExpectations
  FILE = File.expand_path("../../golden/expectations.json", __dir__)

  ADAPTER = "activerecord"
  REGENERATE = "./scripts/golden-update.sh"

  # The ActiveRecord major the file was generated under, and the reason this asset carries a
  # generator key at all (conformance/README.md, "When the generator is an input").
  #
  # The recorded value is not the translator's return value: the translator returns an
  # ActiveRecord::Relation, and what is written down is that relation RENDERED. Arel and the
  # connection adapter do the rendering, so the ActiveRecord version is an input to the bytes in
  # the same way SQLAlchemy's compiler is to sqlalchemy's asset. CI runs both ends of the range
  # the gemspec declares, so the other leg asserts a pinned divergence list rather than the
  # bytes — see spec/translator_spec.rb.
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

  # The notes of the file about to be overwritten. Header validation is deliberately skipped:
  # the file may legitimately carry an older header, and that is what a header change looks like.
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
