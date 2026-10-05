# frozen_string_literal: true

require "date"
require "time"

module Cerbos
  module MongoDB
    # CEL timestamps as BSON dates. A BSON date holds milliseconds, so a literal with more
    # precision, or one outside CEL's range, would become a different instant on the way in.
    module Timestamp
      RFC3339 = /\A((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)\z/

      # The same pattern for MongoDB's PCRE2. `\z` rather than `$`, which also matches before a
      # final newline, so "...Z\n" would pass and +$convert+ would accept it.
      RFC3339_MONGO = '^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)\z'

      # CEL's timestamp range, inclusive.
      MIN = Time.utc(1, 1, 1, 0, 0, 0).freeze
      MAX = Time.utc(9999, 12, 31, 23, 59, Rational(59_999, 1000)).freeze

      # RFC3339 with CEL's full nanosecond precision.
      RFC3339_NANOS = /\A((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)\z/

      # Go's duration syntax (time.ParseDuration), which the planner writes a duration literal in
      # ("86400s"): a sign, then one or more decimal numbers each with a unit.
      DURATION = /\A[+-]?(?:(?:\d+(?:\.\d*)?|\.\d+)(?:ns|us|µs|μs|ms|s|m|h))+\z/
      DURATION_PART = /(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)/
      DURATION_UNIT_MILLISECONDS = {
        "ns" => Rational(1, 1_000_000), "us" => Rational(1, 1000), "µs" => Rational(1, 1000), "μs" => Rational(1, 1000),
        "ms" => 1, "s" => 1000, "m" => 60_000, "h" => 3_600_000
      }.freeze
      # CEL's duration range: a signed 64-bit count of nanoseconds, in whole milliseconds.
      MAX_DURATION_MILLISECONDS = (2**63 - 1) / 1_000_000

      module_function

      # @return [Integer, nil] a Go duration string as a whole number of milliseconds, or nil when
      #   it is not one, is finer than a millisecond, or is outside CEL's range
      def duration_milliseconds(value)
        return nil unless value.is_a?(String)
        return 0 if %w[0 +0 -0].include?(value)
        return nil unless DURATION.match?(value)

        total = value.scan(DURATION_PART).sum(Rational(0)) { |digits, unit|
          whole, fraction = digits.split(".", 2)
          number = Rational(whole.to_s.empty? ? 0 : whole.to_i) +
            (fraction.to_s.empty? ? 0 : Rational(fraction.to_i, 10**fraction.length))
          number * DURATION_UNIT_MILLISECONDS.fetch(unit)
        }
        total = -total if value.start_with?("-")
        return nil unless total.denominator == 1 && total.abs <= MAX_DURATION_MILLISECONDS

        total.to_i
      end

      # A literal finer than a millisecond: the instant truncated to the millisecond below it,
      # or nil when the literal is millisecond-exact, malformed or outside CEL's range.
      #
      # @return [Time, nil]
      def sub_millisecond_floor(value)
        return nil unless value.is_a?(String) && parse(value).nil?

        match = RFC3339_NANOS.match(value)
        return nil unless match && Date.valid_date?(match[1].to_i, match[2].to_i, match[3].to_i)

        instant = Time.iso8601(value.sub(/[Tt]/, "T").sub(/z\z/, "Z")).utc
        return nil unless instant.between?(MIN, MAX)

        floor = Time.at(instant.to_r.floor(3)).utc
        (floor == instant) ? nil : floor
      rescue ArgumentError
        nil
      end

      # @return [Time, nil] the UTC instant, or nil when the literal is not a millisecond-exact
      #   RFC 3339 instant inside CEL's range
      def parse(value)
        return nil unless value.is_a?(String)

        match = RFC3339.match(value)
        return nil unless match && Date.valid_date?(match[1].to_i, match[2].to_i, match[3].to_i)

        instant = Time.iso8601(value.sub(/[Tt]/, "T").sub(/z\z/, "Z")).utc
        instant.between?(MIN, MAX) ? instant : nil
      rescue ArgumentError
        nil
      end
    end
  end
end
