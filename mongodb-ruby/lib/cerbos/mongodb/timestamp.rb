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

      module_function

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
