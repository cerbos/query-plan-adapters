# frozen_string_literal: true

require "time"
require_relative "errors"

module Cerbos
  module ActiveRecord
    # Reads the `timestamp()` literals in a query plan.
    #
    # @private
    module Timestamps
      RFC3339 = /\A
        (?!0000)(\d{4})-(\d{2})-(\d{2})[Tt]
        (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d
        (?:\.(\d{1,9}))?
        (?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)
      \z/x

      # ActiveRecord binds a Time with at most six fractional digits, and every supported
      # database stores at most six.
      MAX_SUBSECOND_DIGITS = 6

      module_function

      # @param literal [String] an RFC-3339 instant from the plan
      # @return [Time] in UTC
      def parse(literal)
        unless literal.is_a?(::String) && RFC3339.match?(literal)
          raise InvalidPlanError, "Invalid RFC-3339 timestamp literal: #{literal.inspect}"
        end

        assert_representable_precision(literal)

        begin
          ::Time.iso8601(literal).utc
        rescue ArgumentError => e
          raise InvalidPlanError, "Invalid RFC-3339 timestamp literal: #{literal.inspect} (#{e.message})"
        end
      end

      # Raises if the literal has non-zero digits past the sixth fractional place.
      #
      # ActiveRecord would truncate them, so the query would compare against a different
      # instant than the policy. The planner emits such literals for `now()`.
      #
      # @private
      def assert_representable_precision(literal)
        digits = RFC3339.match(literal)[4].to_s
        return if digits.length <= MAX_SUBSECOND_DIGITS
        return if digits[MAX_SUBSECOND_DIGITS..].each_char.all? { |d| d == "0" }

        raise UnsupportedOperatorError,
          "Timestamp literal #{literal.inspect} carries sub-microsecond precision, which " \
          "ActiveRecord truncates when binding a Time into SQL; translating it would " \
          "compare against a different instant than the policy specifies"
      end
    end
  end
end
