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
      # @return [Time] in UTC, at the literal's full precision. {.assert_bindable} refuses the
      #   instant if it reaches SQL with more precision than a bound Time keeps.
      def parse(literal)
        unless literal.is_a?(::String) && RFC3339.match?(literal)
          raise InvalidPlanError, "Invalid RFC-3339 timestamp literal: #{literal.inspect}"
        end

        begin
          ::Time.iso8601(literal).utc
        rescue ArgumentError => e
          raise InvalidPlanError, "Invalid RFC-3339 timestamp literal: #{literal.inspect} (#{e.message})"
        end
      end

      # Raises if the instant has sub-microsecond precision.
      #
      # ActiveRecord would truncate it when binding, so the query would compare against a
      # different instant than the policy. The planner emits such literals for `now()`. Checked
      # where a Time is bound, not where it is parsed: a literal that never reaches SQL (one a
      # type mismatch makes an error, or one compared with another literal) is exact in Ruby.
      #
      # @param time [Time]
      # @return [void]
      # @private
      def assert_bindable(time)
        return if (time.subsec * 1_000_000).denominator == 1

        raise UnsupportedOperatorError,
          "Timestamp literal #{time.iso8601(9).inspect} carries sub-microsecond precision, which " \
          "ActiveRecord truncates when binding a Time into SQL; translating it would " \
          "compare against a different instant than the policy specifies"
      end
    end
  end
end
