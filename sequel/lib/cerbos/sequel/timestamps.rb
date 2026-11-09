# frozen_string_literal: true

require "time"
require_relative "errors"

module Cerbos
  module Sequel
    # Reads the +timestamp()+ literals in a query plan.
    module Timestamps
      RFC3339 = /\A
        (?!0000)(\d{4})-(\d{2})-(\d{2})[Tt]
        (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d
        (?:\.(\d{1,9}))?
        (?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)
      \z/x

      # Sequel puts a Time into SQL with a maximum of six decimal places. Each database
      # that this adapter supports keeps a datetime with six decimal places or fewer.
      MAX_SUBSECOND_DIGITS = 6

      module_function

      # @param literal [String] an RFC-3339 instant from the plan
      # @return [Time] in UTC, at the literal's full precision. {.assert_bindable} refuses the
      #   instant if it reaches SQL with more precision than a literalized Time keeps.
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

      DIGITS_KEY = :cerbos_sequel_bindable_subsecond_digits

      # Sequel's MySQL adapter drops every fractional digit of a Time it literalizes unless the
      # connection sets +fractional_seconds: true+.
      #
      # @api private
      def bindable_digits(db)
        return MAX_SUBSECOND_DIGITS unless db.database_type == :mysql

        db.supports_timestamp_usecs? ? MAX_SUBSECOND_DIGITS : 0
      end

      # @api private
      def with_bindable_digits(digits)
        previous = Thread.current[DIGITS_KEY]
        Thread.current[DIGITS_KEY] = digits
        yield
      ensure
        Thread.current[DIGITS_KEY] = previous
      end

      # @api private
      def current_bindable_digits
        Thread.current[DIGITS_KEY] || MAX_SUBSECOND_DIGITS
      end

      # Raises if the instant has more fractional digits than a literalized Time keeps.
      #
      # The query would otherwise compare with a different instant from the one in the policy.
      # Checked where a Time reaches SQL, not where it is parsed: a literal that never reaches
      # SQL (one a type mismatch makes an error, or one compared with another literal) is exact
      # in Ruby.
      #
      # @api private
      def assert_bindable(time)
        digits = current_bindable_digits
        return if (time.subsec * (10**digits)).denominator == 1

        if digits.zero?
          raise UnsupportedOperatorError,
            "Timestamp literal #{time.iso8601(9).inspect} carries fractional seconds, which " \
            "Sequel's MySQL adapter drops when literalizing a Time unless the connection sets " \
            "fractional_seconds: true; translating it would compare against a different " \
            "instant than the policy specifies"
        end

        raise UnsupportedOperatorError,
          "Timestamp literal #{time.iso8601(9).inspect} carries sub-microsecond precision, which " \
          "Sequel truncates when literalizing a Time into SQL; translating it would " \
          "compare against a different instant than the policy specifies"
      end
    end
  end
end
