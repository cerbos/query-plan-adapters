# frozen_string_literal: true

require "time"
require_relative "errors"

module Cerbos
  module ActiveRecord
    # Reads the +timestamp()+ literals in a query plan.
    module Timestamps
      RFC3339 = /\A
        (?!0000)(\d{4})-(\d{2})-(\d{2})[Tt]
        (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d
        (?:\.(\d{1,9}))?
        (?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)
      \z/x

      # ActiveRecord puts a Time into SQL with a maximum of six decimal places. Each database
      # that this adapter supports keeps a datetime with six decimal places or fewer.
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

      # Examines the decimal places of the literal.
      #
      # If a literal has more than six decimal places, ActiveRecord removes the last digits
      # when it puts the literal into SQL. Thus the query compares with a different instant
      # from the instant in the policy. The planner makes such a literal for +now()+, and thus
      # this is a real shape and not only a theoretical one. The adapter must raise an error.
      #
      # @api private
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
