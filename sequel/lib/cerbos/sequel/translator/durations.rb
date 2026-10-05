# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # `duration()`, `timeSince()`, and a timestamp shifted by a duration.
      #
      # SQL has no portable interval arithmetic, so nothing here reaches the database as
      # arithmetic: the duration moves to the constant side of the comparison and is applied to
      # the instant in Ruby, leaving `column <op> instant`.
      module Durations
        # Go's duration units, which CEL's `duration()` accepts, in seconds.
        DURATION_UNITS = {
          "h" => 3600, "m" => 60, "s" => 1,
          "ms" => Rational(1, 1_000), "us" => Rational(1, 1_000_000), "µs" => Rational(1, 1_000_000),
          "ns" => Rational(1, 1_000_000_000)
        }.freeze
        DURATION_TERM = /(\d+(?:\.\d*)?|\.\d+)(h|ms|m|s|us|µs|ns)/
        DURATION = /\A[+-]?(?:#{DURATION_TERM})+\z/

        # `<` with its operands swapped is `>`.
        MIRRORED = {"eq" => "eq", "ne" => "ne", "lt" => "gt", "gt" => "lt", "le" => "ge", "ge" => "le"}.freeze

        private

        def duration(value)
          unless value.is_a?(::String) && DURATION.match?(value)
            raise UnsupportedOperatorError,
              "duration() is translated only over a literal such as \"86400s\", got #{describe(value)}"
          end

          seconds = value.scan(DURATION_TERM).sum { |amount, unit| amount.to_r * DURATION_UNITS.fetch(unit) }
          Values::Duration.new(seconds: value.start_with?("-") ? -seconds : seconds)
        end

        # `timeSince()` reads the clock. The plan leaves it unevaluated, so the translation's
        # clock is the query's: {#now}, read once per plan.
        def time_since(value)
          return Values::Duration.new(seconds: now.to_r - value.to_r) if value.is_a?(::Time)
          return Values::TimeSince.new(timestamp: value) if timestamp_operand?(value)

          raise UnsupportedOperatorError,
            "timeSince() needs timestamp() over a temporal column, got #{describe(value)}"
        end

        def temporal_value?(value)
          value.is_a?(Values::Duration) || value.is_a?(Values::TimeSince) ||
            value.is_a?(Values::ShiftedTimestamp)
        end

        # `+` or `-` with a duration operand.
        def duration_arithmetic(operator, left, right)
          unless %w[add sub].include?(operator)
            raise UnsupportedOperatorError, "#{operator} cannot take a duration: only + and - are translated"
          end

          sign = (operator == "add") ? 1 : -1
          left, right = right, left if operator == "add" && left.is_a?(Values::Duration) && !right.is_a?(Values::Duration)

          case [left, right]
          in [Values::Duration, Values::Duration]
            Values::Duration.new(seconds: left.seconds + sign * right.seconds)
          in [::Time, Values::Duration]
            ::Time.at(left.to_r + sign * right.seconds).utc
          in [Values::ShiftedTimestamp, Values::Duration]
            Values::ShiftedTimestamp.new(timestamp: left.timestamp, seconds: left.seconds + sign * right.seconds)
          in [_, Values::Duration] if timestamp_operand?(left)
            Values::ShiftedTimestamp.new(timestamp: left, seconds: sign * right.seconds)
          else
            raise UnsupportedOperatorError,
              "#{operator} of #{describe(left)} and #{describe(right)}: only an instant shifted " \
              "by a duration, or two durations, are translated"
          end
        end

        # A comparison with a duration, `timeSince()` or a shifted timestamp on one side.
        # `now - ts > d` becomes `ts < now - d`, and `ts + o < t` becomes `ts < t - o`.
        def compare_temporal(operator, left, right)
          case [left, right]
          in [Values::Duration, Values::Duration]
            fold_comparison(operator, left.seconds, right.seconds)
          in [Values::TimeSince, Values::Duration]
            compare(MIRRORED.fetch(operator), left.timestamp, shifted(now, -right.seconds))
          in [Values::Duration, Values::TimeSince]
            compare(operator, right.timestamp, shifted(now, -left.seconds))
          in [Values::ShiftedTimestamp, ::Time]
            compare(operator, left.timestamp, shifted(right, -left.seconds))
          in [::Time, Values::ShiftedTimestamp]
            compare(operator, shifted(left, -right.seconds), right.timestamp)
          else
            raise UnsupportedOperatorError,
              "#{operator} between #{describe(left)} and #{describe(right)}: a duration compares " \
              "only with a duration or timeSince(), and a shifted timestamp only with a timestamp " \
              "literal"
          end
        end

        def shifted(instant, seconds)
          ::Time.at(instant.to_r + seconds).utc
        end

        # The translation's clock, at the microsecond precision a bound Time keeps.
        def now
          @now ||= ::Time.now.utc.floor(Timestamps::MAX_SUBSECOND_DIGITS)
        end
      end
    end
  end
end
