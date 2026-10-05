# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `contains`, `startsWith` and `endsWith`, and the checks for a string operand. See
      # {StringMatcher} for the LIKE escaping.
      #
      # @private
      module Strings
        private

        def string_match(operator, receiver, needle)
          reject_collection(operator, receiver)
          reject_collection(operator, needle)
          return cel_type_error if known_non_string?(receiver) || known_non_string?(needle)

          require_string_operand(operator, receiver)
          require_string_operand(operator, needle)
          matcher.match(receiver, needle, **STRING_MATCHES.fetch(operator))
        end

        # CEL's `upperAscii()` folds only `a`-`z`. SQL `UPPER` follows the database's locale
        # and folds `é` to `É` too, so each ASCII letter is replaced on its own; `REPLACE`
        # matches exactly, whatever the column's collation.
        def upper_ascii(value)
          require_string_operand("upperAscii", value)
          return value.tr("a-z", "A-Z") if value.is_a?(::String)

          folded = ("a".."z").reduce(value) { |text, letter| ArelSupport.function("REPLACE", [text, letter, letter.upcase]) }
          record_cel_type(folded, :string)
        end

        # `receiver.matches(pattern)`, lowered through {Regex.compile} into the exact string
        # predicates this module already writes, never into the store's own regex dialect, none of
        # which is RE2. The receiver must be a string column: a NULL there is a missing attribute
        # or a null value, and CEL has no `matches()` for either, so the whole predicate is NULL
        # for it, even where the pattern matches every string.
        def regex_match(receiver, pattern)
          reject_collection("matches", receiver)
          return cel_type_error if known_non_string?(receiver)
          unless pattern.is_a?(::String)
            raise UnsupportedOperatorError,
              "matches is translated only with a constant pattern, got #{describe(pattern)}"
          end
          unless ArelSupport.arel_node?(receiver) && STRING_COLUMN_TYPES.include?(column_type(receiver))
            raise UnsupportedOperatorError,
              "matches is translated only over a string column, got #{describe(receiver)}"
          end

          plans = Regex.compile(pattern)
          return cel_type_error if plans == :error

          matched = ArelSupport.or_node(plans.map { |plan| regex_condition(receiver, plan) })
          ArelSupport.case_node([[ArelSupport.is_null(receiver), nil]], else_value: matched)
        end

        def regex_condition(receiver, plan)
          case plan
          when Regex::Literals
            if plan.kind == :equals
              return ArelSupport.or_node(plan.literals.map { |literal| ArelSupport.comparison("eq", receiver, literal) })
            end

            affixes = {starts_with: [false, true], ends_with: [true, false], contains: [true, true]}.fetch(plan.kind)
            ArelSupport.or_node(plan.literals.map { |literal|
              matcher.match(receiver, literal, prefix: affixes[0], suffix: affixes[1])
            })
          when Regex::AllCharactersIn
            # Remove every allowed character; nothing may be left. REPLACE is literal and
            # case-sensitive on every store, as the LIKE escaping already relies on. The residue is
            # compared with '' rather than measured: SQLite's LENGTH stops at a NUL.
            rest = plan.characters.reduce(receiver) { |current, character|
              ArelSupport.function("REPLACE", [current, character, ""])
            }
            ArelSupport.and_node([ArelSupport.comparison("eq", rest, ""), at_least(receiver, plan.min)].compact)
          when Regex::NoNewline
            ArelSupport.and_node([no_newline(receiver), at_least(receiver, plan.min)].compact)
          when Regex::PrefixSuffix
            pairs = plan.pairs.map { |prefix, suffix|
              ArelSupport.and_node([
                matcher.match(receiver, prefix, prefix: false, suffix: true),
                matcher.match(receiver, suffix, prefix: true, suffix: false),
                at_least(receiver, prefix.length + plan.min + suffix.length)
              ].compact)
            }
            ArelSupport.and_node([ArelSupport.or_node(pairs), no_newline(receiver)])
          end
        end

        # At least `count` characters, or nil when any string has that many.
        def at_least(receiver, count)
          ArelSupport.comparison("ge", dialect.char_length(receiver), count) if count.positive?
        end

        def no_newline(receiver)
          ArelSupport.not_node(matcher.match(receiver, "\n", prefix: true, suffix: true))
        end

        # A value CEL certainly holds as a number or a boolean: a constant, a numeric or boolean
        # column, or a computed number or boolean. `contains`, `startsWith`, `endsWith` and
        # `size()` have no overload for either, so CEL raises a no-such-overload error on every
        # row, decided by the declared type and not by the row's value. A temporal column is not
        # one of these: its attribute is an RFC-3339 string in CEL, so it stays refused below.
        def known_non_string?(value)
          %i[number boolean].include?(scalar_kind(value))
        end

        # The value of an expression that is a CEL error on every row: SQL NULL, in a fresh node
        # each time so nothing recorded against one use leaks to another. A grouping, not a bare
        # SqlLiteral, which is a String and would read as a CEL string. UNKNOWN denies under both
        # polarities, as the error does: `NOT NULL` is NULL, `NULL OR TRUE` is TRUE as
        # `error || true` is, and `NULL AND FALSE` is FALSE as `error && false` is. A strict
        # operator over it is the error again ({#apply}), never `NULL IS NULL`.
        def cel_type_error
          node = Arel::Nodes::Grouping.new(ArelSupport::NULL_SQL)
          @cel_errors[node] = true
          node
        end

        def cel_error?(value)
          ArelSupport.arel_node?(value) && @cel_errors.key?(value)
        end

        def require_string_operand(operator, value)
          type = column_type(value)
          return if value.is_a?(::String) || (ArelSupport.arel_node?(value) && (type.nil? || STRING_COLUMN_TYPES.include?(type)))

          raise UnsupportedOperatorError,
            "#{operator} requires a string operand, got #{type || describe(value)}"
        end

        def string_valued?(value)
          value.is_a?(::String) || STRING_COLUMN_TYPES.include?(column_type(value))
        end
      end
    end
  end
end
