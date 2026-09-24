# frozen_string_literal: true

module Cerbos
  module Sequel
    class Translator
      # +hierarchy()+ and the operators over it: +ancestorOf+, +descendentOf+ and +overlaps+.
      module Hierarchies
        private

        def hierarchy(value, delimiter)
          delimiter ||= "."

          unless delimiter.is_a?(::String) && !delimiter.empty?
            raise InvalidPlanError, "hierarchy() delimiter must be a non-empty string"
          end

          if value.is_a?(Array)
            return Values::Hierarchy.new(value: nil, segments: value, delimiter: delimiter)
          end

          # CEL has hierarchy() for a string and for a list of strings, and for nothing else: a
          # number is a no-overload error, and Cerbos denies the row under both polarities. The
          # descendant test below is a LIKE, and a LIKE over an integer column is an error on
          # PostgreSQL and a text coercion on SQLite and MySQL, so the adapter refuses the
          # operand here rather than leave the answer to the store.
          require_string_operand("hierarchy", value)

          Values::Hierarchy.new(value: value, segments: nil, delimiter: delimiter)
        end

        def assert_hierarchies(left, right)
          unless left.is_a?(Values::Hierarchy) && right.is_a?(Values::Hierarchy)
            raise UnsupportedOperatorError, "Hierarchy operators need hierarchy() operands"
          end
          # A hierarchy from a list is already in segments. Thus its delimiter has no meaning,
          # and a comparison with a path that has a different delimiter is correct.
          if left.segments.nil? && right.segments.nil? && left.delimiter != right.delimiter
            raise UnsupportedOperatorError,
              "Hierarchy operands use different delimiters: " \
              "#{left.delimiter.inspect} and #{right.delimiter.inspect}"
          end
        end

        # A hierarchy from a list is compared segment by segment, and so is the other side.
        def segment_wise?(left, right)
          !left.segments.nil? || !right.segments.nil?
        end

        # Gives the segments of a hierarchy. It must be possible to know them during the
        # translation: a column is a string with a delimiter until the query runs, and SQL cannot
        # divide it into segments.
        def require_segments(hierarchy)
          return hierarchy.segments if hierarchy.segments
          return hierarchy.value.split(hierarchy.delimiter, -1) if hierarchy.value.is_a?(::String)

          raise UnsupportedOperatorError,
            "A hierarchy built from a list can only be compared against another hierarchy " \
            "whose segments are known when the query is built; this one is a column"
        end

        # Each segment of +above+ equals the segment in the same position in +below+.
        def segments_equal(above, below)
          SqlSupport.and_node(
            above.each_with_index.map { |segment, index| as_predicate(compare("eq", segment, below[index])) }
          )
        end

        def hierarchy_equal(left, right)
          return compare("eq", left.value, right.value) unless segment_wise?(left, right)

          above = require_segments(left)
          below = require_segments(right)
          return false if above.length != below.length

          segments_equal(above, below)
        end

        def ancestor_of(ancestor, descendent)
          assert_hierarchies(ancestor, descendent)

          if segment_wise?(ancestor, descendent)
            above = require_segments(ancestor)
            below = require_segments(descendent)
            # An ancestor is a shorter path, and each of its segments agrees with the segment in
            # the same position in the other path.
            return false if above.length >= below.length

            return segments_equal(above, below)
          end

          delimiter = ancestor.delimiter
          above = ancestor.value
          below = descendent.value

          if above.is_a?(::String) && below.is_a?(::String)
            return below.start_with?(above + delimiter)
          end

          if below.is_a?(::String)
            # A descendant that is a constant has a known and limited set of ancestors. The
            # adapter compares them exactly. A LIKE operation would need an escape character for
            # its metacharacters.
            parts = below.split(delimiter, -1)
            prefixes = (1...parts.length).map { |i| parts[0, i].join(delimiter) }
            return scalar_membership(above, prefixes)
          end

          if above.is_a?(::String)
            return matcher.match(below, above + delimiter, prefix: false, suffix: true)
          end

          raise UnsupportedOperatorError,
            "Hierarchy comparison between two columns is not supported: the descendant test " \
            "needs a literal prefix to match against"
        end

        def overlaps(left, right)
          assert_hierarchies(left, right)

          result = SqlSupport.or_node([
            as_predicate(hierarchy_equal(left, right)),
            as_predicate(ancestor_of(left, right)),
            as_predicate(ancestor_of(right, left))
          ])
          # Every list segment is evaluated, even when a constant prefix decides overlap.
          segments = [left, right].flat_map { |path| path.segments || [path.value] }
          missing = segments.select { |segment| SqlSupport.sql_node?(segment) }
            .map { |segment| SqlSupport.is_null(segment) }

          unknown_if_any(missing, result)
        end
      end
    end
  end
end
