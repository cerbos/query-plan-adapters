# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    class Translator
      # `hierarchy()` and the operators over it: `ancestorOf`, `descendentOf` and `overlaps`.
      #
      # @private
      module Hierarchies
        private

        def hierarchy(value, delimiter)
          delimiter ||= "."

          # An empty delimiter is valid: it splits one segment per character.
          raise InvalidPlanError, "hierarchy() delimiter must be a string" unless delimiter.is_a?(::String)

          if value.is_a?(Array)
            return Values::Hierarchy.new(value: nil, segments: value, delimiter: delimiter)
          end

          Values::Hierarchy.new(value: value, segments: nil, delimiter: delimiter)
        end

        def assert_hierarchies(left, right)
          unless left.is_a?(Values::Hierarchy) && right.is_a?(Values::Hierarchy)
            raise UnsupportedOperatorError, "Hierarchy operators need hierarchy() operands"
          end
          # A list-built hierarchy is already split, so its delimiter does not matter.
          if left.segments.nil? && right.segments.nil? && left.delimiter != right.delimiter
            raise UnsupportedOperatorError,
              "Hierarchy operands use different delimiters: " \
              "#{left.delimiter.inspect} and #{right.delimiter.inspect}"
          end
        end

        # `hierarchy()` of a number or boolean column is a no-such-overload error in CEL, which
        # denies the row under either polarity, so the operator is UNKNOWN (nil). Rendered, it
        # would put a LIKE on a number: PostgreSQL refuses it, SQLite and MySQL coerce it.
        def hierarchy_type_error?(*hierarchies)
          hierarchies.flat_map { |path| path.segments || [path.value] }.any? do |part|
            ArelSupport.arel_node?(part) && !scalar_kind(part).nil? && scalar_kind(part) != :string
          end
        end

        # If either side is list-built, compare both segment by segment.
        def segment_wise?(left, right)
          !left.segments.nil? || !right.segments.nil?
        end

        # Splits a path as Go's `strings.Split` does: the empty delimiter gives one segment
        # per character, and no segment at all for the empty path.
        def split_path(path, delimiter)
          delimiter.empty? ? path.chars : path.split(delimiter, -1)
        end

        # The segments of a hierarchy, known at translation time. SQL cannot split a column.
        def require_segments(hierarchy)
          return hierarchy.segments if hierarchy.segments
          return split_path(hierarchy.value, hierarchy.delimiter) if hierarchy.value.is_a?(::String)

          raise UnsupportedOperatorError,
            "A hierarchy built from a list can only be compared against another hierarchy " \
            "whose segments are known when the query is built; this one is a column"
        end

        # Each segment of `above` equals the segment in the same position in `below`.
        def segments_equal(above, below)
          ArelSupport.and_node(
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
          return nil if hierarchy_type_error?(ancestor, descendent)

          if segment_wise?(ancestor, descendent)
            above = require_segments(ancestor)
            below = require_segments(descendent)
            # An ancestor is a shorter path whose segments all match.
            return false if above.length >= below.length

            return segments_equal(above, below)
          end

          delimiter = ancestor.delimiter
          above = ancestor.value
          below = descendent.value

          if above.is_a?(::String) && below.is_a?(::String)
            return below.start_with?(above + delimiter) && below.length > above.length
          end

          if below.is_a?(::String)
            # A constant descendant has a fixed set of ancestors: match them exactly rather
            # than use LIKE, which would need escaping. Under the empty delimiter the empty
            # path has no segment, so it is an ancestor too.
            parts = split_path(below, delimiter)
            first = delimiter.empty? ? 0 : 1
            prefixes = (first...parts.length).map { |i| parts[0, i].join(delimiter) }
            return scalar_membership(above, prefixes)
          end

          if above.is_a?(::String)
            descendant = matcher.match(below, above + delimiter, prefix: false, suffix: true)
            return descendant unless delimiter.empty?

            # Without a delimiter the prefix LIKE also matches the path itself.
            return ArelSupport.and_node([descendant, as_predicate(compare("ne", below, above))])
          end

          raise UnsupportedOperatorError,
            "Hierarchy comparison between two columns is not supported: the descendant test " \
            "needs a literal prefix to match against"
        end

        def overlaps(left, right)
          assert_hierarchies(left, right)
          return nil if hierarchy_type_error?(left, right)

          result = ArelSupport.or_node([
            as_predicate(hierarchy_equal(left, right)),
            as_predicate(ancestor_of(left, right)),
            as_predicate(ancestor_of(right, left))
          ])
          # Every list segment is evaluated, even when a constant prefix decides overlap.
          segments = [left, right].flat_map { |path| path.segments || [path.value] }
          missing = segments.select { |segment| ArelSupport.arel_node?(segment) }
            .map { |segment| ArelSupport.is_null(segment) }

          unknown_if_any(missing, result)
        end
      end
    end
  end
end
