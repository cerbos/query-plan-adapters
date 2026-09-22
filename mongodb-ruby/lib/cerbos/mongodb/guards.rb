# frozen_string_literal: true

require_relative "aggregation"
require_relative "operands"

module Cerbos
  module MongoDB
    # The conjuncts that keep out documents on which CEL raises. CEL denies on an error under
    # BOTH polarities, while a MongoDB filter has no UNKNOWN: a negation over a predicate that
    # could not be evaluated would match. So every guard is ANDed OUTSIDE the predicate — and
    # outside any +$nor+ a negation wraps it in.
    module Guards
      extend Operands

      module_function

      # ["a", "b"], v → {"a" => {"b" => v}}; an empty path is v itself.
      def field_filter(path, value)
        path.reverse.reduce(value) { |acc, key| {key => acc} }
      end

      # A single-field comparison, with the field required to exist (+require_exists+) and/or to
      # be non-null (+nullable+: a stored null is a missing Cerbos attribute) ANDed before it.
      def guarded_field_filter(path, value, nullable, require_exists = false)
        filter = field_filter(path, value)
        return filter unless nullable || require_exists

        guards = []
        guards << field_filter(path, {"$exists" => true}) if require_exists
        guards << field_filter(path, {"$ne" => nil}) if nullable
        {"$and" => guards + [filter]}
      end

      # ANDs {field: {$ne: null}} before +filter+ for every nullable field the operands read.
      def with_nullable(filter, operands, mapper)
        names = operands.flat_map { |operand| variable_names(operand) }.uniq.select { |name| mapper.nullable?(name) }
        return filter if names.empty?

        {"$and" => names.map { |name| field_filter(mapper.resolve_field(name).path, {"$ne" => nil}) } + [filter]}
      end

      # "Every to-one parent this operand dots through is present", or nil when it dots through
      # none.
      #
      # CEL cannot dot through a list, so every intermediate segment of `a.b.c` is a to-ONE
      # parent: absent, the application sends no attribute and CEL raises a missing-path error,
      # which denies. The flattened path a MongoDB filter matches cannot see that — an absent
      # parent and a childless parent both fail the match — so a +$nor+ over it is TRUE for a
      # parentless document. Requiring the parent OUTSIDE the +$nor+ is faithful under both
      # polarities (cerbos/query-plan-adapters#315, #316).
      #
      # `<parent>.0` exists exactly when the parent array is non-empty, which is how the document
      # model spells "the to-one parent was serialised" for a +requires_parent+ declaration. A
      # +type: :one+ relation needs no declaration: a to-one hop is by definition a level that
      # can be absent, and {path: {$ne: null}} excludes both the absent and the stored-null case
      # (cerbos/query-plan-adapters#375).
      def required_parents(operand, mapper)
        array_parents = []
        to_one_paths = []
        variable_names(operand).each do |name|
          relation = mapper.resolve_field(name).relation
          next if relation.nil?

          array_parents << relation.requires_parent if relation.requires_parent
          to_one_paths << relation.name if relation.type == :one
        end
        clauses = array_parents.uniq.map { |parent| {"#{parent}.0" => {"$exists" => true}} } +
          to_one_paths.uniq.map { |path| {path => {"$ne" => nil}} }
        return nil if clauses.empty?

        (clauses.length == 1) ? clauses.first : {"$and" => clauses}
      end

      # Keeps out every document on which an operand of +filter+ could not be evaluated: a
      # nullable field that is null, a guarded aggregation expression, and an absent to-one
      # parent. Each is ANDed OUTSIDE +filter+.
      def with_evaluation(filter, operands, mapper)
        guarded = with_nullable(filter, operands, mapper)
        conjuncts = operands.flat_map { |operand| Aggregation.evaluation_guards(operand, mapper) } +
          operands.filter_map { |operand| required_parents(operand, mapper) }
        conjuncts.empty? ? guarded : {"$and" => conjuncts + [guarded]}
      end
    end
  end
end
