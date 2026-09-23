# frozen_string_literal: true

require "arel"
require_relative "arel_support"
require_relative "errors"

module Cerbos
  module ActiveRecord
    # Turns an association mapping into a correlated subquery scope: aliased tables, their
    # joins, and the predicates tying the subquery to the outer row.
    #
    # Every scope gets fresh aliases. A macro nested inside another on the same association
    # would otherwise correlate to its own row instead of the outer one.
    #
    # @private
    module Relations
      # One association hop: the aliased table and the predicates joining it to the previous one.
      #
      # @private
      Hop = Struct.new(:table, :predicates, :model)

      # Makes unique table aliases within one translation.
      #
      # @private
      class Aliaser
        def initialize
          @counter = 0
        end

        def next_alias(table_name)
          @counter += 1
          "cerbos_#{table_name}_#{@counter}"
        end
      end

      # A resolved collection, renderable as an EXISTS or COUNT subquery.
      #
      # @private
      class Scope
        def initialize(hops:, model:, mapping: nil, guard_hops: [])
          @hops = hops
          @mapping = mapping
          @model = model
          @guard_hops = guard_hops
        end

        attr_reader :hops, :mapping, :model, :guard_hops

        # The aliased table of the last hop, which holds the member columns.
        def table
          hops.last.table
        end

        # Predicates tying this scope to the outer query. They go in the subquery's WHERE, not a
        # join, so it correlates instead of cross-joining.
        def correlation
          hops.first.predicates
        end

        def member_column
          field = mapping&.member_field
          if field.nil?
            raise UnmappedAttributeError,
              "Relation #{mapping&.association.inspect} is used as a collection of bare values " \
              "but declares no member_field"
          end
          table[field]
        end

        # Makes `SELECT 1 FROM ... WHERE <correlation> [AND <conditions>]` in an EXISTS node.
        def exists(*conditions)
          # Pass the AST, not the manager: a SelectManager adds its own parentheses, and
          # `EXISTS ((SELECT ...))` is not a subquery.
          Arel::Nodes::Exists.new(select_manager(hops, Arel.sql("1"), conditions).ast)
        end

        # Makes `(SELECT COUNT(*) FROM ... WHERE <correlation> [AND <conditions>])` as a
        # scalar value.
        def count(*conditions)
          Arel::Nodes::Grouping.new(select_manager(hops, Arel.star.count, conditions).ast)
        end

        # Makes `(SELECT <column> FROM ... WHERE <correlation>)` as a scalar value, for dotted
        # to-one field paths. Unlike a JOIN, it cannot add rows.
        def scalar(column_name)
          Arel::Nodes::Grouping.new(select_manager(hops, table[column_name], []).ast)
        end

        # Makes `CASE WHEN EXISTS (<the hops before the collection>) THEN <expression> END`.
        #
        # Every hop before the collection is a to-one parent. If the parent is missing, CEL
        # errors and denies, but a plain subquery sees "no children": `all` is TRUE, `!exists`
        # is TRUE and the count is 0, all returning denied rows (cerbos/query-plan-adapters#309).
        #
        # No ELSE, so a missing parent gives NULL, which stays excluded under NOT too. Every
        # operator reading a chain must use this, not just the macros: a plain EXISTS is FALSE
        # for a missing parent, so its negation is TRUE (#315, #316).
        #
        # `guard_hops` is empty for a directly mapped relation, where an empty collection is
        # real: `!tags.exists(...)` over zero tags is TRUE.
        def guarded(expression)
          return expression if guard_hops.empty?

          ArelSupport.case_node([[
            Arel::Nodes::Exists.new(select_manager(guard_hops, Arel.sql("1"), []).ast),
            expression
          ]])
        end

        private

        def select_manager(hop_list, projection, conditions)
          manager = Arel::SelectManager.new
          manager.from(hop_list.first.table)
          hop_list.drop(1).each do |hop|
            manager.join(hop.table).on(ArelSupport.and_node(hop.predicates))
          end
          manager.project(projection)
          (correlation + conditions.compact).each do |condition|
            manager.where(ArelSupport.to_predicate(condition))
          end
          manager
        end
      end

      module_function

      # @param owner_model [Class] the ActiveRecord model that has the association
      # @param owner_table [Arel::Table] the table of that model in the query around it
      # @param mapping [AttributeMapping::Relation]
      # @param aliaser [Aliaser]
      # @return [Scope]
      def build(owner_model:, owner_table:, mapping:, aliaser:)
        reflection = owner_model.reflect_on_association(mapping.association)
        unless reflection
          raise UnsupportedAssociationError,
            "#{owner_model.name} has no association #{mapping.association.inspect}"
        end

        # Require a collection. The database does not enforce `has_one`, so Cerbos could see one
        # element while a subquery sees every row with that foreign key.
        unless reflection.collection?
          raise UnsupportedAssociationError,
            "Association #{mapping.association.inspect} on #{owner_model.name} is a " \
            "#{reflection.macro}, not a collection. Map a to-one association as a field path " \
            "with dots, for example Cerbos::ActiveRecord.field(\"profile.name\")."
        end

        hops = hops_for(reflection, owner_table, owner_model, aliaser)
        Scope.new(hops: hops, mapping: mapping, model: hops.last.model)
      end

      # Resolves a relation reached through another, e.g. `R.attr.mainCategory.subCategories`,
      # written as a nested `fields:` mapping. Each step adds hops to the same subquery.
      #
      # Prefer nesting over one `has_many :through`: the joins are the same, but only nesting
      # says which hops are the parent and which is the collection. A `through:` may be a
      # plain join table, where empty really is empty. See {Scope#guarded}.
      #
      # @param outer_scope [Scope] the relation that holds the nested mapping
      # @return [Scope] the full chain, which requires the hops of `outer_scope` to exist
      def chain(outer_scope:, mapping:, aliaser:)
        inner = build(
          owner_model: outer_scope.model,
          owner_table: outer_scope.table,
          mapping: mapping,
          aliaser: aliaser
        )

        Scope.new(
          hops: outer_scope.hops + inner.hops,
          mapping: mapping,
          model: inner.model,
          guard_hops: outer_scope.hops
        )
      end

      # Resolves the to-one chain of a dotted {AttributeMapping::Field} path.
      #
      # Collections are refused: the adapter will not pick one element for a scalar comparison.
      #
      # @return [Scope]
      def build_path(owner_model:, owner_table:, association_names:, aliaser:)
        hops = []
        model = owner_model
        table = owner_table

        association_names.each do |name|
          reflection = model.reflect_on_association(name.to_sym)
          unless reflection
            raise UnmappedAttributeError, "#{model.name} has no association #{name.inspect}"
          end
          if reflection.collection?
            raise UnsupportedAssociationError,
              "Field path segment #{name.inspect} on #{model.name} is a collection " \
              "association; a scalar attribute cannot be read from it — map the attribute " \
              "with Cerbos::ActiveRecord.relation instead"
          end

          hop = hops_for(reflection, table, model, aliaser)
          hops.concat(hop)
          table = hops.last.table
          model = hops.last.model
        end

        Scope.new(hops: hops, model: model)
      end

      # @private
      def hops_for(reflection, owner_table, owner_model, aliaser)
        assert_no_scope(reflection, owner_model)

        if reflection.respond_to?(:through_reflection) && reflection.through_reflection
          through = hops_for(reflection.through_reflection, owner_table, owner_model, aliaser)
          source = hops_for(
            reflection.source_reflection, through.last.table, through.last.model, aliaser
          )
          return through + source
        end

        [direct_hop(reflection, owner_table, owner_model, aliaser)]
      end

      # An association scope hides rows from Cerbos, and the adapter cannot re-apply it to its
      # alias, so the filter would disagree with the decision.
      #
      # Checked here, not in `direct_hop`, because `hops_for` splits a `through` association
      # into parts first and would lose its own scope.
      #
      # @private
      def assert_no_scope(reflection, owner_model)
        return unless reflection.scope

        raise UnsupportedAssociationError,
          "Association #{reflection.name.inspect} on #{owner_model.name} carries a scope, " \
          "whose conditions this adapter cannot re-bind onto the correlated alias it " \
          "generates; map the attribute onto an unscoped association instead"
      end

      # A composite key comes back as an array, which `table[...]` would quote as one bogus
      # column name. Refuse it here with a clear message instead.
      #
      # @private
      def assert_single_key(reflection, owner_model, *keys)
        return if keys.none?(Array)

        raise UnsupportedAssociationError,
          "Association #{reflection.name.inspect} on #{owner_model.name} joins on more than " \
          "one column. This adapter builds one equality for a correlated subquery and cannot " \
          "express a composite key. Give an operator override for this attribute."
      end

      # Refuses targets a plain table alias cannot select correctly: polymorphic, STI subclass,
      # or default-scoped models.
      #
      # @private
      # @return [Class] the target model
      def assert_plain_target(reflection, owner_model)
        if reflection.respond_to?(:polymorphic?) && reflection.polymorphic?
          raise UnsupportedAssociationError,
            "Association #{reflection.name.inspect} on #{owner_model.name} is a polymorphic " \
            "belongs_to, so its target table is not known until a row is read; map the " \
            "attribute onto a concrete association instead"
        end

        target = reflection.klass

        # An STI subclass association also filters on the type column; without it the subquery
        # would see sibling or base-class rows Cerbos never sees. The adapter does not add that
        # condition because the subclass list depends on what Ruby has loaded.
        if target.respond_to?(:finder_needs_type_condition?) && target.finder_needs_type_condition?
          raise UnsupportedAssociationError,
            "#{target.name} is a subclass in a single-table hierarchy. Its association also " \
            "filters on the inheritance column, and this adapter does not add that condition, " \
            "because the set of subclasses depends on which of them are loaded. Map the " \
            "attribute onto an association that points at the base class, or give an operator " \
            "override."
        end

        # A default scope hides rows from Cerbos, but the subquery reads the raw table.
        if target.respond_to?(:default_scopes) && target.default_scopes.any?
          raise UnsupportedAssociationError,
            "#{target.name} has a default scope, whose conditions this adapter cannot put " \
            "onto the correlated alias that it makes. The rows that the scope removes are " \
            "absent from the attributes that Cerbos evaluates, so the filter would not agree " \
            "with the decision. Use unscoped models for the attributes in a policy."
        end

        target
      end

      # @private
      def direct_hop(reflection, owner_table, owner_model, aliaser)
        target = assert_plain_target(reflection, owner_model)

        table = target.arel_table.alias(aliaser.next_alias(target.table_name))
        predicates = []

        case reflection.macro
        when :has_many, :has_one
          assert_single_key(reflection, owner_model,
            reflection.foreign_key, reflection.active_record_primary_key)
          predicates << table[reflection.foreign_key].eq(
            owner_table[reflection.active_record_primary_key]
          )
          # An `as:` association also needs its type column, or it matches other owner classes.
          if reflection.type
            predicates << table[reflection.type].eq(owner_model.polymorphic_name)
          end
        when :belongs_to
          assert_single_key(reflection, owner_model,
            reflection.association_primary_key, reflection.foreign_key)
          predicates << table[reflection.association_primary_key].eq(
            owner_table[reflection.foreign_key]
          )
        else
          raise UnsupportedAssociationError,
            "Association #{reflection.name.inspect} on #{owner_model.name} has macro " \
            "#{reflection.macro.inspect}, which this adapter cannot express as a correlated " \
            "subquery"
        end

        Hop.new(table, predicates, target)
      end
    end
  end
end
