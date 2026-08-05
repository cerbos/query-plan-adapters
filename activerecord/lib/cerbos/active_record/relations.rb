# frozen_string_literal: true

require "arel"
require_relative "arel_support"
require_relative "errors"

module Cerbos
  module ActiveRecord
    # Turns an association mapping into a *correlated subquery scope*: the aliased tables, the
    # joins between them, and the predicates tying the whole thing back to the row in the
    # enclosing query.
    #
    # Every scope gets fresh table aliases. That is not cosmetic — a policy may nest a macro
    # over the same association inside itself, and an unaliased inner subquery would silently
    # correlate against its own row instead of the outer one.
    module Relations
      # One association hop: the aliased table it introduces, and the predicates linking it to
      # whatever came before.
      Hop = Struct.new(:table, :predicates, :model)

      # Generates collision-free table aliases for one translation.
      class Aliaser
        def initialize
          @counter = 0
        end

        def next_alias(table_name)
          @counter += 1
          "cerbos_#{table_name}_#{@counter}"
        end
      end

      # A resolved collection, ready to be turned into EXISTS / COUNT subqueries.
      class Scope
        def initialize(hops:, model:, mapping: nil)
          @hops = hops
          @mapping = mapping
          @model = model
        end

        attr_reader :hops, :mapping, :model

        # The aliased table of the last hop — where member columns live.
        def table
          hops.last.table
        end

        # Predicates tying this scope to the enclosing query. They belong in the subquery's
        # WHERE clause, not in a join, so the subquery correlates rather than cross-joining.
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

        # +(SELECT <column> FROM ... WHERE <correlation>)+ as a scalar value, used for
        # dotted field paths through to-one associations. A scalar subquery cannot multiply
        # the outer result set the way a JOIN can.
        def scalar(column_name)
          Arel::Nodes::Grouping.new(select_manager(table[column_name], []).ast)
        end

        # +SELECT 1 FROM ... WHERE <correlation> [AND <conditions>]+, wrapped in EXISTS.
        def exists(*conditions)
          # `.ast`, not the manager: a SelectManager renders its own parentheses, and
          # `EXISTS ((SELECT ...))` is a parenthesised expression rather than a subquery.
          Arel::Nodes::Exists.new(select_manager(Arel.sql("1"), conditions).ast)
        end

        # +(SELECT COUNT(*) FROM ... WHERE <correlation> [AND <conditions>])+ as a scalar.
        def count(*conditions)
          Arel::Nodes::Grouping.new(select_manager(Arel.star.count, conditions).ast)
        end

        private

        def select_manager(projection, conditions)
          manager = Arel::SelectManager.new
          manager.from(hops.first.table)
          hops.drop(1).each do |hop|
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

      # @param owner_model [Class] the ActiveRecord model the association hangs off
      # @param owner_table [Arel::Table] that model's table in the enclosing query
      # @param mapping [AttributeMapping::Relation]
      # @param aliaser [Aliaser]
      # @return [Scope]
      def build(owner_model:, owner_table:, mapping:, aliaser:)
        reflection = owner_model.reflect_on_association(mapping.association)
        unless reflection
          raise UnsupportedAssociationError,
            "#{owner_model.name} has no association #{mapping.association.inspect}"
        end

        hops = hops_for(reflection, owner_table, owner_model, aliaser)
        Scope.new(hops: hops, mapping: mapping, model: hops.last.model)
      end

      # Resolve a chain of to-one associations for a dotted {AttributeMapping::Field} path.
      #
      # Collections are rejected here rather than silently reduced to one of their rows: a
      # scalar comparison against "some element" is not what the policy asked for.
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

      # @api private
      def hops_for(reflection, owner_table, owner_model, aliaser)
        if reflection.respond_to?(:through_reflection) && reflection.through_reflection
          through = hops_for(reflection.through_reflection, owner_table, owner_model, aliaser)
          source = hops_for(
            reflection.source_reflection, through.last.table, through.last.model, aliaser
          )
          return through + source
        end

        [direct_hop(reflection, owner_table, owner_model, aliaser)]
      end

      # @api private
      def direct_hop(reflection, owner_table, owner_model, aliaser)
        if reflection.respond_to?(:polymorphic?) && reflection.polymorphic?
          raise UnsupportedAssociationError,
            "Association #{reflection.name.inspect} on #{owner_model.name} is a polymorphic " \
            "belongs_to, so its target table is not known until a row is read; map the " \
            "attribute onto a concrete association instead"
        end

        if reflection.scope
          raise UnsupportedAssociationError,
            "Association #{reflection.name.inspect} on #{owner_model.name} carries a scope, " \
            "whose conditions this adapter cannot re-bind onto the correlated alias it " \
            "generates; map the attribute onto an unscoped association instead"
        end

        target = reflection.klass
        table = target.arel_table.alias(aliaser.next_alias(target.table_name))
        predicates = []

        case reflection.macro
        when :has_many, :has_one
          predicates << table[reflection.foreign_key].eq(
            owner_table[reflection.active_record_primary_key]
          )
          # `as:` associations discriminate on a type column; without it the subquery would
          # match rows belonging to a different owner class.
          if reflection.type
            predicates << table[reflection.type].eq(owner_model.polymorphic_name)
          end
        when :belongs_to
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
