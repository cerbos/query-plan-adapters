# frozen_string_literal: true

require "arel"
require_relative "arel_support"
require_relative "errors"

module Cerbos
  module ActiveRecord
    # Changes an association mapping into a correlated subquery scope. The scope holds the
    # tables with their aliases, the joins between those tables, and the predicates that
    # connect the subquery to the row in the query around it.
    #
    # Each scope gets new table aliases. This is necessary and not only a preference. A policy
    # can put a macro on an association inside another macro on the same association. If the
    # inner subquery had no alias, it would connect to its own row and not to the outer row.
    module Relations
      # One association hop. It has the table with its alias, and the predicates that connect
      # that table to the tables before it.
      Hop = Struct.new(:table, :predicates, :model)

      # Makes table aliases that are all different, for one translation.
      class Aliaser
        def initialize
          @counter = 0
        end

        def next_alias(table_name)
          @counter += 1
          "cerbos_#{table_name}_#{@counter}"
        end
      end

      # A collection after the adapter resolves it. The adapter can change it into an EXISTS
      # subquery or a COUNT subquery.
      class Scope
        def initialize(hops:, model:, mapping: nil)
          @hops = hops
          @mapping = mapping
          @model = model
        end

        attr_reader :hops, :mapping, :model

        # The table with its alias for the last hop. The member columns are on this table.
        def table
          hops.last.table
        end

        # The predicates that connect this scope to the query around it. They belong in the
        # WHERE clause of the subquery and not in a join. Thus the subquery correlates and it
        # does not make a cross join.
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

        # Makes +SELECT 1 FROM ... WHERE <correlation> [AND <conditions>]+ in an EXISTS node.
        def exists(*conditions)
          # The adapter gives the AST to the EXISTS node and not the manager. A SelectManager
          # makes its own parentheses, and +EXISTS ((SELECT ...))+ is an expression in
          # parentheses and not a subquery.
          Arel::Nodes::Exists.new(select_manager(Arel.sql("1"), conditions).ast)
        end

        # Makes +(SELECT COUNT(*) FROM ... WHERE <correlation> [AND <conditions>])+ as a
        # scalar value.
        def count(*conditions)
          Arel::Nodes::Grouping.new(select_manager(Arel.star.count, conditions).ast)
        end

        # Makes +(SELECT <column> FROM ... WHERE <correlation>)+ as a scalar value. A field
        # path with dots through to-one associations uses this. A scalar subquery cannot
        # increase the number of rows in the result. A JOIN can do that.
        def scalar(column_name)
          Arel::Nodes::Grouping.new(select_manager(table[column_name], []).ast)
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

        # A collection mapping needs a collection. ActiveRecord does not make the database
        # enforce that a `has_one` has only one row. Thus the association gives one row and
        # Cerbos sees one element, while a subquery would examine every row with that foreign
        # key, and the two answers differ.
        unless reflection.collection?
          raise UnsupportedAssociationError,
            "Association #{mapping.association.inspect} on #{owner_model.name} is a " \
            "#{reflection.macro}, not a collection. Map a to-one association as a field path " \
            "with dots, for example Cerbos::ActiveRecord.field(\"profile.name\")."
        end

        hops = hops_for(reflection, owner_table, owner_model, aliaser)
        Scope.new(hops: hops, mapping: mapping, model: hops.last.model)
      end

      # Resolves a chain of to-one associations for an {AttributeMapping::Field} path with
      # dots.
      #
      # The adapter refuses a collection here. It does not select one row of the collection by
      # itself. A scalar comparison with "one of the elements" is not the request of the
      # policy.
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

      # A scope on an association removes rows from it, and thus from the attributes that
      # Cerbos sees. This adapter cannot put those conditions onto the alias that it makes for
      # the correlated subquery, so the filter would select rows that the decision did not.
      #
      # The check is here and not in +direct_hop+ because a `through` association carries its
      # own scope, and +hops_for+ opens such an association into its parts before it reaches
      # +direct_hop+. The scope of the outer association would then be lost.
      #
      # @api private
      def assert_no_scope(reflection, owner_model)
        return unless reflection.scope

        raise UnsupportedAssociationError,
          "Association #{reflection.name.inspect} on #{owner_model.name} carries a scope, " \
          "whose conditions this adapter cannot re-bind onto the correlated alias it " \
          "generates; map the attribute onto an unscoped association instead"
      end

      # ActiveRecord gives an array for a key that has more than one column. Such an array
      # would reach +table[...]+ and become one quoted name, and the query would then fail with
      # "no such column". The adapter refuses the association here instead, with a message that
      # says why.
      #
      # @api private
      def assert_single_key(reflection, owner_model, *keys)
        return if keys.none?(Array)

        raise UnsupportedAssociationError,
          "Association #{reflection.name.inspect} on #{owner_model.name} joins on more than " \
          "one column. This adapter builds one equality for a correlated subquery and cannot " \
          "express a composite key. Give an operator override for this attribute."
      end

      # @api private
      def direct_hop(reflection, owner_table, owner_model, aliaser)
        if reflection.respond_to?(:polymorphic?) && reflection.polymorphic?
          raise UnsupportedAssociationError,
            "Association #{reflection.name.inspect} on #{owner_model.name} is a polymorphic " \
            "belongs_to, so its target table is not known until a row is read; map the " \
            "attribute onto a concrete association instead"
        end

        target = reflection.klass

        # An association that points at a subclass in a single-table hierarchy also filters on
        # the inheritance column. Without that condition the subquery would find the rows of a
        # sibling class or of the base class, which are absent from the association and thus
        # from the attributes that Cerbos evaluates.
        #
        # The adapter does not add the condition itself. The set of subclasses depends on which
        # of them Ruby has loaded, so the condition could be short and the filter would then
        # disagree in the other direction.
        if target.respond_to?(:finder_needs_type_condition?) && target.finder_needs_type_condition?
          raise UnsupportedAssociationError,
            "#{target.name} is a subclass in a single-table hierarchy. Its association also " \
            "filters on the inheritance column, and this adapter does not add that condition, " \
            "because the set of subclasses depends on which of them are loaded. Map the " \
            "attribute onto an association that points at the base class, or give an operator " \
            "override."
        end

        # A default scope on the model removes rows from the association, and thus from the
        # attributes that Cerbos sees. The subquery below reads the table and does not apply
        # that scope, so the two answers would differ.
        if target.respond_to?(:default_scopes) && target.default_scopes.any?
          raise UnsupportedAssociationError,
            "#{target.name} has a default scope, whose conditions this adapter cannot put " \
            "onto the correlated alias that it makes. The rows that the scope removes are " \
            "absent from the attributes that Cerbos evaluates, so the filter would not agree " \
            "with the decision. Use unscoped models for the attributes in a policy."
        end

        table = target.arel_table.alias(aliaser.next_alias(target.table_name))
        predicates = []

        case reflection.macro
        when :has_many, :has_one
          assert_single_key(reflection, owner_model,
            reflection.foreign_key, reflection.active_record_primary_key)
          predicates << table[reflection.foreign_key].eq(
            owner_table[reflection.active_record_primary_key]
          )
          # An `as:` association uses a type column to select its rows. Without a condition on
          # that column, the subquery also finds the rows of a different owner class.
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
