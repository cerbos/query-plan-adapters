# frozen_string_literal: true

require_relative "sql_support"
require_relative "errors"

module Cerbos
  module Sequel
    # Changes an association mapping into a correlated subquery scope. The scope holds the
    # tables with their aliases, the joins between those tables, and the predicates that
    # connect the subquery to the row in the query around it.
    #
    # Each scope gets new table aliases. This is necessary and not only a preference. A policy
    # can put a macro on an association inside another macro on the same association. If the
    # inner subquery had no alias, it would connect to its own row and not to the outer row.
    module Relations
      # One association hop: the table name, its alias, the predicates that connect that
      # table to the tables before it, and the model whose columns it holds. The join table of
      # a +many_to_many+ is a hop with no model, because no Cerbos attribute reads it.
      Hop = Struct.new(:table_name, :alias_name, :predicates, :model) do
        def table
          ::Sequel[alias_name]
        end

        def source
          ::Sequel.as(table_name, alias_name)
        end
      end

      # Makes table aliases that are all different, for one translation.
      class Aliaser
        def initialize
          @counter = 0
        end

        def next_alias(table_name)
          @counter += 1
          :"cerbos_#{table_name}_#{@counter}"
        end
      end

      # A collection after the adapter resolves it. The adapter can change it into an EXISTS
      # subquery or a COUNT subquery.
      class Scope
        def initialize(db:, hops:, model:, mapping: nil, guard_hops: [])
          @db = db
          @hops = hops
          @mapping = mapping
          @model = model
          @guard_hops = guard_hops
        end

        attr_reader :db, :hops, :mapping, :model, :guard_hops

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
              "Association #{mapping&.association.inspect} is used as a collection of bare values " \
              "but declares no member_field"
          end
          table[field.to_sym]
        end

        # Makes +EXISTS (SELECT 1 FROM ... WHERE <correlation> [AND <conditions>])+.
        def exists(*conditions)
          subquery(hops, 1, conditions).exists
        end

        # Makes +(SELECT count(*) FROM ... WHERE <correlation> [AND <conditions>])+ as a
        # scalar value.
        def count(*conditions)
          subquery(hops, ::Sequel.function(:count).*, conditions)
        end

        # Makes +(SELECT <column> FROM ... WHERE <correlation>)+ as a scalar value. A field
        # path with dots through to-one associations uses this. A scalar subquery cannot
        # increase the number of rows in the result. A JOIN can do that.
        def scalar(column_name)
          subquery(hops, table[column_name.to_sym], [])
        end

        # Makes +CASE WHEN EXISTS (<the hops before the collection>) THEN <expression> END+.
        #
        # CEL cannot read a field from a list, so each part before the last part of a path is
        # a to-ONE parent. When that parent is absent, the application sends no attribute, CEL
        # makes a missing-path error, and the decision is a deny. A subquery from the resource
        # row cannot see the difference: an absent parent and a parent with no children both
        # give no rows. Then +all+ reads TRUE, +!exists+ reads TRUE and the count reads 0, and
        # each one of those gives back rows that the PDP denies
        # (cerbos/query-plan-adapters#309).
        #
        # The CASE has no ELSE value. Thus an absent parent gives NULL, and +NOT NULL+ is also
        # NULL, so the row stays out of the result under both polarities. Every operator that
        # reads a chain must come through here and not only the collection macros: a plain
        # EXISTS has two values, so it is FALSE for an absent parent and its negation is TRUE
        # (#315, #316).
        #
        # +guard_hops+ is empty for a relation that the caller mapped directly. Such a relation
        # keeps the meaning of an empty collection: +!tags.exists(...)+ over zero tags is TRUE.
        def guarded(expression)
          return expression if guard_hops.empty?

          SqlSupport.case_node([[subquery(guard_hops, 1, []).exists, expression]])
        end

        private

        def subquery(hop_list, projection, conditions)
          dataset = db.from(hop_list.first.source)
          hop_list.drop(1).each do |hop|
            dataset = dataset.join(hop.table_name, SqlSupport.and_node(hop.predicates),
              table_alias: hop.alias_name)
          end
          dataset = dataset.select(projection)
          (hop_list.first.predicates + conditions.compact).each do |condition|
            dataset = dataset.where(condition)
          end
          dataset
        end
      end

      COLLECTION_TYPES = %i[one_to_many many_to_many].freeze
      TO_ONE_TYPES = %i[many_to_one one_to_one].freeze

      # The options of an association that remove rows from it. The subquery reads the target
      # table through its own alias, so it cannot apply any of them, and the rows that they
      # remove would still reach the filter while Cerbos never saw them.
      FILTERING_OPTIONS = %i[conditions block dataset limit].freeze

      # The parts of a model's dataset that the subquery cannot reproduce. A model built on a
      # filtered dataset — `Sequel::Model(DB[:tags].where(visible: true))`, or a subclass under
      # the `single_table_inheritance` plugin — reads only some rows of its table.
      FILTERING_DATASET_OPTIONS = %i[where having join group limit offset distinct].freeze

      module_function

      # @param owner_model [Class] the Sequel model that has the association
      # @param owner_table [Sequel::SQL::Identifier] the table of that model in the query
      #   around it
      # @param mapping [AttributeMapping::Association]
      # @param aliaser [Aliaser]
      # @return [Scope]
      def build(owner_model:, owner_table:, mapping:, aliaser:)
        reflection = reflection_for(owner_model, mapping.association, UnsupportedAssociationError)

        # A collection mapping needs a collection. The database does not enforce that a
        # `one_to_one` has only one row. Thus the association gives one row and Cerbos sees one
        # element, while a subquery would examine every row with that foreign key, and the two
        # answers differ.
        unless COLLECTION_TYPES.include?(reflection[:type])
          raise UnsupportedAssociationError,
            "Association #{mapping.association.inspect} on #{owner_model.name} is a " \
            "#{reflection[:type]}, not a collection. Map a to-one association as a field path " \
            "with dots, for example Cerbos::Sequel.field(\"profile.name\")."
        end

        hops = hops_for(reflection, owner_table, owner_model, aliaser)
        Scope.new(db: owner_model.db, hops: hops, mapping: mapping, model: hops.last.model)
      end

      # Resolves a relation that the plan reaches THROUGH another relation, for the attribute
      # path +R.attr.mainCategory.subCategories+. The caller writes the chain as a nested
      # +fields:+ mapping, and each step becomes one more set of hops in the same correlated
      # subquery.
      #
      # The nesting is what makes the hop requirement visible to the adapter: only the nested
      # form says which hops are the parent and which hop is the collection. See
      # {Scope#guarded}.
      #
      # @param outer_scope [Scope] the relation that holds the nested mapping
      # @return [Scope] the full chain, which requires the hops of +outer_scope+ to exist
      def chain(outer_scope:, mapping:, aliaser:)
        inner = build(
          owner_model: outer_scope.model,
          owner_table: outer_scope.table,
          mapping: mapping,
          aliaser: aliaser
        )

        Scope.new(
          db: outer_scope.db,
          hops: outer_scope.hops + inner.hops,
          mapping: mapping,
          model: inner.model,
          guard_hops: outer_scope.hops
        )
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
          reflection = reflection_for(model, name.to_sym, UnmappedAttributeError)
          unless TO_ONE_TYPES.include?(reflection[:type])
            raise UnsupportedAssociationError,
              "Field path segment #{name.inspect} on #{model.name} is a #{reflection[:type]} " \
              "association; a scalar attribute can only be read through many_to_one and " \
              "one_to_one — map a collection with Cerbos::Sequel.association instead"
          end

          hops.concat(hops_for(reflection, table, model, aliaser))
          table = hops.last.table
          model = hops.last.model
        end

        Scope.new(db: owner_model.db, hops: hops, model: model)
      end

      # @api private
      def reflection_for(model, name, error_class)
        reflection = model.association_reflection(name)
        return reflection if reflection

        raise error_class, "#{model.name} has no association #{name.inspect}"
      end

      # @api private
      def hops_for(reflection, owner_table, owner_model, aliaser)
        assert_unfiltered(reflection, owner_model)
        target = assert_plain_target(reflection, owner_model)

        case reflection[:type]
        when :one_to_many, :one_to_one
          assert_single_key(reflection, owner_model, :uses_composite_keys)
          [hop(target.table_name, target, aliaser) { |table|
            [SqlSupport.comparison("eq", table[reflection[:key]], owner_table[reflection.primary_key])]
          }]
        when :many_to_one
          assert_single_key(reflection, owner_model, :uses_composite_keys)
          [hop(target.table_name, target, aliaser) { |table|
            [SqlSupport.comparison("eq", table[reflection.primary_key], owner_table[reflection[:key]])]
          }]
        when :many_to_many
          assert_single_key(reflection, owner_model, :uses_left_composite_keys, :uses_right_composite_keys)
          join = hop(reflection[:join_table], nil, aliaser) { |table|
            [SqlSupport.comparison("eq", table[reflection[:left_key]], owner_table[reflection[:left_primary_key]])]
          }
          member = hop(target.table_name, target, aliaser) { |table|
            [SqlSupport.comparison("eq", table[reflection.right_primary_key], join.table[reflection[:right_key]])]
          }
          [join, member]
        else
          raise UnsupportedAssociationError,
            "Association #{reflection[:name].inspect} on #{owner_model.name} has type " \
            "#{reflection[:type].inspect}, which this adapter cannot express as a correlated " \
            "subquery"
        end
      end

      # @api private
      def hop(table_name, model, aliaser)
        table_name = table_name.to_sym
        alias_name = aliaser.next_alias(table_name)
        Hop.new(table_name, alias_name, yield(::Sequel[alias_name]), model)
      end

      # An association with its own conditions removes rows from it, and thus from the
      # attributes that Cerbos sees. This adapter cannot put those conditions onto the alias
      # that it makes for the correlated subquery, so the filter would select rows that the
      # decision did not. A block and a custom dataset are the same hazard with no way for the
      # adapter to read the conditions at all.
      #
      # The options are read from what the caller WROTE (+:orig_opts+): Sequel fills +:dataset+
      # with a default for every association, so the reflection itself cannot tell a custom
      # dataset from the default one.
      #
      # @api private
      def assert_unfiltered(reflection, owner_model)
        written = reflection[:orig_opts] || {}
        # A value and not only a key: Sequel records `block: nil` for an association that was
        # declared without one.
        filtering = FILTERING_OPTIONS.reject { |option| written[option].nil? }
        return if filtering.empty?

        raise UnsupportedAssociationError,
          "Association #{reflection[:name].inspect} on #{owner_model.name} carries " \
          "#{filtering.map(&:inspect).join(", ")}, which filter the association in a way this " \
          "adapter cannot re-bind onto the correlated alias it generates; map the attribute " \
          "onto an unfiltered association instead"
      end

      # Sequel gives an array for a key that has more than one column. The adapter builds one
      # equality for a correlated subquery and refuses the association here, instead of joining
      # on the first column only.
      #
      # @api private
      def assert_single_key(reflection, owner_model, *flags)
        return unless flags.any? { |flag| reflection[flag] }

        raise UnsupportedAssociationError,
          "Association #{reflection[:name].inspect} on #{owner_model.name} joins on more than " \
          "one column. This adapter builds one equality for a correlated subquery and cannot " \
          "express a composite key. Give an operator override for this attribute."
      end

      # Refuses an association whose target model reads only some rows of its table. The
      # subquery reads the table through a plain alias and would see the rest.
      #
      # This one check covers a model built on a filtered dataset and a subclass under the
      # `single_table_inheritance` plugin, because that plugin is exactly a filter on the
      # subclass dataset. The base class of such a hierarchy reads its whole table and passes.
      #
      # @api private
      # @return [Class] the target model
      def assert_plain_target(reflection, owner_model)
        target = reflection.associated_class
        filtering = FILTERING_DATASET_OPTIONS.select { |option| target.dataset.opts[option] }
        return target if filtering.empty?

        raise UnsupportedAssociationError,
          "#{target.name} reads a filtered dataset (#{filtering.map(&:inspect).join(", ")}), " \
          "whose conditions this adapter cannot put onto the correlated alias that it makes. " \
          "The rows that the dataset removes are absent from the attributes that Cerbos " \
          "evaluates, so the filter would not agree with the decision. Point the association " \
          "at a model over the whole table, or give an operator override."
      end
    end
  end
end
