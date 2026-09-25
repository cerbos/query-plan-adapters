# frozen_string_literal: true

require_relative "aggregation"
require_relative "errors"
require_relative "mapper"
require_relative "operands"

module Cerbos
  module MongoDB
    # CEL's three-valued logic as one aggregation expression: every condition evaluates to
    # true, false, or null for a CEL error. It is what a query filter cannot say. $elemMatch
    # rebases every path onto the array element, and MongoDB accepts $expr only at the top level,
    # so an element compared with the enclosing document, arithmetic or a ternary inside a macro,
    # and a macro nested over the same collection have no filter form; and a filter has no
    # UNKNOWN, so a negated macro would turn an element's error into a match. Evaluated here
    # instead, a condition is kept where it is `true`, and a negated one where it is `false`: an
    # error is neither, so it denies under both polarities, as it does in CEL.
    #
    # Errors follow CEL: `&&` is false if any operand is false, `||` true if any is true, and an
    # error otherwise wins; exists() is true if any element is, all() false if any element is,
    # and exists_one() raises if any element does. A leaf is an error where one of its operands
    # cannot be evaluated: a nullable field that is null or missing, an absent to-one parent, a
    # field read on an element that is not a document, or any guard Aggregation declares.
    module Logic
      extend Operands

      # A macro's elements are bound to `$$cel_<name>`, apart from the internal `$$cerbos_*`.
      BINDING_PREFIX = "cel_"

      module_function

      # @return [Object] an aggregation expression evaluating to true, false or null
      def truth(node, mapper, bound = [])
        return constant_truth(node.value) if value?(node)
        return bare_truth(node, mapper, bound) if variable?(node)
        raise InvalidPlanError, "Invalid Cerbos expression structure" unless expression?(node)

        case node.operator
        when "and" then all_of(node.operands.map { |child| truth(child, mapper, bound) })
        when "or" then any_of(node.operands.map { |child| truth(child, mapper, bound) })
        when "not" then negate(truth(single(node), mapper, bound))
        when "exists", "all", "exists_one" then macro(node, mapper, bound)
        when "if" then ternary(node, mapper, bound)
        when *Aggregation::COMPARISONS.keys, "matches", "contains", "startsWith", "endsWith", "in"
          leaf(node, mapper, bound)
        when "filter", "map", "except", "list", "struct"
          # A list or map where CEL needs a boolean is a runtime type error.
          nil
        else
          raise UnsupportedError, "#{node.operator} has no three-valued aggregation form"
        end
      end

      def constant_truth(value)
        raise UnsupportedError, "A non-boolean constant in boolean position is an error" unless boolean?(value)

        value
      end

      def single(node)
        operand_at(node.operands, 0, "#{node.operator} requires an operand")
      end

      def all_of(values)
        with_values(values) { |list| {"$cond" => [{"$in" => [false, list]}, false, {"$cond" => [{"$in" => [nil, list]}, nil, true]}]} }
      end

      def any_of(values)
        with_values(values) { |list| {"$cond" => [{"$in" => [true, list]}, true, {"$cond" => [{"$in" => [nil, list]}, nil, false]}]} }
      end

      def negate(value)
        {"$let" => {"vars" => {"cerbos_x" => value}, "in" => {"$cond" => [{"$eq" => ["$$cerbos_x", nil]}, nil, {"$not" => ["$$cerbos_x"]}]}}}
      end

      def with_values(values)
        {"$let" => {"vars" => {"cerbos_values" => values}, "in" => yield("$$cerbos_values")}}
      end

      # A boolean field in boolean position: anything but a boolean is an error.
      def bare_truth(node, mapper, bound)
        value = Aggregation.build(node, mapper)
        guarded(ok(variable_oks([node.name], mapper, bound) + [{"$eq" => [{"$type" => value}, "bool"]}]), value)
      end

      # A comparison or string predicate: its value where every operand evaluates, else null.
      def leaf(node, mapper, bound)
        if Aggregation::COMPARISONS.key?(node.operator) &&
            node.operands.any? { |op| variable?(op) && mapper.value_type(op.name) == :date_time } &&
            node.operands.none? { |op| value?(op) && op.value.nil? }
          raise FinalUnsupportedError,
            "Bare temporal field comparison cannot preserve CEL string comparison: stored Dates " \
            "discard the original lexical spelling; compare timestamp(...) values instead"
        end

        guarded(evaluates(node, mapper, bound), Aggregation.build(node, mapper))
      end

      # Whether +node+ evaluates without a CEL error: each operand does, and the node's own guard
      # holds. A ternary evaluates only the branch its condition selects, and only a boolean
      # condition. `&&` and `||` inside a value are required to evaluate on every operand, which
      # can deny where CEL absorbs an error, but never allows where it raises.
      def evaluates(node, mapper, bound)
        return true if value?(node)
        return ok(variable_oks([node.name], mapper, bound)) if variable?(node)

        if node.operator == "if" && node.operands.length == 3
          condition, then_branch, else_branch = node.operands
          selected = Aggregation.build(condition, mapper)
          return ok([
            evaluates(condition, mapper, bound),
            {"$eq" => [{"$type" => selected}, "bool"]},
            {"$cond" => [selected, evaluates(then_branch, mapper, bound), evaluates(else_branch, mapper, bound)]}
          ])
        end
        return {"$ne" => [filter_value(node, mapper, bound), nil]} if node.operator == "filter"
        if Aggregation::LAMBDA_OPERATORS.include?(node.operator)
          raise UnsupportedError, "#{node.operator} inside a value has no three-valued form"
        end

        guard = Aggregation.guard_for(node, mapper)
        if guard && !(guard.is_a?(Hash) && guard.keys == ["$expr"])
          raise UnsupportedError, "A guard with no aggregation form"
        end

        ok(node.operands.map { |child| evaluates(child, mapper, bound) } + (guard ? [guard["$expr"]] : []))
      end

      def ternary(node, mapper, bound)
        condition, then_branch, else_branch = node.operands
        raise InvalidPlanError, "if requires three operands" unless condition && then_branch && else_branch

        {"$let" => {
          "vars" => {"cerbos_condition" => truth(condition, mapper, bound)},
          "in" => {"$switch" => {
            "branches" => [
              {"case" => {"$eq" => ["$$cerbos_condition", true]}, "then" => truth(then_branch, mapper, bound)},
              {"case" => {"$eq" => ["$$cerbos_condition", false]}, "then" => truth(else_branch, mapper, bound)}
            ],
            "default" => nil
          }}
        }}
      end

      def ok(conditions) = conditions.empty? || {"$and" => conditions.uniq}

      def guarded(ok, value) = {"$cond" => [ok, value, nil]}

      # What must hold for each variable to be readable: a nullable one is neither null nor
      # missing, a to-one parent is a document, and a macro element read through a field is a
      # document.
      def variable_oks(names, mapper, bound)
        names.uniq.flat_map { |name|
          element = bound.find { |variable| name.start_with?("#{variable}.") }
          if element
            [{"$eq" => [{"$type" => "$$#{BINDING_PREFIX}#{element}"}, "object"]}] + nullable_ok(name, mapper)
          elsif bound.include?(name)
            # A relation's element read as its projected field is read through a document.
            projected = Aggregation.build(Plan::Variable.new(name), mapper).include?(".")
            (projected ? [{"$eq" => [{"$type" => "$$#{BINDING_PREFIX}#{name}"}, "object"]}] : []) + nullable_ok(name, mapper)
          else
            relation = mapper.relation_of(name)
            parent = if relation&.type == :one
              [{"$eq" => [{"$type" => "$#{relation.name}"}, "object"]}]
            elsif relation&.requires_parent
              # The to-one parent, stored as an array, is stored at all.
              [{"$ne" => [Aggregation.build(Plan::Variable.new(name), mapper), nil]}]
            else
              []
            end
            parent + nullable_ok(name, mapper)
          end
        }
      end

      def nullable_ok(name, mapper)
        return [] unless mapper.nullable?(name)

        [{"$not" => [{"$in" => [{"$type" => Aggregation.build(Plan::Variable.new(name), mapper)}, %w[missing null]]}]}]
      end

      # exists(), all() and exists_one() over a list: the elements' truths, folded as CEL folds
      # them, where the list is an array; an error otherwise.
      def macro(node, mapper, bound)
        collection, lambda = node.operands
        unless collection && expression_with?(lambda, "lambda") && variable?(lambda.operands[1])
          raise UnsupportedError, "#{node.operator} requires a collection and a single-variable lambda"
        end

        variable = lambda.operands[1].name
        input, list_ok, scoped = element_scope(collection, variable, mapper, bound)
        values = {"$map" => {
          "input" => input,
          "as" => "#{BINDING_PREFIX}#{variable}",
          "in" => truth(lambda.operands[0], scoped, bound + [variable])
        }}
        folded = case node.operator
        when "exists"
          {"$cond" => [{"$in" => [true, "$$cerbos_values"]}, true, {"$cond" => [{"$in" => [nil, "$$cerbos_values"]}, nil, false]}]}
        when "all"
          {"$cond" => [{"$in" => [false, "$$cerbos_values"]}, false, {"$cond" => [{"$in" => [nil, "$$cerbos_values"]}, nil, true]}]}
        else
          {"$cond" => [
            {"$in" => [nil, "$$cerbos_values"]},
            nil,
            {"$eq" => [{"$size" => {"$filter" => {"input" => "$$cerbos_values", "cond" => {"$eq" => ["$$this", true]}}}}, 1]}
          ]}
        end
        guarded(list_ok, {"$let" => {"vars" => {"cerbos_values" => values}, "in" => folded}})
      end

      # filter(): the elements whose condition is true, or null (a CEL error) where the list is
      # not an array or any element's condition raises. A relation's projection yields the
      # projected field of each kept element.
      def filter_value(node, mapper, bound = mapper.bound)
        collection, lambda = node.operands
        unless collection && expression_with?(lambda, "lambda") && variable?(lambda.operands[1])
          raise UnsupportedError, "filter requires a collection and a single-variable lambda"
        end

        variable = lambda.operands[1].name
        binding = "#{BINDING_PREFIX}#{variable}"
        input, list_ok, scoped = element_scope(collection, variable, mapper, bound)
        condition = truth(lambda.operands[0], scoped, bound + [variable])
        kept = {"$filter" => {"input" => input, "as" => binding, "cond" => {"$eq" => [condition, true]}}}
        relation = variable?(collection) ? mapper.lookup(collection.name)&.relation : nil
        if relation&.field
          field = relation.fields[relation.field]&.field || relation.field
          kept = {"$map" => {"input" => kept, "as" => binding, "in" => "$$#{binding}.#{field}"}}
        end
        values = {"$map" => {"input" => input, "as" => binding, "in" => condition}}
        guarded(list_ok, {"$cond" => [{"$in" => [nil, values]}, nil, kept]})
      end

      # The array a macro ranges over, what must hold for it to be one, and the mapper its body
      # reads the element with.
      def element_scope(collection, variable, mapper, bound)
        binding = "$#{BINDING_PREFIX}#{variable}"
        if value?(collection)
          raise UnsupportedError, "A macro over a constant that is not a list" unless collection.value.is_a?(Array)

          input = Aggregation.constant(collection.value)
          return [input, true, element_mapper(mapper, variable, binding, nil)]
        end
        raise UnsupportedError, "A macro's collection must be a field or a list" unless variable?(collection)

        relation = mapper.lookup(collection.name)&.relation
        if relation && relation.type != :many
          raise UnsupportedError, "A macro over a to-one relation ranges over its keys, which have no three-valued form"
        end

        input = if relation&.requires_parent
          Aggregation.relation_elements(relation, nil)
        elsif relation
          "$#{relation.name}"
        else
          Aggregation.build(collection, mapper)
        end
        list_ok = ok(variable_oks([collection.name], mapper, bound) + [{"$isArray" => input}])
        [input, list_ok, element_mapper(mapper, variable, binding, relation)]
      end

      # The mapper a macro's body reads with: the element as `$$cel_<variable>` (its projected
      # field for a relation that names one), each element field under it, and every other key
      # as the enclosing mapper reads it.
      def element_mapper(outer, variable, binding, relation)
        prefix = binding
        Mapper.new(lambda { |key|
          next outer.lookup(key) unless key == variable || key.start_with?("#{variable}.")

          field = (key == variable) ? relation&.field : key[(variable.length + 1)..]
          next Mapper::Config.new(prefix, nil, nil, nil, nil) if field.nil?

          config = relation&.fields&.[](field)
          path = "#{prefix}.#{config&.field || field}"
          nested = config&.relation && Mapper::Relation.new(
            "#{prefix}.#{config.relation.name}", config.relation.type, config.relation.field, config.relation.requires_parent, config.relation.fields
          )
          Mapper::Config.new(path, config&.nullable, config&.value_parser, config&.value_type, nested)
        }, nullable_default: outer.nullable_default, bound: outer.bound + [variable])
      end
    end
  end
end
