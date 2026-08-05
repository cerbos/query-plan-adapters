# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    # The parent class of all the errors from this adapter.
    #
    # The adapter is fail-closed. If it cannot translate a query plan correctly, it raises an
    # error. It does not make a filter that is only approximately correct. An incorrect filter
    # is an authorization bug, because it gives rows that the PDP denies. An error is a bug
    # report.
    class Error < StandardError; end

    # The plan refers to an attribute that the attribute map does not contain. Or the plan
    # uses that attribute in a position that its mapping cannot fill. For example, it uses a
    # relation where a column is necessary.
    class UnmappedAttributeError < Error; end

    # The plan uses an operator, or a shape of operand, that this adapter cannot translate
    # into a correct SQL filter.
    class UnsupportedOperatorError < Error; end

    # The structure of the plan is bad. Or the plan contains a literal value that the adapter
    # cannot show correctly, such as a timestamp or a hierarchy delimiter.
    class InvalidPlanError < Error; end

    # An attribute maps to an association that the adapter cannot change into a correlated
    # subquery.
    class UnsupportedAssociationError < Error; end
  end
end
