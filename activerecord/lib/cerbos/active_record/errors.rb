# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    # Base class for every error this adapter raises.
    #
    # The adapter fails closed: a query plan shape it cannot translate faithfully raises
    # rather than emitting a best-effort filter. A wrong filter is an authorization bug that
    # returns rows the PDP denies; a raise is a bug report.
    class Error < StandardError; end

    # A plan referenced an attribute that the caller's mapping does not cover, or mapped it
    # to something the surrounding operator cannot use (a relation where a column is
    # required, or vice versa).
    class UnmappedAttributeError < Error; end

    # The plan uses an operator, or an operand shape, that this adapter cannot express as a
    # faithful SQL filter.
    class UnsupportedOperatorError < Error; end

    # The plan is structurally malformed, or a literal it carries (a timestamp, a hierarchy
    # delimiter) is not representable.
    class InvalidPlanError < Error; end

    # An attribute maps onto an association this adapter cannot turn into a correlated
    # subquery.
    class UnsupportedAssociationError < Error; end
  end
end
