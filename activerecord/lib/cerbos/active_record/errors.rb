# frozen_string_literal: true

module Cerbos
  module ActiveRecord
    # Base class for every error this adapter raises.
    #
    # The adapter fails closed: if it cannot translate a plan exactly, it raises. A wrong filter
    # would return rows the PDP denies.
    class Error < StandardError; end

    # The plan uses an attribute that is not mapped, or uses it where its mapping does not fit
    # (for example, a relation where a column is needed).
    class UnmappedAttributeError < Error; end

    # The plan uses an operator or operand shape this adapter cannot translate to correct SQL.
    class UnsupportedOperatorError < Error; end

    # The plan is malformed, or has a literal the adapter cannot represent (such as a timestamp
    # or a hierarchy delimiter).
    class InvalidPlanError < Error; end

    # An attribute maps to an association that cannot become a correlated subquery.
    class UnsupportedAssociationError < Error; end

    # The model's connection is not SQLite, PostgreSQL, MySQL 8.0 or later, or MariaDB 11.4.5 or
    # later, the stores the adapter's translation is proven on.
    class UnsupportedDatabaseError < Error; end
  end
end
