# frozen_string_literal: true

module Cerbos
  module MongoDB
    # The parent class of every error this adapter raises.
    #
    # The adapter is fail-closed. If it cannot translate a query plan faithfully, it raises. It
    # never returns a filter that is only approximately right: a wrong filter is an authorization
    # bug, because it returns documents the PDP denies. An error is a bug report.
    class Error < StandardError; end

    # The plan uses an operator, or a shape of operands, that no MongoDB filter can express with
    # CEL's semantics.
    class UnsupportedError < Error; end

    # A refusal that holds whatever form the filter takes: the documents the store keeps cannot
    # answer the question (a date field has lost the string CEL compares), or the caller's null
    # convention rules the shape out. The three-valued evaluation is not tried after it.
    class FinalUnsupportedError < UnsupportedError; end

    # The plan is malformed, or carries a literal the adapter cannot represent exactly (a
    # timestamp with more than millisecond precision, an empty hierarchy separator).
    class InvalidPlanError < Error; end

    # The mapper is malformed: an unknown key, or a value of the wrong type.
    class MapperError < Error; end
  end
end
