# frozen_string_literal: true

module MongoDomains
  module Fintech
    NULLS = :omitted
    R = "request.resource.attr."

    # Each document is the resource's attributes as check() received them, so every attribute
    # maps to the field of its own name.
    MAPPER = ->(ref) { {field: ref.delete_prefix(R)} if ref.start_with?(R) }
  end
end
