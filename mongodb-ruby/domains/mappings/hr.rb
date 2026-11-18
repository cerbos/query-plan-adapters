# frozen_string_literal: true

module MongoDomains
  module Hr
    NULLS = :omitted
    R = "request.resource.attr."

    # Each document is the resource's attributes as check() received them, so every attribute
    # maps to the field of its own name. `R.id` is the document's `_id`.
    MAPPER = lambda do |ref|
      next {field: "_id"} if ref == "request.resource.id"

      {field: ref.delete_prefix(R)} if ref.start_with?(R)
    end
  end
end
