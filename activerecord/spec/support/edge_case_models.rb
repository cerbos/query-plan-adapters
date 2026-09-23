# frozen_string_literal: true

# Small models for the association shapes that the adapter must refuse.
module EdgeCaseModels
  module_function

  def establish!
    return if @established
    @established = true

    Database.establish!
    ActiveRecord::Schema.verbose = false
    ActiveRecord::Schema.define do
      create_table :edge_documents, force: true do |t|
        t.string :title
        t.integer :author_id
        # Zero, positive and negative rows for the division tests: x/0 is NaN, +Inf or -Inf.
        t.integer :n
        # For the int() cast: CEL truncates toward zero, PostgreSQL and MySQL round.
        t.float :score
      end

      create_table :edge_comments, force: true do |t|
        t.string :body
        t.boolean :approved
        t.integer :commentable_id
        t.string :commentable_type
      end

      create_table :edge_authors, force: true do |t|
        t.string :name
      end

      create_table :edge_tags, force: true do |t|
        t.string :name
        t.boolean :visible, default: true
        t.integer :document_id
      end

      create_table :edge_softs, force: true do |t|
        t.string :name
        t.integer :document_id
      end

      create_table :edge_profiles, force: true do |t|
        t.string :name
        t.integer :document_id
      end

      create_table :edge_kinds, force: true do |t|
        t.string :name
        t.string :type
        t.integer :document_id
      end

      create_table :edge_cpk_parents, id: false, force: true do |t|
        t.string :tenant_id, null: false
        t.string :code, null: false
      end

      create_table :edge_cpk_kids, force: true do |t|
        t.string :tenant_id
        t.string :parent_code
        t.string :name
      end

      # The second hop of a chain, for the absent-parent guard.
      create_table :edge_tag_labels, force: true do |t|
        t.string :name
        t.integer :tag_id
      end
    end

    EdgeDocument.create!(id: 1, title: "zero", n: 0, score: 0.0)
    EdgeDocument.create!(id: 2, title: "two", n: 2, score: 2.5)
    EdgeDocument.create!(id: 3, title: "negative", n: -3, score: -0.6)

    # A chain must tell apart: parent with a matching child, parent without one, and no parent
    # at all. Only the last is a missing path for CEL.
    chained = EdgeTag.create!(id: 91, name: "chained", document_id: 1)
    EdgeTag.create!(id: 92, name: "childless", document_id: 2)
    EdgeTagLabel.create!(name: "urgent", tag_id: chained.id)
  end
end

class EdgeTagLabel < ActiveRecord::Base
  self.table_name = "edge_tag_labels"
end

class EdgeAuthor < ActiveRecord::Base
  self.table_name = "edge_authors"
end

class EdgeComment < ActiveRecord::Base
  self.table_name = "edge_comments"
  belongs_to :commentable, polymorphic: true
end

# Has a default scope, which hides rows from every association to it.
class EdgeSoft < ActiveRecord::Base
  self.table_name = "edge_softs"
  default_scope { where("name != 'hidden'") }
end

class EdgeTag < ActiveRecord::Base
  self.table_name = "edge_tags"
  has_many :labels, class_name: "EdgeTagLabel", foreign_key: :tag_id
end

class EdgeProfile < ActiveRecord::Base
  self.table_name = "edge_profiles"
end

# Single-table inheritance. An association to the subclass filters on the type column, which
# the adapter does not add.
class EdgeKind < ActiveRecord::Base
  self.table_name = "edge_kinds"
end

class EdgeSpecialKind < EdgeKind; end

class EdgeCpkKid < ActiveRecord::Base
  self.table_name = "edge_cpk_kids"
end

# Composite primary key: association keys are an array, and one equality cannot join on two
# columns.
class EdgeCpkParent < ActiveRecord::Base
  self.table_name = "edge_cpk_parents"
  self.primary_key = [:tenant_id, :code]

  # 7.1 needs `query_constraints:` and 8.0 needs `foreign_key:`; each rejects the other.
  # Fixture only: the adapter's refusal is the same on both.
  if ::ActiveRecord.version >= Gem::Version.new("7.2")
    has_many :kids, class_name: "EdgeCpkKid", foreign_key: [:tenant_id, :parent_code]
  else
    has_many :kids, class_name: "EdgeCpkKid", query_constraints: [:tenant_id, :parent_code]
  end
end

class EdgeDocument < ActiveRecord::Base
  self.table_name = "edge_documents"
  belongs_to :author, class_name: "EdgeAuthor", foreign_key: :author_id
  has_many :comments, class_name: "EdgeComment", as: :commentable
  has_many :approved_comments, -> { where(approved: true) },
    class_name: "EdgeComment", as: :commentable

  has_many :tags, class_name: "EdgeTag", foreign_key: :document_id
  # A scope on the OUTER association of a through chain.
  has_many :visible_tags, -> { where(visible: true) },
    class_name: "EdgeTag", foreign_key: :document_id
  has_many :softs, class_name: "EdgeSoft", foreign_key: :document_id
  has_one :profile, class_name: "EdgeProfile", foreign_key: :document_id
  has_many :kinds, class_name: "EdgeKind", foreign_key: :document_id
  has_many :special_kinds, class_name: "EdgeSpecialKind", foreign_key: :document_id
end
