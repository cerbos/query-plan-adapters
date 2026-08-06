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
        # A zero, a positive number and a negative number, for the division tests. IEEE-754
        # gives NaN for 0/0, +Infinity for a positive numerator and -Infinity for a negative
        # one, and each of the three needs a row.
        t.integer :n
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
    end

    EdgeDocument.create!(id: 1, title: "zero", n: 0)
    EdgeDocument.create!(id: 2, title: "two", n: 2)
    EdgeDocument.create!(id: 3, title: "negative", n: -3)
  end
end

class EdgeAuthor < ActiveRecord::Base
  self.table_name = "edge_authors"
end

class EdgeComment < ActiveRecord::Base
  self.table_name = "edge_comments"
  belongs_to :commentable, polymorphic: true
end

# A model with a default scope. The scope removes rows from every association that points at
# it, and thus from the attributes that Cerbos sees.
class EdgeSoft < ActiveRecord::Base
  self.table_name = "edge_softs"
  default_scope { where("name != 'hidden'") }
end

class EdgeTag < ActiveRecord::Base
  self.table_name = "edge_tags"
end

class EdgeProfile < ActiveRecord::Base
  self.table_name = "edge_profiles"
end

# A single-table hierarchy. An association that points at the subclass also filters on the
# inheritance column, and the adapter does not add that condition.
class EdgeKind < ActiveRecord::Base
  self.table_name = "edge_kinds"
end

class EdgeSpecialKind < EdgeKind; end

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
