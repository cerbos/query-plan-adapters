# frozen_string_literal: true

# Small models for the association shapes the adapter must refuse to guess at.
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
    end
  end
end

class EdgeAuthor < ActiveRecord::Base
  self.table_name = "edge_authors"
end

class EdgeComment < ActiveRecord::Base
  self.table_name = "edge_comments"
  belongs_to :commentable, polymorphic: true
end

class EdgeDocument < ActiveRecord::Base
  self.table_name = "edge_documents"
  belongs_to :author, class_name: "EdgeAuthor", foreign_key: :author_id
  has_many :comments, class_name: "EdgeComment", as: :commentable
  has_many :approved_comments, -> { where(approved: true) },
    class_name: "EdgeComment", as: :commentable
end
