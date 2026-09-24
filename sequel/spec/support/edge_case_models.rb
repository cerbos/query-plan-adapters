# frozen_string_literal: true

# Small models for the association shapes that the adapter must refuse, and for the one shape a
# Sequel application has that the corpus does not: a many_to_many through a join table.
#
# The tables are created when this file is loaded, because a Sequel::Model class reads the
# columns of its table when it is defined.
module EdgeCaseModels
  DB = Database::DB

  DB.create_table!(:edge_documents, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :title
    Integer :author_id
    # A zero, a positive number and a negative number, for the division tests. IEEE-754 gives
    # NaN for 0/0, +Infinity for a positive numerator and -Infinity for a negative one, and each
    # of the three needs a row.
    Integer :n
    # A double column, so a test can reach the int() cast that CEL and SQL disagree about: CEL
    # removes the fraction toward zero, PostgreSQL and MySQL round to the nearest.
    Float :score
  end

  DB.create_table!(:edge_authors, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
  end

  DB.create_table!(:edge_tags, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
    TrueClass :visible, default: true
    Integer :document_id
  end

  # The second hop of a chain, for the absent-parent guard.
  DB.create_table!(:edge_tag_labels, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
    Integer :tag_id
  end

  DB.create_table!(:edge_softs, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
    Integer :document_id
  end

  DB.create_table!(:edge_profiles, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
    Integer :document_id
  end

  DB.create_table!(:edge_kinds, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
    String :kind
    Integer :document_id
  end

  DB.create_table!(:edge_cpk_parents, **Database::TABLE_OPTIONS) do
    String :tenant_id, null: false
    String :code, null: false
    Integer :document_id
    primary_key [:tenant_id, :code]
  end

  DB.create_table!(:edge_cpk_kids, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :tenant_id
    String :parent_code
    String :name
  end

  # A many_to_many, the one association type that has a join table between the owner and the
  # target. Cerbos never sees the join table, so it is a hop with no model.
  DB.create_table!(:edge_keywords, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :name
  end

  DB.create_table!(:edge_document_keywords, **Database::TABLE_OPTIONS) do
    Integer :document_id
    Integer :keyword_id
  end

  module_function

  def establish!
    return if @established
    @established = true

    DB[:edge_documents].insert(id: 1, title: "zero", n: 0, score: 0.0)
    DB[:edge_documents].insert(id: 2, title: "two", n: 2, score: 2.5)
    DB[:edge_documents].insert(id: 3, title: "negative", n: -3, score: -0.6)

    # The three rows a chain must tell apart: a parent with a matching child, a parent with no
    # matching child, and NO parent at all. Only the last one is a missing path for CEL.
    DB[:edge_tags].insert(id: 91, name: "chained", document_id: 1)
    DB[:edge_tags].insert(id: 92, name: "childless", document_id: 2)
    DB[:edge_tag_labels].insert(name: "urgent", tag_id: 91)

    # One keyword each on the first two documents, none on the third.
    DB[:edge_keywords].insert(id: 1, name: "alpha")
    DB[:edge_keywords].insert(id: 2, name: "beta")
    DB[:edge_document_keywords].insert(document_id: 1, keyword_id: 1)
    DB[:edge_document_keywords].insert(document_id: 2, keyword_id: 2)
  end
end

class EdgeTagLabel < Sequel::Model(Database::DB[:edge_tag_labels]); end

class EdgeAuthor < Sequel::Model(Database::DB[:edge_authors]); end

# A model over a FILTERED dataset. Every read of the application applies the filter, and a
# subquery over the plain table would not.
class EdgeSoft < Sequel::Model(Database::DB[:edge_softs].exclude(name: "hidden")); end

class EdgeTag < Sequel::Model(Database::DB[:edge_tags])
  one_to_many :labels, class: :EdgeTagLabel, key: :tag_id
end

class EdgeProfile < Sequel::Model(Database::DB[:edge_profiles]); end

# A single-table hierarchy. The plugin filters the dataset of the subclass on the key column,
# and the adapter does not add that condition.
class EdgeKind < Sequel::Model(Database::DB[:edge_kinds])
  plugin :single_table_inheritance, :kind
end

class EdgeSpecialKind < EdgeKind; end

class EdgeCpkKid < Sequel::Model(Database::DB[:edge_cpk_kids]); end

# A composite primary key. Sequel then gives an ARRAY for the keys of the association, and one
# equality cannot join on two columns.
class EdgeCpkParent < Sequel::Model(Database::DB[:edge_cpk_parents])
  one_to_many :kids, class: :EdgeCpkKid,
    key: [:tenant_id, :parent_code], primary_key: [:tenant_id, :code]
end

class EdgeKeyword < Sequel::Model(Database::DB[:edge_keywords]); end

class EdgeDocument < Sequel::Model(Database::DB[:edge_documents])
  many_to_one :author, class: :EdgeAuthor
  one_to_many :tags, class: :EdgeTag, key: :document_id
  # The three ways a Sequel association filters its own rows.
  one_to_many :visible_tags, class: :EdgeTag, key: :document_id, conditions: {visible: true}
  one_to_many :block_tags, class: :EdgeTag, key: :document_id do |dataset|
    dataset.where(visible: true)
  end
  one_to_many :dataset_tags, class: :EdgeTag,
    dataset: proc { EdgeTag.where(document_id: id, visible: true) }
  one_to_many :limited_tags, class: :EdgeTag, key: :document_id, limit: 1
  one_to_many :softs, class: :EdgeSoft, key: :document_id
  one_to_one :profile, class: :EdgeProfile, key: :document_id
  one_to_many :kinds, class: :EdgeKind, key: :document_id
  one_to_many :special_kinds, class: :EdgeSpecialKind, key: :document_id
  one_to_many :cpk_parents, class: :EdgeCpkParent, key: :document_id
  many_to_many :keywords, class: :EdgeKeyword,
    join_table: :edge_document_keywords, left_key: :document_id, right_key: :keyword_id
  one_through_one :first_keyword, class: :EdgeKeyword,
    join_table: :edge_document_keywords, left_key: :document_id, right_key: :keyword_id
end
