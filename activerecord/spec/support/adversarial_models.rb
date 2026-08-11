# frozen_string_literal: true

require "time"

# These tables are only for this suite. Thus each difficult row from the corpus is possible:
# NULL element columns, names that are the same or that are a mirror of each other, LIKE
# metacharacters, empty strings and empty collections.
module AdversarialModels
  module_function

  def establish!
    return if @established
    @established = true

    Database.establish!
    define_schema!
    seed!
  end

  def define_schema!
    ActiveRecord::Schema.verbose = false
    ActiveRecord::Schema.define do
      create_table :adversarial_resources, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.boolean :a_bool, null: false
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.float :a_double
        t.string :a_optional_string
        t.string :created_by, null: false
        t.string :scope
        t.datetime :created_at
      end

      # The one REAL to-one relation of the corpus (ADR 0005). `parent` and `parent.inner` are
      # separate rows that a join reaches. `obj.inner` looks the same in a policy and is not a
      # join at all: every harness maps it to the `a_string` column of the row itself. The two
      # stay beside each other so a reader can see which of the two dotted attributes makes a
      # join.
      #
      # The unique index on the foreign key is what makes each level to-ONE. Without it,
      # ActiveRecord would still accept the mapping and the adapter would make a subquery that
      # can give more than one row.
      create_table :adversarial_parents, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.boolean :a_bool, null: false
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.string :a_optional_string
        t.string :resource_id, null: false, index: {unique: true}
      end

      create_table :adversarial_inners, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.boolean :a_bool, null: false
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.string :a_optional_string
        t.string :parent_id, null: false, index: {unique: true}
      end

      create_table :adversarial_tags, force: true do |t|
        t.string :tag_id, null: false
        t.string :name
        t.string :resource_id, null: false
      end

      create_table :adversarial_categories, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
        t.string :resource_id, null: false
      end

      create_table :adversarial_sub_categories, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
        t.string :category_id, null: false
      end

      create_table :adversarial_labels, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name
        t.string :sub_category_id, null: false
      end
    end
  end

  # Each row gets its own category graph, with one category for each sub-name. The prisma and
  # sqlalchemy harnesses make the same shape. Thus no two resources use the same relation
  # rows.
  def seed!
    ConformanceCorpus::SEEDS.each do |seed|
      id = seed.fetch("id")

      AdvResource.create!(
        id: id,
        a_bool: seed.fetch("aBool"),
        a_string: seed.fetch("aString"),
        a_number: seed.fetch("aNumber"),
        a_double: ConformanceCorpus.a_double(seed),
        a_optional_string: seed.fetch("aOptionalString"),
        created_by: ConformanceCorpus.created_by(seed),
        scope: ConformanceCorpus.scope(seed),
        created_at: ConformanceCorpus.created_at(seed)&.then { |iso| Time.iso8601(iso) }
      )

      # The to-one chain, with one owned row for each level. A seed with no parent gets no row
      # at all, and that is what makes the absent-parent hazard possible through a SCALAR and
      # not only through the collection of mainCategory.
      parent_seed = ConformanceCorpus.parent_seed_of(seed)
      if parent_seed
        parent_id = "#{id}-parent"
        AdvParent.create!(
          id: parent_id,
          a_bool: parent_seed.fetch("aBool"),
          a_string: parent_seed.fetch("aString"),
          a_number: parent_seed.fetch("aNumber"),
          a_optional_string: parent_seed.fetch("aOptionalString"),
          resource_id: id
        )

        inner_seed = ConformanceCorpus.parent_seed_of(parent_seed)
        if inner_seed
          AdvInner.create!(
            id: "#{parent_id}-inner",
            a_bool: inner_seed.fetch("aBool"),
            a_string: inner_seed.fetch("aString"),
            a_number: inner_seed.fetch("aNumber"),
            a_optional_string: inner_seed.fetch("aOptionalString"),
            parent_id: parent_id
          )
        end
      end

      seed.fetch("tags").each do |tag|
        AdvTag.create!(tag_id: tag.fetch("id"), name: tag.fetch("name"), resource_id: id)
      end

      seed.fetch("subCategoryNames").each_with_index do |sub_name, index|
        category = AdvCategory.create!(id: "#{id}-cat#{index}", name: "business", resource_id: id)
        sub_category = AdvSubCategory.create!(
          id: "#{id}-sub#{index}", name: sub_name, category_id: category.id
        )
        ConformanceCorpus.labels(seed).each_with_index do |label_name, label_index|
          AdvLabel.create!(
            id: "#{id}-label#{index}-#{label_index}",
            name: label_name,
            sub_category_id: sub_category.id
          )
        end
      end
    end
  end
end

class AdvLabel < ActiveRecord::Base
  self.table_name = "adversarial_labels"
end

class AdvSubCategory < ActiveRecord::Base
  self.table_name = "adversarial_sub_categories"
  has_many :labels, class_name: "AdvLabel", foreign_key: :sub_category_id, primary_key: :id
end

class AdvCategory < ActiveRecord::Base
  self.table_name = "adversarial_categories"
  has_many :sub_categories, class_name: "AdvSubCategory", foreign_key: :category_id, primary_key: :id
end

class AdvTag < ActiveRecord::Base
  self.table_name = "adversarial_tags"
end

class AdvInner < ActiveRecord::Base
  self.table_name = "adversarial_inners"
end

class AdvParent < ActiveRecord::Base
  self.table_name = "adversarial_parents"
  has_one :inner, class_name: "AdvInner", foreign_key: :parent_id, primary_key: :id
end

class AdvResource < ActiveRecord::Base
  self.table_name = "adversarial_resources"
  # Here `created_at` is an attribute from the corpus. It is not the time of the creation of
  # the row. Without this line, the automatic timestamps of ActiveRecord would write over the
  # value from the corpus. That includes the NULL on a3, which the three-valued-logic tests
  # need.
  self.record_timestamps = false
  has_many :tags, class_name: "AdvTag", foreign_key: :resource_id, primary_key: :id
  has_many :categories, class_name: "AdvCategory", foreign_key: :resource_id, primary_key: :id
  # A to-ONE association, so the adapter maps `parent.<column>` as a path with dots and makes a
  # correlated scalar subquery. A `has_many` here would make the adapter refuse the mapping,
  # which is the point of the unique index on the foreign key.
  has_one :parent, class_name: "AdvParent", foreign_key: :resource_id, primary_key: :id
end
