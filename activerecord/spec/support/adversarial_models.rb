# frozen_string_literal: true

require "time"

# Dedicated tables so every hostile seed is representable: NULL element columns, duplicate
# and mirrored names, LIKE metacharacters, empty strings and empty collections.
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

  # Distinct category graphs per seed (one category per sub-name, the same shape the prisma
  # and sqlalchemy reference harnesses seed) so that no two resources share relation rows.
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

class AdvResource < ActiveRecord::Base
  self.table_name = "adversarial_resources"
  # `created_at` is a corpus attribute here, not a row-creation timestamp: without this,
  # ActiveRecord's automatic timestamps would overwrite the seeded value — including the
  # deliberate NULL on a3 that the three-valued-logic probes discriminate on.
  self.record_timestamps = false
  has_many :tags, class_name: "AdvTag", foreign_key: :resource_id, primary_key: :id
  has_many :categories, class_name: "AdvCategory", foreign_key: :resource_id, primary_key: :id
  # The flattened two-hop chain the `mainCategory.*` attributes address: one correlated
  # subquery joining through the intermediate category, not an EXISTS inside an EXISTS
  # (which would count leaf rows per-category rather than per-resource).
  has_many :sub_categories, through: :categories, source: :sub_categories
end
