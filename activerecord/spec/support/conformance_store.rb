# frozen_string_literal: true

require "time"

# The conformance dataset in the store under test (spec/support/database.rb): one table per
# attribute shape, able to hold every hostile corpus row (NULL elements, duplicate or mirrored
# names, LIKE metacharacters, empty strings and empty collections). conformance/README.md,
# "The dataset", says what each row must hold.
module ConformanceStore
  module_function

  def establish!
    return if @established
    @established = true

    Database.establish!
    define_schema!
    verify_mysql_collation! if Database::STORE == "mysql"
    seed!
  end

  # A column or a session left on MySQL's default collation would pass the case-sensitivity
  # cases only where no seed happens to discriminate, so check both before any case runs.
  def verify_mysql_collation!
    connection = ActiveRecord::Base.connection
    columns = connection.select_values(<<~SQL)
      SELECT DISTINCT collation_name FROM information_schema.columns
      WHERE table_schema = DATABASE() AND collation_name IS NOT NULL
    SQL
    session = connection.select_value("SELECT @@collation_connection")
    return if columns == [Database::MYSQL_COLLATION] && session == Database::MYSQL_COLLATION

    raise "MySQL must run on #{Database::MYSQL_COLLATION}: columns #{columns}, session #{session}"
  end

  def define_schema!
    ActiveRecord::Schema.verbose = false
    ActiveRecord::Schema.define do
      create_table :adversarial_resources, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        # Nullable: seeds j1, j2 and j3 each leave one of these NULL, a missing attribute (#488).
        t.boolean :a_bool
        t.string :a_string
        t.integer :a_number
        # limit 53: a double. MySQL makes a bare `float` a 4-byte single.
        t.float :a_double, limit: 53
        t.string :a_optional_string
        t.string :created_by, null: false
        t.string :scope
        t.datetime :created_at
        t.datetime :updated_at
      end

      # The corpus's one real to-one relation (ADR 0005): `parent` and `parent.inner` are
      # joined rows. (`obj.inner` looks similar but maps to the row's own `a_string`.)
      #
      # The unique foreign-key index makes each level to-one. Without it the subquery could
      # return more than one row.
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

      # `aNumberList` and `aBoolList` as child rows: one per element, with a nullable value
      # and a position. A relation mapping cannot use the position, so every case indexing these
      # lists (e.g. `aNumberList[0]`) is refused at `index`.
      create_table :adversarial_number_list_elements, force: true do |t|
        t.integer :position, null: false
        t.float :value, limit: 53
        t.string :resource_id, null: false
      end

      create_table :adversarial_bool_list_elements, force: true do |t|
        t.integer :position, null: false
        t.boolean :value
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

  # Each row gets its own category graph (one category per sub-name), so no two resources
  # share relation rows.
  def seed!
    ConformanceCorpus::SEEDS.each do |seed|
      id = seed.fetch("id")

      AdvResource.create!(
        id: id,
        a_bool: seed.fetch("aBool"),
        a_string: seed.fetch("aString"),
        a_number: seed.fetch("aNumber"),
        a_double: ConformanceCorpus.derived(seed, "aDouble"),
        a_optional_string: seed.fetch("aOptionalString"),
        created_by: ConformanceCorpus.derived(seed, "createdBy"),
        scope: ConformanceCorpus.derived(seed, "scope"),
        created_at: ConformanceCorpus.derived(seed, "createdAt")&.then { |iso| Time.iso8601(iso) },
        updated_at: ConformanceCorpus.derived(seed, "updatedAt")&.then { |iso| Time.iso8601(iso) }
      )

      # One row per level. A seed with no parent gets no row, which tests the absent-parent
      # case through a scalar, not only through mainCategory.
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

      seed.fetch("aNumberList").each_with_index do |value, position|
        AdvNumberListElement.create!(position: position, value: value, resource_id: id)
      end
      seed.fetch("aBoolList").each_with_index do |value, position|
        AdvBoolListElement.create!(position: position, value: value, resource_id: id)
      end

      # One category holding every subcategory name (conformance/README.md, "The dataset").
      sub_names = seed.fetch("subCategoryNames")
      category = AdvCategory.create!(id: "#{id}-cat", name: "business", resource_id: id) unless sub_names.empty?
      sub_names.each_with_index do |sub_name, index|
        sub_category = AdvSubCategory.create!(
          id: "#{id}-sub#{index}", name: sub_name, category_id: category.id
        )
        ConformanceCorpus.derived(seed, "labels").each_with_index do |label_name, label_index|
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

class AdvNumberListElement < ActiveRecord::Base
  self.table_name = "adversarial_number_list_elements"
end

class AdvBoolListElement < ActiveRecord::Base
  self.table_name = "adversarial_bool_list_elements"
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
  # `created_at` is corpus data. Automatic timestamps would overwrite it, including the NULL
  # on a3 that the three-valued-logic tests need.
  self.record_timestamps = false
  has_many :tags, class_name: "AdvTag", foreign_key: :resource_id, primary_key: :id
  has_many :number_list_elements,
    class_name: "AdvNumberListElement", foreign_key: :resource_id, primary_key: :id
  has_many :bool_list_elements,
    class_name: "AdvBoolListElement", foreign_key: :resource_id, primary_key: :id
  has_many :categories, class_name: "AdvCategory", foreign_key: :resource_id, primary_key: :id
  # To-one, so `parent.<column>` becomes a scalar subquery. The adapter refuses a `has_many`.
  has_one :parent, class_name: "AdvParent", foreign_key: :resource_id, primary_key: :id
end
