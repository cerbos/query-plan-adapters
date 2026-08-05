# frozen_string_literal: true

# Schema and fixtures for the shared policy suite (/policies/resource.yaml), which every
# adapter in this repository is exercised against.
#
# Collections are modelled as explicit join tables reached through `has_many :through`, and
# the `nested` / `nextlevel` attributes as `belongs_to` chains, so the suite covers both
# relation traversals and dotted scalar paths.
module SharedModels
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
      create_table :shared_users, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.boolean :a_bool, null: false
      end

      create_table :shared_next_levels, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.boolean :a_bool, null: false
      end

      create_table :shared_nested_resources, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.boolean :a_bool, null: false
        t.string :a_optional_string
        t.string :next_level_id, null: false
      end

      create_table :shared_resources, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :a_string, null: false
        t.integer :a_number, null: false
        t.boolean :a_bool, null: false
        t.string :a_optional_string
        t.string :scope
        t.string :creator_id, null: false
        t.string :nested_resource_id, null: false
      end

      create_table :shared_tags, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
      end

      create_table :shared_categories, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
      end

      create_table :shared_sub_categories, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
      end

      create_table :shared_labels, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
      end

      create_table :shared_resource_owners, force: true do |t|
        t.string :resource_id, null: false
        t.string :user_id, null: false
      end

      create_table :shared_resource_tags, force: true do |t|
        t.string :resource_id, null: false
        t.string :tag_id, null: false
      end

      create_table :shared_resource_categories, force: true do |t|
        t.string :resource_id, null: false
        t.string :category_id, null: false
      end

      create_table :shared_category_sub_categories, force: true do |t|
        t.string :category_id, null: false
        t.string :sub_category_id, null: false
      end

      create_table :shared_sub_category_labels, force: true do |t|
        t.string :sub_category_id, null: false
        t.string :label_id, null: false
      end
    end
  end

  # One row per interesting combination, chosen so that most policy actions allow some
  # resources and deny others — a suite where every oracle came back empty would pass
  # without proving anything.
  FIXTURES = [
    {
      id: "507f1f77bcf86cd799439011", a_string: "string", a_number: 1, a_bool: true,
      a_optional_string: "optional", scope: "acme.dept.eng", creator_id: "user1",
      owners: %w[user1 user2], tags: %w[tag1 tag2],
      nested: "nested1", categories: %w[cat-business]
    },
    {
      id: "resource2", a_string: "anotherString", a_number: 2, a_bool: false,
      a_optional_string: nil, scope: "acme.dept", creator_id: "user2",
      owners: %w[user2], tags: %w[tag2],
      nested: "nested2", categories: %w[cat-tech]
    },
    {
      id: "resource3", a_string: "string", a_number: 3, a_bool: true,
      a_optional_string: "another", scope: "acme.dept.eng.platform", creator_id: "user1",
      owners: [], tags: [],
      nested: "nested1", categories: []
    },
    {
      id: "resource4", a_string: "prefix:resource4", a_number: 1, a_bool: true,
      a_optional_string: nil, scope: "other", creator_id: "user3",
      owners: %w[user1], tags: %w[tag1],
      nested: "nested3", categories: %w[cat-business cat-tech]
    },
    {
      # aString == id, so the field-to-field probes discriminate.
      id: "resource5", a_string: "resource5", a_number: 4, a_bool: false,
      a_optional_string: "set", scope: nil, creator_id: "user2",
      owners: %w[user1 user2 user3], tags: %w[tag1],
      nested: "nested2", categories: %w[cat-business]
    },
    {
      id: "resource6", a_string: "12", a_number: 5, a_bool: true,
      a_optional_string: nil, scope: "acme", creator_id: "user3",
      owners: %w[user3], tags: %w[tag3],
      nested: "nested3", categories: []
    }
  ].freeze

  def seed!
    SharedNextLevel.create!(id: "next1", a_string: "string next", a_number: 1, a_bool: true)
    SharedNextLevel.create!(id: "next2", a_string: "other next", a_number: 2, a_bool: false)

    SharedNested.create!(id: "nested1", a_string: "test string", a_number: 1, a_bool: true,
      a_optional_string: "nested-optional", next_level_id: "next1")
    SharedNested.create!(id: "nested2", a_string: "other", a_number: 2, a_bool: false,
      a_optional_string: nil, next_level_id: "next2")
    SharedNested.create!(id: "nested3", a_string: "test string three", a_number: 3, a_bool: true,
      a_optional_string: "third", next_level_id: "next1")

    %w[user1 user2 user3].each_with_index do |id, index|
      SharedUser.create!(id: id, a_string: id, a_number: index + 1, a_bool: index.even?)
    end

    {"tag1" => "public", "tag2" => "draft", "tag3" => "private"}.each do |id, name|
      SharedTag.create!(id: id, name: name)
    end

    SharedLabel.create!(id: "label-important", name: "important")
    SharedLabel.create!(id: "label-minor", name: "minor")

    SharedSubCategory.create!(id: "sub-finance", name: "finance")
    SharedSubCategory.create!(id: "sub-devops", name: "devops")
    SharedSubCategoryLabel.create!(sub_category_id: "sub-finance", label_id: "label-important")
    SharedSubCategoryLabel.create!(sub_category_id: "sub-devops", label_id: "label-minor")

    SharedCategory.create!(id: "cat-business", name: "business")
    SharedCategory.create!(id: "cat-tech", name: "tech")
    SharedCategorySubCategory.create!(category_id: "cat-business", sub_category_id: "sub-finance")
    SharedCategorySubCategory.create!(category_id: "cat-tech", sub_category_id: "sub-devops")

    FIXTURES.each do |fixture|
      SharedResource.create!(
        id: fixture[:id],
        a_string: fixture[:a_string],
        a_number: fixture[:a_number],
        a_bool: fixture[:a_bool],
        a_optional_string: fixture[:a_optional_string],
        scope: fixture[:scope],
        creator_id: fixture[:creator_id],
        nested_resource_id: fixture[:nested]
      )
      fixture[:owners].each { |user| SharedResourceOwner.create!(resource_id: fixture[:id], user_id: user) }
      fixture[:tags].each { |tag| SharedResourceTag.create!(resource_id: fixture[:id], tag_id: tag) }
      fixture[:categories].each do |category|
        SharedResourceCategory.create!(resource_id: fixture[:id], category_id: category)
      end
    end
  end
end

class SharedUser < ActiveRecord::Base
  self.table_name = "shared_users"
end

class SharedNextLevel < ActiveRecord::Base
  self.table_name = "shared_next_levels"
end

class SharedNested < ActiveRecord::Base
  self.table_name = "shared_nested_resources"
  belongs_to :nextlevel, class_name: "SharedNextLevel", foreign_key: :next_level_id
end

class SharedTag < ActiveRecord::Base
  self.table_name = "shared_tags"
end

class SharedLabel < ActiveRecord::Base
  self.table_name = "shared_labels"
end

class SharedSubCategoryLabel < ActiveRecord::Base
  self.table_name = "shared_sub_category_labels"
  belongs_to :label, class_name: "SharedLabel", foreign_key: :label_id
end

class SharedSubCategory < ActiveRecord::Base
  self.table_name = "shared_sub_categories"
  has_many :sub_category_labels, class_name: "SharedSubCategoryLabel",
    foreign_key: :sub_category_id, primary_key: :id
  has_many :labels, through: :sub_category_labels, source: :label
end

class SharedCategorySubCategory < ActiveRecord::Base
  self.table_name = "shared_category_sub_categories"
  belongs_to :sub_category, class_name: "SharedSubCategory", foreign_key: :sub_category_id
end

class SharedCategory < ActiveRecord::Base
  self.table_name = "shared_categories"
  has_many :category_sub_categories, class_name: "SharedCategorySubCategory",
    foreign_key: :category_id, primary_key: :id
  has_many :sub_categories, through: :category_sub_categories, source: :sub_category
end

class SharedResourceOwner < ActiveRecord::Base
  self.table_name = "shared_resource_owners"
  belongs_to :user, class_name: "SharedUser", foreign_key: :user_id
end

class SharedResourceTag < ActiveRecord::Base
  self.table_name = "shared_resource_tags"
  belongs_to :tag, class_name: "SharedTag", foreign_key: :tag_id
end

class SharedResourceCategory < ActiveRecord::Base
  self.table_name = "shared_resource_categories"
  belongs_to :category, class_name: "SharedCategory", foreign_key: :category_id
end

class SharedResource < ActiveRecord::Base
  self.table_name = "shared_resources"
  belongs_to :nested, class_name: "SharedNested", foreign_key: :nested_resource_id
  belongs_to :created_by, class_name: "SharedUser", foreign_key: :creator_id

  has_many :resource_owners, class_name: "SharedResourceOwner", foreign_key: :resource_id, primary_key: :id
  has_many :owned_by, through: :resource_owners, source: :user

  has_many :resource_tags, class_name: "SharedResourceTag", foreign_key: :resource_id, primary_key: :id
  has_many :tags, through: :resource_tags, source: :tag

  has_many :resource_categories, class_name: "SharedResourceCategory", foreign_key: :resource_id, primary_key: :id
  has_many :categories, through: :resource_categories, source: :category

  # The flattened two-hop chain `R.attr.categories.subCategories` addresses.
  has_many :sub_categories, through: :categories, source: :sub_categories
end
