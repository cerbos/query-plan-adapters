# frozen_string_literal: true

require "time"

# The conformance dataset in the store under test (spec/support/database.rb): one table per
# attribute shape, able to hold every hostile corpus row (NULL elements, duplicate or mirrored
# names, LIKE metacharacters, empty strings and empty collections). conformance/README.md,
# "The dataset", says what each row must hold.
#
# The schema is created when this file is loaded, and not in a method, because a Sequel::Model
# class reads the columns of its table when it is defined. It is created on whichever store
# ADAPTER_TEST_DB chose (spec/support/database.rb), with that store's table options.
module ConformanceStore
  DB = Database::DB

  DB.create_table!(:adversarial_resources, **Database::TABLE_OPTIONS) do
    String :id, primary_key: true
    # Nullable: seeds j1, j2 and j3 each leave one of these NULL, a missing attribute (#488).
    TrueClass :a_bool
    String :a_string
    Integer :a_number
    Float :a_double
    String :a_optional_string
    String :created_by, null: false
    String :scope
    column :created_at, Database::TIMESTAMP_TYPE
    column :updated_at, Database::TIMESTAMP_TYPE
  end

  # The one REAL to-one relation of the corpus (ADR 0005). `parent` and `parent.inner` are
  # separate rows that a join reaches. `obj.inner` looks the same in a policy and is not a
  # join at all: every harness maps it to the `a_string` column of the row itself.
  #
  # The unique index on the foreign key is what makes each level to-ONE. Without it, Sequel
  # would still accept the one_to_one association and the adapter would make a subquery that
  # can give more than one row.
  DB.create_table!(:adversarial_parents, **Database::TABLE_OPTIONS) do
    String :id, primary_key: true
    TrueClass :a_bool, null: false
    String :a_string, null: false
    Integer :a_number, null: false
    String :a_optional_string
    String :resource_id, null: false, unique: true
  end

  DB.create_table!(:adversarial_inners, **Database::TABLE_OPTIONS) do
    String :id, primary_key: true
    TrueClass :a_bool, null: false
    String :a_string, null: false
    Integer :a_number, null: false
    String :a_optional_string
    String :parent_id, null: false, unique: true
  end

  DB.create_table!(:adversarial_tags, **Database::TABLE_OPTIONS) do
    primary_key :id
    String :tag_id, null: false
    String :name
    String :resource_id, null: false
  end

  # `aNumberList` and `aBoolList`, the homogeneous scalar lists, as child rows the way a
  # relational schema holds a list: one row per element, a nullable value (a null element is a
  # value, as it is in `tagNames`), and the element's position. The position is the one thing
  # `aNumberList[0]` needs and the one thing an association mapping cannot carry, which is why
  # every corpus case that indexes these lists is refused at `index`.
  DB.create_table!(:adversarial_number_list_elements, **Database::TABLE_OPTIONS) do
    primary_key :id
    Integer :position, null: false
    Float :value
    String :resource_id, null: false
  end

  DB.create_table!(:adversarial_bool_list_elements, **Database::TABLE_OPTIONS) do
    primary_key :id
    Integer :position, null: false
    TrueClass :value
    String :resource_id, null: false
  end

  DB.create_table!(:adversarial_categories, **Database::TABLE_OPTIONS) do
    String :id, primary_key: true
    String :name, null: false
    String :resource_id, null: false
  end

  DB.create_table!(:adversarial_sub_categories, **Database::TABLE_OPTIONS) do
    String :id, primary_key: true
    String :name, null: false
    String :category_id, null: false
  end

  DB.create_table!(:adversarial_labels, **Database::TABLE_OPTIONS) do
    String :id, primary_key: true
    String :name
    String :sub_category_id, null: false
  end

  module_function

  def establish!
    return if @established
    @established = true

    verify_mysql_collation! if Database::STORE == "mysql"
    seed!
  end

  # A column or a session left on MySQL's default collation would pass the case-sensitivity
  # cases only where no seed happens to discriminate, so check both before any case runs.
  def verify_mysql_collation!
    columns = DB.fetch(<<~SQL).map(:collation_name)
      SELECT DISTINCT collation_name AS collation_name FROM information_schema.columns
      WHERE table_schema = DATABASE() AND collation_name IS NOT NULL
    SQL
    session = DB.get(::Sequel.lit("@@collation_connection"))
    return if columns == [Database::MYSQL_COLLATION] && session == Database::MYSQL_COLLATION

    raise "MySQL must run on #{Database::MYSQL_COLLATION}: columns #{columns}, session #{session}"
  end

  # Each row gets its own category graph, so no two resources share relation rows.
  #
  # The rows go in through the datasets and not through the models: the models exist for the
  # associations the adapter reads, and a model's typecasting or hooks must not be what decides
  # which value reaches a column.
  def seed!
    ConformanceCorpus::SEEDS.each do |seed|
      id = seed.fetch("id")

      DB[:adversarial_resources].insert(
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

      # The to-one chain, with one owned row for each level. A seed with no parent gets no row
      # at all, and that is what makes the absent-parent hazard possible through a SCALAR and
      # not only through the collection of mainCategory.
      parent_seed = ConformanceCorpus.parent_seed_of(seed)
      if parent_seed
        parent_id = "#{id}-parent"
        DB[:adversarial_parents].insert(
          id: parent_id,
          a_bool: parent_seed.fetch("aBool"),
          a_string: parent_seed.fetch("aString"),
          a_number: parent_seed.fetch("aNumber"),
          a_optional_string: parent_seed.fetch("aOptionalString"),
          resource_id: id
        )

        inner_seed = ConformanceCorpus.parent_seed_of(parent_seed)
        if inner_seed
          DB[:adversarial_inners].insert(
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
        DB[:adversarial_tags].insert(tag_id: tag.fetch("id"), name: tag.fetch("name"), resource_id: id)
      end

      seed.fetch("aNumberList").each_with_index do |value, position|
        DB[:adversarial_number_list_elements].insert(position: position, value: value, resource_id: id)
      end
      seed.fetch("aBoolList").each_with_index do |value, position|
        DB[:adversarial_bool_list_elements].insert(position: position, value: value, resource_id: id)
      end

      # One category holding every subcategory name (conformance/README.md, "The dataset").
      sub_names = seed.fetch("subCategoryNames")
      category_id = "#{id}-cat"
      unless sub_names.empty?
        DB[:adversarial_categories].insert(id: category_id, name: "business", resource_id: id)
      end
      sub_names.each_with_index do |sub_name, index|
        sub_category_id = "#{id}-sub#{index}"
        DB[:adversarial_sub_categories].insert(
          id: sub_category_id, name: sub_name, category_id: category_id
        )
        ConformanceCorpus.derived(seed, "labels").each_with_index do |label_name, label_index|
          DB[:adversarial_labels].insert(
            id: "#{id}-label#{index}-#{label_index}",
            name: label_name,
            sub_category_id: sub_category_id
          )
        end
      end
    end
  end
end

class AdvLabel < Sequel::Model(Database::DB[:adversarial_labels]); end

class AdvSubCategory < Sequel::Model(Database::DB[:adversarial_sub_categories])
  one_to_many :labels, class: :AdvLabel, key: :sub_category_id
end

class AdvCategory < Sequel::Model(Database::DB[:adversarial_categories])
  one_to_many :sub_categories, class: :AdvSubCategory, key: :category_id
end

class AdvTag < Sequel::Model(Database::DB[:adversarial_tags]); end

class AdvNumberListElement < Sequel::Model(Database::DB[:adversarial_number_list_elements]); end

class AdvBoolListElement < Sequel::Model(Database::DB[:adversarial_bool_list_elements]); end

class AdvInner < Sequel::Model(Database::DB[:adversarial_inners]); end

class AdvParent < Sequel::Model(Database::DB[:adversarial_parents])
  one_to_one :inner, class: :AdvInner, key: :parent_id
end

class AdvResource < Sequel::Model(Database::DB[:adversarial_resources])
  one_to_many :tags, class: :AdvTag, key: :resource_id
  one_to_many :number_list_elements, class: :AdvNumberListElement, key: :resource_id
  one_to_many :bool_list_elements, class: :AdvBoolListElement, key: :resource_id
  one_to_many :categories, class: :AdvCategory, key: :resource_id
  # A to-ONE association, so the adapter maps `parent.<column>` as a path with dots and makes a
  # correlated scalar subquery. A one_to_many here would make the adapter refuse the mapping,
  # which is the point of the unique index on the foreign key.
  one_to_one :parent, class: :AdvParent, key: :resource_id
end
