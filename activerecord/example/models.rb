# frozen_string_literal: true

require "active_record"

# The database, the models and the data of the example.
#
# The data is small and always the same. Thus the smoke tests can compare the identifiers of
# the rows exactly.
module Store
  # A database in a file, and not a database in memory. Each connection to a database in
  # memory gets an empty database of its own. Thus the threads of the web server could not
  # read the rows.
  DB_PATH = ENV.fetch("DATABASE_PATH", "/tmp/cerbos-example.sqlite3")

  module_function

  def setup!
    File.delete(DB_PATH) if File.exist?(DB_PATH)

    ActiveRecord::Base.establish_connection(
      adapter: "sqlite3",
      database: DB_PATH,
      pool: 5,
      # CEL compares strings with attention to the case of the letters. The LIKE operator of
      # SQLite does not do this with its default configuration. The hierarchy rule in
      # workspace.yaml becomes a LIKE. Thus this PRAGMA is part of the policy contract.
      #
      # ActiveRecord applies these pragmas to each new connection in the pool.
      pragmas: {"case_sensitive_like" => "ON"}
    )

    define_schema!
    seed!
    assert_case_sensitive_like!
  end

  # Stops the application if the PRAGMA above did not apply. Without it, the LIKE operator
  # ignores the case of the letters, and thus the policies select more rows than they permit.
  # A silent failure of a collation setting is an authorization bug.
  def assert_case_sensitive_like!
    ActiveRecord::Base.connection_pool.with_connection do |connection|
      next if connection.select_value("SELECT 'A' LIKE 'a'").to_i.zero?

      raise "PRAGMA case_sensitive_like is not in effect: the LIKE operator ignores the " \
            "case of the letters, and thus the policies would permit more rows than they must"
    end
  end

  def define_schema!
    ActiveRecord::Schema.verbose = false
    ActiveRecord::Schema.define do
      create_table :users, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
        t.string :department, null: false
        t.string :tenant, null: false
      end

      create_table :workspaces, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
        t.string :tenant, null: false
        t.string :owner_id, null: false
        t.string :scope, null: false
      end

      create_table :albums, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :title, null: false
        t.string :tenant, null: false
        t.string :owner_id, null: false
        t.string :workspace_id, null: false
        t.boolean :shared, null: false, default: false
      end

      create_table :album_collaborators, force: true do |t|
        t.string :album_id, null: false
        t.string :user_id, null: false
      end

      create_table :photos, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :title, null: false
        t.string :tenant, null: false
        t.string :owner_id, null: false
        t.string :album_id, null: false
        t.boolean :published, null: false, default: false
      end

      create_table :tags, id: false, force: true do |t|
        t.string :id, null: false, primary_key: true
        t.string :name, null: false
      end

      create_table :photo_tags, force: true do |t|
        t.string :photo_id, null: false
        t.string :tag_id, null: false
      end
    end
  end

  def seed!
    User.create!(id: "ana", name: "Ana", department: "engineering", tenant: "acme")
    User.create!(id: "ben", name: "Ben", department: "sales", tenant: "acme")
    User.create!(id: "cara", name: "Cara", department: "engineering", tenant: "globex")

    Workspace.create!(id: "w-platform", name: "Platform", tenant: "acme",
      owner_id: "ana", scope: "acme.engineering.platform")
    Workspace.create!(id: "w-sales", name: "Sales", tenant: "acme",
      owner_id: "ben", scope: "acme.sales")
    Workspace.create!(id: "w-globex", name: "Globex Engineering", tenant: "globex",
      owner_id: "cara", scope: "globex.engineering")

    Album.create!(id: "al-launch", title: "Launch", tenant: "acme",
      owner_id: "ana", workspace_id: "w-platform", shared: false)
    Album.create!(id: "al-team", title: "Team", tenant: "acme",
      owner_id: "ben", workspace_id: "w-sales", shared: true)
    Album.create!(id: "al-secret", title: "Secret", tenant: "globex",
      owner_id: "cara", workspace_id: "w-globex", shared: false)

    AlbumCollaborator.create!(album_id: "al-launch", user_id: "ben")

    Tag.create!(id: "t-public", name: "public")
    Tag.create!(id: "t-internal", name: "internal")

    Photo.create!(id: "ph-hero", title: "Hero", tenant: "acme",
      owner_id: "ana", album_id: "al-launch", published: false)
    Photo.create!(id: "ph-banner", title: "Banner", tenant: "acme",
      owner_id: "ana", album_id: "al-launch", published: true)
    Photo.create!(id: "ph-team", title: "Team", tenant: "acme",
      owner_id: "ben", album_id: "al-team", published: false)
    Photo.create!(id: "ph-draft", title: "Draft", tenant: "acme",
      owner_id: "ben", album_id: "al-team", published: false)
    Photo.create!(id: "ph-globex", title: "Globex", tenant: "globex",
      owner_id: "cara", album_id: "al-secret", published: true)

    PhotoTag.create!(photo_id: "ph-hero", tag_id: "t-internal")
    PhotoTag.create!(photo_id: "ph-banner", tag_id: "t-public")
    PhotoTag.create!(photo_id: "ph-team", tag_id: "t-public")
    PhotoTag.create!(photo_id: "ph-team", tag_id: "t-internal")
  end
end

class User < ActiveRecord::Base
  self.table_name = "users"
end

class Workspace < ActiveRecord::Base
  self.table_name = "workspaces"
  belongs_to :owner, class_name: "User", foreign_key: :owner_id
end

class AlbumCollaborator < ActiveRecord::Base
  self.table_name = "album_collaborators"
  belongs_to :user, class_name: "User", foreign_key: :user_id
end

class Album < ActiveRecord::Base
  self.table_name = "albums"
  belongs_to :owner, class_name: "User", foreign_key: :owner_id
  has_many :album_collaborators, foreign_key: :album_id, primary_key: :id
  has_many :collaborators, through: :album_collaborators, source: :user
end

class Tag < ActiveRecord::Base
  self.table_name = "tags"
end

class PhotoTag < ActiveRecord::Base
  self.table_name = "photo_tags"
  belongs_to :tag, foreign_key: :tag_id
end

class Photo < ActiveRecord::Base
  self.table_name = "photos"
  belongs_to :owner, class_name: "User", foreign_key: :owner_id
  belongs_to :album, foreign_key: :album_id
  has_many :photo_tags, foreign_key: :photo_id, primary_key: :id
  has_many :tags, through: :photo_tags, source: :tag
end
