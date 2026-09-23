# frozen_string_literal: true

# Demo-domain example app for the ActiveRecord adapter.
#
# Tests plumbing, not semantics: the packed gem installs and loads, and its relation composes
# with `where`, `order` and `offset`/`limit`. All data comes from demo/seeds.json.

require "json"

require "active_record"
require "cerbos"
require "cerbos/active_record"

# stdout is for the JSON document only; send everything else printed to stderr.
REAL_STDOUT = $stdout.dup
$stdout = $stderr

DEMO_DIR = ENV.fetch("DEMO_DIR") {
  File.expand_path("../../demo", __dir__)
}

# No default: a fallback address could point at the wrong PDP.
CERBOS_HOST = ENV.fetch("CERBOS_HOST") {
  raise "CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh activerecord"
}

# Force UTF-8: the seeds contain non-ASCII text, and CI runners without LANG default to ASCII.
SEEDS = JSON.parse(
  File.read(File.join(DEMO_DIR, "seeds.json"), encoding: "UTF-8")
).freeze

# --- the store ------------------------------------------------------------------------------

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
ActiveRecord::Schema.verbose = false
ActiveRecord::Schema.define do
  create_table :documents, id: false, force: true do |t|
    t.string :id, primary_key: true
    t.string :owner_id
    t.boolean :is_public
    t.string :region
    t.boolean :archived
  end
end

class Document < ActiveRecord::Base
  self.primary_key = "id"
end

SEEDS.fetch("documents").each do |row|
  Document.create!(
    id: row.fetch("id"),
    owner_id: row.fetch("ownerId"),
    is_public: row.fetch("public"),
    region: row.fetch("region"),
    archived: row.fetch("archived")
  )
end

# Maps Cerbos attributes to columns. `public` is stored as `is_public`.
ATTRIBUTES = {
  "request.resource.attr.ownerId" => Cerbos::ActiveRecord.field("owner_id"),
  "request.resource.attr.public" => Cerbos::ActiveRecord.field("is_public")
}.freeze

CLIENT = Cerbos::Client.new(CERBOS_HOST, tls: false)

# Read principals from demo/seeds.json rather than copying them here.
def principal(id)
  found = SEEDS.fetch("principals").find { |candidate| candidate.fetch("id") == id }
  raise "demo/seeds.json declares no principal #{id.inspect}" if found.nil?

  {id: found.fetch("id"), roles: found.fetch("roles")}
end

def plan(principal_id, action)
  CLIENT.plan_resources(
    principal: principal(principal_id),
    resource: {kind: "document"},
    action: action
  )
end

def authorized(plan)
  Cerbos::ActiveRecord.query_plan_to_relation(
    plan: plan, model: Document, attributes: ATTRIBUTES
  )
end

def ids(relation) = relation.pluck(:id).sort

# The app's own filter, from the seeds. Not part of any policy (see usage shape 5).
def application_filter(relation)
  filter = SEEDS.fetch("applicationFilter")
  relation.where(archived: filter.fetch("archived"), region: filter.fetch("region"))
end

# --- the five usage shapes ------------------------------------------------------------------

# 1. A plain filtered list: the adapter's relation IS the query.
def filtered(principal_id, action)
  result = plan(principal_id, action)
  {"kind" => result.kind.to_s, "ids" => ids(authorized(result))}
end

# 4. Pagination. Reports page sizes and all ids sorted, not per-page order (that comes from
#    ORDER BY, not the filter).
def paginated(principal_id, action, page_size)
  result = plan(principal_id, action)
  relation = authorized(result).order(:id)

  page_sizes = []
  collected = []
  offset = 0
  loop do
    page = relation.offset(offset).limit(page_size).pluck(:id)
    break if page.empty?

    page_sizes << page.size
    collected.concat(page)
    offset += page_size
    break if page.size < page_size
  end

  {
    "kind" => result.kind.to_s,
    "ids" => collected.sort,
    "pageSize" => page_size,
    "pageSizes" => page_sizes
  }
end

# 5. The adapter's filter ANDed with the app's own filter.
#    ALWAYS_DENIED plans still run the query, to show the app's `where` can't bring back a
#    denied row. (Skipping the query is allowed in real apps.)
def composed(principal_id, action)
  result = plan(principal_id, action)
  {"kind" => result.kind.to_s, "ids" => ids(application_filter(authorized(result)))}
end

shapes = {
  "filtered" => {
    "alice/view" => filtered("alice", "view"),
    "bob/view" => filtered("bob", "view")
  },
  # 2. Always allowed: the adapter returns the whole relation.
  "alwaysAllowed" => {
    "admin/admin-view" => filtered("admin", "admin-view")
  },
  # 3. Always denied: the policy has no rule for this action.
  "alwaysDenied" => {
    "alice/publish" => filtered("alice", "publish")
  },
  "paginated" => {
    "alice/view" => paginated("alice", "view", 2),
    "admin/admin-view" => paginated("admin", "admin-view", 3)
  },
  "composed" => {
    "alice/view" => composed("alice", "view"),
    "bob/view" => composed("bob", "view"),
    "admin/admin-view" => composed("admin", "admin-view"),
    "alice/publish" => composed("alice", "publish")
  }
}

REAL_STDOUT.puts(JSON.pretty_generate({"adapter" => "activerecord", "shapes" => shapes}))
