# frozen_string_literal: true

# The demo-domain example application for the ActiveRecord adapter.
#
# It proves PLUMBING, not semantics: that the published gem installs, that `require "cerbos/
# active_record"` resolves from it, and that the relation it returns composes with the query
# methods a consumer actually reaches for — `where`, `count`, `order`, `offset`/`limit`. The
# conformance harness cannot show any of that, because it loads the adapter from source and only
# ever runs one flat filtered query.
#
# Every row, every principal and the application's own predicate come from demo/seeds.json.
# Nothing about the domain is written down twice — see demo/README.md.

require "json"

require "active_record"
require "cerbos"
require "cerbos/active_record"

# stdout carries the JSON document and nothing else, so anything ActiveRecord or a gem decides
# to print has to go elsewhere. Captured before the swap, exactly as spring-data's example does.
REAL_STDOUT = $stdout.dup
$stdout = $stderr

DEMO_DIR = ENV.fetch("DEMO_DIR") {
  File.expand_path("../../demo", __dir__)
}

# No fallback. The PDP address belongs to whoever started the PDP — demo/scripts/run-example.sh
# — and a default here would let the example pass against something nobody meant to test.
CERBOS_HOST = ENV.fetch("CERBOS_HOST") {
  raise "CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh activerecord"
}

# Read as UTF-8 explicitly. The corpus carries non-ASCII prose, and Ruby falls back to the
# locale's encoding — which on a CI runner with no LANG set is US-ASCII.
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

# The mapper. Cerbos attribute names are not column names — `public` is a reserved-ish word in
# more than one database and this schema calls it `is_public`, which is precisely the mismatch a
# mapper exists to absorb.
ATTRIBUTES = {
  "request.resource.attr.ownerId" => Cerbos::ActiveRecord.field("owner_id"),
  "request.resource.attr.public" => Cerbos::ActiveRecord.field("is_public")
}.freeze

CLIENT = Cerbos::Client.new(CERBOS_HOST, tls: false)

# Looked up in the corpus, never restated here: an inline `{id: "alice", roles: ["user"]}` is a
# second copy of demo/seeds.json's principals array, and the first divergence would read as an
# adapter quirk rather than the bug it is.
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

# The application's OWN predicate, from the corpus. It is never expressed in policy, which is
# the whole point of usage shape 5.
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

# 4. Pagination. The relation is ordered and walked a page at a time, and what is reported is
#    the page sizes plus the SORTED UNION of the ids — never the per-page order, which is a
#    property of the ORDER BY rather than of the authorization filter.
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

# 5. The load-bearing one: the adapter's filter ANDed with the application's own predicate.
#
#    An ALWAYS_DENIED plan still runs its query here rather than short-circuiting on the plan
#    kind. Skipping the database is a supported optimisation, but executing the denial together
#    with the application's predicate is what actually shows that the application's own `where`
#    cannot resurrect a denied row.
def composed(principal_id, action)
  result = plan(principal_id, action)
  {"kind" => result.kind.to_s, "ids" => ids(application_filter(authorized(result)))}
end

shapes = {
  "filtered" => {
    "alice/view" => filtered("alice", "view"),
    "bob/view" => filtered("bob", "view")
  },
  # 2. An unconditional allow. The adapter returns the whole relation, and every seed row comes
  #    back through the same code path as a conditional plan.
  "alwaysAllowed" => {
    "admin/admin-view" => filtered("admin", "admin-view")
  },
  # 3. An unconditional deny, for an action the policy carries no rule for at all.
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
