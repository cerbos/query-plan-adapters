# frozen_string_literal: true

# The demo-domain example application for the MongoDB Ruby adapter.
#
# It proves PLUMBING, not semantics: that the published gem installs, that
# `require "cerbos/mongodb"` resolves from it, and that the filter it returns composes with the
# driver methods a consumer actually reaches for — `find`, `$and` with the application's own
# predicate, `sort`/`skip`/`limit` — and, through `require "cerbos/mongodb/mongoid"`, with a Mongoid
# criteria. Every shape is computed both ways and the two must agree. The conformance harness
# cannot show any of that, because it loads the adapter from source.
#
# Every document, every principal and the application's own predicate come from demo/seeds.json.
# Nothing about the domain is written down twice — see demo/README.md.

require "json"
require "logger"

require "cerbos"
require "cerbos/mongodb"
require "cerbos/mongodb/mongoid"
require "mongo"
require "mongoid"

# stdout carries the JSON document and nothing else, so anything a gem decides to print has to
# go elsewhere.
REAL_STDOUT = $stdout.dup
$stdout = $stderr

DEMO_DIR = ENV.fetch("DEMO_DIR") { File.expand_path("../../demo", __dir__) }

# No fallbacks. The PDP belongs to demo/scripts/run-example.sh and the server to run.sh, and a
# default here would let the example pass against something nobody meant to test.
CERBOS_HOST = ENV.fetch("CERBOS_HOST") {
  raise "CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh mongodb-ruby"
}
MONGODB_URI = ENV.fetch("MONGODB_URI") {
  raise "MONGODB_URI is not set — run this example through demo/scripts/run-example.sh mongodb-ruby"
}

# Read as UTF-8 explicitly: on a runner with no LANG set Ruby would fall back to US-ASCII.
SEEDS = JSON.parse(File.read(File.join(DEMO_DIR, "seeds.json"), encoding: "UTF-8")).freeze

# --- the store ------------------------------------------------------------------------------

Mongo::Logger.logger = Logger.new($stderr, level: Logger::WARN)
DOCUMENTS = Mongo::Client.new(MONGODB_URI, server_selection_timeout: 10)[:documents]
DOCUMENTS.drop
DOCUMENTS.insert_many(SEEDS.fetch("documents").map { |row|
  {
    "_id" => row.fetch("id"),
    "owner_id" => row.fetch("ownerId"),
    "is_public" => row.fetch("public"),
    "region" => row.fetch("region"),
    "archived" => row.fetch("archived")
  }
})

# The mapper. Cerbos attribute names are not field names: this collection calls `public`
# `is_public`, which is precisely the mismatch a mapper exists to absorb.
MAPPER = {
  "request.resource.attr.ownerId" => {field: "owner_id"},
  "request.resource.attr.public" => {field: "is_public"}
}.freeze

CLIENT = Cerbos::Client.new(CERBOS_HOST, tls: false)

# The same collection as a Mongoid model. Typed fields on purpose: Mongoid converts a query
# constant to its field's type, and Cerbos::MongoDB::Mongoid.criteria is what keeps that out.
Mongoid.configure do |config|
  config.clients.default = {uri: MONGODB_URI}
  config.logger = Logger.new($stderr, level: Logger::WARN)
end

class Document
  include Mongoid::Document

  store_in collection: "documents"
  field :_id, type: String
  field :owner_id, type: String
  field :is_public, type: Mongoid::Boolean
  field :region, type: String
  field :archived, type: Mongoid::Boolean
end

# Looked up in the corpus, never restated here.
def principal(id)
  found = SEEDS.fetch("principals").find { |candidate| candidate.fetch("id") == id }
  raise "demo/seeds.json declares no principal #{id.inspect}" if found.nil?

  {id: found.fetch("id"), roles: found.fetch("roles")}
end

def authorized(principal_id, action)
  plan = CLIENT.plan_resources(principal: principal(principal_id), resource: {kind: "document"}, action: action)
  Cerbos::MongoDB.query_plan_to_filter(plan: plan, mapper: MAPPER)
end

def ids(filter) = DOCUMENTS.find(filter, projection: {_id: 1}).map { |doc| doc.fetch("_id") }.sort

# Every shape is computed twice — through the driver and through a Mongoid criteria — and the two
# must agree before anything is printed. The printed document is the driver's.
def both(driver_ids, mongoid_criteria)
  mongoid_ids = mongoid_criteria.pluck(:_id).sort
  raise "Mongoid returned #{mongoid_ids.inspect} where the driver returned #{driver_ids.inspect}" unless mongoid_ids == driver_ids

  driver_ids
end

# The application's OWN predicate, from the corpus. It is never expressed in policy, which is the
# whole point of usage shape 5.
def application_filter
  filter = SEEDS.fetch("applicationFilter")
  {"archived" => filter.fetch("archived"), "region" => filter.fetch("region")}
end

# --- the five usage shapes ------------------------------------------------------------------

# 1. A plain filtered list: the adapter's filter IS the query.
def filtered(principal_id, action)
  result = authorized(principal_id, action)
  {"kind" => result.kind, "ids" => both(ids(result.filter), Cerbos::MongoDB::Mongoid.criteria(Document, result))}
end

# 4. Pagination. The filtered cursor is sorted and walked a page at a time, and what is reported
#    is the page sizes plus the SORTED UNION of the ids — never the per-page order, which is a
#    property of the sort rather than of the authorization filter.
def paginated(principal_id, action, page_size)
  result = authorized(principal_id, action)
  page_sizes = []
  collected = []
  offset = 0
  loop do
    page = DOCUMENTS.find(result.filter, projection: {_id: 1}).sort(_id: 1).skip(offset).limit(page_size)
      .map { |doc| doc.fetch("_id") }
    break if page.empty?

    page_sizes << page.size
    collected.concat(page)
    offset += page_size
    break if page.size < page_size
  end
  mongoid_pages = (0...page_sizes.size).map { |index|
    Cerbos::MongoDB::Mongoid.criteria(Document, result).order(_id: 1).skip(index * page_size).limit(page_size).pluck(:_id)
  }
  unless mongoid_pages.map(&:size) == page_sizes && mongoid_pages.flatten.sort == collected.sort
    raise "Mongoid paginated #{mongoid_pages.inspect} where the driver returned #{collected.inspect}"
  end
  {"kind" => result.kind, "ids" => collected.sort, "pageSize" => page_size, "pageSizes" => page_sizes}
end

# 5. The load-bearing one: the adapter's filter ANDed with the application's own predicate.
#
#    An ALWAYS_DENIED plan still runs its query here rather than short-circuiting on the kind.
#    Skipping the database is a supported optimisation (`result.always_denied?`), but running the
#    denial together with the application's predicate is what shows that the application's own
#    filter cannot resurrect a denied document.
def composed(principal_id, action)
  result = authorized(principal_id, action)
  filter = SEEDS.fetch("applicationFilter")
  mongoid = Cerbos::MongoDB::Mongoid.criteria(Document, result).where(archived: filter.fetch("archived"), region: filter.fetch("region"))
  {"kind" => result.kind, "ids" => both(ids({"$and" => [result.filter, application_filter]}), mongoid)}
end

shapes = {
  "filtered" => {
    "alice/view" => filtered("alice", "view"),
    "bob/view" => filtered("bob", "view")
  },
  # 2. An unconditional allow: the filter is `{}`, and every document comes back through the same
  #    code path as a conditional plan.
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

REAL_STDOUT.puts(JSON.pretty_generate({"adapter" => "mongodb-ruby", "shapes" => shapes}))
