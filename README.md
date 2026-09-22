# Cerbos Query Plan Adapters

Turn a [Cerbos](https://cerbos.dev) query plan into a database filter, so a list query returns only
the records the current user is allowed to see.

You call Cerbos's [`PlanResources`](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)
API with a principal, a resource kind and an action. Cerbos answers with one of three plan kinds:
**always allowed**, **always denied**, or **conditional** with an expression tree. An adapter turns
that tree into the native filter for your ORM or store (a Prisma `where`, a SQL fragment, a Mongo
query, an Elasticsearch query, and so on) using a mapping you provide from Cerbos attribute paths
(`request.resource.attr.status`) to your fields.

## Pick an adapter

| Store / ORM | Language | Install | Docs |
| --- | --- | --- | --- |
| Prisma | TypeScript | `npm install @cerbos/orm-prisma @cerbos/core` | [prisma](prisma/) |
| Drizzle ORM | TypeScript | `npm install @cerbos/orm-drizzle @cerbos/core` | [drizzle](drizzle/) |
| Mongoose | TypeScript | `npm install @cerbos/orm-mongoose @cerbos/core` | [mongoose](mongoose/) |
| Convex | TypeScript | `npm install @cerbos/orm-convex @cerbos/core` | [convex](convex/) |
| LangChain.js / ChromaDB | TypeScript | `npm install @cerbos/langchain-chromadb @cerbos/core` | [langchain-chromadb](langchain-chromadb/) |
| SQLAlchemy | Python | `pip install cerbos-sqlalchemy` | [sqlalchemy](sqlalchemy/) |
| Ent | Go | `go get github.com/cerbos/query-plan-adapters/ent` | [ent](ent/) |
| pgx / PostgreSQL | Go | `go get github.com/cerbos/query-plan-adapters/pgx` | [pgx](pgx/) |
| Spring Data JPA | Java | Build from source (not yet on Maven Central) | [spring-data](spring-data/) |
| Elasticsearch | Java | Build from source (not yet on Maven Central) | [elasticsearch-java](elasticsearch-java/) |
| ActiveRecord | Ruby | Not released yet | [activerecord](activerecord/) |

Each adapter's README starts with an install step and a quick-start snippet.

> [!WARNING]
> **The ActiveRecord adapter is a work-in-progress prototype.** It is unreleased, has not been used
> in production, and its interface may change without deprecation. Do not rely on it to enforce
> access control in a live system yet.

## The shape of every integration

1. Ask Cerbos for a plan: `planResources({ principal, resource: { kind }, action })`.
2. Pass the plan and your attribute mapping to the adapter.
3. Branch on the result: return nothing for *always denied*, run the query unfiltered for
   *always allowed*, and apply the returned filter for *conditional*.

If a policy uses a shape the adapter cannot express in your store, the adapter **throws** rather than
return a filter that might over-grant. Each adapter's `Conformance contract` section lists what it
supports and what it refuses.

## How the adapters are tested

Every adapter is tested against two shared corpora in this repository:

- [`conformance/`](conformance/): hostile policy shapes and seed rows. Each adapter's filter runs
  against a real store and must return exactly the rows a real Cerbos PDP allows via `check()`.
- [`demo/`](demo/): one realistic domain. Each adapter's example app installs the **packaged**
  adapter and uses it with its ORM's real query methods. The Go adapters use a local `replace`
  directive, so their examples cover usage but not packaging.

Contributors: start with [CLAUDE.md](CLAUDE.md) and [conformance/README.md](conformance/README.md).

## License

[Apache 2.0](LICENSE)
