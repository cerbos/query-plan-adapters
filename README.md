# Cerbos Query Plan Adapters

These are reference implementations of adapters that take a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and convert it into a filter which can be applied to your data fetching layer to return just the instances of a resource that a user would have access to.

Current supported adapters:

- [Convex](https://github.com/cerbos/query-plan-adapters/tree/main/convex)
- [Drizzle ORM](https://github.com/cerbos/query-plan-adapters/tree/main/drizzle)
- [Elasticsearch (Java)](https://github.com/cerbos/query-plan-adapters/tree/main/elasticsearch-java)
- [Ent (Go)](https://github.com/cerbos/query-plan-adapters/tree/main/ent)
- [LangChain / ChromaDB](https://github.com/cerbos/query-plan-adapters/tree/main/langchain-chromadb)
- [Mongoose](https://github.com/cerbos/query-plan-adapters/tree/main/mongoose)
- [pgx (Go)](https://github.com/cerbos/query-plan-adapters/tree/main/pgx)
- [Prisma](https://github.com/cerbos/query-plan-adapters/tree/main/prisma)
- [Spring Data JPA](https://github.com/cerbos/query-plan-adapters/tree/main/spring-data)
- [SQLAlchemy](https://github.com/cerbos/query-plan-adapters/tree/main/sqlalchemy)

Every adapter is proved against two shared corpora at the root of this repository:

- [`conformance/`](conformance/) — deliberately hostile shapes, proving each adapter's filter
  returns exactly the rows the PDP allows.
- [`demo/`](demo/) — one realistic domain, proving each adapter's **published package** installs,
  imports, and composes with its ORM's real query methods.
