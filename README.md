# Cerbos Query Plan Adapters

These are reference implementations of adapters that take a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and convert it into a filter which can be applied to your data fetching layer to return just the instances of a resource that a user would have access to.

Current supported adapters:

- [Drizzle ORM](https://github.com/cerbos/query-plan-adapters/tree/main/drizzle)
- [Prisma](https://github.com/cerbos/query-plan-adapters/tree/main/prisma)
- [SQLAlchemy](https://github.com/cerbos/query-plan-adapters/tree/main/sqlalchemy)
- [Mongoose](https://github.com/cerbos/query-plan-adapters/tree/main/mongoose)
