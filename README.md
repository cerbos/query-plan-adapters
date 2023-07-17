# Cerbos Query Plan Adapters

These adapters take a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a filter which can be applied to your data fetching layer to return just the instances of a resource that a user would have access to.

Current supported adapters:

- [Prisma](https://github.com/cerbos/query-plan-adapters/tree/main/prisma)
- [SQLAlchemy](https://github.com/cerbos/query-plan-adapters/tree/main/sqlalchemy)
- [Mongoose](https://github.com/cerbos/query-plan-adapters/tree/main/mongoose)
