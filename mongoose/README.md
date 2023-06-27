# Cerbos + Mongoose ORM Adapter

An adapater library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Mongoose](https://mongoosejs.com/) filter. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

The following conditions are supported: `and`, `or`, `eq`, `ne`, `lt`, `gt`, `lte`, `gte` and `in`, as well as relational filters `some`, `none`, `is` and `isNot`.

Not Supported:

- `every`
- `contains`
- `search`
- `mode`
- `startsWith`
- `endsWith`
- `isSet`
- Scalar filters
- Atomic number operations
- Composite keys

## Requirements

- Cerbos > v0.16
- `@cerbos/http` or `@cerbos/grpc` client

## Usage

```
npm install @cerbos/orm-mongoose
```

This package exports a function:
