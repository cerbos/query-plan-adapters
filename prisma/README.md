# Cerbos + Prisma ORM Adapter

An adapater library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into a [Prisma](https://prisma.io) where class object. This is designed to work alongside a project using the [Cerbos Javascript SDK](https://github.com/cerbos/cerbos-sdk-javascript).

The following conditions are supported: `and`, `or`, `eq`, `ne`, `lt`, `gt`, `lte`, `gte` and `in`.

## Requirements
- Cerbos > v0.16

## Usage

```
npm install @cerbos/orm-prisma
```

This package exports a single function:

```js
queryPlanToPrisma({ queryPlan, fieldNameMapper }): PrismaCondition
```

The function reqiures the full query plan from Cerbos to be passed in an object along with a `fieldNameMapper`.

The `fieldNameMapper` is used to convert the field names in the query plan response to names of fields in the Prisma model - this can be done as a map or a function:

```js
{
  "request.resource.attr.aFieldName": "prismaModelFieldName"
}

//or

(fieldName: string): string => {
  if(fieldName.indexOf("request.resource.") > 0) {
    return fieldName.replace("request.resource.attr", "")
  }

  if(fieldName.indexOf("request.principal.") > 0) {
    return fieldName.replace("request.principal.attr", "")
  }
}
```

## Full Example

A full Prisma application making use of this adapater can be found at [https://github.com/cerbos/express-prisma-cerbos](https://github.com/cerbos/express-prisma-cerbos)