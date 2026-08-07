import { beforeAll, test, expect, afterAll, describe } from "@jest/globals";
import { queryPlanToMongoose, PlanKind, Mapper } from ".";
import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanResourcesConditionalResponse,
  PlanResourcesResponse,
} from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import mongoose, { Schema, model, Types } from "mongoose";

const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

beforeAll(async () => {
  await mongoose.connect("mongodb://127.0.0.1:27017/test");
  await Resource.deleteMany({});
  for (const resource of fixtureResources) {
    await Resource.create(resource);
  }
});

afterAll(async () => {
  await mongoose.disconnect();
});

interface IResource {
  key: string;
  id: string;
  aBool: Boolean;
  aNumber: Number;
  aString: String;
  aOptionalString?: string;
  nested: {
    id: string;
    aBool: Boolean;
    aNumber: Number;
    aString: String;
  };
  tags: {
    id: string;
    name: string;
  }[];
  createdBy: {
    id: string;
    aBool: Boolean;
    aNumber: Number;
    aString: String;
  };
  ownedBy: {
    id: string;
    aBool: Boolean;
    aNumber: Number;
    aString: String;
  }[];
}

const resourceSchema = new Schema<IResource>({
  key: String,
  aBool: { type: Boolean },
  aNumber: { type: Number, required: true },
  aString: String,
  aOptionalString: { type: String, required: false },
  nested: {
    id: String,
    aBool: { type: Boolean },
    aNumber: { type: Number, required: true },
    aString: String,
  },
  tags: [
    {
      id: String,
      name: String,
    },
  ],
  createdBy: {
    id: String,
    aBool: { type: Boolean },
    aNumber: { type: Number, required: true },
    aString: String,
  },
  ownedBy: [
    {
      id: String,
      aBool: { type: Boolean },
      aNumber: { type: Number, required: true },
      aString: String,
    },
  ],
});

const Resource = model<IResource>("Resource", resourceSchema);

const fixtureResources: IResource[] = [
  {
    key: "a",
    id: "resource1",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "string",
    nested: {
      id: "nested1",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    tags: [
      {
        id: "tag1",
        name: "public",
      },
    ],
    createdBy: {
      id: "user1",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    ownedBy: [
      {
        id: "user1",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
    ],
  },
  {
    key: "b",
    id: "resource2",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    nested: {
      id: "nested2",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    tags: [
      {
        id: "tag2",
        name: "private",
      },
    ],
    createdBy: {
      id: "user2",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    ownedBy: [
      {
        id: "user2",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
    ],
  },
  {
    key: "c",
    id: "resource3",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    nested: {
      id: "nested3",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    tags: [
      {
        id: "tag1",
        name: "public",
      },
      {
        id: "tag3",
        name: "draft",
      },
    ],
    createdBy: {
      id: "user2",
      aBool: true,
      aNumber: 1,
      aString: "string",
    },
    ownedBy: [
      {
        id: "user1",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
      {
        id: "user2",
        aBool: true,
        aNumber: 1,
        aString: "string",
      },
    ],
  },
];

const allowedTagNames = new Set<string>(["public", "draft"]);
const allowedStringValues = new Set<string>(["string", "anotherString"]);

const defaultMapper: Mapper = {
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  "request.resource.attr.nested.aBool": { field: "nested.aBool" },
  "request.resource.attr.nested.aNumber": { field: "nested.aNumber" },
  "request.resource.attr.nested.aString": { field: "nested.aString" },
};

const conditionalPlan = (condition: PlanExpression): PlanResourcesResponse => ({
  kind: PlanKind.CONDITIONAL,
  condition,
  cerbosCallId: "test",
  requestId: "test",
  validationErrors: [],
  metadata: undefined,
});

const checkedConversion = (
  input: unknown,
  allowedTypes: string[],
  targetType: "string" | "double" | "long"
) => ({
  $cond: {
    if: { $in: [{ $type: input }, allowedTypes] },
    then: {
      $convert: {
        input,
        to: targetType,
        onError: null,
        onNull: null,
      },
    },
    else: null,
  },
});

// cerbos/query-plan-adapters#302. Both NULL-field conventions produce the identical
// `eq(attr, null)` wire node, so the adapter cannot infer which one the caller uses. Under
// "omitted" a NULL field carries no attribute at all, CEL raises a missing-attribute error, and
// check() denies every document — a null-matching filter would return precisely the documents
// the PDP refuses.
describe("nullAttributeRepresentation", () => {
  const mapper: Mapper = {
    "request.resource.attr.aOptionalString": { field: "aOptionalString" },
  };

  const planFor = (operator: string, value: unknown): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: new PlanExpression(operator, [
        new PlanExpressionVariable("request.resource.attr.aOptionalString"),
        new PlanExpressionValue(value as never),
      ]),
    }) as PlanResourcesResponse;

  test('"explicit" is the default and keeps the null-matching translation', () => {
    const queryPlan = planFor("eq", null);

    expect(queryPlanToMongoose({ queryPlan, mapper })).toStrictEqual(
      queryPlanToMongoose({
        queryPlan,
        mapper,
        nullAttributeRepresentation: "explicit",
      })
    );
  });

  test('"omitted" rejects eq against a null operand', () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: planFor("eq", null),
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toThrow(/missing-attribute error/);
  });

  // Conservatively rejected too: `ne` alone is aligned under "omitted", but negation is applied
  // by wrapping the built filter, so a leaf cannot see whether an enclosing `not` will flip a
  // not-null predicate back into a null-selecting one.
  test('"omitted" rejects ne against a null operand', () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: planFor("ne", null),
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toThrow(/missing-attribute error/);
  });

  test('"omitted" rejects a null element in an in-list', () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: planFor("in", ["x", null]),
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toThrow(/missing-attribute error/);
  });

  test('"omitted" leaves null-free comparisons untouched', () => {
    expect(
      queryPlanToMongoose({
        queryPlan: planFor("eq", "x"),
        mapper,
        nullAttributeRepresentation: "omitted",
      })
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aOptionalString: { $eq: "x" } },
    });
  });
});

describe("Adapter Unit Behavior", () => {
  test("maps single-object relations without elemMatch", async () => {
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: new PlanExpression("eq", [
        new PlanExpressionVariable("request.resource.attr.createdBy.id"),
        new PlanExpressionValue("user1"),
      ]),
    } as PlanResourcesResponse;

    const mapper: Mapper = {
      "request.resource.attr.createdBy": {
        relation: {
          name: "createdBy",
          type: "one",
          field: "id",
        },
      },
    };

    const result = queryPlanToMongoose({
      queryPlan,
      mapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        "createdBy.id": { $eq: "user1" },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) => resource.createdBy.id === "user1")
        .map((resource) => resource.key)
    );
  });

  test("handles hasIntersection map projection", async () => {
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: new PlanExpression("hasIntersection", [
        new PlanExpression("map", [
          new PlanExpressionVariable("request.resource.attr.tags"),
          new PlanExpression("lambda", [
            new PlanExpressionVariable("tag.name"),
            new PlanExpressionVariable("tag"),
          ]),
        ]),
        new PlanExpressionValue(["public", "draft"]),
      ]),
    } as PlanResourcesResponse;

    const mapper: Mapper = {
      "request.resource.attr.tags": {
        relation: {
          name: "tags",
          type: "many",
        },
      },
    };

    const result = queryPlanToMongoose({
      queryPlan,
      mapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        tags: {
          $elemMatch: {
            name: { $in: ["public", "draft"] },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) =>
          resource.tags.some((tag) => allowedTagNames.has(tag.name))
        )
        .map((resource) => resource.key)
    );
  });

  test("comparison with value-expression on the right side uses $expr", async () => {
    // CEL doesn't normalise operand order, so the value-producing expression
    // may appear as the *right* operand of a comparison (e.g. resource attr
    // compared to `int(other_attr)` or `(a + 1)`). The adapter must emit a
    // `$expr` in either orientation, otherwise it would fall through and try
    // to treat operators like `int`/`add` as leaf comparators.
    const queryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: new PlanExpression("eq", [
        new PlanExpressionValue(3),
        new PlanExpression("add", [
          new PlanExpressionVariable("request.resource.attr.aNumber"),
          new PlanExpressionValue(1),
        ]),
      ]),
    } as PlanResourcesResponse;

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    // Preserve source operand order. This is immaterial for `$eq`, but is
    // essential for directional comparisons such as `<` and `>=`.
    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: { $eq: [3, { $add: ["$aNumber", 1] }] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => 3 === (r.aNumber as number) + 1)
        .map((r) => r.key)
        .sort()
    );
  });
});

describe("Core Functionality", () => {
  test("always allowed", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-allow",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_ALLOWED,
    });

    const query = await Resource.find({});
    expect(query.length).toEqual(fixtureResources.length);
  });

  test("always denied", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "always-deny",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.ALWAYS_DENIED,
    });
  });
});

describe("Field Operations", () => {
  describe("Basic Field Tests", () => {
    test("conditional - eq", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aBool: {
            $eq: true,
          },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.key)
      );
    });

    test("conditional - eq - inverted order", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal",
      });
      const typeQp = queryPlan as PlanResourcesConditionalResponse;

      const condition = typeQp.condition as PlanExpression;
      const [firstOperand, secondOperand] = condition.operands;
      if (!firstOperand || !secondOperand) {
        throw new Error("Expected two operands in the conditional query plan");
      }

      const invertedQueryPlan: PlanResourcesConditionalResponse = {
        ...typeQp,
        condition: {
          ...condition,
          operands: [secondOperand, firstOperand],
        },
      };

      const result = queryPlanToMongoose({
        queryPlan: invertedQueryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aBool: {
            $eq: true,
          },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.aBool).map((r) => r.key)
      );
    });

    test("conditional - ne", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ne",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { aString: { $ne: "string" } },
      });
      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.aString != "string").map((r) => r.key)
      );
    });

    test("conditional - explicit-deny", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "explicit-deny",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { $nor: [{ aBool: { $eq: true } }] },
      });
      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => !a.aBool).map((r) => r.key)
      );
    });
  });

  describe("Comparison Tests", () => {
    test("conditional - gt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gt",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $gt: 1 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) > 1;
          })
          .map((r) => r.key)
      );
    });

    test("conditional - lt", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lt",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $lt: 2 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) < 2;
          })
          .map((r) => r.key)
      );
    });

    test("conditional - gte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "gte",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $gte: 1 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) >= 1;
          })
          .map((r) => r.key)
      );
    });

    test("conditional - lte", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "lte",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aNumber: { $lte: 2 },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => {
            return (r.aNumber as number) <= 2;
          })
          .map((r) => r.key)
      );
    });
  });

  describe("String Operations", () => {
    test("conditional - contains", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "contains",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $regex: "str" },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => r.aString.includes("str"))
          .map((r) => r.key)
      );
    });

    test("conditional - startsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "starts-with",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $regex: "^str" },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => r.aString.startsWith("str"))
          .map((r) => r.key)
      );
    });

    test("conditional - endsWith", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ends-with",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $regex: "ing\\z" },
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources
          .filter((r) => r.aString.endsWith("ing"))
          .map((r) => r.key)
      );
    });

    test("conditional - endsWith anchors at the absolute end of the string", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "ends-with",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      // `\z`, not `$`: Mongo evaluates $regex with PCRE2, whose `$` also
      // matches immediately before a final newline, so "ing$" would admit
      // "string\n" even though CEL "string\n".endsWith("ing") is false.
      expect(result.filters).toStrictEqual({ aString: { $regex: "ing\\z" } });

      const template = fixtureResources[0];
      if (!template) {
        throw new Error("fixtureResources must not be empty");
      }
      await Resource.create({
        ...template,
        key: "trailing-newline",
        id: "resource-trailing-newline",
        aString: "string\n",
      });
      try {
        const query = await Resource.find(result.filters || {});
        expect(query.map((r) => r.key)).not.toContain("trailing-newline");
      } finally {
        await Resource.deleteMany({ key: "trailing-newline" });
      }
    });
  });
});

describe("Collection Operations", () => {
  test("conditional - in", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        aString: { $in: ["string", "anotherString"] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => {
          return allowedStringValues.has(r.aString as string);
        })
        .map((r) => r.key)
    );
  });

  test("conditional - exists", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
            field: "name",
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        tags: {
          $elemMatch: {
            name: { $eq: "public" },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.some((t) => t.name === "public"))
        .map((r) => r.key)
    );
  });

  test("conditional - exists_one fails loudly", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "exists-one",
    });

    expect(() =>
      queryPlanToMongoose({
        queryPlan,
        mapper: {
          ...defaultMapper,
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
              field: "name",
            },
          },
        },
      })
    ).toThrow("exists_one requires exact match cardinality and is unsupported");
  });

  test("conditional - all", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
            field: "name",
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        tags: {
          $type: "array",
          $not: {
            $elemMatch: {
              $nor: [{ name: { $eq: "public" } }],
            },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.every((t) => t.name === "public"))
        .map((r) => r.key)
    );
  });

  test("conditional - all rejects a missing collection", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all-nested",
    });
    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
          },
        },
      },
    });
    const original = fixtureResources.find(({ key }) => key === "a");
    if (!original) {
      throw new Error("Missing fixture resource a");
    }

    await Resource.collection.updateOne(
      { key: original.key },
      { $unset: { tags: "" } }
    );
    try {
      const query = await Resource.find(result.filters || {});
      expect(query.map(({ key }) => key)).not.toContain(original.key);
    } finally {
      await Resource.collection.updateOne(
        { key: original.key },
        { $set: { tags: original.tags } }
      );
    }
  });

  test("conditional - hasIntersection", async () => {
    const queryPlan = await cerbos.planResources({
      principal: {
        id: "user1",
        roles: ["USER"],
        attr: { tags: ["public", "draft"] },
      },
      resource: { kind: "resource" },
      action: "has-intersection",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        tags: {
          $elemMatch: {
            name: {
              $in: ["public", "draft"],
            },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.tags.some((t) => allowedTagNames.has(t.name)))
        .map((r) => r.key)
    );
  });

  test("conditional - all-nested (multi-clause lambda body)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "all-nested",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "all",
      operands: [
        { name: "request.resource.attr.tags" },
        {
          operator: "lambda",
          operands: [
            {
              operator: "and",
              operands: [
                {
                  operator: "eq",
                  operands: [{ name: "tag.name" }, { value: "public" }],
                },
                {
                  operator: "ne",
                  operands: [{ name: "tag.id" }, { value: "tag1" }],
                },
              ],
            },
            { name: "tag" },
          ],
        },
      ],
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        tags: {
          $type: "array",
          $not: {
            $elemMatch: {
              $nor: [
                {
                  $and: [
                    { name: { $eq: "public" } },
                    { id: { $ne: "tag1" } },
                  ],
                },
              ],
            },
          },
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) =>
          r.tags.every((t) => t.name === "public" && t.id !== "tag1")
        )
        .map((r) => r.key)
        .sort()
    );
  });

  test("conditional - map-compared (map(...) == literal list)", async () => {
    // TODO(#232): mongoose adapter does not yet support `map(...) == [..]` —
    // the comparison falls through to the `$expr` aggregation path, which
    // does not implement `map`/`filter` operators. Locks in current behavior.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "map-compared",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "eq",
      operands: [
        {
          operator: "map",
          operands: [
            { name: "request.resource.attr.tags" },
            {
              operator: "lambda",
              operands: [{ name: "t.id" }, { name: "t" }],
            },
          ],
        },
        { value: ["tag1", "tag2"] },
      ],
    });

    expect(() =>
      queryPlanToMongoose({
        queryPlan,
        mapper: {
          ...defaultMapper,
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      })
    ).toThrow();
  });

  test("conditional - filter-count-gt (size(filter(...)) > 0)", async () => {
    // TODO(#232): mongoose adapter does not yet unwrap
    // `size(filter(collection, lambda)) > N`. The outer `gt` routes to the
    // `$expr` aggregation builder, which does not implement `filter`.
    // Locks in current behavior.
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "filter-count-gt",
    });

    expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
    expect((queryPlan as PlanResourcesConditionalResponse).condition).toEqual({
      operator: "gt",
      operands: [
        {
          operator: "size",
          operands: [
            {
              operator: "filter",
              operands: [
                { name: "request.resource.attr.tags" },
                {
                  operator: "lambda",
                  operands: [
                    {
                      operator: "eq",
                      operands: [{ name: "t.name" }, { value: "public" }],
                    },
                    { name: "t" },
                  ],
                },
              ],
            },
          ],
        },
        { value: 0 },
      ],
    });

    expect(() =>
      queryPlanToMongoose({
        queryPlan,
        mapper: {
          ...defaultMapper,
          "request.resource.attr.tags": {
            relation: {
              name: "tags",
              type: "many",
            },
          },
        },
      })
    ).toThrow();
  });
});

describe("Logical Operations", () => {
  test("conditional - and", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "and",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $and: [{ aBool: { $eq: true } }, { aString: { $ne: "string" } }],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => r.aBool && r.aString !== "string")
        .map((r) => r.key)
    );
  });

  test("conditional - or", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $or: [
          {
            aBool: {
              $eq: true,
            },
          },
          {
            aString: { $ne: "string" },
          },
        ],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => {
          return r.aBool || r.aString != "string";
        })
        .map((r) => r.key)
    );
  });
});

describe("Negation Operations", () => {
  test("conditional - not-and (DeMorgan over AND)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-and",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $nor: [
          {
            $and: [
              { aBool: { $eq: true } },
              { aString: { $ne: "string" } },
            ],
          },
        ],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aBool && r.aString !== "string"))
        .map((r) => r.key)
    );
  });

  test("conditional - not-or (DeMorgan over OR)", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-or",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $nor: [
          {
            $or: [
              { aBool: { $eq: true } },
              { aString: { $ne: "string" } },
            ],
          },
        ],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aBool || r.aString !== "string"))
        .map((r) => r.key)
    );
  });

  test("conditional - not-gt", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-gt",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $nor: [{ aNumber: { $gt: 1 } }],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !((r.aNumber as number) > 1))
        .map((r) => r.key)
    );
  });

  test("conditional - not-lt", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-lt",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $nor: [{ aNumber: { $lt: 2 } }],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !((r.aNumber as number) < 2))
        .map((r) => r.key)
    );
  });

  test("conditional - not-contains", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-contains",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $nor: [{ aString: { $regex: "str" } }],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aString as string).includes("str"))
        .map((r) => r.key)
    );
  });

  test("conditional - not-starts-with", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "not-starts-with",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $nor: [{ aString: { $regex: "^str" } }],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((r) => !(r.aString as string).startsWith("str"))
        .map((r) => r.key)
    );
  });
});

describe("Relations", () => {
  describe("Nested Relations", () => {
    test("conditional - eq nested", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal-nested",
      });

      const result = queryPlanToMongoose({
        queryPlan,
        mapper: defaultMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          "nested.aBool": {
            $eq: true,
          },
        },
      });
      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key)).toEqual(
        fixtureResources.filter((a) => a.nested.aBool).map((r) => r.key)
      );
    });
  });
});

describe("Mapper Functions", () => {
  test("function mapper for field names", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: (key: string) => ({
        field: key.replace("request.resource.attr.", ""),
      }),
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aBool: { $eq: true } },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) => resource.aBool === true)
        .map((resource) => resource.key)
    );
  });

  test("function mapper for relations", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "relation-is",
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: (_key: string) => ({
        relation: {
          name: "createdBy",
          type: "one",
          field: "id",
        },
      }),
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        "createdBy.id": { $eq: "user1" },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key)).toEqual(
      fixtureResources
        .filter((resource) => resource.createdBy.id === "user1")
        .map((resource) => resource.key)
    );
  });
});

describe("Error Handling", () => {
  test("throws error for invalid query plan", () => {
    const invalidQueryPlan = {
      kind: "INVALID_KIND" as PlanKind,
    };

    expect(() =>
      queryPlanToMongoose({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Invalid query plan.");
  });

  test("throws error for invalid expression structure", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        // Missing operator and operands
      },
    };

    expect(() =>
      queryPlanToMongoose({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("throws error for unsupported operator", () => {
    const invalidQueryPlan = {
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "unsupported",
        operands: [],
      },
    };

    expect(() =>
      queryPlanToMongoose({
        queryPlan: invalidQueryPlan as unknown as PlanResourcesResponse,
        mapper: {},
      })
    ).toThrow("Unsupported operator: unsupported");
  });
});

describe("valueParser functionality", () => {
    test("applies valueParser to 'equal' operator", async () => {
      const queryPlan = await cerbos.planResources({
        principal: { id: "user1", roles: ["USER"] },
        resource: { kind: "resource" },
        action: "equal",
      });

      const mapper: Mapper = {
        "request.resource.attr.aBool": {
          field: "aBool",
          valueParser: (value) => !value,
        },
      };

      const result = queryPlanToMongoose({
        queryPlan,
        mapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aBool: { $eq: false },
        },
      });
    });

    test("applies valueParser to 'in' operator", async () => {
      const queryPlan = await cerbos.planResources({
          principal: { id: "user1", roles: ["USER"] },
          resource: { kind: "resource" },
          action: "in",
      });
  
      const mapper: Mapper = {
        "request.resource.attr.aString": {
          field: "aString",
          valueParser: (value) => String(value).toUpperCase(),
        },
      };
  
      const result = queryPlanToMongoose({
        queryPlan,
        mapper,
      });
      
      const expectedValues = ["string", "anotherString"].map(v => v.toUpperCase());
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          aString: { $in: expectedValues },
        },
      });
    });

    test("applies valueParser for ObjectId conversion on 'eq'", async () => {
        const queryPlan = await cerbos.planResources({
            principal: { id: "user1", roles: ["USER"] },
            resource: { kind: "resource" },
            action: "equal-oid",
        });

        const mapper: Mapper = {
            "request.resource.attr.id": {
                field: "_id",
                valueParser: (value: string) => new Types.ObjectId(value),
            },
        };

        const result = queryPlanToMongoose({
            queryPlan,
            mapper,
        });

        expect(result).toStrictEqual({
            kind: PlanKind.CONDITIONAL,
            filters: {
                _id: { $eq: new Types.ObjectId("507f1f77bcf86cd799439011") },
            },
        });
    });

    test("applies valueParser from nested relation fields on 'eq'", () => {
      // #given
      const queryPlan = {
        kind: PlanKind.CONDITIONAL,
        condition: new PlanExpression("eq", [
          new PlanExpressionVariable("request.resource.attr.createdBy.id"),
          new PlanExpressionValue("507f1f77bcf86cd799439011"),
        ]),
      } as PlanResourcesResponse;

      const mapper: Mapper = {
        "request.resource.attr.createdBy": {
          relation: {
            name: "createdBy",
            type: "one",
            field: "id",
            fields: {
              id: {
                field: "id",
                valueParser: (value: string) => new Types.ObjectId(value),
              },
            },
          },
        },
      };

      // #when
      const result = queryPlanToMongoose({ queryPlan, mapper });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          "createdBy.id": {
            $eq: new Types.ObjectId("507f1f77bcf86cd799439011"),
          },
        },
      });
    });

    test("applies valueParser from nested relation fields on 'in'", () => {
      // #given
      const queryPlan = {
        kind: PlanKind.CONDITIONAL,
        condition: new PlanExpression("in", [
          new PlanExpressionVariable("request.resource.attr.createdBy.id"),
          new PlanExpressionValue([
            "507f1f77bcf86cd799439011",
            "507f1f77bcf86cd799439012",
          ]),
        ]),
      } as PlanResourcesResponse;

      const mapper: Mapper = {
        "request.resource.attr.createdBy": {
          relation: {
            name: "createdBy",
            type: "one",
            field: "id",
            fields: {
              id: {
                field: "id",
                valueParser: (value: string) => new Types.ObjectId(value),
              },
            },
          },
        },
      };

      // #when
      const result = queryPlanToMongoose({ queryPlan, mapper });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          "createdBy.id": {
            $in: [
              new Types.ObjectId("507f1f77bcf86cd799439011"),
              new Types.ObjectId("507f1f77bcf86cd799439012"),
            ],
          },
        },
      });
    });

    test("skips valueParser when nested relation field has none", () => {
      // #given
      const queryPlan = {
        kind: PlanKind.CONDITIONAL,
        condition: new PlanExpression("eq", [
          new PlanExpressionVariable("request.resource.attr.createdBy.id"),
          new PlanExpressionValue("user1"),
        ]),
      } as PlanResourcesResponse;

      const mapper: Mapper = {
        "request.resource.attr.createdBy": {
          relation: {
            name: "createdBy",
            type: "one",
            field: "id",
            fields: {
              id: { field: "id" },
            },
          },
        },
      };

      // #when
      const result = queryPlanToMongoose({ queryPlan, mapper });

      // #then
      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          "createdBy.id": { $eq: "user1" },
        },
      });
    });
});

describe("Arithmetic Operations", () => {
  const arithMapper: Mapper = {
    ...defaultMapper,
  };

  test("arith-add: (aNumber + 1) > 2", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-add",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "add",
            operands: [
              { name: "request.resource.attr.aNumber" },
              { value: 1 },
            ],
          },
          { value: 2 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: arithMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: { $gt: [{ $add: ["$aNumber", 1] }, 2] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.aNumber as number) + 1 > 2)
        .map((r) => r.key)
        .sort()
    );
  });

  test("arith-sub: (aNumber - 1) < 2", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-sub",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "lt",
        operands: [
          {
            operator: "sub",
            operands: [
              { name: "request.resource.attr.aNumber" },
              { value: 1 },
            ],
          },
          { value: 2 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: arithMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: { $lt: [{ $subtract: ["$aNumber", 1] }, 2] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.aNumber as number) - 1 < 2)
        .map((r) => r.key)
        .sort()
    );
  });

  test("arith-mult: (aNumber * 2) > 2", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-mult",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "mult",
            operands: [
              { name: "request.resource.attr.aNumber" },
              { value: 2 },
            ],
          },
          { value: 2 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: arithMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: { $gt: [{ $multiply: ["$aNumber", 2] }, 2] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.aNumber as number) * 2 > 2)
        .map((r) => r.key)
        .sort()
    );
  });

  test("arith-div: (aNumber / 2) > 0", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-div",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "div",
            operands: [
              { name: "request.resource.attr.aNumber" },
              { value: 2 },
            ],
          },
          { value: 0 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: arithMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: { $gt: [{ $divide: ["$aNumber", 2] }, 0] },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.aNumber as number) / 2 > 0)
        .map((r) => r.key)
        .sort()
    );
  });

  // #311: the arith-mod policy is `int(aNumber) % 2 == 0`, so it inherits the int()
  // rejection — $convert parses a numeric prefix where CEL demands the whole string, and
  // rounds where CEL truncates toward zero.
  test("arith-mod: int() in the policy makes the shape fail closed", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "arith-mod",
    });

    expect(() =>
      queryPlanToMongoose({ queryPlan, mapper: defaultMapper })
    ).toThrow(/'int\(\)' cannot be translated/);
  });

  test("matches-regex: aString.matches('str.*')", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "matches-regex",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "matches",
        operands: [
          { name: "request.resource.attr.aString" },
          { value: "^str.*" },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        aString: { $regex: "^str.*" },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => /str.*/.test(r.aString as string))
        .map((r) => r.key)
        .sort()
    );
  });

  test("terminal $ keeps RE2 absolute end-of-text semantics", async () => {
    const matches = new PlanExpression("matches", [
      new PlanExpressionVariable("request.resource.attr.aString"),
      new PlanExpressionValue("^foo$"),
    ]);
    const plans = [
      conditionalPlan(matches),
      conditionalPlan(
        new PlanExpression("eq", [matches, new PlanExpressionValue(true)])
      ),
    ];
    const original = fixtureResources.find(({ key }) => key === "a");
    if (!original) {
      throw new Error("Missing fixture resource a");
    }

    try {
      for (const plan of plans) {
        const result = queryPlanToMongoose({
          queryPlan: plan,
          mapper: defaultMapper,
        });
        await Resource.collection.updateOne(
          { key: original.key },
          { $set: { aString: "foo" } }
        );
        expect(
          (await Resource.find(result.filters || {})).map(({ key }) => key)
        ).toContain(original.key);

        await Resource.collection.updateOne(
          { key: original.key },
          { $set: { aString: "foo\n" } }
        );
        expect(
          (await Resource.find(result.filters || {})).map(({ key }) => key)
        ).not.toContain(original.key);
      }
    } finally {
      await Resource.collection.updateOne(
        { key: original.key },
        { $set: { aString: original.aString } }
      );
    }
  });

  test("unsupported regex syntax fails closed in both Mongo paths", () => {
    const matches = new PlanExpression("matches", [
      new PlanExpressionVariable("request.resource.attr.aString"),
      new PlanExpressionValue("(?=foo)"),
    ]);
    const plans = [
      conditionalPlan(matches),
      conditionalPlan(
        new PlanExpression("eq", [matches, new PlanExpressionValue(true)])
      ),
    ];

    for (const queryPlan of plans) {
      expect(() =>
        queryPlanToMongoose({ queryPlan, mapper: defaultMapper })
      ).toThrow("common RE2/PCRE2 subset");
    }
  });
});

describe("Index Access", () => {
  test("index-list: ownedBy[0] == 'user1'", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "index-list",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          {
            operator: "index",
            operands: [
              { name: "request.resource.attr.ownedBy" },
              { value: 0 },
            ],
          },
          { value: "user1" },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.ownedBy": { field: "ownedBy" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $and: [
          {
            $expr: {
              $cond: {
                if: { $isArray: "$ownedBy" },
                then: { $gt: [{ $size: "$ownedBy" }, 0] },
                else: false,
              },
            },
          },
          {
            $expr: { $eq: [{ $arrayElemAt: ["$ownedBy", 0] }, "user1"] },
          },
        ],
      },
    });

    // ownedBy is an array of objects in the fixture, so comparing element to a
    // string returns no matches in both Mongo and JS — the filter shape is
    // still correct.
    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.ownedBy as any[])[0] === "user1")
        .map((r) => r.key)
        .sort()
    );
  });

  test.each([-1, 1.5])("rejects the unsound constant index %s", (index) => {
    const queryPlan = conditionalPlan(
      new PlanExpression("eq", [
        new PlanExpression("index", [
          new PlanExpressionVariable("request.resource.attr.ownedBy"),
          new PlanExpressionValue(index),
        ]),
        new PlanExpressionValue("user1"),
      ])
    );

    expect(() =>
      queryPlanToMongoose({
        queryPlan,
        mapper: {
          ...defaultMapper,
          "request.resource.attr.ownedBy": { field: "ownedBy" },
        },
      })
    ).toThrow("index operator requires a non-negative integer constant");
  });

  test("an out-of-bounds index cannot make ne authorize", async () => {
    const queryPlan = conditionalPlan(
      new PlanExpression("ne", [
        new PlanExpression("index", [
          new PlanExpressionVariable("request.resource.attr.ownedBy"),
          new PlanExpressionValue(10),
        ]),
        new PlanExpressionValue("user1"),
      ])
    );
    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.ownedBy": { field: "ownedBy" },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query).toHaveLength(0);
  });
});

describe("Type Conversion", () => {
  test("convert-string: string(aNumber) == '1'", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "convert-string",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          {
            operator: "string",
            operands: [{ name: "request.resource.attr.aNumber" }],
          },
          { value: "1" },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });
    const conversion = checkedConversion(
      "$aNumber",
      ["string", "bool", "int", "long", "double", "decimal"],
      "string"
    );

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $and: [
          { $expr: { $ne: [conversion, null] } },
          { $expr: { $eq: [conversion, "1"] } },
        ],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => String(r.aNumber) === "1")
        .map((r) => r.key)
        .sort()
    );
  });

  // #311: CEL's int()/double() are not $convert. CEL reads a WHOLE string or raises, and
  // an error DENIES the row, while $convert parses a leading numeric prefix — the corpus
  // seeds "100%_done" and "50%_off", which became 100 and 50 and returned records the PDP
  // denies. The numeric direction is no safer: $convert to "long" ROUNDS where CEL
  // truncates toward zero, so int(-0.6) is 0 to CEL and -1 here. Nothing in the plan says
  // what type the field holds, so the adapter fails closed for the whole family.
  test.each([
    ["convert-double", /'double\(\)' cannot be translated/],
    ["convert-int", /'int\(\)' cannot be translated/],
  ] as const)("%s fails closed", async (action, message) => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action,
    });

    expect(() =>
      queryPlanToMongoose({ queryPlan, mapper: defaultMapper })
    ).toThrow(message);
  });

  test.each(["double", "int"] as const)(
    "%s is rejected under both positive and negative polarity",
    async (operator) => {
      const conversion = new PlanExpression(operator, [
        new PlanExpressionVariable("request.resource.attr.aBool"),
      ]);
      const comparison = new PlanExpression("eq", [
        conversion,
        new PlanExpressionValue(1),
      ]);
      for (const condition of [
        comparison,
        new PlanExpression("not", [comparison]),
      ]) {
        expect(() =>
          queryPlanToMongoose({
            queryPlan: conditionalPlan(condition),
            mapper: defaultMapper,
          })
        ).toThrow(/cannot be translated/);
      }
    }
  );

  test("string conversion rejects null and missing inputs under negation", async () => {
    const condition = new PlanExpression("ne", [
      new PlanExpression("string", [
        new PlanExpressionVariable(
          "request.resource.attr.aOptionalString"
        ),
      ]),
      new PlanExpressionValue("x"),
    ]);
    const result = queryPlanToMongoose({
      queryPlan: conditionalPlan(condition),
      mapper: defaultMapper,
    });

    await Resource.collection.updateOne(
      { key: "c" },
      { $set: { aOptionalString: null } }
    );
    try {
      expect(
        (await Resource.find(result.filters || {})).map(({ key }) => key)
      ).toEqual(["a"]);
    } finally {
      await Resource.collection.updateOne(
        { key: "c" },
        { $unset: { aOptionalString: "" } }
      );
    }
  });

  test("int conversion is rejected rather than reading a BSON date's milliseconds", async () => {
    const condition = new PlanExpression("eq", [
      new PlanExpression("int", [
        new PlanExpressionVariable("request.resource.attr.aString"),
      ]),
      new PlanExpressionValue(1704067200000),
    ]);

    expect(() =>
      queryPlanToMongoose({
        queryPlan: conditionalPlan(condition),
        mapper: defaultMapper,
      })
    ).toThrow(/'int\(\)' cannot be translated/);
  });

  test("timestamp comparisons reject malformed and non-RFC 3339 fields", async () => {
    const timestamp = (operator: "lt" | "gt") =>
      new PlanExpression(operator, [
        new PlanExpression("timestamp", [
          new PlanExpressionVariable("request.resource.attr.aString"),
        ]),
        new PlanExpression("timestamp", [
          new PlanExpressionValue("2025-01-01T00:00:00Z"),
        ]),
      ]);
    const conditions = [
      timestamp("lt"),
      new PlanExpression("not", [timestamp("gt")]),
    ];
    const original = fixtureResources.find(({ key }) => key === "a");
    if (!original) {
      throw new Error("Missing fixture resource a");
    }

    try {
      for (const invalidTimestamp of [
        "2024-01-01",
        "0000-01-01T00:00:00Z",
        "2024-01-01T00:00:00.0001Z",
        "2024-01-01T00:00:00.1234567890Z",
        "9999-12-31T23:00:00-02:00",
      ]) {
        await Resource.collection.updateOne(
          { key: original.key },
          { $set: { aString: invalidTimestamp } }
        );
        for (const condition of conditions) {
          const queryPlan = conditionalPlan(condition);
          const result = queryPlanToMongoose({
            queryPlan,
            mapper: defaultMapper,
          });
          const query = await Resource.find(result.filters || {});
          expect(query).toHaveLength(0);
        }
      }
    } finally {
      await Resource.collection.updateOne(
        { key: original.key },
        { $set: { aString: original.aString } }
      );
    }
  });

  test.each([
    "2024-01-01",
    "0000-01-01T00:00:00Z",
    "2024-01-01T00:00:00.0001Z",
    "9999-12-31T23:00:00-02:00",
  ])("timestamp rejects the unsupported literal %s", (literal) => {
    const queryPlan = conditionalPlan(
      new PlanExpression("eq", [
        new PlanExpression("timestamp", [
          new PlanExpressionValue(literal),
        ]),
        new PlanExpression("timestamp", [
          new PlanExpressionValue("2024-01-01T00:00:00Z"),
        ]),
      ])
    );

    expect(() =>
      queryPlanToMongoose({ queryPlan, mapper: defaultMapper })
    ).toThrow(
      "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range"
    );
  });

  test("timestamp rejects precision that BSON Date would silently truncate", async () => {
    const queryPlan = conditionalPlan(
      new PlanExpression("eq", [
        new PlanExpression("timestamp", [
          new PlanExpressionVariable("request.resource.attr.aString"),
        ]),
        new PlanExpression("timestamp", [
          new PlanExpressionValue("2024-01-01T00:00:00.000Z"),
        ]),
      ])
    );
    const result = queryPlanToMongoose({ queryPlan, mapper: defaultMapper });
    const original = fixtureResources.find(({ key }) => key === "a");
    if (!original) {
      throw new Error("Missing fixture resource a");
    }

    await Resource.collection.updateOne(
      { key: original.key },
      { $set: { aString: "2024-01-01T00:00:00.0001Z" } }
    );
    try {
      expect(await Resource.find(result.filters || {})).toHaveLength(0);
    } finally {
      await Resource.collection.updateOne(
        { key: original.key },
        { $set: { aString: original.aString } }
      );
    }
  });

  test("timestamp rejects BSON dates outside the CEL instant range", async () => {
    const queryPlan = conditionalPlan(
      new PlanExpression("ne", [
        new PlanExpression("timestamp", [
          new PlanExpressionVariable("request.resource.attr.aString"),
        ]),
        new PlanExpression("timestamp", [
          new PlanExpressionValue("2024-01-01T00:00:00.000Z"),
        ]),
      ])
    );
    const result = queryPlanToMongoose({ queryPlan, mapper: defaultMapper });
    const original = fixtureResources.find(({ key }) => key === "a");
    if (!original) {
      throw new Error("Missing fixture resource a");
    }

    await Resource.collection.updateOne(
      { key: original.key },
      { $set: { aString: new Date("+010000-01-01T00:00:00.000Z") } }
    );
    try {
      expect(await Resource.find(result.filters || {})).toHaveLength(0);
    } finally {
      await Resource.collection.updateOne(
        { key: original.key },
        { $set: { aString: original.aString } }
      );
    }
  });
});

describe("Ternary", () => {
  test("ternary: (aBool ? aNumber : 0) > 0", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "ternary",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "if",
            operands: [
              { name: "request.resource.attr.aBool" },
              { name: "request.resource.attr.aNumber" },
              { value: 0 },
            ],
          },
          { value: 0 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: {
          $gt: [
            {
              $cond: {
                if: "$aBool",
                then: "$aNumber",
                else: 0,
              },
            },
            0,
          ],
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.aBool ? (r.aNumber as number) : 0) > 0)
        .map((r) => r.key)
        .sort()
    );
  });
});

describe("Size", () => {
  test("string-size: size(aString) > 0", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "string-size",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "gt",
        operands: [
          {
            operator: "size",
            operands: [{ name: "request.resource.attr.aString" }],
          },
          { value: 0 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: {
          $gt: [
            {
              $cond: [
                { $isArray: "$aString" },
                { $size: "$aString" },
                { $strLenCP: "$aString" },
              ],
            },
            0,
          ],
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => (r.aString as string).length > 0)
        .map((r) => r.key)
        .sort()
    );
  });

  test("empty-collection: size(tags) == 0", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "empty-collection",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          {
            operator: "size",
            operands: [{ name: "request.resource.attr.tags" }],
          },
          { value: 0 },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": { field: "tags" },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $expr: {
          $eq: [
            {
              $cond: [
                { $isArray: "$tags" },
                { $size: "$tags" },
                { $strLenCP: "$tags" },
              ],
            },
            0,
          ],
        },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => r.tags.length === 0)
        .map((r) => r.key)
        .sort()
    );
  });
});

describe("Missing operator shapes (issue #229)", () => {
  test("is-not-set: aOptionalString == null", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "is-not-set",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aOptionalString" },
          { value: null },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $and: [
          { aOptionalString: { $exists: true } },
          { aOptionalString: { $eq: null } },
        ],
      },
    });

    await Resource.collection.updateOne(
      { key: "c" },
      { $set: { aOptionalString: null } }
    );
    try {
      expect(
        (await Resource.find(result.filters || {})).map(({ key }) => key)
      ).toEqual(["c"]);
    } finally {
      await Resource.collection.updateOne(
        { key: "c" },
        { $unset: { aOptionalString: "" } }
      );
    }
  });

  test("membership containing null excludes a missing field", async () => {
    const queryPlan = conditionalPlan(
      new PlanExpression("in", [
        new PlanExpressionVariable(
          "request.resource.attr.aOptionalString"
        ),
        new PlanExpressionValue([null]),
      ])
    );
    const result = queryPlanToMongoose({ queryPlan, mapper: defaultMapper });

    await Resource.collection.updateOne(
      { key: "c" },
      { $set: { aOptionalString: null } }
    );
    try {
      expect(
        (await Resource.find(result.filters || {})).map(({ key }) => key)
      ).toEqual(["c"]);
    } finally {
      await Resource.collection.updateOne(
        { key: "c" },
        { $unset: { aOptionalString: "" } }
      );
    }
  });

  test("equal-field-to-field: aString == id", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal-field-to-field",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aString" },
          { name: "request.resource.attr.id" },
        ],
      },
    });

    // TODO: field-to-field comparison is a follow-up. The current adapter
    // resolves both operands as field references and falls through to a
    // `{ field: { $eq: undefined } }` filter rather than emitting a `$expr`
    // with `$eq: ["$aString", "$id"]`. Once supported, this test should
    // assert the `$expr` shape and that resources with `aString == id`
    // are returned.
    const mapper: Mapper = {
      ...defaultMapper,
      "request.resource.attr.id": { field: "id" },
    };
    expect(() => queryPlanToMongoose({ queryPlan, mapper })).not.toThrow();
  });

  test("equal-bool-false: aBool == false", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "equal-bool-false",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "eq",
        operands: [
          { name: "request.resource.attr.aBool" },
          { value: false },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        aBool: { $eq: false },
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => r.aBool === false)
        .map((r) => r.key)
        .sort()
    );
  });

  test("in-number: aNumber in [1, 2, 3]", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "in-number",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "in",
        operands: [
          { name: "request.resource.attr.aNumber" },
          { value: [1, 2, 3] },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: defaultMapper,
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        aNumber: { $in: [1, 2, 3] },
      },
    });

    const allowedNumbers = new Set<number>([1, 2, 3]);
    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter((r) => allowedNumbers.has(r.aNumber as number))
        .map((r) => r.key)
        .sort()
    );
  });

  test("or-leaf-exists: aBool == true || tags.exists(t, t.name == 'public')", async () => {
    const queryPlan = await cerbos.planResources({
      principal: { id: "user1", roles: ["USER"] },
      resource: { kind: "resource" },
      action: "or-leaf-exists",
    });

    expect(queryPlan).toMatchObject({
      kind: PlanKind.CONDITIONAL,
      condition: {
        operator: "or",
        operands: [
          {
            operator: "eq",
            operands: [
              { name: "request.resource.attr.aBool" },
              { value: true },
            ],
          },
          {
            operator: "exists",
            operands: [
              { name: "request.resource.attr.tags" },
              expect.objectContaining({ operator: "lambda" }),
            ],
          },
        ],
      },
    });

    const result = queryPlanToMongoose({
      queryPlan,
      mapper: {
        ...defaultMapper,
        "request.resource.attr.tags": {
          relation: {
            name: "tags",
            type: "many",
            field: "name",
          },
        },
      },
    });

    expect(result).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: {
        $or: [
          { aBool: { $eq: true } },
          {
            tags: {
              $elemMatch: {
                name: { $eq: "public" },
              },
            },
          },
        ],
      },
    });

    const query = await Resource.find(result.filters || {});
    expect(query.map((r) => r.key).sort()).toEqual(
      fixtureResources
        .filter(
          (r) =>
            r.aBool === true || r.tags.some((t) => t.name === "public")
        )
        .map((r) => r.key)
        .sort()
    );
  });
});

describe("Known-Value Collections (planner unroll cliff)", () => {
  // The planner unrolls exists/all over a known collection (e.g. a folded
  // principal attribute) into an or/and chain at <= 10 elements
  // (cerbos/cerbos#2570, #2817) and emits the lambda with a literal value-list
  // collection above that. These tests straddle the 10-item cliff so both wire
  // shapes stay exercised regardless of the PDP version behind the sidecar.
  describe("live plans across the 10-item threshold", () => {
    const buildTeams = (size: number): string[] => {
      const teams = ["string", "string3"];
      while (teams.length < size) {
        teams.push(`filler-${teams.length}`);
      }
      return teams;
    };

    // Supported PDPs are >= 0.54, where both macros unroll at <= 10 elements
    // and ship the value-list lambda above that. Pin the wire shape so each
    // leg provably exercises its side of the cliff — if a future planner moves
    // the threshold, this fails loudly instead of silently testing one shape.
    const expectShape = (
      queryPlan: PlanResourcesResponse,
      size: number,
      unrolledOperator: string,
      macroOperator: string
    ): void => {
      const condition = (queryPlan as PlanResourcesConditionalResponse)
        .condition;
      expect((condition as PlanExpression).operator).toEqual(
        size <= 10 ? unrolledOperator : macroOperator
      );
    };

    describe.each([9, 10, 11])(
      "with %i-element principal collection",
      (size) => {
        test("conditional - principal-exists", async () => {
          const teams = buildTeams(size);
          const queryPlan = await cerbos.planResources({
            principal: { id: "user1", roles: ["USER"], attr: { teams } },
            resource: { kind: "resource" },
            action: "principal-exists",
          });

          expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
          expectShape(queryPlan, size, "or", "exists");

          const result = queryPlanToMongoose({
            queryPlan,
            mapper: { "request.resource.attr.aString": { field: "aString" } },
          });

          const query = await Resource.find(result.filters || {});
          expect(query.map((r) => r.key).sort()).toEqual(
            fixtureResources
              .filter((r) => teams.includes(r.aString as string))
              .map((r) => r.key)
              .sort()
          );
        });

        test("conditional - principal-all", async () => {
          const teams = buildTeams(size);
          const queryPlan = await cerbos.planResources({
            principal: { id: "user1", roles: ["USER"], attr: { teams } },
            resource: { kind: "resource" },
            action: "principal-all",
          });

          expect(queryPlan.kind).toEqual(PlanKind.CONDITIONAL);
          expectShape(queryPlan, size, "and", "all");

          const result = queryPlanToMongoose({
            queryPlan,
            mapper: { "request.resource.attr.aString": { field: "aString" } },
          });

          const query = await Resource.find(result.filters || {});
          expect(query.map((r) => r.key).sort()).toEqual(
            fixtureResources
              .filter((r) => !teams.includes(r.aString as string))
              .map((r) => r.key)
              .sort()
          );
        });
      }
    );
  });

  describe("value-list lambda fold", () => {
    const valueListPlan = (
      operator: string,
      elements: unknown[] | Record<string, unknown>,
      body: PlanExpression | PlanExpressionVariable,
      variable = "t"
    ): PlanResourcesResponse =>
      conditionalPlan(
        new PlanExpression(operator, [
          new PlanExpressionValue(elements as never),
          new PlanExpression("lambda", [
            body,
            new PlanExpressionVariable(variable),
          ]),
        ])
      );

    const compareBody = (operator: string, variable = "t"): PlanExpression =>
      new PlanExpression(operator, [
        new PlanExpressionVariable("request.resource.attr.aString"),
        new PlanExpressionVariable(variable),
      ]);

    const stringMapper: Mapper = {
      "request.resource.attr.aString": { field: "aString" },
    };

    test("exists over a value list folds to $or of the substituted body", async () => {
      const result = queryPlanToMongoose({
        queryPlan: valueListPlan("exists", ["string", "string3"], compareBody("eq")),
        mapper: stringMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          $or: [{ aString: { $eq: "string" } }, { aString: { $eq: "string3" } }],
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key).sort()).toEqual(["a", "c"]);
    });

    test("all over a value list folds to $and of the substituted body", async () => {
      const result = queryPlanToMongoose({
        queryPlan: valueListPlan("all", ["string", "string3"], compareBody("ne")),
        mapper: stringMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          $and: [{ aString: { $ne: "string" } }, { aString: { $ne: "string3" } }],
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key).sort()).toEqual(["b"]);
    });

    test("substitutes variable path references into element fields", async () => {
      const result = queryPlanToMongoose({
        queryPlan: valueListPlan(
          "exists",
          [
            { name: "string", meta: { rank: 1 } },
            { name: "string3", meta: { rank: 2 } },
          ],
          new PlanExpression("eq", [
            new PlanExpressionVariable("request.resource.attr.aString"),
            new PlanExpressionVariable("t.name"),
          ])
        ),
        mapper: stringMapper,
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          $or: [{ aString: { $eq: "string" } }, { aString: { $eq: "string3" } }],
        },
      });

      const query = await Resource.find(result.filters || {});
      expect(query.map((r) => r.key).sort()).toEqual(["a", "c"]);
    });

    test("empty value list yields CEL identity semantics", async () => {
      // exists over [] is false; all over [] is true. MongoDB rejects an empty
      // $or/$and, so the constant is stated through $expr.
      const existsResult = queryPlanToMongoose({
        queryPlan: valueListPlan("exists", [], compareBody("eq")),
        mapper: stringMapper,
      });
      expect(existsResult).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { $expr: false },
      });
      expect(await Resource.find(existsResult.filters || {})).toHaveLength(0);

      const allResult = queryPlanToMongoose({
        queryPlan: valueListPlan("all", [], compareBody("ne")),
        mapper: stringMapper,
      });
      expect(allResult).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: { $expr: true },
      });
      expect(await Resource.find(allResult.filters || {})).toHaveLength(
        fixtureResources.length
      );
    });

    test("nested lambda rebinding the variable shadows the outer substitution", async () => {
      // The outer t is substituted; the inner exists rebinds t over a relation,
      // so its body must keep referencing the inner binding untouched.
      const result = queryPlanToMongoose({
        queryPlan: valueListPlan(
          "exists",
          ["public", "private"],
          new PlanExpression("exists", [
            new PlanExpressionVariable("request.resource.attr.tags"),
            new PlanExpression("lambda", [
              new PlanExpression("eq", [
                new PlanExpressionVariable("t.name"),
                new PlanExpressionValue("public"),
              ]),
              new PlanExpressionVariable("t"),
            ]),
          ])
        ),
        mapper: {
          "request.resource.attr.tags": {
            relation: { name: "tags", type: "many", field: "name" },
          },
        },
      });

      expect(result).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters: {
          $or: [
            { tags: { $elemMatch: { name: { $eq: "public" } } } },
            { tags: { $elemMatch: { name: { $eq: "public" } } } },
          ],
        },
      });
    });

    test("keeps the enclosing collection scope when nested in another macro", () => {
      // A fold inside another macro's lambda must still reject outer-document
      // references: $elemMatch resolves paths against the element, so emitting
      // a filter here would silently compare the wrong field.
      const queryPlan = conditionalPlan(
        new PlanExpression("exists", [
          new PlanExpressionVariable("request.resource.attr.tags"),
          new PlanExpression("lambda", [
            new PlanExpression("exists", [
              new PlanExpressionValue(["public"]),
              new PlanExpression("lambda", [
                new PlanExpression("eq", [
                  new PlanExpressionVariable("request.resource.attr.aBool"),
                  new PlanExpressionVariable("t"),
                ]),
                new PlanExpressionVariable("t"),
              ]),
            ]),
            new PlanExpressionVariable("tag"),
          ]),
        ])
      );

      expect(() =>
        queryPlanToMongoose({
          queryPlan,
          mapper: {
            ...defaultMapper,
            "request.resource.attr.tags": {
              relation: { name: "tags", type: "many", field: "name" },
            },
          },
        })
      ).toThrow("Outer reference request.resource.attr.aBool");
    });

    test("throws when a variable path is missing on an element", () => {
      expect(() =>
        queryPlanToMongoose({
          queryPlan: valueListPlan(
            "exists",
            [{ name: "string" }],
            new PlanExpression("eq", [
              new PlanExpressionVariable("request.resource.attr.aString"),
              new PlanExpressionVariable("t.missing"),
            ])
          ),
          mapper: stringMapper,
        })
      ).toThrow('Cannot resolve "t.missing"');
    });

    test.each(["exists_one", "filter", "map", "except"])(
      "throws for %s over a value list",
      (operator) => {
        expect(() =>
          queryPlanToMongoose({
            queryPlan: valueListPlan(operator, ["string"], compareBody("eq")),
            mapper: stringMapper,
          })
        ).toThrow(
          `${operator} over a literal collection value is not supported`
        );
      }
    );

    test("throws for a non-list collection value", () => {
      expect(() =>
        queryPlanToMongoose({
          queryPlan: valueListPlan(
            "exists",
            { not: "a list" },
            compareBody("eq")
          ),
          mapper: stringMapper,
        })
      ).toThrow("exists over a literal collection requires a list value");
    });
  });
});
