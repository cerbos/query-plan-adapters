import Type from "typebox";

const draft = "https://json-schema.org/draft/2020-12/schema";
const schemaBase = "https://cerbos.dev/query-plan-adapters/adapterctl";
const allPropertyNames = Type.String({ pattern: "^[\\s\\S]*$" });
const nonEmptyString = Type.String({ minLength: 1 });
const openObject = Type.Record(allPropertyNames, Type.Unknown());

const commandDocumentSchema = Type.Array(nonEmptyString, { minItems: 1 });

const outcomeDocumentSchema = Type.Union([
  Type.Object({ status: Type.Literal("matched") }, { additionalProperties: false }),
  Type.Object({
    status: Type.Literal("rejected"),
    reason: nonEmptyString,
    message: nonEmptyString,
  }, { additionalProperties: false }),
  Type.Object({
    status: Type.Literal("upstream-blocked"),
    reason: nonEmptyString,
  }, { additionalProperties: false }),
  Type.Object({ status: Type.Literal("unassessed") }, { additionalProperties: false }),
]);

export const manifestDocumentSchema = Type.Object({
  schemaVersion: Type.Literal(1),
  adapter: Type.String({ pattern: "^[a-z0-9]+(?:-[a-z0-9]+)*$" }),
  package: Type.Object({
    ecosystem: nonEmptyString,
    name: nonEmptyString,
  }, { additionalProperties: false }),
  workflow: nonEmptyString,
  commands: Type.Object({
    test: Type.Union([Type.Null(), commandDocumentSchema]),
    typecheck: Type.Union([Type.Null(), commandDocumentSchema]),
    golden: Type.Union([Type.Null(), commandDocumentSchema]),
    consumer: Type.Union([Type.Null(), commandDocumentSchema]),
  }, { additionalProperties: false }),
  semanticEnvironments: Type.Array(Type.Object({
    name: nonEmptyString,
    command: commandDocumentSchema,
    env: Type.Record(allPropertyNames, Type.String()),
  }, { additionalProperties: false })),
  consumer: Type.Object({
    coverage: Type.Union([Type.Literal("artifact-install"), Type.Literal("usage-only")]),
  }, { additionalProperties: false }),
  outcomes: Type.Record(allPropertyNames, outcomeDocumentSchema),
}, {
  $schema: draft,
  $id: `${schemaBase}/manifest-v1.schema.json`,
  additionalProperties: false,
});

const oracleExpectationSchema = Type.Union([
  Type.Object({ kind: Type.Literal("proper-subset") }, { additionalProperties: false }),
  Type.Object({
    kind: Type.Literal("empty"),
    reason: nonEmptyString,
  }, { additionalProperties: false }),
  Type.Object({
    kind: Type.Literal("total"),
    reason: nonEmptyString,
  }, { additionalProperties: false }),
]);

export const catalogDocumentSchema = Type.Object({
  schemaVersion: Type.Literal(1),
  actions: Type.Array(Type.Object({
    name: nonEmptyString,
    oracleExpectation: oracleExpectationSchema,
  }, { additionalProperties: false }), { minItems: 1 }),
}, {
  $schema: draft,
  $id: `${schemaBase}/catalog-v1.schema.json`,
  additionalProperties: false,
});

const expectedConsumerResultSchema = Type.Object({
  kind: Type.Union([
    Type.Literal("KIND_CONDITIONAL"),
    Type.Literal("KIND_ALWAYS_ALLOWED"),
    Type.Literal("KIND_ALWAYS_DENIED"),
  ]),
  ids: Type.Array(nonEmptyString),
}, { additionalProperties: false });

const consumerCaseProperties = {
  id: nonEmptyString,
  principal: nonEmptyString,
  action: nonEmptyString,
  expected: expectedConsumerResultSchema,
};

const nonPaginatedConsumerCaseSchema = Type.Object({
  ...consumerCaseProperties,
  operation: Type.Union([
    Type.Literal("filtered"),
    Type.Literal("alwaysAllowed"),
    Type.Literal("alwaysDenied"),
    Type.Literal("composed"),
  ]),
  pagination: Type.Null(),
}, { additionalProperties: false });

const paginatedConsumerCaseSchema = Type.Object({
  ...consumerCaseProperties,
  operation: Type.Literal("paginated"),
  pagination: Type.Object({
    pageSize: Type.Integer({ minimum: 1 }),
    pageSizes: Type.Array(Type.Integer({ minimum: 1 }), { minItems: 1 }),
  }, { additionalProperties: false }),
}, { additionalProperties: false });

export const consumerCasesDocumentSchema = Type.Object({
  schemaVersion: Type.Literal(1),
  cases: Type.Array(Type.Union([
    nonPaginatedConsumerCaseSchema,
    paginatedConsumerCaseSchema,
  ]), { minItems: 1 }),
}, {
  $schema: draft,
  $id: `${schemaBase}/consumer-cases-v1.schema.json`,
  additionalProperties: false,
});

export const checkResourcesDocumentSchema = Type.Object({
  schemaVersion: Type.Literal(1),
  principal: openObject,
  resources: Type.Array(Type.Object({
    kind: nonEmptyString,
    id: nonEmptyString,
    attr: openObject,
  }, { additionalProperties: false })),
}, {
  $schema: draft,
  $id: `${schemaBase}/check-resources-v1.schema.json`,
  additionalProperties: false,
});

export type ManifestDocument = Type.Static<typeof manifestDocumentSchema>;
export type CatalogDocument = Type.Static<typeof catalogDocumentSchema>;
export type ConsumerCasesDocument = Type.Static<typeof consumerCasesDocumentSchema>;
export type CheckResourcesDocument = Type.Static<typeof checkResourcesDocumentSchema>;

export type SchemaName = "manifest" | "catalog" | "check-resources" | "consumer-cases";

export const schemaArtifacts = [
  { name: "manifest", fileName: "manifest-v1.schema.json", schema: manifestDocumentSchema },
  { name: "catalog", fileName: "catalog-v1.schema.json", schema: catalogDocumentSchema },
  {
    name: "check-resources",
    fileName: "check-resources-v1.schema.json",
    schema: checkResourcesDocumentSchema,
  },
  {
    name: "consumer-cases",
    fileName: "consumer-cases-v1.schema.json",
    schema: consumerCasesDocumentSchema,
  },
] satisfies Array<{ name: SchemaName; fileName: string; schema: object }>;
