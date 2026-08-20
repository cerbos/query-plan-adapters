import type {
  CatalogDocument,
  CheckResourcesDocument,
  ConsumerCasesDocument,
  ManifestDocument,
} from "./schemas.ts";

export type Command =
  | { kind: "unavailable" }
  | { kind: "command"; arguments: [string, ...string[]] };

export type Outcome =
  | { kind: "matched" }
  | { kind: "rejected"; reason: string; message: string }
  | { kind: "upstream-blocked"; reason: string }
  | { kind: "unassessed" };

export type SemanticEnvironment = Omit<ManifestDocument["semanticEnvironments"][number], "command"> & {
  command: Extract<Command, { kind: "command" }>;
};

export type Manifest = Omit<ManifestDocument, "commands" | "semanticEnvironments" | "outcomes"> & {
  commands: {
    test: Command;
    typecheck: Command;
    golden: Command;
    consumer: Command;
  };
  semanticEnvironments: SemanticEnvironment[];
  outcomes: Map<string, Outcome>;
};

export type OracleExpectation = CatalogDocument["actions"][number]["oracleExpectation"];
export type CatalogAction = CatalogDocument["actions"][number];
export type Catalog = CatalogDocument;

export type ConsumerCase = ConsumerCasesDocument["cases"][number];
export type ConsumerCases = ConsumerCasesDocument;
export type CheckResource = CheckResourcesDocument["resources"][number];
export type CheckResources = CheckResourcesDocument;

export type ControlPlane = {
  catalog: Catalog;
  cases: ConsumerCases;
  checkResources: CheckResources;
  manifests: Manifest[];
};
