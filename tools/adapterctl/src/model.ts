export type Command =
  | { kind: "unavailable" }
  | { kind: "command"; arguments: [string, ...string[]] };

export type Outcome =
  | { kind: "matched" }
  | { kind: "rejected"; reason: string; message: string }
  | { kind: "upstream-blocked"; reason: string }
  | { kind: "unassessed" };

export type SemanticEnvironment = {
  name: string;
  command: Extract<Command, { kind: "command" }>;
  env: Record<string, string>;
};

export type Manifest = {
  schemaVersion: 1;
  adapter: string;
  package: { ecosystem: string; name: string };
  workflow: string;
  commands: {
    test: Command;
    typecheck: Command;
    golden: Command;
    consumer: Command;
  };
  semanticEnvironments: SemanticEnvironment[];
  consumer: { coverage: "artifact-install" | "usage-only" };
  outcomes: Map<string, Outcome>;
};

export type OracleExpectation =
  | { kind: "proper-subset" }
  | { kind: "empty"; reason: string }
  | { kind: "total"; reason: string };

export type CatalogAction = { name: string; oracleExpectation: OracleExpectation };
export type Catalog = { schemaVersion: 1; actions: CatalogAction[] };

export type ConsumerCase = {
  id: string;
  operation: "filtered" | "alwaysAllowed" | "alwaysDenied" | "paginated" | "composed";
  principal: string;
  action: string;
  pagination: null | { pageSize: number; pageSizes: number[] };
  expected: {
    kind: "KIND_CONDITIONAL" | "KIND_ALWAYS_ALLOWED" | "KIND_ALWAYS_DENIED";
    ids: string[];
  };
};

export type ConsumerCases = { schemaVersion: 1; cases: ConsumerCase[] };
export type CheckResource = { kind: string; id: string; attr: Record<string, unknown> };
export type CheckResources = {
  schemaVersion: 1;
  principal: Record<string, unknown>;
  resources: CheckResource[];
};

export type ControlPlane = {
  catalog: Catalog;
  cases: ConsumerCases;
  checkResources: CheckResources;
  manifests: Manifest[];
};
