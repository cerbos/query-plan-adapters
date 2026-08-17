const prismaVersion = process.env.PRISMA_VERSION || "7";
const store = process.env.ADAPTER_TEST_DB || "sqlite";

// `./test-setup.adversarial` resolves to the (store x Prisma major) fixture: each combination has
// its own generated client, because the provider is baked into a generated Prisma client and the
// two majors generate incompatible ones. SQLite carries no infix, being the default the file
// names were written around.
const STORE_SUFFIXES = {
  sqlite: "",
  postgres: ".pg",
  mysql: ".mysql",
};
if (!Object.hasOwn(STORE_SUFFIXES, store)) {
  // A typo must fail rather than silently fall back to SQLite: a CI leg that believes it is
  // proving PostgreSQL or MySQL while replaying SQLite is exactly the coverage gap #320 and #340
  // close.
  throw new Error(
    `Unknown ADAPTER_TEST_DB "${store}": expected one of ${Object.keys(
      STORE_SUFFIXES
    ).join(", ")}`
  );
}

const setupSuffix = STORE_SUFFIXES[store];

/** @type {import("ts-jest").JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest/presets/default-esm",
  testEnvironment: "node",
  extensionsToTreatAsEsm: [".ts"],
  transform: {
    "^.+\\.[tj]sx?$": [
      "ts-jest",
      {
        useESM: true,
        tsconfig: "tsconfig.jest.json",
      },
    ],
  },
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
    "^(.*)/test-setup\\.adversarial$":
      `$1/test-setup.adversarial${setupSuffix}.v${prismaVersion}`,
  },
  // A container-backed leg's container has to exist before any test module builds a client, and
  // its connection string only exists once it does — so it is started here rather than by the npm
  // script. The SQLite leg's setup is a no-op.
  globalSetup: "<rootDir>/jest.globalSetup.adversarial.js",
  globalTeardown: "<rootDir>/jest.globalTeardown.adversarial.js",
  // Isolated from the main jest.config.js run: this suite talks to a Cerbos sidecar loaded
  // with conformance/policies (resource kind "adversarial").
  testMatch: ["<rootDir>/src/adversarial.test.ts"],
};
