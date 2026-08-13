const esModules = ["uuid", "@cerbos"].join("|");

/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest/presets/default-esm",
  testEnvironment: "node",
  extensionsToTreatAsEsm: [".ts"],
  transform: {
    "^.+\\.[tj]sx?$": [
      "ts-jest",
      {
        useESM: true,
      },
    ],
  },
  transformIgnorePatterns: [`node_modules/(?!(${esModules})/)`],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
  // The offline suite. Both other suites need a Convex backend — and the adversarial one a Cerbos
  // sidecar as well — and both import `convex/_generated`, which `npx convex codegen` produces
  // against a live deployment and .gitignore excludes. They have their own configs
  // (jest.adversarial.config.js, jest.integration.config.js) and are skipped here, so `npm test`
  // stays runnable with nothing installed but node.
  testPathIgnorePatterns: [
    "/node_modules/",
    "<rootDir>/src/adversarial.test.ts",
    "<rootDir>/src/integration.test.ts",
  ],
};
