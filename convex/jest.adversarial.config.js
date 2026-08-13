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
  // Isolated from the main jest.config.js run: this suite needs a Cerbos sidecar loaded with
  // conformance/policies (resource kind "adversarial") AND a deployed Convex backend.
  testMatch: ["<rootDir>/src/adversarial.test.ts"],
};
