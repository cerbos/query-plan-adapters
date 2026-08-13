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
  // Isolated from the main jest.config.js run: this suite executes the translated filter inside a
  // deployed Convex backend, against a Cerbos sidecar loaded with the shared ../policies suite.
  testMatch: ["<rootDir>/src/integration.test.ts"],
};
