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
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
  // Isolated from the main jest.config.js run: this suite talks to a Cerbos sidecar loaded
  // with conformance/policies (resource kind "adversarial") on dedicated ports (gRPC 3621),
  // not the ../policies sidecar on the default ports.
  testMatch: ["<rootDir>/src/adversarial.test.ts"],
};
