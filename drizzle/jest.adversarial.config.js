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
  // Isolated from the main jest.config.js run: this suite replays the conformance goldens against
  // a real store (Docker for the PostgreSQL and MySQL legs).
  testMatch: ["<rootDir>/src/adversarial.test.ts"],
};
