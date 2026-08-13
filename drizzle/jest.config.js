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
  // The offline suite. The adversarial harness needs a Cerbos sidecar and (on the PostgreSQL leg)
  // Docker, so it has its own config (jest.adversarial.config.js) and is skipped here — `npm test`
  // must stay runnable with nothing installed but node.
  testPathIgnorePatterns: [
    "/node_modules/",
    "<rootDir>/src/adversarial.test.ts",
  ],
};
