/** @type {import("ts-jest").JestConfigWithTsJest} */
module.exports = {
  testEnvironment: "node",
  extensionsToTreatAsEsm: [".ts"],
  transform: {
    "^.+\\.[tj]sx?$": [
      "ts-jest",
      { useESM: true, tsconfig: "<rootDir>/tsconfig.json" },
    ],
  },
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
  // Replays the conformance goldens against a real MongoDB (`npm run mongo` in the adapter).
  testMatch: ["<rootDir>/adversarial.test.ts"],
};
