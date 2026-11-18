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
  // Replays the conformance goldens against the deployed Convex backend in ./convex (`npm run up`, then `deploy` and `codegen`).
  testMatch: ["<rootDir>/adversarial.test.ts"],
};
