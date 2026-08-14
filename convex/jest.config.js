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
  // The offline suite. The adversarial suite needs a Convex backend and a Cerbos sidecar, and it
  // imports `convex/_generated`, which `npx convex codegen` produces against a live deployment and
  // .gitignore excludes. It has its own config (jest.adversarial.config.js) and is skipped here,
  // so `npm test` stays runnable with nothing installed but node.
  testPathIgnorePatterns: [
    "/node_modules/",
    "<rootDir>/src/adversarial.test.ts",
  ],
};
